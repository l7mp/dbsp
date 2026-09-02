// The conformance harness runs the full delta-gateway stack on localhost so
// the upstream Gateway API conformance suite can be executed without a
// Kubernetes cluster:
//
//   - an envtest control plane (kube-apiserver + etcd) with the Gateway API
//     CRDs installed,
//   - the delta-gateway operator (a dbsp JavaScript program) as a child
//     process pointed at the envtest API server,
//   - a real Envoy connected to the operator's xDS server over ADS,
//   - a fake infrastructure controller that stands in for the kubelet and the
//     endpoint-slice controller: every conformance backend Service is backed
//     by an in-process echo server on a loopback port, published to the
//     operator through a hand-written EndpointSlice.
//
// The suite derives the gateway address from Gateway status.addresses and the
// listener spec port (80, 443, ...). Binding those privileged ports needs no
// root when the harness runs inside a user+network namespace; see run.sh,
// which wraps everything in `unshare -r -n` with the loopback up.
//
// With a command after `--` the harness runs it once the stack is ready
// (KUBECONFIG set) and exits with its status; without one it runs until
// SIGINT/SIGTERM.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
)

// conformanceNamespaces are the namespaces the suite's base manifests create;
// the fake infra controller only manages Services in these.
var conformanceNamespaces = []string{
	"gateway-conformance-infra",
	"gateway-conformance-app-backend",
	"gateway-conformance-web-backend",
}

type options struct {
	crdDir         string
	envtestBin     string
	repoRoot       string
	dbspBin        string
	dgwMode        string
	envoyBin       string
	envoyAdmin     string
	xdsAddress     string
	backendAddress string
	gatewayClass   string
	controllerName string
	kubeconfig     string
	readyTimeout   time.Duration
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "harness:", err)
		os.Exit(1)
	}
}

func run() error {
	var opts options
	flag.StringVar(&opts.crdDir, "crd-dir", os.Getenv("DELTA_GW_CRD_DIR"), "directory with the Gateway API CRD manifests")
	flag.StringVar(&opts.envtestBin, "envtest-bin", os.Getenv("DELTA_GW_ENVTEST_BIN"), "directory with the envtest kube-apiserver/etcd binaries")
	flag.StringVar(&opts.repoRoot, "repo-root", ".", "dbsp repository root")
	flag.StringVar(&opts.dbspBin, "dbsp", "js/bin/dbsp", "dbsp runtime binary, relative to the repo root")
	flag.StringVar(&opts.dgwMode, "dgw-mode", "reconciler", "delta-gateway --mode (reconciler|open|sotw|smith)")
	flag.StringVar(&opts.envoyBin, "envoy", "/usr/bin/envoy", "envoy binary")
	flag.StringVar(&opts.envoyAdmin, "envoy-admin", "127.0.0.1:19001", "envoy admin address")
	flag.StringVar(&opts.xdsAddress, "xds-address", "127.0.0.1:18000", "operator xDS server address")
	flag.StringVar(&opts.backendAddress, "backend-address", "10.44.0.1", "non-loopback local address the echo backends listen on (the API server rejects loopback EndpointSlice addresses)")
	flag.StringVar(&opts.gatewayClass, "gateway-class", "delta-gateway", "GatewayClass name to create")
	flag.StringVar(&opts.controllerName, "controller-name", "dbsp.l7mp.io/delta-gateway", "GatewayClass controllerName")
	flag.StringVar(&opts.kubeconfig, "kubeconfig-out", "/tmp/delta-gateway-conformance.kubeconfig", "path to write the envtest kubeconfig to")
	flag.DurationVar(&opts.readyTimeout, "ready-timeout", 90*time.Second, "how long to wait for the stack to become ready")
	flag.Parse()
	command := flag.Args()

	if opts.crdDir == "" || opts.envtestBin == "" {
		return fmt.Errorf("both -crd-dir and -envtest-bin are required (or DELTA_GW_CRD_DIR / DELTA_GW_ENVTEST_BIN)")
	}
	repoRoot, err := filepath.Abs(opts.repoRoot)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Control plane.
	logf("starting envtest control plane")
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{opts.crdDir},
		ErrorIfCRDPathMissing: true,
		BinaryAssetsDirectory: opts.envtestBin,
	}
	// The network namespace has no default route, from which kube-apiserver
	// would otherwise derive its advertise address.
	env.ControlPlane.GetAPIServer().Configure().Set("advertise-address", "127.0.0.1")
	cfg, err := env.Start()
	if err != nil {
		return fmt.Errorf("start envtest: %w", err)
	}
	defer func() {
		logf("stopping envtest control plane")
		_ = env.Stop()
	}()

	if err := writeKubeconfig(cfg, opts.kubeconfig); err != nil {
		return err
	}
	logf("kubeconfig written to %s (server %s)", opts.kubeconfig, cfg.Host)

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := gatewayv1.Install(scheme); err != nil {
		return err
	}
	cl, err := ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}

	// GatewayClass.
	gc := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: opts.gatewayClass},
		Spec:       gatewayv1.GatewayClassSpec{ControllerName: gatewayv1.GatewayController(opts.controllerName)},
	}
	if err := cl.Create(ctx, gc); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create GatewayClass: %w", err)
	}
	logf("GatewayClass %s created (controller %s)", opts.gatewayClass, opts.controllerName)

	// Fake infrastructure: echo backends and EndpointSlices.
	infra := &fakeInfra{clientset: clientset, address: opts.backendAddress, echos: map[string]*echoBackend{}}
	// The service-types conformance test fills its manually-managed
	// EndpointSlices with the infra-backend-v1 Pod IPs at the container
	// port (3000), so that app answers on the fixed port too (the fake Pods
	// created in reconcile() report the shared backend address as their IP).
	if err := infra.serveFixedPort("gateway-conformance-infra", "infra-backend-v1", 3000); err != nil {
		return fmt.Errorf("fixed-port echo: %w", err)
	}
	go infra.runLoop(ctx)

	// The operator.
	operator, err := spawn(ctx, "operator", repoRoot,
		append(os.Environ(), "KUBECONFIG="+opts.kubeconfig),
		filepath.Join(repoRoot, opts.dbspBin), "apps/dgateway/index.js", "controller",
		"--mode", opts.dgwMode,
		"--xds-address", opts.xdsAddress,
		"--address-pool", "127.0.0.0/16")
	if err != nil {
		return fmt.Errorf("start operator: %w", err)
	}
	defer stop(operator)

	// Envoy.
	bootstrap, err := writeEnvoyBootstrap(opts.xdsAddress, opts.envoyAdmin)
	if err != nil {
		return err
	}
	envoy, err := spawn(ctx, "envoy", repoRoot, os.Environ(),
		opts.envoyBin, "-c", bootstrap, "--disable-hot-restart", "-l", "warn")
	if err != nil {
		return fmt.Errorf("start envoy: %w", err)
	}
	defer stop(envoy)

	if err := waitReady(ctx, cl, opts); err != nil {
		return err
	}
	logf("READY: stack is up, kubeconfig at %s", opts.kubeconfig)

	if len(command) > 0 {
		cmd := exec.Command(command[0], command[1:]...)
		cmd.Dir = repoRoot
		cmd.Env = append(os.Environ(),
			"KUBECONFIG="+opts.kubeconfig,
			"DELTA_GATEWAY_CONFORMANCE=1")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		err := cmd.Run()
		if err != nil {
			return fmt.Errorf("command failed: %w", err)
		}
		return nil
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logf("signal received, shutting down")
	return nil
}

func logf(format string, args ...any) {
	fmt.Printf("[harness] "+format+"\n", args...)
}

// ---------------------------------------------------------------------------
// Child processes.

func spawn(ctx context.Context, name, dir string, env []string, bin string, args ...string) (*exec.Cmd, error) {
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = dir
	cmd.Env = env
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = cmd.Stdout // interleave; both go through the prefixer
	go prefixLines(name, stdout)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	logf("%s started (pid %d)", name, cmd.Process.Pid)
	return cmd, nil
}

func prefixLines(name string, r io.Reader) {
	buf := make([]byte, 64*1024)
	var pending string
	for {
		n, err := r.Read(buf)
		if n > 0 {
			pending += string(buf[:n])
			for {
				idx := strings.IndexByte(pending, '\n')
				if idx < 0 {
					break
				}
				fmt.Printf("[%s] %s\n", name, pending[:idx])
				pending = pending[idx+1:]
			}
		}
		if err != nil {
			if pending != "" {
				fmt.Printf("[%s] %s\n", name, pending)
			}
			return
		}
	}
}

func stop(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
}

// ---------------------------------------------------------------------------
// Kubeconfig.

func writeKubeconfig(cfg *rest.Config, path string) error {
	c := clientcmdapi.NewConfig()
	c.Clusters["envtest"] = &clientcmdapi.Cluster{
		Server:                   cfg.Host,
		CertificateAuthorityData: cfg.CAData,
	}
	c.AuthInfos["envtest"] = &clientcmdapi.AuthInfo{
		ClientCertificateData: cfg.CertData,
		ClientKeyData:         cfg.KeyData,
		Token:                 cfg.BearerToken,
	}
	c.Contexts["envtest"] = &clientcmdapi.Context{Cluster: "envtest", AuthInfo: "envtest"}
	c.CurrentContext = "envtest"
	return clientcmd.WriteToFile(*c, path)
}

// ---------------------------------------------------------------------------
// Readiness.

func waitReady(ctx context.Context, cl ctrlclient.Client, opts options) error {
	deadline := time.Now().Add(opts.readyTimeout)
	var lastErr error
	for time.Now().Before(deadline) {
		lastErr = func() error {
			// Envoy admin answers /ready.
			resp, err := http.Get("http://" + opts.envoyAdmin + "/ready")
			if err != nil {
				return fmt.Errorf("envoy not ready: %w", err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("envoy not ready: status %d", resp.StatusCode)
			}
			// The operator has accepted the GatewayClass, which proves the
			// full watch -> circuit -> status-patch loop is live.
			gc := &gatewayv1.GatewayClass{}
			if err := cl.Get(ctx, ctrlclient.ObjectKey{Name: opts.gatewayClass}, gc); err != nil {
				return fmt.Errorf("get GatewayClass: %w", err)
			}
			for _, cond := range gc.Status.Conditions {
				if cond.Type == string(gatewayv1.GatewayClassConditionStatusAccepted) && cond.Status == metav1.ConditionTrue {
					return nil
				}
			}
			return fmt.Errorf("GatewayClass not accepted yet")
		}()
		if lastErr == nil {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("stack not ready after %s: %w", opts.readyTimeout, lastErr)
}

// ---------------------------------------------------------------------------
// Envoy bootstrap.

func writeEnvoyBootstrap(xdsAddress, adminAddress string) (string, error) {
	xdsHost, xdsPort, err := net.SplitHostPort(xdsAddress)
	if err != nil {
		return "", fmt.Errorf("bad xds address %q: %w", xdsAddress, err)
	}
	adminHost, adminPort, err := net.SplitHostPort(adminAddress)
	if err != nil {
		return "", fmt.Errorf("bad admin address %q: %w", adminAddress, err)
	}
	bootstrap := fmt.Sprintf(`node:
  id: delta-gateway-conformance
  cluster: delta-gateway
dynamic_resources:
  ads_config:
    api_type: DELTA_GRPC
    transport_api_version: V3
    grpc_services:
    - envoy_grpc:
        cluster_name: xds_cluster
  lds_config:
    ads: {}
    resource_api_version: V3
  cds_config:
    ads: {}
    resource_api_version: V3
static_resources:
  clusters:
  - name: xds_cluster
    type: STATIC
    connect_timeout: 1s
    typed_extension_protocol_options:
      envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
        "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
        explicit_http_config:
          http2_protocol_options: {}
    load_assignment:
      cluster_name: xds_cluster
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address:
                address: %s
                port_value: %s
admin:
  address:
    socket_address:
      address: %s
      port_value: %s
`, xdsHost, xdsPort, adminHost, adminPort)
	f, err := os.CreateTemp("", "delta-gw-envoy-*.yaml")
	if err != nil {
		return "", err
	}
	defer f.Close()
	if _, err := f.WriteString(bootstrap); err != nil {
		return "", err
	}
	return f.Name(), nil
}

// ---------------------------------------------------------------------------
// Fake infrastructure: echo backends + EndpointSlices.
//
// The conformance base manifests create Deployments and selector Services for
// the echo backends. Without a kubelet no Pods ever run, so for every Service
// with an `app` selector in the conformance namespaces the harness runs an
// in-process echo server on a loopback port and publishes it through an
// EndpointSlice, exactly as the endpoint-slice controller would. The echo
// protocol mirrors the upstream conformance echo-basic server: the response
// body is a JSON document with the request path/host/method/proto/headers
// plus the pod/namespace identity the tests assert on.

type fakeInfra struct {
	clientset *kubernetes.Clientset
	address   string // local non-loopback address the echo servers bind
	mu        sync.Mutex
	echos     map[string]*echoBackend // key: namespace/app
}

type echoBackend struct {
	namespace string
	app       string
	port      int
}

func (f *fakeInfra) runLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			f.reconcile(ctx)
		}
	}
}

func (f *fakeInfra) reconcile(ctx context.Context) {
	for _, ns := range conformanceNamespaces {
		svcs, err := f.clientset.CoreV1().Services(ns).List(ctx, metav1.ListOptions{})
		if err != nil {
			continue // namespace not created yet, or being torn down
		}
		// One fake Running/Ready Pod per Deployment: some tests list a
		// Deployment's Pods and use their IPs (the service-types test
		// patches its manual EndpointSlices from them).
		if deps, err := f.clientset.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{}); err == nil {
			for i := range deps.Items {
				if err := f.ensurePod(ctx, &deps.Items[i]); err != nil && !apierrors.IsConflict(err) {
					logf("fake pod for %s/%s: %v", ns, deps.Items[i].Name, err)
				}
			}
		}
		for i := range svcs.Items {
			svc := &svcs.Items[i]
			app := svc.Spec.Selector["app"]
			if app == "" {
				continue // ExternalName and selectorless Services get no endpoints from us
			}
			tcpPorts := []corev1.ServicePort{}
			for _, p := range svc.Spec.Ports {
				if p.Protocol == "" || p.Protocol == corev1.ProtocolTCP {
					tcpPorts = append(tcpPorts, p)
				}
			}
			if len(tcpPorts) == 0 {
				continue // e.g. coredns (UDP only)
			}
			echo, err := f.ensureEcho(svc.Namespace, app)
			if err != nil {
				logf("echo for %s/%s: %v", svc.Namespace, app, err)
				continue
			}
			if err := f.ensureEndpointSlice(ctx, svc, tcpPorts, echo); err != nil && !apierrors.IsConflict(err) {
				logf("endpointslice for %s/%s: %v", svc.Namespace, svc.Name, err)
			}
		}
	}
}

// ensurePod keeps one fake Running/Ready Pod alive per Deployment, labeled
// by the Deployment's selector and reporting the shared backend address as
// its Pod IP. No kubelet ever runs it; the object only exists for tests that
// list a Deployment's Pods and read their IPs.
func (f *fakeInfra) ensurePod(ctx context.Context, dep *appsv1.Deployment) error {
	name := dep.Name + "-fake0000-fake0"
	pods := f.clientset.CoreV1().Pods(dep.Namespace)
	if _, err := pods.Get(ctx, name, metav1.GetOptions{}); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: dep.Namespace,
			Labels:    dep.Spec.Selector.MatchLabels,
		},
		Spec: corev1.PodSpec{
			NodeName:   "delta-gateway-fake-node",
			Containers: []corev1.Container{{Name: "echo", Image: "fake"}},
		},
	}
	created, err := pods.Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	created.Status = corev1.PodStatus{
		Phase:  corev1.PodRunning,
		PodIP:  f.address,
		PodIPs: []corev1.PodIP{{IP: f.address}},
		Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			{Type: corev1.ContainersReady, Status: corev1.ConditionTrue},
		},
	}
	if _, err := pods.UpdateStatus(ctx, created, metav1.UpdateOptions{}); err != nil {
		return err
	}
	logf("fake pod %s/%s (ip %s)", dep.Namespace, name, f.address)
	return nil
}

// serveFixedPort binds an extra echo listener for one app on a well-known
// port (the conformance Deployments' container port, which manually-managed
// EndpointSlices reference via the fake Pod IPs).
func (f *fakeInfra) serveFixedPort(namespace, app string, port int) error {
	ln, err := net.Listen("tcp", net.JoinHostPort(f.address, strconv.Itoa(port)))
	if err != nil {
		return err
	}
	e := &echoBackend{namespace: namespace, app: app, port: port}
	srv := &http.Server{Handler: &slashPreservingHandler{newEchoHandler(e)}}
	go func() { _ = srv.Serve(ln) }()
	logf("fixed-port echo %s/%s listening on %s:%d", namespace, app, f.address, port)
	return nil
}

func (f *fakeInfra) ensureEcho(namespace, app string) (*echoBackend, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := namespace + "/" + app
	if e, ok := f.echos[key]; ok {
		return e, nil
	}
	ln, err := net.Listen("tcp", net.JoinHostPort(f.address, "0"))
	if err != nil {
		return nil, err
	}
	e := &echoBackend{namespace: namespace, app: app, port: ln.Addr().(*net.TCPAddr).Port}
	srv := &http.Server{Handler: &slashPreservingHandler{newEchoHandler(e)}}
	go func() { _ = srv.Serve(ln) }()
	f.echos[key] = e
	logf("echo backend %s/%s listening on %s:%d", namespace, app, f.address, e.port)
	return e, nil
}

func (f *fakeInfra) ensureEndpointSlice(ctx context.Context, svc *corev1.Service, tcpPorts []corev1.ServicePort, echo *echoBackend) error {
	name := svc.Name + "-local"
	ports := []discoveryv1.EndpointPort{}
	for i := range tcpPorts {
		p := tcpPorts[i]
		proto := corev1.ProtocolTCP
		port := int32(echo.port)
		ports = append(ports, discoveryv1.EndpointPort{
			Name:     &p.Name,
			Protocol: &proto,
			Port:     &port,
		})
	}
	ready := true
	slice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: svc.Namespace,
			Labels: map[string]string{
				"kubernetes.io/service-name":             svc.Name,
				"endpointslice.kubernetes.io/managed-by": "delta-gateway-conformance-harness",
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses:  []string{f.address},
			Conditions: discoveryv1.EndpointConditions{Ready: &ready},
		}},
		Ports: ports,
	}
	existing, err := f.clientset.DiscoveryV1().EndpointSlices(svc.Namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = f.clientset.DiscoveryV1().EndpointSlices(svc.Namespace).Create(ctx, slice, metav1.CreateOptions{})
		if err == nil {
			logf("endpointslice %s/%s -> %s:%d", svc.Namespace, name, f.address, echo.port)
		}
		return err
	}
	if err != nil {
		return err
	}
	if endpointSliceUpToDate(existing, slice) {
		return nil
	}
	slice.ResourceVersion = existing.ResourceVersion
	_, err = f.clientset.DiscoveryV1().EndpointSlices(svc.Namespace).Update(ctx, slice, metav1.UpdateOptions{})
	return err
}

func endpointSliceUpToDate(existing, desired *discoveryv1.EndpointSlice) bool {
	a, _ := json.Marshal([]any{existing.AddressType, existing.Endpoints, existing.Ports})
	b, _ := json.Marshal([]any{desired.AddressType, desired.Endpoints, desired.Ports})
	return string(a) == string(b)
}

// ---------------------------------------------------------------------------
// The echo protocol (mirrors conformance/echo-basic).

// requestAssertions is the response body contract the conformance
// roundtripper parses (roundtripper.CapturedRequest).
type requestAssertions struct {
	Path    string              `json:"path"`
	Host    string              `json:"host"`
	Method  string              `json:"method"`
	Proto   string              `json:"proto"`
	Headers map[string][]string `json:"headers"`

	Namespace string `json:"namespace"`
	Ingress   string `json:"ingress"`
	Service   string `json:"service"`
	Pod       string `json:"pod"`
}

type slashPreservingHandler struct{ next http.Handler }

func (s *slashPreservingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.URL.Path = strings.ReplaceAll(r.URL.Path, "//", "/")
	s.next.ServeHTTP(w, r)
}

var statusRe = regexp.MustCompile(`^/status/(\d\d\d)$`)

func newEchoHandler(e *echoBackend) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/health":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
			return
		case statusRe.MatchString(r.RequestURI):
			code, _ := strconv.Atoi(statusRe.FindStringSubmatch(r.RequestURI)[1])
			w.WriteHeader(code)
			return
		}
		if d := r.FormValue("delay"); d != "" {
			if t, err := time.ParseDuration(d); err == nil {
				time.Sleep(t)
			}
		}
		body, err := json.MarshalIndent(requestAssertions{
			Path:      r.RequestURI,
			Host:      r.Host,
			Method:    r.Method,
			Proto:     r.Proto,
			Headers:   r.Header,
			Namespace: e.namespace,
			Service:   e.app,
			// Tests assert that the pod name starts with the backend name,
			// and the weight-distribution test recovers the backend name by
			// stripping the last two dash-segments (the ReplicaSet and Pod
			// hashes of a Deployment-managed pod) - so the fake name must
			// carry exactly two suffix segments.
			Pod: e.app + "-fake0000-fake0",
		}, "", " ")
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// X-Echo-Set-Header lets tests instruct the backend to set response
		// headers (used by the response-header-modifier tests).
		for _, kvList := range r.Header["X-Echo-Set-Header"] {
			for _, kv := range strings.Split(kvList, ",") {
				name, value, _ := strings.Cut(strings.TrimSpace(kv), ":")
				if len(w.Header()[name]) == 0 {
					w.Header()[name] = []string{value}
				} else {
					w.Header()[name][0] += "," + strings.TrimSpace(value)
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		_, _ = w.Write(body)
	})
}
