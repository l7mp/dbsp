//go:build benchmark

// Package delta hosts stock Envoy Gateway as a pure controller process for
// the E5 macro benchmark: the runners are booted unmodified against an
// externally provisioned Kubernetes plant, and the xDS server is exposed to
// the benchmark driver's delta-ADS measurement clients.
package delta

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"
	gwapiv1a2 "sigs.k8s.io/gateway-api/apis/v1alpha2"

	discoveryv1 "k8s.io/api/discovery/v1"

	egv1a1 "github.com/envoyproxy/gateway/api/v1alpha1"
	"github.com/envoyproxy/gateway/internal/crypto"
	"github.com/envoyproxy/gateway/internal/envoygateway/config"
	gatewayapirunner "github.com/envoyproxy/gateway/internal/gatewayapi/runner"
	"github.com/envoyproxy/gateway/internal/message"
	providerkube "github.com/envoyproxy/gateway/internal/provider/kubernetes"
	providerrunner "github.com/envoyproxy/gateway/internal/provider/runner"
	xdsrunner "github.com/envoyproxy/gateway/internal/xds/runner"
)

const (
	ns             = "bench"
	controllerName = "gateway.envoyproxy.io/gatewayclass-controller"
)

// TestServe runs in-process Envoy Gateway as a pure controller process and
// idles, serving as the system-under-test for an external driver (the dbsp
// benchmark, which writes Gateway API objects through the kubeconfig and
// connects delta-ADS clients to the xDS server with the published client
// certs). A handshake file tells the driver where everything is; a stop
// file shuts us down.
//
// The Kubernetes plant is externally provisioned (the shared envtest plant
// of the E5 macro benchmark, with the Gateway API + Envoy Gateway CRDs
// installed): this process only connects to it.
//
// Run (stays up until $EG_SERVE_DIR/stop appears or the timeout hits):
//
//	EG_SERVE_DIR=/tmp/eg-serve EG_KUBECONFIG=/tmp/plant/kubeconfig \
//	    go test -v -timeout 4h -tags benchmark ./benchmarks/delta -run TestServe
func TestServe(t *testing.T) {
	serveDir := os.Getenv("EG_SERVE_DIR")
	if serveDir == "" {
		t.Skip("EG_SERVE_DIR not set")
	}
	kubeconfig := os.Getenv("EG_KUBECONFIG")
	if kubeconfig == "" {
		t.Fatal("EG_KUBECONFIG not set (the externally provisioned plant's kubeconfig)")
	}
	if err := os.MkdirAll(serveDir, 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.Remove(filepath.Join(serveDir, "stop"))
	_ = os.Remove(filepath.Join(serveDir, "handshake.json"))

	sch := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(sch))
	utilruntime.Must(gwapiv1.Install(sch))
	utilruntime.Must(gwapiv1a2.Install(sch))
	utilruntime.Must(discoveryv1.AddToScheme(sch))
	utilruntime.Must(egv1a1.AddToScheme(sch))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	restCfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatalf("EG_KUBECONFIG %s: %v", kubeconfig, err)
	}
	t.Setenv("KUBECONFIG", kubeconfig)

	c, err := client.New(restCfg, client.Options{Scheme: sch})
	if err != nil {
		t.Fatalf("client: %v", err)
	}

	certDir := startEG(t, ctx, c)

	// The world container: the driver creates everything else.
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	if err := c.Create(ctx, gatewayClass()); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}

	hs := map[string]string{
		"kubeconfig":     kubeconfig,
		"xdsAddress":     "127.0.0.1:18000",
		"clientCert":     filepath.Join(certDir, "envoy-tls.crt"),
		"clientKey":      filepath.Join(certDir, "envoy-tls.key"),
		"ca":             filepath.Join(certDir, "ca.crt"),
		"serverName":     "envoy-gateway",
		"namespace":      ns,
		"gatewayClass":   "eg",
		"controllerName": controllerName,
	}
	b, _ := json.MarshalIndent(hs, "", "  ")
	if err := os.WriteFile(filepath.Join(serveDir, "handshake.json"), b, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("EG serving; handshake at %s", filepath.Join(serveDir, "handshake.json"))

	stop := filepath.Join(serveDir, "stop")
	for {
		if _, err := os.Stat(stop); err == nil {
			t.Log("stop file found, shutting down")
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// --- Envoy Gateway in-process boot. ---

type runner interface {
	Name() string
	Start(ctx context.Context) error
}

// startEG boots the provider, gateway-api translator and xDS server
// runners - the controller path, exactly as the stock binary wires them.
// The infra manager is not started (the benchmark plant has no kubelet to
// run provisioned proxies), and the xDS TLS material is generated locally
// (the stock binary reads it from the hardcoded /certs mount).
func startEG(t *testing.T, ctx context.Context, c client.Client) string {
	cfg, err := config.New(os.Stdout, os.Stderr)
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	cfg.EnvoyGateway.Provider.Kubernetes = &egv1a1.EnvoyGatewayKubernetesProvider{
		LeaderElection: &egv1a1.LeaderElection{Disable: ptr.To(true)},
		// The topology injector webhook serves TLS from the hardcoded
		// /certs directory; the benchmark has no pods to inject anyway.
		TopologyInjector: &egv1a1.EnvoyGatewayTopologyInjector{Disable: ptr.To(true)},
	}
	// Refill the defaults around the fields set above (client rate limits,
	// namespaces, ...): the runners dereference them unconditionally.
	cfg.EnvoyGateway.SetEnvoyGatewayDefaults()
	cfg.Elected = make(chan struct{})
	cfg.ProviderReady = make(chan struct{})

	// The xDS server needs its TLS material on disk.
	certs, err := crypto.GenerateCerts(cfg)
	if err != nil {
		t.Fatalf("generate certs: %v", err)
	}
	certDir := t.TempDir()
	for name, data := range map[string][]byte{
		"tls.crt": certs.EnvoyGatewayCertificate,
		"tls.key": certs.EnvoyGatewayPrivateKey,
		"ca.crt":  certs.CACertificate,
		// The Envoy-side client material, for external delta-ADS clients
		// (the dbsp driver) connecting to the xDS server.
		"envoy-tls.crt": certs.EnvoyCertificate,
		"envoy-tls.key": certs.EnvoyPrivateKey,
	} {
		if err := os.WriteFile(filepath.Join(certDir, name), data, 0o600); err != nil {
			t.Fatalf("write cert %s: %v", name, err)
		}
	}

	// Replicate certgen: the controller namespace and the full secret set
	// (envoy-gateway, envoy, HMAC, ...) that translation and the runners
	// expect to find in-cluster.
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: cfg.ControllerNamespace}}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	if _, err := providerkube.CreateOrUpdateSecrets(ctx, c, providerkube.CertsToSecret(cfg.ControllerNamespace, certs), true); err != nil {
		t.Fatal(err)
	}

	runnerErrors := &message.RunnerErrors{}
	pResources := new(message.ProviderResources)
	xdsIR := new(message.XdsIR)
	infraIR := new(message.InfraIR)
	_ = infraIR // The infra manager runner is deliberately not started.

	runners := []runner{
		providerrunner.New(&providerrunner.Config{
			Server:            *cfg,
			ProviderResources: pResources,
			RunnerErrors:      runnerErrors,
		}),
		gatewayapirunner.New(&gatewayapirunner.Config{
			Server:            *cfg,
			ProviderResources: pResources,
			RunnerErrors:      runnerErrors,
			XdsIR:             xdsIR,
			InfraIR:           infraIR,
		}),
		xdsrunner.New(&xdsrunner.Config{
			Server:            *cfg,
			XdsIR:             xdsIR,
			ProviderResources: pResources,
			RunnerErrors:      runnerErrors,
			TLSCertPath:       filepath.Join(certDir, "tls.crt"),
			TLSKeyPath:        filepath.Join(certDir, "tls.key"),
			TLSCaPath:         filepath.Join(certDir, "ca.crt"),
		}),
	}
	for _, r := range runners {
		if err := r.Start(ctx); err != nil {
			t.Fatalf("start runner %s: %v", r.Name(), err)
		}
	}
	return certDir
}

// --- Fixtures. ---

func gatewayClass() *gwapiv1.GatewayClass {
	return &gwapiv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{Name: "eg"},
		Spec:       gwapiv1.GatewayClassSpec{ControllerName: controllerName},
	}
}
