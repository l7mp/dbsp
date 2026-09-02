// The shared Kubernetes plant of the macro benchmarks. It boots an envtest
// control plane (kube-apiserver + etcd) with the CRDs from -crd-dir
// installed and API request-audit logging enabled (the symmetric API-load
// instrument: one JSON line per request, attributable to each contender by
// user agent), optionally spawns the stock gatekeeper binary against it
// (the E6 baseline), publishes a handshake file for the dbsp driver, and
// idles until a stop file appears.
//
// Every contender of a benchmark runs against this same launcher - E6
// points -crd-dir at the Gatekeeper CRDs (dpolicy mode skips the
// gatekeeper spawn with -gk=false), E5 points it at the Gateway API +
// Envoy Gateway CRD bundle with -gk=false and spawns its own systems under
// test - so the plant is identical across contenders.
//
// Run (stays up until $PLANT_DIR/stop appears):
//
//	PLANT_DIR=/tmp/plant GOWORK=off go run . \
//	    -gk-bin ../.cache/gatekeeper/bin/gatekeeper \
//	    -crd-dir ../.cache/gatekeeper/charts/gatekeeper/crds \
//	    [-gk=false] [-audit-interval 60] [-audit-from-cache]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const (
	gkNamespace = "gatekeeper-system"
	gkPodName   = "gatekeeper-audit-bench"
)

// requestAuditPolicy is the kube-apiserver audit policy: one Metadata-level
// line per request (user agent, verb, resource), no request/response bodies.
const requestAuditPolicy = `apiVersion: audit.k8s.io/v1
kind: Policy
omitStages:
  - RequestReceived
rules:
  - level: Metadata
`

func main() {
	var (
		gkEnabled     = flag.Bool("gk", true, "spawn the gatekeeper binary (false: plant only, for dpolicy modes)")
		gkBin         = flag.String("gk-bin", envOr("GK_BIN", ""), "path to the gatekeeper binary")
		crdDir        = flag.String("crd-dir", "", "directory with the benchmark's CRD manifests")
		extraCRDDir   = flag.String("extra-crd-dir", "", "additional CRD manifests (vendored constraint-kind CRDs for E6, the istio bundle for E5's istio points)")
		auditInterval = flag.Int("audit-interval", 60, "gatekeeper --audit-interval (seconds)")
		fromCache     = flag.Bool("audit-from-cache", false, "gatekeeper --audit-from-cache (reads the audited inventory from the replicated cache)")
		violationsCap = flag.Int("constraint-violations-limit", 20, "gatekeeper --constraint-violations-limit")
		minReqTimeout = flag.Int("min-request-timeout", 0, "apiserver --min-request-timeout (seconds; 0 = default). Small values expire watches fast - the watch-reconnect test knob")
		createNs      = flag.String("create-ns", "", "comma-separated namespaces to pre-create (contenders like istiod expect their root namespace to exist)")
	)
	flag.Parse()

	serveDir := envOr("PLANT_DIR", "/tmp/plant")
	if err := run(serveDir, *gkEnabled, *gkBin, *crdDir, *extraCRDDir, *createNs, *auditInterval, *fromCache, *violationsCap, *minReqTimeout); err != nil {
		log.Fatal(err)
	}
}

func run(serveDir string, gkEnabled bool, gkBin, crdDir, extraCRDDir, createNs string, auditInterval int, fromCache bool, violationsCap, minReqTimeout int) error {
	if err := os.MkdirAll(serveDir, 0o755); err != nil {
		return err
	}
	_ = os.Remove(filepath.Join(serveDir, "stop"))
	_ = os.Remove(filepath.Join(serveDir, "handshake.json"))

	if gkEnabled && gkBin == "" {
		return fmt.Errorf("-gk-bin is required unless -gk=false")
	}
	if crdDir == "" {
		return fmt.Errorf("-crd-dir is required (the Gatekeeper CRD manifests)")
	}

	policyPath := filepath.Join(serveDir, "request-audit-policy.yaml")
	if err := os.WriteFile(policyPath, []byte(requestAuditPolicy), 0o644); err != nil {
		return err
	}
	requestLog := filepath.Join(serveDir, "apiserver-requests.log")

	sch := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(sch))

	crdDirs := []string{crdDir}
	if extraCRDDir != "" {
		crdDirs = append(crdDirs, extraCRDDir)
	}
	env := &envtest.Environment{
		CRDDirectoryPaths:     crdDirs,
		ErrorIfCRDPathMissing: true,
		Scheme:                sch,
	}
	apiServerArgs := env.ControlPlane.GetAPIServer().Configure().
		Append("audit-policy-file", policyPath).
		Append("audit-log-path", requestLog).
		Append("audit-log-format", "json").
		// Room for one ClusterIP per Service at macro scale (the envtest
		// default /24 caps out at ~250 Services).
		Append("service-cluster-ip-range", "10.96.0.0/16")
	if minReqTimeout > 0 {
		apiServerArgs.Append("min-request-timeout", fmt.Sprintf("%d", minReqTimeout))
	}

	restCfg, err := env.Start()
	if err != nil {
		return fmt.Errorf("envtest: %w", err)
	}
	defer func() { _ = env.Stop() }()

	adminUser, err := env.ControlPlane.AddUser(envtest.User{Name: "admin", Groups: []string{"system:masters"}}, nil)
	if err != nil {
		return fmt.Errorf("add user: %w", err)
	}
	kubeconfigBytes, err := adminUser.KubeConfig()
	if err != nil {
		return fmt.Errorf("kubeconfig: %w", err)
	}
	kubeconfig := filepath.Join(serveDir, "kubeconfig")
	if err := os.WriteFile(kubeconfig, kubeconfigBytes, 0o644); err != nil {
		return err
	}

	ctx := context.Background()
	c, err := client.New(restCfg, client.Options{Scheme: sch})
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}

	if createNs != "" {
		for _, name := range strings.Split(createNs, ",") {
			ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: strings.TrimSpace(name)}}
			if err := c.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create namespace %s: %w", name, err)
			}
		}
	}

	var gk *exec.Cmd
	gkPid := 0
	gkLog := ""
	if gkEnabled {
		// Gatekeeper expects its own namespace, and its status controllers
		// look up their own Pod object (byPod ownership), so the harness
		// fakes one.
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: gkNamespace}}
		if err := c.Create(ctx, ns); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      gkPodName,
				Namespace: gkNamespace,
				Labels:    map[string]string{"app": "gatekeeper", "gatekeeper.sh/operation": "audit"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "manager", Image: "gatekeeper:bench"}},
			},
		}
		if err := c.Create(ctx, pod); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
		// The audit manager pages LISTed resources through a disk cache
		// (an emptyDir in the real pod).
		apiCacheDir := filepath.Join(serveDir, "audit-cache")
		if err := os.MkdirAll(apiCacheDir, 0o755); err != nil {
			return err
		}
		gkLog = filepath.Join(serveDir, "gatekeeper.log")
		logFile, err := os.Create(gkLog)
		if err != nil {
			return err
		}
		defer logFile.Close()

		healthPort, err := freePort()
		if err != nil {
			return err
		}
		metricsPort, err := freePort()
		if err != nil {
			return err
		}
		promPort, err := freePort()
		if err != nil {
			return err
		}
		args := []string{
			"--operation=audit",
			"--operation=status",
			// The generate operation owns ConstraintTemplate -> CRD
			// generation (upstream it runs in the controller-manager pod;
			// audit alone would wait forever for the constraint CRDs).
			"--operation=generate",
			"--disable-cert-rotation",
			// The external-data feature (on by default) wants a client cert
			// watcher on /certs even in audit-only mode.
			"--enable-external-data=false",
			// Harness-assigned ports: the defaults (:9090, :8888) collide
			// with whatever else runs on a dev box.
			fmt.Sprintf("--health-addr=:%d", healthPort),
			fmt.Sprintf("--metrics-addr=:%d", metricsPort),
			fmt.Sprintf("--prometheus-port=%d", promPort),
			fmt.Sprintf("--api-cache-dir=%s", apiCacheDir),
			fmt.Sprintf("--audit-interval=%d", auditInterval),
			fmt.Sprintf("--constraint-violations-limit=%d", violationsCap),
		}
		if fromCache {
			args = append(args, "--audit-from-cache=true")
		}
		gk = exec.Command(gkBin, args...)
		gk.Env = append(os.Environ(),
			"KUBECONFIG="+kubeconfig,
			"POD_NAMESPACE="+gkNamespace,
			"POD_NAME="+gkPodName,
			"NAMESPACE="+gkNamespace,
			"CONTAINER_NAME=manager",
		)
		gk.Stdout = logFile
		gk.Stderr = logFile
		if err := gk.Start(); err != nil {
			return fmt.Errorf("start gatekeeper: %w", err)
		}
		gkPid = gk.Process.Pid
		defer func() { _ = gk.Process.Kill() }()
		log.Printf("gatekeeper started: pid %d, log %s", gkPid, gkLog)
	}

	hs := map[string]any{
		"kubeconfig": kubeconfig,
		"requestLog": requestLog,
		"gkPid":      gkPid,
		"gkLog":      gkLog,
		"harnessPid": os.Getpid(),
	}
	b, _ := json.MarshalIndent(hs, "", "  ")
	if err := os.WriteFile(filepath.Join(serveDir, "handshake.json"), b, 0o644); err != nil {
		return err
	}
	log.Printf("serving; handshake at %s", filepath.Join(serveDir, "handshake.json"))

	stop := filepath.Join(serveDir, "stop")
	for {
		if _, err := os.Stat(stop); err == nil {
			log.Print("stop file found, shutting down")
			return nil
		}
		if gk != nil && gk.ProcessState != nil {
			return fmt.Errorf("gatekeeper exited early, see %s", gkLog)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// freePort asks the kernel for an unused TCP port.
func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
