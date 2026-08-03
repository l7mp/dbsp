package testsuite

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	dbspjs "github.com/l7mp/dbsp/js"
)

// Suite wraps an envtest environment and provides helpers for running JS
// integration tests against it.
type Suite struct {
	Cfg    *rest.Config
	Ctx    context.Context
	Cancel context.CancelFunc

	scheme      *runtime.Scheme
	k8sClient   client.Client
	testEnv     *envtest.Environment
	kubecfgPath string // lazily written on first call to KubeconfigPath
	stdlibPath  string
}

// NewSuite starts an envtest Kubernetes cluster and installs the CRDs found
// in crdPaths. The caller must call Close when done.
func NewSuite(crdPaths ...string) (*Suite, error) {
	s := &Suite{
		scheme: runtime.NewScheme(),
	}
	s.Ctx, s.Cancel = context.WithCancel(context.Background())

	if err := clientgoscheme.AddToScheme(s.scheme); err != nil {
		return nil, fmt.Errorf("register scheme: %w", err)
	}

	assetsDir := discoverBinaryAssetsDirectory()
	s.testEnv = &envtest.Environment{
		CRDDirectoryPaths:        crdPaths,
		ErrorIfCRDPathMissing:    true,
		AttachControlPlaneOutput: false,
		BinaryAssetsDirectory:    assetsDir,
	}

	var err error
	s.Cfg, err = s.testEnv.Start()
	if err != nil {
		return nil, fmt.Errorf("start envtest: %w", err)
	}

	s.k8sClient, err = client.New(s.Cfg, client.Options{Scheme: s.scheme})
	if err != nil {
		_ = s.testEnv.Stop()
		return nil, fmt.Errorf("create client: %w", err)
	}

	// Create standard namespaces; ignore AlreadyExists since envtest may
	// pre-create "default".
	for _, ns := range []string{"default", "testnamespace", "other"} {
		obj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}
		if err := s.k8sClient.Create(s.Ctx, obj); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				_ = s.testEnv.Stop()
				return nil, fmt.Errorf("create namespace %q: %w", ns, err)
			}
		}
	}

	// Locate the JS stdlib by walking up to the workspace root (contains go.work).
	wd, err := os.Getwd()
	if err != nil {
		_ = s.testEnv.Stop()
		return nil, err
	}
	root, err := findWorkspaceRoot(wd)
	if err != nil {
		_ = s.testEnv.Stop()
		return nil, fmt.Errorf("locate workspace root: %w", err)
	}
	s.stdlibPath, err = filepath.Abs(filepath.Join(root, "js", "stdlib"))
	if err != nil {
		_ = s.testEnv.Stop()
		return nil, err
	}

	return s, nil
}

// KubeconfigPath writes the envtest rest.Config to a kubeconfig file the first
// time it is called and returns the path on subsequent calls.
func (s *Suite) KubeconfigPath() (string, error) {
	if s.kubecfgPath != "" {
		return s.kubecfgPath, nil
	}

	dir, err := os.MkdirTemp("", "dcontroller-testenv-*")
	if err != nil {
		return "", err
	}

	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["envtest"] = &clientcmdapi.Cluster{
		Server:                   s.Cfg.Host,
		InsecureSkipTLSVerify:    s.Cfg.Insecure,
		CertificateAuthorityData: s.Cfg.CAData,
	}
	cfg.AuthInfos["envtest"] = &clientcmdapi.AuthInfo{
		Token:                 s.Cfg.BearerToken,
		ClientCertificateData: s.Cfg.CertData,
		ClientKeyData:         s.Cfg.KeyData,
	}
	cfg.Contexts["envtest"] = &clientcmdapi.Context{Cluster: "envtest", AuthInfo: "envtest"}
	cfg.CurrentContext = "envtest"

	path := filepath.Join(dir, "kubeconfig")
	if err := clientcmd.WriteToFile(*cfg, path); err != nil {
		return "", err
	}
	s.kubecfgPath = path
	return path, nil
}

// RunJS creates a fresh JS VM configured to point at the envtest cluster,
// runs scriptPath, and fails t if the script exits non-zero.
func (s *Suite) RunJS(t *testing.T, scriptPath string) {
	t.Helper()

	kubecfgPath, err := s.KubeconfigPath()
	if err != nil {
		t.Fatalf("suite kubeconfig: %v", err)
	}

	absScript, err := filepath.Abs(scriptPath)
	if err != nil {
		t.Fatalf("resolve script path: %v", err)
	}

	t.Setenv("DCONTROLLER_KUBECONFIG", kubecfgPath)
	t.Setenv("DCONTROLLER_API_SERVER_ENABLED", "false")
	t.Setenv("DCONTROLLER_RUNTIME_CONFIG", "")

	vm, err := dbspjs.NewVMWithOptions(dbspjs.Options{
		Logger:      logr.Discard(),
		StdlibPaths: []string{s.stdlibPath},
	})
	if err != nil {
		t.Fatalf("create VM: %v", err)
	}
	t.Cleanup(vm.Close)

	if err := vm.RunFile(absScript); err != nil {
		t.Fatalf("script %s: %v", filepath.Base(scriptPath), err)
	}
}

// RunJSWithAPIServer is like RunJS but enables the embedded Kubernetes API
// server on the given port (HTTP, insecure, development mode).  Use it for
// tests that rely on view GVKs (PodView, HealthView, EndpointView, …).
func (s *Suite) RunJSWithAPIServer(t *testing.T, scriptPath string, port int) {
	t.Helper()

	kubecfgPath, err := s.KubeconfigPath()
	if err != nil {
		t.Fatalf("suite kubeconfig: %v", err)
	}

	absScript, err := filepath.Abs(scriptPath)
	if err != nil {
		t.Fatalf("resolve script path: %v", err)
	}

	t.Setenv("DCONTROLLER_KUBECONFIG", kubecfgPath)
	t.Setenv("DCONTROLLER_API_SERVER_ENABLED", "true")
	t.Setenv("DCONTROLLER_API_SERVER_PORT", fmt.Sprint(port))
	t.Setenv("DCONTROLLER_API_SERVER_HTTP", "true")
	t.Setenv("DCONTROLLER_API_SERVER_INSECURE", "true")
	t.Setenv("DCONTROLLER_RUNTIME_CONFIG", "")

	vm, err := dbspjs.NewVMWithOptions(dbspjs.Options{
		Logger:      logr.Discard(),
		StdlibPaths: []string{s.stdlibPath},
	})
	if err != nil {
		t.Fatalf("create VM: %v", err)
	}
	t.Cleanup(vm.Close)

	if err := vm.RunFile(absScript); err != nil {
		t.Fatalf("script %s: %v", filepath.Base(scriptPath), err)
	}
}

// FreePort returns an ephemeral TCP port that is free at the time of the call.
// There is a small race window between the return and the caller binding to the
// port; this is acceptable for tests.
func FreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// Close shuts down the envtest cluster.
func (s *Suite) Close() {
	if s == nil {
		return
	}
	if s.Cancel != nil {
		s.Cancel()
	}
	if s.testEnv != nil {
		_ = s.testEnv.Stop()
	}
}

// findWorkspaceRoot walks up the directory tree until it finds a go.work file.
func findWorkspaceRoot(start string) (string, error) {
	for dir := start; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}
	return "", fmt.Errorf("go.work not found starting from %s", start)
}

func discoverBinaryAssetsDirectory() string {
	if assetsDir := os.Getenv("KUBEBUILDER_ASSETS"); assetsDir != "" {
		return assetsDir
	}

	wd, err := os.Getwd()
	if err != nil {
		return ""
	}

	for dir := wd; ; dir = filepath.Dir(dir) {
		root := filepath.Join(dir, "bin", "k8s")
		entries, err := os.ReadDir(root)
		if err == nil {
			names := make([]string, 0, len(entries))
			for _, entry := range entries {
				if entry.IsDir() {
					names = append(names, entry.Name())
				}
			}
			sort.Sort(sort.Reverse(sort.StringSlice(names)))
			for _, name := range names {
				candidate := filepath.Join(root, name)
				if hasEnvtestBinaries(candidate) {
					return candidate
				}
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
	}
	return ""
}

func hasEnvtestBinaries(dir string) bool {
	for _, binary := range []string{"etcd", "kube-apiserver", "kubectl"} {
		if _, err := os.Stat(filepath.Join(dir, binary)); err != nil {
			return false
		}
	}
	return true
}
