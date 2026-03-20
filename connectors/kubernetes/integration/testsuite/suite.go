package testsuite

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

const DefaultNamespace = "kubernetes-connector-test"

type Suite struct {
	Timeout, Interval time.Duration
	Cfg               *rest.Config
	Scheme            *runtime.Scheme
	K8sClient         client.Client
	WatchClient       client.WithWatch
	TestEnv           *envtest.Environment
	Ctx               context.Context
	Cancel            context.CancelFunc
	Namespace         string
}

func New() (*Suite, error) {
	s := &Suite{
		Timeout:   10 * time.Second,
		Interval:  200 * time.Millisecond,
		Scheme:    runtime.NewScheme(),
		Namespace: DefaultNamespace,
	}

	s.Ctx, s.Cancel = context.WithCancel(context.Background())

	if err := clientgoscheme.AddToScheme(s.Scheme); err != nil {
		return nil, fmt.Errorf("add kubernetes scheme: %w", err)
	}

	s.TestEnv = &envtest.Environment{}

	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		if assetsDir, ok := resolveEnvtestAssetsDir(); ok {
			_ = os.Setenv("KUBEBUILDER_ASSETS", assetsDir)
		}
	}

	var err error
	s.Cfg, err = s.TestEnv.Start()
	if err != nil {
		return nil, fmt.Errorf("start envtest: %w (set KUBEBUILDER_ASSETS to a valid envtest assets dir)", err)
	}

	s.K8sClient, err = client.New(s.Cfg, client.Options{Scheme: s.Scheme})
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}

	s.WatchClient, err = client.NewWithWatch(s.Cfg, client.Options{Scheme: s.Scheme})
	if err != nil {
		return nil, fmt.Errorf("create watch client: %w", err)
	}

	ns := &corev1.Namespace{}
	ns.SetName(s.Namespace)
	if err := s.K8sClient.Create(s.Ctx, ns); err != nil {
		return nil, fmt.Errorf("create test namespace: %w", err)
	}

	return s, nil
}

func (s *Suite) Close() {
	ns := &corev1.Namespace{}
	ns.SetName(s.Namespace)
	_ = s.K8sClient.Delete(s.Ctx, ns)

	s.Cancel()

	if s.TestEnv != nil {
		_ = s.TestEnv.Stop()
	}
}

func resolveEnvtestAssetsDir() (string, bool) {
	candidates := []string{}

	if assetsDir, err := envtest.SetupEnvtestDefaultBinaryAssetsDirectory(); err == nil {
		candidates = append(candidates, assetsDir)
	}

	if wd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Clean(filepath.Join(wd, "..", "..", "..", "..", "bin", "k8s", "1.30.0-linux-amd64")))
	}

	for _, dir := range candidates {
		if hasEnvtestBinaries(dir) {
			return dir, true
		}
	}

	return "", false
}

func hasEnvtestBinaries(dir string) bool {
	for _, name := range []string{"etcd", "kube-apiserver", "kubectl"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			return false
		}
	}
	return true
}
