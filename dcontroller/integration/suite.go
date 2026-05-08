package integration

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/go-logr/logr"
	"go.uber.org/zap/zapcore"
	appsv1 "k8s.io/api/apps/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	opv1a1 "github.com/l7mp/dbsp/dcontroller/api/operator/v1alpha1"
)

type Suite struct {
	Timeout, Interval time.Duration
	LogLevel          int8
	Cfg               *rest.Config
	Scheme            *runtime.Scheme
	K8sClient         client.Client
	TestEnv           *envtest.Environment
	Ctx               context.Context
	Cancel            context.CancelFunc
	Log               logr.Logger
}

func NewSuite(loglevel int8, crdPaths ...string) (*Suite, error) {
	s := &Suite{
		Timeout:  time.Second * 5,
		Interval: time.Millisecond * 250,
		LogLevel: loglevel,
		Scheme:   runtime.NewScheme(),
	}

	opts := zap.Options{
		Development:     true,
		DestWriter:      GinkgoWriter,
		StacktraceLevel: zapcore.Level(4),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
		Level:           zapcore.Level(loglevel),
	}
	log := zap.New(zap.UseFlagOptions(&opts))
	s.Log = log
	ctrl.SetLogger(log)

	s.Ctx, s.Cancel = context.WithCancel(context.Background())

	if err := clientgoscheme.AddToScheme(s.Scheme); err != nil {
		return nil, err
	}
	if err := appsv1.AddToScheme(s.Scheme); err != nil {
		return nil, err
	}
	if err := discoveryv1.AddToScheme(s.Scheme); err != nil {
		return nil, err
	}
	if err := opv1a1.AddToScheme(s.Scheme); err != nil {
		return nil, err
	}

	By("bootstrapping test environment")
	assetsDir := discoverBinaryAssetsDirectory()
	s.TestEnv = &envtest.Environment{
		CRDDirectoryPaths:        crdPaths,
		ErrorIfCRDPathMissing:    true,
		AttachControlPlaneOutput: true,
		BinaryAssetsDirectory:    assetsDir,
	}

	// cfg is defined in this file globally.
	var err error
	s.Cfg, err = s.TestEnv.Start()
	if err != nil {
		return nil, err
	}

	// a spearate client whose client.Reader does not go through the caches
	s.K8sClient, err = client.New(s.Cfg, client.Options{Scheme: s.Scheme})
	if err != nil {
		return nil, err
	}

	// create default test namespace
	if err := s.K8sClient.Create(s.Ctx, TestNs.DeepCopy()); err != nil {
		return nil, err
	}

	// create another testing namespace
	if err := s.K8sClient.Create(s.Ctx, TestNs2.DeepCopy()); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Suite) Close() {
	if s == nil {
		return
	}

	if s.K8sClient != nil {
		if err := s.K8sClient.Delete(s.Ctx, TestNs); err != nil {
			s.Log.Error(err, "removing test namespace")
		}
		if err := s.K8sClient.Delete(s.Ctx, TestNs2); err != nil {
			s.Log.Error(err, "removing test namespace")
		}
	}

	if s.Cancel != nil {
		s.Cancel()
	}

	if s.TestEnv != nil {
		if err := s.TestEnv.Stop(); err != nil {
			Expect(err).NotTo(HaveOccurred(), "tearing down the test environment")
		}
	}
}

func TimestampEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.Format(time.RFC3339Nano))
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
