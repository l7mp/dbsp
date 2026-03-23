// Package runtime provides a self-contained Kubernetes runtime that bundles the composite
// store API (client, cache, discovery, RESTMapper), an optional embedded API server, and an
// optional JWT authentication framework.
package runtime

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/apiserver"
	kauth "github.com/l7mp/dbsp/connectors/kubernetes/runtime/auth"
	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/store"
)

// APIServerConfig is a re-export of apiserver.Config. Callers do not need to import the
// apiserver package directly. Use NewDefaultAPIServerConfig to construct a value with sensible
// defaults; the runtime will inject DelegatingClient and DiscoveryClient automatically.
type APIServerConfig = apiserver.Config

// AuthConfig configures JWT authentication for the embedded API server. If PrivateKeyFile and
// PublicKeyFile are both empty, an RSA key pair is auto-generated at runtime.
type AuthConfig struct {
	PrivateKeyFile string
	PublicKeyFile  string
}

// Config is the top-level configuration for the Kubernetes runtime.
type Config struct {
	// RESTConfig is the Kubernetes REST config. If nil, only view objects are supported and
	// native Kubernetes resources are unavailable.
	RESTConfig *rest.Config

	// CacheOptions configures the composite cache.
	CacheOptions store.CacheOptions

	// ClientOptions configures the composite client and discovery layer.
	ClientOptions store.ClientOptions

	// APIServer configures the optional embedded API server. Set to nil to disable it.
	APIServer *APIServerConfig

	// Auth configures JWT-based authentication for the embedded API server. Set to nil to
	// disable auth (all requests are allowed). Has no effect when APIServer is nil.
	Auth *AuthConfig

	// Logger is the structured logger used by all components.
	Logger logr.Logger
}

// NewDefaultAPIServerConfig creates an APIServerConfig with sensible defaults. addr and port
// define the listen address; httpMode disables TLS (useful for testing); insecure skips TLS
// certificate verification. The runtime will inject DelegatingClient and DiscoveryClient.
func NewDefaultAPIServerConfig(addr string, port int, httpMode, insecure bool, log logr.Logger) (APIServerConfig, error) {
	return apiserver.NewDefaultConfig(addr, port, nil, httpMode, insecure, log)
}

// Runtime is the self-contained Kubernetes runtime.
type Runtime struct {
	cfg Config
	log logr.Logger

	client     *store.CompositeClient
	cache      *store.CompositeCache
	discovery  *store.CompositeDiscoveryClient
	restMapper meta.RESTMapper

	apiServer *apiserver.APIServer

	// Auth state, populated when cfg.Auth != nil.
	privateKey *rsa.PrivateKey
}

// New creates a new Kubernetes runtime and initialises all configured components.
// Start must be called to actually run the cache and any optional API server.
func New(cfg Config) (*Runtime, error) {
	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}

	r := &Runtime{cfg: cfg, log: log}

	// 1. Composite discovery.
	var nativeDiscovery discovery.DiscoveryInterface
	if cfg.RESTConfig != nil {
		nd, err := discovery.NewDiscoveryClientForConfig(cfg.RESTConfig)
		if err != nil {
			return nil, fmt.Errorf("runtime: discovery client: %w", err)
		}
		nativeDiscovery = nd
	}
	r.discovery = store.NewCompositeDiscoveryClient(nativeDiscovery)

	// 2. Composite RESTMapper.
	r.restMapper = store.NewCompositeRESTMapper(r.discovery)

	// 3. Composite cache.
	cache, err := store.NewCompositeCache(cfg.RESTConfig, store.CacheOptions{
		Options:      cfg.CacheOptions.Options,
		DefaultCache: cfg.CacheOptions.DefaultCache,
		Logger:       log,
	})
	if err != nil {
		return nil, fmt.Errorf("runtime: cache: %w", err)
	}
	r.cache = cache

	// 4. Composite client.
	client, err := r.cache.NewClient(cfg.RESTConfig, cfg.ClientOptions)
	if err != nil {
		return nil, fmt.Errorf("runtime: client: %w", err)
	}
	r.client = client

	if cfg.RESTConfig == nil {
		log.Info("native Kubernetes resources unavailable: no REST config provided")
	}

	// 5. Optional embedded API server.
	if cfg.APIServer != nil {
		if err := r.buildAPIServer(); err != nil {
			return nil, fmt.Errorf("runtime: apiserver: %w", err)
		}
	}

	return r, nil
}

// buildAPIServer constructs the embedded API server and optional auth components.
func (r *Runtime) buildAPIServer() error {
	// Copy to avoid mutating the caller's config; inject internal components.
	apicfg := *r.cfg.APIServer
	apicfg.DelegatingClient = r.cache.GetViewCache().GetClient()
	apicfg.DiscoveryClient = r.discovery

	// Optional auth.
	if r.cfg.Auth != nil {
		privateKey, publicKey, err := r.loadOrGenerateKeys(r.cfg.Auth)
		if err != nil {
			return fmt.Errorf("auth keys: %w", err)
		}
		r.privateKey = privateKey

		apicfg.Authenticator = kauth.NewJWTAuthenticator(publicKey)
		apicfg.Authorizer = kauth.NewCompositeAuthorizer()
	}

	srv, err := apiserver.NewAPIServer(apicfg)
	if err != nil {
		return err
	}
	r.apiServer = srv
	return nil
}

// loadOrGenerateKeys loads RSA keys from files or generates a new key pair.
func (r *Runtime) loadOrGenerateKeys(auth *AuthConfig) (*rsa.PrivateKey, *rsa.PublicKey, error) {
	if auth.PrivateKeyFile != "" && auth.PublicKeyFile != "" {
		priv, err := kauth.LoadPrivateKey(auth.PrivateKeyFile)
		if err != nil {
			return nil, nil, fmt.Errorf("load private key: %w", err)
		}
		pub, err := kauth.LoadPublicKey(auth.PublicKeyFile)
		if err != nil {
			return nil, nil, fmt.Errorf("load public key: %w", err)
		}
		return priv, pub, nil
	}

	// Auto-generate a 2048-bit RSA key pair.
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("generate RSA key pair: %w", err)
	}
	return priv, &priv.PublicKey, nil
}

// Start initialises all components in the correct order and blocks until ctx is cancelled.
//
// Startup order:
//  1. Composite cache goroutine is launched.
//  2. Cache sync is awaited.
//  3. Optional embedded API server goroutine is launched.
//  4. Blocks until ctx is cancelled or any component returns an error.
func (r *Runtime) Start(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return r.cache.Start(gctx)
	})

	if !r.cache.WaitForCacheSync(ctx) {
		return errors.New("runtime: cache sync timed out")
	}

	if r.apiServer != nil {
		g.Go(func() error {
			return r.apiServer.Start(gctx)
		})
	}

	return g.Wait()
}

// GetClient returns the composite client (handles both view and native K8s objects).
func (r *Runtime) GetClient() *store.CompositeClient { return r.client }

// GetCache returns the composite cache.
func (r *Runtime) GetCache() *store.CompositeCache { return r.cache }

// GetViewCache returns the view-specific in-memory cache.
func (r *Runtime) GetViewCache() store.ViewCacheInterface { return r.cache.GetViewCache() }

// GetDiscovery returns the composite discovery client with view-specific extensions.
func (r *Runtime) GetDiscovery() *store.CompositeDiscoveryClient { return r.discovery }

// GetRESTMapper returns the composite REST mapper.
func (r *Runtime) GetRESTMapper() meta.RESTMapper { return r.restMapper }

// GetAPIServer returns the embedded API server, or nil if it was not configured.
func (r *Runtime) GetAPIServer() *apiserver.APIServer { return r.apiServer }

// GenerateKubeconfig generates a kubeconfig for accessing the embedded API server with a JWT
// token for the given username. Returns an error if no API server or auth was configured.
func (r *Runtime) GenerateKubeconfig(username string, opts *kauth.KubeconfigOptions) (*clientcmdapi.Config, error) {
	if r.apiServer == nil {
		return nil, errors.New("runtime: no API server configured")
	}
	if r.privateKey == nil {
		return nil, errors.New("runtime: no auth configured")
	}

	gen := kauth.NewTokenGenerator(r.privateKey)
	token, err := gen.GenerateToken(username, nil, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("runtime: generate token: %w", err)
	}

	addr := r.apiServer.GetServerAddress()
	if r.cfg.APIServer.HTTPMode {
		addr = r.apiServer.GetInsecureServerAddress()
	}

	return kauth.GenerateKubeconfig(addr, username, token, opts), nil
}

// GetClientConfig returns an in-memory REST config with a JWT bearer token for the given
// username, suitable for programmatic access to the embedded API server. Returns an error if
// no API server or auth was configured.
func (r *Runtime) GetClientConfig(username string) (*rest.Config, error) {
	if r.apiServer == nil {
		return nil, errors.New("runtime: no API server configured")
	}
	if r.privateKey == nil {
		return nil, errors.New("runtime: no auth configured")
	}

	gen := kauth.NewTokenGenerator(r.privateKey)
	token, err := gen.GenerateToken(username, nil, nil, 0)
	if err != nil {
		return nil, fmt.Errorf("runtime: generate token: %w", err)
	}

	addr := r.apiServer.GetServerAddress()
	if r.cfg.APIServer.HTTPMode {
		addr = r.apiServer.GetInsecureServerAddress()
	}

	return kauth.CreateRestConfig(addr, token, r.cfg.APIServer.Insecure), nil
}
