package js

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/dop251/goja"
	"github.com/golang-jwt/jwt/v5"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/client-go/tools/clientcmd"
	ctrlcfg "sigs.k8s.io/controller-runtime/pkg/client/config"

	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	kauth "github.com/l7mp/dbsp/connectors/kubernetes/runtime/auth"
)

const (
	defaultK8sAPIServerAddr     = "0.0.0.0"
	defaultK8sAPIServerPort     = 8443
	defaultK8sCertFile          = "apiserver.crt"
	defaultK8sKeyFile           = "apiserver.key"
	defaultKubeconfigServerAddr = "localhost:8443"
	k8sRuntimeConfigDataKey     = "__dbsp_kubernetes_runtime_config"
)

type k8sRuntimeStartConfig struct {
	APIServer *k8sRuntimeAPIServerConfig `json:"apiServer,omitempty"`
	Auth      *k8sRuntimeAuthConfig      `json:"auth,omitempty"`
}

type k8sRuntimeAPIServerConfig struct {
	Addr          string `json:"addr"`
	Port          int    `json:"port"`
	HTTP          bool   `json:"http"`
	Insecure      bool   `json:"insecure"`
	CertFile      string `json:"certFile"`
	KeyFile       string `json:"keyFile"`
	EnableOpenAPI bool   `json:"enableOpenAPI"`
}

type k8sRuntimeAuthConfig struct {
	PrivateKeyFile string `json:"privateKeyFile"`
	PublicKeyFile  string `json:"publicKeyFile"`
}

type k8sRuntimeConfigInput struct {
	APIServer *k8sRuntimeAPIServerInput `json:"apiServer"`
	Auth      *k8sRuntimeAuthInput      `json:"auth"`
}

type k8sRuntimeAPIServerInput struct {
	Addr          string `json:"addr"`
	Port          int    `json:"port"`
	HTTP          bool   `json:"http"`
	Insecure      bool   `json:"insecure"`
	CertFile      string `json:"certFile"`
	KeyFile       string `json:"keyFile"`
	EnableOpenAPI *bool  `json:"enableOpenAPI"`
}

type k8sRuntimeAuthInput struct {
	PrivateKeyFile string `json:"privateKeyFile"`
	PublicKeyFile  string `json:"publicKeyFile"`
	AutoGenerate   bool   `json:"autoGenerate"`
}

type k8sGenerateKeysOptions struct {
	Hostnames []string `json:"hostnames"`
	KeyFile   string   `json:"keyFile"`
	CertFile  string   `json:"certFile"`
}

type k8sGenerateKubeconfigOptions struct {
	User             string              `json:"user"`
	Namespaces       []string            `json:"namespaces"`
	Rules            []rbacv1.PolicyRule `json:"rules"`
	RulesFile        string              `json:"rulesFile"`
	ResourceNames    []string            `json:"resourceNames"`
	Expiry           string              `json:"expiry"`
	KeyFile          string              `json:"keyFile"`
	ServerAddress    string              `json:"serverAddress"`
	DefaultNamespace string              `json:"defaultNamespace"`
	Insecure         *bool               `json:"insecure"`
	HTTP             *bool               `json:"http"`
	OutputFile       string              `json:"outputFile"`
}

type k8sInspectKubeconfigOptions struct {
	Kubeconfig string `json:"kubeconfig"`
	CertFile   string `json:"certFile"`
}

func (v *VM) newK8sRuntimeNamespace() (*goja.Object, error) {
	obj := v.rt.NewObject()
	if err := obj.Set("config", v.wrap(v.k8sRuntimeConfig)); err != nil {
		return nil, err
	}
	if err := obj.Set("start", v.wrap(v.k8sRuntimeStart)); err != nil {
		return nil, err
	}
	if err := obj.Set("toJSON", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.rt.ToValue(map[string]any{
			"kind": "kubernetes.runtime",
			"apis": []string{"config", "start"},
		}), nil
	})); err != nil {
		return nil, err
	}
	return obj, nil
}

func (v *VM) k8sRuntimeConfig(call goja.FunctionCall) (goja.Value, error) {
	cfg, err := v.decodeK8sRuntimeConfigValue(call.Argument(0))
	if err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.config: %w", err)
	}

	return v.newK8sRuntimeConfigObject(cfg)
}

func (v *VM) k8sRuntimeStart(call goja.FunctionCall) (goja.Value, error) {
	cfg, err := v.decodeK8sRuntimeConfigValue(call.Argument(0))
	if err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.start: %w", err)
	}

	if err := v.startK8sRuntime(cfg); err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.start: %w", err)
	}

	return goja.Undefined(), nil
}

func (v *VM) newK8sRuntimeConfigObject(cfg k8sRuntimeStartConfig) (goja.Value, error) {
	data := cfg.toMap()
	obj := v.rt.NewObject()

	if err := obj.Set(k8sRuntimeConfigDataKey, data); err != nil {
		return nil, err
	}
	if api, ok := data["apiServer"]; ok {
		if err := obj.Set("apiServer", api); err != nil {
			return nil, err
		}
	}
	if auth, ok := data["auth"]; ok {
		if err := obj.Set("auth", auth); err != nil {
			return nil, err
		}
	}

	if err := obj.Set("start", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		if len(call.Arguments) != 0 {
			return nil, fmt.Errorf("kubernetes.runtime.config(...).start() takes no arguments")
		}
		if err := v.startK8sRuntime(cfg); err != nil {
			return nil, fmt.Errorf("kubernetes.runtime.start: %w", err)
		}
		return goja.Undefined(), nil
	})); err != nil {
		return nil, err
	}

	if err := obj.Set("generateKeys", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.k8sRuntimeGenerateKeys(cfg, call.Argument(0))
	})); err != nil {
		return nil, err
	}
	if err := obj.Set("generateKubeConfig", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.k8sRuntimeGenerateKubeConfig(cfg, call.Argument(0))
	})); err != nil {
		return nil, err
	}
	if err := obj.Set("generateConfig", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.k8sRuntimeGenerateKubeConfig(cfg, call.Argument(0))
	})); err != nil {
		return nil, err
	}
	if err := obj.Set("inspectKubeConfig", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.k8sRuntimeInspectKubeConfig(cfg, call.Argument(0))
	})); err != nil {
		return nil, err
	}
	if err := obj.Set("getConfig", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.k8sRuntimeInspectKubeConfig(cfg, call.Argument(0))
	})); err != nil {
		return nil, err
	}
	if err := obj.Set("toJSON", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.rt.ToValue(data), nil
	})); err != nil {
		return nil, err
	}

	return obj, nil
}

func (v *VM) decodeK8sRuntimeConfigValue(value goja.Value) (k8sRuntimeStartConfig, error) {
	if value == nil {
		value = goja.Undefined()
	}
	if !goja.IsUndefined(value) && !goja.IsNull(value) {
		if obj := value.ToObject(v.rt); obj != nil {
			embedded := obj.Get(k8sRuntimeConfigDataKey)
			if !goja.IsUndefined(embedded) && !goja.IsNull(embedded) {
				value = embedded
			}
		}
	}

	var in k8sRuntimeConfigInput
	if err := decodeOptionValue(value, &in); err != nil {
		return k8sRuntimeStartConfig{}, err
	}
	return normalizeK8sRuntimeConfig(in)
}

func normalizeK8sRuntimeConfig(in k8sRuntimeConfigInput) (k8sRuntimeStartConfig, error) {
	cfg := k8sRuntimeStartConfig{}

	if in.APIServer != nil {
		enableOpenAPI := true
		if in.APIServer.EnableOpenAPI != nil {
			enableOpenAPI = *in.APIServer.EnableOpenAPI
		}

		api := &k8sRuntimeAPIServerConfig{
			Addr:          in.APIServer.Addr,
			Port:          in.APIServer.Port,
			HTTP:          in.APIServer.HTTP,
			Insecure:      in.APIServer.Insecure,
			CertFile:      in.APIServer.CertFile,
			KeyFile:       in.APIServer.KeyFile,
			EnableOpenAPI: enableOpenAPI,
		}

		if api.Addr == "" {
			api.Addr = defaultK8sAPIServerAddr
		}
		if api.Port == 0 {
			api.Port = defaultK8sAPIServerPort
		}
		if !api.HTTP {
			if api.CertFile == "" {
				api.CertFile = defaultK8sCertFile
			}
			if api.KeyFile == "" {
				api.KeyFile = defaultK8sKeyFile
			}
		}

		cfg.APIServer = api
	}

	if in.Auth != nil {
		if cfg.APIServer == nil {
			return k8sRuntimeStartConfig{}, fmt.Errorf("auth requires apiServer configuration")
		}

		authCfg := &k8sRuntimeAuthConfig{}
		if in.Auth.AutoGenerate {
			if in.Auth.PrivateKeyFile != "" || in.Auth.PublicKeyFile != "" {
				return k8sRuntimeStartConfig{}, fmt.Errorf("auth.autoGenerate cannot be combined with privateKeyFile/publicKeyFile")
			}
		} else {
			authCfg.PrivateKeyFile = in.Auth.PrivateKeyFile
			authCfg.PublicKeyFile = in.Auth.PublicKeyFile
			if (authCfg.PrivateKeyFile == "") != (authCfg.PublicKeyFile == "") {
				return k8sRuntimeStartConfig{}, fmt.Errorf("auth.privateKeyFile and auth.publicKeyFile must be set together")
			}
		}

		cfg.Auth = authCfg
	}

	return cfg, nil
}

func (v *VM) startK8sRuntime(cfg k8sRuntimeStartConfig) error {
	v.k8sMu.Lock()
	defer v.k8sMu.Unlock()

	if v.k8sRuntime != nil {
		if v.k8sStartConfig != nil && reflect.DeepEqual(*v.k8sStartConfig, cfg) {
			return nil
		}
		return fmt.Errorf("runtime already started")
	}

	restCfg, err := ctrlcfg.GetConfig()
	nativeAvailable := true
	if err != nil {
		if !clientcmd.IsEmptyConfig(err) {
			return fmt.Errorf("get kubeconfig: %w", err)
		}
		nativeAvailable = false
		restCfg = nil
		fmt.Fprintln(os.Stderr, "warning: kubeconfig is unavailable: native Kubernetes resources are disabled, only view resources can be used")
	}

	k8sCfg := k8sruntime.Config{
		RESTConfig: restCfg,
		Logger:     v.logger.WithName("kubernetes-runtime"),
	}
	if cfg.APIServer != nil {
		apiCfg, err := k8sruntime.NewDefaultAPIServerConfig(
			cfg.APIServer.Addr,
			cfg.APIServer.Port,
			cfg.APIServer.HTTP,
			cfg.APIServer.Insecure,
			v.logger.WithName("kubernetes-apiserver"),
		)
		if err != nil {
			return fmt.Errorf("build apiServer config: %w", err)
		}
		apiCfg.EnableOpenAPI = cfg.APIServer.EnableOpenAPI
		if !cfg.APIServer.HTTP {
			apiCfg.CertFile = cfg.APIServer.CertFile
			apiCfg.KeyFile = cfg.APIServer.KeyFile
		}
		k8sCfg.APIServer = &apiCfg
	}
	if cfg.Auth != nil {
		k8sCfg.Auth = &k8sruntime.AuthConfig{
			PrivateKeyFile: cfg.Auth.PrivateKeyFile,
			PublicKeyFile:  cfg.Auth.PublicKeyFile,
		}
	}

	krt, err := k8sruntime.New(k8sCfg)
	if err != nil {
		return fmt.Errorf("create runtime: %w", err)
	}

	if nativeAvailable || k8sCfg.APIServer != nil {
		if err := v.runtime.Add(&k8sRuntimeRunner{rt: krt}); err != nil {
			return fmt.Errorf("register runtime: %w", err)
		}
	}

	v.k8sRuntime = krt
	v.k8sNativeAvailable = nativeAvailable
	cfgCopy := cfg
	v.k8sStartConfig = &cfgCopy

	return nil
}

func (cfg k8sRuntimeStartConfig) defaultPrivateKeyFile() string {
	if cfg.Auth != nil && cfg.Auth.PrivateKeyFile != "" {
		return cfg.Auth.PrivateKeyFile
	}
	if cfg.APIServer != nil && cfg.APIServer.KeyFile != "" {
		return cfg.APIServer.KeyFile
	}
	return defaultK8sKeyFile
}

func (cfg k8sRuntimeStartConfig) defaultPublicKeyFile() string {
	if cfg.Auth != nil && cfg.Auth.PublicKeyFile != "" {
		return cfg.Auth.PublicKeyFile
	}
	if cfg.APIServer != nil && cfg.APIServer.CertFile != "" {
		return cfg.APIServer.CertFile
	}
	return defaultK8sCertFile
}

func (cfg k8sRuntimeStartConfig) defaultServerAddress() string {
	if cfg.APIServer == nil {
		return defaultKubeconfigServerAddr
	}
	return fmt.Sprintf("%s:%d", cfg.APIServer.Addr, cfg.APIServer.Port)
}

func (cfg k8sRuntimeStartConfig) defaultHTTPMode() bool {
	if cfg.APIServer == nil {
		return false
	}
	return cfg.APIServer.HTTP
}

func (cfg k8sRuntimeStartConfig) defaultInsecureMode() bool {
	if cfg.APIServer == nil {
		return false
	}
	return cfg.APIServer.Insecure
}

func (cfg k8sRuntimeStartConfig) toMap() map[string]any {
	out := map[string]any{}
	if cfg.APIServer != nil {
		out["apiServer"] = map[string]any{
			"addr":          cfg.APIServer.Addr,
			"port":          cfg.APIServer.Port,
			"http":          cfg.APIServer.HTTP,
			"insecure":      cfg.APIServer.Insecure,
			"certFile":      cfg.APIServer.CertFile,
			"keyFile":       cfg.APIServer.KeyFile,
			"enableOpenAPI": cfg.APIServer.EnableOpenAPI,
		}
	}
	if cfg.Auth != nil {
		out["auth"] = map[string]any{
			"privateKeyFile": cfg.Auth.PrivateKeyFile,
			"publicKeyFile":  cfg.Auth.PublicKeyFile,
		}
	}
	return out
}

func (v *VM) k8sRuntimeGenerateKeys(cfg k8sRuntimeStartConfig, value goja.Value) (goja.Value, error) {
	var opts k8sGenerateKeysOptions
	if err := decodeOptionValue(value, &opts); err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.config(...).generateKeys: %w", err)
	}

	if len(opts.Hostnames) == 0 {
		host := "localhost"
		if cfg.APIServer != nil && cfg.APIServer.Addr != "" && cfg.APIServer.Addr != defaultK8sAPIServerAddr {
			host = cfg.APIServer.Addr
		}
		opts.Hostnames = []string{host}
	}
	if opts.KeyFile == "" {
		opts.KeyFile = cfg.defaultPrivateKeyFile()
	}
	if opts.CertFile == "" {
		opts.CertFile = cfg.defaultPublicKeyFile()
	}

	cert, key, err := kauth.GenerateSelfSignedCertWithSANs(opts.Hostnames)
	if err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.config(...).generateKeys: %w", err)
	}
	if err := kauth.WriteCertAndKey(opts.CertFile, opts.KeyFile, cert, key); err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.config(...).generateKeys: %w", err)
	}

	return v.rt.ToValue(map[string]any{
		"keyFile":   opts.KeyFile,
		"certFile":  opts.CertFile,
		"hostnames": opts.Hostnames,
	}), nil
}

func (v *VM) k8sRuntimeGenerateKubeConfig(cfg k8sRuntimeStartConfig, value goja.Value) (goja.Value, error) {
	var opts k8sGenerateKubeconfigOptions
	if err := decodeOptionValue(value, &opts); err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.config(...).generateKubeConfig: %w", err)
	}
	if opts.User == "" {
		return nil, fmt.Errorf("kubernetes.runtime.config(...).generateKubeConfig: missing user")
	}

	expiry := 24 * 365 * time.Hour
	if opts.Expiry != "" {
		d, err := time.ParseDuration(opts.Expiry)
		if err != nil {
			return nil, fmt.Errorf("kubernetes.runtime.config(...).generateKubeConfig: parse expiry: %w", err)
		}
		expiry = d
	}

	rules := append([]rbacv1.PolicyRule(nil), opts.Rules...)
	if opts.RulesFile != "" {
		body, err := os.ReadFile(opts.RulesFile)
		if err != nil {
			return nil, fmt.Errorf("kubernetes.runtime.config(...).generateKubeConfig: read rulesFile: %w", err)
		}
		if err := json.Unmarshal(body, &rules); err != nil {
			return nil, fmt.Errorf("kubernetes.runtime.config(...).generateKubeConfig: parse rulesFile: %w", err)
		}
	}
	if len(opts.ResourceNames) > 0 && len(rules) > 0 {
		last := len(rules) - 1
		rules[last].ResourceNames = append([]string(nil), opts.ResourceNames...)
	}

	keyFile := opts.KeyFile
	if keyFile == "" {
		keyFile = cfg.defaultPrivateKeyFile()
	}
	privateKey, err := kauth.LoadPrivateKey(keyFile)
	if err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.config(...).generateKubeConfig: load private key: %w", err)
	}

	token, err := kauth.NewTokenGenerator(privateKey).GenerateToken(opts.User, opts.Namespaces, rules, expiry)
	if err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.config(...).generateKubeConfig: generate token: %w", err)
	}

	serverAddress := opts.ServerAddress
	if serverAddress == "" {
		serverAddress = cfg.defaultServerAddress()
	}
	httpMode := cfg.defaultHTTPMode()
	if opts.HTTP != nil {
		httpMode = *opts.HTTP
	}
	insecure := cfg.defaultInsecureMode()
	if opts.Insecure != nil {
		insecure = *opts.Insecure
	}

	kubeCfg := kauth.GenerateKubeconfig(serverAddress, opts.User, token, &kauth.KubeconfigOptions{
		ClusterName:      "dcontroller",
		ContextName:      "dcontroller",
		DefaultNamespace: opts.DefaultNamespace,
		Insecure:         insecure,
		HTTPMode:         httpMode,
	})

	body, err := clientcmd.Write(*kubeCfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.config(...).generateKubeConfig: write kubeconfig: %w", err)
	}
	if opts.OutputFile != "" {
		if err := clientcmd.WriteToFile(*kubeCfg, opts.OutputFile); err != nil {
			return nil, fmt.Errorf("kubernetes.runtime.config(...).generateKubeConfig: write outputFile: %w", err)
		}
	}

	return v.rt.ToValue(string(body)), nil
}

func (v *VM) k8sRuntimeInspectKubeConfig(cfg k8sRuntimeStartConfig, value goja.Value) (goja.Value, error) {
	var opts k8sInspectKubeconfigOptions
	if err := decodeOptionValue(value, &opts); err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.config(...).inspectKubeConfig: %w", err)
	}

	kubeconfigPath := opts.Kubeconfig
	if kubeconfigPath == "" {
		kubeconfigPath = os.Getenv("KUBECONFIG")
	}
	if kubeconfigPath == "" {
		return nil, fmt.Errorf("kubernetes.runtime.config(...).inspectKubeConfig: no kubeconfig path provided")
	}

	certFile := opts.CertFile
	if certFile == "" {
		certFile = cfg.defaultPublicKeyFile()
	}

	token, err := kauth.ExtractTokenFromKubeconfig(kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.config(...).inspectKubeConfig: extract token: %w", err)
	}
	publicKey, err := kauth.LoadPublicKey(certFile)
	if err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.config(...).inspectKubeConfig: load public key: %w", err)
	}

	claims := &kauth.Claims{}
	jwtToken, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (any, error) {
		return publicKey, nil
	})
	if err != nil {
		return nil, err
	}
	if claims.ExpiresAt != nil && time.Now().After(claims.ExpiresAt.Time) {
		return nil, fmt.Errorf("token expired")
	}

	result := map[string]any{
		"username":   claims.Username,
		"namespaces": claims.Namespaces,
		"rules":      claims.Rules,
		"issuer":     claims.Issuer,
		"valid":      jwtToken != nil && jwtToken.Valid,
	}
	if claims.IssuedAt != nil {
		result["issuedAt"] = claims.IssuedAt.Time.Format(time.RFC3339)
	}
	if claims.ExpiresAt != nil {
		result["expiresAt"] = claims.ExpiresAt.Time.Format(time.RFC3339)
	}
	if claims.NotBefore != nil {
		result["notBefore"] = claims.NotBefore.Time.Format(time.RFC3339)
	}

	return v.rt.ToValue(result), nil
}
