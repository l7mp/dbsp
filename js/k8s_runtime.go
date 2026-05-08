package js

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/dop251/goja"
	"github.com/golang-jwt/jwt/v5"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrlcfg "sigs.k8s.io/controller-runtime/pkg/client/config"

	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
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
	Kubeconfig string                     `json:"kubeconfig,omitempty"`
	APIServer  *k8sRuntimeAPIServerConfig `json:"apiServer,omitempty"`
	Auth       *k8sRuntimeAuthConfig      `json:"auth,omitempty"`
}

type k8sRuntimeAPIServerConfig struct {
	Addr          string `json:"addr"`
	Port          int    `json:"port"`
	HTTP          bool   `json:"http"`
	Insecure      bool   `json:"insecure"`
	CertFile      string `json:"certFile"`
	KeyFile       string `json:"keyFile"`
	EnableOpenAPI bool   `json:"enableOpenAPI"` //nolint:tagliatelle
}

type k8sRuntimeAuthConfig struct {
	PrivateKeyFile string `json:"privateKeyFile"`
	PublicKeyFile  string `json:"publicKeyFile"`
}

type k8sRuntimeConfigInput struct {
	Kubeconfig string                    `json:"kubeconfig"`
	APIServer  *k8sRuntimeAPIServerInput `json:"apiServer"`
	Auth       *k8sRuntimeAuthInput      `json:"auth"`
}

type k8sRuntimeAPIServerInput struct {
	Addr          string `json:"addr"`
	Port          int    `json:"port"`
	HTTP          bool   `json:"http"`
	Insecure      bool   `json:"insecure"`
	CertFile      string `json:"certFile"`
	KeyFile       string `json:"keyFile"`
	EnableOpenAPI *bool  `json:"enableOpenAPI"` //nolint:tagliatelle
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

type k8sResolveGVKOptions struct {
	Operator string  `json:"operator"`
	APIGroup *string `json:"apiGroup"`
	Version  *string `json:"version"`
	Kind     string  `json:"kind"`
}

type k8sViewRegistrationOptions struct {
	GVKs []string `json:"gvks"`
}

func (v *VM) newK8sRuntimeNamespace() (*goja.Object, error) {
	obj := v.rt.NewObject()
	if err := obj.Set("config", v.wrap(v.k8sRuntimeConfig)); err != nil {
		return nil, err
	}
	if err := obj.Set("start", v.wrap(v.k8sRuntimeStart)); err != nil {
		return nil, err
	}
	if err := obj.Set("resolveGVK", v.wrap(v.k8sRuntimeResolveGVK)); err != nil {
		return nil, err
	}
	if err := obj.Set("registerViews", v.wrap(v.k8sRuntimeRegisterViews)); err != nil {
		return nil, err
	}
	if err := obj.Set("unregisterViews", v.wrap(v.k8sRuntimeUnregisterViews)); err != nil {
		return nil, err
	}
	if err := obj.Set("toJSON", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.rt.ToValue(map[string]any{
			"kind": "kubernetes.runtime",
			"apis": []string{"config", "start", "resolveGVK", "registerViews", "unregisterViews"},
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

func (v *VM) k8sRuntimeResolveGVK(call goja.FunctionCall) (goja.Value, error) {
	var opts k8sResolveGVKOptions
	if err := decodeOptionValue(call.Argument(0), &opts); err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.resolveGVK: %w", err)
	}

	gvk, err := v.resolveRuntimeGVK(opts)
	if err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.resolveGVK: %w", err)
	}

	return v.rt.ToValue(map[string]any{
		"group":   gvk.Group,
		"version": gvk.Version,
		"kind":    gvk.Kind,
		"gvk":     fmt.Sprintf("%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind),
	}), nil
}

func (v *VM) k8sRuntimeRegisterViews(call goja.FunctionCall) (goja.Value, error) {
	gvks, err := decodeViewGVKList(call.Argument(0))
	if err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.registerViews: %w", err)
	}

	krt, err := v.ensureK8sRuntime()
	if err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.registerViews: %w", err)
	}

	for _, gvk := range gvks {
		if !viewv1a1.IsViewGroup(gvk.Group) {
			return nil, fmt.Errorf("%s is not a view group", gvk.Group)
		}
		if err := krt.GetDiscovery().RegisterViewGVK(gvk); err != nil {
			return nil, fmt.Errorf("register discovery GVK %s: %w", gvk.String(), err)
		}
	}

	if api := krt.GetAPIServer(); api != nil {
		if err := api.RegisterGVKs(gvks); err != nil {
			return nil, fmt.Errorf("register API server GVKs: %w", err)
		}
	}

	return goja.Undefined(), nil
}

func (v *VM) k8sRuntimeUnregisterViews(call goja.FunctionCall) (goja.Value, error) {
	gvks, err := decodeViewGVKList(call.Argument(0))
	if err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.unregisterViews: %w", err)
	}

	krt, err := v.ensureK8sRuntime()
	if err != nil {
		return nil, fmt.Errorf("kubernetes.runtime.unregisterViews: %w", err)
	}

	if api := krt.GetAPIServer(); api != nil {
		api.UnregisterGVKs(gvks)
	}

	for _, gvk := range gvks {
		if !viewv1a1.IsViewGroup(gvk.Group) {
			return nil, fmt.Errorf("%s is not a view group", gvk.Group)
		}
		if err := krt.GetDiscovery().UnregisterViewGVK(gvk); err != nil {
			return nil, fmt.Errorf("unregister discovery GVK %s: %w", gvk.String(), err)
		}
	}

	return goja.Undefined(), nil
}

func (v *VM) resolveRuntimeGVK(opts k8sResolveGVKOptions) (schema.GroupVersionKind, error) {
	kind := strings.TrimSpace(opts.Kind)
	if kind == "" {
		return schema.GroupVersionKind{}, fmt.Errorf("missing kind")
	}

	version := ""
	if opts.Version != nil {
		version = strings.TrimSpace(*opts.Version)
	}

	if opts.APIGroup == nil {
		op := strings.TrimSpace(opts.Operator)
		if op == "" {
			return schema.GroupVersionKind{}, fmt.Errorf("operator is required when apiGroup is omitted")
		}
		return viewv1a1.GroupVersionKind(op, kind), nil
	}

	group := strings.TrimSpace(*opts.APIGroup)
	if group == "" {
		if version == "" {
			version = "v1"
		}
		return schema.GroupVersionKind{Group: "", Version: version, Kind: kind}, nil
	}

	if viewv1a1.IsViewGroup(group) {
		return schema.GroupVersionKind{Group: group, Version: viewv1a1.Version, Kind: kind}, nil
	}

	if version != "" {
		return schema.GroupVersionKind{Group: group, Version: version, Kind: kind}, nil
	}
	krt, err := v.ensureK8sRuntime()
	if err != nil {
		return schema.GroupVersionKind{}, err
	}
	if !v.k8sNativeAvailable {
		return schema.GroupVersionKind{}, fmt.Errorf("native Kubernetes resources unavailable: kubeconfig is missing, only view resources can be used")
	}

	mapping, err := krt.GetRESTMapper().RESTMapping(schema.GroupKind{Group: group, Kind: kind})
	if err != nil {
		return schema.GroupVersionKind{}, fmt.Errorf("resolve GVK for %s/%s: %w", group, kind, err)
	}

	return mapping.GroupVersionKind, nil
}

func decodeViewGVKList(value goja.Value) ([]schema.GroupVersionKind, error) {
	if value == nil || goja.IsUndefined(value) || goja.IsNull(value) {
		return nil, fmt.Errorf("missing GVK list")
	}

	var list []string
	if err := decodeOptionValue(value, &list); err == nil {
		return parseViewGVKStrings(list)
	}

	var opts k8sViewRegistrationOptions
	if err := decodeOptionValue(value, &opts); err != nil {
		return nil, fmt.Errorf("expected string[] or {gvks:string[]}")
	}

	return parseViewGVKStrings(opts.GVKs)
}

func parseViewGVKStrings(raw []string) ([]schema.GroupVersionKind, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty GVK list")
	}

	out := make([]schema.GroupVersionKind, 0, len(raw))
	for i, item := range raw {
		gvk, err := parseRawGVK(item)
		if err != nil {
			return nil, fmt.Errorf("index %d: %w", i, err)
		}
		out = append(out, gvk)
	}

	return out, nil
}

func parseRawGVK(raw string) (schema.GroupVersionKind, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return schema.GroupVersionKind{}, fmt.Errorf("empty GVK")
	}

	parts := strings.Split(s, "/")
	switch len(parts) {
	case 2:
		gv, err := schema.ParseGroupVersion(strings.TrimSpace(parts[0]))
		if err != nil {
			return schema.GroupVersionKind{}, fmt.Errorf("invalid apiVersion: %w", err)
		}
		kind := strings.TrimSpace(parts[1])
		if kind == "" {
			return schema.GroupVersionKind{}, fmt.Errorf("missing kind")
		}
		return schema.GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: kind}, nil
	case 3:
		group := strings.TrimSpace(parts[0])
		version := strings.TrimSpace(parts[1])
		kind := strings.TrimSpace(parts[2])
		if group == "" || version == "" || kind == "" {
			return schema.GroupVersionKind{}, fmt.Errorf("expected group/version/kind")
		}
		return schema.GroupVersionKind{Group: group, Version: version, Kind: kind}, nil
	default:
		return schema.GroupVersionKind{}, fmt.Errorf("expected v1/Kind or group/version/kind")
	}
}

func (v *VM) newK8sRuntimeConfigObject(cfg k8sRuntimeStartConfig) (goja.Value, error) {
	data := cfg.toMap()
	obj := v.rt.NewObject()

	if err := obj.Set(k8sRuntimeConfigDataKey, data); err != nil {
		return nil, err
	}
	for key, val := range data {
		if err := obj.Set(key, val); err != nil {
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
			if embedded != nil && !goja.IsUndefined(embedded) && !goja.IsNull(embedded) {
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
	cfg := k8sRuntimeStartConfig{Kubeconfig: strings.TrimSpace(in.Kubeconfig)}

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

	var (
		restCfg         *rest.Config
		err             error
		nativeAvailable = true
	)

	if cfg.Kubeconfig != "" {
		restCfg, err = clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
		if err != nil {
			return fmt.Errorf("load kubeconfig %q: %w", cfg.Kubeconfig, err)
		}
	} else {
		restCfg, err = ctrlcfg.GetConfig()
		if err != nil {
			if !clientcmd.IsEmptyConfig(err) {
				return fmt.Errorf("get kubeconfig: %w", err)
			}
			nativeAvailable = false
			restCfg = nil
			fmt.Fprintln(os.Stderr, "warning: kubeconfig is unavailable: native Kubernetes resources are disabled, only view resources can be used")
		}
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
	if cfg.Kubeconfig != "" {
		out["kubeconfig"] = cfg.Kubeconfig
	}
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
