package apiserver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"

	viewv1a1 "github.com/l7mp/connectors/kubernetes/runtime/api/view/v1alpha1"
	"github.com/l7mp/connectors/kubernetes/runtime/store"
)

func TestAPIServerStartAndDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	api, err := store.NewAPI(nil, store.APIOptions{Logger: logr.Discard()})
	if err != nil {
		t.Fatalf("new api: %v", err)
	}

	config, err := NewDefaultConfig("127.0.0.1", 0, api.Client, true, false, logr.Discard())
	if err != nil {
		t.Fatalf("new config: %v", err)
	}
	config.DiscoveryClient = api.GetDiscovery()
	config.EnableOpenAPI = false

	s, err := NewAPIServer(config)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	gvk := viewv1a1.GroupVersionKind("test", "TestView")
	if err := s.RegisterGVKs([]schema.GroupVersionKind{gvk}); err != nil {
		t.Fatalf("register gvks: %v", err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- s.Start(ctx) }()

	if !waitUntil(2*time.Second, 20*time.Millisecond, func() bool { return s.running }) {
		t.Fatalf("server did not become running")
	}

	addr := s.GetInsecureServerAddress()
	if addr == "<unknown>" || addr == "" {
		t.Fatalf("invalid insecure address: %q", addr)
	}

	dc, err := discovery.NewDiscoveryClientForConfig(&rest.Config{Host: fmt.Sprintf("http://%s", addr)})
	if err != nil {
		t.Fatalf("new discovery client: %v", err)
	}

	if !waitUntil(2*time.Second, 50*time.Millisecond, func() bool {
		groups, resources, err := dc.ServerGroupsAndResources()
		if err != nil || len(groups) == 0 {
			return false
		}
		for _, g := range groups {
			if g.Name != viewv1a1.Group("test") {
				continue
			}
			for _, rl := range resources {
				if rl.GroupVersion != gvk.GroupVersion().String() {
					continue
				}
				for _, r := range rl.APIResources {
					if r.Kind == gvk.Kind {
						return true
					}
				}
			}
		}
		return false
	}) {
		t.Fatalf("registered GVK not discoverable")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server exit error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("server did not stop")
	}
}

func TestRegisterAndUnregisterGVKs(t *testing.T) {
	api, err := store.NewAPI(nil, store.APIOptions{Logger: logr.Discard()})
	if err != nil {
		t.Fatalf("new api: %v", err)
	}

	config, err := NewDefaultConfig("127.0.0.1", 0, api.Client, true, false, logr.Discard())
	if err != nil {
		t.Fatalf("new config: %v", err)
	}
	config.DiscoveryClient = api.GetDiscovery()
	config.EnableOpenAPI = false

	s, err := NewAPIServer(config)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	g1 := viewv1a1.GroupVersionKind("g1", "View1")
	g2 := viewv1a1.GroupVersionKind("g2", "View2")

	if err := s.RegisterGVKs([]schema.GroupVersionKind{g1, g2}); err != nil {
		t.Fatalf("register gvks: %v", err)
	}

	s.mu.RLock()
	if _, ok := s.groupGVKs[g1.Group]; !ok {
		t.Fatalf("group %s not registered", g1.Group)
	}
	if _, ok := s.groupGVKs[g2.Group]; !ok {
		t.Fatalf("group %s not registered", g2.Group)
	}
	s.mu.RUnlock()

	s.UnregisterGVKs([]schema.GroupVersionKind{g1, g2})

	s.mu.RLock()
	_, ok1 := s.groupGVKs[g1.Group]
	_, ok2 := s.groupGVKs[g2.Group]
	s.mu.RUnlock()
	if ok1 || ok2 {
		t.Fatalf("expected groups unregistered, got ok1=%v ok2=%v", ok1, ok2)
	}
}

func TestFindAPIResourceViewOnly(t *testing.T) {
	api, err := store.NewAPI(nil, store.APIOptions{Logger: logr.Discard()})
	if err != nil {
		t.Fatalf("new api: %v", err)
	}

	config, err := NewDefaultConfig("127.0.0.1", 0, api.Client, true, false, logr.Discard())
	if err != nil {
		t.Fatalf("new config: %v", err)
	}
	config.DiscoveryClient = api.GetDiscovery()
	config.EnableOpenAPI = false

	s, err := NewAPIServer(config)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	if _, err := s.findAPIResource(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Pod"}); err == nil {
		t.Fatalf("expected native resource rejection")
	}

	gvk := viewv1a1.GroupVersionKind("t", "MyView")
	r, err := s.findAPIResource(gvk)
	if err != nil {
		t.Fatalf("find view resource failed: %v", err)
	}
	if r.APIResource == nil || r.APIResource.Kind != "MyView" {
		t.Fatalf("unexpected API resource: %#v", r.APIResource)
	}
	if !r.HasStatus {
		t.Fatalf("expected HasStatus=true for view resource")
	}
}

func waitUntil(timeout, interval time.Duration, pred func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(interval)
	}
	return pred()
}
