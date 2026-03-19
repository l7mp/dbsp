package apiserver

import (
	"testing"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"

	viewv1a1 "github.com/l7mp/connectors/kubernetes/runtime/api/view/v1alpha1"
	"github.com/l7mp/connectors/kubernetes/runtime/store"
)

func TestRegisterGVKsGroupsByAPIGroup(t *testing.T) {
	api, err := store.NewAPI(nil, store.APIOptions{Logger: logr.Discard()})
	if err != nil {
		t.Fatalf("new api: %v", err)
	}
	config, err := NewDefaultConfig("127.0.0.1", 0, api.Client, true, false, logr.Discard())
	if err != nil {
		t.Fatalf("new config: %v", err)
	}
	config.EnableOpenAPI = false

	s, err := NewAPIServer(config)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	g1 := viewv1a1.GroupVersionKind("reg1", "V1")
	g2 := viewv1a1.GroupVersionKind("reg2", "V2")
	if err := s.RegisterGVKs([]schema.GroupVersionKind{g1, g2}); err != nil {
		t.Fatalf("register gvks: %v", err)
	}

	s.mu.RLock()
	_, ok1 := s.groupGVKs[g1.Group]
	_, ok2 := s.groupGVKs[g2.Group]
	s.mu.RUnlock()
	if !ok1 || !ok2 {
		t.Fatalf("expected both groups registered, got ok1=%v ok2=%v", ok1, ok2)
	}
}

func TestUnregisterAPIGroupIdempotent(t *testing.T) {
	api, err := store.NewAPI(nil, store.APIOptions{Logger: logr.Discard()})
	if err != nil {
		t.Fatalf("new api: %v", err)
	}
	config, err := NewDefaultConfig("127.0.0.1", 0, api.Client, true, false, logr.Discard())
	if err != nil {
		t.Fatalf("new config: %v", err)
	}
	config.EnableOpenAPI = false

	s, err := NewAPIServer(config)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	group := viewv1a1.Group("idem")
	gvk := viewv1a1.GroupVersionKind("idem", "View")
	if err := s.RegisterAPIGroup(group, []schema.GroupVersionKind{gvk}); err != nil {
		t.Fatalf("register group: %v", err)
	}

	s.UnregisterAPIGroup(group)
	s.UnregisterAPIGroup(group)

	s.mu.RLock()
	_, ok := s.groupGVKs[group]
	s.mu.RUnlock()
	if ok {
		t.Fatalf("group should be unregistered")
	}
}
