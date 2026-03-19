package apiserver

import (
	"testing"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"

	viewv1a1 "github.com/l7mp/connectors/kubernetes/runtime/api/view/v1alpha1"
	"github.com/l7mp/connectors/kubernetes/runtime/store"
)

func TestListGVK(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "g", Version: "v1", Kind: "Thing"}
	list := listGVK(gvk)
	if list.Kind != "ThingList" || list.Group != "g" || list.Version != "v1" {
		t.Fatalf("unexpected list GVK: %#v", list)
	}
}

func TestFindAPIResourceForView(t *testing.T) {
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

	gvk := viewv1a1.GroupVersionKind("res", "Sample")
	r, err := s.findAPIResource(gvk)
	if err != nil {
		t.Fatalf("find resource: %v", err)
	}
	if r.APIResource == nil {
		t.Fatalf("api resource is nil")
	}
	if r.APIResource.Name != "sample" || r.APIResource.Kind != "Sample" {
		t.Fatalf("unexpected APIResource: %#v", r.APIResource)
	}
	if !r.APIResource.Namespaced || !r.HasStatus {
		t.Fatalf("unexpected namespaced/status flags: namespaced=%v hasStatus=%v",
			r.APIResource.Namespaced, r.HasStatus)
	}
}
