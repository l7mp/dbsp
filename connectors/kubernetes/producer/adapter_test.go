package producer

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"

	kobject "github.com/l7mp/connectors/kubernetes/runtime/object"
	"github.com/l7mp/connectors/kubernetes/runtime/store"
)

func TestConvertDeltaToZSetLifecycle(t *testing.T) {
	p := &Producer{sourceCache: map[schema.GroupVersionKind]*store.Store{}}

	obj := kobject.New()
	gvk := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	obj.SetGroupVersionKind(gvk)
	obj.SetNamespace("default")
	obj.SetName("app")
	kobject.SetContent(obj, map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      "app",
			"namespace": "default",
		},
		"spec": map[string]any{"replicas": int64(1)},
	})

	zs, err := p.convertDeltaToZSet(kobject.Delta{Type: kobject.Added, Object: obj})
	if err != nil {
		t.Fatalf("add failed: %v", err)
	}
	if zs.Size() != 1 {
		t.Fatalf("expected one entry on add, got %d", zs.Size())
	}

	updated := kobject.DeepCopy(obj)
	kobject.SetContent(updated, map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      "app",
			"namespace": "default",
		},
		"spec": map[string]any{"replicas": int64(2)},
	})

	zs, err = p.convertDeltaToZSet(kobject.Delta{Type: kobject.Updated, Object: updated})
	if err != nil {
		t.Fatalf("update failed: %v", err)
	}
	if zs.Size() != 2 {
		t.Fatalf("expected delete+add on update, got size=%d", zs.Size())
	}

	zs, err = p.convertDeltaToZSet(kobject.Delta{Type: kobject.Deleted, Object: updated})
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if zs.Size() != 1 {
		t.Fatalf("expected one entry on delete, got %d", zs.Size())
	}
}

func TestConvertDeltaToZSetNoopSuppression(t *testing.T) {
	p := &Producer{sourceCache: map[schema.GroupVersionKind]*store.Store{}}

	obj := kobject.New()
	gvk := schema.GroupVersionKind{Group: "", Version: "v1", Kind: "ConfigMap"}
	obj.SetGroupVersionKind(gvk)
	obj.SetNamespace("default")
	obj.SetName("cfg")
	kobject.SetContent(obj, map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      "cfg",
			"namespace": "default",
		},
		"data": map[string]any{"a": "1"},
	})

	if _, err := p.convertDeltaToZSet(kobject.Delta{Type: kobject.Added, Object: obj}); err != nil {
		t.Fatalf("initial add failed: %v", err)
	}

	zs, err := p.convertDeltaToZSet(kobject.Delta{Type: kobject.Updated, Object: kobject.DeepCopy(obj)})
	if err != nil {
		t.Fatalf("noop update failed: %v", err)
	}
	if !zs.IsZero() {
		t.Fatalf("expected zero zset for noop update, got %s", zs.String())
	}
}
