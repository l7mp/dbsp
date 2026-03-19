package consumer

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"

	dbunstructured "github.com/l7mp/dbsp/dbsp/datamodel/unstructured"
	"github.com/l7mp/dbsp/dbsp/zset"
)

func TestNormalizeResultObject(t *testing.T) {
	g := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}

	input := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      "demo",
			"namespace": "default",
		},
		"spec": map[string]any{"replicas": int64(3)},
	}

	obj, err := toObject(dbunstructured.New(input, nil))
	if err != nil {
		t.Fatalf("toObject failed: %v", err)
	}

	out := normalizeResultObject(obj, g)
	if out == nil {
		t.Fatalf("normalizeResultObject returned nil")
	}
	if out.GetName() != "demo" || out.GetNamespace() != "default" {
		t.Fatalf("unexpected key: %s/%s", out.GetNamespace(), out.GetName())
	}
	if out.GroupVersionKind() != g {
		t.Fatalf("unexpected gvk: %#v", out.GroupVersionKind())
	}
}

func TestNormalizeResultObjectInvalidMetadata(t *testing.T) {
	g := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	obj, err := toObject(dbunstructured.New(map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "spec": map[string]any{"a": 1}}, nil))
	if err != nil {
		t.Fatalf("toObject failed: %v", err)
	}

	if out := normalizeResultObject(obj, g); out != nil {
		t.Fatalf("expected nil, got object")
	}
}

func TestObjectFromElemWeightHandling(t *testing.T) {
	b, err := newBase(Config{TargetGVK: schema.GroupVersionKind{Group: "g", Version: "v1", Kind: "K"}}, "test")
	if err == nil {
		t.Fatalf("expected error for nil client")
	}

	_ = b

	doc := dbunstructured.New(map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "n"}}, nil)
	e := zset.Elem{Document: doc, Weight: -1}

	// Use a lightweight base without New() validation for pure adapter test.
	bc := &baseConsumer{targetGVK: schema.GroupVersionKind{Group: "g", Version: "v1", Kind: "K"}}
	obj, isDelete, err := bc.objectFromElem(e)
	if err != nil {
		t.Fatalf("objectFromElem failed: %v", err)
	}
	if obj == nil {
		t.Fatalf("expected object")
	}
	if !isDelete {
		t.Fatalf("expected delete=true for negative weight")
	}
}
