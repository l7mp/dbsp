package producer

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	kobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	"github.com/l7mp/dbsp/connectors/kubernetes/runtime/store"
	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/zset"
)

// convertDeltaToZSet converts a source delta into an input Z-set while maintaining source cache.
// This mirrors the old dcontroller ConvertDeltaToZSet semantics (without pipeline reconciler bits).
func (p *baseProducer) convertDeltaToZSet(delta kobject.Delta) (zset.ZSet, error) {
	deltaObj := kobject.DeepCopy(delta.Object)
	gvk := deltaObj.GetObjectKind().GroupVersionKind()

	if _, ok := p.sourceCache[gvk]; !ok {
		p.sourceCache[gvk] = store.NewStore()
	}

	var old kobject.Object
	if obj, exists, err := p.sourceCache[gvk].Get(deltaObj); err == nil && exists {
		old = obj
	}

	kobject.RemoveUID(deltaObj)

	if old != nil && (delta.Type == kobject.Updated || delta.Type == kobject.Replaced || delta.Type == kobject.Upserted) {
		oldNoUID := kobject.DeepCopy(old)
		kobject.RemoveUID(oldNoUID)
		if kobject.DeepEqual(deltaObj, oldNoUID) {
			p.log.V(5).Info("suppressing no-op delta in convertDeltaToZSet",
				"key", objectKey(deltaObj), "type", delta.Type)
			return zset.New(), nil
		}
	}

	zs := zset.New()
	switch delta.Type {
	case kobject.Added:
		zs.Insert(toDocument(deltaObj), 1)
		if err := p.sourceCache[gvk].Add(deltaObj); err != nil {
			return zset.New(), fmt.Errorf("add object %s to source cache: %w", objectKey(deltaObj), err)
		}

	case kobject.Updated, kobject.Replaced, kobject.Upserted:
		if old != nil {
			zs.Insert(toDocument(old), -1)
		}
		zs.Insert(toDocument(deltaObj), 1)
		if err := p.sourceCache[gvk].Update(deltaObj); err != nil {
			return zset.New(), fmt.Errorf("update object %s in source cache: %w", objectKey(deltaObj), err)
		}

	case kobject.Deleted:
		if old == nil {
			return zset.New(), fmt.Errorf("delete for non-existent object %s", objectKey(deltaObj))
		}
		// Use cached old object for delete tombstones. Kubernetes delete events can
		// carry a final object variant (for example with a newer resourceVersion),
		// which would not cancel the previously added/updated document by hash.
		zs.Insert(toDocument(old), -1)
		if err := p.sourceCache[gvk].Delete(old); err != nil {
			return zset.New(), fmt.Errorf("delete object %s from source cache: %w", objectKey(deltaObj), err)
		}

	default:
		return zset.New(), fmt.Errorf("unknown delta type %q for %s", delta.Type, objectKey(deltaObj))
	}

	return zs, nil
}

func toDocument(obj kobject.Object) *dbspunstructured.Unstructured {
	content := kobject.DeepCopyAny(obj.UnstructuredContent()).(map[string]any)
	unstructured.RemoveNestedField(content, "metadata", "managedFields")
	unstructured.RemoveNestedField(content, "metadata", "generation")

	return dbspunstructured.New(content, nil)
}
