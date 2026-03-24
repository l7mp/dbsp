package object

import (
	"encoding/json"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

var scheme = runtime.NewScheme()

// Dump converts an object into a compact, deterministic, human-readable form.
func Dump(obj Object) string {
	if obj == nil {
		return "<nil>"
	}
	return DumpContent(obj.UnstructuredContent())
}

// DumpContent converts an object content map into a compact,
// deterministic, human-readable form.
//
// It strips metadata fields that are typically noisy in logs and unstable across
// reconciliations (for example managedFields, resourceVersion, uid).
func DumpContent(content map[string]any) string {
	if content == nil {
		return "null"
	}

	obj := runtime.DeepCopyJSON(content)

	meta := obj["metadata"]
	if m, ok := meta.(map[string]any); ok {
		delete(m, "creationTimestamp")
		delete(m, "deletionTimestamp")
		delete(m, "generation")
		delete(m, "managedFields")
		delete(m, "selfLink")
		delete(m, "uid")

		if anns, ok := m["annotations"].(map[string]any); ok {
			delete(anns, "kubectl.kubernetes.io/last-applied-configuration")
			if len(anns) == 0 {
				delete(m, "annotations")
			}
		}

		if len(m) == 0 {
			delete(obj, "metadata")
		}
	}

	b, err := json.Marshal(obj)
	if err != nil {
		return fmt.Sprintf("%#v", obj)
	}
	return string(b)
}

// ConvertRuntimeObjectToClientObject converts a core runtime objects into a full client.Object.
func ConvertRuntimeObjectToClientObject(runtimeObj runtime.Object) (client.Object, error) {
	// Try direct type assertion first
	if clientObj, ok := runtimeObj.(client.Object); ok {
		return clientObj, nil
	}

	// Get the GVK for the runtime.Object
	gvk, err := apiutil.GVKForObject(runtimeObj, scheme)
	if err != nil {
		return nil, fmt.Errorf("failed to get GVK: %w", err)
	}

	// Create a new object of the correct type
	newObj, err := scheme.New(gvk)
	if err != nil {
		return nil, fmt.Errorf("failed to create new object: %w", err)
	}

	// Convert the runtime.Object to the new object
	if err := scheme.Convert(runtimeObj, newObj, nil); err != nil {
		return nil, fmt.Errorf("failed to convert object: %w", err)
	}

	// Assert the new object as client.Object
	clientObj, ok := newObj.(client.Object)
	if !ok {
		return nil, fmt.Errorf("converted object is not a client.Object")
	}

	// Copy metadata if the original object implements metav1.Object
	if metaObj, ok := runtimeObj.(metav1.Object); ok {
		clientObj.SetName(metaObj.GetName())
		clientObj.SetNamespace(metaObj.GetNamespace())
		clientObj.SetLabels(metaObj.GetLabels())
		clientObj.SetAnnotations(metaObj.GetAnnotations())
		clientObj.SetResourceVersion(metaObj.GetResourceVersion())
		clientObj.SetUID(metaObj.GetUID())
	}

	return clientObj, nil
}

// GetBaseScheme returns a base scheme. Used mostly for testing.
func GetBaseScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	corev1.AddToScheme(scheme) //nolint:errcheck
	appsv1.AddToScheme(scheme) //nolint:errcheck
	return scheme
}

// DeepCopyAny clones an arbitrary value.
func DeepCopyAny(value any) any {
	switch v := value.(type) {
	case bool, int64, float64, string:
		return v
	case []any:
		newList := make([]any, len(v))
		for i, item := range v {
			newList[i] = DeepCopyAny(item)
		}
		return newList
	case map[string]any:
		newMap := make(map[string]any)
		for k, item := range v {
			newMap[k] = DeepCopyAny(item)
		}
		return newMap
	default:
		return v
	}
}

// MergeAny merges two arbitrary values.
func MergeAny(a, b any) (any, error) {
	if a == nil {
		return b, nil
	}
	switch vb := b.(type) {
	case bool, int64, float64, string:
		return b, nil
	case []any:
		if va, ok := a.([]any); ok {
			return append(va, vb...), nil
		}
		return vb, nil
	case map[string]any:
		if va, ok := a.(map[string]any); ok {
			ret := DeepCopyAny(va).(map[string]any)
			for k, mva := range va {
				if mvb, ok := vb[k]; ok {
					x, err := MergeAny(mva, mvb)
					if err != nil {
						return nil, err
					}
					ret[k] = x
				} else {
					ret[k] = mva
				}
			}

			for k, mvb := range vb {
				if _, ok := va[k]; !ok {
					ret[k] = mvb
				}
			}
			return ret, nil
		}
		return vb, nil
	}
	return nil, fmt.Errorf("could not merge argument %q and %q", a, b)
}
