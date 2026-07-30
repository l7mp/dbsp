package object

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
)

// LastAppliedConfigAnnotation is kubectl's copy of the last applied object.
const LastAppliedConfigAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

// The default API-field converter table.
//
// Kubernetes objects carry fields that belong to the API machinery rather than to the
// controller. This table decides what the connector does with them; every reader and writer
// consults it, so the rules cannot drift apart. There is a third class the table cannot express:
// fields the connector must write but must not compute inside a circuit, because the value is a
// clock read. Those are bound at the write boundary instead, and today the only one is the
// lastTransitionTime of a status condition.
var apiFields = []apiField{
	// Server-side-apply bookkeeping: rewritten by every writer, large, and
	// never data.
	{path: []string{"metadata", "managedFields"}, ingest: ingestDrop},

	// Removed from the API in 1.20; whatever still arrives is vestigial.
	{path: []string{"metadata", "selfLink"}, ingest: ingestDrop},

	// An opaque store cursor. It bumps on every write to the object by
	// anyone, including writes that touch nothing the pipeline reads, so
	// keeping it would turn every foreign write into a delta. A pipeline
	// that needs a content-change token (the "restart the Deployment when
	// its ConfigMap changes" idiom) takes @hash of the content it cares
	// about: that changes when the content changes, and not before.
	{path: []string{"metadata", "resourceVersion"}, ingest: ingestDrop},

	// kubectl bookkeeping: a serialized copy of the whole object, rewritten
	// on every apply.
	{path: []string{"metadata", "annotations", LastAppliedConfigAnnotation}, ingest: ingestDrop},

	// Stable identity, and the only way to build an ownerReference. View
	// UIDs are derived from the object identity (see WithUID), so in a view
	// document they are redundant.
	{path: []string{"metadata", "uid"}, ingest: ingestKeepNative},

	// Bumped only when the spec changes, so it is data: it is the input to
	// the status.observedGeneration convention.
	{path: []string{"metadata", "generation"}, ingest: ingestKeep},

	// Set once, at creation. Object age is legitimate input.
	{path: []string{"metadata", "creationTimestamp"}, ingest: ingestKeep},

	// Set once, when deletion starts. The signal a finalizing controller
	// waits for.
	{path: []string{"metadata", "deletionTimestamp"}, ingest: ingestKeep},
}

// ingestPolicy says whether a field survives into pipeline documents.
type ingestPolicy int

const (
	// ingestDrop: the field never reaches a document.
	ingestDrop ingestPolicy = iota
	// ingestKeep: the field reaches documents unchanged.
	ingestKeep
	// ingestKeepNative: kept for native objects, dropped for view objects.
	ingestKeepNative
)

// apiField is one API-machinery field and the connector's policy for it.
type apiField struct {
	path   []string
	ingest ingestPolicy
}

// StripOnIngest removes the API fields that must not reach pipeline
// documents. The view/native distinction is read from the content's own
// apiVersion and kind. The map is modified in place.
func StripOnIngest(content map[string]any) {
	apiVersion, _ := content["apiVersion"].(string)
	kind, _ := content["kind"].(string)
	isView := viewv1a1.IsViewKind(schema.FromAPIVersionAndKind(apiVersion, kind))

	for _, f := range apiFields {
		if f.ingest == ingestDrop || (f.ingest == ingestKeepNative && isView) {
			unstructured.RemoveNestedField(content, f.path...)
		}
	}
	dropEmptyAnnotations(content)
}

// StripOnWrite removes every API field: no write the connector issues may
// carry one, and no comparison of a desired object against an observed one
// may count one as a difference. The map is modified in place.
func StripOnWrite(content map[string]any) {
	for _, f := range apiFields {
		unstructured.RemoveNestedField(content, f.path...)
	}
	dropEmptyAnnotations(content)
}

// dropEmptyAnnotations removes an annotation map emptied by the stripping,
// so that an object that had only a stripped annotation compares equal to
// one that never had any.
func dropEmptyAnnotations(content map[string]any) {
	meta, ok := content["metadata"].(map[string]any)
	if !ok {
		return
	}
	if anns, ok := meta["annotations"].(map[string]any); ok && len(anns) == 0 {
		delete(meta, "annotations")
	}
}
