package object

import (
	"fmt"

	"github.com/l7mp/dbsp/engine/datamodel"
	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
)

// Converter translates between Kubernetes objects and pipeline documents.
// It is the connector's only crossing of that boundary: producers turn every
// observed object into a document through ToDocument, consumers turn every
// document they are about to write into an object through ToObject.
type Converter interface {
	// ToDocument converts an observed object into a pipeline document.
	ToDocument(obj Object) datamodel.Document

	// ToObject converts a document into an object ready to be written.
	ToObject(doc datamodel.Document) (Object, error)
}

// TableConverter is the connector's default Converter: it canonicalizes in
// both directions with the API-field table, so a document carries what the
// pipeline may see and a write carries what the API server accepts.
type TableConverter struct{}

// DefaultConverter is the converter used when a producer or a consumer is
// configured without one.
var DefaultConverter Converter = TableConverter{}

var _ Converter = TableConverter{}

// ToDocument converts an observed object into a pipeline document.
func (TableConverter) ToDocument(obj Object) datamodel.Document {
	content := datamodel.DeepCopyAny(obj.UnstructuredContent()).(map[string]any)
	StripOnIngest(content)

	return dbspunstructured.New(content)
}

// ToObject converts a document into an object ready to be written.
func (TableConverter) ToObject(doc datamodel.Document) (Object, error) {
	udoc, ok := doc.(*dbspunstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("unsupported document type %T", doc)
	}

	// Fields returns a copy, so the stripping cannot reach the document.
	content := udoc.Fields()
	StripOnWrite(content)

	obj := New()
	obj.SetUnstructuredContent(content)

	return obj, nil
}
