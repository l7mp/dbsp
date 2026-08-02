package xds

import (
	"encoding/json"
	"fmt"

	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/zset"
)

// decodeDocument maps one pipeline document to its typed xDS resource and the
// resource's self-assigned name. The document is shaped exactly like the xDS
// resource (no metadata/spec envelope): it is marshaled to JSON and parsed with
// protojson, which handles enums, wrappers, oneof, and Any (via @type) that
// plain encoding/json mishandles. Parsing is strict: an unknown or malformed
// field surfaces as an error.
func decodeDocument(entry typeEntry, doc datamodel.Document) (types.Resource, string, error) {
	b, err := json.Marshal(doc)
	if err != nil {
		return nil, "", fmt.Errorf("xds: marshal document: %w", err)
	}

	msg := entry.new()
	if err := protojson.Unmarshal(b, msg); err != nil {
		return nil, "", fmt.Errorf("xds: protojson into %s: %w", entry.url, err)
	}

	name := cache.GetResourceName(msg)
	if name == "" {
		return nil, "", fmt.Errorf("xds: %s resource has no name", entry.url)
	}
	return msg, name, nil
}

// resourceOps is the effective set of cache mutations for one Z-set delta:
// resources to upsert keyed by name, and names to delete.
type resourceOps struct {
	upserts map[string]types.Resource
	deletes []string
}

// classify nets a Z-set delta into resource operations through the keyed fold
// (zset.Fold, the same netting the Kubernetes write path applies), keyed by
// the resource's self-assigned name. Per name the fold yields one effective
// op: an asserted document is upserted as its typed resource, a bare
// retraction deletes the name. Keys violating the functional-delta contract
// or failing to decode are omitted from the result and reported. The result
// is deterministic: names are visited in sorted order.
func classify(entry typeEntry, data zset.ZSet) (resourceOps, []zset.KeyError) {
	pairs, kerrs := zset.Fold(data, func(doc datamodel.Document) (string, error) {
		_, name, err := decodeDocument(entry, doc)
		return name, err
	})

	ops := resourceOps{upserts: map[string]types.Resource{}}
	for _, p := range pairs {
		if p.New == nil {
			ops.deletes = append(ops.deletes, p.Key)
			continue
		}
		res, _, err := decodeDocument(entry, p.New)
		if err != nil {
			kerrs = append(kerrs, zset.KeyError{Key: p.Key, Err: err})
			continue
		}
		ops.upserts[p.Key] = res
	}
	return ops, kerrs
}

// snapshot nets a Z-set into the full desired resource set keyed by name, for
// a Setter's SetResources: the fold's assertions. Retracted-only names are
// simply absent (deletion is by omission). The returned map is never nil, so
// an empty event correctly clears the type.
func snapshot(entry typeEntry, data zset.ZSet) (map[string]types.Resource, []zset.KeyError) {
	ops, kerrs := classify(entry, data)
	return ops.upserts, kerrs
}

// encodeResource maps a typed xDS resource back to a free-form document, the
// inverse of decodeDocument: the proto is marshaled with protojson (camelCase,
// enum strings, wrappers, Any via @type) and wrapped as an Unstructured whose
// fields mirror the resource exactly (no metadata/spec envelope). Used by the
// ingest producers to turn upstream resources into DBSP inputs.
func encodeResource(res proto.Message) (*unstructured.Unstructured, error) {
	b, err := protojson.Marshal(res)
	if err != nil {
		return nil, fmt.Errorf("xds: protojson marshal: %w", err)
	}

	var fields map[string]any
	if err := json.Unmarshal(b, &fields); err != nil {
		return nil, fmt.Errorf("xds: unmarshal resource to document: %w", err)
	}

	return unstructured.New(fields), nil
}

// anyToDoc unmarshals an xDS resource carried as an Any (as served on the wire)
// into its document form and the resource's self-assigned name. The proto types
// must be linked into the binary for the registry lookup to resolve.
func anyToDoc(any *anypb.Any) (*unstructured.Unstructured, string, error) {
	msg, err := any.UnmarshalNew()
	if err != nil {
		return nil, "", fmt.Errorf("xds: unmarshal resource: %w", err)
	}
	doc, err := encodeResource(msg)
	if err != nil {
		return nil, "", err
	}
	return doc, cache.GetResourceName(msg), nil
}
