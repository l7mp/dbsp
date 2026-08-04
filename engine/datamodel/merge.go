package datamodel

import (
	"fmt"
)

// CreateMergePatch returns the RFC 7386 merge patch that transforms old
// into new: fields whose value changed carry the new value, fields present
// in old and absent from new are removed, everything else is absent.
// Arrays are atomic values (no schema means no merge keys), so any element
// change replaces the whole array. Maps merge, so removal recurses: a
// vanished map-valued field nulls its leaves rather than the map, because
// the map at the target may hold entries other writers own. An explicit
// null in new means removal, exactly as RFC 7386 defines it. Both sides
// are normalized through JSON serialization first, so numeric types
// compare by value. An empty result means the documents are equal and
// there is nothing to patch.
func CreateMergePatch(old, new map[string]any) (map[string]any, error) {
	oldNorm, err := Normalize(old)
	if err != nil {
		return nil, fmt.Errorf("old document: %w", err)
	}
	newNorm, err := Normalize(new)
	if err != nil {
		return nil, fmt.Errorf("new document: %w", err)
	}
	return diffMaps(oldNorm, newNorm), nil
}

func diffMaps(old, new map[string]any) map[string]any {
	out := map[string]any{}
	for k, nv := range new {
		ov, ok := old[k]
		if !ok {
			// A null on a field the old document lacks is a no-op removal.
			if nv != nil {
				out[k] = nv
			}
			continue
		}
		om, oIsMap := ov.(map[string]any)
		nm, nIsMap := nv.(map[string]any)
		if oIsMap && nIsMap {
			if sub := diffMaps(om, nm); len(sub) > 0 {
				out[k] = sub
			}
			continue
		}
		if !DeepEqual(ov, nv) {
			out[k] = nv
		}
	}
	for k, ov := range old {
		if _, ok := new[k]; !ok {
			out[k] = removal(ov)
		}
	}
	return out
}

// removal builds the merge-patch entry that removes a value: a null for
// scalars and arrays, a recursive leaf-nulling for maps.
func removal(v any) any {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	out := map[string]any{}
	for k, mv := range m {
		out[k] = removal(mv)
	}
	return out
}
