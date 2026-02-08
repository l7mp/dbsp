// Package zset implements Z-sets (weighted multisets) for DBSP.
package zset

// Element is the interface that Z-set elements must implement.
type Element interface {
	// Key returns a comparable identifier for equality checking.
	// Two elements are equal iff their keys are equal.
	Key() any
}

// Weight is multiplicity in a Z-set.
type Weight int64

// ZSet is a Z-set over Elements.
type ZSet struct {
	entries map[any]*entry
}

type entry struct {
	elem   Element
	weight Weight
}

// New creates an empty Z-set.
func New() ZSet {
	return ZSet{entries: make(map[any]*entry)}
}

// Insert adds an element with given weight.
// Weights are summed; zero-weight entries are removed.
func (z ZSet) Insert(elem Element, weight Weight) {
	if weight == 0 {
		return
	}
	key := elem.Key()
	if e, exists := z.entries[key]; exists {
		e.weight += weight
		if e.weight == 0 {
			delete(z.entries, key)
		}
	} else {
		z.entries[key] = &entry{elem: elem, weight: weight}
	}
}

// Lookup returns the weight for an element (0 if absent).
func (z ZSet) Lookup(elem Element) Weight {
	if e, exists := z.entries[elem.Key()]; exists {
		return e.weight
	}
	return 0
}

// LookupByKey returns the weight for a key (0 if absent).
func (z ZSet) LookupByKey(key any) Weight {
	if e, exists := z.entries[key]; exists {
		return e.weight
	}
	return 0
}

// Iter iterates over all (element, weight) pairs.
func (z ZSet) Iter(fn func(elem Element, weight Weight) bool) {
	for _, e := range z.entries {
		if !fn(e.elem, e.weight) {
			return
		}
	}
}

// Add returns z + other.
func (z ZSet) Add(other ZSet) ZSet {
	result := z.Clone()
	other.Iter(func(elem Element, weight Weight) bool {
		result.Insert(elem, weight)
		return true
	})
	return result
}

// Negate returns -z.
func (z ZSet) Negate() ZSet {
	result := New()
	for key, e := range z.entries {
		result.entries[key] = &entry{elem: e.elem, weight: -e.weight}
	}
	return result
}

// Subtract returns z - other.
func (z ZSet) Subtract(other ZSet) ZSet {
	return z.Add(other.Negate())
}

// Clone creates a copy.
func (z ZSet) Clone() ZSet {
	result := New()
	for key, e := range z.entries {
		result.entries[key] = &entry{elem: e.elem, weight: e.weight}
	}
	return result
}

// IsZero returns true if empty.
func (z ZSet) IsZero() bool {
	return len(z.entries) == 0
}

// Equal compares two Z-sets.
func (z ZSet) Equal(other ZSet) bool {
	if len(z.entries) != len(other.entries) {
		return false
	}
	for key, e := range z.entries {
		if oe, exists := other.entries[key]; !exists || oe.weight != e.weight {
			return false
		}
	}
	return true
}

// Size returns the number of distinct elements.
func (z ZSet) Size() int {
	return len(z.entries)
}

// Entries returns a slice of all entries for inspection (testing).
func (z ZSet) Entries() []struct {
	Elem   Element
	Weight Weight
} {
	result := make([]struct {
		Elem   Element
		Weight Weight
	}, 0, len(z.entries))
	for _, e := range z.entries {
		result = append(result, struct {
			Elem   Element
			Weight Weight
		}{Elem: e.elem, Weight: e.weight})
	}
	return result
}
