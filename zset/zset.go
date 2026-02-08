// Package zset implements Z-sets (weighted multisets) for DBSP.
package zset

// Document is the interface that Z-set elements must implement.
type Document interface {
	// Key returns a string identifier for equality checking.  Two elements are equal iff their
	// keys are equal.  This is based on full content (like a hash of all fields).
	Key() string

	// PrimaryKey returns the primary key for the element, like in SQL.  Multiple elements with
	// different Key() values may share the same PrimaryKey().  Returns an error if the primary
	// key is unavailable (e.g., lost during schemaless processing).
	PrimaryKey() (string, error)
}

// Weight is multiplicity in a Z-set.
type Weight int64

// ZSet is a Z-set over Elements. ZSet is not thread-safe.
type ZSet struct {
	entries map[string]*Value
}

type Value struct {
	Document Document
	Weight   Weight
}

// New creates an empty Z-set.
func New() ZSet {
	return ZSet{entries: make(map[string]*Value)}
}

// Insert adds an element with given weight.
// Weights are summed; zero-weight entries are removed.
func (z ZSet) Insert(elem Document, weight Weight) {
	if weight == 0 {
		return
	}
	key := elem.Key()
	if e, exists := z.entries[key]; exists {
		e.Weight += weight
		if e.Weight == 0 {
			delete(z.entries, key)
		}
	} else {
		z.entries[key] = &Value{Document: elem, Weight: weight}
	}
}

// LookupByKey returns the value for a key and a boolean to indicate whether the key is present in
// the zset.
func (z ZSet) LookupByKey(key string) (v *Value, ok bool) {
	v, ok = z.entries[key]
	return
}

// Lookup returns the weight for an element (0 if absent).
func (z ZSet) Lookup(elem Document) Weight {
	if e, exists := z.entries[elem.Key()]; exists {
		return e.Weight
	}
	return 0
}

// Iter iterates over all (element, weight) pairs.
func (z ZSet) Iter(fn func(elem Document, weight Weight) bool) {
	for _, e := range z.entries {
		if !fn(e.Document, e.Weight) {
			return
		}
	}
}

// Add returns z + other.
func (z ZSet) Add(other ZSet) ZSet {
	result := z.Clone()
	other.Iter(func(elem Document, weight Weight) bool {
		result.Insert(elem, weight)
		return true
	})
	return result
}

// Negate returns -z.
func (z ZSet) Negate() ZSet {
	result := New()
	for key, e := range z.entries {
		result.entries[key] = &Value{Document: e.Document, Weight: -e.Weight}
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
		result.entries[key] = &Value{Document: e.Document, Weight: e.Weight}
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
		if oe, exists := other.entries[key]; !exists || oe.Weight != e.Weight {
			return false
		}
	}
	return true
}

// Size returns the number of distinct elements.
func (z ZSet) Size() int {
	return len(z.entries)
}

// Entries returns a slice of all entries for inspection.
func (z ZSet) Entries() []Value {
	result := make([]Value, 0, len(z.entries))
	for _, e := range z.entries {
		result = append(result, Value{Document: e.Document, Weight: e.Weight})
	}
	return result
}
