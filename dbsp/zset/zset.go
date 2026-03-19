package zset

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/l7mp/dbsp/dbsp/datamodel"
)

// Weight is multiplicity in a Z-set.
type Weight int64

// ZSet is a Z-set over Elems. ZSet is not thread-safe.
type ZSet struct {
	entries map[string]*Elem
}

type Elem struct {
	Document datamodel.Document
	Weight   Weight
}

type jsonElem struct {
	Elem   json.RawMessage `json:"elem"`
	Weight Weight          `json:"weight"`
}

// New creates an empty Z-set.
func New() ZSet {
	return ZSet{entries: make(map[string]*Elem)}
}

func (z ZSet) WithElems(es ...Elem) ZSet {
	for _, e := range es {
		z.Insert(e.Document, e.Weight)
	}
	return z
}

// Insert adds an element with given weight.
// Weights are summed; zero-weight entries are removed.
func (z ZSet) Insert(elem datamodel.Document, weight Weight) {
	if weight == 0 {
		return
	}
	key := elem.Hash()
	if e, exists := z.entries[key]; exists {
		e.Weight += weight
		if e.Weight == 0 {
			delete(z.entries, key)
		}
	} else {
		z.entries[key] = &Elem{Document: elem, Weight: weight}
	}
}

// Elem returns the element for a key and a boolean to indicate whether the key is present in the
// zset.
func (z ZSet) Elem(key string) (v *Elem, ok bool) {
	v, ok = z.entries[key]
	return
}

// Lookup returns the weight for an element (0 if absent).
func (z ZSet) Lookup(key string) Weight {
	if e, exists := z.entries[key]; exists {
		return e.Weight
	}
	return 0
}

// Iter iterates over all (element, weight) pairs.
func (z ZSet) Iter(fn func(elem datamodel.Document, weight Weight) bool) {
	for _, e := range z.entries {
		if !fn(e.Document, e.Weight) {
			return
		}
	}
}

// Add returns z + other.
func (z ZSet) Add(other ZSet) ZSet {
	result := z.Clone()
	other.Iter(func(elem datamodel.Document, weight Weight) bool {
		result.Insert(elem, weight)
		return true
	})
	return result
}

// Negate returns -z.
func (z ZSet) Negate() ZSet {
	result := New()
	for key, e := range z.entries {
		result.entries[key] = &Elem{Document: e.Document, Weight: -e.Weight}
	}
	return result
}

// Subtract returns z - other.
func (z ZSet) Subtract(other ZSet) ZSet {
	return z.Add(other.Negate())
}

// Scale returns a new Z-set with every weight multiplied by c. Entries whose
// scaled weight is zero are dropped to maintain the Z-set invariant.
func (z ZSet) Scale(c Weight) ZSet {
	result := New()
	for key, e := range z.entries {
		w := e.Weight * c
		if w != 0 {
			result.entries[key] = &Elem{Document: e.Document, Weight: w}
		}
	}
	return result
}

// Clone creates a copy.
func (z ZSet) Clone() ZSet {
	result := New()
	for key, e := range z.entries {
		result.entries[key] = &Elem{Document: e.Document, Weight: e.Weight}
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
func (z ZSet) Entries() []Elem {
	result := make([]Elem, 0, len(z.entries))
	for _, e := range z.entries {
		result = append(result, Elem{Document: e.Document, Weight: e.Weight})
	}
	return result
}

// String returns a deterministic string representation of the Z-set.
func (z ZSet) String() string {
	if len(z.entries) == 0 {
		return "ZSet{}"
	}

	keys := make([]string, 0, len(z.entries))
	for key := range z.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString("ZSet{")
	for i, key := range keys {
		if i > 0 {
			b.WriteString(", ")
		}
		entry := z.entries[key]
		b.WriteString(key)
		b.WriteString(": ")
		b.WriteString(entry.Document.String())
		b.WriteString("@")
		b.WriteString(fmt.Sprintf("%d", entry.Weight))
	}
	b.WriteString("}")
	return b.String()
}

// MarshalJSON implements json.Marshaler.
func (z ZSet) MarshalJSON() ([]byte, error) {
	entries := make([]jsonElem, 0, len(z.entries))
	for _, entry := range z.entries {
		if entry.Document == nil {
			return nil, fmt.Errorf("zset entry document is nil")
		}
		payload, err := entry.Document.MarshalJSON()
		if err != nil {
			return nil, err
		}
		entries = append(entries, jsonElem{Elem: payload, Weight: entry.Weight})
	}
	return json.Marshal(entries)
}

// UnmarshalJSON implements json.Unmarshaler.
func (z *ZSet) UnmarshalJSON(data []byte) error {
	if z == nil {
		return fmt.Errorf("zset must be non-nil for JSON decoding")
	}
	return fmt.Errorf("zset JSON unmarshaling requires a concrete document type")
}
