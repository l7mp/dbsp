package zset

import (
	"fmt"
	"sort"

	"github.com/l7mp/dbsp/engine/datamodel"
)

// KeyFunc derives the plant identity of a document: the object a write would address (GVK plus
// namespace/name for Kubernetes, type URL plus resource name for xDS, etc).
type KeyFunc func(doc datamodel.Document) (string, error)

// Pair is the effective change for one plant object. Old is the document
// the pipeline retracted (nil when the key is newly asserted), New the one
// it asserted (nil when the key is retracted).
type Pair struct {
	Key string
	Old datamodel.Document
	New datamodel.Document
}

// KeyError reports a key whose delta cannot be folded into a Pair. Key is
// empty when the document could not be keyed at all.
type KeyError struct {
	Key string
	Err error
}

// Error implements the error interface.
func (e KeyError) Error() string {
	if e.Key == "" {
		return e.Err.Error()
	}
	return fmt.Sprintf("object %q: %s", e.Key, e.Err)
}

// Fold groups a Z-set delta by plant key and derives one Pair per key, in
// deterministic key order. Entries with the same content hash net out
// inside the Z-set itself, so per key the delta must hold at most one
// retraction and one assertion, each with unit weight; keys violating the
// contract are omitted from the result and reported in the errors.
func Fold(z ZSet, key KeyFunc) ([]Pair, []KeyError) {
	type group struct {
		neg, pos []Elem
	}
	groups := map[string]*group{}
	var errs []KeyError

	for _, e := range z.Entries() {
		k, err := key(e.Document)
		if err != nil {
			errs = append(errs, KeyError{Err: err})
			continue
		}
		if k == "" {
			continue
		}
		g, ok := groups[k]
		if !ok {
			g = &group{}
			groups[k] = g
		}
		if e.Weight < 0 {
			g.neg = append(g.neg, e)
		} else {
			g.pos = append(g.pos, e)
		}
	}

	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	pairs := make([]Pair, 0, len(keys))
	for _, k := range keys {
		g := groups[k]
		if err := checkFunctional(g.neg, g.pos); err != nil {
			errs = append(errs, KeyError{Key: k, Err: err})
			continue
		}
		p := Pair{Key: k}
		if len(g.neg) == 1 {
			p.Old = g.neg[0].Document
		}
		if len(g.pos) == 1 {
			p.New = g.pos[0].Document
		}
		pairs = append(pairs, p)
	}

	return pairs, errs
}

func checkFunctional(neg, pos []Elem) error {
	if len(neg) > 1 {
		return fmt.Errorf("%d retracted documents in one delta", len(neg))
	}
	if len(pos) > 1 {
		return fmt.Errorf("%d asserted documents in one delta", len(pos))
	}
	if len(neg) == 1 && neg[0].Weight != -1 {
		return fmt.Errorf("retracted document carries weight %d", neg[0].Weight)
	}
	if len(pos) == 1 && pos[0].Weight != 1 {
		return fmt.Errorf("asserted document carries weight %d", pos[0].Weight)
	}
	return nil
}
