package transform

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/l7mp/dbsp/engine/circuit"
	dbspexpr "github.com/l7mp/dbsp/engine/expression/dbsp"
)

// TransformSpec is the serialized form of one transform entry, the shape
// carried by circuit.transform() in JavaScript and by the transforms list
// of a serialized controller (engine/spec):
//
//	{"name": "Incrementalizer"}
//	{"name": "Reconciler", "pairs": [["observed", "status"], ...]}
//	{"name": "SmithPredictor", "pairs": [...], "k": 2}
//	{"name": "Distincter", "key": <expression>}
//
// Pairs name topics (or input_/output_ node IDs; bare names are
// canonicalized), the SmithPredictor's k is its dead time in circuit
// steps, and the optional Distincter key turns the plain distinct into
// distinct_pi.
type TransformSpec struct {
	Name  string          `json:"name"`
	Pairs [][]string      `json:"pairs,omitempty"`
	K     int             `json:"k,omitempty"`
	Key   json.RawMessage `json:"key,omitempty"`
}

// Spec parses the wire entry into the Spec accepted by New and NewChain.
func (ts TransformSpec) Spec() (Spec, error) {
	name := strings.TrimSpace(ts.Name)
	if name == "" {
		return Spec{}, fmt.Errorf("transform: missing transformer name")
	}
	typ := TransformerType(name)

	var args []any
	switch typ {
	case Incrementalizer:
	case Distincter:
		if len(ts.Key) > 0 && string(ts.Key) != "null" {
			key, err := dbspexpr.Compile(ts.Key)
			if err != nil {
				return Spec{}, fmt.Errorf("transform %s: key: %w", typ, err)
			}
			args = append(args, key)
		}
	case Reconciler:
		if len(ts.Pairs) > 0 {
			pairs, err := parseSpecPairs(typ, ts.Pairs)
			if err != nil {
				return Spec{}, err
			}
			args = append(args, pairs)
		}
	case SmithPredictor:
		if len(ts.Pairs) > 0 {
			pairs, err := parseSpecPairs(typ, ts.Pairs)
			if err != nil {
				return Spec{}, err
			}
			args = append(args, pairs)
		}
		args = append(args, ts.K)
	case Rewriter:
		return Spec{}, fmt.Errorf("transform %s: not a user-facing transform", typ)
	default:
		return Spec{}, fmt.Errorf("unknown transformer: %q", typ)
	}
	return Spec{Type: typ, Args: args}, nil
}

// NewChainFromSpecs parses a list of wire entries and returns the Chain
// applying them in canonical order.
func NewChainFromSpecs(entries []TransformSpec) (*Chain, error) {
	specs := make([]Spec, 0, len(entries))
	for i, e := range entries {
		s, err := e.Spec()
		if err != nil {
			return nil, fmt.Errorf("transform entry %d: %w", i, err)
		}
		specs = append(specs, s)
	}
	return NewChain(specs...)
}

// parseSpecPairs converts [input, output] topic-name pairs into
// ReconcilerPairs with canonical node IDs.
func parseSpecPairs(typ TransformerType, raw [][]string) ([]ReconcilerPair, error) {
	pairs := make([]ReconcilerPair, 0, len(raw))
	for i, p := range raw {
		if len(p) != 2 {
			return nil, fmt.Errorf("transform %s: pair %d must have exactly 2 elements", typ, i)
		}
		inputID := strings.TrimSpace(p[0])
		outputID := strings.TrimSpace(p[1])
		if inputID == "" || outputID == "" {
			return nil, fmt.Errorf("transform %s: pair %d must not contain empty values", typ, i)
		}
		if !strings.HasPrefix(inputID, "input_") {
			inputID = circuit.InputNodeID(inputID)
		}
		if !strings.HasPrefix(outputID, "output_") {
			outputID = circuit.OutputNodeID(outputID)
		}
		pairs = append(pairs, ReconcilerPair{InputID: inputID, OutputID: outputID})
	}
	return pairs, nil
}
