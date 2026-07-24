package js

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dop251/goja"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/compiler"
	"github.com/l7mp/dbsp/engine/operator"
)

// circuitCreate implements the circuit.create(name) global: it returns a fresh,
// empty circuit handle for hand construction. Nodes and edges are added with
// .node()/.input()/.output()/.edge(), and .commit() installs the finished
// circuit into the runtime, as it does for a compiled one. A half-built
// circuit is not well-formed, so committing (or validating) mid-build is an
// error rather than a way to check progress.
func (v *VM) circuitCreate(call goja.FunctionCall) (goja.Value, error) {
	name := "circuit"
	if len(call.Arguments) > 0 && !goja.IsUndefined(call.Argument(0)) && !goja.IsNull(call.Argument(0)) {
		if n := strings.TrimSpace(call.Argument(0).String()); n != "" {
			name = n
		}
	}
	c := circuit.New(name)
	q := &compiler.Query{
		Circuit:          c,
		InputMap:         map[string]string{},
		OutputMap:        map[string]string{},
		InputLogicalMap:  map[string]string{},
		OutputLogicalMap: map[string]string{},
	}
	h := &circuitHandle{c: c, query: q, vm: v}
	return h.jsObject(), nil
}

// addNode adds an operator node built from a spec, returning its ID. When id is
// empty an ID is generated from the operator kind and a per-handle counter.
// Every node, structural (input/output/delay), primitive (plus/minus/negate),
// or relational (project/select/group_by), flows through the one operator
// unmarshaler, so there are no per-operator builder methods; see parseOpSpec.
func (h *circuitHandle) addNode(spec any, id string) (string, error) {
	op, err := parseOpSpec(spec)
	if err != nil {
		return "", err
	}
	if id == "" {
		id = fmt.Sprintf("%s_%d", op.Kind().String(), h.seq)
		h.seq++
	}
	if err := h.c.AddNode(circuit.Op(id, op)); err != nil {
		return "", err
	}
	return id, nil
}

// addBoundary adds an input or output node bound to an external topic, recording
// the topic-to-node mapping the runtime uses to wire pub/sub. Returns the node
// ID.
func (h *circuitHandle) addBoundary(topic string, isInput bool) (string, error) {
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return "", fmt.Errorf("circuit boundary requires a topic name")
	}
	if isInput {
		id := circuit.InputNodeID(topic)
		if err := h.c.AddNode(circuit.Input(id)); err != nil {
			return "", err
		}
		h.query.InputMap[topic] = id
		return id, nil
	}
	id := circuit.OutputNodeID(topic)
	if err := h.c.AddNode(circuit.Output(id)); err != nil {
		return "", err
	}
	h.query.OutputMap[topic] = id
	return id, nil
}

// addEdge connects two nodes at the given input port of the target.
func (h *circuitHandle) addEdge(from, to string, port int) error {
	from = strings.TrimSpace(from)
	to = strings.TrimSpace(to)
	if from == "" || to == "" {
		return fmt.Errorf("circuit.edge requires non-empty node IDs")
	}
	return h.c.AddEdge(circuit.NewEdge(from, to, port))
}

// opAliases maps friendly operator names to the canonical wire types understood
// by operator.UnmarshalOperator. Names that already match a wire type (project,
// select, plus, delay, ...) need no entry; only camel-cased or shorthand names
// do. A leading "@" is stripped before lookup, so "@groupBy" and "group_by"
// both resolve.
var opAliases = map[string]string{
	"groupBy":             "group_by",
	"groupByIncremental":  "group_by_incremental",
	"linearCombination":   "linear_combination",
	"equiJoin":            "equi_join",
	"equijoin":            "equi_join",
	"equiJoinIncremental": "equi_join_incremental",
	"join":                "cartesian",
	"product":             "cartesian",
	"diff":                "differentiate",
	"int":                 "integrate",
	"delta":               "delta0",
}

// opArgFields names the single wire field the ":arg" shorthand fills for each
// single-argument operator. Multi-field operators (group_by, equi_join) take an
// object argument whose keys merge into the wire form instead.
var opArgFields = map[string]string{
	"project":            "projection",
	"select":             "predicate",
	"unwind":             "field",
	"linear_combination": "coeffs",
}

// parseOpSpec builds an operator from a node spec. Two forms are accepted:
//
//   - a wire object, e.g. { type: "project", projection: {...} }, passed
//     straight to operator.UnmarshalOperator;
//   - a string "op[:arg]", e.g. "plus", "delay", "@project:{...}",
//     "linear_combination:[1,1]", "unwind:$.items". The name (minus any "@")
//     resolves through opAliases to a wire type; the ":arg" JSON, when present,
//     fills the operator's argument field (opArgFields) or, for multi-field
//     operators, merges its object keys into the wire form.
//
// Routing every node through the one unmarshaler is what lets the builder carry
// arbitrary operators without a special constructor per kind.
func parseOpSpec(spec any) (operator.Operator, error) {
	if s, ok := spec.(string); ok {
		return parseOpSpecString(s)
	}
	b, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("node spec must be a string or object: %w", err)
	}
	op, err := operator.UnmarshalOperator(b)
	if err != nil {
		return nil, fmt.Errorf("node spec: %w", err)
	}
	return op, nil
}

func parseOpSpecString(s string) (operator.Operator, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("empty node spec")
	}
	head, rest, hasArgs := strings.Cut(s, ":")
	head = strings.TrimPrefix(strings.TrimSpace(head), "@")
	if head == "" {
		return nil, fmt.Errorf("node spec %q: missing operator name", s)
	}
	wireType := head
	if alias, ok := opAliases[head]; ok {
		wireType = alias
	}

	wire := map[string]any{"type": wireType}
	if hasArgs {
		rest = strings.TrimSpace(rest)
		var arg any
		if err := json.Unmarshal([]byte(rest), &arg); err != nil {
			// A bare, unquoted string argument, e.g. "unwind:$.items".
			arg = rest
		}
		switch {
		case opArgFields[wireType] != "":
			wire[opArgFields[wireType]] = arg
		case wireType == "group_by" || wireType == "group_by_incremental":
			if a, ok := arg.([]any); ok && len(a) == 2 {
				wire["keyExpr"], wire["valueExpr"] = a[0], a[1]
			} else if m, ok := arg.(map[string]any); ok {
				for k, v := range m {
					wire[k] = v
				}
			} else {
				return nil, fmt.Errorf("operator %q expects [keyExpr, valueExpr] or an object argument", wireType)
			}
		default:
			m, ok := arg.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("operator %q takes an object argument, got %q", wireType, rest)
			}
			for k, v := range m {
				wire[k] = v
			}
		}
	}

	b, err := json.Marshal(wire)
	if err != nil {
		return nil, err
	}
	op, err := operator.UnmarshalOperator(b)
	if err != nil {
		return nil, fmt.Errorf("node spec %q: %w", s, err)
	}
	return op, nil
}
