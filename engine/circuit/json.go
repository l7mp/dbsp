package circuit

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/l7mp/dbsp/engine/operator"
)

type jsonEdge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Port int    `json:"port"`
}

type jsonCircuit struct {
	Name  string                     `json:"name"`
	Nodes map[string]json.RawMessage `json:"nodes"`
	Edges []jsonEdge                 `json:"edges"`
}

// MarshalJSON implements json.Marshaler.
// Each node is serialized as its operator's JSON. Absorb nodes are omitted
// (they are auto-created by AddNode when the delay emit node is added).
// Edge targets pointing at absorb nodes are rewritten back to the emit ID so
// that the round-trip through UnmarshalJSON is correct.
func (c *Circuit) MarshalJSON() ([]byte, error) {
	if c == nil {
		return []byte("null"), nil
	}

	nodes := make(map[string]json.RawMessage, len(c.nodes))
	for id, node := range c.nodes {
		// Absorb nodes are auto-created; skip them.
		if node.Operator.Kind() == operator.KindDelayAbsorb {
			continue
		}
		data, err := json.Marshal(node.Operator)
		if err != nil {
			return nil, fmt.Errorf("marshal node %s: %w", id, err)
		}
		nodes[id] = data
	}

	edges := make([]jsonEdge, 0, len(c.edges))
	for _, edge := range c.edges {
		if edge == nil {
			return nil, fmt.Errorf("edge is nil")
		}
		// Rewrite absorb targets back to emit IDs for correct round-tripping.
		to := edge.To
		if c.isDelayAbsorbID(to) {
			to = strings.TrimSuffix(to, "_absorb")
		}
		edges = append(edges, jsonEdge{From: edge.From, To: to, Port: edge.Port})
	}

	return json.Marshal(jsonCircuit{
		Name:  c.name,
		Nodes: nodes,
		Edges: edges,
	})
}

// UnmarshalJSON implements json.Unmarshaler.
func (c *Circuit) UnmarshalJSON(data []byte) error {
	if c == nil {
		return fmt.Errorf("circuit must be non-nil for JSON decoding")
	}

	var payload jsonCircuit
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	if payload.Name == "" {
		return fmt.Errorf("circuit name required")
	}
	if len(payload.Nodes) == 0 {
		return fmt.Errorf("circuit nodes required")
	}

	*c = *New(payload.Name)

	for id, opJSON := range payload.Nodes {
		op, err := operator.UnmarshalOperator(opJSON)
		if err != nil {
			return fmt.Errorf("node %s: %w", id, err)
		}
		if err := c.AddNode(&Node{ID: id, Operator: op}); err != nil {
			return err
		}
	}

	for _, edge := range payload.Edges {
		if err := c.AddEdge(&Edge{From: edge.From, To: edge.To, Port: edge.Port}); err != nil {
			return err
		}
	}

	return nil
}

// MarshalJSON implements json.Marshaler.
// The node is serialized as its operator JSON with "id" injected.
func (n *Node) MarshalJSON() ([]byte, error) {
	if n == nil {
		return []byte("null"), nil
	}
	opData, err := json.Marshal(n.Operator)
	if err != nil {
		return nil, err
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(opData, &m); err != nil {
		return nil, err
	}
	idJSON, _ := json.Marshal(n.ID)
	m["id"] = idJSON
	return json.Marshal(m)
}

// UnmarshalJSON implements json.Unmarshaler.
func (n *Node) UnmarshalJSON(data []byte) error {
	if n == nil {
		return fmt.Errorf("node must be non-nil for JSON decoding")
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	idRaw, ok := m["id"]
	if !ok {
		return fmt.Errorf("node: id required")
	}
	var id string
	if err := json.Unmarshal(idRaw, &id); err != nil {
		return fmt.Errorf("node: id: %w", err)
	}
	delete(m, "id")
	opData, err := json.Marshal(m)
	if err != nil {
		return err
	}
	op, err := operator.UnmarshalOperator(opData)
	if err != nil {
		return fmt.Errorf("node %s: %w", id, err)
	}
	*n = Node{ID: id, Operator: op}
	return nil
}

// MarshalJSON implements json.Marshaler.
func (e *Edge) MarshalJSON() ([]byte, error) {
	if e == nil {
		return []byte("null"), nil
	}
	return json.Marshal(jsonEdge{From: e.From, To: e.To, Port: e.Port})
}

// UnmarshalJSON implements json.Unmarshaler.
func (e *Edge) UnmarshalJSON(data []byte) error {
	if e == nil {
		return fmt.Errorf("edge must be non-nil for JSON decoding")
	}
	var payload jsonEdge
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	*e = Edge{From: payload.From, To: payload.To, Port: payload.Port}
	return nil
}
