package operator

import (
	"encoding/json"
	"fmt"

	"github.com/l7mp/dbsp/dbsp/expression"
	exprdbsp "github.com/l7mp/dbsp/dbsp/expression/dbsp"
)

// jsonOp is the wire format for all operators. It implements MarshalJSON and
// UnmarshalJSON so it can be embedded directly in operator structs whose only
// wire representation is a {"type":"..."} object (no extra fields).
type jsonOp struct {
	Type       string          `json:"type"`
	Coeffs     []int           `json:"coeffs,omitempty"`
	Predicate  json.RawMessage `json:"predicate,omitempty"`
	Projection json.RawMessage `json:"projection,omitempty"`
	Field      string          `json:"field,omitempty"`
	IndexField string          `json:"indexField,omitempty"`
	NameAppend bool            `json:"nameAppend,omitempty"`
	SumField   string          `json:"sumField,omitempty"`
	KeyExpr    json.RawMessage `json:"keyExpr,omitempty"`
	ValueExpr  json.RawMessage `json:"valueExpr,omitempty"`
	ReduceExpr json.RawMessage `json:"reduceExpr,omitempty"`
	SetExpr    json.RawMessage `json:"setExpr,omitempty"`
	OutField   string          `json:"outField,omitempty"`
}

// MarshalJSON implements json.Marshaler. Uses a local type alias to avoid
// infinite recursion.
func (j jsonOp) MarshalJSON() ([]byte, error) {
	type wire jsonOp
	return json.Marshal(wire(j))
}

// UnmarshalJSON implements json.Unmarshaler.
func (j *jsonOp) UnmarshalJSON(data []byte) error {
	type wire jsonOp
	return json.Unmarshal(data, (*wire)(j))
}

// MarshalJSON implements json.Marshaler.
func (o *LinearCombination) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonOp{Type: "linear_combination", Coeffs: o.coeffs})
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *LinearCombination) UnmarshalJSON(data []byte) error {
	var p jsonOp
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	o.coeffs = append([]int(nil), p.Coeffs...)
	return nil
}

// MarshalJSON implements json.Marshaler.
func (o *Select) MarshalJSON() ([]byte, error) {
	predJSON, err := json.Marshal(o.predicate)
	if err != nil {
		return nil, fmt.Errorf("marshal select predicate: %w", err)
	}
	return json.Marshal(jsonOp{Type: "select", Predicate: predJSON})
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *Select) UnmarshalJSON(data []byte) error {
	var p jsonOp
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	if len(p.Predicate) == 0 {
		return fmt.Errorf("select: predicate required")
	}
	pred, err := exprdbsp.Compile(p.Predicate)
	if err != nil {
		return fmt.Errorf("select: compile predicate: %w", err)
	}
	o.predicate = pred
	return nil
}

// MarshalJSON implements json.Marshaler.
func (o *Project) MarshalJSON() ([]byte, error) {
	projJSON, err := json.Marshal(o.projection)
	if err != nil {
		return nil, fmt.Errorf("marshal project projection: %w", err)
	}
	return json.Marshal(jsonOp{Type: "project", Projection: projJSON})
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *Project) UnmarshalJSON(data []byte) error {
	var p jsonOp
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	if len(p.Projection) == 0 {
		return fmt.Errorf("project: projection required")
	}
	proj, err := exprdbsp.Compile(p.Projection)
	if err != nil {
		return fmt.Errorf("project: compile projection: %w", err)
	}
	o.projection = proj
	return nil
}

// MarshalJSON implements json.Marshaler.
func (o *Unwind) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonOp{Type: "unwind", Field: o.fieldPath, IndexField: o.indexField, NameAppend: o.nameAppend})
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *Unwind) UnmarshalJSON(data []byte) error {
	var p jsonOp
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	o.fieldPath = p.Field
	o.indexField = p.IndexField
	o.nameAppend = p.NameAppend
	return nil
}

// UnmarshalOperator decodes an operator from its JSON wire format.
func UnmarshalOperator(data []byte) (Operator, error) {
	var p jsonOp
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("unmarshal operator: %w", err)
	}
	switch p.Type {
	case "input":
		return NewInput(), nil
	case "output":
		return NewOutput(), nil
	case "delay":
		emit, _ := NewDelay()
		return emit, nil
	case "integrate":
		return NewIntegrate(), nil
	case "differentiate":
		return NewDifferentiate(), nil
	case "delta0":
		return NewDelta0(), nil
	case "negate":
		return NewNegate(), nil
	case "plus":
		return NewPlus(), nil
	case "minus":
		return NewMinus(), nil
	case "sum":
		return NewSum(), nil
	case "subtract":
		return NewSubtract(), nil
	case "linear_combination":
		if len(p.Coeffs) == 0 {
			return nil, fmt.Errorf("linear_combination operator: coeffs required")
		}
		return NewLinearCombination(p.Coeffs), nil
	case "cartesian":
		return NewCartesianProduct(), nil
	case "distinct":
		return NewDistinct(), nil
	case "distinct_pi", "hkeyed":
		return NewDistinctPi(), nil
	case "aggregate_keyed":
		if len(p.KeyExpr) == 0 || len(p.ValueExpr) == 0 || len(p.ReduceExpr) == 0 {
			return nil, fmt.Errorf("aggregate_keyed operator: keyExpr, valueExpr, and reduceExpr are required")
		}
		keyExpr, err := exprdbsp.Compile(p.KeyExpr)
		if err != nil {
			return nil, fmt.Errorf("aggregate_keyed operator: compile keyExpr: %w", err)
		}
		valueExpr, err := exprdbsp.Compile(p.ValueExpr)
		if err != nil {
			return nil, fmt.Errorf("aggregate_keyed operator: compile valueExpr: %w", err)
		}
		reduceExpr, err := exprdbsp.Compile(p.ReduceExpr)
		if err != nil {
			return nil, fmt.Errorf("aggregate_keyed operator: compile reduceExpr: %w", err)
		}
		if len(p.SetExpr) != 0 {
			setExpr, err := exprdbsp.Compile(p.SetExpr)
			if err != nil {
				return nil, fmt.Errorf("aggregate_keyed operator: compile setExpr: %w", err)
			}
			return NewAggregateWithSet(keyExpr, valueExpr, reduceExpr, setExpr), nil
		}
		return NewAggregate(keyExpr, valueExpr, reduceExpr, p.OutField), nil
	case "select":
		if len(p.Predicate) == 0 {
			return nil, fmt.Errorf("select operator: predicate required")
		}
		pred, err := exprdbsp.Compile(p.Predicate)
		if err != nil {
			return nil, fmt.Errorf("select operator: compile predicate: %w", err)
		}
		return NewSelect(pred), nil
	case "project":
		if len(p.Projection) == 0 {
			return nil, fmt.Errorf("project operator: projection required")
		}
		proj, err := exprdbsp.Compile(p.Projection)
		if err != nil {
			return nil, fmt.Errorf("project operator: compile projection: %w", err)
		}
		return NewProject(proj), nil
	case "unwind":
		op := NewUnwind(p.Field)
		if p.IndexField != "" {
			op = op.WithIndexField(p.IndexField)
		}
		op = op.WithNameAppend(p.NameAppend)
		return op, nil
	default:
		return nil, fmt.Errorf("unknown operator type %q", p.Type)
	}
}

// MarshalJSON implements json.Marshaler.
func (o *Aggregate) MarshalJSON() ([]byte, error) {
	keyJSON, err := json.Marshal(o.keyExpr)
	if err != nil {
		return nil, fmt.Errorf("marshal aggregate_keyed keyExpr: %w", err)
	}
	valueJSON, err := json.Marshal(o.valueExpr)
	if err != nil {
		return nil, fmt.Errorf("marshal aggregate_keyed valueExpr: %w", err)
	}
	reduceJSON, err := json.Marshal(o.reduceExpr)
	if err != nil {
		return nil, fmt.Errorf("marshal aggregate_keyed reduceExpr: %w", err)
	}
	wire := jsonOp{Type: "aggregate_keyed", KeyExpr: keyJSON, ValueExpr: valueJSON, ReduceExpr: reduceJSON, OutField: o.outField}
	if o.setExpr != nil {
		setJSON, err := json.Marshal(o.setExpr)
		if err != nil {
			return nil, fmt.Errorf("marshal aggregate_keyed setExpr: %w", err)
		}
		wire.SetExpr = setJSON
	}
	return json.Marshal(wire)
}

// UnmarshalJSON implements json.Unmarshaler.
func (o *Aggregate) UnmarshalJSON(data []byte) error {
	var p jsonOp
	if err := json.Unmarshal(data, &p); err != nil {
		return err
	}
	if len(p.KeyExpr) == 0 || len(p.ValueExpr) == 0 || len(p.ReduceExpr) == 0 {
		return fmt.Errorf("aggregate_keyed: keyExpr, valueExpr, and reduceExpr are required")
	}
	var keyExpr expression.Expression
	if string(p.KeyExpr) != "null" {
		k, err := exprdbsp.Compile(p.KeyExpr)
		if err != nil {
			return fmt.Errorf("aggregate_keyed: compile keyExpr: %w", err)
		}
		keyExpr = k
	}
	valueExpr, err := exprdbsp.Compile(p.ValueExpr)
	if err != nil {
		return fmt.Errorf("aggregate_keyed: compile valueExpr: %w", err)
	}
	reduceExpr, err := exprdbsp.Compile(p.ReduceExpr)
	if err != nil {
		return fmt.Errorf("aggregate_keyed: compile reduceExpr: %w", err)
	}
	var setExpr expression.Expression
	if len(p.SetExpr) != 0 {
		s, err := exprdbsp.Compile(p.SetExpr)
		if err != nil {
			return fmt.Errorf("aggregate_keyed: compile setExpr: %w", err)
		}
		setExpr = s
	}
	o.keyExpr = keyExpr
	o.valueExpr = valueExpr
	o.reduceExpr = reduceExpr
	o.setExpr = setExpr
	o.outField = p.OutField
	if o.outField == "" {
		o.outField = "value"
	}
	return nil
}
