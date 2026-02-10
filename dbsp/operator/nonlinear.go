package operator

import (
	"fmt"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/datamodel"
	"github.com/l7mp/dbsp/dbsp/zset"
	"github.com/l7mp/dbsp/internal/logger"
)

// Distinct converts Z-set to set (all positive weights become 1).
type Distinct struct {
	name   string
	logger logr.Logger
}

// NewDistinct creates a new Distinct operator.
func NewDistinct(name string, opts ...Option) *Distinct {
	o := &Distinct{name: name}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

// Name implements Operator.
func (o *Distinct) Name() string { return o.name }

// String implements fmt.Stringer.
func (o *Distinct) String() string {
	return fmt.Sprintf("Distinct(%s)", o.name)
}

// Arity implements Operator.
func (o *Distinct) Arity() int { return 1 }

// Linearity implements Operator.
func (o *Distinct) Linearity() Linearity { return NonLinear }

// Apply implements Operator.
func (o *Distinct) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	result := zset.New()
	inputs[0].Iter(func(elem datamodel.Document, weight zset.Weight) bool {
		if weight > 0 {
			result.Insert(elem, 1)
		}
		return true
	})
	o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	return result, nil
}

func (o *Distinct) setLogger(l logr.Logger) { o.logger = l }

// AggType represents the type of aggregation.
type AggType int

const (
	AggSum AggType = iota
	AggCount
	AggMin
	AggMax
	AggAvg
)

func (a AggType) String() string {
	switch a {
	case AggSum:
		return "SUM"
	case AggCount:
		return "COUNT"
	case AggMin:
		return "MIN"
	case AggMax:
		return "MAX"
	case AggAvg:
		return "AVG"
	default:
		return "UNKNOWN"
	}
}

// Aggregation defines a single aggregation operation.
type Aggregation struct {
	Type        AggType // The aggregation function.
	InputField  string  // Field to aggregate (empty for COUNT(*)).
	OutputField string  // Field name in output document.
}

// aggregator holds state for a single aggregation during grouping.
type aggregator struct {
	agg      Aggregation
	sum      float64
	count    int64
	min      float64
	max      float64
	hasValue bool
}

func newAggregator(agg Aggregation) *aggregator {
	return &aggregator{agg: agg}
}

func (a *aggregator) add(val any, weight zset.Weight) error {
	if a.agg.Type == AggCount {
		a.count += int64(weight)
		return nil
	}

	// For other aggregations, we need a numeric value.
	var f float64
	switch v := val.(type) {
	case int:
		f = float64(v)
	case int64:
		f = float64(v)
	case float64:
		f = v
	case nil:
		return nil // Skip nil values.
	default:
		return fmt.Errorf("cannot aggregate non-numeric value: %T", val)
	}

	w := float64(weight)

	switch a.agg.Type {
	case AggSum:
		a.sum += f * w
	case AggAvg:
		a.sum += f * w
		a.count += int64(weight)
	case AggMin:
		if !a.hasValue || f < a.min {
			a.min = f
		}
	case AggMax:
		if !a.hasValue || f > a.max {
			a.max = f
		}
	}
	a.hasValue = true
	return nil
}

func (a *aggregator) result() any {
	switch a.agg.Type {
	case AggSum:
		return a.sum
	case AggCount:
		return a.count
	case AggMin:
		if !a.hasValue {
			return nil
		}
		return a.min
	case AggMax:
		if !a.hasValue {
			return nil
		}
		return a.max
	case AggAvg:
		if a.count == 0 {
			return nil
		}
		return a.sum / float64(a.count)
	default:
		return nil
	}
}

// Group performs GROUP BY with aggregation.
//
// Example:
//
//	keyFields: ["department"]
//	aggregations: [{Type: AggSum, InputField: "salary", OutputField: "total_salary"}]
//
//	Input:  {dept: "eng", salary: 100}, {dept: "eng", salary: 150}, {dept: "sales", salary: 80}
//	Output: {dept: "eng", total_salary: 250}, {dept: "sales", total_salary: 80}
type Group struct {
	name         string
	keyFields    []string      // Fields to group by.
	aggregations []Aggregation // Aggregations to compute.
	logger       logr.Logger
}

// NewGroup creates a new Group operator.
func NewGroup(name string, keyFields []string, aggregations []Aggregation, opts ...Option) *Group {
	o := &Group{
		name:         name,
		keyFields:    keyFields,
		aggregations: aggregations,
	}
	for _, opt := range opts {
		opt.apply(o)
	}
	o.logger = logger.NormalizeLogger(o.logger)
	return o
}

// Name implements Operator.
func (o *Group) Name() string { return o.name }

// String implements fmt.Stringer.
func (o *Group) String() string {
	return fmt.Sprintf("Group(%s, keys=%v, aggs=%d)", o.name, o.keyFields, len(o.aggregations))
}

// Arity implements Operator.
func (o *Group) Arity() int { return 1 }

// Linearity implements Operator.
func (o *Group) Linearity() Linearity { return NonLinear }

// groupKey extracts the grouping key from a document.
func (o *Group) groupKey(elem datamodel.Document) (string, map[string]any, error) {
	keyVals := make(map[string]any, len(o.keyFields))
	keyStr := ""
	for i, field := range o.keyFields {
		val, err := elem.GetField(field)
		if err != nil {
			return "", nil, err
		}
		keyVals[field] = val
		if i > 0 {
			keyStr += "|"
		}
		keyStr += fmt.Sprintf("%v", val)
	}
	return keyStr, keyVals, nil
}

// Apply implements Operator.
func (o *Group) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	// Map from group key string to (key values, aggregators).
	type groupState struct {
		keyVals     map[string]any
		aggregators []*aggregator
	}
	groups := make(map[string]*groupState)

	var err error
	inputs[0].Iter(func(elem datamodel.Document, weight zset.Weight) bool {
		keyStr, keyVals, e := o.groupKey(elem)
		if e != nil {
			err = e
			return false
		}

		state, exists := groups[keyStr]
		if !exists {
			// Initialize aggregators for this group.
			aggs := make([]*aggregator, len(o.aggregations))
			for i, agg := range o.aggregations {
				aggs[i] = newAggregator(agg)
			}
			state = &groupState{keyVals: keyVals, aggregators: aggs}
			groups[keyStr] = state
		}

		// Feed values to aggregators.
		for i, agg := range o.aggregations {
			var val any
			if agg.InputField != "" {
				val, e = elem.GetField(agg.InputField)
				if e != nil {
					// Field not found - skip for this aggregation.
					continue
				}
			}
			if e := state.aggregators[i].add(val, weight); e != nil {
				err = e
				return false
			}
		}

		return true
	})

	if err != nil {
		return zset.ZSet{}, err
	}

	// Build output documents.
	result := zset.New()
	for _, state := range groups {
		// Create output document with key fields and aggregation results.
		outFields := make(map[string]any)

		// Copy key fields.
		for k, v := range state.keyVals {
			outFields[k] = v
		}

		// Add aggregation results.
		for i, agg := range o.aggregations {
			outFields[agg.OutputField] = state.aggregators[i].result()
		}

		// Create a simple document from the fields.
		outDoc := &SimpleDocument{fields: outFields}
		result.Insert(outDoc, 1)
	}

	o.logger.V(2).Info("operator", "op", o.String(), "result", result.String())
	return result, nil
}

func (o *Group) setLogger(l logr.Logger) { o.logger = l }

// SimpleDocument is a basic Document implementation for Group output.
type SimpleDocument struct {
	fields map[string]any
}

func (d *SimpleDocument) Hash() string {
	return fmt.Sprintf("%v", d.fields)
}

func (d *SimpleDocument) PrimaryKey() (string, error) {
	return d.Hash(), nil
}

func (d *SimpleDocument) String() string {
	return fmt.Sprintf("%v", d.fields)
}

func (d *SimpleDocument) Concat(other datamodel.Document) datamodel.Document {
	newFields := make(map[string]any)
	for k, v := range d.fields {
		newFields[k] = v
	}
	if od, ok := other.(*SimpleDocument); ok {
		for k, v := range od.fields {
			newFields[k] = v
		}
	}
	return &SimpleDocument{fields: newFields}
}

func (d *SimpleDocument) Copy() datamodel.Document {
	newFields := make(map[string]any, len(d.fields))
	for k, v := range d.fields {
		newFields[k] = v
	}
	return &SimpleDocument{fields: newFields}
}

func (d *SimpleDocument) GetField(key string) (any, error) {
	v, ok := d.fields[key]
	if !ok {
		return nil, datamodel.ErrFieldNotFound
	}
	return v, nil
}

func (d *SimpleDocument) SetField(key string, value any) error {
	d.fields[key] = value
	return nil
}
