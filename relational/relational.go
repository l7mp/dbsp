// Package relational provides SQL-like relational algebra primitives for DBSP testing.
package relational

import (
	"fmt"
	"sort"

	"github.com/l7mp/dbsp/expr"
	"github.com/l7mp/dbsp/operator"
	"github.com/l7mp/dbsp/zset"
)

// Row represents a relational tuple with named columns.
// Key() returns a composite key based on all column values (true relational semantics).
type Row struct {
	cols map[string]any
}

// NewRow creates a Row from column name-value pairs.
// Example: NewRow("id", 1, "name", "Alice", "age", 30)
func NewRow(pairs ...any) Row {
	if len(pairs)%2 != 0 {
		panic("NewRow requires even number of arguments (name-value pairs)")
	}
	cols := make(map[string]any, len(pairs)/2)
	for i := 0; i < len(pairs); i += 2 {
		name, ok := pairs[i].(string)
		if !ok {
			panic(fmt.Sprintf("column name must be string, got %T", pairs[i]))
		}
		cols[name] = pairs[i+1]
	}
	return Row{cols: cols}
}

// RowFrom creates a Row from a map.
func RowFrom(cols map[string]any) Row {
	// Make a copy to avoid mutation issues.
	copied := make(map[string]any, len(cols))
	for k, v := range cols {
		copied[k] = v
	}
	return Row{cols: copied}
}

// Key returns a composite key based on all column values.
// Columns are sorted by name for deterministic key generation.
func (r Row) Key() string {
	if len(r.cols) == 0 {
		return ""
	}

	// Sort column names for deterministic ordering.
	names := make([]string, 0, len(r.cols))
	for name := range r.cols {
		names = append(names, name)
	}
	sort.Strings(names)

	// Build composite key as string representation.
	key := ""
	for i, name := range names {
		if i > 0 {
			key += "|"
		}
		key += fmt.Sprintf("%s=%v", name, r.cols[name])
	}
	return key
}

// PrimaryKey returns the primary key for the row.
// By default, it returns the same as Key() (all columns).
// For SQL-like primary key behavior, use a Row with only PK columns.
func (r Row) PrimaryKey() (string, error) {
	return r.Key(), nil
}

// Get returns the value of a column.
func (r Row) Get(name string) any {
	return r.cols[name]
}

// GetInt returns the value of a column as int.
func (r Row) GetInt(name string) int {
	v := r.cols[name]
	switch val := v.(type) {
	case int:
		return val
	case int64:
		return int(val)
	case float64:
		return int(val)
	default:
		return 0
	}
}

// GetString returns the value of a column as string.
func (r Row) GetString(name string) string {
	v := r.cols[name]
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// Columns returns all column names.
func (r Row) Columns() []string {
	names := make([]string, 0, len(r.cols))
	for name := range r.cols {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// With returns a new Row with additional/updated columns.
func (r Row) With(pairs ...any) Row {
	if len(pairs)%2 != 0 {
		panic("With requires even number of arguments")
	}
	newCols := make(map[string]any, len(r.cols)+len(pairs)/2)
	for k, v := range r.cols {
		newCols[k] = v
	}
	for i := 0; i < len(pairs); i += 2 {
		name := pairs[i].(string)
		newCols[name] = pairs[i+1]
	}
	return Row{cols: newCols}
}

// Project returns a new Row with only the specified columns.
func (r Row) Project(names ...string) Row {
	newCols := make(map[string]any, len(names))
	for _, name := range names {
		if v, ok := r.cols[name]; ok {
			newCols[name] = v
		}
	}
	return Row{cols: newCols}
}

// String returns a string representation of the row.
func (r Row) String() string {
	return fmt.Sprintf("Row%v", r.cols)
}

// Table helpers.

// TableOf creates a Z-set from rows (all weight 1).
func TableOf(rows ...Row) zset.ZSet {
	z := zset.New()
	for _, row := range rows {
		z.Insert(row, 1)
	}
	return z
}

// RowOf creates a single-row Z-set with weight 1.
func RowOf(pairs ...any) zset.ZSet {
	z := zset.New()
	z.Insert(NewRow(pairs...), 1)
	return z
}

// Expression helpers for common patterns.

// Col creates an expression that extracts a column value from a Row.
func Col(name string) expr.Expression {
	return expr.Func(func(e zset.Document) (any, error) {
		row, ok := e.(Row)
		if !ok {
			return nil, fmt.Errorf("expected Row, got %T", e)
		}
		return row.Get(name), nil
	})
}

// SelectWhere creates a predicate expression for filtering rows.
// The predicate function receives the Row and returns true to keep it.
func SelectWhere(pred func(Row) bool) expr.Expression {
	return expr.Func(func(e zset.Document) (any, error) {
		row, ok := e.(Row)
		if !ok {
			return false, fmt.Errorf("expected Row, got %T", e)
		}
		return pred(row), nil
	})
}

// ProjectTo creates a projection expression that transforms a Row.
// The project function receives the input Row and returns the output Row.
func ProjectTo(proj func(Row) Row) expr.Expression {
	return expr.Func(func(e zset.Document) (any, error) {
		row, ok := e.(Row)
		if !ok {
			return nil, fmt.Errorf("expected Row, got %T", e)
		}
		return proj(row), nil
	})
}

// ProjectCols creates a projection expression that keeps only specified columns.
func ProjectCols(names ...string) expr.Expression {
	return expr.Func(func(e zset.Document) (any, error) {
		row, ok := e.(Row)
		if !ok {
			return nil, fmt.Errorf("expected Row, got %T", e)
		}
		return row.Project(names...), nil
	})
}

// Join predicate helpers.

// JoinOn creates a join predicate that compares columns from left and right rows.
// Example: JoinOn("R.id", "S.id") creates predicate for R.id = S.id.
func JoinOn(leftCol, rightCol string) expr.Expression {
	return expr.Func(func(e zset.Document) (any, error) {
		pair, ok := e.(*operator.Pair)
		if !ok {
			return false, fmt.Errorf("expected Pair, got %T", e)
		}
		left, ok := pair.Left().(Row)
		if !ok {
			return false, fmt.Errorf("expected Row on left, got %T", pair.Left())
		}
		right, ok := pair.Right().(Row)
		if !ok {
			return false, fmt.Errorf("expected Row on right, got %T", pair.Right())
		}
		return left.Get(leftCol) == right.Get(rightCol), nil
	})
}

// JoinPred creates a join predicate with custom comparison logic.
func JoinPred(pred func(left, right Row) bool) expr.Expression {
	return expr.Func(func(e zset.Document) (any, error) {
		pair, ok := e.(*operator.Pair)
		if !ok {
			return false, fmt.Errorf("expected Pair, got %T", e)
		}
		left, ok := pair.Left().(Row)
		if !ok {
			return false, fmt.Errorf("expected Row on left, got %T", pair.Left())
		}
		right, ok := pair.Right().(Row)
		if !ok {
			return false, fmt.Errorf("expected Row on right, got %T", pair.Right())
		}
		return pred(left, right), nil
	})
}

// JoinProject creates a projection for join results (Pairs).
func JoinProject(proj func(left, right Row) Row) expr.Expression {
	return expr.Func(func(e zset.Document) (any, error) {
		pair, ok := e.(*operator.Pair)
		if !ok {
			return nil, fmt.Errorf("expected Pair, got %T", e)
		}
		left, ok := pair.Left().(Row)
		if !ok {
			return nil, fmt.Errorf("expected Row on left, got %T", pair.Left())
		}
		right, ok := pair.Right().(Row)
		if !ok {
			return nil, fmt.Errorf("expected Row on right, got %T", pair.Right())
		}
		return proj(left, right), nil
	})
}

// FlattenPair creates a projection that merges left and right rows.
// If there are column name conflicts, right columns are prefixed.
func FlattenPair(leftPrefix, rightPrefix string) expr.Expression {
	return expr.Func(func(e zset.Document) (any, error) {
		pair, ok := e.(*operator.Pair)
		if !ok {
			return nil, fmt.Errorf("expected Pair, got %T", e)
		}
		left, ok := pair.Left().(Row)
		if !ok {
			return nil, fmt.Errorf("expected Row on left, got %T", pair.Left())
		}
		right, ok := pair.Right().(Row)
		if !ok {
			return nil, fmt.Errorf("expected Row on right, got %T", pair.Right())
		}

		merged := make(map[string]any)
		for _, col := range left.Columns() {
			merged[leftPrefix+col] = left.Get(col)
		}
		for _, col := range right.Columns() {
			merged[rightPrefix+col] = right.Get(col)
		}
		return RowFrom(merged), nil
	})
}

// Aggregation helpers.

// GroupKey creates a key expression for GROUP BY.
func GroupKey(cols ...string) expr.Expression {
	return expr.Func(func(e zset.Document) (any, error) {
		row, ok := e.(Row)
		if !ok {
			return nil, fmt.Errorf("expected Row, got %T", e)
		}
		if len(cols) == 1 {
			return row.Get(cols[0]), nil
		}
		// Composite key.
		key := ""
		for i, col := range cols {
			if i > 0 {
				key += "|"
			}
			key += fmt.Sprintf("%v", row.Get(col))
		}
		return key, nil
	})
}

// CountAgg creates aggregation expressions for COUNT(*).
// Returns (zeroExpr, foldExpr, outputExpr) for use with operator.NewGroup.
func CountAgg(groupCols ...string) (zero, fold, output expr.Expression) {
	zero = expr.Func(func(e zset.Document) (any, error) {
		return int64(0), nil
	})
	fold = expr.Func(func(e zset.Document) (any, error) {
		fi := e.(operator.FoldInput)
		acc := fi.Acc().(int64)
		return acc + int64(fi.Weight()), nil
	})
	output = expr.Func(func(e zset.Document) (any, error) {
		go_ := e.(operator.GroupOutput)
		count := go_.Acc().(int64)
		// Build output row with group key + count.
		if len(groupCols) == 1 {
			return NewRow(groupCols[0], go_.GroupKey(), "count", count), nil
		}
		// For composite keys, we just include the key as-is.
		return NewRow("_key", go_.GroupKey(), "count", count), nil
	})
	return
}

// SumAgg creates aggregation expressions for SUM(col).
func SumAgg(sumCol string, groupCols ...string) (zero, fold, output expr.Expression) {
	zero = expr.Func(func(e zset.Document) (any, error) {
		return int64(0), nil
	})
	fold = expr.Func(func(e zset.Document) (any, error) {
		fi := e.(operator.FoldInput)
		acc := fi.Acc().(int64)
		row := fi.Elem().(Row)
		val := int64(row.GetInt(sumCol))
		return acc + val*int64(fi.Weight()), nil
	})
	output = expr.Func(func(e zset.Document) (any, error) {
		go_ := e.(operator.GroupOutput)
		sum := go_.Acc().(int64)
		if len(groupCols) == 1 {
			return NewRow(groupCols[0], go_.GroupKey(), "sum", sum), nil
		}
		return NewRow("_key", go_.GroupKey(), "sum", sum), nil
	})
	return
}
