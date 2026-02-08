package sql

import (
	"errors"
	"fmt"

	"github.com/l7mp/dbsp/expr"
	"github.com/l7mp/dbsp/zset"
	"github.com/xwb1989/sqlparser"
)

// ==========================================
// Expression Evaluator (The Mini SQL Engine)
// ==========================================

// ParseExpression parses a SQL string expression into our Expression interface.
func ParseExpression(exprSql string) (expr.Expression, error) {
	// We wrap in a dummy SELECT to use the parser
	stmt, err := sqlparser.Parse("SELECT " + exprSql)
	if err != nil {
		return nil, err
	}
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, errors.New("failed to parse expression")
	}
	if len(sel.SelectExprs) == 0 {
		return nil, errors.New("no expression found")
	}
	aliased, ok := sel.SelectExprs[0].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, errors.New("complex select expressions not supported")
	}

	return compileExpr(aliased.Expr)
}

func compileExpr(node sqlparser.Expr) (expr.Expression, error) {
	switch e := node.(type) {
	case *sqlparser.ColName:
		return &ColRef{Name: e.Name.String()}, nil
	case *sqlparser.SQLVal:
		return &Literal{Val: string(e.Val), Type: e.Type}, nil
	case *sqlparser.BinaryExpr:
		left, err := compileExpr(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := compileExpr(e.Right)
		if err != nil {
			return nil, err
		}
		return &BinaryOp{Left: left, Right: right, Op: e.Operator}, nil
	case *sqlparser.ComparisonExpr:
		left, err := compileExpr(e.Left)
		if err != nil {
			return nil, err
		}
		right, err := compileExpr(e.Right)
		if err != nil {
			return nil, err
		}
		return &CompareOp{Left: left, Right: right, Op: e.Operator}, nil
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", node)
	}
}

// --- Concrete Expression Types ---

type Literal struct {
	Val  string
	Type sqlparser.ValType // To know if it's Int or Str
}

func (l *Literal) Evaluate(d zset.Document) (any, error) {
	// Simple auto-typing
	// In a real engine, we'd use l.Type to cast strict.
	// Here we try to allow loose typing.
	return l.Val, nil
}

type ColRef struct {
	Name string
}

func (c *ColRef) Evaluate(d zset.Document) (any, error) {
	return d.(*Row).GetField(c.Name)
}

type BinaryOp struct {
	Left, Right expr.Expression
	Op          string // +, -, *, /
}

func (b *BinaryOp) Evaluate(d zset.Document) (any, error) {
	l, err := b.Left.Evaluate(d)
	if err != nil {
		return nil, err
	}
	r, err := b.Right.Evaluate(d)
	if err != nil {
		return nil, err
	}

	// Super simplistic type coercion for the toy engine
	f1, isNum1 := toFloat(l)
	f2, isNum2 := toFloat(r)

	if isNum1 && isNum2 {
		switch b.Op {
		case "+":
			return f1 + f2, nil
		case "-":
			return f1 - f2, nil
		case "*":
			return f1 * f2, nil
		case "/":
			if f2 == 0 {
				return nil, errors.New("division by zero")
			}
			return f1 / f2, nil
		}
	}
	return nil, fmt.Errorf("binary op %s only supported on numbers", b.Op)
}

type CompareOp struct {
	Left, Right expr.Expression
	Op          string // =, >, <, !=
}

func (c *CompareOp) Evaluate(d zset.Document) (any, error) {
	l, err := c.Left.Evaluate(d)
	if err != nil {
		return nil, err
	}
	r, err := c.Right.Evaluate(d)
	if err != nil {
		return nil, err
	}

	// Try numeric comparison first
	f1, isNum1 := toFloat(l)
	f2, isNum2 := toFloat(r)

	if isNum1 && isNum2 {
		switch c.Op {
		case "=":
			return f1 == f2, nil
		case ">":
			return f1 > f2, nil
		case "<":
			return f1 < f2, nil
		case "!=":
			return f1 != f2, nil
		case ">=":
			return f1 >= f2, nil
		case "<=":
			return f1 <= f2, nil
		}
	}

	// Fallback to string comparison
	s1 := fmt.Sprintf("%v", l)
	s2 := fmt.Sprintf("%v", r)
	switch c.Op {
	case "=":
		return s1 == s2, nil
	case "!=":
		return s1 != s2, nil
	}

	return false, fmt.Errorf("unsupported comparison %s between %v and %v", c.Op, l, r)
}

// helper for "Any" math
func toFloat(v any) (float64, bool) {
	switch i := v.(type) {
	case int:
		return float64(i), true
	case int64:
		return float64(i), true
	case float64:
		return i, true
	case string:
		// In a real engine we might try strconv.ParseFloat here
		// For now, if it's a string literal from parser, we might parse it
		var f float64
		if _, err := fmt.Sscanf(i, "%f", &f); err == nil {
			return f, true
		}
	}
	return 0, false
}
