package sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/l7mp/dbsp/expression"
	"github.com/l7mp/dbsp/expression/dbsp"
	"github.com/xwb1989/sqlparser"
)

// CompileExpression compiles a SQL expression AST into a DBSP expression.
func CompileExpression(expr sqlparser.Expr) (expression.Expression, error) {
	root, err := compileExpr(expr, nil)
	if err != nil {
		return nil, err
	}
	return dbsp.NewExpression(root), nil
}

// CompileExpressionWithAliases compiles a SQL expression using qualifier aliases.
func CompileExpressionWithAliases(expr sqlparser.Expr, aliases map[string]string) (expression.Expression, error) {
	root, err := compileExpr(expr, aliases)
	if err != nil {
		return nil, err
	}
	return dbsp.NewExpression(root), nil
}

// CompilePredicateWithAliases compiles a SQL predicate using SQL NULL semantics.
func CompilePredicateWithAliases(expr sqlparser.Expr, aliases map[string]string) (expression.Expression, error) {
	root, err := compilePredicate(expr, aliases)
	if err != nil {
		return nil, err
	}
	return dbsp.NewExpression(root), nil
}

func compilePredicate(expr sqlparser.Expr, aliases map[string]string) (dbsp.Expr, error) {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		left, err := compilePredicate(e.Left, aliases)
		if err != nil {
			return nil, err
		}
		right, err := compilePredicate(e.Right, aliases)
		if err != nil {
			return nil, err
		}
		leftIsNull := dbsp.NewOpExpr(&dbsp.IsNullOp{}, dbsp.UnaryArgs{Operand: left})
		rightIsNull := dbsp.NewOpExpr(&dbsp.IsNullOp{}, dbsp.UnaryArgs{Operand: right})
		leftIsFalse := dbsp.NewOpExpr(&dbsp.EqOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{left, boolLiteral(false)}})
		rightIsFalse := dbsp.NewOpExpr(&dbsp.EqOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{right, boolLiteral(false)}})
		falseExpr := boolLiteral(false)
		unknownExpr := dbsp.NewLiteralExpr(&dbsp.NilOp{}, nil)

		falseCase := dbsp.NewOpExpr(&dbsp.OrOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{leftIsFalse, rightIsFalse}})
		unknownCase := dbsp.NewOpExpr(&dbsp.OrOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{leftIsNull, rightIsNull}})
		trueCase := dbsp.NewOpExpr(&dbsp.AndOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{left, right}})

		return dbsp.NewOpExpr(&dbsp.CondOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{
			falseCase,
			falseExpr,
			dbsp.NewOpExpr(&dbsp.CondOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{
				unknownCase,
				unknownExpr,
				trueCase,
			}}),
		}}), nil
	case *sqlparser.OrExpr:
		left, err := compilePredicate(e.Left, aliases)
		if err != nil {
			return nil, err
		}
		right, err := compilePredicate(e.Right, aliases)
		if err != nil {
			return nil, err
		}
		leftIsNull := dbsp.NewOpExpr(&dbsp.IsNullOp{}, dbsp.UnaryArgs{Operand: left})
		rightIsNull := dbsp.NewOpExpr(&dbsp.IsNullOp{}, dbsp.UnaryArgs{Operand: right})
		leftIsTrue := dbsp.NewOpExpr(&dbsp.EqOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{left, boolLiteral(true)}})
		rightIsTrue := dbsp.NewOpExpr(&dbsp.EqOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{right, boolLiteral(true)}})
		trueExpr := boolLiteral(true)
		unknownExpr := dbsp.NewLiteralExpr(&dbsp.NilOp{}, nil)

		trueCase := dbsp.NewOpExpr(&dbsp.OrOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{leftIsTrue, rightIsTrue}})
		unknownCase := dbsp.NewOpExpr(&dbsp.OrOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{leftIsNull, rightIsNull}})
		falseCase := dbsp.NewOpExpr(&dbsp.AndOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{left, right}})

		return dbsp.NewOpExpr(&dbsp.CondOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{
			trueCase,
			trueExpr,
			dbsp.NewOpExpr(&dbsp.CondOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{
				unknownCase,
				unknownExpr,
				falseCase,
			}}),
		}}), nil
	case *sqlparser.NotExpr:
		inner, err := compilePredicate(e.Expr, aliases)
		if err != nil {
			return nil, err
		}
		return sqlNot(inner), nil
	case *sqlparser.ComparisonExpr:
		left, err := compileExpr(e.Left, aliases)
		if err != nil {
			return nil, err
		}
		right, err := compileExpr(e.Right, aliases)
		if err != nil {
			return nil, err
		}
		op, err := comparisonOp(e.Operator)
		if err != nil {
			return nil, err
		}
		cmp := dbsp.NewOpExpr(op, dbsp.ListArgs{Elements: []dbsp.Expr{left, right}})
		return compareWithNull(left, right, cmp), nil
	case *sqlparser.IsExpr:
		inner, err := compileExpr(e.Expr, aliases)
		if err != nil {
			return nil, err
		}
		switch strings.ToLower(e.Operator) {
		case "is null":
			return dbsp.NewOpExpr(&dbsp.IsNullOp{}, dbsp.UnaryArgs{Operand: inner}), nil
		case "is not null":
			isNull := dbsp.NewOpExpr(&dbsp.IsNullOp{}, dbsp.UnaryArgs{Operand: inner})
			return dbsp.NewOpExpr(&dbsp.NotOp{}, dbsp.UnaryArgs{Operand: isNull}), nil
		default:
			return nil, UnimplementedError{Feature: fmt.Sprintf("is expression %q", e.Operator)}
		}
	case *sqlparser.ParenExpr:
		return compilePredicate(e.Expr, aliases)
	default:
		return compileExpr(expr, aliases)
	}
}

func compareWithNull(left, right, cmp dbsp.Expr) dbsp.Expr {
	leftIsNull := dbsp.NewOpExpr(&dbsp.IsNullOp{}, dbsp.UnaryArgs{Operand: left})
	rightIsNull := dbsp.NewOpExpr(&dbsp.IsNullOp{}, dbsp.UnaryArgs{Operand: right})
	unknown := dbsp.NewLiteralExpr(&dbsp.NilOp{}, nil)
	checkNull := dbsp.NewOpExpr(&dbsp.OrOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{leftIsNull, rightIsNull}})
	return dbsp.NewOpExpr(&dbsp.CondOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{
		checkNull,
		unknown,
		cmp,
	}})
}

func sqlNot(expr dbsp.Expr) dbsp.Expr {
	isNull := dbsp.NewOpExpr(&dbsp.IsNullOp{}, dbsp.UnaryArgs{Operand: expr})
	negated := dbsp.NewOpExpr(&dbsp.NotOp{}, dbsp.UnaryArgs{Operand: expr})
	return dbsp.NewOpExpr(&dbsp.CondOp{}, dbsp.ListArgs{Elements: []dbsp.Expr{
		isNull,
		dbsp.NewLiteralExpr(&dbsp.NilOp{}, nil),
		negated,
	}})
}

func boolLiteral(value bool) dbsp.Expr {
	return dbsp.NewLiteralExpr(&dbsp.BoolOp{}, value)
}

func compileExpr(expr sqlparser.Expr, aliases map[string]string) (dbsp.Expr, error) {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		return compilePredicate(e, aliases)
	case *sqlparser.OrExpr:
		return compilePredicate(e, aliases)
	case *sqlparser.NotExpr:
		return compilePredicate(e, aliases)
	case *sqlparser.ComparisonExpr:
		return compilePredicate(e, aliases)
	case *sqlparser.BinaryExpr:
		left, err := compileExpr(e.Left, aliases)
		if err != nil {
			return nil, err
		}
		right, err := compileExpr(e.Right, aliases)
		if err != nil {
			return nil, err
		}
		op, err := binaryOp(e.Operator)
		if err != nil {
			return nil, err
		}
		return dbsp.NewOpExpr(op, dbsp.ListArgs{Elements: []dbsp.Expr{left, right}}), nil
	case *sqlparser.UnaryExpr:
		inner, err := compileExpr(e.Expr, aliases)
		if err != nil {
			return nil, err
		}
		switch e.Operator {
		case sqlparser.PlusStr:
			return inner, nil
		case sqlparser.MinusStr:
			return dbsp.NewOpExpr(&dbsp.NegOp{}, dbsp.UnaryArgs{Operand: inner}), nil
		default:
			return nil, UnimplementedError{Feature: fmt.Sprintf("unary operator %q", e.Operator)}
		}
	case *sqlparser.IsExpr:
		return compilePredicate(e, aliases)
	case *sqlparser.SQLVal:
		return compileSQLVal(e)
	case *sqlparser.NullVal:
		return dbsp.NewLiteralExpr(&dbsp.NilOp{}, nil), nil
	case sqlparser.BoolVal:
		return dbsp.NewLiteralExpr(&dbsp.BoolOp{}, bool(e)), nil
	case *sqlparser.ColName:
		name := e.Name.String()
		if qualifier := e.Qualifier.Name.String(); qualifier != "" {
			if aliases != nil {
				if actual, ok := aliases[qualifier]; ok {
					qualifier = actual
				}
			}
			name = qualifier + "." + name
		}
		return dbsp.NewLiteralExpr(&dbsp.GetOp{}, name), nil
	case sqlparser.ValTuple:
		return nil, UnimplementedError{Feature: "value tuple"}
	case *sqlparser.ParenExpr:
		return compileExpr(e.Expr, aliases)
	default:
		return nil, UnimplementedError{Feature: fmt.Sprintf("expression %T", expr)}
	}
}

func compileSQLVal(val *sqlparser.SQLVal) (dbsp.Expr, error) {
	switch val.Type {
	case sqlparser.IntVal:
		i, err := strconv.ParseInt(string(val.Val), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("int literal: %w", err)
		}
		return dbsp.NewLiteralExpr(&dbsp.IntOp{}, i), nil
	case sqlparser.FloatVal:
		f, err := strconv.ParseFloat(string(val.Val), 64)
		if err != nil {
			return nil, fmt.Errorf("float literal: %w", err)
		}
		return dbsp.NewLiteralExpr(&dbsp.FloatOp{}, f), nil
	case sqlparser.StrVal, sqlparser.HexVal:
		return dbsp.NewLiteralExpr(&dbsp.StringOp{}, string(val.Val)), nil
	case sqlparser.BitVal:
		return nil, UnimplementedError{Feature: "bit literal"}
	default:
		return nil, UnimplementedError{Feature: fmt.Sprintf("literal type %v", val.Type)}
	}
}

func comparisonOp(op string) (dbsp.Operator, error) {
	switch op {
	case sqlparser.EqualStr:
		return &dbsp.EqOp{}, nil
	case sqlparser.NotEqualStr:
		return &dbsp.NeqOp{}, nil
	case sqlparser.GreaterThanStr:
		return &dbsp.GtOp{}, nil
	case sqlparser.GreaterEqualStr:
		return &dbsp.GteOp{}, nil
	case sqlparser.LessThanStr:
		return &dbsp.LtOp{}, nil
	case sqlparser.LessEqualStr:
		return &dbsp.LteOp{}, nil
	default:
		return nil, UnimplementedError{Feature: fmt.Sprintf("comparison operator %q", op)}
	}
}

func binaryOp(op string) (dbsp.Operator, error) {
	switch op {
	case sqlparser.PlusStr:
		return &dbsp.AddOp{}, nil
	case sqlparser.MinusStr:
		return &dbsp.SubOp{}, nil
	case sqlparser.MultStr:
		return &dbsp.MulOp{}, nil
	case sqlparser.DivStr:
		return &dbsp.DivOp{}, nil
	case sqlparser.ModStr:
		return &dbsp.ModOp{}, nil
	default:
		return nil, UnimplementedError{Feature: fmt.Sprintf("binary operator %q", op)}
	}
}
