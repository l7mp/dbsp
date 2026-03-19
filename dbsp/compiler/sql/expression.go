package sql

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/l7mp/dbsp/dbsp/expression"
	"github.com/l7mp/dbsp/dbsp/expression/dbsp"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

// CompileExpression compiles a SQL expression AST into a DBSP expression.
func CompileExpression(expr sqlparser.Expr) (expression.Expression, error) {
	return compileExpr(expr, nil)
}

// CompilePredicate compiles a SQL predicate using SQL NULL semantics.
func CompilePredicate(expr sqlparser.Expr, bindVars map[string]*querypb.BindVariable) (expression.Expression, error) {
	return compilePredicate(expr, bindVars)
}

func compilePredicate(expr sqlparser.Expr, bindVars map[string]*querypb.BindVariable) (dbsp.Expression, error) {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		left, err := compilePredicate(e.Left, bindVars)
		if err != nil {
			return nil, err
		}
		right, err := compilePredicate(e.Right, bindVars)
		if err != nil {
			return nil, err
		}
		leftIsNull := dbsp.NewIsNull(left)
		rightIsNull := dbsp.NewIsNull(right)
		leftIsFalse := dbsp.NewEq(left, dbsp.NewBool(false))
		rightIsFalse := dbsp.NewEq(right, dbsp.NewBool(false))
		falseExpr := dbsp.NewBool(false)
		unknownExpr := dbsp.NewNil()

		falseCase := dbsp.NewOr(leftIsFalse, rightIsFalse)
		unknownCase := dbsp.NewOr(leftIsNull, rightIsNull)
		trueCase := dbsp.NewAnd(left, right)

		return dbsp.NewCond(
			falseCase,
			falseExpr,
			dbsp.NewCond(unknownCase, unknownExpr, trueCase),
		), nil
	case *sqlparser.OrExpr:
		left, err := compilePredicate(e.Left, bindVars)
		if err != nil {
			return nil, err
		}
		right, err := compilePredicate(e.Right, bindVars)
		if err != nil {
			return nil, err
		}
		leftIsNull := dbsp.NewIsNull(left)
		rightIsNull := dbsp.NewIsNull(right)
		leftIsTrue := dbsp.NewEq(left, dbsp.NewBool(true))
		rightIsTrue := dbsp.NewEq(right, dbsp.NewBool(true))
		trueExpr := dbsp.NewBool(true)
		unknownExpr := dbsp.NewNil()

		trueCase := dbsp.NewOr(leftIsTrue, rightIsTrue)
		unknownCase := dbsp.NewOr(leftIsNull, rightIsNull)
		falseCase := dbsp.NewAnd(left, right)

		return dbsp.NewCond(
			trueCase,
			trueExpr,
			dbsp.NewCond(unknownCase, unknownExpr, falseCase),
		), nil
	case *sqlparser.NotExpr:
		inner, err := compilePredicate(e.Expr, bindVars)
		if err != nil {
			return nil, err
		}
		return sqlNot(inner), nil
	case *sqlparser.ComparisonExpr:
		left, err := compileExpr(e.Left, bindVars)
		if err != nil {
			return nil, err
		}
		right, err := compileExpr(e.Right, bindVars)
		if err != nil {
			return nil, err
		}
		cmp, err := comparisonExpr(e.Operator, left, right)
		if err != nil {
			return nil, err
		}
		return compareWithNull(left, right, cmp), nil
	case *sqlparser.IsExpr:
		inner, err := compileExpr(e.Expr, bindVars)
		if err != nil {
			return nil, err
		}
		switch strings.ToLower(e.Operator) {
		case "is null":
			return dbsp.NewIsNull(inner), nil
		case "is not null":
			return dbsp.NewNot(dbsp.NewIsNull(inner)), nil
		default:
			return nil, UnimplementedError{Feature: fmt.Sprintf("is expression %q", e.Operator)}
		}
	case *sqlparser.ParenExpr:
		return compilePredicate(e.Expr, bindVars)
	default:
		return compileExpr(expr, bindVars)
	}
}

func compareWithNull(left, right, cmp dbsp.Expression) dbsp.Expression {
	leftIsNull := dbsp.NewIsNull(left)
	rightIsNull := dbsp.NewIsNull(right)
	checkNull := dbsp.NewOr(leftIsNull, rightIsNull)
	return dbsp.NewCond(checkNull, dbsp.NewNil(), cmp)
}

func sqlNot(expr dbsp.Expression) dbsp.Expression {
	isNull := dbsp.NewIsNull(expr)
	negated := dbsp.NewNot(expr)
	return dbsp.NewCond(isNull, dbsp.NewNil(), negated)
}

func compileExpr(expr sqlparser.Expr, bindVars map[string]*querypb.BindVariable) (dbsp.Expression, error) {
	switch e := expr.(type) {
	case *sqlparser.AndExpr:
		return compilePredicate(e, bindVars)
	case *sqlparser.OrExpr:
		return compilePredicate(e, bindVars)
	case *sqlparser.NotExpr:
		return compilePredicate(e, bindVars)
	case *sqlparser.ComparisonExpr:
		return compilePredicate(e, bindVars)
	case *sqlparser.BinaryExpr:
		left, err := compileExpr(e.Left, bindVars)
		if err != nil {
			return nil, err
		}
		right, err := compileExpr(e.Right, bindVars)
		if err != nil {
			return nil, err
		}
		return binaryExpr(e.Operator, left, right)
	case *sqlparser.UnaryExpr:
		inner, err := compileExpr(e.Expr, bindVars)
		if err != nil {
			return nil, err
		}
		switch e.Operator {
		case sqlparser.PlusStr:
			return inner, nil
		case sqlparser.MinusStr:
			return dbsp.NewNeg(inner), nil
		default:
			return nil, UnimplementedError{Feature: fmt.Sprintf("unary operator %q", e.Operator)}
		}
	case *sqlparser.IsExpr:
		return compilePredicate(e, bindVars)
	case *sqlparser.SQLVal:
		if e.Type == sqlparser.ValArg {
			name := strings.TrimPrefix(string(e.Val), ":")
			return compileBindVar(name, bindVars)
		}
		return compileSQLVal(e)
	case *sqlparser.NullVal:
		return dbsp.NewNil(), nil
	case sqlparser.BoolVal:
		return dbsp.NewBool(bool(e)), nil
	case *sqlparser.ValTuple:
		return nil, UnimplementedError{Feature: "value tuple"}
	case *sqlparser.ColName:
		name := e.Name.String()
		if qualifier := e.Qualifier.Name.String(); qualifier != "" {
			name = qualifier + "." + name
		}
		return dbsp.NewGet(name), nil
	case *sqlparser.ParenExpr:
		return compileExpr(e.Expr, bindVars)
	default:
		return nil, UnimplementedError{Feature: fmt.Sprintf("expression %T", expr)}
	}
}

func compileBindVar(name string, bindVars map[string]*querypb.BindVariable) (dbsp.Expression, error) {
	if name == "" {
		return nil, UnimplementedError{Feature: "empty bind var"}
	}
	if bindVars == nil {
		return nil, UnimplementedError{Feature: fmt.Sprintf("bind var %q", name)}
	}
	bv, ok := bindVars[name]
	if !ok {
		return nil, UnimplementedError{Feature: fmt.Sprintf("bind var %q", name)}
	}
	return compileBindVariable(name, bv)
}

func compileBindVariable(name string, bv *querypb.BindVariable) (dbsp.Expression, error) {
	switch bv.Type {
	case querypb.Type_NULL_TYPE:
		return dbsp.NewNil(), nil
	case querypb.Type_INT64, querypb.Type_INT32, querypb.Type_INT16, querypb.Type_INT24, querypb.Type_INT8:
		val, err := strconv.ParseInt(string(bv.Value), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bind var %s int: %w", name, err)
		}
		return dbsp.NewInt(val), nil
	case querypb.Type_UINT64, querypb.Type_UINT32, querypb.Type_UINT16, querypb.Type_UINT24, querypb.Type_UINT8:
		val, err := strconv.ParseUint(string(bv.Value), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("bind var %s uint: %w", name, err)
		}
		return dbsp.NewInt(int64(val)), nil
	case querypb.Type_FLOAT64, querypb.Type_FLOAT32, querypb.Type_DECIMAL:
		val, err := strconv.ParseFloat(string(bv.Value), 64)
		if err != nil {
			return nil, fmt.Errorf("bind var %s float: %w", name, err)
		}
		return dbsp.NewFloat(val), nil
	case querypb.Type_TEXT, querypb.Type_VARCHAR, querypb.Type_CHAR:
		return dbsp.NewString(string(bv.Value)), nil
	default:
		return nil, UnimplementedError{Feature: fmt.Sprintf("bind var %s type %s", name, bindVarTypeName(bv.Type))}
	}
}

func bindVarTypeName(t querypb.Type) string {
	if name, ok := querypb.Type_name[int32(t)]; ok {
		return name
	}
	return fmt.Sprintf("%d", t)
}

func compileSQLVal(val *sqlparser.SQLVal) (dbsp.Expression, error) {
	switch val.Type {
	case sqlparser.IntVal:
		i, err := strconv.ParseInt(string(val.Val), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("int literal: %w", err)
		}
		return dbsp.NewInt(i), nil
	case sqlparser.FloatVal:
		f, err := strconv.ParseFloat(string(val.Val), 64)
		if err != nil {
			return nil, fmt.Errorf("float literal: %w", err)
		}
		return dbsp.NewFloat(f), nil
	case sqlparser.StrVal, sqlparser.HexVal:
		return dbsp.NewString(string(val.Val)), nil
	case sqlparser.BitVal:
		return nil, UnimplementedError{Feature: "bit literal"}
	default:
		return nil, UnimplementedError{Feature: fmt.Sprintf("literal type %s", literalTypeName(val.Type))}
	}
}

func comparisonExpr(op string, left, right dbsp.Expression) (dbsp.Expression, error) {
	switch op {
	case sqlparser.EqualStr:
		return dbsp.NewEq(left, right), nil
	case sqlparser.NotEqualStr:
		return dbsp.NewNeq(left, right), nil
	case sqlparser.GreaterThanStr:
		return dbsp.NewGt(left, right), nil
	case sqlparser.GreaterEqualStr:
		return dbsp.NewGte(left, right), nil
	case sqlparser.LessThanStr:
		return dbsp.NewLt(left, right), nil
	case sqlparser.LessEqualStr:
		return dbsp.NewLte(left, right), nil
	default:
		return nil, UnimplementedError{Feature: fmt.Sprintf("comparison operator %q", op)}
	}
}

func binaryExpr(op string, left, right dbsp.Expression) (dbsp.Expression, error) {
	switch op {
	case sqlparser.PlusStr:
		return dbsp.NewAdd(left, right), nil
	case sqlparser.MinusStr:
		return dbsp.NewSub(left, right), nil
	case sqlparser.MultStr:
		return dbsp.NewMul(left, right), nil
	case sqlparser.DivStr:
		return dbsp.NewDiv(left, right), nil
	case sqlparser.ModStr:
		return dbsp.NewMod(left, right), nil
	default:
		return nil, UnimplementedError{Feature: fmt.Sprintf("binary operator %q", op)}
	}
}
