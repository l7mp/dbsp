package sql

import (
	"testing"

	"github.com/l7mp/dbsp/expression/dbsp"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

func TestCompileExpressionBasic(t *testing.T) {
	stmt, err := sqlparser.Parse("select * from t where a = 1 and b is null")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		t.Fatalf("expected select, got %T", stmt)
	}
	where := selectStmt.Where
	if where == nil {
		t.Fatalf("expected where clause")
	}
	bindVars := make(map[string]*querypb.BindVariable)
	sqlparser.Normalize(selectStmt, bindVars, "v")

	expr, err := CompileExpression(where.Expr)
	if err != nil {
		t.Fatalf("compile expression: %v", err)
	}

	dbspExpr, ok := expr.(*dbsp.Expression)
	if !ok {
		t.Fatalf("expected dbsp expression, got %T", expr)
	}
	if dbspExpr.Root() == nil {
		t.Fatalf("expected non-nil root")
	}
}

func TestCompileExpressionJoinAlias(t *testing.T) {
	stmt, err := sqlparser.Parse("select * from a join b on a.id = b.id")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		t.Fatalf("expected select, got %T", stmt)
	}
	join, ok := selectStmt.From[0].(*sqlparser.JoinTableExpr)
	if !ok {
		t.Fatalf("expected join, got %T", selectStmt.From[0])
	}
	aliases := map[string]string{"a": "left", "b": "right"}
	if _, err := CompileExpressionWithAliases(join.Condition.On, aliases); err != nil {
		t.Fatalf("compile expression: %v", err)
	}
}
