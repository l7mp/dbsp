package sql

import (
	"testing"

	"github.com/l7mp/dbsp/datamodel/relation"
	"github.com/l7mp/dbsp/dbsp/exprutil"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
)

func TestSQLNullSemanticsAndOrNot(t *testing.T) {
	row := &relation.Row{
		Table: relation.NewTable("t", &relation.Schema{Columns: []relation.Column{{Name: "a"}}}),
		Data:  []any{nil},
	}

	stmt, err := sqlparser.Parse("select * from t where (a = 1) and true")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*sqlparser.Select)
	bindVars := make(map[string]*querypb.BindVariable)
	sqlparser.Normalize(selectStmt, bindVars, "v")

	predicate, err := CompilePredicateWithAliases(selectStmt.Where.Expr, nil)
	if err != nil {
		t.Fatalf("compile predicate: %v", err)
	}
	val, err := predicate.Evaluate(row)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if val != nil {
		t.Fatalf("expected NULL result, got %v", val)
	}
}

func TestSQLNullSemanticsOr(t *testing.T) {
	row := &relation.Row{
		Table: relation.NewTable("t", &relation.Schema{Columns: []relation.Column{{Name: "a"}}}),
		Data:  []any{nil},
	}
	stmt, err := sqlparser.Parse("select * from t where (a = 1) or false")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	selectStmt := stmt.(*sqlparser.Select)
	bindVars := make(map[string]*querypb.BindVariable)
	sqlparser.Normalize(selectStmt, bindVars, "v")

	predicate, err := CompilePredicateWithAliases(selectStmt.Where.Expr, nil)
	if err != nil {
		t.Fatalf("compile predicate: %v", err)
	}
	val, err := predicate.Evaluate(row)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if val != nil {
		t.Fatalf("expected NULL result, got %v", val)
	}
}

func TestProjectionWildcard(t *testing.T) {
	row := &relation.Row{
		Table: relation.NewTable("t", &relation.Schema{Columns: []relation.Column{{Name: "a"}}}),
		Data:  []any{int64(42)},
	}
	if _, err := compileProjection(sqlparser.SelectExprs{&sqlparser.StarExpr{}}, nil); err == nil {
		t.Fatalf("expected error for star in projection builder")
	}
	projection, err := compileProjection(sqlparser.SelectExprs{&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: sqlparser.NewColIdent("a")}}}, nil)
	if err != nil {
		t.Fatalf("compile projection: %v", err)
	}
	val, err := projection.Evaluate(row)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if _, ok := val.(*exprutil.MapDocument); !ok {
		t.Fatalf("expected map document projection")
	}
}

func TestProjectionStarWithAliasMap(t *testing.T) {
	star := &sqlparser.StarExpr{TableName: sqlparser.TableName{Name: sqlparser.NewTableIdent("t")}}
	projection, err := compileProjectionStar(star, map[string]string{"t": "t"})
	if err != nil {
		t.Fatalf("compile star projection: %v", err)
	}
	row := &relation.Row{
		Table: relation.NewTable("t", &relation.Schema{Columns: []relation.Column{{Name: "a"}}}),
		Data:  []any{int64(42)},
	}
	val, err := projection.Evaluate(row)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if _, ok := val.(*exprutil.MapDocument); !ok {
		t.Fatalf("expected map document projection")
	}
}
