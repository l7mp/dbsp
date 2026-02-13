package sql

import (
	"testing"

	"github.com/l7mp/dbsp/datamodel/relation"
	"github.com/l7mp/dbsp/dbsp/executor"
	"github.com/l7mp/dbsp/dbsp/zset"
	"github.com/l7mp/dbsp/internal/logger"
)

func TestCompileAndExecuteSelectWhere(t *testing.T) {
	compiler := NewCompiler(nil)
	query, err := compiler.CompileString("select * from t where a = 1")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	exec, err := executor.New(query.Circuit, logger.DiscardLogger())
	if err != nil {
		t.Fatalf("executor: %v", err)
	}

	table := relation.NewTable("t", &relation.Schema{Columns: []relation.Column{{Name: "a"}}})
	row1 := &relation.Row{Table: table, Data: []any{int64(1)}}
	row2 := &relation.Row{Table: table, Data: []any{int64(2)}}
	input := zset.New().WithElems(zset.Elem{Document: row1, Weight: 1}, zset.Elem{Document: row2, Weight: 1})

	outputs, err := exec.Execute(map[string]zset.ZSet{"input_t": input})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	out := outputs["output"]
	if out.Size() != 1 {
		t.Fatalf("expected 1 row, got %d", out.Size())
	}
}

func TestCompileAndExecuteJoin(t *testing.T) {
	compiler := NewCompiler(nil)
	query, err := compiler.CompileString("select * from a join b on a.id = b.id")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	exec, err := executor.New(query.Circuit, logger.DiscardLogger())
	if err != nil {
		t.Fatalf("executor: %v", err)
	}

	tableA := relation.NewTable("a", &relation.Schema{Columns: []relation.Column{{Name: "id", QualifiedName: "a.id"}}})
	tableB := relation.NewTable("b", &relation.Schema{Columns: []relation.Column{{Name: "id", QualifiedName: "b.id"}}})
	rowA := &relation.Row{Table: tableA, Data: []any{int64(1)}}
	rowB := &relation.Row{Table: tableB, Data: []any{int64(1)}}
	inputA := zset.New().WithElems(zset.Elem{Document: rowA, Weight: 1})
	inputB := zset.New().WithElems(zset.Elem{Document: rowB, Weight: 1})

	outputs, err := exec.Execute(map[string]zset.ZSet{"input_a": inputA, "input_b": inputB})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	out := outputs["output"]
	if out.Size() != 1 {
		t.Fatalf("expected 1 joined row, got %d", out.Size())
	}
}
