package main

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/dop251/goja"

	compilersql "github.com/l7mp/dbsp/engine/compiler/sql"
	"github.com/l7mp/dbsp/engine/datamodel/relation"
)

func (v *VM) sqlTable(call goja.FunctionCall) (goja.Value, error) {
	if len(call.Arguments) < 2 {
		return nil, fmt.Errorf("sql.table(name, ddlCols) requires 2 arguments")
	}

	name := strings.TrimSpace(call.Argument(0).String())
	cols := strings.TrimSpace(call.Argument(1).String())
	if name == "" {
		return nil, fmt.Errorf("sql.table: empty table name")
	}
	if cols == "" {
		return nil, fmt.Errorf("sql.table: empty column definition")
	}

	if _, err := v.db.GetTable(name); err == nil {
		return nil, fmt.Errorf("sql.table: table %q already exists", name)
	}

	schema, err := parseColumnDefs(name, cols)
	if err != nil {
		return nil, fmt.Errorf("sql.table: %w", err)
	}

	v.db.RegisterTable(name, relation.NewTable(name, schema))
	return goja.Undefined(), nil
}

func (v *VM) sqlCompile(call goja.FunctionCall) (goja.Value, error) {
	if len(call.Arguments) < 1 {
		return nil, fmt.Errorf("sql.compile(query, { output }) requires query")
	}

	query := strings.TrimSpace(call.Argument(0).String())
	if query == "" {
		return nil, fmt.Errorf("sql.compile: empty query")
	}

	options := struct {
		Output string `json:"output"`
	}{}
	if len(call.Arguments) > 1 {
		if err := decodeOptionValue(call.Argument(1), &options); err != nil {
			return nil, fmt.Errorf("sql.compile options: %w", err)
		}
	}
	if options.Output == "" {
		options.Output = "output"
	}

	compiled, err := compilersql.New(v.db).CompileString(query)
	if err != nil {
		return nil, fmt.Errorf("sql.compile: %w", err)
	}

	outputMap, err := renameOutputMap(options.Output, compiled.OutputMap)
	if err != nil {
		return nil, fmt.Errorf("sql.compile: %w", err)
	}

	compiled.OutputMap = outputMap
	h := &circuitHandle{c: compiled.Circuit, query: compiled, vm: v}
	return h.jsObject(), nil
}

func renameOutputMap(outputBase string, out map[string]string) (map[string]string, error) {
	if len(out) == 0 {
		return nil, fmt.Errorf("missing output mapping")
	}

	if outputBase == "" {
		outputBase = "output"
	}

	renamed := make(map[string]string, len(out))
	for logical, nodeID := range out {
		if logical == "output" {
			renamed[outputBase] = nodeID
			continue
		}
		key := outputBase + "-" + logical
		if _, exists := renamed[key]; exists {
			return nil, fmt.Errorf("duplicate output key %q", key)
		}
		renamed[key] = nodeID
	}

	if len(renamed) == 0 {
		return nil, fmt.Errorf("missing output mapping")
	}

	return renamed, nil
}

func parseColumnDefs(tableName, defs string) (*relation.Schema, error) {
	parts := splitOnComma(defs)
	if len(parts) == 0 {
		return nil, fmt.Errorf("no column definitions found")
	}

	type colDef struct {
		name string
		typ  relation.ColumnType
		isPK bool
	}

	cols := make([]colDef, 0, len(parts))
	hasPK := false
	for _, part := range parts {
		part = strings.TrimSpace(part)
		fields := strings.Fields(part)
		if len(fields) == 0 {
			continue
		}
		cd := colDef{name: fields[0], typ: relation.TypeAny}
		if len(fields) >= 2 {
			cd.typ = parseColumnType(fields[1])
		}
		if strings.Contains(strings.ToUpper(part), "PRIMARY KEY") {
			cd.isPK = true
			hasPK = true
		}
		cols = append(cols, cd)
	}

	if len(cols) == 0 {
		return nil, fmt.Errorf("no valid column definitions")
	}

	if !hasPK {
		cols[0].isPK = true
	}

	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.name
	}

	schema := relation.NewSchema(names...).WithQualifiedNames(tableName)
	for i, c := range cols {
		schema.Columns[i].Type = c.typ
	}

	pkIndices := make([]int, 0, len(cols))
	for i, c := range cols {
		if c.isPK {
			pkIndices = append(pkIndices, i)
		}
	}

	return schema.WithPrimaryKey(pkIndices...), nil
}

func splitOnComma(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i, ch := range s {
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

func parseColumnType(typeName string) relation.ColumnType {
	t := strings.ToUpper(typeName)
	t = regexp.MustCompile(`\(.*\)$`).ReplaceAllString(t, "")

	switch t {
	case "INT", "INTEGER", "BIGINT", "SMALLINT":
		return relation.TypeInt
	case "FLOAT", "REAL", "DOUBLE", "NUMERIC", "DECIMAL":
		return relation.TypeFloat
	case "TEXT", "VARCHAR", "CHAR", "STRING":
		return relation.TypeString
	default:
		return relation.TypeAny
	}
}
