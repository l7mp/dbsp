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

	if len(call.Arguments) < 2 {
		return nil, fmt.Errorf("sql.compile options.output is required")
	}

	options := struct {
		Output any `json:"output"`
	}{}
	binding := sqlOutputBinding{}
	if err := decodeOptionValue(call.Argument(1), &options); err != nil {
		return nil, fmt.Errorf("sql.compile options: %w", err)
	}
	parsed, err := parseSQLOutputBinding(options.Output)
	if err != nil {
		return nil, fmt.Errorf("sql.compile options.output: %w", err)
	}
	if parsed.Name == "" {
		return nil, fmt.Errorf("sql.compile options.output.name is required")
	}
	binding = parsed

	compiled, err := compilersql.New(v.db).CompileString(query)
	if err != nil {
		return nil, fmt.Errorf("sql.compile: %w", err)
	}

	originalOutputMap := compiled.OutputMap
	originalOutputLogicalMap := compiled.OutputLogicalMap

	outputMap, err := renameOutputMap(binding.Name, compiled.OutputMap)
	if err != nil {
		return nil, fmt.Errorf("sql.compile: %w", err)
	}

	compiled.OutputMap = outputMap
	compiled.OutputLogicalMap = remapOutputLogicalMap(originalOutputMap, originalOutputLogicalMap, outputMap, binding.Name, binding.Logical)
	h := &circuitHandle{c: compiled.Circuit, query: compiled, vm: v}
	return h.jsObject(), nil
}

type sqlOutputBinding struct {
	Name    string
	Logical string
}

func parseSQLOutputBinding(raw any) (sqlOutputBinding, error) {
	if raw == nil {
		return sqlOutputBinding{}, nil
	}

	switch out := raw.(type) {
	case string:
		if out == "" {
			return sqlOutputBinding{}, nil
		}
		return sqlOutputBinding{Name: out, Logical: "output"}, nil
	case map[string]any:
		name := ""
		if x, ok := out["name"]; ok {
			s, ok := x.(string)
			if !ok {
				return sqlOutputBinding{}, fmt.Errorf("field 'name' must be a string")
			}
			name = s
		}
		logical := "output"
		if x, ok := out["logicalName"]; ok {
			s, ok := x.(string)
			if !ok {
				return sqlOutputBinding{}, fmt.Errorf("field 'logicalName' must be a string")
			}
			logical = s
		}
		if x, ok := out["logical"]; ok {
			s, ok := x.(string)
			if !ok {
				return sqlOutputBinding{}, fmt.Errorf("field 'logical' must be a string")
			}
			logical = s
		}
		return sqlOutputBinding{Name: name, Logical: logical}, nil
	default:
		return sqlOutputBinding{}, fmt.Errorf("must be a string or binding object")
	}
}

func remapOutputLogicalMap(originalOutputMap, originalLogicalMap, renamedOutputMap map[string]string, baseName, baseLogical string) map[string]string {
	logicalByNode := make(map[string]string, len(originalOutputMap))
	for oldName, nodeID := range originalOutputMap {
		logical := oldName
		if originalLogicalMap != nil {
			if l, ok := originalLogicalMap[oldName]; ok && l != "" {
				logical = l
			}
		}
		logicalByNode[nodeID] = logical
	}

	out := make(map[string]string, len(renamedOutputMap))
	for newName, nodeID := range renamedOutputMap {
		logical := logicalByNode[nodeID]
		if logical == "" {
			logical = newName
		}
		if newName == baseName {
			logical = baseLogical
		}
		out[newName] = logical
	}
	return out
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
