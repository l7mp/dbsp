package main

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/xwb1989/sqlparser"

	compilersql "github.com/l7mp/dbsp/engine/compiler/sql"
	"github.com/l7mp/dbsp/engine/datamodel/relation"
	"github.com/l7mp/dbsp/engine/executor"
	"github.com/l7mp/dbsp/engine/transform"
	"github.com/l7mp/dbsp/engine/zset"
)

func sqlRootCommand(state *appState) *cobra.Command {
	root := &cobra.Command{
		Use:   "sql",
		Short: "SQL table and query management",
	}
	for _, cmd := range sqlCommands(state) {
		root.AddCommand(cmd)
	}
	return root
}

func sqlCommands(state *appState) []*cobra.Command {
	createCmd := &cobra.Command{
		Use:   "create <name> <SQL...>",
		Short: "Create a named SQL statement",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			stmt := strings.Join(args[1:], " ")
			out, err := cmd.Flags().GetString("output")
			if err != nil {
				return err
			}
			if _, err := sqlparser.Parse(stmt); err != nil {
				return fmt.Errorf("parse: %w", err)
			}
			if out == "" {
				out = name + "-output"
			}
			state.sql[name] = sqlSpec{Source: stmt, Output: out}
			state.logger.V(1).Info("sql statement created", "name", name)
			return nil
		},
	}
	createCmd.Flags().String("output", "", "Consumer topic for compiled circuit output (default: <name>-output)")

	tableCmd := &cobra.Command{Use: "table", Short: "SQL table DDL"}
	tableCreateCmd := &cobra.Command{
		Use:   "create <TOKEN>...",
		Short: "Execute a CREATE TABLE statement",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return handleCreateTable("CREATE "+strings.Join(args, " "), state)
		},
	}
	tableDropCmd := &cobra.Command{
		Use:   "drop <TOKEN>...",
		Short: "Execute a DROP TABLE statement",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return handleDrop("DROP "+strings.Join(args, " "), state)
		},
	}
	tableCmd.AddCommand(tableCreateCmd, tableDropCmd)

	compileCmd := &cobra.Command{
		Use:   "compile <sql-name> <circuit-name>",
		Short: "Compile a named SQL statement into a circuit",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			sqlName, circuitName := args[0], args[1]
			spec, ok := state.sql[sqlName]
			if !ok {
				return fmt.Errorf("sql statement %s not found", sqlName)
			}
			if _, exists := state.circuits[circuitName]; exists {
				return fmt.Errorf("circuit %s already exists", circuitName)
			}
			q, err := compilersql.NewCompiler(state.db).CompileString(spec.Source)
			if err != nil {
				return fmt.Errorf("compile: %w", err)
			}
			renamed, err := renameSQLOutputs(spec.Output, q.OutputMap)
			if err != nil {
				return err
			}
			q.OutputMap = renamed
			state.circuits[circuitName] = q.Circuit
			state.queries[circuitName] = q
			state.logger.V(1).Info("sql compile output", "statement", sqlName, "output", spec.Output)
			state.logger.V(1).Info("sql compiled", "statement", sqlName, "circuit", circuitName)
			return nil
		},
	}

	getCmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Print a named SQL statement",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spec, ok := state.sql[args[0]]
			if !ok {
				return fmt.Errorf("sql statement %s not found", args[0])
			}
			fmt.Fprintln(os.Stdout, spec.Source)
			return nil
		},
	}

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List named SQL statements",
		Run: func(cmd *cobra.Command, args []string) {
			if len(state.sql) == 0 {
				fmt.Fprintln(os.Stdout, "(no sql statements)")
				return
			}
			names := make([]string, 0, len(state.sql))
			for n := range state.sql {
				names = append(names, n)
			}
			sort.Strings(names)
			for _, n := range names {
				fmt.Fprintln(os.Stdout, n)
			}
		},
	}

	deleteCmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a named SQL statement",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if _, ok := state.sql[args[0]]; !ok {
				return fmt.Errorf("sql statement %s not found", args[0])
			}
			delete(state.sql, args[0])
			return nil
		},
	}

	insertCmd := &cobra.Command{
		Use:   "insert <TOKEN>...",
		Short: "Execute an INSERT INTO statement",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return handleInsert("INSERT "+strings.Join(args, " "), state)
		},
	}

	selectCmd := &cobra.Command{
		Use:   "select <TOKEN>...",
		Short: "Compile and execute a SELECT query",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			saveName, err := cmd.Flags().GetString("save")
			if err != nil {
				return err
			}
			return handleSelect("SELECT "+strings.Join(args, " "), saveName, state)
		},
	}
	selectCmd.Flags().String("save", "", "Save compiled circuit under this name")

	tablesCmd := &cobra.Command{
		Use:   "tables",
		Short: "List all tables in the database",
		Run: func(cmd *cobra.Command, args []string) {
			names := state.db.Tables()
			if len(names) == 0 {
				fmt.Fprintln(os.Stdout, "(no tables)")
				return
			}
			for _, name := range names {
				fmt.Fprintln(os.Stdout, name)
			}
		},
	}

	describeCmd := &cobra.Command{
		Use:   "schema <table>",
		Short: "Show schema of a table",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			table, err := state.db.GetTable(args[0])
			if err != nil {
				return err
			}
			for _, col := range table.Schema.Columns {
				fmt.Fprintf(os.Stdout, "  %-20s  %s\n", col.Name, columnTypeName(col.Type))
			}
			return nil
		},
	}

	evalCmd := &cobra.Command{
		Use:   "eval <SQL-tokens...>",
		Short: "Compile and evaluate a full SQL query",
		Long: `Compile and evaluate a complete SQL statement.

Tokens are joined with spaces to form the query, so the full SELECT keyword must
be included: sql eval SELECT * FROM t WHERE id > 1.

Flags:
  --save <name>       Save the base (non-incremental) compiled circuit.
  --save-zset <name>  Store the output Z-set(s) in the registry.
  --incr              Run the incremental version of the circuit.`,
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("SQL query required")
			}
			saveName, err := cmd.Flags().GetString("save")
			if err != nil {
				return err
			}
			saveZSet, err := cmd.Flags().GetString("save-zset")
			if err != nil {
				return err
			}
			incr, err := cmd.Flags().GetBool("incr")
			if err != nil {
				return err
			}
			return handleEval(strings.Join(args, " "), saveName, saveZSet, incr, state)
		},
	}
	evalCmd.Flags().String("save", "", "Save compiled circuit under this name")
	evalCmd.Flags().String("save-zset", "", "Store output documents in a named Z-set")
	evalCmd.Flags().Bool("incr", false, "Run the incremental version of the circuit")

	return []*cobra.Command{
		tableCmd,
		createCmd,
		compileCmd,
		getCmd,
		listCmd,
		deleteCmd,
		insertCmd,
		selectCmd,
		evalCmd,
		tablesCmd,
		describeCmd,
	}
}

func renameSQLOutputs(outputBase string, out map[string]string) (map[string]string, error) {
	if len(out) == 0 {
		return out, nil
	}
	renamed := make(map[string]string, len(out))
	if nodeID, ok := out["output"]; ok {
		renamed[outputBase] = nodeID
	}
	for logical, nodeID := range out {
		if logical == "output" {
			continue
		}
		key := outputBase + "-" + logical
		if _, exists := renamed[key]; exists {
			return nil, fmt.Errorf("duplicate output key %s", key)
		}
		renamed[key] = nodeID
	}
	if len(renamed) == 0 {
		return nil, fmt.Errorf("sql compile: missing output mapping")
	}
	return renamed, nil
}

// handleCreateTable parses and executes a CREATE TABLE SQL statement.
func handleCreateTable(sql string, state *appState) error {
	re := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(\w+)\s*\((.+)\)\s*$`)
	m := re.FindStringSubmatch(strings.TrimSpace(sql))
	if m == nil {
		return fmt.Errorf("invalid CREATE TABLE syntax: expected CREATE TABLE <name> (<columns>)")
	}
	tableName := m[1]
	colDefs := m[2]
	if _, err := state.db.GetTable(tableName); err == nil {
		return fmt.Errorf("table %s already exists", tableName)
	}
	schema, err := parseColumnDefs(tableName, colDefs)
	if err != nil {
		return err
	}
	table := relation.NewTable(tableName, schema)
	state.db.RegisterTable(tableName, table)
	state.logger.V(1).Info("table created", "name", tableName)
	return nil
}

// handleInsert parses and executes an INSERT INTO SQL statement.
func handleInsert(sql string, state *appState) error {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}
	insert, ok := stmt.(*sqlparser.Insert)
	if !ok {
		return fmt.Errorf("expected INSERT statement")
	}
	tableName := insert.Table.Name.String()
	table, err := state.db.GetTable(tableName)
	if err != nil {
		return err
	}
	rows, ok := insert.Rows.(sqlparser.Values)
	if !ok {
		return fmt.Errorf("expected VALUES clause")
	}

	// Determine column order from the INSERT column list (positional if absent).
	colNames := make([]string, len(insert.Columns))
	for i, col := range insert.Columns {
		colNames[i] = col.Lowered()
	}

	for _, row := range rows {
		data := make([]any, len(table.Schema.Columns))
		if len(colNames) > 0 {
			// Named columns: match values by column name.
			if len(row) != len(colNames) {
				return fmt.Errorf("value count %d does not match column count %d", len(row), len(colNames))
			}
			for i, name := range colNames {
				idx := -1
				for j, col := range table.Schema.Columns {
					if strings.EqualFold(col.Name, name) {
						idx = j
						break
					}
				}
				if idx < 0 {
					return fmt.Errorf("unknown column %s", name)
				}
				val, err := parseSQLValue(row[i], table.Schema.Columns[idx].Type)
				if err != nil {
					return fmt.Errorf("column %s: %w", name, err)
				}
				data[idx] = val
			}
		} else {
			// Positional: must match column count exactly.
			if len(row) != len(table.Schema.Columns) {
				return fmt.Errorf("value count %d does not match column count %d", len(row), len(table.Schema.Columns))
			}
			for i, expr := range row {
				val, err := parseSQLValue(expr, table.Schema.Columns[i].Type)
				if err != nil {
					return fmt.Errorf("column %s: %w", table.Schema.Columns[i].Name, err)
				}
				data[i] = val
			}
		}
		if err := table.Insert(data); err != nil {
			return err
		}
	}
	fmt.Fprintf(os.Stdout, "%d row(s) inserted into %s.\n", len(rows), tableName)
	return nil
}

// handleSelect compiles and executes a SELECT query, printing results to stdout.
// If saveName is non-empty, the compiled circuit is stored in state.circuits.
func handleSelect(sql string, saveName string, state *appState) error {
	q, err := compilersql.NewCompiler(state.db).CompileString(sql)
	if err != nil {
		return fmt.Errorf("compile: %w", err)
	}

	// Build inputs from database tables.
	inputs := make(map[string]zset.ZSet)
	for tableName, nodeID := range q.InputMap {
		table, err := state.db.GetTable(tableName)
		if err != nil {
			return fmt.Errorf("input table %s: %w", tableName, err)
		}
		inputs[nodeID] = table.ToZSet()
	}

	// Execute the circuit.
	exec, err := executor.New(q.Circuit, state.logger)
	if err != nil {
		return fmt.Errorf("executor: %w", err)
	}
	outputs, err := exec.Execute(inputs)
	if err != nil {
		return fmt.Errorf("execute: %w", err)
	}

	// Print results for each output node.
	for _, outName := range q.OutputNames() {
		nodeID := q.OutputMap[outName]
		z := outputs[nodeID]
		elems := z.Entries()
		sort.Slice(elems, func(i, j int) bool {
			return elems[i].Document.Hash() < elems[j].Document.Hash()
		})
		for i, elem := range elems {
			b, err := json.Marshal(elem.Document)
			if err != nil {
				b = []byte(elem.Document.String())
			}
			fmt.Fprintf(os.Stdout, "[%d]  %s  weight=%+d\n", i+1, string(b), elem.Weight)
		}
	}

	// Optionally save the compiled circuit.
	if saveName != "" {
		if _, exists := state.circuits[saveName]; exists {
			return fmt.Errorf("circuit %s already exists", saveName)
		}
		state.circuits[saveName] = q.Circuit
		state.queries[saveName] = q
		fmt.Fprintf(os.Stdout, "Circuit saved as %s.\n", saveName)
	}

	return nil
}

// handleEval compiles and executes a complete SQL statement, optionally using
// the incremental version of the compiled circuit.
// saveName, if non-empty, stores the base (non-incremental) circuit.
// saveZSet, if non-empty, stores the output Z-set(s) in state.zsets.
func handleEval(sql, saveName, saveZSet string, incr bool, state *appState) error {
	q, err := compilersql.NewCompiler(state.db).CompileString(sql)
	if err != nil {
		return fmt.Errorf("compile: %w", err)
	}

	// Choose the circuit to execute (optionally incrementalized).
	execCircuit := q.Circuit
	if incr {
		execCircuit, err = transform.Incrementalize(q.Circuit)
		if err != nil {
			return fmt.Errorf("incrementalize: %w", err)
		}
	}

	// Build inputs from database tables.
	inputs := make(map[string]zset.ZSet)
	for tableName, nodeID := range q.InputMap {
		table, err := state.db.GetTable(tableName)
		if err != nil {
			return fmt.Errorf("input table %s: %w", tableName, err)
		}
		inputs[nodeID] = table.ToZSet()
	}

	// Execute the circuit.
	exec, err := executor.New(execCircuit, state.logger)
	if err != nil {
		return fmt.Errorf("executor: %w", err)
	}
	outputs, err := exec.Execute(inputs)
	if err != nil {
		return fmt.Errorf("execute: %w", err)
	}

	// Print results and optionally save each output Z-set.
	outNames := q.OutputNames()
	for _, outName := range outNames {
		nodeID := q.OutputMap[outName]
		z := outputs[nodeID]
		elems := z.Entries()
		sort.Slice(elems, func(i, j int) bool {
			return elems[i].Document.Hash() < elems[j].Document.Hash()
		})
		for i, elem := range elems {
			b, err := json.Marshal(elem.Document)
			if err != nil {
				b = []byte(elem.Document.String())
			}
			fmt.Fprintf(os.Stdout, "[%d]  %s  weight=%+d\n", i+1, string(b), elem.Weight)
		}

		if saveZSet != "" {
			key := saveZSet
			if len(outNames) > 1 {
				key = saveZSet + "-" + outName
			}
			if _, exists := state.zsets[key]; exists {
				return fmt.Errorf("zset %s already exists", key)
			}
			state.zsets[key] = &boundZSet{data: z}
			fmt.Fprintf(os.Stdout, "Output stored as %s.\n", key)
		}
	}

	// Save the base circuit (not the incremental one).
	if saveName != "" {
		if _, exists := state.circuits[saveName]; exists {
			return fmt.Errorf("circuit %s already exists", saveName)
		}
		state.circuits[saveName] = q.Circuit
		state.queries[saveName] = q
		fmt.Fprintf(os.Stdout, "Circuit saved as %s.\n", saveName)
	}

	return nil
}

// handleDrop parses and executes a DROP TABLE statement.
func handleDrop(sql string, state *appState) error {
	re := regexp.MustCompile(`(?i)TABLE\s+(\w+)`)
	m := re.FindStringSubmatch(sql)
	if m == nil {
		return fmt.Errorf("invalid DROP TABLE syntax: expected DROP TABLE <name>")
	}
	tableName := m[1]
	if err := state.db.DropTable(tableName); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "Table %s dropped.\n", tableName)
	return nil
}

// parseColumnDefs parses a comma-separated list of column definitions.
// Each definition is "<name> [TYPE] [PRIMARY KEY]". If no PRIMARY KEY clause
// is present, the first column is used as the primary key by default.
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
		// Detect "PRIMARY KEY" annotation anywhere in the definition.
		if strings.Contains(strings.ToUpper(part), "PRIMARY KEY") {
			cd.isPK = true
			hasPK = true
		}
		cols = append(cols, cd)
	}

	if len(cols) == 0 {
		return nil, fmt.Errorf("no valid column definitions")
	}

	// Default: first column is primary key when none is explicitly specified.
	if !hasPK {
		cols[0].isPK = true
	}

	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.name
	}

	schema := relation.NewSchema(names...).WithQualifiedNames(tableName)

	// Apply column types.
	for i, c := range cols {
		schema.Columns[i].Type = c.typ
	}

	// Collect primary key column indices.
	pkIndices := make([]int, 0, len(cols))
	for i, c := range cols {
		if c.isPK {
			pkIndices = append(pkIndices, i)
		}
	}
	schema = schema.WithPrimaryKey(pkIndices...)

	return schema, nil
}

// splitOnComma splits s on commas, ignoring commas inside parentheses.
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

// parseColumnType maps a SQL type keyword to a relation.ColumnType.
func parseColumnType(typeName string) relation.ColumnType {
	switch strings.ToUpper(typeName) {
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

// columnTypeName returns a human-readable name for a ColumnType.
func columnTypeName(t relation.ColumnType) string {
	switch t {
	case relation.TypeInt:
		return "INT"
	case relation.TypeFloat:
		return "FLOAT"
	case relation.TypeString:
		return "TEXT"
	default:
		return "ANY"
	}
}

// parseSQLValue converts a sqlparser expression to a Go value.
func parseSQLValue(expr sqlparser.Expr, colType relation.ColumnType) (any, error) {
	switch v := expr.(type) {
	case *sqlparser.SQLVal:
		switch v.Type {
		case sqlparser.IntVal:
			n, err := strconv.ParseInt(string(v.Val), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid integer %q: %w", v.Val, err)
			}
			if colType == relation.TypeFloat {
				return float64(n), nil
			}
			return n, nil
		case sqlparser.FloatVal:
			f, err := strconv.ParseFloat(string(v.Val), 64)
			if err != nil {
				return nil, fmt.Errorf("invalid float %q: %w", v.Val, err)
			}
			return f, nil
		case sqlparser.StrVal:
			return string(v.Val), nil
		default:
			return string(v.Val), nil
		}
	case *sqlparser.NullVal:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported expression type %T", expr)
	}
}
