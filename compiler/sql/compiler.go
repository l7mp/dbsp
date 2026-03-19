package sql

import (
	"fmt"
	"sort"

	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"

	"github.com/l7mp/dbsp/compiler"
	"github.com/l7mp/dbsp/datamodel/relation"
	"github.com/l7mp/dbsp/dbsp/circuit"
	"github.com/l7mp/dbsp/dbsp/operator"
	"github.com/l7mp/dbsp/expression"
	"github.com/l7mp/dbsp/expression/dbsp"
)

// Compiler compiles SQL statements into DBSP circuits.
type Compiler struct {
	db *relation.Database
}

// NewCompiler creates a new SQL compiler with the given catalog.
func NewCompiler(db *relation.Database) *Compiler {
	return &Compiler{db: db}
}

// Parse parses SQL source into normalized SQL IR.
func (c *Compiler) Parse(source []byte) (compiler.IR, error) {
	return Normalize(string(source), c.db)
}

// ParseString parses SQL source into normalized SQL IR.
func (c *Compiler) ParseString(source string) (compiler.IR, error) {
	return c.Parse([]byte(source))
}

// CompileString is a convenience wrapper that parses then compiles.
func (c *Compiler) CompileString(source string) (*compiler.Query, error) {
	ir, err := c.ParseString(source)
	if err != nil {
		return nil, err
	}
	return c.Compile(ir)
}

// Compile compiles parsed SQL IR into a DBSP query.
func (c *Compiler) Compile(ir compiler.IR) (*compiler.Query, error) {
	normalized, ok := ir.(*NormalizedQuery)
	if !ok {
		return nil, fmt.Errorf("sql: expected IR kind %q, got %T", (&NormalizedQuery{}).IRKind(), ir)
	}
	return c.compileSelect(normalized.Stmt, normalized.BindVars)
}

func (c *Compiler) compileSelect(sel *sqlparser.Select, bindVars map[string]*querypb.BindVariable) (*compiler.Query, error) {
	if sel.From == nil || len(sel.From) != 1 {
		return nil, UnimplementedError{Feature: "select with multiple FROM items"}
	}

	compiledCircuit := circuit.New("sql")
	inputMap := make(map[string]string)
	rootID, err := c.compileFrom(sel.From[0], compiledCircuit, inputMap)
	if err != nil {
		return nil, err
	}
	current := rootID

	if sel.Where != nil {
		predicate, err := CompilePredicate(sel.Where.Expr, bindVars)
		if err != nil {
			return nil, err
		}
		selectOp := operator.NewSelect(predicate)
		selectID := "select"
		if err := compiledCircuit.AddNode(circuit.Op(selectID, selectOp)); err != nil {
			return nil, err
		}
		if err := compiledCircuit.AddEdge(circuit.NewEdge(current, selectID, 0)); err != nil {
			return nil, err
		}
		current = selectID
	}

	current, err = c.addProjection(sel.SelectExprs, compiledCircuit, current, bindVars)
	if err != nil {
		return nil, err
	}

	outputID := "output"
	if err := compiledCircuit.AddNode(circuit.Output(outputID)); err != nil {
		return nil, err
	}
	if err := compiledCircuit.AddEdge(circuit.NewEdge(current, outputID, 0)); err != nil {
		return nil, err
	}

	return &compiler.Query{
		Circuit:  compiledCircuit,
		InputMap: inputMap,
		OutputMap: map[string]string{
			"output": outputID,
		},
	}, nil
}

func (c *Compiler) compileFrom(expr sqlparser.TableExpr, compiledCircuit *circuit.Circuit, inputMap map[string]string) (string, error) {
	switch t := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return c.compileTable(t, compiledCircuit, inputMap)
	case *sqlparser.JoinTableExpr:
		return c.compileJoin(t, compiledCircuit, inputMap)
	default:
		return "", UnimplementedError{Feature: fmt.Sprintf("from %T", expr)}
	}
}

func (c *Compiler) compileTable(expr *sqlparser.AliasedTableExpr, compiledCircuit *circuit.Circuit, inputMap map[string]string) (string, error) {
	nameExpr, ok := expr.Expr.(sqlparser.TableName)
	if !ok {
		return "", UnimplementedError{Feature: fmt.Sprintf("from expr %T", expr.Expr)}
	}
	baseName := nameExpr.Name.String()
	if baseName == "" {
		return "", fmt.Errorf("empty table name")
	}
	if c.db != nil {
		if _, err := c.db.GetTable(baseName); err != nil {
			return "", err
		}
	}

	logicalName := baseName
	if !expr.As.IsEmpty() {
		logicalName = expr.As.String()
	}
	inputID := fmt.Sprintf("input_%s", logicalName)
	if err := compiledCircuit.AddNode(circuit.Input(inputID)); err != nil {
		return "", err
	}
	inputMap[logicalName] = inputID
	return inputID, nil
}

func (c *Compiler) compileJoin(expr *sqlparser.JoinTableExpr, compiledCircuit *circuit.Circuit, inputMap map[string]string) (string, error) {
	if expr.Join != sqlparser.JoinStr && expr.Join != sqlparser.StraightJoinStr {
		return "", UnimplementedError{Feature: fmt.Sprintf("join %q", expr.Join)}
	}
	leftID, err := c.compileFrom(expr.LeftExpr, compiledCircuit, inputMap)
	if err != nil {
		return "", err
	}
	rightID, err := c.compileFrom(expr.RightExpr, compiledCircuit, inputMap)
	if err != nil {
		return "", err
	}
	if expr.Condition.On == nil {
		return "", UnimplementedError{Feature: "join without ON"}
	}
	predicate, err := CompilePredicate(expr.Condition.On, nil)
	if err != nil {
		return "", err
	}

	productID := fmt.Sprintf("product_%d", len(compiledCircuit.Nodes()))
	selectID := fmt.Sprintf("select_%d", len(compiledCircuit.Nodes())+1)

	if err := compiledCircuit.AddNode(circuit.Op(productID, operator.NewCartesianProduct())); err != nil {
		return "", err
	}
	if err := compiledCircuit.AddNode(circuit.Op(selectID, operator.NewSelect(predicate))); err != nil {
		return "", err
	}
	if err := compiledCircuit.AddEdge(circuit.NewEdge(leftID, productID, 0)); err != nil {
		return "", err
	}
	if err := compiledCircuit.AddEdge(circuit.NewEdge(rightID, productID, 1)); err != nil {
		return "", err
	}
	if err := compiledCircuit.AddEdge(circuit.NewEdge(productID, selectID, 0)); err != nil {
		return "", err
	}
	return selectID, nil
}

func (c *Compiler) addProjection(selectExprs sqlparser.SelectExprs, compiledCircuit *circuit.Circuit, inputID string, bindVars map[string]*querypb.BindVariable) (string, error) {
	if len(selectExprs) == 0 {
		return inputID, nil
	}
	if len(selectExprs) == 1 {
		if _, ok := selectExprs[0].(*sqlparser.StarExpr); ok {
			return inputID, nil
		}
	}
	for _, expr := range selectExprs {
		if _, ok := expr.(*sqlparser.StarExpr); ok {
			return "", UnimplementedError{Feature: "mixed select with *"}
		}
	}

	projection, err := compileProjection(selectExprs, bindVars)
	if err != nil {
		return "", err
	}
	projectID := fmt.Sprintf("project_%d", len(compiledCircuit.Nodes()))
	if err := compiledCircuit.AddNode(circuit.Op(projectID, operator.NewProject(projection))); err != nil {
		return "", err
	}
	if err := compiledCircuit.AddEdge(circuit.NewEdge(inputID, projectID, 0)); err != nil {
		return "", err
	}
	return projectID, nil
}

func compileProjection(selectExprs sqlparser.SelectExprs, bindVars map[string]*querypb.BindVariable) (expression.Expression, error) {
	entries := make(map[string]dbsp.Expression)
	for i, expr := range selectExprs {
		aliased, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, UnimplementedError{Feature: fmt.Sprintf("select expr %T", expr)}
		}
		switch inner := aliased.Expr.(type) {
		case *sqlparser.ColName:
			if aliased.As.IsEmpty() {
				entries[inner.Name.String()] = dbsp.NewGet(fieldName(inner))
				continue
			}
			entries[aliased.As.String()] = dbsp.NewGet(fieldName(inner))
		default:
			exprRoot, err := compileExpr(aliased.Expr, bindVars)
			if err != nil {
				return nil, err
			}
			name := aliased.As.String()
			if name == "" {
				name = fmt.Sprintf("col_%d", i)
			}
			entries[name] = exprRoot
		}
	}
	// Sort keys for deterministic evaluation order.
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return expression.Func(func(ctx *expression.EvalContext) (any, error) {
		if ctx == nil || ctx.Document() == nil {
			return nil, fmt.Errorf("projection: missing document")
		}
		newDoc := ctx.Document().New()
		// Evaluate each expression in the original input context so that
		// field lookups read from the input document, not from the (empty)
		// output document.
		for _, key := range keys {
			val, err := entries[key].Evaluate(ctx)
			if err != nil {
				return nil, fmt.Errorf("projection %s: %w", key, err)
			}
			if err := newDoc.SetField(key, val); err != nil {
				return nil, fmt.Errorf("set field %s: %w", key, err)
			}
		}
		return newDoc, nil
	}), nil
}

func fieldName(col *sqlparser.ColName) string {
	name := col.Name.String()
	if qualifier := col.Qualifier.Name.String(); qualifier != "" {
		name = qualifier + "." + name
	}
	return name
}

var _ compiler.Compiler = (*Compiler)(nil)
