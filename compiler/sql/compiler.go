package sql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"

	"github.com/l7mp/dbsp/compiler"
	"github.com/l7mp/dbsp/datamodel"
	"github.com/l7mp/dbsp/datamodel/relation"
	"github.com/l7mp/dbsp/dbsp/circuit"
	"github.com/l7mp/dbsp/dbsp/exprutil"
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

// Compile parses and compiles a SQL statement.
func (c *Compiler) Compile(source []byte) (*compiler.CompiledQuery, error) {
	stmt, err := sqlparser.Parse(string(source))
	if err != nil {
		return nil, err
	}
	// Normalize binds literals to generated bind vars. We ignore bindVars for now,
	// which means CompileExpression will not see literal values after normalization.
	bindVars := make(map[string]*querypb.BindVariable)
	// TODO: Normalization converts literals to bind vars, which loses literal
	// values for CompileExpression. We ignore bindVars for now.
	sqlparser.Normalize(stmt, bindVars, "v")

	switch s := stmt.(type) {
	case *sqlparser.Select:
		return c.compileSelect(s)
	default:
		return nil, UnimplementedError{Feature: fmt.Sprintf("statement %T", stmt)}
	}
}

// CompileString is a convenience wrapper for string input.
func (c *Compiler) CompileString(source string) (*compiler.CompiledQuery, error) {
	return c.Compile([]byte(source))
}

func (c *Compiler) compileSelect(sel *sqlparser.Select) (*compiler.CompiledQuery, error) {
	if sel.From == nil || len(sel.From) != 1 {
		return nil, UnimplementedError{Feature: "select with multiple FROM items"}
	}

	compiledCircuit := circuit.New("sql")
	inputMap := make(map[string]string)
	aliasMap := make(map[string]string)

	rootID, err := c.compileFrom(sel.From[0], compiledCircuit, inputMap, aliasMap)
	if err != nil {
		return nil, err
	}
	current := rootID

	if sel.Where != nil {
		predicate, err := CompilePredicateWithAliases(sel.Where.Expr, aliasMap)
		if err != nil {
			return nil, err
		}
		selectOp := operator.NewSelect("where", predicate)
		selectID := "select"
		if err := compiledCircuit.AddNode(circuit.Op(selectID, selectOp)); err != nil {
			return nil, err
		}
		if err := compiledCircuit.AddEdge(circuit.NewEdge(current, selectID, 0)); err != nil {
			return nil, err
		}
		current = selectID
	}

	current, err = c.addProjection(sel.SelectExprs, compiledCircuit, current, aliasMap)
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

	return &compiler.CompiledQuery{
		Circuit:  compiledCircuit,
		InputMap: inputMap,
		OutputMap: map[string]string{
			"output": outputID,
		},
	}, nil
}

func (c *Compiler) compileFrom(expr sqlparser.TableExpr, compiledCircuit *circuit.Circuit, inputMap map[string]string, aliasMap map[string]string) (string, error) {
	switch t := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		return c.compileTable(t, compiledCircuit, inputMap, aliasMap)
	case *sqlparser.JoinTableExpr:
		return c.compileJoin(t, compiledCircuit, inputMap, aliasMap)
	default:
		return "", UnimplementedError{Feature: fmt.Sprintf("from %T", expr)}
	}
}

func (c *Compiler) compileTable(expr *sqlparser.AliasedTableExpr, compiledCircuit *circuit.Circuit, inputMap map[string]string, aliasMap map[string]string) (string, error) {
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
		aliasMap[logicalName] = baseName
	}
	inputID := fmt.Sprintf("input_%s", logicalName)
	if err := compiledCircuit.AddNode(circuit.Input(inputID)); err != nil {
		return "", err
	}
	inputMap[logicalName] = inputID
	return inputID, nil
}

func (c *Compiler) compileJoin(expr *sqlparser.JoinTableExpr, compiledCircuit *circuit.Circuit, inputMap map[string]string, aliasMap map[string]string) (string, error) {
	if expr.Join != sqlparser.JoinStr && expr.Join != sqlparser.StraightJoinStr {
		return "", UnimplementedError{Feature: fmt.Sprintf("join %q", expr.Join)}
	}
	leftID, err := c.compileFrom(expr.LeftExpr, compiledCircuit, inputMap, aliasMap)
	if err != nil {
		return "", err
	}
	rightID, err := c.compileFrom(expr.RightExpr, compiledCircuit, inputMap, aliasMap)
	if err != nil {
		return "", err
	}
	if expr.Condition.On == nil {
		return "", UnimplementedError{Feature: "join without ON"}
	}
	predicate, err := CompilePredicateWithAliases(expr.Condition.On, aliasMap)
	if err != nil {
		return "", err
	}

	productID := fmt.Sprintf("product_%d", len(compiledCircuit.Nodes()))
	selectID := fmt.Sprintf("select_%d", len(compiledCircuit.Nodes())+1)

	if err := compiledCircuit.AddNode(circuit.Op(productID, operator.NewCartesianProduct("×"))); err != nil {
		return "", err
	}
	if err := compiledCircuit.AddNode(circuit.Op(selectID, operator.NewSelect("σ", predicate))); err != nil {
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

func (c *Compiler) addProjection(selectExprs sqlparser.SelectExprs, compiledCircuit *circuit.Circuit, inputID string, aliasMap map[string]string) (string, error) {
	if len(selectExprs) == 0 {
		return inputID, nil
	}
	if len(selectExprs) == 1 {
		if star, ok := selectExprs[0].(*sqlparser.StarExpr); ok {
			projection, err := compileProjectionStar(star, aliasMap)
			if err != nil {
				return "", err
			}
			projectID := fmt.Sprintf("project_%d", len(compiledCircuit.Nodes()))
			wrapped := wrapProjectionWithSQLBool(projection)
			if err := compiledCircuit.AddNode(circuit.Op(projectID, operator.NewProject("project", wrapped))); err != nil {
				return "", err
			}
			if err := compiledCircuit.AddEdge(circuit.NewEdge(inputID, projectID, 0)); err != nil {
				return "", err
			}
			return projectID, nil
		}
	}
	for _, expr := range selectExprs {
		if _, ok := expr.(*sqlparser.StarExpr); ok {
			return "", UnimplementedError{Feature: "mixed select with *"}
		}
	}

	projection, err := compileProjection(selectExprs, aliasMap)
	if err != nil {
		return "", err
	}
	projectID := fmt.Sprintf("project_%d", len(compiledCircuit.Nodes()))
	wrapped := wrapProjectionWithSQLBool(projection)
	if err := compiledCircuit.AddNode(circuit.Op(projectID, operator.NewProject("project", wrapped))); err != nil {
		return "", err
	}
	if err := compiledCircuit.AddEdge(circuit.NewEdge(inputID, projectID, 0)); err != nil {
		return "", err
	}
	return projectID, nil
}

func compileProjection(selectExprs sqlparser.SelectExprs, aliasMap map[string]string) (expression.Expression, error) {
	entries := make(map[string]dbsp.Expr)
	for i, expr := range selectExprs {
		aliased, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, UnimplementedError{Feature: fmt.Sprintf("select expr %T", expr)}
		}
		switch inner := aliased.Expr.(type) {
		case *sqlparser.ColName:
			exprRoot, err := compileExpr(inner, aliasMap)
			if err != nil {
				return nil, err
			}
			name := aliased.As.String()
			if name == "" {
				name = inner.Name.String()
			}
			if name == "" {
				name = fmt.Sprintf("col_%d", i)
			}
			entries[name] = exprRoot
		default:
			exprRoot, err := compileExpr(aliased.Expr, aliasMap)
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
	root := dbsp.NewOpExpr(&dbsp.DictOp{}, dbsp.DictArgs{Entries: entries})
	return dbsp.NewExpression(root), nil
}

func compileProjectionStar(star *sqlparser.StarExpr, aliasMap map[string]string) (expression.Expression, error) {
	entries := make(map[string]dbsp.Expr)
	qualifier := star.TableName.Name.String()
	for alias, table := range aliasMap {
		if qualifier != "" && !strings.EqualFold(alias, qualifier) {
			continue
		}
		entries[alias+".*"] = dbsp.NewLiteralExpr(&dbsp.GetOp{}, table+".*")
	}
	root := dbsp.NewOpExpr(&dbsp.DictOp{}, dbsp.DictArgs{Entries: entries})
	return dbsp.NewExpression(root), nil
}

func wrapProjectionWithSQLBool(expr expression.Expression) expression.Expression {
	return expression.Func(func(doc datamodel.Document) (any, error) {
		val, err := expr.Evaluate(doc)
		if err != nil {
			return nil, err
		}
		document, ok := val.(datamodel.Document)
		if !ok {
			return val, nil
		}
		mapped, ok := document.(*exprutil.MapDocument)
		if !ok {
			return document, nil
		}
		out := exprutil.NewMapDocument(nil)
		if err := applySQLBoolToMap(mapped, out); err != nil {
			return nil, err
		}
		return out, nil
	})
}

func applySQLBoolToMap(in *exprutil.MapDocument, out *exprutil.MapDocument) error {
	for _, key := range in.Fields() {
		val, err := in.GetField(key)
		if err != nil {
			return err
		}
		if val == nil {
			out.SetField(key, false)
			continue
		}
		out.SetField(key, val)
	}
	return nil
}

var _ compiler.Compiler = (*Compiler)(nil)
