package sql

import (
	"fmt"
	"strings"

	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"

	"github.com/l7mp/dbsp/engine/datamodel/relation"
)

// NormalizedQuery holds a normalized AST and bind vars.
type NormalizedQuery struct {
	Stmt     *sqlparser.Select
	BindVars map[string]*querypb.BindVariable
}

func (n *NormalizedQuery) IRKind() string { return "sql.normalized" }

// Normalize parses and normalizes SQL into a canonical subset.
// It rewrites aliases, expands stars, and rejects unsupported syntax.
func Normalize(source string, db *relation.Database) (*NormalizedQuery, error) {
	stmt, err := sqlparser.Parse(source)
	if err != nil {
		return nil, err
	}
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return nil, fmt.Errorf("unsupported statement: %T", stmt)
	}

	bindVars := make(map[string]*querypb.BindVariable)
	sqlparser.Normalize(selectStmt, bindVars, "v")

	if err := validateSelect(selectStmt); err != nil {
		return nil, err
	}
	aliasMap, err := buildAliasMap(selectStmt, db)
	if err != nil {
		return nil, err
	}
	if err := rewriteColumnRefs(selectStmt, aliasMap); err != nil {
		return nil, err
	}
	if err := expandStarTargets(selectStmt, aliasMap, db); err != nil {
		return nil, err
	}

	return &NormalizedQuery{Stmt: selectStmt, BindVars: bindVars}, nil
}

func validateSelect(sel *sqlparser.Select) error {
	if sel.Distinct != "" {
		return UnimplementedError{Feature: "distinct"}
	}
	if sel.GroupBy != nil {
		return UnimplementedError{Feature: "group by"}
	}
	if sel.Having != nil {
		return UnimplementedError{Feature: "having"}
	}
	if sel.OrderBy != nil {
		return UnimplementedError{Feature: "order by"}
	}
	if sel.Limit != nil {
		return UnimplementedError{Feature: "limit"}
	}
	if sel.Lock != "" {
		return UnimplementedError{Feature: "locking"}
	}
	if sel.From == nil || len(sel.From) != 1 {
		return UnimplementedError{Feature: "multiple FROM items"}
	}
	if err := validateSelectExprs(sel.SelectExprs); err != nil {
		return err
	}
	return nil
}

func validateSelectExprs(exprs sqlparser.SelectExprs) error {
	for _, expr := range exprs {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			continue
		case *sqlparser.AliasedExpr:
			switch e.Expr.(type) {
			case *sqlparser.ColName, *sqlparser.SQLVal, *sqlparser.BinaryExpr, *sqlparser.UnaryExpr, *sqlparser.ComparisonExpr:
				continue
			default:
				return UnimplementedError{Feature: fmt.Sprintf("select expr %T", e.Expr)}
			}
		default:
			return UnimplementedError{Feature: fmt.Sprintf("select expr %T", expr)}
		}
	}
	return nil
}

func buildAliasMap(sel *sqlparser.Select, db *relation.Database) (map[string]string, error) {
	aliases := make(map[string]string)
	var walkFrom func(expr sqlparser.TableExpr) error
	walkFrom = func(expr sqlparser.TableExpr) error {
		switch t := expr.(type) {
		case *sqlparser.AliasedTableExpr:
			nameExpr, ok := t.Expr.(sqlparser.TableName)
			if !ok {
				return UnimplementedError{Feature: fmt.Sprintf("from expr %T", t.Expr)}
			}
			base := nameExpr.Name.String()
			if base == "" {
				return fmt.Errorf("empty table name")
			}
			if db != nil {
				if _, err := db.GetTable(base); err != nil {
					return err
				}
			}
			logical := base
			if !t.As.IsEmpty() {
				logical = t.As.String()
			}
			aliases[strings.ToLower(logical)] = base
		case *sqlparser.JoinTableExpr:
			if err := walkFrom(t.LeftExpr); err != nil {
				return err
			}
			if err := walkFrom(t.RightExpr); err != nil {
				return err
			}
			if t.Condition.On == nil {
				return UnimplementedError{Feature: "join without ON"}
			}
			if t.Join != sqlparser.JoinStr && t.Join != sqlparser.StraightJoinStr {
				return UnimplementedError{Feature: fmt.Sprintf("join %q", t.Join)}
			}
		default:
			return UnimplementedError{Feature: fmt.Sprintf("from %T", expr)}
		}
		return nil
	}
	if err := walkFrom(sel.From[0]); err != nil {
		return nil, err
	}
	return aliases, nil
}

func rewriteColumnRefs(node sqlparser.SQLNode, aliases map[string]string) error {
	switch n := node.(type) {
	case *sqlparser.ColName:
		qual := n.Qualifier.Name.String()
		if qual == "" {
			return nil
		}
		if actual, ok := aliases[strings.ToLower(qual)]; ok {
			n.Qualifier.Name = sqlparser.NewTableIdent(actual)
		}
	case *sqlparser.Select:
		if n.Where != nil {
			if err := rewriteColumnRefs(n.Where.Expr, aliases); err != nil {
				return err
			}
		}
		for _, expr := range n.SelectExprs {
			switch e := expr.(type) {
			case *sqlparser.AliasedExpr:
				if err := rewriteColumnRefs(e.Expr, aliases); err != nil {
					return err
				}
			case *sqlparser.StarExpr:
				if !e.TableName.IsEmpty() {
					if actual, ok := aliases[strings.ToLower(e.TableName.Name.String())]; ok {
						e.TableName.Name = sqlparser.NewTableIdent(actual)
					}
				}
			default:
				return UnimplementedError{Feature: fmt.Sprintf("select expr %T", expr)}
			}
		}
		for _, from := range n.From {
			if err := rewriteColumnRefs(from, aliases); err != nil {
				return err
			}
		}
	case *sqlparser.AndExpr:
		if err := rewriteColumnRefs(n.Left, aliases); err != nil {
			return err
		}
		return rewriteColumnRefs(n.Right, aliases)
	case *sqlparser.OrExpr:
		if err := rewriteColumnRefs(n.Left, aliases); err != nil {
			return err
		}
		return rewriteColumnRefs(n.Right, aliases)
	case *sqlparser.NotExpr:
		return rewriteColumnRefs(n.Expr, aliases)
	case *sqlparser.ComparisonExpr:
		if err := rewriteColumnRefs(n.Left, aliases); err != nil {
			return err
		}
		return rewriteColumnRefs(n.Right, aliases)
	case *sqlparser.BinaryExpr:
		if err := rewriteColumnRefs(n.Left, aliases); err != nil {
			return err
		}
		return rewriteColumnRefs(n.Right, aliases)
	case *sqlparser.UnaryExpr:
		return rewriteColumnRefs(n.Expr, aliases)
	case *sqlparser.IsExpr:
		return rewriteColumnRefs(n.Expr, aliases)
	case *sqlparser.ParenExpr:
		return rewriteColumnRefs(n.Expr, aliases)
	case *sqlparser.JoinTableExpr:
		if err := rewriteColumnRefs(n.LeftExpr, aliases); err != nil {
			return err
		}
		if err := rewriteColumnRefs(n.RightExpr, aliases); err != nil {
			return err
		}
		if n.Condition.On != nil {
			return rewriteColumnRefs(n.Condition.On, aliases)
		}
	case *sqlparser.AliasedTableExpr:
		return nil
	}
	return nil
}

func expandStarTargets(sel *sqlparser.Select, aliases map[string]string, db *relation.Database) error {
	var expanded sqlparser.SelectExprs
	for _, expr := range sel.SelectExprs {
		switch e := expr.(type) {
		case *sqlparser.StarExpr:
			cols, err := resolveStarColumns(e.TableName, aliases, db)
			if err != nil {
				return err
			}
			for _, col := range cols {
				expanded = append(expanded, &sqlparser.AliasedExpr{Expr: col})
			}
		case *sqlparser.AliasedExpr:
			expanded = append(expanded, e)
		default:
			return UnimplementedError{Feature: fmt.Sprintf("select expr %T", expr)}
		}
	}
	sel.SelectExprs = expanded
	return nil
}

func resolveStarColumns(table sqlparser.TableName, aliases map[string]string, db *relation.Database) ([]*sqlparser.ColName, error) {
	if db == nil {
		return nil, fmt.Errorf("star expansion requires schema")
	}
	qual := strings.ToLower(table.Name.String())
	if qual != "" {
		if actual, ok := aliases[qual]; ok {
			qual = actual
		}
		tbl, err := db.GetTable(qual)
		if err != nil {
			return nil, err
		}
		return tableColumns(qual, tbl), nil
	}
	var cols []*sqlparser.ColName
	for _, base := range aliases {
		tbl, err := db.GetTable(base)
		if err != nil {
			return nil, err
		}
		cols = append(cols, tableColumns(base, tbl)...)
	}
	return cols, nil
}

func tableColumns(table string, tbl *relation.Table) []*sqlparser.ColName {
	cols := make([]*sqlparser.ColName, 0, len(tbl.Schema.Columns))
	for _, col := range tbl.Schema.Columns {
		name := col.Name
		if col.QualifiedName != "" {
			name = col.QualifiedName
		}
		parts := strings.SplitN(name, ".", 2)
		colName := name
		qual := table
		if len(parts) == 2 {
			qual = parts[0]
			colName = parts[1]
		}
		cols = append(cols, &sqlparser.ColName{
			Name:      sqlparser.NewColIdent(colName),
			Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent(qual)},
		})
	}
	return cols
}

//nolint:unused
func qualifyUnqualifiedColumns(sel *sqlparser.Select, aliases map[string]string, db *relation.Database) error {
	if db == nil {
		return nil
	}
	if len(aliases) <= 1 {
		return nil
	}
	columnOwners := map[string]string{}
	for _, base := range aliases {
		tbl, err := db.GetTable(base)
		if err != nil {
			return err
		}
		for _, col := range tbl.Schema.Columns {
			name := strings.ToLower(col.Name)
			if _, exists := columnOwners[name]; !exists {
				columnOwners[name] = base
			} else {
				columnOwners[name] = ""
			}
		}
	}

	var walkExpr func(expr sqlparser.Expr) error
	walkExpr = func(expr sqlparser.Expr) error {
		switch e := expr.(type) {
		case *sqlparser.ColName:
			if e.Qualifier.Name.String() != "" {
				return nil
			}
			owner, ok := columnOwners[strings.ToLower(e.Name.String())]
			if !ok || owner == "" {
				return nil
			}
			e.Qualifier.Name = sqlparser.NewTableIdent(owner)
		case *sqlparser.AndExpr:
			if err := walkExpr(e.Left); err != nil {
				return err
			}
			return walkExpr(e.Right)
		case *sqlparser.OrExpr:
			if err := walkExpr(e.Left); err != nil {
				return err
			}
			return walkExpr(e.Right)
		case *sqlparser.NotExpr:
			return walkExpr(e.Expr)
		case *sqlparser.ComparisonExpr:
			if err := walkExpr(e.Left); err != nil {
				return err
			}
			return walkExpr(e.Right)
		case *sqlparser.BinaryExpr:
			if err := walkExpr(e.Left); err != nil {
				return err
			}
			return walkExpr(e.Right)
		case *sqlparser.UnaryExpr:
			return walkExpr(e.Expr)
		case *sqlparser.IsExpr:
			return walkExpr(e.Expr)
		case *sqlparser.ParenExpr:
			return walkExpr(e.Expr)
		}
		return nil
	}

	if sel.Where != nil {
		if err := walkExpr(sel.Where.Expr); err != nil {
			return err
		}
	}
	for _, se := range sel.SelectExprs {
		if ae, ok := se.(*sqlparser.AliasedExpr); ok {
			if err := walkExpr(ae.Expr); err != nil {
				return err
			}
		}
	}
	for _, from := range sel.From {
		if jt, ok := from.(*sqlparser.JoinTableExpr); ok && jt.Condition.On != nil {
			if err := walkExpr(jt.Condition.On); err != nil {
				return err
			}
		}
	}
	return nil
}
