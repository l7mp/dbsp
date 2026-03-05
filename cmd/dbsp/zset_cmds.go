package main

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"encoding/json"

	"github.com/l7mp/dbsp/datamodel"
	"github.com/l7mp/dbsp/datamodel/unstructured"
	"github.com/l7mp/dbsp/dbsp/zset"
	"github.com/l7mp/dbsp/expression"
	exprdbsp "github.com/l7mp/dbsp/expression/dbsp"
)

// boundZSet couples a Z-set to an optional table name for future schema
// binding. An empty tableName means the Z-set holds Unstructured documents.
type boundZSet struct {
	tableName string
	data      zset.ZSet
	pkFunc    func(datamodel.Document) (string, error)
}

func (bz *boundZSet) newDocument(fields map[string]any) datamodel.Document {
	return unstructured.New(fields, bz.pkFunc)
}

func zsetRootCommand(state *appState) *cobra.Command {
	root := &cobra.Command{
		Use:   "zset",
		Short: "Z-set management",
	}
	root.AddCommand(
		createZSetCmd(state),
		printZSetCmd(state),
		deleteZSetCmd(state),
		listZSetsCmd(state),
		insertCmd(state),
		setCmd(state),
		weightCmd(state),
		negateCmd(state),
		clearCmd(state),
	)
	return root
}

func createZSetCmd(state *appState) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new empty Z-set",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			tableName, err := cmd.Flags().GetString("table")
			if err != nil {
				return err
			}
			pkExpr, err := cmd.Flags().GetString("pk")
			if err != nil {
				return err
			}
			if tableName != "" {
				return fmt.Errorf("table-bound zsets not yet implemented")
			}
			if _, exists := state.zsets[name]; exists {
				return fmt.Errorf("zset %s already exists", name)
			}

			var pkFunc func(datamodel.Document) (string, error)
			if strings.TrimSpace(pkExpr) != "" {
				expr, err := compilePrimaryKeyExpression(pkExpr)
				if err != nil {
					return fmt.Errorf("invalid --pk expression: %w", err)
				}
				pkFunc = primaryKeyFuncFromExpression(expr)
			}

			state.zsets[name] = &boundZSet{data: zset.New(), pkFunc: pkFunc}
			return nil
		},
	}
	cmd.Flags().String("table", "", "Bind Z-set to a table (not yet implemented)")
	cmd.Flags().String("pk", "", "Primary-key expression for unstructured documents")
	return cmd
}

func printZSetCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "print <name>",
		Short: "Print Z-set entries",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bz, err := requireZSet(state, args[0])
			if err != nil {
				return err
			}
			return printZSetEntries(bz.data)
		},
	}
}

func deleteZSetCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a Z-set",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if _, exists := state.zsets[name]; !exists {
				return fmt.Errorf("zset %s not found", name)
			}
			delete(state.zsets, name)
			return nil
		},
	}
}

func listZSetsCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List Z-sets",
		Run: func(cmd *cobra.Command, args []string) {
			if len(state.zsets) == 0 {
				fmt.Fprintln(os.Stdout, "(no z-sets)")
				return
			}
			names := make([]string, 0, len(state.zsets))
			for name := range state.zsets {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				fmt.Fprintln(os.Stdout, name)
			}
		},
	}
}

func insertCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "insert <name> (json,weight)|[(json,weight),...]",
		Short: "Insert document(s) into a Z-set",
		Long: `Insert one or more documents into a Z-set, accumulating into existing weights.

Single document:
  zset insert my-set '({"id":1},1)'

Multiple documents:
  zset insert my-set '[({"id":1},2),({"id":2},-1)]'`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			bz, err := requireZSet(state, args[0])
			if err != nil {
				return err
			}
			entries, err := parseZSetEntries(strings.Trim(args[1], "'\""))
			if err != nil {
				return err
			}
			for _, e := range entries {
				bz.data.Insert(bz.newDocument(e.fields), zset.Weight(e.weight))
			}
			return nil
		},
	}
}

func weightCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "weight <name> (json,weight)",
		Short: "Set the absolute weight of a document",
		Long: `Set a document's weight to an exact value, regardless of its current weight.

  zset weight my-set '({"id":1},5)'`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			bz, err := requireZSet(state, args[0])
			if err != nil {
				return err
			}
			entry, err := parseZSetEntry(strings.Trim(args[1], "'\""))
			if err != nil {
				return err
			}
			doc := bz.newDocument(entry.fields)
			delta := zset.Weight(entry.weight) - bz.data.Lookup(doc.Hash())
			if delta != 0 {
				bz.data.Insert(doc, delta)
			}
			return nil
		},
	}
}

func negateCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "negate <name>",
		Short: "Negate all weights in a Z-set",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bz, err := requireZSet(state, args[0])
			if err != nil {
				return err
			}
			bz.data = bz.data.Negate()
			return nil
		},
	}
}

func clearCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "clear <name>",
		Short: "Remove all entries from a Z-set",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bz, err := requireZSet(state, args[0])
			if err != nil {
				return err
			}
			bz.data = zset.New()
			return nil
		},
	}
}

func setCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "set <name> (json,weight)|[(json,weight),...]",
		Short: "Replace Z-set contents with a literal weighted multiset",
		Long: `Replace the entire Z-set with the given entries (clear then insert).

Single document:
  zset set my-set '({"key":false},1)'

Multiple documents:
  zset set my-set '[({"key":false},1),({"key":true},-1)]'`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			bz, err := requireZSet(state, args[0])
			if err != nil {
				return err
			}
			entries, err := parseZSetEntries(strings.Trim(args[1], "'\""))
			if err != nil {
				return err
			}
			bz.data = zset.New()
			for _, e := range entries {
				bz.data.Insert(bz.newDocument(e.fields), zset.Weight(e.weight))
			}
			return nil
		},
	}
}

func primaryKeyFuncFromExpression(expr expression.Expression) func(datamodel.Document) (string, error) {
	return func(doc datamodel.Document) (string, error) {
		value, err := expr.Evaluate(expression.NewContext(doc.Copy()))
		if err != nil {
			return "", fmt.Errorf("primary key expression evaluate: %w", err)
		}
		if value == nil {
			return "", fmt.Errorf("primary key expression evaluate: returned nil")
		}

		switch v := value.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		default:
			b, err := json.Marshal(v)
			if err != nil {
				return fmt.Sprintf("%v", v), nil
			}
			return string(b), nil
		}
	}
}

func compilePrimaryKeyExpression(raw string) (expression.Expression, error) {
	trimmed := strings.TrimSpace(raw)
	expr, err := exprdbsp.CompileString(trimmed)
	if err == nil {
		return expr, nil
	}

	// Convenience: allow shorthand like $.id without JSON quoting.
	quoted, marshalErr := json.Marshal(trimmed)
	if marshalErr != nil {
		return nil, err
	}
	expr2, err2 := exprdbsp.Compile(quoted)
	if err2 == nil {
		return expr2, nil
	}

	return nil, err
}

// parseZSetEntries parses either a single tuple (json,weight) or a list
// [(json,weight),...] and returns the entries.
func parseZSetEntries(s string) ([]zsetLiteralEntry, error) {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "[") {
		return parseZSetLiteral(s)
	}
	entry, err := parseZSetEntry(s)
	if err != nil {
		return nil, err
	}
	return []zsetLiteralEntry{entry}, nil
}

// parseZSetEntry parses a single (json,weight) tuple.
func parseZSetEntry(s string) (zsetLiteralEntry, error) {
	entries, err := parseZSetLiteral("[" + s + "]")
	if err != nil {
		return zsetLiteralEntry{}, err
	}
	return entries[0], nil
}

// zsetLiteralEntry holds one parsed (document, weight) pair from a Z-set literal.
type zsetLiteralEntry struct {
	fields map[string]any
	weight int64
}

// parseZSetLiteral parses a Z-set literal of the form [(json,weight),...].
// Each tuple contains a JSON object and an integer weight. The outer list may
// contain any number of tuples separated by commas.
func parseZSetLiteral(s string) ([]zsetLiteralEntry, error) {
	s = strings.TrimSpace(s)
	if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ']' {
		return nil, fmt.Errorf("expected [(json,weight),...], got %q", s)
	}
	s = s[1 : len(s)-1] // strip outer [ ]

	var entries []zsetLiteralEntry
	for {
		s = strings.TrimSpace(s)
		if len(s) == 0 {
			break
		}
		if s[0] == ',' {
			s = s[1:]
			continue
		}
		if s[0] != '(' {
			return nil, fmt.Errorf("expected '(' got %q", string(s[0]))
		}

		// Find the closing ')' for this tuple, tracking nesting depth so that
		// commas and parens inside JSON objects are ignored.
		depth, end := 0, -1
		for i, c := range s {
			switch c {
			case '(', '{', '[':
				depth++
			case ')', '}', ']':
				depth--
				if depth == 0 {
					end = i
				}
			}
			if end >= 0 {
				break
			}
		}
		if end < 0 {
			return nil, fmt.Errorf("unmatched '(' in %q", s)
		}
		tuple := s[1:end]
		s = s[end+1:]

		// The tuple is "<json_object>,<weight>". Split after the closing '}'
		// of the JSON object, which is always the last '}' since the weight
		// is a plain integer with no braces.
		lastBrace := strings.LastIndex(tuple, "}")
		if lastBrace < 0 {
			return nil, fmt.Errorf("expected JSON object in tuple %q", tuple)
		}
		rest := tuple[lastBrace+1:]
		commaIdx := strings.Index(rest, ",")
		if commaIdx < 0 {
			return nil, fmt.Errorf("expected ',<weight>' after JSON in tuple %q", tuple)
		}
		jsonPart := strings.TrimSpace(tuple[:lastBrace+1])
		weightStr := strings.TrimSpace(rest[commaIdx+1:])

		var fields map[string]any
		if err := json.Unmarshal([]byte(jsonPart), &fields); err != nil {
			return nil, fmt.Errorf("invalid JSON %q: %w", jsonPart, err)
		}
		weight, err := strconv.ParseInt(weightStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid weight %q in tuple %q", weightStr, tuple)
		}
		entries = append(entries, zsetLiteralEntry{fields: fields, weight: weight})
	}
	return entries, nil
}

func requireZSet(state *appState, name string) (*boundZSet, error) {
	bz, exists := state.zsets[name]
	if !exists {
		return nil, fmt.Errorf("zset %s not found", name)
	}
	return bz, nil
}

// sortedEntries returns ZSet entries sorted deterministically by document hash.
func sortedEntries(z zset.ZSet) []zset.Elem {
	entries := z.Entries()
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Document.Hash() < entries[j].Document.Hash()
	})
	return entries
}

// printZSetEntries prints each entry with a 1-based index, JSON, and weight.
func printZSetEntries(z zset.ZSet) error {
	entries := sortedEntries(z)
	if len(entries) == 0 {
		fmt.Fprintln(os.Stdout, "(empty)")
		return nil
	}
	for i, e := range entries {
		b, err := e.Document.MarshalJSON()
		if err != nil {
			return err
		}
		sign := "+"
		if e.Weight < 0 {
			sign = ""
		}
		fmt.Fprintf(os.Stdout, "[%d]  %s  weight=%s%d\n", i+1, string(b), sign, e.Weight)
	}
	return nil
}
