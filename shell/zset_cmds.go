package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/cobra"

	"encoding/json"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/expression"
	exprdbsp "github.com/l7mp/dbsp/engine/expression/dbsp"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// boundZSet couples a Z-set to an optional table name for future schema
// binding. An empty tableName means the Z-set holds Unstructured documents.
type boundZSet struct {
	tableName string
	data      zset.ZSet
	pkFunc    func(datamodel.Document) (string, error)

	mu         sync.Mutex
	buffer     []dbspruntime.Event
	publisher  dbspruntime.Publisher
	producing  bool
	consuming  bool
	subscriber dbspruntime.Subscriber
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
		produceZSetCmd(state),
		consumeZSetCmd(state),
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

type zsetProducer struct{}

func (p *zsetProducer) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func produceZSetCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "produce <zset-name> <input-name>",
		Short: "Start producing changes of a Z-set",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := state.runtimeFailure(); err != nil {
				return fmt.Errorf("runtime failure: %w", err)
			}
			name := args[0]
			topic := args[1]
			bz, err := requireZSet(state, name)
			if err != nil {
				return err
			}

			bz.mu.Lock()
			if bz.producing {
				bz.mu.Unlock()
				return nil
			}
			bz.publisher = state.runtime.NewPublisher()
			bz.producing = true
			buffer := append([]dbspruntime.Event(nil), bz.buffer...)
			bz.buffer = nil
			pub := bz.publisher
			bz.mu.Unlock()

			state.runtime.Add(&zsetProducer{})
			state.logger.V(1).Info("zset producer started", "name", name, "topic", topic, "flush_events", len(buffer))
			for _, event := range buffer {
				event.Name = topic
				if err := pub.Publish(event); err != nil {
					return err
				}
			}
			bz.publisher = dbspruntime.PublishFunc(func(event dbspruntime.Event) error {
				event.Name = topic
				return pub.Publish(event)
			})
			return nil
		},
	}
}

func consumeZSetCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "consume <zset-name> <output-name>",
		Short: "Start consuming events into a Z-set buffer",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := state.runtimeFailure(); err != nil {
				return fmt.Errorf("runtime failure: %w", err)
			}
			name := args[0]
			topic := args[1]
			bz, err := requireZSet(state, name)
			if err != nil {
				return err
			}

			bz.mu.Lock()
			if bz.consuming {
				bz.mu.Unlock()
				return nil
			}
			sub := state.runtime.NewSubscriber()
			sub.Subscribe(topic)
			bz.subscriber = sub
			bz.consuming = true
			bz.mu.Unlock()
			state.logger.V(1).Info("zset consumer started", "name", name, "topic", topic)
			return nil
		},
	}
}

func printZSetCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "print <name>",
		Short: "Print and remove first buffered event",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bz, err := requireZSet(state, args[0])
			if err != nil {
				return err
			}

			bz.mu.Lock()
			consuming := bz.consuming && bz.subscriber != nil
			sub := bz.subscriber
			bz.mu.Unlock()
			if consuming {
				event, ok := <-sub.GetChannel()
				if !ok {
					return fmt.Errorf("zset %s consumer channel closed", args[0])
				}
				return printZSetEntries(args[0], event.Data)
			}

			event, ok := popBufferedEvent(bz)
			if !ok {
				return fmt.Errorf("zset %s buffer is empty: no events received yet; did you run zset consume %s and zset produce on relevant input zsets?", args[0], args[0])
			}
			return printZSetEntries(args[0], event.Data)
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
			if state.zsets[name].subscriber != nil {
				state.zsets[name].subscriber.Unsubscribe(name)
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
			bz.mu.Lock()
			for _, e := range entries {
				bz.data.Insert(bz.newDocument(e.fields), zset.Weight(e.weight))
			}
			bz.mu.Unlock()
			return publishCurrentSnapshot(args[0], bz)
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
			bz.mu.Lock()
			doc := bz.newDocument(entry.fields)
			delta := zset.Weight(entry.weight) - bz.data.Lookup(doc.Hash())
			if delta != 0 {
				bz.data.Insert(doc, delta)
			}
			bz.mu.Unlock()
			return publishCurrentSnapshot(args[0], bz)
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
			bz.mu.Lock()
			bz.data = bz.data.Negate()
			bz.mu.Unlock()
			return publishCurrentSnapshot(args[0], bz)
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
			bz.mu.Lock()
			bz.data = zset.New()
			bz.mu.Unlock()
			return publishCurrentSnapshot(args[0], bz)
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
			bz.mu.Lock()
			bz.data = zset.New()
			for _, e := range entries {
				bz.data.Insert(bz.newDocument(e.fields), zset.Weight(e.weight))
			}
			bz.mu.Unlock()
			return publishCurrentSnapshot(args[0], bz)
		},
	}
}

func publishCurrentSnapshot(name string, bz *boundZSet) error {
	bz.mu.Lock()
	event := dbspruntime.Event{Name: name, Data: bz.data.Clone()}
	if !bz.producing || bz.publisher == nil {
		bz.buffer = append(bz.buffer, event)
		bz.mu.Unlock()
		return nil
	}
	pub := bz.publisher
	bz.mu.Unlock()

	return pub.Publish(event)
}

func popBufferedEvent(bz *boundZSet) (dbspruntime.Event, bool) {
	bz.mu.Lock()
	defer bz.mu.Unlock()
	if len(bz.buffer) == 0 {
		return dbspruntime.Event{}, false
	}
	event := bz.buffer[0]
	bz.buffer = bz.buffer[1:]
	return event, true
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

// printZSetEntries prints the named Z-set and its entries.
func printZSetEntries(name string, z zset.ZSet) error {
	entries := sortedEntries(z)
	label := "entries"
	if len(entries) == 1 {
		label = "entry"
	}
	fmt.Fprintf(os.Stdout, "zset %s (%d %s):\n", name, len(entries), label)
	if len(entries) == 0 {
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
