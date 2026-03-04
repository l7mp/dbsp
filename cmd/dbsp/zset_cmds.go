package main

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"encoding/json"

	"github.com/l7mp/dbsp/datamodel/unstructured"
	"github.com/l7mp/dbsp/dbsp/zset"
)

// boundZSet couples a Z-set to an optional table name for future schema
// binding. An empty tableName means the Z-set holds Unstructured documents.
type boundZSet struct {
	tableName string
	data      zset.ZSet
}

func zsetRootCommand(state *appState) *cobra.Command {
	root := &cobra.Command{
		Use:   "zset",
		Short: "Z-set management",
	}
	root.AddCommand(
		createZSetCmd(state),
		getZSetCmd(state),
		deleteZSetCmd(state),
		listZSetsCmd(state),
		insertCmd(state),
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
			if tableName != "" {
				return fmt.Errorf("table-bound zsets not yet implemented")
			}
			if _, exists := state.zsets[name]; exists {
				return fmt.Errorf("zset %s already exists", name)
			}
			state.zsets[name] = &boundZSet{data: zset.New()}
			return nil
		},
	}
	cmd.Flags().String("table", "", "Bind Z-set to a table (not yet implemented)")
	return cmd
}

func getZSetCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "get <name>",
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
	cmd := &cobra.Command{
		Use:   "insert <name> <json>",
		Short: "Insert a JSON document into a Z-set",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			bz, err := requireZSet(state, args[0])
			if err != nil {
				return err
			}
			w, err := cmd.Flags().GetInt64("weight")
			if err != nil {
				return err
			}
			// Strip surrounding shell quotes if present (common in interactive use).
			raw := strings.Trim(args[1], "'\"")
			var fields map[string]any
			if err := json.Unmarshal([]byte(raw), &fields); err != nil {
				return fmt.Errorf("invalid JSON: %w", err)
			}
			doc := unstructured.New(fields, nil)
			bz.data.Insert(doc, zset.Weight(w))
			return nil
		},
	}
	cmd.Flags().Int64("weight", 1, "Element weight (default 1)")
	return cmd
}

func weightCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "weight <name> <idx> <w>",
		Short: "Change the weight of an element by 1-based index",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			bz, err := requireZSet(state, args[0])
			if err != nil {
				return err
			}
			idx, err := strconv.Atoi(args[1])
			if err != nil || idx < 1 {
				return fmt.Errorf("invalid index %q: must be a positive integer", args[1])
			}
			newW, err := strconv.ParseInt(args[2], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid weight %q: must be an integer", args[2])
			}
			entries := sortedEntries(bz.data)
			if idx > len(entries) {
				return fmt.Errorf("index %d out of range (z-set has %d elements)", idx, len(entries))
			}
			entry := entries[idx-1]
			delta := zset.Weight(newW) - entry.Weight
			if delta != 0 {
				bz.data.Insert(entry.Document, delta)
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
