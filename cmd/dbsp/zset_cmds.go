package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/reeflective/console"
	"github.com/spf13/cobra"

	"github.com/l7mp/dbsp/datamodel"
	"github.com/l7mp/dbsp/dbsp/zset"
)

// boundZSet couples a Z-set to an optional table name for future schema
// binding. An empty tableName means the Z-set holds Unstructured documents.
type boundZSet struct {
	tableName string
	data      zset.ZSet
}

func zsetRootCommand(app *console.Console, state *appState) *cobra.Command {
	root := &cobra.Command{
		Use:   "zset",
		Short: "Z-set management",
	}
	for _, cmd := range zsetMenuCommands(app, state) {
		root.AddCommand(cmd)
	}
	return root
}

func setupZSetMenu(app *console.Console, state *appState) {
	menu := app.NewMenu("zset")
	setupPrompt(menu, state)
	menu.AddInterrupt(io.EOF, func(c *console.Console) {
		switchToParentMenu(app, state)
	})

	menu.SetCommands(func() *cobra.Command {
		root := &cobra.Command{}
		for _, cmd := range zsetMenuCommands(app, state) {
			root.AddCommand(cmd)
		}
		root.AddCommand(newExitCommand(app, state))
		return root
	})
}

func zsetMenuCommands(app *console.Console, state *appState) []*cobra.Command {
	// --- Lifecycle commands ---

	createZSet := &cobra.Command{
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
			enterZSetContext(app, state, name)
			return nil
		},
	}
	createZSet.Flags().String("table", "", "Bind Z-set to a table (not yet implemented)")
	if app == nil {
		createZSet.RunE = func(cmd *cobra.Command, args []string) error {
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
			state.currentZSet = name
			return nil
		}
	}

	updateZSet := &cobra.Command{
		Use:   "update <name>",
		Short: "Enter Z-set edit mode",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if _, exists := state.zsets[name]; !exists {
				return fmt.Errorf("zset %s not found", name)
			}
			enterZSetContext(app, state, name)
			return nil
		},
	}
	if app == nil {
		updateZSet.Short = "Set the current Z-set"
		updateZSet.RunE = func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if _, exists := state.zsets[name]; !exists {
				return fmt.Errorf("zset %s not found", name)
			}
			state.currentZSet = name
			return nil
		}
	}

	getZSet := &cobra.Command{
		Use:   "get [<name>]",
		Short: "Print Z-set entries",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var bz *boundZSet
			var err error
			if len(args) == 1 {
				bz, err = requireZSet(state, args[0])
			} else {
				bz, err = requireCurrentZSet(state)
			}
			if err != nil {
				return err
			}
			return printZSetEntries(bz.data)
		},
	}

	deleteZSet := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a Z-set",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if _, exists := state.zsets[name]; !exists {
				return fmt.Errorf("zset %s not found", name)
			}
			delete(state.zsets, name)
			if state.currentZSet == name {
				state.currentZSet = ""
			}
			return nil
		},
	}

	listZSets := &cobra.Command{
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

	// --- Content commands (require current Z-set context) ---

	insertCmd := &cobra.Command{
		Use:   "insert <json>",
		Short: "Insert a JSON document into the current Z-set",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			bz, err := requireCurrentZSet(state)
			if err != nil {
				return err
			}
			w, err := cmd.Flags().GetInt64("weight")
			if err != nil {
				return err
			}
			// Strip surrounding shell quotes if present (common in interactive use).
			raw := strings.Trim(args[0], "'\"")
			doc, err := datamodel.ParseUnstructured([]byte(raw))
			if err != nil {
				return err
			}
			bz.data.Insert(doc, zset.Weight(w))
			return nil
		},
	}
	insertCmd.Flags().Int64("weight", 1, "Element weight (default 1)")

	weightCmd := &cobra.Command{
		Use:   "weight <idx> <w>",
		Short: "Change the weight of an element by 1-based index",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			bz, err := requireCurrentZSet(state)
			if err != nil {
				return err
			}
			idx, err := strconv.Atoi(args[0])
			if err != nil || idx < 1 {
				return fmt.Errorf("invalid index %q: must be a positive integer", args[0])
			}
			newW, err := strconv.ParseInt(args[1], 10, 64)
			if err != nil {
				return fmt.Errorf("invalid weight %q: must be an integer", args[1])
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

	negateCmd := &cobra.Command{
		Use:   "negate",
		Short: "Negate all weights in the current Z-set",
		RunE: func(cmd *cobra.Command, args []string) error {
			bz, err := requireCurrentZSet(state)
			if err != nil {
				return err
			}
			bz.data = bz.data.Negate()
			return nil
		},
	}

	clearCmd := &cobra.Command{
		Use:   "clear",
		Short: "Remove all entries from the current Z-set",
		RunE: func(cmd *cobra.Command, args []string) error {
			bz, err := requireCurrentZSet(state)
			if err != nil {
				return err
			}
			bz.data = zset.New()
			return nil
		},
	}

	return []*cobra.Command{
		createZSet,
		updateZSet,
		getZSet,
		deleteZSet,
		listZSets,
		insertCmd,
		weightCmd,
		negateCmd,
		clearCmd,
	}
}

func enterZSetContext(app *console.Console, state *appState, name string) {
	if app == nil {
		state.currentZSet = name
		return
	}
	if app.ActiveMenu().Name() != "zset" {
		state.parentMenu = app.ActiveMenu().Name()
		app.SwitchMenu("zset")
	}
	state.currentZSet = name
}

func requireZSet(state *appState, name string) (*boundZSet, error) {
	bz, exists := state.zsets[name]
	if !exists {
		return nil, fmt.Errorf("zset %s not found", name)
	}
	return bz, nil
}

func requireCurrentZSet(state *appState) (*boundZSet, error) {
	if state.currentZSet == "" {
		return nil, fmt.Errorf("no z-set selected (use 'zset update <name>')")
	}
	return requireZSet(state, state.currentZSet)
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
