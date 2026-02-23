package main

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/reeflective/console"
	"github.com/spf13/cobra"

	"github.com/l7mp/dbsp/dbsp/executor"
	"github.com/l7mp/dbsp/dbsp/transform"
	"github.com/l7mp/dbsp/dbsp/zset"
)

// boundExecutor couples an executor to the name of the circuit it was built
// from, so the incrementalize command can locate the circuit in appState.
type boundExecutor struct {
	circuitName string
	exec        *executor.Executor
}

func executorRootCommand(app *console.Console, state *appState) *cobra.Command {
	root := &cobra.Command{
		Use:   "executor",
		Short: "Executor management",
	}
	for _, cmd := range executorMenuCommands(app, state) {
		root.AddCommand(cmd)
	}
	return root
}

func setupExecutorMenu(app *console.Console, state *appState) {
	menu := app.NewMenu("executor")
	setupPrompt(menu, state)
	menu.AddInterrupt(io.EOF, func(c *console.Console) {
		switchToParentMenu(app, state)
	})

	menu.SetCommands(func() *cobra.Command {
		root := &cobra.Command{}
		for _, cmd := range executorMenuCommands(app, state) {
			root.AddCommand(cmd)
		}
		root.AddCommand(newExitCommand(app, state))
		return root
	})
}

func executorMenuCommands(app *console.Console, state *appState) []*cobra.Command {
	// --- Lifecycle commands ---

	createExecutor := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new executor for a circuit",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			circuitName, err := cmd.Flags().GetString("circuit")
			if err != nil {
				return err
			}
			if circuitName == "" {
				return fmt.Errorf("circuit name required (use --circuit)")
			}
			c, err := requireCircuit(state, circuitName)
			if err != nil {
				return err
			}
			name := args[0]
			if _, exists := state.executors[name]; exists {
				return fmt.Errorf("executor %s already exists", name)
			}
			exec, err := executor.New(c, state.logger)
			if err != nil {
				return err
			}
			state.executors[name] = &boundExecutor{circuitName: circuitName, exec: exec}
			enterExecutorContext(app, state, name)
			return nil
		},
	}
	createExecutor.Flags().String("circuit", "", "Circuit to build the executor for")
	if app == nil {
		createExecutor.RunE = func(cmd *cobra.Command, args []string) error {
			circuitName, err := cmd.Flags().GetString("circuit")
			if err != nil {
				return err
			}
			if circuitName == "" {
				return fmt.Errorf("circuit name required (use --circuit)")
			}
			c, err := requireCircuit(state, circuitName)
			if err != nil {
				return err
			}
			name := args[0]
			if _, exists := state.executors[name]; exists {
				return fmt.Errorf("executor %s already exists", name)
			}
			exec, err := executor.New(c, state.logger)
			if err != nil {
				return err
			}
			state.executors[name] = &boundExecutor{circuitName: circuitName, exec: exec}
			state.currentExecutor = name
			return nil
		}
	}

	updateExecutor := &cobra.Command{
		Use:   "update <name>",
		Short: "Enter executor edit mode",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if _, exists := state.executors[name]; !exists {
				return fmt.Errorf("executor %s not found", name)
			}
			enterExecutorContext(app, state, name)
			return nil
		},
	}
	if app == nil {
		updateExecutor.Short = "Set the current executor"
		updateExecutor.RunE = func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if _, exists := state.executors[name]; !exists {
				return fmt.Errorf("executor %s not found", name)
			}
			state.currentExecutor = name
			return nil
		}
	}

	getExecutor := &cobra.Command{
		Use:   "get [<name>]",
		Short: "Print executor info",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			var be *boundExecutor
			var err error
			if len(args) == 1 {
				be, err = requireExecutor(state, args[0])
			} else {
				be, err = requireCurrentExecutor(state)
			}
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stdout, "circuit: %s\n", be.circuitName)
			return nil
		},
	}

	deleteExecutor := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete an executor",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if _, exists := state.executors[name]; !exists {
				return fmt.Errorf("executor %s not found", name)
			}
			delete(state.executors, name)
			if state.currentExecutor == name {
				state.currentExecutor = ""
			}
			return nil
		},
	}

	listExecutors := &cobra.Command{
		Use:   "list",
		Short: "List executors",
		Run: func(cmd *cobra.Command, args []string) {
			if len(state.executors) == 0 {
				fmt.Fprintln(os.Stdout, "(no executors)")
				return
			}
			names := make([]string, 0, len(state.executors))
			for name := range state.executors {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				fmt.Fprintln(os.Stdout, name)
			}
		},
	}

	// --- Content commands (require current executor context) ---

	executeCmd := &cobra.Command{
		Use:   "execute [--out <prefix>] [<node>=<zset>...]",
		Short: "Run one circuit step with the given inputs",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			be, err := requireCurrentExecutor(state)
			if err != nil {
				return err
			}
			prefix, err := cmd.Flags().GetString("out")
			if err != nil {
				return err
			}
			if prefix == "" {
				prefix = state.currentExecutor
			}
			// Parse node=zset pairs.
			inputs := make(map[string]zset.ZSet)
			for _, arg := range args {
				parts := strings.SplitN(arg, "=", 2)
				if len(parts) != 2 {
					return fmt.Errorf("invalid input %q: expected node=zset", arg)
				}
				nodeName, zsetName := parts[0], parts[1]
				bz, err := requireZSet(state, zsetName)
				if err != nil {
					return err
				}
				inputs[nodeName] = bz.data
			}
			outputs, err := be.exec.Execute(inputs)
			if err != nil {
				return err
			}
			// Store and report each output Z-set.
			outNames := make([]string, 0, len(outputs))
			for node := range outputs {
				outNames = append(outNames, node)
			}
			sort.Strings(outNames)
			for _, node := range outNames {
				z := outputs[node]
				name := prefix + "-" + node
				state.zsets[name] = &boundZSet{data: z}
				fmt.Fprintf(os.Stdout, "Output: %s  (+%d docs)\n", name, z.Size())
			}
			return nil
		},
	}
	executeCmd.Flags().String("out", "", "Output Z-set name prefix (default: executor name)")

	incrementalizeCmd := &cobra.Command{
		Use:   "incrementalize <new-name>",
		Short: "Create an incremental twin circuit and executor",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			be, err := requireCurrentExecutor(state)
			if err != nil {
				return err
			}
			newName := args[0]
			// Resolve the source circuit.
			c, err := requireCircuit(state, be.circuitName)
			if err != nil {
				return err
			}
			incCircuitName := be.circuitName + "-inc"
			if _, exists := state.circuits[incCircuitName]; exists {
				return fmt.Errorf("circuit %s already exists", incCircuitName)
			}
			incCircuit, err := transform.Incrementalize(c)
			if err != nil {
				return err
			}
			state.circuits[incCircuitName] = incCircuit
			// Create the incremental executor.
			if _, exists := state.executors[newName]; exists {
				return fmt.Errorf("executor %s already exists", newName)
			}
			incExec, err := executor.New(incCircuit, state.logger)
			if err != nil {
				return err
			}
			state.executors[newName] = &boundExecutor{circuitName: incCircuitName, exec: incExec}
			fmt.Fprintf(os.Stdout, "Created circuit %s and executor %s\n", incCircuitName, newName)
			return nil
		},
	}

	resetCmd := &cobra.Command{
		Use:   "reset",
		Short: "Reset executor state",
		RunE: func(cmd *cobra.Command, args []string) error {
			be, err := requireCurrentExecutor(state)
			if err != nil {
				return err
			}
			be.exec.Reset()
			fmt.Fprintln(os.Stdout, "Executor state reset.")
			return nil
		},
	}

	return []*cobra.Command{
		createExecutor,
		updateExecutor,
		getExecutor,
		deleteExecutor,
		listExecutors,
		executeCmd,
		incrementalizeCmd,
		resetCmd,
	}
}

func enterExecutorContext(app *console.Console, state *appState, name string) {
	if app == nil {
		state.currentExecutor = name
		return
	}
	if app.ActiveMenu().Name() != "executor" {
		state.parentMenu = app.ActiveMenu().Name()
		app.SwitchMenu("executor")
	}
	state.currentExecutor = name
}

func requireExecutor(state *appState, name string) (*boundExecutor, error) {
	be, exists := state.executors[name]
	if !exists {
		return nil, fmt.Errorf("executor %s not found", name)
	}
	return be, nil
}

func requireCurrentExecutor(state *appState) (*boundExecutor, error) {
	if state.currentExecutor == "" {
		return nil, fmt.Errorf("no executor selected (use 'executor update <name>')")
	}
	return requireExecutor(state, state.currentExecutor)
}
