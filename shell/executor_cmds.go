package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/l7mp/dbsp/engine/executor"
	"github.com/l7mp/dbsp/engine/transform"
	"github.com/l7mp/dbsp/engine/zset"
)

// boundExecutor couples an executor to the name of the circuit it was built
// from, so the incrementalize command can locate the circuit in appState.
type boundExecutor struct {
	circuitName string
	exec        *executor.Executor
}

func executorRootCommand(state *appState) *cobra.Command {
	root := &cobra.Command{
		Use:   "executor",
		Short: "Executor management",
	}
	root.AddCommand(
		createExecutorCmd(state),
		getExecutorCmd(state),
		deleteExecutorCmd(state),
		listExecutorsCmd(state),
		executeCmd(state),
		incrementalizeExecutorCmd(state),
		resetCmd(state),
	)
	return root
}

func createExecutorCmd(state *appState) *cobra.Command {
	cmd := &cobra.Command{
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
			return nil
		},
	}
	cmd.Flags().String("circuit", "", "Circuit to build the executor for")
	return cmd
}

func getExecutorCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "get <name>",
		Short: "Print executor info",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			be, err := requireExecutor(state, args[0])
			if err != nil {
				return err
			}
			fmt.Fprintf(os.Stdout, "circuit: %s\n", be.circuitName)
			return nil
		},
	}
}

func deleteExecutorCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete an executor",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if _, exists := state.executors[name]; !exists {
				return fmt.Errorf("executor %s not found", name)
			}
			delete(state.executors, name)
			return nil
		},
	}
}

func listExecutorsCmd(state *appState) *cobra.Command {
	return &cobra.Command{
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
}

func executeCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "execute <name> [<node>=<zset>...]",
		Short: "Run one circuit step with the given inputs and outputs",
		Long: `Run one step of the named executor's circuit.

Each additional argument is a node=zset pair. Whether it is treated as an
input or an output is determined by the circuit topology:
  - input node:  the named Z-set is fed into the circuit
  - output node: the circuit result is stored in the named Z-set,
                 overwriting any existing Z-set with that name

Output assignments are optional when the circuit has exactly one output node;
in that case the result is stored under <executor>-<node> by default. If there
are multiple output nodes and not all of them are assigned, the command errors.`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			execName := args[0]
			be, err := requireExecutor(state, execName)
			if err != nil {
				return err
			}
			c, err := requireCircuit(state, be.circuitName)
			if err != nil {
				return err
			}

			// Build sets of input and output node names from the circuit.
			inputNodes := make(map[string]bool)
			for _, n := range c.Inputs() {
				inputNodes[n.ID] = true
			}
			outputNodes := make(map[string]bool)
			for _, n := range c.Outputs() {
				outputNodes[n.ID] = true
			}

			// Parse node=zset pairs, routing each to inputs or outputs.
			inputs := make(map[string]zset.ZSet)
			outAssign := make(map[string]string) // circuit node → zset name
			for _, arg := range args[1:] {
				parts := strings.SplitN(arg, "=", 2)
				if len(parts) != 2 {
					return fmt.Errorf("invalid argument %q: expected node=zset", arg)
				}
				node, zsetName := parts[0], parts[1]
				switch {
				case inputNodes[node]:
					bz, err := requireZSet(state, zsetName)
					if err != nil {
						return err
					}
					inputs[node] = bz.data
				case outputNodes[node]:
					outAssign[node] = zsetName
				default:
					return fmt.Errorf("unknown node %q", node)
				}
			}

			// Validate output assignments: all outputs must be assigned
			// unless there is exactly one output (default naming applies).
			for _, n := range c.Outputs() {
				if _, ok := outAssign[n.ID]; !ok {
					if len(c.Outputs()) > 1 {
						return fmt.Errorf("output node %q has no zset assignment; "+
							"use %s=<zset> to assign it", n.ID, n.ID)
					}
					// Single unassigned output: use default name.
					outAssign[n.ID] = execName + "-" + n.ID
				}
			}

			outputs, err := be.exec.Execute(inputs)
			if err != nil {
				return err
			}

			// Store results, overwriting any existing zset with the same name.
			outNames := make([]string, 0, len(outputs))
			for node := range outputs {
				outNames = append(outNames, node)
			}
			sort.Strings(outNames)
			for _, node := range outNames {
				name := outAssign[node]
				state.zsets[name] = &boundZSet{data: outputs[node]}
				fmt.Fprintf(os.Stdout, "Output: %s  (+%d docs)\n", name, outputs[node].Size())
			}
			return nil
		},
	}
}

func incrementalizeExecutorCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "incrementalize <name> <new-name>",
		Short: "Create an incremental twin circuit and executor",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			be, err := requireExecutor(state, args[0])
			if err != nil {
				return err
			}
			newName := args[1]
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
}

func resetCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "reset <name>",
		Short: "Reset executor state",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			be, err := requireExecutor(state, args[0])
			if err != nil {
				return err
			}
			be.exec.Reset()
			fmt.Fprintln(os.Stdout, "Executor state reset.")
			return nil
		},
	}
}

func requireExecutor(state *appState, name string) (*boundExecutor, error) {
	be, exists := state.executors[name]
	if !exists {
		return nil, fmt.Errorf("executor %s not found", name)
	}
	return be, nil
}
