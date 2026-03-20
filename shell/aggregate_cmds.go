package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	compileragg "github.com/l7mp/dbsp/engine/compiler/aggregation"
)

func aggregateRootCommand(state *appState) *cobra.Command {
	root := &cobra.Command{Use: "aggregate", Short: "Aggregate statement management"}

	createCmd := &cobra.Command{
		Use:   "create <name> <pipeline-json>",
		Short: "Create a named aggregate statement",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			src := strings.Join(args[1:], " ")
			input, err := cmd.Flags().GetString("input")
			if err != nil {
				return err
			}
			output, err := cmd.Flags().GetString("output")
			if err != nil {
				return err
			}
			if input == "" {
				input = "input"
			}
			if output == "" {
				output = name + "-output"
			}
			sources := parseAggregateSources(input)
			c := compileragg.New(sources, []string{"output"})
			if _, err := c.ParseString(src); err != nil {
				return fmt.Errorf("parse: %w", err)
			}
			state.aggregate[name] = aggregateSpec{Source: src, Input: input, Output: output}
			fmt.Fprintf(os.Stdout, "Aggregate statement %s created.\n", name)
			return nil
		},
	}
	createCmd.Flags().String("input", "", "Producer topic for compiled circuit input (default: input)")
	createCmd.Flags().String("output", "", "Consumer topic for compiled circuit output (default: <name>-output)")

	compileCmd := &cobra.Command{
		Use:   "compile <aggregate-name> <circuit-name>",
		Short: "Compile a named aggregate statement into a circuit",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			aggName, circuitName := args[0], args[1]
			spec, ok := state.aggregate[aggName]
			if !ok {
				return fmt.Errorf("aggregate statement %s not found", aggName)
			}
			if _, exists := state.circuits[circuitName]; exists {
				return fmt.Errorf("circuit %s already exists", circuitName)
			}
			sources := parseAggregateSources(spec.Input)
			c := compileragg.New(sources, []string{"output"})
			q, err := c.CompileString(spec.Source)
			if err != nil {
				return fmt.Errorf("compile: %w", err)
			}
			if nodeID, ok := q.OutputMap["output"]; ok {
				q.OutputMap = map[string]string{spec.Output: nodeID}
			}
			state.circuits[circuitName] = q.Circuit
			state.queries[circuitName] = q
			state.logger.V(1).Info("aggregate compile defaults", "statement", aggName, "default_output", spec.Output)
			fmt.Fprintf(os.Stdout, "Circuit %s compiled from aggregate %s.\n", circuitName, aggName)
			return nil
		},
	}

	getCmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Print a named aggregate statement",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			s, ok := state.aggregate[args[0]]
			if !ok {
				return fmt.Errorf("aggregate statement %s not found", args[0])
			}
			fmt.Fprintln(os.Stdout, s.Source)
			return nil
		},
	}

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List named aggregate statements",
		Run: func(cmd *cobra.Command, args []string) {
			if len(state.aggregate) == 0 {
				fmt.Fprintln(os.Stdout, "(no aggregate statements)")
				return
			}
			names := make([]string, 0, len(state.aggregate))
			for n := range state.aggregate {
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
		Short: "Delete a named aggregate statement",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if _, ok := state.aggregate[args[0]]; !ok {
				return fmt.Errorf("aggregate statement %s not found", args[0])
			}
			delete(state.aggregate, args[0])
			return nil
		},
	}

	root.AddCommand(createCmd, compileCmd, getCmd, listCmd, deleteCmd)
	return root
}

func parseAggregateSources(input string) []string {
	parts := strings.Split(input, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return []string{"input"}
	}
	return out
}
