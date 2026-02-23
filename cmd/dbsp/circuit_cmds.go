package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/reeflective/console"
	"github.com/spf13/cobra"

	"github.com/l7mp/dbsp/dbsp/circuit"
	"github.com/l7mp/dbsp/dbsp/operator"
	"github.com/l7mp/dbsp/dbsp/transform"
	exprdbsp "github.com/l7mp/dbsp/expression/dbsp"
)

func circuitRootCommand(app *console.Console, state *appState) *cobra.Command {
	root := &cobra.Command{
		Use:   "circuit",
		Short: "Circuit management",
	}
	for _, cmd := range circuitMenuCommands(app, state) {
		root.AddCommand(cmd)
	}
	return root
}

func circuitMenuCommands(app *console.Console, state *appState) []*cobra.Command {
	createCircuit := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new empty circuit",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if _, exists := state.circuits[name]; exists {
				return fmt.Errorf("circuit %s already exists", name)
			}
			state.circuits[name] = circuit.New(name)
			enterCircuitContext(app, state, name)
			return nil
		},
	}
	if app == nil {
		createCircuit.Short = "Create a new empty circuit and select it"
		createCircuit.RunE = func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if _, exists := state.circuits[name]; exists {
				return fmt.Errorf("circuit %s already exists", name)
			}
			state.circuits[name] = circuit.New(name)
			state.currentCircuit = name
			return nil
		}
	}

	updateCircuit := &cobra.Command{
		Use:   "update <name>",
		Short: "Enter circuit edit mode",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if _, exists := state.circuits[name]; !exists {
				return fmt.Errorf("circuit %s not found", name)
			}
			enterCircuitContext(app, state, name)
			return nil
		},
	}
	if app == nil {
		updateCircuit.Short = "Set the current circuit"
		updateCircuit.RunE = func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if _, exists := state.circuits[name]; !exists {
				return fmt.Errorf("circuit %s not found", name)
			}
			state.currentCircuit = name
			return nil
		}
	}

	getCircuit := &cobra.Command{
		Use:   "get <name>",
		Short: "Print a circuit",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCircuit(state, args[0])
			if err != nil {
				return err
			}
			return printCircuit(c)
		},
	}

	deleteCircuit := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a circuit",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if _, exists := state.circuits[name]; !exists {
				return fmt.Errorf("circuit %s not found", name)
			}
			delete(state.circuits, name)
			if state.currentCircuit == name {
				state.currentCircuit = ""
			}
			return nil
		},
	}

	listCircuits := &cobra.Command{
		Use:   "list",
		Short: "List circuits",
		Run: func(cmd *cobra.Command, args []string) {
			if len(state.circuits) == 0 {
				fmt.Fprintln(os.Stdout, "(no circuits)")
				return
			}
			names := make([]string, 0, len(state.circuits))
			for name := range state.circuits {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				fmt.Fprintln(os.Stdout, name)
			}
		},
	}

	return []*cobra.Command{
		createCircuit,
		updateCircuit,
		getCircuit,
		deleteCircuit,
		listCircuits,
		nodeCommand(state),
		edgeCommand(state),
		printCommand(state),
		validateCommand(state),
		incrementalizeCommand(state),
	}
}

func nodeCommand(state *appState) *cobra.Command {
	root := &cobra.Command{
		Use:   "node",
		Short: "Manage circuit nodes",
	}
	root.PersistentFlags().String("circuit", "", "Circuit name")

	addCmd := &cobra.Command{
		Use:   "add <name> <kind> [args...]",
		Short: "Add a node to the circuit",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCurrentCircuit(cmd, state)
			if err != nil {
				return err
			}
			node, err := buildNode(args[0], strings.ToLower(args[1]), args[2:])
			if err != nil {
				return err
			}
			return c.AddNode(node)
		},
	}

	getCmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Get a node from the circuit",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCurrentCircuit(cmd, state)
			if err != nil {
				return err
			}
			node := c.Node(args[0])
			if node == nil {
				return fmt.Errorf("node %s not found", args[0])
			}
			return printNode(node)
		},
	}

	deleteCmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a node from the circuit",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCurrentCircuit(cmd, state)
			if err != nil {
				return err
			}
			return c.RemoveNode(args[0])
		},
	}

	updateCmd := &cobra.Command{
		Use:   "update <name> <kind> [args...]",
		Short: "Update a node in the circuit",
		Args:  cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCurrentCircuit(cmd, state)
			if err != nil {
				return err
			}
			if err := c.RemoveNode(args[0]); err != nil {
				return err
			}
			node, err := buildNode(args[0], strings.ToLower(args[1]), args[2:])
			if err != nil {
				return err
			}
			return c.AddNode(node)
		},
	}

	root.AddCommand(addCmd, getCmd, deleteCmd, updateCmd)
	return root
}

func edgeCommand(state *appState) *cobra.Command {
	root := &cobra.Command{
		Use:   "edge",
		Short: "Manage circuit edges",
	}
	root.PersistentFlags().String("circuit", "", "Circuit name")

	addCmd := &cobra.Command{
		Use:   "add <from> <to> <port>",
		Short: "Add an edge to the circuit",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCurrentCircuit(cmd, state)
			if err != nil {
				return err
			}
			port, err := parsePort(args[2])
			if err != nil {
				return err
			}
			return c.AddEdge(circuit.NewEdge(args[0], args[1], port))
		},
	}

	getCmd := &cobra.Command{
		Use:   "get <from> <to> <port>",
		Short: "Get an edge from the circuit",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCurrentCircuit(cmd, state)
			if err != nil {
				return err
			}
			port, err := parsePort(args[2])
			if err != nil {
				return err
			}
			for _, e := range c.Edges() {
				if e.From == args[0] && e.To == args[1] && e.Port == port {
					return printEdge(e)
				}
			}
			return fmt.Errorf("edge %s -> %s (port %d) not found", args[0], args[1], port)
		},
	}

	deleteCmd := &cobra.Command{
		Use:   "delete <from> <to> <port>",
		Short: "Delete an edge from the circuit",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCurrentCircuit(cmd, state)
			if err != nil {
				return err
			}
			port, err := parsePort(args[2])
			if err != nil {
				return err
			}
			return c.RemoveEdge(args[0], args[1], port)
		},
	}

	updateCmd := &cobra.Command{
		Use:   "update <from> <to> <port>",
		Short: "Update an edge in the circuit",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCurrentCircuit(cmd, state)
			if err != nil {
				return err
			}
			port, err := parsePort(args[2])
			if err != nil {
				return err
			}
			if err := c.RemoveEdge(args[0], args[1], port); err != nil {
				return err
			}
			return c.AddEdge(circuit.NewEdge(args[0], args[1], port))
		},
	}

	root.AddCommand(addCmd, getCmd, deleteCmd, updateCmd)
	return root
}

func printCommand(state *appState) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "print [nodes|edges|all]",
		Short: "Print circuit components",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCurrentCircuit(cmd, state)
			if err != nil {
				return err
			}
			target := "all"
			if len(args) == 1 {
				target = strings.ToLower(args[0])
			}
			switch target {
			case "nodes":
				return printNodes(c)
			case "edges":
				return printEdges(c)
			case "all":
				return printCircuit(c)
			default:
				return fmt.Errorf("unknown print target %q", target)
			}
		},
	}
	cmd.Flags().String("circuit", "", "Circuit name")
	return cmd
}

func validateCommand(state *appState) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate the current circuit",
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCurrentCircuit(cmd, state)
			if err != nil {
				return err
			}
			errs := c.Validate()
			if len(errs) == 0 {
				fmt.Fprintln(os.Stdout, "OK")
				return nil
			}
			for _, err := range errs {
				fmt.Fprintln(os.Stdout, err.Error())
			}
			return fmt.Errorf("circuit has %d validation errors", len(errs))
		},
	}
	cmd.Flags().String("circuit", "", "Circuit name")
	return cmd
}

func incrementalizeCommand(state *appState) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "incrementalize <new-name>",
		Short: "Create an incrementalized circuit",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCurrentCircuit(cmd, state)
			if err != nil {
				return err
			}
			name := args[0]
			if _, exists := state.circuits[name]; exists {
				return fmt.Errorf("circuit %s already exists", name)
			}
			inc, err := transform.Incrementalize(c)
			if err != nil {
				return err
			}
			state.circuits[name] = inc
			return nil
		},
	}
	cmd.Flags().String("circuit", "", "Circuit name")
	return cmd
}

func enterCircuitContext(app *console.Console, state *appState, name string) {
	if app == nil {
		state.currentCircuit = name
		return
	}
	if app.ActiveMenu().Name() != "circuit" {
		state.parentMenu = app.ActiveMenu().Name()
		app.SwitchMenu("circuit")
	}
	state.currentCircuit = name
}

func requireCircuit(state *appState, name string) (*circuit.Circuit, error) {
	c, exists := state.circuits[name]
	if !exists {
		return nil, fmt.Errorf("circuit %s not found", name)
	}
	return c, nil
}

func requireCurrentCircuit(cmd *cobra.Command, state *appState) (*circuit.Circuit, error) {
	if state.currentCircuit != "" {
		return requireCircuit(state, state.currentCircuit)
	}
	if cmd == nil {
		return nil, fmt.Errorf("circuit name required (use --circuit)")
	}
	name, err := cmd.Flags().GetString("circuit")
	if err != nil {
		return nil, err
	}
	if name == "" {
		return nil, fmt.Errorf("circuit name required (use --circuit)")
	}
	return requireCircuit(state, name)
}

func buildNode(name, kind string, args []string) (*circuit.Node, error) {
	switch kind {
	case "input":
		return circuit.Input(name), nil
	case "output":
		return circuit.Output(name), nil
	case "delay":
		return circuit.Delay(name), nil
	case "integrate":
		return circuit.Integrate(name), nil
	case "differentiate":
		return circuit.Differentiate(name), nil
	case "delta0":
		return circuit.Delta0(name), nil
	case "operator":
		if len(args) == 0 {
			return nil, fmt.Errorf("operator name required")
		}
		op, err := buildOperator(name, args[0], args[1:])
		if err != nil {
			return nil, err
		}
		return circuit.Op(name, op), nil
	default:
		return nil, fmt.Errorf("unknown node kind %q", kind)
	}
}

func buildOperator(nodeName, opType string, args []string) (operator.Operator, error) {
	switch strings.ToLower(opType) {
	case "plus":
		return operator.NewPlus(), nil
	case "negate":
		return operator.NewNegate(), nil
	case "distinct":
		return operator.NewDistinct(nodeName), nil
	case "cartesian":
		return operator.NewCartesianProduct(nodeName), nil
	case "@filter", "filter", "select":
		if len(args) == 0 {
			return nil, fmt.Errorf("filter expression required")
		}
		expr, err := exprdbsp.CompileString(strings.Join(args, " "))
		if err != nil {
			return nil, err
		}
		return operator.NewSelect(nodeName, expr), nil
	case "@project", "project":
		if len(args) == 0 {
			return nil, fmt.Errorf("project expression required")
		}
		expr, err := exprdbsp.CompileString(strings.Join(args, " "))
		if err != nil {
			return nil, err
		}
		return operator.NewProject(nodeName, expr), nil
	case "unwind":
		if len(args) == 0 {
			return nil, fmt.Errorf("unwind field required")
		}
		op := operator.NewUnwind(nodeName, args[0])
		if len(args) > 1 {
			op = op.WithIndexField(args[1])
		}
		return op, nil
	default:
		return nil, fmt.Errorf("unknown operator %q", opType)
	}
}

func parsePort(raw string) (int, error) {
	port, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid port %q", raw)
	}
	if port < 0 {
		return 0, fmt.Errorf("port must be >= 0")
	}
	return port, nil
}

func printCircuit(c *circuit.Circuit) error { return printJSON(c) }
func printNode(node *circuit.Node) error    { return printJSON(node) }
func printEdge(edge *circuit.Edge) error    { return printJSON(edge) }

func printNodes(c *circuit.Circuit) error {
	nodes := c.Nodes()
	byName := make(map[string]*circuit.Node, len(nodes))
	for _, n := range nodes {
		byName[n.ID] = n
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	sorted := make([]*circuit.Node, 0, len(names))
	for _, name := range names {
		sorted = append(sorted, byName[name])
	}
	return printJSON(sorted)
}

func printEdges(c *circuit.Circuit) error {
	edges := c.Edges()
	sorted := make([]*circuit.Edge, len(edges))
	copy(sorted, edges)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].From != sorted[j].From {
			return sorted[i].From < sorted[j].From
		}
		if sorted[i].To != sorted[j].To {
			return sorted[i].To < sorted[j].To
		}
		return sorted[i].Port < sorted[j].Port
	})
	return printJSON(sorted)
}

func printJSON(value any) error {
	payload, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, string(payload))
	return nil
}
