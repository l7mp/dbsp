package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/l7mp/dbsp/dbsp/circuit"
	"github.com/l7mp/dbsp/dbsp/operator"
	"github.com/l7mp/dbsp/dbsp/transform"
	exprdbsp "github.com/l7mp/dbsp/expression/dbsp"
)

func circuitRootCommand(state *appState) *cobra.Command {
	root := &cobra.Command{
		Use:   "circuit",
		Short: "Circuit management",
	}
	root.AddCommand(
		createCircuitCmd(state),
		getCircuitCmd(state),
		deleteCircuitCmd(state),
		listCircuitsCmd(state),
		nodeCommand(state),
		edgeCommand(state),
		printCommand(state),
		validateCommand(state),
		incrementalizeCommand(state),
	)
	return root
}

func createCircuitCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new empty circuit",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if _, exists := state.circuits[name]; exists {
				return fmt.Errorf("circuit %s already exists", name)
			}
			state.circuits[name] = circuit.New(name)
			return nil
		},
	}
}

func getCircuitCmd(state *appState) *cobra.Command {
	return &cobra.Command{
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
}

func deleteCircuitCmd(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a circuit",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if _, exists := state.circuits[name]; !exists {
				return fmt.Errorf("circuit %s not found", name)
			}
			delete(state.circuits, name)
			return nil
		},
	}
}

func listCircuitsCmd(state *appState) *cobra.Command {
	return &cobra.Command{
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
}

func nodeCommand(state *appState) *cobra.Command {
	root := &cobra.Command{
		Use:   "node",
		Short: "Manage circuit nodes",
	}

	addCmd := &cobra.Command{
		Use:   "add <circuit> <node> <operator-kind> [args...]",
		Short: "Add a node to a circuit",
		Args:  cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCircuit(state, args[0])
			if err != nil {
				return err
			}
			node, err := buildNode(args[1], strings.ToLower(args[2]), args[3:])
			if err != nil {
				return err
			}
			return c.AddNode(node)
		},
	}

	getCmd := &cobra.Command{
		Use:   "get <circuit> <node>",
		Short: "Get a node from a circuit",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCircuit(state, args[0])
			if err != nil {
				return err
			}
			node := c.Node(args[1])
			if node == nil {
				return fmt.Errorf("node %s not found", args[1])
			}
			return printNode(node)
		},
	}

	deleteCmd := &cobra.Command{
		Use:   "delete <circuit> <node>",
		Short: "Delete a node from a circuit",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCircuit(state, args[0])
			if err != nil {
				return err
			}
			return c.RemoveNode(args[1])
		},
	}

	setCmd := &cobra.Command{
		Use:   "set <circuit> <node> <zset>",
		Short: "Set a node's operator state from a Z-set",
		Args:  cobra.ExactArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCircuit(state, args[0])
			if err != nil {
				return err
			}
			node := c.Node(args[1])
			if node == nil {
				return fmt.Errorf("node %s not found", args[1])
			}
			bz, err := requireZSet(state, args[2])
			if err != nil {
				return err
			}
			node.Set(bz.data)
			return nil
		},
	}

	updateCmd := &cobra.Command{
		Use:   "update <circuit> <node> <operator-kind> [args...]",
		Short: "Update a node in a circuit",
		Args:  cobra.MinimumNArgs(3),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCircuit(state, args[0])
			if err != nil {
				return err
			}
			if err := c.RemoveNode(args[1]); err != nil {
				return err
			}
			node, err := buildNode(args[1], strings.ToLower(args[2]), args[3:])
			if err != nil {
				return err
			}
			return c.AddNode(node)
		},
	}

	root.AddCommand(addCmd, getCmd, setCmd, deleteCmd, updateCmd)
	return root
}

func edgeCommand(state *appState) *cobra.Command {
	root := &cobra.Command{
		Use:   "edge",
		Short: "Manage circuit edges",
	}

	addCmd := &cobra.Command{
		Use:   "add <circuit> <from> <to> <port>",
		Short: "Add an edge to a circuit",
		Args:  cobra.ExactArgs(4),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCircuit(state, args[0])
			if err != nil {
				return err
			}
			port, err := parsePort(args[3])
			if err != nil {
				return err
			}
			return c.AddEdge(circuit.NewEdge(args[1], args[2], port))
		},
	}

	getCmd := &cobra.Command{
		Use:   "get <circuit> <from> <to> <port>",
		Short: "Get an edge from a circuit",
		Args:  cobra.ExactArgs(4),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCircuit(state, args[0])
			if err != nil {
				return err
			}
			port, err := parsePort(args[3])
			if err != nil {
				return err
			}
			for _, e := range c.Edges() {
				if e.From == args[1] && e.To == args[2] && e.Port == port {
					return printEdge(e)
				}
			}
			return fmt.Errorf("edge %s -> %s (port %d) not found", args[1], args[2], port)
		},
	}

	deleteCmd := &cobra.Command{
		Use:   "delete <circuit> <from> <to> <port>",
		Short: "Delete an edge from a circuit",
		Args:  cobra.ExactArgs(4),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCircuit(state, args[0])
			if err != nil {
				return err
			}
			port, err := parsePort(args[3])
			if err != nil {
				return err
			}
			return c.RemoveEdge(args[1], args[2], port)
		},
	}

	updateCmd := &cobra.Command{
		Use:   "update <circuit> <from> <to> <port>",
		Short: "Update an edge in a circuit",
		Args:  cobra.ExactArgs(4),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCircuit(state, args[0])
			if err != nil {
				return err
			}
			port, err := parsePort(args[3])
			if err != nil {
				return err
			}
			if err := c.RemoveEdge(args[1], args[2], port); err != nil {
				return err
			}
			return c.AddEdge(circuit.NewEdge(args[1], args[2], port))
		},
	}

	root.AddCommand(addCmd, getCmd, deleteCmd, updateCmd)
	return root
}

func printCommand(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "print <circuit> [nodes|edges|all]",
		Short: "Print circuit components",
		Args:  cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCircuit(state, args[0])
			if err != nil {
				return err
			}
			target := "all"
			if len(args) == 2 {
				target = strings.ToLower(args[1])
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
}

func validateCommand(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "validate <circuit>",
		Short: "Validate a circuit",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCircuit(state, args[0])
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
}

func incrementalizeCommand(state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "incrementalize <circuit> <new-name>",
		Short: "Create an incrementalized circuit",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := requireCircuit(state, args[0])
			if err != nil {
				return err
			}
			name := args[1]
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
}

func requireCircuit(state *appState, name string) (*circuit.Circuit, error) {
	c, exists := state.circuits[name]
	if !exists {
		return nil, fmt.Errorf("circuit %s not found", name)
	}
	return c, nil
}

func buildNode(name, kind string, args []string) (*circuit.Node, error) {
	op, err := buildOperator(kind, args)
	if err != nil {
		return nil, err
	}
	return circuit.Op(name, op), nil
}

func buildOperator(opType string, args []string) (operator.Operator, error) {
	switch strings.ToLower(opType) {
	case "input":
		return operator.NewInput(), nil
	case "output":
		return operator.NewOutput(), nil
	case "delay":
		emit, _ := operator.NewDelay()
		return emit, nil
	case "integrate":
		return operator.NewIntegrate(), nil
	case "differentiate":
		return operator.NewDifferentiate(), nil
	case "delta0":
		return operator.NewDelta0(), nil
	case "plus":
		return operator.NewPlus(), nil
	case "minus":
		return operator.NewMinus(), nil
	case "negate":
		return operator.NewNegate(), nil
	case "lc":
		if len(args) == 0 {
			return nil, fmt.Errorf("lc requires at least one coefficient (e.g. +1 -1)")
		}
		coeffs := make([]int, len(args))
		for i, a := range args {
			c, err := strconv.Atoi(a)
			if err != nil {
				return nil, fmt.Errorf("lc: invalid coefficient %q: must be an integer", a)
			}
			coeffs[i] = c
		}
		return operator.NewLinearCombination(coeffs), nil
	case "distinct":
		return operator.NewDistinct(), nil
	case "distinct_pi", "distinct-pi":
		return operator.NewDistinctPi(), nil
	case "hkeyed":
		return operator.NewDistinctPi(), nil
	case "aggregate", "groupby":
		if len(args) < 3 {
			return nil, fmt.Errorf("aggregate_keyed requires at least 3 args: <key-expr> <value-expr> <reduce-expr> [out-field|set-expr]")
		}
		keyExpr, err := exprdbsp.CompileString(args[0])
		if err != nil {
			return nil, fmt.Errorf("aggregate_keyed: key expression: %w", err)
		}
		valueExpr, err := exprdbsp.CompileString(args[1])
		if err != nil {
			return nil, fmt.Errorf("aggregate_keyed: value expression: %w", err)
		}
		reduceExpr, err := exprdbsp.CompileString(normalizeAggregateReduceArg(args[2]))
		if err != nil {
			return nil, fmt.Errorf("aggregate_keyed: reduce expression: %w", err)
		}
		if len(args) > 3 {
			if len(args[3]) > 0 && args[3][0] == '{' {
				setExpr, err := exprdbsp.CompileString(args[3])
				if err != nil {
					return nil, fmt.Errorf("aggregate_keyed: set expression: %w", err)
				}
				return operator.NewAggregateWithSet(keyExpr, valueExpr, reduceExpr, setExpr), nil
			}
			return operator.NewAggregate(keyExpr, valueExpr, reduceExpr, args[3]), nil
		}
		return operator.NewAggregate(keyExpr, valueExpr, reduceExpr, "value"), nil
	case "cartesian":
		return operator.NewCartesianProduct(), nil
	case "@filter", "filter", "select":
		if len(args) == 0 {
			return nil, fmt.Errorf("filter expression required")
		}
		expr, err := exprdbsp.CompileString(strings.Join(args, " "))
		if err != nil {
			return nil, err
		}
		return operator.NewSelect(expr), nil
	case "@project", "project":
		if len(args) == 0 {
			return nil, fmt.Errorf("project expression required")
		}
		expr, err := exprdbsp.CompileString(strings.Join(args, " "))
		if err != nil {
			return nil, err
		}
		return operator.NewProject(expr), nil
	case "unwind":
		if len(args) == 0 {
			return nil, fmt.Errorf("unwind field required")
		}
		op := operator.NewUnwind(args[0])
		if len(args) > 1 {
			op = op.WithIndexField(args[1])
		}
		return op, nil
	default:
		return nil, fmt.Errorf("unknown operator %q", opType)
	}
}

func normalizeAggregateReduceArg(arg string) string {
	switch arg {
	case "@sum", "sum":
		return `{"@sum":["$."]}`
	case "@len", "len":
		return `{"@len":"$."}`
	case "@lexmin", "lexmin":
		return `{"@lexmin":["$."]}`
	case "@lexmax", "lexmax":
		return `{"@lexmax":["$."]}`
	default:
		return arg
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
