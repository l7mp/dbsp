package aggregation

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/l7mp/dbsp/engine/expression"
	dbspexpr "github.com/l7mp/dbsp/engine/expression/dbsp"
	"github.com/l7mp/dbsp/engine/internal/utils"
	"github.com/l7mp/dbsp/engine/operator"

	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/simple"
	"gonum.org/v1/gonum/graph/topo"
)

type program struct {
	Branches []branchSpec
	Order    []int
}

func (p *program) IRKind() string { return "aggregation.program" }

type branchSpec struct {
	Index  int
	Inputs []string
	Output string
	Stages []stageSpec
}

type stageSpec struct {
	Index      int
	Op         string
	RawArgs    json.RawMessage
	Predicate  expression.Expression
	JoinInputs []string // explicit @join participants list (hard ∪ soft); nil means "default to all of branch's @inputs"
	SoftInputs []string
	Projection expression.Expression
	UnwindPath string
	GroupBy    operator.Operator
	Distinct   bool
}

func parseProgram(source []byte, sources, outputs []string) (*program, error) {
	var top []json.RawMessage
	if err := json.Unmarshal(source, &top); err != nil {
		var single Pipeline
		if err2 := json.Unmarshal(source, &single); err2 != nil {
			return nil, fmt.Errorf("parse pipeline: %w", err)
		}
		b, err := parseBranch(0, single, sources, outputs, false)
		if err != nil {
			return nil, err
		}
		branches := []branchSpec{b}
		if err := validateProgramGraph(branches, sources, outputs); err != nil {
			return nil, err
		}
		order, err := buildTopoOrder(branches, sources)
		if err != nil {
			return nil, err
		}
		return &program{Branches: branches, Order: order}, nil
	}

	if len(top) == 0 {
		b := branchSpec{Index: 0, Inputs: append([]string(nil), sources...), Output: firstOutput(outputs), Stages: nil}
		branches := []branchSpec{b}
		if err := validateProgramGraph(branches, sources, outputs); err != nil {
			return nil, err
		}
		order, err := buildTopoOrder(branches, sources)
		if err != nil {
			return nil, err
		}
		return &program{Branches: branches, Order: order}, nil
	}

	var asPipeline Pipeline
	if err := json.Unmarshal(source, &asPipeline); err == nil && isPipelineShape(asPipeline) {
		b, err := parseBranch(0, asPipeline, sources, outputs, false)
		if err != nil {
			return nil, err
		}
		branches := []branchSpec{b}
		if err := validateProgramGraph(branches, sources, outputs); err != nil {
			return nil, err
		}
		order, err := buildTopoOrder(branches, sources)
		if err != nil {
			return nil, err
		}
		return &program{Branches: branches, Order: order}, nil
	}

	branches := make([]branchSpec, 0, len(top))
	for i, raw := range top {
		var p Pipeline
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, fmt.Errorf("parse branch[%d]: expected pipeline array/object: %w", i, err)
		}
		b, err := parseBranch(i, p, sources, outputs, true)
		if err != nil {
			return nil, err
		}
		branches = append(branches, b)
	}

	if err := validateProgramGraph(branches, sources, outputs); err != nil {
		return nil, err
	}

	order, err := buildTopoOrder(branches, sources)
	if err != nil {
		return nil, err
	}

	return &program{Branches: branches, Order: order}, nil
}

func parseBranch(index int, pipeline Pipeline, sources, outputs []string, explicit bool) (branchSpec, error) {
	b := branchSpec{Index: index}
	seenInputs := false
	seenOutput := false

	for si, stage := range pipeline {
		switch stage.Op {
		case "@inputs":
			if seenInputs {
				return b, wrapStageErr(si, stage.Op, "directive", stage.Args, fmt.Errorf("@inputs declared more than once in branch[%d]", index))
			}
			seenInputs = true
			if err := json.Unmarshal(stage.Args, &b.Inputs); err != nil {
				return b, wrapStageErr(si, stage.Op, "directive", stage.Args, fmt.Errorf("@inputs expects list of strings"))
			}
		case "@output":
			if seenOutput {
				return b, wrapStageErr(si, stage.Op, "directive", stage.Args, fmt.Errorf("@output declared more than once in branch[%d]", index))
			}
			seenOutput = true
			if err := json.Unmarshal(stage.Args, &b.Output); err != nil {
				return b, wrapStageErr(si, stage.Op, "directive", stage.Args, fmt.Errorf("@output expects a string name"))
			}
		default:
			parsed, err := parseStage(si, stage)
			if err != nil {
				return b, err
			}
			b.Stages = append(b.Stages, parsed)
		}
	}

	if !seenInputs {
		if explicit {
			return b, fmt.Errorf("branch[%d]: missing @inputs directive", index)
		}
		b.Inputs = append([]string(nil), sources...)
	}
	if !seenOutput {
		if explicit {
			return b, fmt.Errorf("branch[%d]: missing @output directive", index)
		}
		b.Output = firstOutput(outputs)
	}

	if b.Output == "" {
		return b, fmt.Errorf("branch[%d]: empty @output name", index)
	}
	if len(b.Inputs) == 0 {
		return b, fmt.Errorf("branch[%d]: @inputs cannot be empty", index)
	}

	if len(b.Stages) > 0 && b.Stages[0].Op == "@join" {
		participants := b.Stages[0].JoinInputs // explicit participants (hard ∪ soft); nil means "all of @inputs"
		soft := b.Stages[0].SoftInputs

		inputSet := make(map[string]bool, len(b.Inputs))
		for _, in := range b.Inputs {
			inputSet[in] = true
		}

		participantSet := make(map[string]bool, len(b.Inputs))
		if participants != nil {
			seenP := make(map[string]bool, len(participants))
			for _, in := range participants {
				if seenP[in] {
					return b, fmt.Errorf("branch[%d]: @join input %q declared more than once", index, in)
				}
				seenP[in] = true
				if !inputSet[in] {
					return b, fmt.Errorf("branch[%d]: @join input %q is not listed in @inputs", index, in)
				}
				participantSet[in] = true
			}
		} else {
			for _, in := range b.Inputs {
				participantSet[in] = true
			}
		}

		seenSoft := make(map[string]bool, len(soft))
		for _, in := range soft {
			if seenSoft[in] {
				return b, fmt.Errorf("branch[%d]: @join soft input %q declared more than once", index, in)
			}
			seenSoft[in] = true
			if !participantSet[in] {
				if participants != nil && inputSet[in] {
					return b, fmt.Errorf("branch[%d]: @join soft input %q is not listed in @join inputs", index, in)
				}
				return b, fmt.Errorf("branch[%d]: @join soft input %q is not listed in @inputs", index, in)
			}
		}

		// Strict subset: at least one participant must remain hard.
		if len(participantSet)-len(seenSoft) < 1 {
			return b, fmt.Errorf("branch[%d]: @join must include at least one hard input", index)
		}
	}

	return b, nil
}

func parseStage(i int, stage PipelineOp) (stageSpec, error) {
	s := stageSpec{Index: i, Op: stage.Op, RawArgs: stage.Args}
	switch stage.Op {
	case "@join":
		pred, hard, soft, err := parseJoinArgs(stage.Args)
		if err != nil {
			return s, wrapStageErr(i, stage.Op, "predicate", stage.Args, err)
		}
		s.Predicate = pred
		s.JoinInputs = hard
		s.SoftInputs = soft
	case "@select":
		expr, err := dbspexpr.NewParser().Parse(stage.Args)
		if err != nil {
			return s, wrapStageErr(i, stage.Op, "predicate", stage.Args, err)
		}
		s.Predicate = expr
	case "@project":
		proj, err := compileProjectExpression(stage.Args, i, stage.Op)
		if err != nil {
			return s, err
		}
		s.Projection = proj
	case "@unwind":
		var path string
		if err := json.Unmarshal(stage.Args, &path); err != nil {
			return s, wrapStageErr(i, stage.Op, "path", stage.Args,
				fmt.Errorf("argument must be a string: %w", err))
		}
		if !strings.HasPrefix(path, "$.") {
			return s, wrapStageErr(i, stage.Op, "path", stage.Args,
				fmt.Errorf("argument must start with '$.': %q", path))
		}
		s.UnwindPath = path
	case "@groupBy":
		op, err := compileGroupByOp(stage.Args, i, stage.Op)
		if err != nil {
			return s, err
		}
		s.GroupBy = op
	case "@distinct":
		if err := utils.ValidateNullaryArgs(stage.Args, stage.Op); err != nil {
			return s, wrapStageErr(i, stage.Op, "arguments", stage.Args, err)
		}
		s.Distinct = true
	case "@aggregate", "@gather", "@mux":
		return s, wrapStageErr(i, stage.Op, "stage", stage.Args,
			fmt.Errorf("%s is not supported; use @groupBy and @project", stage.Op))
	default:
		return s, wrapStageErr(i, stage.Op, "stage", stage.Args,
			fmt.Errorf("unsupported pipeline operation: %s", stage.Op))
	}
	return s, nil
}

// parseJoinArgs returns the predicate, the explicit @join participants list
// (the array form's "inputs:" key — hard ∪ soft; nil if the user did not
// specify one, in which case the caller defaults to "all of branch's
// @inputs"), and the soft-input list (a strict subset of the participants
// list when both are given).
func parseJoinArgs(args json.RawMessage) (expression.Expression, []string, []string, error) {
	var list []json.RawMessage
	if err := json.Unmarshal(args, &list); err == nil {
		if len(list) != 2 {
			return nil, nil, nil, fmt.Errorf("@join array form expects [predicate, options]")
		}
		pred, err := dbspexpr.NewParser().Parse(list[0])
		if err != nil {
			return nil, nil, nil, err
		}
		opts := struct {
			Inputs *[]string `json:"inputs"`
			Soft   []string  `json:"soft"`
		}{}
		if err := json.Unmarshal(list[1], &opts); err != nil {
			return nil, nil, nil, fmt.Errorf("@join options must be an object")
		}
		var hard []string
		if opts.Inputs != nil {
			hard = append([]string(nil), (*opts.Inputs)...)
		}
		return pred, hard, append([]string(nil), opts.Soft...), nil
	}

	pred, err := dbspexpr.NewParser().Parse(args)
	if err != nil {
		return nil, nil, nil, err
	}
	return pred, nil, nil, nil
}

func firstOutput(outputs []string) string {
	if len(outputs) == 0 {
		return "output"
	}
	return outputs[0]
}

func isPipelineShape(p Pipeline) bool {
	for _, st := range p {
		if st.Op == "" {
			return false
		}
	}
	return true
}

func validateProgramGraph(branches []branchSpec, sources, outputs []string) error {
	producer := map[string]int{}
	sourceSet := map[string]bool{}
	for _, s := range sources {
		sourceSet[sourceStreamKey(s)] = true
	}

	for _, b := range branches {
		outKey := outputStreamKey(b.Output)
		if prev, ok := producer[outKey]; ok {
			return fmt.Errorf("duplicate producer for %q: branch[%d] and branch[%d]", b.Output, prev, b.Index)
		}
		producer[outKey] = b.Index
	}

	for _, b := range branches {
		for _, in := range b.Inputs {
			_, internal := producer[outputStreamKey(in)]
			external := sourceSet[sourceStreamKey(in)]
			if !internal && !external {
				return fmt.Errorf("branch[%d]: input %q is unbound", b.Index, in)
			}
		}
	}

	if len(outputs) > 0 {
		for _, out := range outputs {
			if sourceSet[sourceStreamKey(out)] {
				continue
			}
			if _, ok := producer[outputStreamKey(out)]; !ok {
				return fmt.Errorf("configured output %q is unbound", out)
			}
		}
	}

	g := simple.NewDirectedGraph()
	nodes := map[int]graph.Node{}
	for _, b := range branches {
		n := g.NewNode()
		g.AddNode(n)
		nodes[b.Index] = n
	}
	for _, b := range branches {
		for _, in := range b.Inputs {
			if sourceSet[sourceStreamKey(in)] {
				continue
			}
			if p, ok := producer[outputStreamKey(in)]; ok {
				if p == b.Index {
					return fmt.Errorf("graph cycle: branch[%d] feeds itself via %q", b.Index, in)
				}
				g.SetEdge(g.NewEdge(nodes[p], nodes[b.Index]))
			}
		}
	}
	if _, err := topo.Sort(g); err != nil {
		return fmt.Errorf("branch dependency graph must be a DAG: %w", err)
	}

	return nil
}

func buildTopoOrder(branches []branchSpec, sources []string) ([]int, error) {
	producer := map[string]int{}
	sourceSet := map[string]bool{}
	for _, s := range sources {
		sourceSet[sourceStreamKey(s)] = true
	}
	for _, b := range branches {
		producer[outputStreamKey(b.Output)] = b.Index
	}

	g := simple.NewDirectedGraph()
	nodes := map[int]graph.Node{}
	for _, b := range branches {
		n := g.NewNode()
		g.AddNode(n)
		nodes[b.Index] = n
	}
	for _, b := range branches {
		for _, in := range b.Inputs {
			if sourceSet[sourceStreamKey(in)] {
				continue
			}
			if p, ok := producer[outputStreamKey(in)]; ok {
				if p == b.Index {
					return nil, fmt.Errorf("graph cycle: branch[%d] feeds itself via %q", b.Index, in)
				}
				g.SetEdge(g.NewEdge(nodes[p], nodes[b.Index]))
			}
		}
	}
	sorted, err := topo.Sort(g)
	if err != nil {
		return nil, fmt.Errorf("branch dependency graph must be a DAG: %w", err)
	}
	idToBranch := map[int64]int{}
	for bi, n := range nodes {
		idToBranch[n.ID()] = bi
	}
	order := make([]int, 0, len(sorted))
	for _, n := range sorted {
		if bi, ok := idToBranch[n.ID()]; ok {
			order = append(order, bi)
		}
	}
	return order, nil
}
