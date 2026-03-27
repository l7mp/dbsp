package aggregation

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/compiler"
	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/product"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/expression"
	dbspexpr "github.com/l7mp/dbsp/engine/expression/dbsp"
	"github.com/l7mp/dbsp/engine/operator"
)

// Compiler compiles aggregation pipelines into DBSP circuits.
type Compiler struct {
	sources []Binding
	outputs []Binding
}

// Binding maps an external stream/topic name to an internal logical name.
//
// Name is used in compiler.Query InputMap/OutputMap and therefore by runtime
// pub/sub wiring. Logical is used inside aggregation expressions and directives
// such as @inputs, @output, and join namespace field names.
type Binding struct {
	Name    string
	Logical string
}

type pipelineParseError struct {
	StageIndex int
	StageOp    string
	Argument   string
	Raw        string
	Err        error
}

func (e *pipelineParseError) Error() string {
	op := e.StageOp
	if op == "" {
		op = "<unknown>"
	}
	msg := fmt.Sprintf("pipeline parse error at stage[%d] %s, argument %s", e.StageIndex, op, e.Argument)
	if e.Raw != "" {
		msg += fmt.Sprintf(" (raw=%s)", e.Raw)
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *pipelineParseError) Unwrap() error { return e.Err }

func wrapStageErr(i int, op, arg string, raw json.RawMessage, err error) error {
	if err == nil {
		return nil
	}
	trimmed := strings.TrimSpace(string(raw))
	if len(trimmed) > 220 {
		trimmed = trimmed[:220] + "..."
	}
	return &pipelineParseError{StageIndex: i, StageOp: op, Argument: arg, Raw: trimmed, Err: err}
}

// New creates a new aggregation compiler with explicit name/logical bindings.
func New(sources, outputs []Binding) *Compiler {
	ins := normalizeBindings(sources)
	outs := normalizeBindings(outputs)
	if len(outs) == 0 {
		outs = []Binding{{Name: "output", Logical: "output"}}
	}
	return &Compiler{sources: ins, outputs: outs}
}

// Parse parses aggregation source into compiler IR.
func (c *Compiler) Parse(source []byte) (compiler.IR, error) {
	return parseProgram(source, c.logicalSources(), c.logicalOutputs())
}

// ParseString parses aggregation source from string input.
func (c *Compiler) ParseString(source string) (compiler.IR, error) {
	return c.Parse([]byte(source))
}

// CompileString is a convenience wrapper that parses then compiles.
func (c *Compiler) CompileString(source string) (*compiler.Query, error) {
	ir, err := c.ParseString(source)
	if err != nil {
		return nil, err
	}
	return c.Compile(ir)
}

// Compile compiles parsed aggregation IR into a DBSP query.
func (c *Compiler) Compile(ir compiler.IR) (*compiler.Query, error) {
	p, ok := ir.(*program)
	if !ok {
		return nil, fmt.Errorf("aggregation: expected IR kind %q, got %T", (&program{}).IRKind(), ir)
	}
	return c.compileProgram(p)
}

// CompilePipeline compiles a typed pipeline.
func (c *Compiler) CompilePipeline(pipeline Pipeline) (*compiler.Query, error) {
	b, err := parseBranch(0, pipeline, c.logicalSources(), c.logicalOutputs(), false)
	if err != nil {
		return nil, err
	}
	return c.compileProgram(&program{Branches: []branchSpec{b}})
}

func (c *Compiler) compileProgram(p *program) (*compiler.Query, error) {
	compiled := circuit.New("aggregation")
	inputMap := map[string]string{}
	streamProducers := map[string][]string{}

	inputLogicalMap := map[string]string{}
	for _, src := range c.sources {
		id := circuit.InputNodeID(src.Name)
		if err := compiled.AddNode(circuit.Input(id)); err != nil {
			return nil, err
		}
		inputMap[src.Name] = id
		inputLogicalMap[src.Name] = src.Logical
		streamProducers[sourceStreamKey(src.Logical)] = append(streamProducers[sourceStreamKey(src.Logical)], id)
	}

	for _, bi := range p.Order {
		b := p.Branches[bi]
		outNodeID, err := c.compileBranch(compiled, b, streamProducers)
		if err != nil {
			return nil, err
		}
		streamProducers[outputStreamKey(b.Output)] = append(streamProducers[outputStreamKey(b.Output)], outNodeID)
	}

	outputMap := map[string]string{}
	outputLogicalMap := map[string]string{}
	for _, out := range c.outputs {
		producers := streamProducers[outputStreamKey(out.Logical)]
		if len(producers) == 0 {
			producers = streamProducers[sourceStreamKey(out.Logical)]
		}
		if len(producers) == 0 {
			return nil, fmt.Errorf("configured output %q (logical %q) is unbound", out.Name, out.Logical)
		}
		merged, err := ensureMergedStream(compiled, out.Logical, producers, "out")
		if err != nil {
			return nil, err
		}
		outID := circuit.OutputNodeID(out.Name)
		if err := compiled.AddNode(circuit.Output(outID)); err != nil {
			return nil, err
		}
		if err := compiled.AddEdge(circuit.NewEdge(merged, outID, 0)); err != nil {
			return nil, err
		}
		outputMap[out.Name] = outID
		outputLogicalMap[out.Name] = out.Logical
	}

	return &compiler.Query{Circuit: compiled, InputMap: inputMap, OutputMap: outputMap, InputLogicalMap: inputLogicalMap, OutputLogicalMap: outputLogicalMap}, nil
}

func toIdentityBindings(names []string) []Binding {
	out := make([]Binding, 0, len(names))
	for _, n := range names {
		out = append(out, Binding{Name: n, Logical: n})
	}
	return out
}

func normalizeBindings(in []Binding) []Binding {
	out := make([]Binding, 0, len(in))
	seenName := map[string]bool{}
	for _, b := range in {
		name := strings.TrimSpace(b.Name)
		logical := strings.TrimSpace(b.Logical)
		if name == "" {
			continue
		}
		if logical == "" {
			logical = name
		}
		if seenName[name] {
			continue
		}
		seenName[name] = true
		out = append(out, Binding{Name: name, Logical: logical})
	}
	return out
}

func (c *Compiler) logicalSources() []string {
	out := make([]string, 0, len(c.sources))
	for _, b := range c.sources {
		out = append(out, b.Logical)
	}
	return out
}

func (c *Compiler) logicalOutputs() []string {
	out := make([]string, 0, len(c.outputs))
	for _, b := range c.outputs {
		out = append(out, b.Logical)
	}
	return out
}

func sourceStreamKey(logical string) string {
	return "src:" + logical
}

func outputStreamKey(logical string) string {
	return "out:" + logical
}

func resolveInputStreamKey(logical string, streams map[string][]string) (string, bool) {
	out := outputStreamKey(logical)
	if _, ok := streams[out]; ok {
		return out, true
	}
	src := sourceStreamKey(logical)
	if _, ok := streams[src]; ok {
		return src, true
	}
	return "", false
}

func (c *Compiler) compileBranch(compiled *circuit.Circuit, b branchSpec, streamProducers map[string][]string) (string, error) {
	hasJoin := len(b.Stages) > 0 && b.Stages[0].Op == "@join"
	if hasJoin && len(b.Inputs) < 2 {
		return "", fmt.Errorf("branch[%d]: @join requires multiple inputs", b.Index)
	}
	if !hasJoin && len(b.Inputs) > 1 {
		return "", fmt.Errorf("branch[%d]: multiple inputs require @join as first stage", b.Index)
	}

	currentInputs := make([]string, 0, len(b.Inputs))
	for i, in := range b.Inputs {
		key, ok := resolveInputStreamKey(in, streamProducers)
		if !ok {
			return "", fmt.Errorf("branch[%d]: input %q is unbound", b.Index, in)
		}
		producers := streamProducers[key]
		if len(producers) == 0 {
			return "", fmt.Errorf("branch[%d]: input %q has no producers", b.Index, in)
		}
		merged, err := ensureMergedStream(compiled, in, producers, fmt.Sprintf("b%d_in%d", b.Index, i))
		if err != nil {
			return "", err
		}
		currentInputs = append(currentInputs, merged)
	}

	start := 0
	current := currentInputs[0]
	if hasJoin {
		nsNodes := make([]string, 0, len(currentInputs))
		for i, sourceNode := range currentInputs {
			nsID := fmt.Sprintf("b%d_ns_%d_%s", b.Index, i, circuit.SanitizeNodeID(b.Inputs[i]))
			src := b.Inputs[i]
			nsExpr := expression.NewCompiled(func(ctx *expression.EvalContext) (any, error) {
				doc := ctx.Document()
				if doc == nil {
					return nil, fmt.Errorf("@join namespace: missing document")
				}
				return product.New(map[string]datamodel.Document{src: doc}), nil
			}, dbspexpr.NewSet(dbspexpr.NewString(src), dbspexpr.NewCopy()))
			if err := compiled.AddNode(circuit.Op(nsID, operator.NewProject(nsExpr))); err != nil {
				return "", err
			}
			if err := compiled.AddEdge(circuit.NewEdge(sourceNode, nsID, 0)); err != nil {
				return "", err
			}
			nsNodes = append(nsNodes, nsID)
		}

		current = nsNodes[0]
		for i := 1; i < len(nsNodes); i++ {
			cartID := fmt.Sprintf("b%d_join_cart_%d", b.Index, i)
			if err := compiled.AddNode(circuit.Op(cartID, operator.NewCartesianProduct())); err != nil {
				return "", err
			}
			if err := compiled.AddEdge(circuit.NewEdge(current, cartID, 0)); err != nil {
				return "", err
			}
			if err := compiled.AddEdge(circuit.NewEdge(nsNodes[i], cartID, 1)); err != nil {
				return "", err
			}
			current = cartID
		}

		predExpr := b.Stages[0].Predicate
		if predExpr == nil {
			return "", wrapStageErr(b.Stages[0].Index, b.Stages[0].Op, "predicate", b.Stages[0].RawArgs, fmt.Errorf("missing parsed predicate"))
		}
		joinSelID := fmt.Sprintf("b%d_join_select", b.Index)
		if err := compiled.AddNode(circuit.Op(joinSelID, operator.NewSelect(predExpr))); err != nil {
			return "", err
		}
		if err := compiled.AddEdge(circuit.NewEdge(current, joinSelID, 0)); err != nil {
			return "", err
		}
		current = joinSelID
		start = 1
	}

	for i := start; i < len(b.Stages); i++ {
		stage := b.Stages[i]
		id := fmt.Sprintf("b%d_op_%d", b.Index, stage.Index)
		switch stage.Op {
		case "@select":
			expr := stage.Predicate
			if expr == nil {
				return "", wrapStageErr(stage.Index, stage.Op, "predicate", stage.RawArgs, fmt.Errorf("missing parsed predicate"))
			}
			pred := expression.NewCompiled(func(ctx *expression.EvalContext) (any, error) {
				v, err := expr.Evaluate(ctx)
				if errors.Is(err, datamodel.ErrFieldNotFound) {
					return false, nil
				}
				return v, err
			}, expr)
			if err := compiled.AddNode(circuit.Op(id, operator.NewSelect(pred))); err != nil {
				return "", err
			}
		case "@project":
			if stage.Projection == nil {
				return "", wrapStageErr(stage.Index, stage.Op, "projection", stage.RawArgs, fmt.Errorf("missing parsed projection"))
			}
			if err := compiled.AddNode(circuit.Op(id, operator.NewProject(stage.Projection))); err != nil {
				return "", err
			}
		case "@unwind":
			path := stage.UnwindPath
			if path == "" {
				return "", wrapStageErr(stage.Index, stage.Op, "path", stage.RawArgs, fmt.Errorf("missing parsed unwind path"))
			}
			fieldPath := strings.TrimPrefix(path, "$.")
			op := operator.NewUnwind(fieldPath).WithNameAppend(true)
			if err := compiled.AddNode(circuit.Op(id, op)); err != nil {
				return "", err
			}
		case "@groupBy":
			if stage.GroupBy == nil {
				return "", wrapStageErr(stage.Index, stage.Op, "arguments", stage.RawArgs, fmt.Errorf("missing parsed groupBy op"))
			}
			if err := compiled.AddNode(circuit.Op(id, stage.GroupBy)); err != nil {
				return "", err
			}
		case "@aggregate", "@gather", "@mux":
			return "", wrapStageErr(stage.Index, stage.Op, "stage", stage.RawArgs, fmt.Errorf("%s is not supported; use @groupBy and @project", stage.Op))
		default:
			return "", wrapStageErr(stage.Index, stage.Op, "stage", stage.RawArgs, fmt.Errorf("unsupported pipeline operation: %s", stage.Op))
		}

		if err := compiled.AddEdge(circuit.NewEdge(current, id, 0)); err != nil {
			return "", err
		}
		current = id
	}

	return current, nil
}

func ensureMergedStream(compiled *circuit.Circuit, name string, producers []string, scope string) (string, error) {
	if len(producers) == 0 {
		return "", fmt.Errorf("stream %q has no producers", name)
	}
	if len(producers) == 1 {
		return producers[0], nil
	}
	lcID := fmt.Sprintf("lc_%s_%s", scope, circuit.SanitizeNodeID(name))
	coeffs := make([]int, len(producers))
	for i := range coeffs {
		coeffs[i] = 1
	}
	if err := compiled.AddNode(circuit.Op(lcID, operator.NewLinearCombination(coeffs))); err != nil {
		return "", err
	}
	for i, pid := range producers {
		if err := compiled.AddEdge(circuit.NewEdge(pid, lcID, i)); err != nil {
			return "", err
		}
	}
	return lcID, nil
}

type projectAssignment struct {
	path string
	expr expression.Expression
}

func compileProjectExpression(args json.RawMessage, stageIndex int, stageOp string) (expression.Expression, error) {
	stages, err := normalizeProjectStages(args, stageIndex, stageOp)
	if err != nil {
		return nil, err
	}

	assignments := make([]projectAssignment, 0)
	hasCopy := false
	for _, stage := range stages {
		keys := make([]string, 0, len(stage))
		for k := range stage {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, key := range keys {
			rawExpr := stage[key]
			expr, err := dbspexpr.NewParser().Parse(rawExpr)
			if err != nil {
				return nil, wrapStageErr(stageIndex, stageOp, fmt.Sprintf("projection[%q]", key), rawExpr, err)
			}
			if key == "$." {
				hasCopy = true
				assignments = append(assignments, projectAssignment{path: "", expr: expr})
				continue
			}
			path := key
			if strings.HasPrefix(path, "$.") {
				path = strings.TrimPrefix(path, "$.")
			}
			assignments = append(assignments, projectAssignment{path: path, expr: expr})
		}
	}

	original, err := dbspexpr.NewParser().Parse(args)
	if err != nil {
		return nil, wrapStageErr(stageIndex, stageOp, "projection", args, err)
	}

	return expression.NewCompiled(func(ctx *expression.EvalContext) (any, error) {
		accum := map[string]any{}
		for _, asg := range assignments {
			val, err := asg.expr.Evaluate(ctx)
			if err != nil {
				return nil, err
			}
			if asg.path == "" {
				m, ok := val.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("$. assignment must evaluate to map, got %T", val)
				}
				for k, v := range m {
					accum[k] = deepCopyAny(v)
				}
				continue
			}
			setNestedMap(accum, asg.path, val)
		}
		if !hasCopy && len(assignments) == 0 {
			return unstructured.New(map[string]any{}, nil), nil
		}
		return unstructured.New(accum, nil), nil
	}, original), nil
}

func normalizeProjectStages(args json.RawMessage, stageIndex int, stageOp string) ([]map[string]json.RawMessage, error) {
	var list []map[string]json.RawMessage
	if err := json.Unmarshal(args, &list); err == nil {
		return list, nil
	}

	var one map[string]json.RawMessage
	if err := json.Unmarshal(args, &one); err != nil {
		return nil, wrapStageErr(stageIndex, stageOp, "projection", args, fmt.Errorf("argument must be object or list of objects"))
	}
	return []map[string]json.RawMessage{one}, nil
}

// compileGroupByOp compiles @groupBy arguments into an engine/operator GroupBy.
//
// Argument forms:
//   - [keyExpr, valueExpr]
//     Produces rows of the form
//     {"key": <group-key>, "values": [...], "documents": [...]}.
//   - [keyExpr, valueExpr, options]
//     options is an object. Currently supported key: "distinct" (bool).
//
// Running-text examples:
//   - {"@groupBy":["$.metadata.namespace","$.spec.a"]} groups values and
//     emits key/values/documents lists.
func compileGroupByOp(args json.RawMessage, stageIndex int, stageOp string) (operator.Operator, error) {
	var list []json.RawMessage
	if err := json.Unmarshal(args, &list); err != nil {
		return nil, wrapStageErr(stageIndex, stageOp, "arguments", args, fmt.Errorf("argument must be [keyExpr, valueExpr] or [keyExpr, valueExpr, options]"))
	}
	if len(list) < 2 || len(list) > 3 {
		return nil, wrapStageErr(stageIndex, stageOp, "arguments", args, fmt.Errorf("expected 2 or 3 arguments"))
	}

	parseExpr := func(raw json.RawMessage) (expression.Expression, error) {
		if string(raw) == "null" {
			return nil, nil
		}
		return dbspexpr.NewParser().Parse(raw)
	}

	keyExpr, err := parseExpr(list[0])
	if err != nil {
		return nil, wrapStageErr(stageIndex, stageOp, "keyExpr", list[0], err)
	}
	valueExpr, err := parseExpr(list[1])
	if err != nil {
		return nil, wrapStageErr(stageIndex, stageOp, "valueExpr", list[1], err)
	}

	op := operator.NewGroupBy(keyExpr, valueExpr)
	if len(list) == 3 {
		opts := struct {
			Distinct bool `json:"distinct"`
		}{}
		if err := json.Unmarshal(list[2], &opts); err != nil {
			return nil, wrapStageErr(stageIndex, stageOp, "options", list[2], fmt.Errorf("options must be an object"))
		}
		op.WithDistinct(opts.Distinct)
	}

	return op, nil
}

func setNestedMap(m map[string]any, path string, value any) {
	parts := strings.SplitN(path, ".", 2)
	if len(parts) == 1 {
		m[path] = value
		return
	}
	sub, ok := m[parts[0]].(map[string]any)
	if !ok {
		sub = map[string]any{}
		m[parts[0]] = sub
	}
	setNestedMap(sub, parts[1], value)
}

func deepCopyAny(v any) any {
	switch val := v.(type) {
	case map[string]any:
		cp := make(map[string]any, len(val))
		for k, vv := range val {
			cp[k] = deepCopyAny(vv)
		}
		return cp
	case []any:
		cp := make([]any, len(val))
		for i, vv := range val {
			cp[i] = deepCopyAny(vv)
		}
		return cp
	default:
		return v
	}
}

var _ compiler.Compiler = (*Compiler)(nil)
