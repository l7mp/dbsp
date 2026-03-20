package aggregation

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/compiler"
	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/product"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/expression"
	exprdbsp "github.com/l7mp/dbsp/engine/expression/dbsp"
	"github.com/l7mp/dbsp/engine/operator"
)

// Compiler compiles aggregation pipelines into DBSP circuits.
type Compiler struct {
	sources []string
	outputs []string
}

var idSanitizer = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

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

// New creates a new aggregation compiler.
func New(sources, outputs []string) *Compiler {
	out := append([]string(nil), outputs...)
	if len(out) == 0 {
		out = []string{"output"}
	}
	return &Compiler{sources: append([]string(nil), sources...), outputs: out}
}

// Parse parses aggregation source into compiler IR.
func (c *Compiler) Parse(source []byte) (compiler.IR, error) {
	return parseProgram(source, c.sources, c.outputs)
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
	b, err := parseBranch(0, pipeline, c.sources, c.outputs, false)
	if err != nil {
		return nil, err
	}
	return c.compileProgram(&program{Branches: []branchSpec{b}})
}

func (c *Compiler) compileProgram(p *program) (*compiler.Query, error) {
	compiled := circuit.New("aggregation")
	inputMap := map[string]string{}
	streamProducers := map[string][]string{}

	for _, src := range c.sources {
		id := "input_" + sanitizeID(src)
		if err := compiled.AddNode(circuit.Input(id)); err != nil {
			return nil, err
		}
		inputMap[src] = id
		streamProducers[src] = append(streamProducers[src], id)
	}

	for _, bi := range p.Order {
		b := p.Branches[bi]
		outNodeID, err := c.compileBranch(compiled, b, streamProducers)
		if err != nil {
			return nil, err
		}
		streamProducers[b.Output] = append(streamProducers[b.Output], outNodeID)
	}

	outputMap := map[string]string{}
	for _, logicalOut := range c.outputs {
		producers := streamProducers[logicalOut]
		if len(producers) == 0 {
			return nil, fmt.Errorf("configured output %q is unbound", logicalOut)
		}
		merged, err := ensureMergedStream(compiled, logicalOut, producers, "out")
		if err != nil {
			return nil, err
		}
		outID := "output_" + sanitizeID(logicalOut)
		if err := compiled.AddNode(circuit.Output(outID)); err != nil {
			return nil, err
		}
		if err := compiled.AddEdge(circuit.NewEdge(merged, outID, 0)); err != nil {
			return nil, err
		}
		outputMap[logicalOut] = outID
	}

	return &compiler.Query{Circuit: compiled, InputMap: inputMap, OutputMap: outputMap}, nil
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
		producers := streamProducers[in]
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
			nsID := fmt.Sprintf("b%d_ns_%d_%s", b.Index, i, sanitizeID(b.Inputs[i]))
			src := b.Inputs[i]
			nsExpr := expression.Func(func(ctx *expression.EvalContext) (any, error) {
				doc := ctx.Document()
				if doc == nil {
					return nil, fmt.Errorf("@join namespace: missing document")
				}
				return product.New(map[string]datamodel.Document{src: doc}), nil
			})
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
			pred := expression.Func(func(ctx *expression.EvalContext) (any, error) {
				v, err := expr.Evaluate(ctx)
				if errors.Is(err, datamodel.ErrFieldNotFound) {
					return false, nil
				}
				return v, err
			})
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
		case "@aggregate":
			if stage.Aggregate == nil {
				return "", wrapStageErr(stage.Index, stage.Op, "arguments", stage.RawArgs, fmt.Errorf("missing parsed aggregate op"))
			}
			if err := compiled.AddNode(circuit.Op(id, stage.Aggregate)); err != nil {
				return "", err
			}
		case "@gather", "@mux":
			return "", wrapStageErr(stage.Index, stage.Op, "stage", stage.RawArgs, fmt.Errorf("@gather is not supported; use @aggregate"))
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
	lcID := fmt.Sprintf("lc_%s_%s", scope, sanitizeID(name))
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

func sanitizeID(s string) string {
	s = idSanitizer.ReplaceAllString(s, "_")
	s = strings.Trim(s, "_")
	if s == "" {
		return "anon"
	}
	return s
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
			expr, err := exprdbsp.NewParser().Parse(rawExpr)
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

	return expression.Func(func(ctx *expression.EvalContext) (any, error) {
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
	}), nil
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

func compileAggregateOp(args json.RawMessage, stageIndex int, stageOp string) (operator.Operator, error) {
	var list []json.RawMessage
	if err := json.Unmarshal(args, &list); err != nil {
		return nil, wrapStageErr(stageIndex, stageOp, "arguments", args, fmt.Errorf("argument must be [keyExpr, valueExpr] or [keyExpr, valueExpr, reduceExpr, optional setExpr|outField]"))
	}
	if len(list) < 2 || len(list) > 4 {
		return nil, wrapStageErr(stageIndex, stageOp, "arguments", args, fmt.Errorf("expected 2, 3 or 4 arguments"))
	}

	parseExpr := func(raw json.RawMessage) (expression.Expression, error) {
		if string(raw) == "null" {
			return nil, nil
		}
		return exprdbsp.NewParser().Parse(raw)
	}

	keyExpr, err := parseExpr(list[0])
	if err != nil {
		return nil, wrapStageErr(stageIndex, stageOp, "keyExpr", list[0], err)
	}
	valueExpr, err := parseExpr(list[1])
	if err != nil {
		return nil, wrapStageErr(stageIndex, stageOp, "valueExpr", list[1], err)
	}

	if len(list) == 2 {
		var valuePath string
		if err := json.Unmarshal(list[1], &valuePath); err != nil {
			return nil, wrapStageErr(stageIndex, stageOp, "valueExpr", list[1], fmt.Errorf("2-arg form requires string JSONPath as second argument"))
		}
		setExpr := exprdbsp.NewSet(exprdbsp.NewString(valuePath), exprdbsp.NewSubject())
		return operator.NewAggregateWithSet(keyExpr, valueExpr, exprdbsp.NewSubject(), setExpr), nil
	}

	reduceExpr, err := parseExpr(list[2])
	if err != nil {
		return nil, wrapStageErr(stageIndex, stageOp, "reduceExpr", list[2], err)
	}

	if len(list) == 4 {
		var outField string
		if err := json.Unmarshal(list[3], &outField); err == nil {
			return operator.NewAggregate(keyExpr, valueExpr, reduceExpr, outField), nil
		}
		setExpr, err := parseExpr(list[3])
		if err != nil {
			return nil, wrapStageErr(stageIndex, stageOp, "setExpr", list[3], err)
		}
		return operator.NewAggregateWithSet(keyExpr, valueExpr, reduceExpr, setExpr), nil
	}

	return operator.NewAggregate(keyExpr, valueExpr, reduceExpr, "value"), nil
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
