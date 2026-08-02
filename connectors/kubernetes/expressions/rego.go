package expressions

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"sync"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/open-policy-agent/opa/v1/ast"
	"github.com/open-policy-agent/opa/v1/rego"
)

// regoEval implements {"@rego": [source, parameters, object]}: a Gatekeeper
// ConstraintTemplate policy evaluated against a single Kubernetes object.
//
// The source is the Rego module of a template target (the policy defines a
// `violation` rule in its own package, per the Gatekeeper convention), the
// parameters are the constraint's spec.parameters, and the object is the
// resource under evaluation. The module is parsed as Rego v0, the dialect
// Gatekeeper templates are written in; modules importing rego.v1 opt into
// the v1 syntax as usual.
//
// The policy sees the Gatekeeper input contract, restricted to the fields an
// audit-time evaluation has:
//
//	input.parameters        the constraint parameters
//	input.review.object     the object under evaluation
//	input.review.kind       {group, version, kind} parsed from the object
//	input.review.name       the object's metadata.name
//	input.review.namespace  the object's metadata.namespace
//
// The result is a document, never an error, so one broken policy cannot
// abort a circuit step that also carries healthy policies:
//
//	{"violations": [{"msg": ..., ...}, ...], "error": nil}    on success
//	{"violations": [],                      "error": "..."}   on compile or
//	                                                          eval failure
//
// Builtin errors follow the OPA default (the semantics Gatekeeper evaluates
// under): a failing builtin makes the enclosing expression undefined rather
// than raising an error, so such a rule silently reports no violation.
//
// The violations list is the policy's violation set in canonical order
// (sorted by the sorted-key JSON serialization), so the result is a
// deterministic function of the arguments (the callback purity contract).
// Calling the operator with a nil object compiles the policy without
// evaluating anything against it, which is how a pipeline validates a
// template at load time.
func regoEval(args []any) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("@rego expects [source, parameters, object], got %d arguments", len(args))
	}
	source, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("@rego: source must be a string, got %T", args[0])
	}

	entry := prepare(source)
	if entry.err != nil {
		return regoResult(nil, entry.err), nil
	}
	if args[2] == nil {
		// Compile check only.
		return regoResult(nil, nil), nil
	}

	parameters, err := toJSONValue(args[1])
	if err != nil {
		return nil, fmt.Errorf("@rego: parameters: %w", err)
	}
	object, err := toJSONValue(args[2])
	if err != nil {
		return nil, fmt.Errorf("@rego: object: %w", err)
	}

	input := map[string]any{
		"parameters": parameters,
		"review":     reviewOf(object),
	}
	rs, err := entry.query.Eval(context.Background(), rego.EvalInput(input))
	if err != nil {
		return regoResult(nil, err), nil
	}
	if len(rs) == 0 || len(rs[0].Expressions) == 0 {
		return regoResult(nil, nil), nil
	}
	violations, ok := rs[0].Expressions[0].Value.([]any)
	if !ok {
		return regoResult(nil, fmt.Errorf("violation rule evaluated to %T, expected a set", rs[0].Expressions[0].Value)), nil
	}
	canonical, err := canonicalize(violations)
	if err != nil {
		return regoResult(nil, err), nil
	}
	return regoResult(canonical, nil), nil
}

// regoResult builds the operator's result document.
func regoResult(violations []any, err error) map[string]any {
	result := map[string]any{"violations": violations, "error": nil}
	if violations == nil {
		result["violations"] = []any{}
	}
	if err != nil {
		result["error"] = err.Error()
	}
	return result
}

// preparedEntry is a compiled policy: the prepared query for its violation
// rule, or the compile error. Both outcomes are cached: a broken policy is
// evaluated against every matched object, and re-parsing it per object would
// turn one bad template into a per-step parse storm.
type preparedEntry struct {
	query rego.PreparedEvalQuery
	err   error
}

var (
	preparedMu    sync.Mutex
	prepared      = map[[sha256.Size]byte]*preparedEntry{}
	preparedLimit = 128
)

// prepare returns the cached compilation of a policy source, compiling on
// first use. The cache is memoization of a pure function, keyed by the
// source hash; policy sets are small in practice, so when the cache
// overflows the limit (edited templates accumulating in a long-running
// auditor) it is simply reset.
func prepare(source string) *preparedEntry {
	key := sha256.Sum256([]byte(source))

	preparedMu.Lock()
	defer preparedMu.Unlock()
	if entry, ok := prepared[key]; ok {
		return entry
	}
	if len(prepared) >= preparedLimit {
		prepared = map[[sha256.Size]byte]*preparedEntry{}
	}
	entry := compile(source)
	prepared[key] = entry
	return entry
}

// compile parses the module to find its package, then prepares a query for
// the violation rule in that package.
func compile(source string) *preparedEntry {
	module, err := ast.ParseModuleWithOpts("policy.rego", source, ast.ParserOptions{
		RegoVersion: ast.RegoV0,
	})
	if err != nil {
		return &preparedEntry{err: err}
	}
	query := module.Package.Path.String() + ".violation"

	q, err := rego.New(
		rego.Query(query),
		rego.Module("policy.rego", source),
		rego.SetRegoVersion(ast.RegoV0),
	).PrepareForEval(context.Background())
	if err != nil {
		return &preparedEntry{err: err}
	}
	return &preparedEntry{query: q}
}

// reviewOf assembles the input.review document for an object.
func reviewOf(object any) map[string]any {
	review := map[string]any{"object": object}
	obj, ok := object.(map[string]any)
	if !ok {
		return review
	}
	u := unstructured.Unstructured{Object: obj}
	if gvk := u.GroupVersionKind(); gvk.Kind != "" {
		review["kind"] = map[string]any{"group": gvk.Group, "version": gvk.Version, "kind": gvk.Kind}
	}
	if name := u.GetName(); name != "" {
		review["name"] = name
	}
	if namespace := u.GetNamespace(); namespace != "" {
		review["namespace"] = namespace
	}
	return review
}

// toJSONValue normalizes an expression value into plain JSON types through a
// serialization round trip: expression values may carry documents (types
// implementing json.Marshaler), while OPA input conversion expects the plain
// form.
func toJSONValue(value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var plain any
	if err := json.Unmarshal(raw, &plain); err != nil {
		return nil, err
	}
	return plain, nil
}

// canonicalize orders a violation list by its sorted-key JSON serialization
// and normalizes the values to plain JSON types. Rego sets carry no order,
// so without the sort the same violations could serialize differently
// across evaluations, and a result that differs between an insertion and
// its matching retraction corrupts downstream state.
func canonicalize(violations []any) ([]any, error) {
	type keyed struct {
		key   string
		value any
	}
	keyedViolations := make([]keyed, 0, len(violations))
	for _, v := range violations {
		// encoding/json serializes map keys in sorted order, so the
		// serialization doubles as the canonical sort key.
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		var plain any
		if err := json.Unmarshal(raw, &plain); err != nil {
			return nil, err
		}
		keyedViolations = append(keyedViolations, keyed{key: string(raw), value: plain})
	}
	sort.Slice(keyedViolations, func(i, j int) bool {
		return keyedViolations[i].key < keyedViolations[j].key
	})
	result := make([]any, 0, len(keyedViolations))
	for _, kv := range keyedViolations {
		result = append(result, kv.value)
	}
	return result, nil
}
