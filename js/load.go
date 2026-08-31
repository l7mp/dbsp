package js

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dop251/goja"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	k8sconsumer "github.com/l7mp/dbsp/connectors/kubernetes/consumer"
	k8sproducer "github.com/l7mp/dbsp/connectors/kubernetes/producer"
	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	kpredicate "github.com/l7mp/dbsp/connectors/kubernetes/runtime/predicate"
	misc "github.com/l7mp/dbsp/connectors/misc"
	xds "github.com/l7mp/dbsp/connectors/xds"
	aggcompiler "github.com/l7mp/dbsp/engine/compiler/aggregation"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/spec"
	"github.com/l7mp/dbsp/engine/transform"
)

// operator.load(name, spec) loads a serialized operator (engine/spec): it
// compiles every controller's pipeline, applies its transform chain,
// installs the circuits, and binds the sources and targets through the
// connectors, routed by the resource group. The one loader behind every
// frontend: a spec frozen from a script and an Operator CRD applied
// through dcontroller run through the same code.

// Connector groups: every connector except Kubernetes lives under
// connector.dcontroller.io; Kubernetes API groups and the operator's own
// view group bind through the Kubernetes connector.
const (
	xdsGroup  = "xds.connector.dcontroller.io"
	miscGroup = "misc.connector.dcontroller.io"
)

// xdsKinds maps the xds connector's resource kinds to delta-ADS type
// shorthands.
var xdsKinds = map[string]string{
	"Listener":              "lds",
	"RouteConfiguration":    "rds",
	"Cluster":               "cds",
	"ClusterLoadAssignment": "eds",
}

// operatorInstance is one loaded operator: its circuits, its bindings, its
// topics and view registrations, all torn down by close in reverse order.
type operatorInstance struct {
	vm        *VM
	name      string
	rawSpec   json.RawMessage
	procs     []*dbspruntime.Circuit
	runnables []dbspruntime.Runnable
	topics    []string
	viewGVKs  []schema.GroupVersionKind
	comps     map[string]bool
	closed    bool
}

func (v *VM) operatorLoad(call goja.FunctionCall) (goja.Value, error) {
	name, ok := call.Argument(0).Export().(string)
	if !ok || strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("operator.load(name, spec): expected non-empty string name, got %s", describeCall(call))
	}
	raw, err := json.Marshal(call.Argument(1).Export())
	if err != nil {
		return nil, fmt.Errorf("operator.load spec: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var op spec.OperatorSpec
	if err := dec.Decode(&op); err != nil {
		return nil, fmt.Errorf("operator.load spec: %w", err)
	}
	if err := op.Validate(); err != nil {
		return nil, fmt.Errorf("operator.load: %w", err)
	}
	v.compileStarted = true

	inst := &operatorInstance{vm: v, name: name, rawSpec: raw, comps: map[string]bool{}}
	if err := inst.load(&op); err != nil {
		inst.close()
		return nil, fmt.Errorf("operator.load: %w", err)
	}
	return inst.jsObject(), nil
}

// resolveSpecGVK resolves a spec resource to a GVK: connector groups pass
// through as is, everything else resolves through the Kubernetes runtime's
// rules (absent group = the operator's own view group).
func (inst *operatorInstance) resolveSpecGVK(r spec.Resource) (schema.GroupVersionKind, error) {
	if r.Group != nil {
		switch g := strings.TrimSpace(*r.Group); g {
		case xdsGroup, miscGroup:
			return schema.GroupVersionKind{Group: g, Version: "v1", Kind: r.Kind}, nil
		}
	}
	return inst.vm.resolveRuntimeGVK(k8sResolveGVKOptions{
		Operator: inst.name,
		APIGroup: r.Group,
		Version:  r.Version,
		Kind:     r.Kind,
	})
}

func (inst *operatorInstance) load(op *spec.OperatorSpec) error {
	v := inst.vm

	// The operator's own view kinds, registered before anything compiles.
	ownGroup := viewv1a1.Group(inst.name)
	seen := map[schema.GroupVersionKind]bool{}
	for ci := range op.Controllers {
		c := &op.Controllers[ci]
		resources := make([]spec.Resource, 0, len(c.Sources)+len(c.Targets))
		for _, s := range c.Sources {
			resources = append(resources, s.Resource)
		}
		for _, t := range c.Targets {
			resources = append(resources, t.Resource)
		}
		for _, r := range resources {
			gvk, err := inst.resolveSpecGVK(r)
			if err != nil {
				return fmt.Errorf("controller %q: %w", c.Name, err)
			}
			if gvk.Group == ownGroup && !seen[gvk] {
				seen[gvk] = true
				inst.viewGVKs = append(inst.viewGVKs, gvk)
			}
		}
	}
	if len(inst.viewGVKs) > 0 {
		if err := v.registerViewGVKList(inst.viewGVKs); err != nil {
			return err
		}
	}

	for ci := range op.Controllers {
		if err := inst.loadController(&op.Controllers[ci]); err != nil {
			return fmt.Errorf("controller %q: %w", op.Controllers[ci].Name, err)
		}
	}
	return nil
}

func (inst *operatorInstance) loadController(c *spec.Controller) error {
	v := inst.vm
	if c.Pipeline == nil {
		return fmt.Errorf("only pipeline programs are supported")
	}
	prefix := inst.name + "." + c.Name

	inputs := make([]aggcompiler.Binding, 0, len(c.Sources))
	for _, s := range c.Sources {
		inputs = append(inputs, aggcompiler.Binding{Name: prefix + "/" + s.Kind + "/input", Logical: s.Kind})
	}
	outputs := make([]aggcompiler.Binding, 0, len(c.Targets))
	for _, t := range c.Targets {
		outputs = append(outputs, aggcompiler.Binding{Name: prefix + "/" + t.Kind + "/output", Logical: t.Kind})
	}

	// The controller owns its topics: start from clean retained integrals
	// so this instance never bootstraps from a predecessor's leftovers.
	for _, b := range append(append([]aggcompiler.Binding{}, inputs...), outputs...) {
		v.runtime.ResetTopic(b.Name)
		inst.topics = append(inst.topics, b.Name)
	}

	compiled, err := aggcompiler.New(inputs, outputs).CompileString(string(*c.Pipeline))
	if err != nil {
		return err
	}
	compiled.Circuit.SetName(prefix)
	if err := validateCircuit(compiled.Circuit); err != nil {
		return err
	}

	// The transform chain, in canonical order; an absent list means the
	// default Reconciler, Distincter, Incrementalizer chain.
	entries := c.Transforms
	if len(entries) == 0 {
		entries = []transform.TransformSpec{{Name: "Reconciler"}, {Name: "Distincter"}, {Name: "Incrementalizer"}}
	}
	chain, err := transform.NewChainFromSpecs(entries)
	if err != nil {
		return fmt.Errorf("transform: %w", err)
	}
	transformed, err := chain.Transform(compiled.Circuit)
	if err != nil {
		return fmt.Errorf("transform: %w", err)
	}
	if err := validateCircuit(transformed); err != nil {
		return fmt.Errorf("transform: %w", err)
	}

	query := *compiled
	query.Circuit = transformed
	proc, err := dbspruntime.NewCircuit(prefix, v.runtime, &query, v.logger)
	if err != nil {
		return fmt.Errorf("runtime circuit: %w", err)
	}
	if err := v.runtime.Add(proc); err != nil {
		return fmt.Errorf("runtime add circuit: %w", err)
	}
	inst.procs = append(inst.procs, proc)
	inst.comps[prefix] = true

	// Targets first: the output consumers must be subscribed before any
	// source starts flowing, or the circuit's first outputs land in the
	// topic integral and replay as one batched delta.
	for i := range c.Targets {
		if err := inst.bindTarget(prefix, &c.Targets[i]); err != nil {
			return err
		}
	}
	for i := range c.Sources {
		if err := inst.bindSource(prefix, &c.Sources[i]); err != nil {
			return err
		}
	}
	return nil
}

func (inst *operatorInstance) add(r dbspruntime.Runnable) error {
	if err := inst.vm.runtime.Add(r); err != nil {
		return err
	}
	inst.runnables = append(inst.runnables, r)
	inst.comps[r.Name()] = true
	return nil
}

func rawParams(p *json.RawMessage, target any) error {
	if p == nil {
		return nil
	}
	return json.Unmarshal(*p, target)
}

func (inst *operatorInstance) bindTarget(prefix string, t *spec.Target) error {
	v := inst.vm
	topic := prefix + "/" + t.Kind + "/output"
	gvk, err := inst.resolveSpecGVK(t.Resource)
	if err != nil {
		return err
	}

	switch gvk.Group {
	case miscGroup:
		return fmt.Errorf("target %q: the misc connector offers no targets", t.Kind)

	case xdsGroup:
		if t.Type != "" && t.Type != spec.Updater {
			return fmt.Errorf("target %q: unknown xds target type %q", t.Kind, t.Type)
		}
		typ, ok := xdsKinds[t.Kind]
		if !ok {
			return fmt.Errorf("target %q: unknown xds resource kind", t.Kind)
		}
		srv, err := inst.ensureXDSServer(t.Parameters)
		if err != nil {
			return fmt.Errorf("target %q: %w", t.Kind, err)
		}
		r, err := xds.NewConsumerFromSpec(srv.srv, topic, xds.ConsumerSpec{Type: typ, Server: srv.name, Level: t.Level}, v.logger)
		if err != nil {
			return fmt.Errorf("target %q: %w", t.Kind, err)
		}
		return inst.add(r)

	default:
		verb := "update"
		switch t.Type {
		case "", spec.Updater:
		case spec.Patcher:
			verb = "patch"
		default:
			return fmt.Errorf("target %q: unknown target type %q", t.Kind, t.Type)
		}
		krt, err := v.ensureK8sRuntime()
		if err != nil {
			return err
		}
		client, err := krt.NewCompositeClient()
		if err != nil {
			return fmt.Errorf("target %q: connector client: %w", t.Kind, err)
		}
		r, err := k8sconsumer.NewFromSpec(topic, verb, k8sconsumer.Spec{GVK: gvkString(gvk), Level: t.Level}, k8sconsumer.Deps{
			Client: client, GVK: gvk, Runtime: v.runtime, Logger: v.logger,
		})
		if err != nil {
			return fmt.Errorf("target %q: %w", t.Kind, err)
		}
		return inst.add(r)
	}
}

func (inst *operatorInstance) bindSource(prefix string, s *spec.Source) error {
	v := inst.vm
	topic := prefix + "/" + s.Kind + "/input"
	gvk, err := inst.resolveSpecGVK(s.Resource)
	if err != nil {
		return err
	}

	switch gvk.Group {
	case miscGroup:
		var ts misc.TriggerSpec
		if err := rawParams(s.Parameters, &ts); err != nil {
			return fmt.Errorf("source %q parameters: %w", s.Kind, err)
		}
		ts.Kind = s.Kind
		if s.Namespace != nil {
			ts.Namespace = *s.Namespace
		}
		deps := misc.Deps{Runtime: v.runtime, Logger: v.logger}
		var r dbspruntime.Runnable
		switch s.Type {
		case "", spec.Watcher, spec.Tick:
			r, err = misc.NewTick(topic, ts, deps)
		case spec.Init:
			r, err = misc.NewInit(topic, ts, deps)
		default:
			return fmt.Errorf("source %q: unknown misc source type %q", s.Kind, s.Type)
		}
		if err != nil {
			return fmt.Errorf("source %q: %w", s.Kind, err)
		}
		return inst.add(r)

	case xdsGroup:
		if s.Type != "" && s.Type != spec.Watcher {
			return fmt.Errorf("source %q: unknown xds source type %q", s.Kind, s.Type)
		}
		typ, ok := xdsKinds[s.Kind]
		if !ok {
			return fmt.Errorf("source %q: unknown xds resource kind", s.Kind)
		}
		var ps xds.ProducerSpec
		if err := rawParams(s.Parameters, &ps); err != nil {
			return fmt.Errorf("source %q parameters: %w", s.Kind, err)
		}
		ps.Type = typ
		ps.Level = s.Level
		r, err := xds.NewProducerFromSpec(topic, ps, xds.Deps{Runtime: v.runtime, Logger: v.logger})
		if err != nil {
			return fmt.Errorf("source %q: %w", s.Kind, err)
		}
		return inst.add(r)

	default:
		if s.Type != "" && s.Type != spec.Watcher {
			return fmt.Errorf("source %q: unknown source type %q", s.Kind, s.Type)
		}
		pspec := k8sproducer.Spec{GVK: gvkString(gvk), Level: s.Level}
		if s.Namespace != nil {
			pspec.Namespace = *s.Namespace
		}
		if s.LabelSelector != nil {
			var sel v1.LabelSelector
			if err := json.Unmarshal(*s.LabelSelector, &sel); err != nil {
				return fmt.Errorf("source %q labelSelector: %w", s.Kind, err)
			}
			pspec.LabelSelector = &sel
		}
		if s.Predicate != nil {
			var pred kpredicate.Predicate
			if err := json.Unmarshal(*s.Predicate, &pred); err != nil {
				return fmt.Errorf("source %q predicate: %w", s.Kind, err)
			}
			pspec.Predicate = &pred
		}
		krt, err := v.ensureK8sRuntime()
		if err != nil {
			return err
		}
		client, err := krt.NewCompositeClient()
		if err != nil {
			return fmt.Errorf("source %q: connector client: %w", s.Kind, err)
		}
		r, err := k8sproducer.NewFromSpec(topic, pspec, k8sproducer.Deps{
			Client: client, GVK: gvk, Runtime: v.runtime, Logger: v.logger,
		})
		if err != nil {
			return fmt.Errorf("source %q: %w", s.Kind, err)
		}
		return inst.add(r)
	}
}

// ensureXDSServer resolves (starting if needed) the operator's egress
// server: the server name is the operator's, or "<operator>/<server>"
// when the target parameters name one, so operators never share servers.
type xdsServerRef struct {
	srv  *xds.Server
	name string
}

func (inst *operatorInstance) ensureXDSServer(params *json.RawMessage) (xdsServerRef, error) {
	var p struct {
		Server  string `json:"server"`
		Address string `json:"address"`
	}
	if err := rawParams(params, &p); err != nil {
		return xdsServerRef{}, fmt.Errorf("parameters: %w", err)
	}
	name := inst.name
	if p.Server != "" {
		name = inst.name + "/" + p.Server
	}
	if srv, err := inst.vm.ensureXDSServer(name); err == nil {
		return xdsServerRef{srv: srv, name: name}, nil
	}
	if p.Address == "" {
		return xdsServerRef{}, fmt.Errorf("xds server %q is not running and the target names no parameters.address to start it", name)
	}
	srv, err := inst.vm.startXDSServer(xdsServerOptions{Name: name, Address: p.Address})
	if err != nil {
		return xdsServerRef{}, err
	}
	return xdsServerRef{srv: srv, name: name}, nil
}

func gvkString(gvk schema.GroupVersionKind) string {
	if gvk.Group == "" {
		return gvk.Version + "/" + gvk.Kind
	}
	return gvk.Group + "/" + gvk.Version + "/" + gvk.Kind
}

// close tears the instance down: bindings and circuits in reverse start
// order, then the topics, then the view registrations.
func (inst *operatorInstance) close() {
	if inst.closed {
		return
	}
	inst.closed = true
	for i := len(inst.runnables) - 1; i >= 0; i-- {
		inst.vm.runtime.Stop(inst.runnables[i])
	}
	for i := len(inst.procs) - 1; i >= 0; i-- {
		inst.vm.runtime.Stop(inst.procs[i])
	}
	for _, t := range inst.topics {
		inst.vm.runtime.ResetTopic(t)
	}
	if len(inst.viewGVKs) > 0 {
		if err := inst.vm.unregisterViewGVKList(inst.viewGVKs); err != nil {
			inst.vm.logger.V(1).Info("unregister views", "operator", inst.name, "error", err.Error())
		}
	}
}

func (inst *operatorInstance) jsObject() *goja.Object {
	v := inst.vm
	obj := v.rt.NewObject()
	_ = obj.Set("close", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		inst.close()
		return goja.Undefined(), nil
	}))
	_ = obj.Set("name", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.rt.ToValue(inst.name), nil
	}))
	_ = obj.Set("components", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		out := make([]string, 0, len(inst.comps))
		for c := range inst.comps {
			out = append(out, c)
		}
		return v.rt.ToValue(out), nil
	}))
	printer := v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		var parsed any
		if err := json.Unmarshal(inst.rawSpec, &parsed); err != nil {
			return nil, err
		}
		return v.rt.ToValue(map[string]any{"name": inst.name, "spec": parsed}), nil
	})
	_ = obj.Set("spec", printer)
	_ = obj.Set("toJSON", printer)
	return obj
}
