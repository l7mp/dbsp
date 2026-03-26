package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/go-logr/logr"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"
	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	dobject "github.com/l7mp/dbsp/connectors/kubernetes/runtime/object"
	doperator "github.com/l7mp/dbsp/dcontroller/operator"
	dbunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/executor"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

const (
	OperatorName                    = "test-ep-operator"
	OperatorSpec                    = "examples/endpointslice-controller/endpointslice-controller-spec.yaml"
	OperatorGroupedSpec             = "examples/endpointslice-controller/endpointslice-controller-gather-spec.yaml"
	EndpointSliceCtrlAnnotationName = "dcontroller.io/endpointslice-controller-enabled"
	endpointViewTopic               = "endpointslice-controller/endpointview/output"
)

var (
	scheme                 = runtime.NewScheme()
	disableEndpointPooling *bool
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	disableEndpointPooling = flag.Bool("disable-endpoint-pooling", false,
		"Generate per-endpoint objects instead of a single object listing all service endpoints.")

	zapOpts := zap.Options{
		Development:     true,
		DestWriter:      os.Stderr,
		StacktraceLevel: zapcore.Level(3),
		TimeEncoder:     zapcore.RFC3339NanoTimeEncoder,
	}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	logger := zap.New(zap.UseFlagOptions(&zapOpts))
	log := logger.WithName("epslice-op")
	ctrl.SetLogger(log)

	// Define the controller pipeline
	specFile := OperatorGroupedSpec
	if *disableEndpointPooling {
		specFile = OperatorSpec
	}

	runner, err := StartEndpointSliceOperator(ctrl.GetConfigOrDie(), specFile, true, nil, logger)
	if err != nil {
		log.Error(err, "unable to set up endpoint slice operator")
		os.Exit(1)
	}
	ctx := ctrl.SetupSignalHandler()
	if err := runner.Start(ctx); err != nil {
		log.Error(err, "problem running operator")
		os.Exit(1)
	}
}

// EndpointViewEvent is emitted by epConsumer for each EndpointView delta.
type EndpointViewEvent struct {
	Namespace string
	Name      string
	EventType dobject.DeltaType
	GVK       schema.GroupVersionKind
	Object    dobject.Object
}

func (e EndpointViewEvent) GetNamespace() string            { return e.Namespace }
func (e EndpointViewEvent) GetName() string                 { return e.Name }
func (e EndpointViewEvent) GetEventType() dobject.DeltaType { return e.EventType }
func (e EndpointViewEvent) GetGVK() schema.GroupVersionKind { return e.GVK }
func (e EndpointViewEvent) GetObject() dobject.Object       { return e.Object }

type EndpointSliceRunner struct {
	k8sRuntime *k8sruntime.Runtime
	op         *doperator.Operator
	consumer   *epConsumer
}

func StartEndpointSliceOperator(cfg *rest.Config, specFile string, withAPIServer bool, sink chan<- EndpointViewEvent, log logr.Logger) (*EndpointSliceRunner, error) {
	k8sCfg := k8sruntime.Config{RESTConfig: cfg, Logger: log}
	if withAPIServer {
		apiServerCfg, err := k8sruntime.NewDefaultAPIServerConfig("", 0, true, false, log)
		if err != nil {
			return nil, fmt.Errorf("configure API server: %w", err)
		}
		apiServerCfg.EnableOpenAPI = false
		k8sCfg.APIServer = &apiServerCfg
	}

	k8sRuntime, err := k8sruntime.New(k8sCfg)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes runtime: %w", err)
	}

	op, err := doperator.NewFromFile(OperatorName, specFile, k8sRuntime, log)
	if err != nil {
		return nil, fmt.Errorf("load operator from file: %w", err)
	}

	consumer, err := NewEPConsumer(op.GetRuntime(), endpointViewTopic,
		viewv1a1.GroupVersionKind(OperatorName, "EndpointView"), sink, log)
	if err != nil {
		return nil, fmt.Errorf("create endpoint view consumer: %w", err)
	}

	if err := op.GetRuntime().Add(consumer); err != nil {
		return nil, fmt.Errorf("register endpoint view consumer: %w", err)
	}

	return &EndpointSliceRunner{k8sRuntime: k8sRuntime, op: op, consumer: consumer}, nil
}

func (r *EndpointSliceRunner) Start(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return r.k8sRuntime.Start(gctx) })
	g.Go(func() error { return r.op.Start(gctx) })
	return g.Wait()
}

func (r *EndpointSliceRunner) APIServer() *k8sruntime.Runtime {
	return r.k8sRuntime
}

// SetControllerObserver attaches an execution observer to a named controller circuit.
// Returns true if a matching controller exists.
func (r *EndpointSliceRunner) SetControllerObserver(controllerName string, observer executor.ObserverFunc) bool {
	if r == nil || r.op == nil {
		return false
	}

	return r.op.SetControllerObserver(controllerName, observer)
}

type epConsumer struct {
	*dbspruntime.BaseConsumer

	topic  string
	gvk    schema.GroupVersionKind
	log    logr.Logger
	sink   chan<- EndpointViewEvent
	known  map[client.ObjectKey]struct{}
	knownM sync.Mutex
}

type classifiedEndpointDelta struct {
	eventType dobject.DeltaType
	key       client.ObjectKey
	obj       dobject.Object
}

type endpointCandidate struct {
	obj dobject.Object
}

type endpointGroupedDelta struct {
	key    client.ObjectKey
	pos    *endpointCandidate
	neg    *endpointCandidate
	hasPos bool
	hasNeg bool
}

var _ dbspruntime.Consumer = (*epConsumer)(nil)

func NewEPConsumer(rt *dbspruntime.Runtime, topic string, gvk schema.GroupVersionKind, sink chan<- EndpointViewEvent, log logr.Logger) (*epConsumer, error) {
	if rt == nil {
		return nil, fmt.Errorf("runtime is required")
	}
	if topic == "" {
		topic = endpointViewTopic
	}

	if log.GetSink() == nil {
		log = logr.Discard()
	}
	log = log.WithName("endpointslice-ctrl")

	base, err := dbspruntime.NewBaseConsumer(dbspruntime.BaseConsumerConfig{
		Name:          "endpoint-view-consumer",
		Subscriber:    rt.NewSubscriber(),
		ErrorReporter: rt,
		Logger:        log,
		Topics:        []string{topic},
	})
	if err != nil {
		return nil, err
	}

	return &epConsumer{
		BaseConsumer: base,
		topic:        topic,
		gvk:          gvk,
		log:          log,
		sink:         sink,
		known:        map[client.ObjectKey]struct{}{},
	}, nil
}

func (c *epConsumer) Start(ctx context.Context) error {
	return c.Run(ctx, c)
}

func (c *epConsumer) Consume(ctx context.Context, event dbspruntime.Event) error {
	if event.Name != c.topic {
		return nil
	}

	deltas, err := c.classifyDeltas(event.Data)
	if err != nil {
		return err
	}

	for _, d := range deltas {
		eventType := d.eventType
		obj := d.obj

		ev := EndpointViewEvent{
			Namespace: obj.GetNamespace(),
			Name:      obj.GetName(),
			EventType: eventType,
			GVK:       obj.GetObjectKind().GroupVersionKind(),
			Object:    obj,
		}

		switch eventType {
		case dobject.Added, dobject.Updated, dobject.Upserted:
			spec, _, _ := unstructured.NestedMap(obj.Object, "spec")
			c.log.Info("Add/update EndpointView object", "name", ev.Name, "namespace", ev.Namespace, "spec", fmt.Sprintf("%#v", spec))
		case dobject.Deleted:
			c.log.Info("Delete EndpointView object", "name", ev.Name, "namespace", ev.Namespace)
		default:
			c.log.Info("Unhandled event", "name", ev.Name, "namespace", ev.Namespace, "type", ev.EventType)
		}

		if c.sink != nil {
			select {
			case c.sink <- ev:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return nil
}

func (c *epConsumer) classifyDeltas(data zset.ZSet) ([]classifiedEndpointDelta, error) {
	groups := map[string]*endpointGroupedDelta{}

	for _, entry := range data.Entries() {
		obj, err := endpointObjectFromDocument(entry.Document, c.gvk)
		if err != nil {
			return nil, fmt.Errorf("decode endpoint view event: %w", err)
		}

		weight := entry.Weight
		if weight == 0 {
			continue
		}

		key := client.ObjectKeyFromObject(obj)
		g, ok := groups[key.String()]
		if !ok {
			g = &endpointGroupedDelta{key: key}
			groups[key.String()] = g
		}

		if weight < 0 {
			g.neg = &endpointCandidate{obj: obj}
			g.hasNeg = true
			continue
		}

		g.pos = &endpointCandidate{obj: obj}
		g.hasPos = true
	}

	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]classifiedEndpointDelta, 0, len(keys))

	c.knownM.Lock()
	defer c.knownM.Unlock()

	for _, key := range keys {
		g := groups[key]
		hasPos := g.hasPos && g.pos != nil && g.pos.obj != nil
		hasNeg := g.hasNeg && g.neg != nil && g.neg.obj != nil
		if !hasPos && !hasNeg {
			continue
		}

		if hasPos && hasNeg {
			c.known[g.key] = struct{}{}
			out = append(out, classifiedEndpointDelta{eventType: dobject.Updated, key: g.key, obj: g.pos.obj})
			continue
		}

		if hasPos {
			eventType := dobject.Added
			if _, exists := c.known[g.key]; exists {
				eventType = dobject.Updated
			}
			c.known[g.key] = struct{}{}
			out = append(out, classifiedEndpointDelta{eventType: eventType, key: g.key, obj: g.pos.obj})
			continue
		}

		delete(c.known, g.key)
		out = append(out, classifiedEndpointDelta{eventType: dobject.Deleted, key: g.key, obj: g.neg.obj})
	}

	return out, nil
}

func endpointObjectFromDocument(doc any, fallbackGVK schema.GroupVersionKind) (dobject.Object, error) {
	udoc, ok := doc.(*dbunstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("unsupported document type %T", doc)
	}

	obj := dobject.New()
	obj.SetUnstructuredContent(udoc.Fields())
	if obj.GetObjectKind().GroupVersionKind().Kind == "" {
		obj.SetGroupVersionKind(fallbackGVK)
	}

	if obj.GetName() == "" {
		return nil, fmt.Errorf("event object is missing metadata.name")
	}

	return obj, nil
}
