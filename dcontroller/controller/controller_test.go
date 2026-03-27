package controller_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap/zapcore"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/compiler"
	"github.com/l7mp/dbsp/engine/datamodel"
	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"

	k8sruntime "github.com/l7mp/dbsp/connectors/kubernetes/runtime"

	opv1a1 "github.com/l7mp/dbsp/dcontroller/api/operator/v1alpha1"
	"github.com/l7mp/dbsp/dcontroller/controller"
)

// testPipelineJSON is a minimal project pipeline: projects spec.x into metadata.name.
const testPipelineJSON = `{"@project":{"$.metadata.name":"$.spec.x"}}`
const timeout = 100 * time.Millisecond
const logLevel = -10

var logger = zap.New(zap.UseFlagOptions(&zap.Options{
	Development: true,
	DestWriter:  os.Stderr,
	Level:       zapcore.Level(logLevel),
	TimeEncoder: zapcore.RFC3339NanoTimeEncoder,
}))

func rawPipeline(s string) *apiextensionsv1.JSON {
	return &apiextensionsv1.JSON{Raw: []byte(s)}
}

func doc(obj map[string]any) *dbspunstructured.Unstructured {
	return dbspunstructured.New(obj, nil)
}

func zsetFromDocs(objs ...map[string]any) zset.ZSet {
	zs := zset.New()
	for _, obj := range objs {
		zs.Insert(doc(obj), 1)
	}
	return zs
}

func collectFieldStrings(zs zset.ZSet, field string) []string {
	out := []string{}
	zs.Iter(func(d datamodel.Document, _ zset.Weight) bool {
		v, err := d.GetField(field)
		if err == nil {
			out = append(out, fmt.Sprint(v))
		}
		return true
	})
	return out
}

// defaultSpec returns a minimal spec with a Periodic source (no K8s required) and one
// Updater target. The period is intentionally long so the producer never fires in tests.
func defaultSpec() opv1a1.Controller {
	return specWithPipeline(testPipelineJSON)
}

func specWithPipeline(pipeline string) opv1a1.Controller {
	return opv1a1.Controller{
		Name: "test-op",
		Sources: []opv1a1.Source{{
			Resource: opv1a1.Resource{Kind: "Foo"},
			Type:     opv1a1.Periodic,
			Parameters: &apiextensionsv1.JSON{
				Raw: []byte(`{"period":"1h"}`),
			},
		}},
		Pipeline: rawPipeline(pipeline),
		Targets: []opv1a1.Target{{
			Resource: opv1a1.Resource{Kind: "Bar"},
			Type:     opv1a1.Updater,
		}},
	}
}

func joinSpecWithPipeline(pipeline string) opv1a1.Controller {
	return opv1a1.Controller{
		Name: "test-join-op",
		Sources: []opv1a1.Source{
			{
				Resource: opv1a1.Resource{Kind: "Foo"},
				Type:     opv1a1.Periodic,
				Parameters: &apiextensionsv1.JSON{
					Raw: []byte(`{"period":"1h"}`),
				},
			},
			{
				Resource: opv1a1.Resource{Kind: "Dep"},
				Type:     opv1a1.Periodic,
				Parameters: &apiextensionsv1.JSON{
					Raw: []byte(`{"period":"1h"}`),
				},
			},
		},
		Pipeline: rawPipeline(pipeline),
		Targets: []opv1a1.Target{{
			Resource: opv1a1.Resource{Kind: "Bar"},
		}},
	}
}

var _ = Describe("Controller", func() {
	Describe("compiles a simple projection pipeline", func() {
		It("should create the controller without error", func() {
			rt := dbspruntime.NewRuntime(logger)
			krt, err := k8sruntime.New(k8sruntime.Config{})
			Expect(err).NotTo(HaveOccurred())
			ctrl, err := controller.New(controller.Config{
				OperatorName: "test",
				Spec:         defaultSpec(),
				Runtime:      rt,
				K8sRuntime:   krt,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(ctrl).NotTo(BeNil())
			Expect(ctrl.GetCircuit()).NotTo(BeNil())
		})

		It("should compile when source and target kinds are identical", func() {
			rt := dbspruntime.NewRuntime(logger)
			krt, err := k8sruntime.New(k8sruntime.Config{})
			Expect(err).NotTo(HaveOccurred())

			spec := opv1a1.Controller{
				Name: "same-kind-op",
				Sources: []opv1a1.Source{{
					Resource: opv1a1.Resource{Kind: "Deployment"},
					Type:     opv1a1.Periodic,
					Parameters: &apiextensionsv1.JSON{
						Raw: []byte(`{"period":"1h"}`),
					},
				}},
				Pipeline: rawPipeline(`[{"@project":{"$.":"$."}}]`),
				Targets: []opv1a1.Target{{
					Resource: opv1a1.Resource{Kind: "Deployment"},
					Type:     opv1a1.Updater,
				}},
			}

			ctrl, err := controller.New(controller.Config{
				OperatorName: "test",
				Spec:         spec,
				Runtime:      rt,
				K8sRuntime:   krt,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(ctrl).NotTo(BeNil())
		})
	})

	Describe("executes a project pipeline end-to-end", func() {
		type pipelineCase struct {
			name     string
			pipeline string
			input    zset.ZSet
			assert   func(dbspruntime.Event)
		}

		cases := []pipelineCase{
			{
				name:     "project passthrough",
				pipeline: `[{"@project":{"$.":"$."}}]`,
				input: zsetFromDocs(map[string]any{
					"metadata": map[string]any{"name": "foo-obj", "namespace": "default"},
					"spec":     map[string]any{"x": int64(1)},
				}),
				assert: func(evt dbspruntime.Event) {
					Expect(evt.Data.Size()).To(Equal(1))
					xs := collectFieldStrings(evt.Data, "spec.x")
					Expect(xs).To(ConsistOf("1"))
				},
			},
			{
				name:     "select keeps matching row",
				pipeline: `[{"@select":{"@eq":["$.spec.x",1]}}]`,
				input: zsetFromDocs(map[string]any{
					"metadata": map[string]any{"name": "foo-obj", "namespace": "default"},
					"spec":     map[string]any{"x": int64(1)},
				}),
				assert: func(evt dbspruntime.Event) {
					Expect(evt.Data.Size()).To(Equal(1))
				},
			},
			{
				name:     "select filters non-matching row",
				pipeline: `[{"@select":{"@eq":["$.spec.x",1]}}]`,
				input: zsetFromDocs(map[string]any{
					"metadata": map[string]any{"name": "foo-obj", "namespace": "default"},
					"spec":     map[string]any{"x": int64(2)},
				}),
				assert: func(evt dbspruntime.Event) {
					Expect(evt.Data.Size()).To(Equal(0))
				},
			},
			{
				name:     "select on missing field yields empty output",
				pipeline: `[{"@select":{"@eq":["$.spec.missing",1]}}]`,
				input: zsetFromDocs(map[string]any{
					"metadata": map[string]any{"name": "foo-obj", "namespace": "default"},
					"spec":     map[string]any{"x": int64(2)},
				}),
				assert: func(evt dbspruntime.Event) {
					Expect(evt.Data.Size()).To(Equal(0))
				},
			},
			{
				name:     "project constants",
				pipeline: `[{"@project":{"metadata":{"name":"fixed","namespace":"default"},"spec":{"y":"ok"}}}]`,
				input: zsetFromDocs(map[string]any{
					"metadata": map[string]any{"name": "ignored", "namespace": "ignored"},
					"spec":     map[string]any{"x": int64(9)},
				}),
				assert: func(evt dbspruntime.Event) {
					Expect(evt.Data.Size()).To(Equal(1))
					names := collectFieldStrings(evt.Data, "metadata.name")
					ys := collectFieldStrings(evt.Data, "spec.y")
					Expect(names).To(ConsistOf("fixed"))
					Expect(ys).To(ConsistOf("ok"))
				},
			},
			{
				name:     "project field copy",
				pipeline: `[{"@project":{"metadata":{"name":"$.metadata.name","namespace":"$.metadata.namespace"},"spec":{"x":"$.spec.x"}}}]`,
				input: zsetFromDocs(map[string]any{
					"metadata": map[string]any{"name": "foo-obj", "namespace": "default"},
					"spec":     map[string]any{"x": int64(7)},
				}),
				assert: func(evt dbspruntime.Event) {
					Expect(evt.Data.Size()).To(Equal(1))
					names := collectFieldStrings(evt.Data, "metadata.name")
					xs := collectFieldStrings(evt.Data, "spec.x")
					Expect(names).To(ConsistOf("foo-obj"))
					Expect(xs).To(ConsistOf("7"))
				},
			},
			{
				name:     "project copy and augment",
				pipeline: `[{"@project":[{"$.":"$."},{"$.spec.done":true}]}]`,
				input: zsetFromDocs(map[string]any{
					"metadata": map[string]any{"name": "foo-obj", "namespace": "default"},
					"spec":     map[string]any{"x": int64(5)},
				}),
				assert: func(evt dbspruntime.Event) {
					Expect(evt.Data.Size()).To(Equal(1))
					dones := collectFieldStrings(evt.Data, "spec.done")
					xs := collectFieldStrings(evt.Data, "spec.x")
					Expect(dones).To(ConsistOf("true"))
					Expect(xs).To(ConsistOf("5"))
				},
			},
			{
				name:     "unwind list emits one row per element",
				pipeline: `[{"@unwind":"$.spec.list"}]`,
				input: zsetFromDocs(map[string]any{
					"metadata": map[string]any{"name": "foo-obj", "namespace": "default"},
					"spec":     map[string]any{"list": []any{int64(1), int64(2), int64(3)}},
				}),
				assert: func(evt dbspruntime.Event) {
					Expect(evt.Data.Size()).To(Equal(3))
					vals := collectFieldStrings(evt.Data, "spec.list")
					Expect(vals).To(ConsistOf("1", "2", "3"))
				},
			},
			{
				name:     "unwind empty list emits no rows",
				pipeline: `[{"@unwind":"$.spec.list"}]`,
				input: zsetFromDocs(map[string]any{
					"metadata": map[string]any{"name": "foo-obj", "namespace": "default"},
					"spec":     map[string]any{"list": []any{}},
				}),
				assert: func(evt dbspruntime.Event) {
					Expect(evt.Data.Size()).To(Equal(0))
				},
			},
			{
				name:     "nested unwind flattens nested arrays",
				pipeline: `[{"@unwind":"$.spec.list"},{"@unwind":"$.spec.list"}]`,
				input: zsetFromDocs(map[string]any{
					"metadata": map[string]any{"name": "foo-obj", "namespace": "default"},
					"spec":     map[string]any{"list": []any{[]any{int64(1), int64(2)}, []any{int64(3)}}},
				}),
				assert: func(evt dbspruntime.Event) {
					Expect(evt.Data.Size()).To(Equal(3))
					vals := collectFieldStrings(evt.Data, "spec.list")
					Expect(vals).To(ConsistOf("1", "2", "3"))
				},
			},
			{
				name: "groupBy emits rows with grouped list field",
				pipeline: `[
					{"@groupBy":["$.class","$.grade"]},
					{"@project":{"key":"$.key","grades":"$.values"}}
				]`,
				input: zsetFromDocs(
					map[string]any{"class": "Algebra", "grade": int64(90)},
					map[string]any{"class": "Algebra", "grade": int64(80)},
				),
				assert: func(evt dbspruntime.Event) {
					Expect(evt.Data.Size()).To(Equal(1))
					grades := collectFieldStrings(evt.Data, "grades")
					Expect(grades).To(HaveLen(1))
					Expect(grades[0]).To(ContainSubstring("80"))
					Expect(grades[0]).To(ContainSubstring("90"))
				},
			},
			{
				name: "groupBy keeps independent rows",
				pipeline: `[
					{"@groupBy":["$.class","$.grade"]},
					{"@project":{"key":"$.key","grades":"$.values"}}
				]`,
				input: zsetFromDocs(
					map[string]any{"class": "Algebra", "grade": int64(90)},
					map[string]any{"class": "Geometry", "grade": int64(70)},
				),
				assert: func(evt dbspruntime.Event) {
					Expect(evt.Data.Size()).To(Equal(2))
					grades := collectFieldStrings(evt.Data, "grades")
					Expect(grades).To(HaveLen(2))
					Expect(grades[0]).To(SatisfyAny(ContainSubstring("70"), ContainSubstring("90")))
					Expect(grades[1]).To(SatisfyAny(ContainSubstring("70"), ContainSubstring("90")))
				},
			},
		}

		for _, tc := range cases {
			tc := tc
			It("should process injected event: "+tc.name, func() {
				inTopic := circuit.InputTopic("test-op", "Foo")
				outTopic := circuit.OutputTopic("test-op", "Bar")

				rt := dbspruntime.NewRuntime(logger)
				krt, err := k8sruntime.New(k8sruntime.Config{})
				Expect(err).NotTo(HaveOccurred())

				sub := rt.NewSubscriber()
				sub.Subscribe(outTopic)

				ctrl, err := controller.New(controller.Config{
					OperatorName: "test",
					Spec:         specWithPipeline(tc.pipeline),
					Runtime:      rt,
					K8sRuntime:   krt,
					Logger:       logger,
				})
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				go func() { _ = ctrl.GetCircuit().Start(ctx) }()

				pub := rt.NewPublisher()
				var out dbspruntime.Event
				Eventually(func() bool {
					_ = pub.Publish(dbspruntime.Event{Name: inTopic, Data: tc.input})
					select {
					case evt, ok := <-sub.GetChannel():
						if !ok || evt.Name != outTopic {
							return false
						}
						out = evt
						return true
					default:
						return false
					}
				}, timeout, 10*time.Millisecond).Should(BeTrue())

				tc.assert(out)
			})
		}

		It("should process a simple two-source join and project expected content", func() {
			fooTopic := circuit.InputTopic("test-join-op", "Foo")
			depTopic := circuit.InputTopic("test-join-op", "Dep")
			outTopic := circuit.OutputTopic("test-join-op", "Bar")

			rt := dbspruntime.NewRuntime(logger)
			krt, err := k8sruntime.New(k8sruntime.Config{})
			Expect(err).NotTo(HaveOccurred())

			sub := rt.NewSubscriber()
			sub.Subscribe(outTopic)

			pipeline := `[
				{"@join":{"@eq":["$.Dep.metadata.name","$.Foo.spec.parent"]}},
				{"@project":{"metadata":{"name":"joined","namespace":"default"},"fooName":"$.Foo.metadata.name","depName":"$.Dep.metadata.name","depVersion":"$.Dep.spec.version"}}
			]`

			ctrl, err := controller.New(controller.Config{
				OperatorName: "test",
				Spec:         joinSpecWithPipeline(pipeline),
				Runtime:      rt,
				K8sRuntime:   krt,
			})
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() { _ = ctrl.GetCircuit().Start(ctx) }()

			foo := zsetFromDocs(map[string]any{
				"metadata": map[string]any{"name": "foo-1", "namespace": "default"},
				"spec":     map[string]any{"parent": "dep-a"},
			})
			dep := zsetFromDocs(map[string]any{
				"metadata": map[string]any{"name": "dep-a", "namespace": "default"},
				"spec":     map[string]any{"version": "v1"},
			})

			pub := rt.NewPublisher()
			var out dbspruntime.Event
			Eventually(func() bool {
				_ = pub.Publish(dbspruntime.Event{Name: fooTopic, Data: foo})
				_ = pub.Publish(dbspruntime.Event{Name: depTopic, Data: dep})

				for {
					select {
					case evt, ok := <-sub.GetChannel():
						if !ok || evt.Name != outTopic {
							continue
						}
						if evt.Data.Size() == 0 {
							continue
						}
						out = evt
						return true
					default:
						return false
					}
				}
			}, timeout, 10*time.Millisecond).Should(BeTrue())

			Expect(out.Data.Size()).To(Equal(1))
			fooNames := collectFieldStrings(out.Data, "fooName")
			depNames := collectFieldStrings(out.Data, "depName")
			depVersions := collectFieldStrings(out.Data, "depVersion")
			Expect(fooNames).To(ConsistOf("foo-1"))
			Expect(depNames).To(ConsistOf("dep-a"))
			Expect(depVersions).To(ConsistOf("v1"))
		})

		It("should process a pipeline with multiple branches", func() {
			inTopic := circuit.InputTopic("test-op", "Foo")
			outTopic := circuit.OutputTopic("test-op", "Bar")

			rt := dbspruntime.NewRuntime(logger)
			krt, err := k8sruntime.New(k8sruntime.Config{})
			Expect(err).NotTo(HaveOccurred())

			sub := rt.NewSubscriber()
			sub.Subscribe(outTopic)

			pipeline := `[
				[
					{"@inputs":["Foo"]},
					{"@project":{"$.":"$."}},
					{"@output":"Mid"}
				],
				[
					{"@inputs":["Mid"]},
					{"@project":[{"$.":"$."},{"$.spec.y":"$.spec.x"}]},
					{"@output":"Bar"}
				]
			]`

			ctrl, err := controller.New(controller.Config{
				OperatorName: "test",
				Spec:         specWithPipeline(pipeline),
				Runtime:      rt,
				K8sRuntime:   krt,
				Logger:       logger,
			})
			Expect(err).NotTo(HaveOccurred())

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() { _ = ctrl.GetCircuit().Start(ctx) }()

			in := zsetFromDocs(map[string]any{
				"metadata": map[string]any{"name": "foo-obj", "namespace": "default"},
				"spec":     map[string]any{"x": int64(7)},
			})

			pub := rt.NewPublisher()
			var out dbspruntime.Event
			Eventually(func() bool {
				_ = pub.Publish(dbspruntime.Event{Name: inTopic, Data: in})
				select {
				case evt, ok := <-sub.GetChannel():
					if !ok || evt.Name != outTopic {
						return false
					}
					if evt.Data.Size() == 0 {
						return false
					}
					out = evt
					return true
				default:
					return false
				}
			}, timeout, 10*time.Millisecond).Should(BeTrue())

			Expect(out.Data.Size()).To(Equal(1))
			xs := collectFieldStrings(out.Data, "spec.x")
			ys := collectFieldStrings(out.Data, "spec.y")
			Expect(xs).To(ConsistOf("7"))
			Expect(ys).To(ConsistOf("7"))
		})
	})

	Describe("applies a QueryTransformer", func() {
		It("should call the transformer and return a valid circuit", func() {
			called := false
			transformer := func(q *compiler.Query) (*compiler.Query, error) {
				called = true
				return q, nil
			}

			rt := dbspruntime.NewRuntime(logger)
			krt, err := k8sruntime.New(k8sruntime.Config{})
			Expect(err).NotTo(HaveOccurred())
			ctrl, err := controller.New(controller.Config{
				OperatorName: "test",
				Spec:         defaultSpec(),
				Runtime:      rt,
				K8sRuntime:   krt,
				Transformer:  transformer,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(called).To(BeTrue())
			Expect(ctrl.GetCircuit()).NotTo(BeNil())
		})
	})

	Describe("wires a watcher source", func() {
		It("should create the controller with a Kubernetes runtime", func() {
			krt, err := k8sruntime.New(k8sruntime.Config{})
			Expect(err).NotTo(HaveOccurred())

			watcherSpec := opv1a1.Controller{
				Name: "test-watcher-op",
				Sources: []opv1a1.Source{{
					Resource: opv1a1.Resource{Kind: "Foo"},
					// Type defaults to Watcher.
				}},
				Pipeline: rawPipeline(testPipelineJSON),
				Targets:  []opv1a1.Target{},
			}

			rt := dbspruntime.NewRuntime(logger)
			ctrl, err := controller.New(controller.Config{
				OperatorName: "test",
				Spec:         watcherSpec,
				Runtime:      rt,
				K8sRuntime:   krt,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(ctrl).NotTo(BeNil())
		})
	})

	Describe("wires a lister source", func() {
		It("should create the controller with a Kubernetes runtime", func() {
			krt, err := k8sruntime.New(k8sruntime.Config{})
			Expect(err).NotTo(HaveOccurred())

			listerSpec := opv1a1.Controller{
				Name: "test-lister-op",
				Sources: []opv1a1.Source{{
					Resource: opv1a1.Resource{Kind: "Foo"},
					Type:     opv1a1.Lister,
				}},
				Pipeline: rawPipeline(testPipelineJSON),
				Targets:  []opv1a1.Target{},
			}

			rt := dbspruntime.NewRuntime(logger)
			ctrl, err := controller.New(controller.Config{
				OperatorName: "test",
				Spec:         listerSpec,
				Runtime:      rt,
				K8sRuntime:   krt,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(ctrl).NotTo(BeNil())
		})
	})

	Describe("wires a periodic source", func() {
		It("should create the controller with a periodic producer", func() {
			spec := opv1a1.Controller{
				Name: "test-periodic-op",
				Sources: []opv1a1.Source{{
					Resource: opv1a1.Resource{Kind: "Foo"},
					Type:     opv1a1.Periodic,
					Parameters: &apiextensionsv1.JSON{
						Raw: []byte(`{"period":"100ms"}`),
					},
				}},
				Pipeline: rawPipeline(testPipelineJSON),
				Targets:  []opv1a1.Target{},
			}

			rt := dbspruntime.NewRuntime(logger)
			ctrl, err := controller.New(controller.Config{
				OperatorName: "test",
				Spec:         spec,
				Runtime:      rt,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(ctrl).NotTo(BeNil())
		})
	})
})
