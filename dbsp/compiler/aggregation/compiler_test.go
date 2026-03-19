package aggregation

import (
	"sort"

	"github.com/l7mp/dbsp/dbsp/datamodel"
	"github.com/l7mp/dbsp/dbsp/datamodel/unstructured"
	"github.com/l7mp/dbsp/dbsp/executor"
	"github.com/l7mp/dbsp/dbsp/operator"
	"github.com/l7mp/dbsp/dbsp/zset"
	"github.com/l7mp/dbsp/internal/logger"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Aggregation compiler parity", func() {
	makeExec := func(srcs []string, pipeline string) (*executor.Executor, string) {
		GinkgoHelper()
		c := New(srcs, []string{"output"})
		q, err := c.CompileString(pipeline)
		Expect(err).NotTo(HaveOccurred())
		exec, err := executor.New(q.Circuit, logger.DiscardLogger())
		Expect(err).NotTo(HaveOccurred())
		return exec, q.OutputMap["output"]
	}

	collectDocs := func(z zset.ZSet) []*unstructured.Unstructured {
		GinkgoHelper()
		docs := make([]*unstructured.Unstructured, 0, z.Size())
		z.Iter(func(doc datamodel.Document, _ zset.Weight) bool {
			docs = append(docs, doc.(*unstructured.Unstructured))
			return true
		})
		return docs
	}

	It("compiles single op and list forms", func() {
		c := New([]string{"Pod"}, []string{"output"})
		q1, err := c.CompileString(`{"@project":{"$.metadata":"$.metadata"}}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(q1.Circuit).NotTo(BeNil())

		q2, err := c.CompileString(`[{"@project":{"$.metadata":"$.metadata"}}]`)
		Expect(err).NotTo(HaveOccurred())
		Expect(q2.Circuit).NotTo(BeNil())
	})

	It("evaluates select true/false and missing fields", func() {
		exec, outID := makeExec([]string{"Pod"}, `[{"@select":{"@eq":["$.spec.b.c",2]}}]`)
		in := zset.New()
		in.Insert(unstructured.New(map[string]any{"spec": map[string]any{"b": map[string]any{"c": int64(2)}}}, nil), 1)
		in.Insert(unstructured.New(map[string]any{"spec": map[string]any{"b": map[string]any{"c": int64(3)}}}, nil), 1)
		in.Insert(unstructured.New(map[string]any{"spec": map[string]any{}}, nil), 1)

		outs, err := exec.Execute(map[string]zset.ZSet{"input_Pod": in})
		Expect(err).NotTo(HaveOccurred())
		Expect(outs[outID].Size()).To(Equal(1))
	})

	It("evaluates project with list merge and mixed setters", func() {
		exec, outID := makeExec([]string{"Pod"}, `[{"@project":[{"metadata":{"name":"name2"}},{"$.metadata.namespace":"default2"},{"$.spec.a":"$.spec.b"}]}]`)
		in := zset.New()
		in.Insert(unstructured.New(map[string]any{
			"metadata": map[string]any{"name": "orig", "namespace": "orig-ns"},
			"spec":     map[string]any{"b": map[string]any{"c": int64(2)}},
		}, nil), 1)

		outs, err := exec.Execute(map[string]zset.ZSet{"input_Pod": in})
		Expect(err).NotTo(HaveOccurred())
		docs := collectDocs(outs[outID])
		Expect(docs).To(HaveLen(1))
		name, err := docs[0].GetField("metadata.name")
		Expect(err).NotTo(HaveOccurred())
		ns, err := docs[0].GetField("metadata.namespace")
		Expect(err).NotTo(HaveOccurred())
		a, err := docs[0].GetField("spec.a.c")
		Expect(err).NotTo(HaveOccurred())
		Expect(name).To(Equal("name2"))
		Expect(ns).To(Equal("default2"))
		Expect(a).To(Equal(int64(2)))
	})

	It("evaluates project copy+override using $. key", func() {
		exec, outID := makeExec([]string{"Pod"}, `[{"@project":[{"$.":"$."},{"$.metadata.name":"fixed"}]}]`)
		in := zset.New()
		in.Insert(unstructured.New(map[string]any{
			"metadata": map[string]any{"name": "pod1", "namespace": "default"},
			"spec":     map[string]any{"x": int64(1)},
		}, nil), 1)

		outs, err := exec.Execute(map[string]zset.ZSet{"input_Pod": in})
		Expect(err).NotTo(HaveOccurred())
		docs := collectDocs(outs[outID])
		Expect(docs).To(HaveLen(1))
		name, _ := docs[0].GetField("metadata.name")
		ns, _ := docs[0].GetField("metadata.namespace")
		x, _ := docs[0].GetField("spec.x")
		Expect(name).To(Equal("fixed"))
		Expect(ns).To(Equal("default"))
		Expect(x).To(Equal(float64(1)))
	})

	It("evaluates unwind and appends metadata.name indexes", func() {
		exec, outID := makeExec([]string{"Pod"}, `[{"@unwind":"$.spec.list"}]`)
		in := zset.New()
		in.Insert(unstructured.New(map[string]any{
			"metadata": map[string]any{"name": "name", "namespace": "default"},
			"spec":     map[string]any{"list": []any{int64(1), "a", true}},
		}, nil), 1)

		outs, err := exec.Execute(map[string]zset.ZSet{"input_Pod": in})
		Expect(err).NotTo(HaveOccurred())
		docs := collectDocs(outs[outID])
		Expect(docs).To(HaveLen(3))

		names := []string{}
		vals := []any{}
		for _, d := range docs {
			name, _ := d.GetField("metadata.name")
			v, _ := d.GetField("spec.list")
			names = append(names, name.(string))
			vals = append(vals, v)
		}
		Expect(names).To(ConsistOf("name-0", "name-1", "name-2"))
		Expect(vals).To(ConsistOf(int64(1), "a", true))
	})

	It("evaluates nested unwind", func() {
		exec, outID := makeExec([]string{"Pod"}, `[{"@unwind":"$.spec.list"},{"@unwind":"$.spec.list"}]`)
		in := zset.New()
		in.Insert(unstructured.New(map[string]any{
			"metadata": map[string]any{"name": "name", "namespace": "default"},
			"spec": map[string]any{"list": []any{
				[]any{int64(1), int64(2), int64(3)},
				[]any{int64(5), int64(6)},
			}},
		}, nil), 1)

		outs, err := exec.Execute(map[string]zset.ZSet{"input_Pod": in})
		Expect(err).NotTo(HaveOccurred())
		Expect(outs[outID].Size()).To(Equal(5))
	})

	It("evaluates two-source join with predicate", func() {
		exec, outID := makeExec([]string{"pod", "dep"}, `[
			{"@join":{"@eq":["$.dep.metadata.name","$.pod.spec.parent"]}},
			{"@project":{"metadata":{"name":"result","namespace":"default"},"pod":"$.pod","dep":"$.dep"}}
		]`)

		pods := zset.New()
		deps := zset.New()
		deps.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "dep1"}}, nil), 1)
		pods.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod1"}, "spec": map[string]any{"parent": "dep1"}}, nil), 1)
		pods.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod2"}, "spec": map[string]any{"parent": "dep2"}}, nil), 1)

		outs, err := exec.Execute(map[string]zset.ZSet{"input_pod": pods, "input_dep": deps})
		Expect(err).NotTo(HaveOccurred())
		docs := collectDocs(outs[outID])
		Expect(docs).To(HaveLen(1))
		pn, err := docs[0].GetField("metadata.name")
		Expect(err).NotTo(HaveOccurred())
		Expect(pn).To(Equal("result"))
	})

	It("evaluates three-source join", func() {
		exec, outID := makeExec([]string{"pod", "dep", "rs"}, `[
			{"@join":{"@and":[{"@eq":["$.dep.metadata.name","$.pod.spec.parent"]},{"@eq":["$.dep.metadata.name","$.rs.spec.dep"]}]}},
			{"@project":{"metadata":{"name":"result","namespace":"default"},"pod":"$.pod","dep":"$.dep","rs":"$.rs"}}
		]`)

		pods := zset.New()
		deps := zset.New()
		rs := zset.New()
		deps.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "dep1"}}, nil), 1)
		rs.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "rs1"}, "spec": map[string]any{"dep": "dep1"}}, nil), 1)
		pods.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod1"}, "spec": map[string]any{"parent": "dep1"}}, nil), 1)

		outs, err := exec.Execute(map[string]zset.ZSet{"input_pod": pods, "input_dep": deps, "input_rs": rs})
		Expect(err).NotTo(HaveOccurred())
		Expect(outs[outID].Size()).To(Equal(1))
	})

	It("errors for gather alias and accepts aggregate form", func() {
		c := New([]string{"Pod"}, []string{"output"})
		_, err := c.CompileString(`[{"@gather":["$.metadata.namespace","$.spec.a"]}]`)
		Expect(err).To(HaveOccurred())

		_, err = c.CompileString(`[{"@aggregate":["$.metadata.namespace","$.spec.a"]}]`)
		Expect(err).NotTo(HaveOccurred())
	})

	It("aggregates values by key using 2-arg gather-compatible form", func() {
		exec, outID := makeExec([]string{"pod"}, `[{"@aggregate":["$.metadata.namespace","$.spec.a"]}]`)
		pkByName := func(doc datamodel.Document) (string, error) {
			v, err := doc.GetField("metadata.name")
			if err != nil {
				return "", err
			}
			return v.(string), nil
		}

		in := zset.New()
		in.Insert(unstructured.New(map[string]any{
			"metadata": map[string]any{"name": "name", "namespace": "default"},
			"spec":     map[string]any{"a": int64(1), "b": map[string]any{"c": int64(2)}},
			"c":        "c",
		}, pkByName), 1)
		in.Insert(unstructured.New(map[string]any{
			"metadata": map[string]any{"name": "name2", "namespace": "default"},
			"spec":     map[string]any{"a": int64(2)},
		}, pkByName), 1)

		outs, err := exec.Execute(map[string]zset.ZSet{"input_pod": in})
		Expect(err).NotTo(HaveOccurred())
		docs := collectDocs(outs[outID])
		Expect(docs).To(HaveLen(2))
		all := make([]int64, 0, 2)
		for _, d := range docs {
			values, err := d.GetField("spec.a")
			Expect(err).NotTo(HaveOccurred())
			list := values.([]any)
			Expect(list).To(HaveLen(1))
			all = append(all, list[0].(int64))
		}
		sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
		Expect(all).To(Equal([]int64{1, 2}))
	})

	It("aggregates then projects stable object name", func() {
		exec, outID := makeExec([]string{"pod"}, `[
			{"@aggregate":["$.metadata.namespace","$.spec.a"]},
			{"@project":[{"$.":"$."},{"$.metadata.name":"stable-name"}]}
		]`)
		pkByName := func(doc datamodel.Document) (string, error) {
			v, err := doc.GetField("metadata.name")
			if err != nil {
				return "", err
			}
			return v.(string), nil
		}
		in := zset.New()
		in.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "a", "namespace": "default"}, "spec": map[string]any{"a": int64(1)}}, pkByName), 1)
		in.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "b", "namespace": "default"}, "spec": map[string]any{"a": int64(2)}}, pkByName), 1)

		outs, err := exec.Execute(map[string]zset.ZSet{"input_pod": in})
		Expect(err).NotTo(HaveOccurred())
		docs := collectDocs(outs[outID])
		Expect(docs).To(HaveLen(2))
		for _, d := range docs {
			name, err := d.GetField("metadata.name")
			Expect(err).NotTo(HaveOccurred())
			Expect(name).To(Equal("stable-name"))
		}
	})

	It("keeps @aggregate explicit reduce/outField form", func() {
		exec, outID := makeExec([]string{"grade"}, `[
			{"@aggregate":["$.class","$.grade","$$.","grades"]}
		]`)
		pkByClass := func(doc datamodel.Document) (string, error) {
			v, err := doc.GetField("class")
			if err != nil {
				return "", err
			}
			return v.(string), nil
		}
		in := zset.New()
		in.Insert(unstructured.New(map[string]any{"class": "Algebra", "grade": int64(90)}, pkByClass), 1)
		in.Insert(unstructured.New(map[string]any{"class": "Algebra", "grade": int64(80)}, pkByClass), 1)

		outs, err := exec.Execute(map[string]zset.ZSet{"input_grade": in})
		Expect(err).NotTo(HaveOccurred())
		docs := collectDocs(outs[outID])
		Expect(docs).To(HaveLen(1))
		v, err := docs[0].GetField("grades")
		Expect(err).NotTo(HaveOccurred())
		vals := v.([]any)
		sort.Slice(vals, func(i, j int) bool { return vals[i].(int64) < vals[j].(int64) })
		Expect(vals).To(Equal([]any{int64(80), int64(90)}))
	})

	It("exposes unwind nameAppend flag in compiled operator", func() {
		c := New([]string{"Pod"}, []string{"output"})
		q, err := c.CompileString(`[{"@unwind":"$.spec.list"}]`)
		Expect(err).NotTo(HaveOccurred())
		n := q.Circuit.Node("b0_op_0")
		Expect(n).NotTo(BeNil())
		u, ok := n.Operator.(*operator.Unwind)
		Expect(ok).To(BeTrue())
		Expect(u.String()).To(ContainSubstring("appendName=true"))
	})

	It("uses configured logical output name", func() {
		c := New([]string{"Pod"}, []string{"pods_out"})
		q, err := c.CompileString(`[{"@project":{"$.metadata":"$.metadata"}}]`)
		Expect(err).NotTo(HaveOccurred())
		Expect(q.OutputMap).To(HaveKeyWithValue("pods_out", "output_pods_out"))
		Expect(q.OutputNames()).To(Equal([]string{"pods_out"}))
	})

	It("rejects multiple configured outputs for now", func() {
		c := New([]string{"Pod"}, []string{"out1", "out2"})
		_, err := c.CompileString(`[{"@project":{"$.metadata":"$.metadata"}}]`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("configured output"))
		Expect(err.Error()).To(ContainSubstring("is unbound"))
	})

	It("returns stage-aware parse errors with argument context", func() {
		c := New([]string{"Pod"}, []string{"output"})
		_, err := c.CompileString(`[
			{"@select":{"@eq":["$.metadata.name", "ok"]}},
			{"@project":{"$.x":{"@doesnotexist":1}}}
		]`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("pipeline parse error at stage[1] @project"))
		Expect(err.Error()).To(ContainSubstring("argument projection[\"$.x\"]"))
		Expect(err.Error()).To(ContainSubstring("raw={\"@doesnotexist\":1}"))
	})

	It("supports explicit multi-branch graph wiring", func() {
		c := New([]string{"Pod"}, []string{"final"})
		q, err := c.CompileString(`[
			[
				{"@inputs":["Pod"]},
				{"@project":{"$.metadata.name":"a"}},
				{"@output":"branch1"}
			],
			[
				{"@inputs":["branch1"]},
				{"@project":[{"$.":"$."},{"$.spec.done":true}]},
				{"@output":"final"}
			]
		]`)
		Expect(err).NotTo(HaveOccurred())

		exec, err := executor.New(q.Circuit, logger.DiscardLogger())
		Expect(err).NotTo(HaveOccurred())
		in := zset.New()
		in.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "p1"}}, nil), 1)
		outs, err := exec.Execute(map[string]zset.ZSet{"input_Pod": in})
		Expect(err).NotTo(HaveOccurred())
		Expect(outs[q.OutputMap["final"]].Size()).To(Equal(1))
	})

	It("rejects branch dependency cycles", func() {
		c := New([]string{"Pod"}, []string{"a"})
		_, err := c.CompileString(`[
			[
				{"@inputs":["b"]},
				{"@project":{"$.metadata.name":"a"}},
				{"@output":"a"}
			],
			[
				{"@inputs":["a"]},
				{"@project":{"$.metadata.name":"b"}},
				{"@output":"b"}
			]
		]`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("branch dependency graph must be a DAG"))
	})
})
