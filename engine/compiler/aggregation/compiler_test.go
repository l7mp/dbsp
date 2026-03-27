package aggregation

import (
	"encoding/json"
	"sort"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/executor"
	"github.com/l7mp/dbsp/engine/internal/logger"
	"github.com/l7mp/dbsp/engine/operator"
	"github.com/l7mp/dbsp/engine/zset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Aggregation compiler parity", func() {
	makeExec := func(srcs []string, pipeline string) (*executor.Executor, string) {
		GinkgoHelper()
		c := New(toIdentityBindings(srcs), toIdentityBindings([]string{"output"}))
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
		c := New(toIdentityBindings([]string{"Pod"}), toIdentityBindings([]string{"output"}))
		q1, err := c.CompileString(`{"@project":{"$.metadata":"$.metadata"}}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(q1.Circuit).NotTo(BeNil())

		q2, err := c.CompileString(`[{"@project":{"$.metadata":"$.metadata"}}]`)
		Expect(err).NotTo(HaveOccurred())
		Expect(q2.Circuit).NotTo(BeNil())
	})

	It("supports explicit external-to-logical input/output bindings", func() {
		c := New(
			[]Binding{{Name: "test-op/foo/input", Logical: "Foo"}},
			[]Binding{{Name: "test-op/bar/output", Logical: "Bar"}},
		)
		q, err := c.CompileString(`[{"@project":{"$.":"$."}}]`)
		Expect(err).NotTo(HaveOccurred())

		Expect(q.InputMap).To(HaveKey("test-op/foo/input"))
		Expect(q.OutputMap).To(HaveKey("test-op/bar/output"))
		Expect(q.InputLogicalName("test-op/foo/input")).To(Equal("Foo"))
		Expect(q.OutputLogicalName("test-op/bar/output")).To(Equal("Bar"))

		exec, err := executor.New(q.Circuit, logger.DiscardLogger())
		Expect(err).NotTo(HaveOccurred())

		in := zset.New()
		in.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod1"}}, nil), 1)
		outs, err := exec.Execute(map[string]zset.ZSet{q.InputMap["test-op/foo/input"]: in})
		Expect(err).NotTo(HaveOccurred())
		Expect(outs[q.OutputMap["test-op/bar/output"]].Size()).To(Equal(1))
	})

	It("allows identical logical names for source and configured output", func() {
		c := New(
			[]Binding{{Name: "test-op/deployment/input", Logical: "Deployment"}},
			[]Binding{{Name: "test-op/deployment/output", Logical: "Deployment"}},
		)
		q, err := c.CompileString(`[{"@project":{"$.":"$."}}]`)
		Expect(err).NotTo(HaveOccurred())

		exec, err := executor.New(q.Circuit, logger.DiscardLogger())
		Expect(err).NotTo(HaveOccurred())

		in := zset.New()
		in.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "d1"}}, nil), 1)
		outs, err := exec.Execute(map[string]zset.ZSet{q.InputMap["test-op/deployment/input"]: in})
		Expect(err).NotTo(HaveOccurred())
		Expect(outs[q.OutputMap["test-op/deployment/output"]].Size()).To(Equal(1))
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

		names := make([]string, 0, len(docs))
		vals := make([]any, 0, len(docs))
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

	It("evaluates double unwind followed by groupBy and project aggregation", func() {
		exec, outID := makeExec([]string{"Service"}, `[
			{"@unwind":"$.spec.ports"},
			{"@project":{
				"metadata":{
					"name":"$.metadata.name",
					"namespace":"$.metadata.namespace"
				},
				"id":{
					"service":"$.metadata.name",
					"namespace":"$.metadata.namespace",
					"port":"$.spec.ports.port",
					"protocol":"$.spec.ports.protocol"
				},
				"endpoints":"$.spec.endpoints"
			}},
			{"@unwind":"$.endpoints"},
			{"@unwind":"$.endpoints.addresses"},
			{"@groupBy":["$.id","$.endpoints.addresses"]},
			{"@project":{
				"metadata":{"name":"$.key.service","namespace":"$.key.namespace"},
				"spec":{"port":"$.key.port","protocol":"$.key.protocol","addresses":"$.values"}
			}}
		]`)

		in := zset.New()
		in.Insert(unstructured.New(map[string]any{
			"metadata": map[string]any{"name": "svc-1", "namespace": "default"},
			"spec": map[string]any{
				"ports": []any{
					map[string]any{"port": int64(80), "protocol": "TCP"},
					map[string]any{"port": int64(443), "protocol": "TCP"},
				},
				"endpoints": []any{
					map[string]any{"addresses": []any{"192.0.2.1", "192.0.2.2"}},
					map[string]any{"addresses": []any{"192.0.2.3"}},
				},
			},
		}, nil), 1)

		outs, err := exec.Execute(map[string]zset.ZSet{"input_Service": in})
		Expect(err).NotTo(HaveOccurred())

		docs := collectDocs(outs[outID])
		Expect(docs).To(HaveLen(2))

		addressesByPort := map[int64][]string{}
		for _, d := range docs {
			portAny, err := d.GetField("spec.port")
			Expect(err).NotTo(HaveOccurred())
			protocolAny, err := d.GetField("spec.protocol")
			Expect(err).NotTo(HaveOccurred())
			addressesAny, err := d.GetField("spec.addresses")
			Expect(err).NotTo(HaveOccurred())

			Expect(protocolAny).To(Equal("TCP"))

			addrList := addressesAny.([]any)
			addrStrings := make([]string, 0, len(addrList))
			for _, a := range addrList {
				addrStrings = append(addrStrings, a.(string))
			}
			sort.Strings(addrStrings)
			addressesByPort[portAny.(int64)] = addrStrings
		}

		Expect(addressesByPort).To(HaveKey(int64(80)))
		Expect(addressesByPort).To(HaveKey(int64(443)))
		Expect(addressesByPort[int64(80)]).To(Equal([]string{"192.0.2.1", "192.0.2.2", "192.0.2.3"}))
		Expect(addressesByPort[int64(443)]).To(Equal([]string{"192.0.2.1", "192.0.2.2", "192.0.2.3"}))
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

	It("evaluates join predicate with bracketed JSONPath", func() {
		exec, outID := makeExec([]string{"ServiceView", "EndpointSlice"}, `[
			{"@join":{"@and":[{"@eq":["$.ServiceView.spec.serviceName","$[\"EndpointSlice\"][\"metadata\"][\"labels\"][\"kubernetes.io/service-name\"]"]},{"@eq":["$.ServiceView.metadata.namespace","$.EndpointSlice.metadata.namespace"]}]}},
			{"@project":{"metadata":{"name":"result","namespace":"$.ServiceView.metadata.namespace"},"spec":{"serviceName":"$.ServiceView.spec.serviceName"}}}
		]`)

		serviceViews := zset.New()
		endpointSlices := zset.New()

		serviceViews.Insert(unstructured.New(map[string]any{
			"metadata": map[string]any{"name": "test-service", "namespace": "testnamespace"},
			"spec":     map[string]any{"serviceName": "test-service"},
		}, nil), 1)

		endpointSlices.Insert(unstructured.New(map[string]any{
			"metadata": map[string]any{
				"name":      "es-1",
				"namespace": "testnamespace",
				"labels": map[string]any{
					"kubernetes.io/service-name": "test-service",
				},
			},
		}, nil), 1)

		outs, err := exec.Execute(map[string]zset.ZSet{
			"input_ServiceView":   serviceViews,
			"input_EndpointSlice": endpointSlices,
		})
		Expect(err).NotTo(HaveOccurred())

		docs := collectDocs(outs[outID])
		Expect(docs).To(HaveLen(1))
		svcName, err := docs[0].GetField("spec.serviceName")
		Expect(err).NotTo(HaveOccurred())
		Expect(svcName).To(Equal("test-service"))
	})

	It("rejects legacy gather alias", func() {
		c := New(toIdentityBindings([]string{"Pod"}), toIdentityBindings([]string{"output"}))

		_, err := c.CompileString(`[{"@gather":["$.metadata.namespace","$.spec.a"]}]`)
		Expect(err).To(HaveOccurred())
	})

	It("supports @groupBy bag semantics", func() {
		exec, outID := makeExec([]string{"pod"}, `[{"@groupBy":["$.metadata.namespace","$.spec.a"]}]`)
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
			"spec":     map[string]any{"a": int64(1)},
		}, pkByName), 1)
		in.Insert(unstructured.New(map[string]any{
			"metadata": map[string]any{"name": "name2", "namespace": "default"},
			"spec":     map[string]any{"a": int64(2)},
		}, pkByName), 1)

		outs, err := exec.Execute(map[string]zset.ZSet{"input_pod": in})
		Expect(err).NotTo(HaveOccurred())
		docs := collectDocs(outs[outID])
		Expect(docs).To(HaveLen(1))

		k, err := docs[0].GetField("key")
		Expect(err).NotTo(HaveOccurred())
		valsAny, err := docs[0].GetField("values")
		Expect(err).NotTo(HaveOccurred())
		docsAny, err := docs[0].GetField("documents")
		Expect(err).NotTo(HaveOccurred())

		vals := valsAny.([]any)
		sort.Slice(vals, func(i, j int) bool { return vals[i].(int64) < vals[j].(int64) })
		Expect(k).To(Equal("default"))
		Expect(vals).To(Equal([]any{int64(1), int64(2)}))
		Expect(docsAny.([]any)).To(HaveLen(2))
	})

	It("supports @groupBy distinct option", func() {
		exec, outID := makeExec([]string{"pod"}, `[{"@groupBy":["$.metadata.namespace","$.spec.a",{"distinct":true}]}]`)
		pkByName := func(doc datamodel.Document) (string, error) {
			v, err := doc.GetField("metadata.name")
			if err != nil {
				return "", err
			}
			return v.(string), nil
		}

		in := zset.New()
		in.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "a", "namespace": "default"}, "spec": map[string]any{"a": int64(1)}}, pkByName), 1)
		in.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "b", "namespace": "default"}, "spec": map[string]any{"a": int64(1)}}, pkByName), 1)

		outs, err := exec.Execute(map[string]zset.ZSet{"input_pod": in})
		Expect(err).NotTo(HaveOccurred())
		docs := collectDocs(outs[outID])
		Expect(docs).To(HaveLen(1))

		vals, err := docs[0].GetField("values")
		Expect(err).NotTo(HaveOccurred())
		Expect(vals.([]any)).To(Equal([]any{int64(1)}))
	})

	It("matches gather via @groupBy + @project", func() {
		exec, outID := makeExec([]string{"pod"}, `[
			{"@groupBy":["$.metadata.namespace","$.spec.a"]},
			{"@project":{"key":"$.key","items":"$.values"}}
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
		Expect(docs).To(HaveLen(1))

		key, err := docs[0].GetField("key")
		Expect(err).NotTo(HaveOccurred())
		itemsAny, err := docs[0].GetField("items")
		Expect(err).NotTo(HaveOccurred())
		items := itemsAny.([]any)
		sort.Slice(items, func(i, j int) bool { return items[i].(int64) < items[j].(int64) })
		Expect(key).To(Equal("default"))
		Expect(items).To(Equal([]any{int64(1), int64(2)}))
	})

	It("exposes unwind nameAppend flag in compiled operator", func() {
		c := New(toIdentityBindings([]string{"Pod"}), toIdentityBindings([]string{"output"}))
		q, err := c.CompileString(`[{"@unwind":"$.spec.list"}]`)
		Expect(err).NotTo(HaveOccurred())
		n := q.Circuit.Node("b0_op_0")
		Expect(n).NotTo(BeNil())
		u, ok := n.Operator.(*operator.Unwind)
		Expect(ok).To(BeTrue())
		Expect(u.String()).To(ContainSubstring("appendName=true"))
	})

	It("uses configured logical output name", func() {
		c := New(toIdentityBindings([]string{"Pod"}), toIdentityBindings([]string{"pods_out"}))
		q, err := c.CompileString(`[{"@project":{"$.metadata":"$.metadata"}}]`)
		Expect(err).NotTo(HaveOccurred())
		Expect(q.OutputMap).To(HaveKeyWithValue("pods_out", "output_pods_out"))
		Expect(q.OutputNames()).To(Equal([]string{"pods_out"}))
	})

	It("rejects multiple configured outputs for now", func() {
		c := New(toIdentityBindings([]string{"Pod"}), toIdentityBindings([]string{"out1", "out2"}))
		_, err := c.CompileString(`[{"@project":{"$.metadata":"$.metadata"}}]`)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("configured output"))
		Expect(err.Error()).To(ContainSubstring("is unbound"))
	})

	It("returns stage-aware parse errors with argument context", func() {
		c := New(toIdentityBindings([]string{"Pod"}), toIdentityBindings([]string{"output"}))
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
		c := New(toIdentityBindings([]string{"Pod"}), toIdentityBindings([]string{"final"}))
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

	It("round-trips compiled circuits with wrapped expressions", func() {
		c := New(toIdentityBindings([]string{"pod", "dep"}), toIdentityBindings([]string{"output"}))
		q, err := c.CompileString(`[
			{"@join":{"@eq":["$.dep.metadata.name","$.pod.spec.parent"]}},
			{"@select":{"@eq":["$.pod.metadata.namespace","default"]}},
			{"@project":{"metadata":{"name":"result"},"pod":"$.pod","dep":"$.dep"}}
		]`)
		Expect(err).NotTo(HaveOccurred())

		payload, err := q.Circuit.MarshalJSON()
		Expect(err).NotTo(HaveOccurred())

		var cloned circuit.Circuit
		Expect(json.Unmarshal(payload, &cloned)).To(Succeed())

		execOrig, err := executor.New(q.Circuit, logger.DiscardLogger())
		Expect(err).NotTo(HaveOccurred())
		execClone, err := executor.New(&cloned, logger.DiscardLogger())
		Expect(err).NotTo(HaveOccurred())

		pods := zset.New()
		deps := zset.New()
		deps.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "dep1"}}, nil), 1)
		pods.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod1", "namespace": "default"}, "spec": map[string]any{"parent": "dep1"}}, nil), 1)
		pods.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "pod2", "namespace": "other"}, "spec": map[string]any{"parent": "dep1"}}, nil), 1)

		inputs := map[string]zset.ZSet{"input_pod": pods, "input_dep": deps}
		outsOrig, err := execOrig.Execute(inputs)
		Expect(err).NotTo(HaveOccurred())
		outsClone, err := execClone.Execute(inputs)
		Expect(err).NotTo(HaveOccurred())

		outID := q.OutputMap["output"]
		Expect(outsClone[outID].Equal(outsOrig[outID])).To(BeTrue())
	})

	It("rejects branch dependency cycles", func() {
		c := New(toIdentityBindings([]string{"Pod"}), toIdentityBindings([]string{"a"}))
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
