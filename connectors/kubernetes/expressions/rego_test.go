package expressions

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// requiredLabels is the stock Gatekeeper required-labels template policy.
const requiredLabels = `
package k8srequiredlabels

violation[{"msg": msg, "details": {"missing_labels": missing}}] {
	provided := {label | input.review.object.metadata.labels[label]}
	required := {label | label := input.parameters.labels[_]}
	missing := required - provided
	count(missing) > 0
	msg := sprintf("you must provide labels: %v", [missing])
}
`

// reviewFields inspects the audit-time review document we assemble.
const reviewFields = `
package reviewfields

violation[{"msg": msg}] {
	msg := sprintf("%v/%v/%v %v/%v", [
		input.review.kind.group,
		input.review.kind.version,
		input.review.kind.kind,
		input.review.namespace,
		input.review.name,
	])
}
`

// regoV1 opts into the v1 dialect from a v0 module, the Gatekeeper way.
const regoV1 = `
package regov1

import rego.v1

violation contains {"msg": "always"} if {
	input.review.object.kind == "Pod"
}
`

func pod(name string, labels map[string]any) map[string]any {
	metadata := map[string]any{"name": name, "namespace": "default"}
	if labels != nil {
		metadata["labels"] = labels
	}
	return map[string]any{"apiVersion": "v1", "kind": "Pod", "metadata": metadata}
}

// eval runs the @rego callback and unpacks the result document.
func eval(source string, parameters, object any) (violations []any, errStr any) {
	result, err := regoEval([]any{source, parameters, object})
	Expect(err).NotTo(HaveOccurred())
	doc, ok := result.(map[string]any)
	Expect(ok).To(BeTrue())
	violations, ok = doc["violations"].([]any)
	Expect(ok).To(BeTrue())
	return violations, doc["error"]
}

var _ = Describe("@rego", func() {
	params := map[string]any{"labels": []any{"owner"}}

	It("reports a violation on a non-compliant object", func() {
		violations, errStr := eval(requiredLabels, params, pod("web", nil))
		Expect(errStr).To(BeNil())
		Expect(violations).To(HaveLen(1))
		v := violations[0].(map[string]any)
		Expect(v["msg"]).To(ContainSubstring("owner"))
		Expect(v["details"]).To(HaveKey("missing_labels"))
	})

	It("passes a compliant object", func() {
		violations, errStr := eval(requiredLabels, params, pod("web", map[string]any{"owner": "me"}))
		Expect(errStr).To(BeNil())
		Expect(violations).To(BeEmpty())
	})

	It("orders multiple violations canonically", func() {
		multi := map[string]any{"labels": []any{"owner", "team", "env"}}
		first, errStr := eval(requiredLabels, multi, pod("web", map[string]any{"env": "prod"}))
		Expect(errStr).To(BeNil())
		second, _ := eval(requiredLabels, multi, pod("web", map[string]any{"env": "prod"}))
		Expect(first).To(Equal(second))
	})

	It("assembles the audit-time review document", func() {
		violations, errStr := eval(reviewFields, nil, map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata":   map[string]any{"name": "web", "namespace": "shop"},
		})
		Expect(errStr).To(BeNil())
		Expect(violations).To(HaveLen(1))
		v := violations[0].(map[string]any)
		Expect(v["msg"]).To(Equal("apps/v1/Deployment shop/web"))
	})

	It("supports the rego.v1 dialect via import", func() {
		violations, errStr := eval(regoV1, nil, pod("web", nil))
		Expect(errStr).To(BeNil())
		Expect(violations).To(HaveLen(1))
	})

	It("compile-checks without an object", func() {
		violations, errStr := eval(requiredLabels, nil, nil)
		Expect(errStr).To(BeNil())
		Expect(violations).To(BeEmpty())

		_, errStr = eval("package broken\nviolation[msg] {", nil, nil)
		Expect(errStr).To(BeAssignableToTypeOf(""))
		Expect(errStr).To(ContainSubstring("rego"))
	})

	It("returns compile errors as a result, not an expression error", func() {
		violations, errStr := eval("this is not rego", params, pod("web", nil))
		Expect(violations).To(BeEmpty())
		Expect(errStr).NotTo(BeNil())
	})

	It("treats builtin eval errors as undefined, the Gatekeeper semantics", func() {
		divByZero := `
package evalerror

violation[{"msg": msg}] {
	x := 1 / 0
	msg := sprintf("%v", [x])
}
`
		violations, errStr := eval(divByZero, nil, pod("web", nil))
		Expect(violations).To(BeEmpty())
		Expect(errStr).To(BeNil())
	})

	It("rejects wrong arity and a non-string source", func() {
		_, err := regoEval([]any{requiredLabels, params})
		Expect(err).To(HaveOccurred())
		_, err = regoEval([]any{42, params, pod("web", nil)})
		Expect(err).To(HaveOccurred())
	})

	It("exposes the operator table", func() {
		Expect(Ops()).To(HaveKey("@rego"))
	})
})
