package consumer

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const tPrev = "2026-07-01T00:00:00Z"
const tNow = "2026-07-07T12:00:00Z"

func cnd(ctype, status string, extra ...string) map[string]any {
	c := map[string]any{"type": ctype, "status": status, "reason": ctype}
	if len(extra) > 0 {
		c["lastTransitionTime"] = extra[0]
	}
	return c
}

var _ = Describe("mergeConditionTimes", func() {
	It("keeps the observed timestamp while (type, status) is unchanged", func() {
		desired := map[string]any{"conditions": []any{cnd("Accepted", "True")}}
		observed := map[string]any{"conditions": []any{cnd("Accepted", "True", tPrev)}}

		mergeConditionTimes(desired, observed, tNow)
		Expect(desired["conditions"].([]any)[0].(map[string]any)["lastTransitionTime"]).To(Equal(tPrev))
	})

	It("stamps the write time on a status flip and on first appearance", func() {
		desired := map[string]any{"conditions": []any{
			cnd("Accepted", "False"),
			cnd("Programmed", "True"),
		}}
		observed := map[string]any{"conditions": []any{cnd("Accepted", "True", tPrev)}}

		mergeConditionTimes(desired, observed, tNow)
		got := desired["conditions"].([]any)
		Expect(got[0].(map[string]any)["lastTransitionTime"]).To(Equal(tNow))
		Expect(got[1].(map[string]any)["lastTransitionTime"]).To(Equal(tNow))
	})

	It("stamps everything when there is no observed status", func() {
		desired := map[string]any{"conditions": []any{cnd("Accepted", "True")}}

		mergeConditionTimes(desired, nil, tNow)
		Expect(desired["conditions"].([]any)[0].(map[string]any)["lastTransitionTime"]).To(Equal(tNow))
	})

	It("respects a timestamp the producer set explicitly", func() {
		explicit := "2025-01-01T00:00:00Z"
		desired := map[string]any{"conditions": []any{cnd("Accepted", "True", explicit)}}
		observed := map[string]any{"conditions": []any{cnd("Accepted", "True", tPrev)}}

		mergeConditionTimes(desired, observed, tNow)
		Expect(desired["conditions"].([]any)[0].(map[string]any)["lastTransitionTime"]).To(Equal(explicit))
	})

	It("aligns listener-style entries by name, not index", func() {
		desired := map[string]any{"listeners": []any{
			map[string]any{"name": "https", "conditions": []any{cnd("Programmed", "True")}},
			map[string]any{"name": "http", "conditions": []any{cnd("Programmed", "True")}},
		}}
		observed := map[string]any{"listeners": []any{
			map[string]any{"name": "http", "conditions": []any{cnd("Programmed", "True", tPrev)}},
		}}

		mergeConditionTimes(desired, observed, tNow)
		ls := desired["listeners"].([]any)
		httpsTime := ls[0].(map[string]any)["conditions"].([]any)[0].(map[string]any)["lastTransitionTime"]
		httpTime := ls[1].(map[string]any)["conditions"].([]any)[0].(map[string]any)["lastTransitionTime"]
		Expect(httpsTime).To(Equal(tNow))
		Expect(httpTime).To(Equal(tPrev))
	})

	It("aligns route parent entries by parentRef", func() {
		refA := map[string]any{"name": "gw-a", "sectionName": "http"}
		refB := map[string]any{"name": "gw-b"}
		desired := map[string]any{"parents": []any{
			map[string]any{"parentRef": refB, "conditions": []any{cnd("Accepted", "True")}},
			map[string]any{"parentRef": refA, "conditions": []any{cnd("Accepted", "True")}},
		}}
		observed := map[string]any{"parents": []any{
			map[string]any{"parentRef": refA, "conditions": []any{cnd("Accepted", "True", tPrev)}},
		}}

		mergeConditionTimes(desired, observed, tNow)
		ps := desired["parents"].([]any)
		bTime := ps[0].(map[string]any)["conditions"].([]any)[0].(map[string]any)["lastTransitionTime"]
		aTime := ps[1].(map[string]any)["conditions"].([]any)[0].(map[string]any)["lastTransitionTime"]
		Expect(bTime).To(Equal(tNow))
		Expect(aTime).To(Equal(tPrev))
	})
})
