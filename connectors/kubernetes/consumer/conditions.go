package consumer

import (
	"encoding/json"
	"time"
)

// Status-condition bookkeeping. The connector owns lastTransitionTime the
// same way controller-runtime's SetStatusCondition does: a condition keeps
// its observed timestamp while its (type, status) pair is unchanged, and is
// stamped with the write time when it flips or first appears. Pipelines
// emit conditions WITHOUT lastTransitionTime: the timestamp is
// reconcile-process metadata, not data. A wall-clock read inside a circuit
// breaks closed-loop idempotence (retractions stop cancelling, and a
// desired-state reconciler never quiesces), so the stamping belongs at the
// write boundary, where the observed object is the source of truth.

// conditionWriteTime is the timestamp freshly transitioned conditions are
// stamped with: wall clock at the write boundary, second precision (the
// metav1.Time wire format), so a written condition reads back identical.
func conditionWriteTime() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// mergeConditionTimes fills lastTransitionTime on every condition inside
// the desired status value, walking desired and observed in lockstep.
// Object lists align by the Kubernetes list conventions: "conditions"
// entries match on type; other object lists match on name (e.g. Gateway
// listener statuses), then on parentRef (route parent statuses), then fall
// back to the index. The desired value is mutated in place.
func mergeConditionTimes(desired, observed any, now string) {
	switch d := desired.(type) {
	case map[string]any:
		o, _ := observed.(map[string]any)
		for k, v := range d {
			var ov any
			if o != nil {
				ov = o[k]
			}
			if k == "conditions" {
				if list, ok := v.([]any); ok {
					olist, _ := ov.([]any)
					stampConditions(list, olist, now)
					continue
				}
			}
			mergeConditionTimes(v, ov, now)
		}
	case []any:
		olist, _ := observed.([]any)
		for i, elem := range d {
			mergeConditionTimes(elem, matchListEntry(elem, olist, i), now)
		}
	}
}

// matchListEntry finds the observed counterpart of a desired list entry.
func matchListEntry(elem any, observed []any, idx int) any {
	m, ok := elem.(map[string]any)
	if !ok {
		return nil
	}
	if name, ok := m["name"].(string); ok {
		for _, o := range observed {
			if om, ok := o.(map[string]any); ok {
				if on, _ := om["name"].(string); on == name {
					return om
				}
			}
		}
		return nil
	}
	if pr, ok := m["parentRef"]; ok {
		want := canonicalJSON(pr)
		for _, o := range observed {
			if om, ok := o.(map[string]any); ok {
				if canonicalJSON(om["parentRef"]) == want {
					return om
				}
			}
		}
		return nil
	}
	if idx < len(observed) {
		return observed[idx]
	}
	return nil
}

// canonicalJSON serializes a value deterministically (encoding/json sorts
// map keys), for structural list-entry matching.
func canonicalJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// stampConditions fills lastTransitionTime on each desired condition:
// carried over from the observed condition of the same type while the
// status is unchanged, stamped with now otherwise. A timestamp already set
// by the producer is respected.
func stampConditions(desired, observed []any, now string) {
	for _, elem := range desired {
		c, ok := elem.(map[string]any)
		if !ok {
			continue
		}
		if t, ok := c["lastTransitionTime"].(string); ok && t != "" {
			continue
		}
		ctype, _ := c["type"].(string)
		cstatus, _ := c["status"].(string)
		if prev := findCondition(observed, ctype); prev != nil {
			prevStatus, _ := prev["status"].(string)
			prevTime, _ := prev["lastTransitionTime"].(string)
			if prevStatus == cstatus && prevTime != "" {
				c["lastTransitionTime"] = prevTime
				continue
			}
		}
		c["lastTransitionTime"] = now
	}
}

// findCondition returns the condition with the given type, or nil.
func findCondition(conditions []any, ctype string) map[string]any {
	for _, elem := range conditions {
		if c, ok := elem.(map[string]any); ok {
			if t, _ := c["type"].(string); t == ctype {
				return c
			}
		}
	}
	return nil
}

// containsConditions reports whether the value carries any "conditions"
// list anywhere, i.e. whether condition-time bookkeeping applies to it.
func containsConditions(v any) bool {
	switch t := v.(type) {
	case map[string]any:
		for k, mv := range t {
			if k == "conditions" {
				if _, ok := mv.([]any); ok {
					return true
				}
			}
			if containsConditions(mv) {
				return true
			}
		}
	case []any:
		for _, e := range t {
			if containsConditions(e) {
				return true
			}
		}
	}
	return false
}

// hasConditionTimes reports whether any condition in the value carries a
// lastTransitionTime, i.e. whether the value can serve as the observed
// side of the merge. A document that came from observation has stamped
// conditions; a timeless pipeline fragment does not.
func hasConditionTimes(v any) bool {
	switch t := v.(type) {
	case map[string]any:
		if ts, ok := t["lastTransitionTime"].(string); ok && ts != "" {
			return true
		}
		for _, mv := range t {
			if hasConditionTimes(mv) {
				return true
			}
		}
	case []any:
		for _, e := range t {
			if hasConditionTimes(e) {
				return true
			}
		}
	}
	return false
}
