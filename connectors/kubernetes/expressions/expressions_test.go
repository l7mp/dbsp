package expressions

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// match evaluates the @selectorMatches callback directly.
func match(selector, labels any) bool {
	result, err := selectorMatches([]any{selector, labels})
	Expect(err).NotTo(HaveOccurred())
	b, ok := result.(bool)
	Expect(ok).To(BeTrue())
	return b
}

var _ = Describe("@selectorMatches", func() {
	It("matches on matchLabels", func() {
		sel := map[string]any{"matchLabels": map[string]any{"app": "web"}}
		Expect(match(sel, map[string]any{"app": "web", "tier": "front"})).To(BeTrue())
		Expect(match(sel, map[string]any{"app": "db"})).To(BeFalse())
		Expect(match(sel, map[string]any{})).To(BeFalse())
		Expect(match(sel, nil)).To(BeFalse())
	})

	It("matches everything on an empty or nil selector", func() {
		Expect(match(map[string]any{}, map[string]any{"a": "b"})).To(BeTrue())
		Expect(match(map[string]any{}, nil)).To(BeTrue())
		Expect(match(nil, map[string]any{"a": "b"})).To(BeTrue())
	})

	It("implements matchExpressions", func() {
		in := map[string]any{
			"matchExpressions": []any{
				map[string]any{"key": "env", "operator": "In", "values": []any{"prod", "dev"}},
			},
		}
		Expect(match(in, map[string]any{"env": "prod"})).To(BeTrue())
		Expect(match(in, map[string]any{"env": "test"})).To(BeFalse())
		Expect(match(in, map[string]any{})).To(BeFalse())

		notIn := map[string]any{
			"matchExpressions": []any{
				map[string]any{"key": "env", "operator": "NotIn", "values": []any{"prod"}},
			},
		}
		Expect(match(notIn, map[string]any{"env": "dev"})).To(BeTrue())
		Expect(match(notIn, map[string]any{"env": "prod"})).To(BeFalse())

		exists := map[string]any{
			"matchExpressions": []any{
				map[string]any{"key": "env", "operator": "Exists"},
			},
		}
		Expect(match(exists, map[string]any{"env": "x"})).To(BeTrue())
		Expect(match(exists, map[string]any{"other": "x"})).To(BeFalse())

		doesNotExist := map[string]any{
			"matchExpressions": []any{
				map[string]any{"key": "env", "operator": "DoesNotExist"},
			},
		}
		Expect(match(doesNotExist, map[string]any{"other": "x"})).To(BeTrue())
		Expect(match(doesNotExist, map[string]any{"env": "x"})).To(BeFalse())
	})

	It("conjoins matchLabels and matchExpressions", func() {
		sel := map[string]any{
			"matchLabels": map[string]any{"app": "web"},
			"matchExpressions": []any{
				map[string]any{"key": "env", "operator": "In", "values": []any{"prod"}},
			},
		}
		Expect(match(sel, map[string]any{"app": "web", "env": "prod"})).To(BeTrue())
		Expect(match(sel, map[string]any{"app": "web", "env": "dev"})).To(BeFalse())
		Expect(match(sel, map[string]any{"app": "db", "env": "prod"})).To(BeFalse())
	})

	It("fails closed on invalid selectors and label maps", func() {
		bad := map[string]any{
			"matchExpressions": []any{
				map[string]any{"key": "env", "operator": "Bogus"},
			},
		}
		Expect(match(bad, map[string]any{"env": "prod"})).To(BeFalse())

		// In requires values; an empty list is invalid per the API contract.
		noValues := map[string]any{
			"matchExpressions": []any{
				map[string]any{"key": "env", "operator": "In"},
			},
		}
		Expect(match(noValues, map[string]any{"env": "prod"})).To(BeFalse())

		sel := map[string]any{"matchLabels": map[string]any{"app": "web"}}
		Expect(match(sel, map[string]any{"app": int64(3)})).To(BeFalse())
	})

	It("rejects wrong arity", func() {
		_, err := selectorMatches([]any{map[string]any{}})
		Expect(err).To(HaveOccurred())
	})

	It("exposes the operator table", func() {
		Expect(Ops()).To(HaveKey("@selectorMatches"))
	})
})
