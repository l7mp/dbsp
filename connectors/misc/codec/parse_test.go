package codec_test

import (
	"errors"

	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/zset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/connectors/misc/codec"
)

// lineZSet builds a Z-set with one {"line": s} document at weight 1.
func lineZSet(s string) zset.ZSet {
	z := zset.New()
	z.Insert(dbspunstructured.New(map[string]any{"line": s}, nil), 1)
	return z
}

// fieldOf returns the named string field from the first entry of the Z-set,
// or "" if not found.
func fieldOf(z zset.ZSet, field string) string {
	for _, e := range z.Entries() {
		u, ok := e.Document.(*dbspunstructured.Unstructured)
		if !ok {
			return ""
		}
		v, ok := u.Fields()[field]
		if !ok {
			return ""
		}
		s, _ := v.(string)
		return s
	}
	return ""
}

var _ = Describe("ParseFunc", func() {
	Describe("JSONL", func() {
		fn := codec.ParseFunc(codec.FormatJSONL, nil)

		It("parses a valid JSON object line", func() {
			out, err := fn(lineZSet(`{"key":"val","n":42}`))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Size()).To(Equal(1))
			Expect(fieldOf(out, "key")).To(Equal("val"))
		})

		It("skips invalid JSON lines and calls errFn", func() {
			var errs []error
			fn2 := codec.ParseFunc(codec.FormatJSONL, func(e error) { errs = append(errs, e) })
			out, err := fn2(lineZSet(`not-json`))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Size()).To(Equal(0))
			Expect(errs).To(HaveLen(1))
		})

		It("handles empty line gracefully", func() {
			out, err := fn(lineZSet("   "))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Size()).To(Equal(0))
		})
	})

	Describe("JSON", func() {
		fn := codec.ParseFunc(codec.FormatJSON, nil)

		It("parses a single JSON object", func() {
			out, err := fn(lineZSet(`{"a":"b"}`))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Size()).To(Equal(1))
		})

		It("expands a JSON array to multiple entries", func() {
			out, err := fn(lineZSet(`[{"id":1},{"id":2}]`))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Size()).To(Equal(2))
		})
	})

	Describe("YAML", func() {
		fn := codec.ParseFunc(codec.FormatYAML, nil)

		It("parses a single YAML document", func() {
			out, err := fn(lineZSet("key: value\nn: 1\n"))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Size()).To(Equal(1))
			Expect(fieldOf(out, "key")).To(Equal("value"))
		})

		It("parses multiple YAML documents", func() {
			out, err := fn(lineZSet("---\na: 1\n---\nb: 2\n"))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Size()).To(Equal(2))
		})
	})

	Describe("CSV", func() {
		fn := codec.ParseFunc(codec.FormatCSV, nil)

		It("uses first row as header", func() {
			out, err := fn(lineZSet("name,age\nalice,30\nbob,25\n"))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Size()).To(Equal(2))
			names := make([]string, 0, 2)
			for _, e := range out.Entries() {
				u, ok := e.Document.(*dbspunstructured.Unstructured)
				if ok {
					if v, ok := u.Fields()["name"]; ok {
						if s, ok := v.(string); ok {
							names = append(names, s)
						}
					}
				}
			}
			Expect(names).To(ConsistOf("alice", "bob"))
		})

		It("returns empty for header-only CSV", func() {
			out, err := fn(lineZSet("name,age\n"))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Size()).To(Equal(0))
		})
	})

	Describe("Auto", func() {
		fn := codec.ParseFunc(codec.FormatAuto, nil)

		It("detects JSON", func() {
			out, err := fn(lineZSet(`{"auto":true}`))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Size()).To(Equal(1))
		})

		It("detects YAML", func() {
			out, err := fn(lineZSet("---\nfoo: bar\n"))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Size()).To(Equal(1))
		})

		It("detects CSV", func() {
			out, err := fn(lineZSet("x,y\n1,2\n"))
			Expect(err).NotTo(HaveOccurred())
			Expect(out.Size()).To(Equal(1))
		})
	})

	Describe("Unknown format", func() {
		It("returns an error", func() {
			fn := codec.ParseFunc("bogus", nil)
			_, err := fn(lineZSet("data"))
			Expect(err).To(HaveOccurred())
			Expect(errors.Is(err, err)).To(BeTrue())
		})
	})
})

var _ = Describe("FormatFunc", func() {
	It("serialises a document back to JSONL", func() {
		fn := codec.FormatFunc(codec.FormatJSONL, nil)
		z := zset.New()
		z.Insert(dbspunstructured.New(map[string]any{"k": "v"}, nil), 1)
		out, err := fn(z)
		Expect(err).NotTo(HaveOccurred())
		Expect(out.Size()).To(Equal(1))
		line := fieldOf(out, "line")
		Expect(line).To(ContainSubstring(`"k"`))
		Expect(line).To(ContainSubstring(`"v"`))
	})

	It("serialises a document to CSV", func() {
		fn := codec.FormatFunc(codec.FormatCSV, nil)
		z := zset.New()
		z.Insert(dbspunstructured.New(map[string]any{"name": "alice", "age": "30"}, nil), 1)
		out, err := fn(z)
		Expect(err).NotTo(HaveOccurred())
		Expect(out.Size()).To(Equal(1))
		line := fieldOf(out, "line")
		Expect(line).To(ContainSubstring("name"))
		Expect(line).To(ContainSubstring("alice"))
	})
})
