package codec

import (
	"fmt"
	"strings"

	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/zset"
	"go.yaml.in/yaml/v3"
)

// yamlParseFunc returns a ZSetFunc that parses each "line" field as a
// single or multi-document YAML stream.
func yamlParseFunc(errFn func(error)) ZSetFunc {
	return func(z zset.ZSet) (zset.ZSet, error) {
		out := zset.New()
		for _, entry := range z.Entries() {
			line, err := lineField(entry.Document)
			if err != nil {
				reportErr(errFn, err)
				continue
			}
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			docs, err := parseYAMLString(line)
			if err != nil {
				reportErr(errFn, fmt.Errorf("codec/yaml parse: %w", err))
				continue
			}
			for _, d := range docs {
				out.Insert(dbspunstructured.New(d, nil), entry.Weight)
			}
		}
		return out, nil
	}
}

// yamlFormatFunc returns a ZSetFunc that serialises each document as YAML
// stored in a "line" field.
func yamlFormatFunc(errFn func(error)) ZSetFunc {
	return func(z zset.ZSet) (zset.ZSet, error) {
		out := zset.New()
		for _, entry := range z.Entries() {
			doc, ok := entry.Document.(*dbspunstructured.Unstructured)
			if !ok {
				reportErr(errFn, fmt.Errorf("codec/yaml format: unsupported document type %T", entry.Document))
				continue
			}
			b, err := yaml.Marshal(doc.Fields())
			if err != nil {
				reportErr(errFn, fmt.Errorf("codec/yaml format: %w", err))
				continue
			}
			out.Insert(dbspunstructured.New(map[string]any{"line": string(b)}, nil), entry.Weight)
		}
		return out, nil
	}
}

// parseYAMLString decodes a YAML string that may contain multiple documents
// separated by "---".  Each document must decode to map[string]any.
func parseYAMLString(s string) ([]map[string]any, error) {
	dec := yaml.NewDecoder(strings.NewReader(s))
	var docs []map[string]any
	for {
		var doc map[string]any
		err := dec.Decode(&doc)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, err
		}
		if doc != nil {
			docs = append(docs, doc)
		}
	}
	return docs, nil
}
