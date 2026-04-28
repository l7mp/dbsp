package codec

import (
	"encoding/json"
	"fmt"
	"strings"

	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/zset"
)

// jsonlParseFunc returns a ZSetFunc that parses each "line" field as a
// JSON object (one object per line; invalid lines are skipped via errFn).
func jsonlParseFunc(errFn func(error)) ZSetFunc {
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
			var fields map[string]any
			if err := json.Unmarshal([]byte(line), &fields); err != nil {
				reportErr(errFn, fmt.Errorf("codec/jsonl parse: %w", err))
				continue
			}
			out.Insert(dbspunstructured.New(fields, nil), entry.Weight)
		}
		return out, nil
	}
}

// jsonlFormatFunc returns a ZSetFunc that serialises each document as a
// single-line JSON object stored in a "line" field.
func jsonlFormatFunc(errFn func(error)) ZSetFunc {
	return func(z zset.ZSet) (zset.ZSet, error) {
		out := zset.New()
		for _, entry := range z.Entries() {
			doc, ok := entry.Document.(*dbspunstructured.Unstructured)
			if !ok {
				reportErr(errFn, fmt.Errorf("codec/jsonl format: unsupported document type %T", entry.Document))
				continue
			}
			b, err := json.Marshal(doc.Fields())
			if err != nil {
				reportErr(errFn, fmt.Errorf("codec/jsonl format: %w", err))
				continue
			}
			out.Insert(dbspunstructured.New(map[string]any{"line": string(b)}, nil), entry.Weight)
		}
		return out, nil
	}
}

// jsonParseFunc returns a ZSetFunc that parses the first "line" field as
// a JSON object or array.  Arrays expand to multiple Z-set entries.
func jsonParseFunc(errFn func(error)) ZSetFunc {
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

			// Try object first, then array.
			var obj map[string]any
			if err := json.Unmarshal([]byte(line), &obj); err == nil {
				out.Insert(dbspunstructured.New(obj, nil), entry.Weight)
				continue
			}
			var arr []map[string]any
			if err := json.Unmarshal([]byte(line), &arr); err == nil {
				for _, item := range arr {
					out.Insert(dbspunstructured.New(item, nil), entry.Weight)
				}
				continue
			}
			reportErr(errFn, fmt.Errorf("codec/json parse: not a JSON object or array"))
		}
		return out, nil
	}
}

// jsonFormatFunc serialises each document as a single-line JSON object.
// Identical to jsonlFormatFunc for individual documents.
func jsonFormatFunc(errFn func(error)) ZSetFunc {
	return jsonlFormatFunc(errFn)
}

// autoParseFunc sniffs the first non-whitespace byte(s) of the "line" field
// and delegates to the appropriate parser.
func autoParseFunc(errFn func(error)) ZSetFunc {
	return func(z zset.ZSet) (zset.ZSet, error) {
		out := zset.New()
		for _, entry := range z.Entries() {
			line, err := lineField(entry.Document)
			if err != nil {
				reportErr(errFn, err)
				continue
			}
			trimmed := strings.TrimSpace(line)
			if trimmed == "" {
				continue
			}

			format := sniffFormat(trimmed)
			var fields map[string]any
			switch format {
			case FormatAuto:
				// sniffFormat never returns FormatAuto; skip unrecognizable input.
				continue
			case FormatJSONL, FormatJSON:
				if err := json.Unmarshal([]byte(trimmed), &fields); err != nil {
					reportErr(errFn, fmt.Errorf("codec/auto parse json: %w", err))
					continue
				}
			case FormatYAML:
				docs, err := parseYAMLString(trimmed)
				if err != nil {
					reportErr(errFn, fmt.Errorf("codec/auto parse yaml: %w", err))
					continue
				}
				for _, d := range docs {
					out.Insert(dbspunstructured.New(d, nil), entry.Weight)
				}
				continue
			case FormatCSV:
				rows, err := parseCSVString(trimmed)
				if err != nil {
					reportErr(errFn, fmt.Errorf("codec/auto parse csv: %w", err))
					continue
				}
				for _, row := range rows {
					out.Insert(dbspunstructured.New(row, nil), entry.Weight)
				}
				continue
			}
			if fields != nil {
				out.Insert(dbspunstructured.New(fields, nil), entry.Weight)
			}
		}
		return out, nil
	}
}

// sniffFormat detects the likely format from the first non-whitespace bytes.
func sniffFormat(s string) Format {
	if strings.HasPrefix(s, "{") || strings.HasPrefix(s, "[") {
		return FormatJSONL
	}
	if strings.HasPrefix(s, "---") || strings.HasPrefix(s, "%YAML") {
		return FormatYAML
	}
	// CSV heuristic: first line contains a comma.
	firstLine, _, _ := strings.Cut(s, "\n")
	if strings.Contains(firstLine, ",") {
		return FormatCSV
	}
	return FormatJSON
}

// lineField extracts the "line" string field from a document.
func lineField(doc any) (string, error) {
	u, ok := doc.(*dbspunstructured.Unstructured)
	if !ok {
		return "", fmt.Errorf("codec: unsupported document type %T", doc)
	}
	v, ok := u.Fields()["line"]
	if !ok {
		return "", fmt.Errorf("codec: document has no %q field", "line")
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("codec: %q field is not a string (%T)", "line", v)
	}
	return s, nil
}
