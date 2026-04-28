package codec

import (
	"encoding/csv"
	"fmt"
	"strings"

	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/zset"
)

// csvParseFunc returns a ZSetFunc that parses each "line" field as CSV.
// The first row is treated as the header; subsequent rows become documents.
func csvParseFunc(errFn func(error)) ZSetFunc {
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
			rows, err := parseCSVString(line)
			if err != nil {
				reportErr(errFn, fmt.Errorf("codec/csv parse: %w", err))
				continue
			}
			for _, row := range rows {
				out.Insert(dbspunstructured.New(row, nil), entry.Weight)
			}
		}
		return out, nil
	}
}

// csvFormatFunc returns a ZSetFunc that serialises each document as a
// single CSV row (header + data) stored in a "line" field.
func csvFormatFunc(errFn func(error)) ZSetFunc {
	return func(z zset.ZSet) (zset.ZSet, error) {
		out := zset.New()
		for _, entry := range z.Entries() {
			doc, ok := entry.Document.(*dbspunstructured.Unstructured)
			if !ok {
				reportErr(errFn, fmt.Errorf("codec/csv format: unsupported document type %T", entry.Document))
				continue
			}
			fields := doc.Fields()
			var sb strings.Builder
			w := csv.NewWriter(&sb)

			headers := sortedStringKeys(fields)
			if err := w.Write(headers); err != nil {
				reportErr(errFn, fmt.Errorf("codec/csv format: %w", err))
				continue
			}
			row := make([]string, len(headers))
			for i, h := range headers {
				row[i] = fmt.Sprintf("%v", fields[h])
			}
			if err := w.Write(row); err != nil {
				reportErr(errFn, fmt.Errorf("codec/csv format: %w", err))
				continue
			}
			w.Flush()
			if err := w.Error(); err != nil {
				reportErr(errFn, fmt.Errorf("codec/csv format flush: %w", err))
				continue
			}
			out.Insert(dbspunstructured.New(map[string]any{"line": sb.String()}, nil), entry.Weight)
		}
		return out, nil
	}
}

// parseCSVString parses a CSV string where the first row is the header.
func parseCSVString(s string) ([]map[string]any, error) {
	r := csv.NewReader(strings.NewReader(s))
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 2 {
		return nil, nil
	}
	headers := records[0]
	rows := make([]map[string]any, 0, len(records)-1)
	for _, rec := range records[1:] {
		row := make(map[string]any, len(headers))
		for i, h := range headers {
			if i < len(rec) {
				row[h] = rec[i]
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// sortedStringKeys returns the map keys in sorted order.
func sortedStringKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// Simple insertion sort for typically-small field counts.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
