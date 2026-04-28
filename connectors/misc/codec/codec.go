// Package codec provides functions that parse external byte streams into DBSP
// Z-sets and serialise Z-sets back to external formats.  Each entry in an
// incoming Z-set is expected to carry a single "line" field containing a raw
// string.  ParseFunc expands that string into structured key/value fields.
// FormatFunc does the reverse.
package codec

import (
	"fmt"

	"github.com/l7mp/dbsp/engine/zset"
)

// Format identifies a serialisation format.
type Format string

const (
	// FormatJSON parses a single JSON object or array.
	FormatJSON Format = "json"
	// FormatJSONL parses one JSON object per line (NDJSON / JSON Lines).
	FormatJSONL Format = "jsonl"
	// FormatYAML parses single or multi-document YAML.
	FormatYAML Format = "yaml"
	// FormatCSV parses CSV with the first row as the header.
	FormatCSV Format = "csv"
	// FormatAuto sniffs the first bytes and selects a format automatically.
	// Use an explicit format in production; FormatAuto is a developer convenience.
	FormatAuto Format = "auto"
)

// ZSetFunc transforms one Z-set into another.
type ZSetFunc func(zset.ZSet) (zset.ZSet, error)

// ParseFunc returns a ZSetFunc that parses the "line" field of each Z-set
// entry into structured fields.  Per-entry errors call errFn (if non-nil) and
// skip that entry; the batch continues.  The returned error is reserved for
// whole-batch failures (e.g. unknown format).
func ParseFunc(f Format, errFn func(error)) ZSetFunc {
	switch f {
	case FormatJSONL:
		return jsonlParseFunc(errFn)
	case FormatJSON:
		return jsonParseFunc(errFn)
	case FormatYAML:
		return yamlParseFunc(errFn)
	case FormatCSV:
		return csvParseFunc(errFn)
	case FormatAuto:
		return autoParseFunc(errFn)
	default:
		return func(zset.ZSet) (zset.ZSet, error) {
			return zset.New(), fmt.Errorf("codec: unknown format %q", f)
		}
	}
}

// FormatFunc returns a ZSetFunc that serialises each document to the target
// format, emitting {"line": "<serialised string>"}.  Symmetric with ParseFunc.
func FormatFunc(f Format, errFn func(error)) ZSetFunc {
	switch f {
	case FormatJSONL:
		return jsonlFormatFunc(errFn)
	case FormatJSON:
		return jsonFormatFunc(errFn)
	case FormatYAML:
		return yamlFormatFunc(errFn)
	case FormatCSV:
		return csvFormatFunc(errFn)
	case FormatAuto:
		return jsonlFormatFunc(errFn) // FormatAuto serialises as JSONL
	default:
		return func(zset.ZSet) (zset.ZSet, error) {
			return zset.New(), fmt.Errorf("codec: unknown format %q", f)
		}
	}
}

func reportErr(errFn func(error), err error) {
	if errFn != nil {
		errFn(err)
	}
}
