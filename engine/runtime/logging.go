package runtime

import (
	"fmt"
	"sort"
	"strings"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/zset"
)

const (
	componentConsumer = "consumer"
	componentProducer = "producer"
)

// EventLogBuffer builds a structured runtime flow log payload.
func EventLogBuffer(eventType, componentType, component, direction, topic, logical string, data zset.ZSet, extra ...any) []any {
	_ = direction
	buf := []any{
		"operator", componentType,
		"event_type", eventType,
		"component", component,
		"topic", topic,
		"zset", ZSetPKSummary(data),
	}
	if logical != "" {
		buf = append(buf, "logical", logical)
	}
	if len(extra) > 0 {
		buf = append(buf, extra...)
	}
	return buf
}

// EventDocsLogBuffer builds a structured runtime flow payload with full docs.
func EventDocsLogBuffer(eventType, componentType, component, direction, topic, logical string, data zset.ZSet, extra ...any) []any {
	buf := EventLogBuffer(eventType, componentType, component, direction, topic, logical, data)
	buf = append(buf, "docs", ZSetFullSummary(data))
	buf = append(buf, extra...)
	return buf
}

// ApplyLogBuffer builds a structured consumer apply payload for a single row.
func ApplyLogBuffer(eventType, componentType, component, direction, topic, logical, pk string, weight zset.Weight, extra ...any) []any {
	_ = direction
	buf := []any{
		"operator", componentType,
		"event_type", eventType,
		"component", component,
		"topic", topic,
		"zset", []string{fmt.Sprintf("%s@%d", pk, weight)},
	}
	if logical != "" {
		buf = append(buf, "logical", logical)
	}
	if len(extra) > 0 {
		buf = append(buf, extra...)
	}
	return buf
}

// LogFlowEvent emits a single exclusive runtime log line:
// - V(2): producer/consumer "dbsp runtime event docs"
// - V(1): producer/consumer "dbsp runtime event"
// - V(0): consumer "dbsp runtime event"
func LogFlowEvent(log logr.Logger, eventType, componentType, component, direction, topic, logical string, data zset.ZSet, docsOverride []string, extra ...any) {
	infoBuf := EventLogBuffer(eventType, componentType, component, direction, topic, logical, data, extra...)
	if log.V(2).Enabled() {
		if !shouldLogDebug(componentType) {
			return
		}
		docsBuf := EventDocsLogBuffer(eventType, componentType, component, direction, topic, logical, data, extra...)
		if docsOverride != nil {
			overrideKey(docsBuf, "docs", docsOverride)
		}
		log.V(2).Info("dbsp runtime event docs", docsBuf...)
		return
	}
	if log.V(1).Enabled() {
		if !shouldLogDebug(componentType) {
			return
		}
		log.V(1).Info("dbsp runtime event", infoBuf...)
		return
	}
	if !shouldLogInfo(componentType) {
		return
	}
	log.Info("dbsp runtime event", infoBuf...)
}

// LogFlowApply emits a single exclusive apply log line:
// - V(2): producer/consumer "dbsp runtime apply doc"
// - V(1): producer/consumer "dbsp runtime apply"
// - V(0): consumer "dbsp runtime apply"
func LogFlowApply(log logr.Logger, eventType, componentType, component, direction, topic, logical, pk string, weight zset.Weight, docSource any, extra ...any) {
	infoBuf := ApplyLogBuffer(eventType, componentType, component, direction, topic, logical, pk, weight, extra...)
	if log.V(2).Enabled() {
		if !shouldLogDebug(componentType) {
			return
		}
		doc := resolveDoc(docSource)
		docBuf := ApplyLogBuffer(eventType, componentType, component, direction, topic, logical, pk, weight, append(extra, "doc", doc)...)
		log.V(2).Info("dbsp runtime apply doc", docBuf...)
		return
	}
	if log.V(1).Enabled() {
		if !shouldLogDebug(componentType) {
			return
		}
		log.V(1).Info("dbsp runtime apply", infoBuf...)
		return
	}
	if !shouldLogInfo(componentType) {
		return
	}
	log.Info("dbsp runtime apply", infoBuf...)
}

func shouldLogInfo(componentType string) bool {
	return componentType == componentConsumer
}

func shouldLogDebug(componentType string) bool {
	return componentType == componentConsumer || componentType == componentProducer
}

// ZSetPKSummary returns a stable primary-key summary (with weights) for logging.
func ZSetPKSummary(z zset.ZSet) []string {
	entries := z.Entries()
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		pk := logPrimaryKey(e.Document)
		out = append(out, fmt.Sprintf("%s@%d", pk, e.Weight))
	}
	sort.Strings(out)
	return out
}

// ZSetFullSummary returns a stable full entry list with docs and weights.
func ZSetFullSummary(z zset.ZSet) []string {
	entries := z.Entries()
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		doc := "<nil>"
		if e.Document != nil {
			doc = e.Document.String()
		}
		out = append(out, fmt.Sprintf("%s@%d", doc, e.Weight))
	}
	sort.Strings(out)
	return out
}

func logPrimaryKey(doc datamodel.Document) string {
	if doc == nil {
		return "<nil>"
	}

	pk, err := doc.PrimaryKey()
	hash := doc.Hash()
	if err == nil && pk != "" && pk != hash {
		return pk
	}

	if md := metadataNameKey(doc); md != "" {
		return md
	}

	if err == nil && pk != "" {
		return abbreviateHash(pk)
	}

	if hash == "" {
		return "<missing-pk>"
	}
	return abbreviateHash(hash)
}

func metadataNameKey(doc datamodel.Document) string {
	nameVal, err := doc.GetField("metadata.name")
	if err != nil {
		return ""
	}
	name, ok := nameVal.(string)
	if !ok || name == "" {
		return ""
	}
	nsVal, err := doc.GetField("metadata.namespace")
	if err == nil {
		if ns, ok := nsVal.(string); ok {
			if ns != "" {
				return ns + "/" + name
			}
		}
	}
	return name
}

func abbreviateHash(s string) string {
	if len(s) <= 24 {
		return s
	}
	t := strings.TrimSpace(s)
	if len(t) <= 24 {
		return t
	}
	return "hash:" + t[:24]
}

func overrideKey(buf []any, key string, value any) {
	for i := 0; i+1 < len(buf); i += 2 {
		k, ok := buf[i].(string)
		if !ok {
			continue
		}
		if k == key {
			buf[i+1] = value
			return
		}
	}
	buf = append(buf, key, value)
}

func resolveDoc(src any) string {
	switch v := src.(type) {
	case nil:
		return ""
	case string:
		return v
	case func() string:
		return v()
	default:
		return fmt.Sprint(v)
	}
}
