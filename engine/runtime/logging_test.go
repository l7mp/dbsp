package runtime_test

import (
	"fmt"

	"github.com/go-logr/logr"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	"github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Runtime logging helpers", func() {
	buildTestZSet := func() zset.ZSet {
		zs := zset.New()
		zs.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "b", "namespace": "default"}}, nil), 1)
		zs.Insert(unstructured.New(map[string]any{"metadata": map[string]any{"name": "a", "namespace": "default"}}, nil), -1)
		return zs
	}

	It("builds stable flow log buffers", func() {
		zs := buildTestZSet()

		buf := runtime.EventLogBuffer("processor.receive", "processor", "proc-1", "input", "in-topic", "Foo", zs)
		m := kv(buf)
		Expect(m["operator"]).To(Equal("processor"))
		Expect(m["event_type"]).To(Equal("processor.receive"))
		Expect(m["component"]).To(Equal("proc-1"))
		Expect(m["topic"]).To(Equal("in-topic"))
		Expect(m["logical"]).To(Equal("Foo"))
		Expect(fmt.Sprint(m["zset"])).To(ContainSubstring("default/a@-1"))
		Expect(fmt.Sprint(m["zset"])).To(ContainSubstring("default/b@1"))
		Expect(fmt.Sprint(m["zset"])).To(ContainSubstring("@-1"))
		Expect(fmt.Sprint(m["zset"])).To(ContainSubstring("@1"))
	})

	It("logs only consumers at info and producers or consumers at debug", func() {
		zs := buildTestZSet()

		infoLog, infoEntries := newCaptureLogger(0)
		runtime.LogFlowEvent(infoLog, "consumer.apply", "consumer", "con-1", "apply", "out", "", zs, nil)
		runtime.LogFlowEvent(infoLog, "producer.emit", "producer", "prod-1", "output", "in", "", zs, nil)
		runtime.LogFlowEvent(infoLog, "processor.send", "processor", "proc-1", "output", "out", "", zs, nil)
		Expect(*infoEntries).To(HaveLen(1))
		Expect((*infoEntries)[0].message).To(Equal("dbsp runtime event"))
		infoMap := kv((*infoEntries)[0].kv)
		Expect(infoMap["operator"]).To(Equal("consumer"))
		_, hasDocs := infoMap["docs"]
		Expect(hasDocs).To(BeFalse())

		debugLog, debugEntries := newCaptureLogger(1)
		runtime.LogFlowEvent(debugLog, "consumer.apply", "consumer", "con-1", "apply", "out", "", zs, nil)
		runtime.LogFlowEvent(debugLog, "producer.emit", "producer", "prod-1", "output", "in", "", zs, nil)
		runtime.LogFlowEvent(debugLog, "processor.send", "processor", "proc-1", "output", "out", "", zs, nil)
		Expect(*debugEntries).To(HaveLen(2))
		for _, e := range *debugEntries {
			Expect(e.message).To(Equal("dbsp runtime event"))
			em := kv(e.kv)
			Expect(em["operator"]).To(BeElementOf("consumer", "producer"))
			_, docsPresent := em["docs"]
			Expect(docsPresent).To(BeFalse())
		}
	})

	It("logs full docs only at trace", func() {
		zs := buildTestZSet()

		traceLog, traceEntries := newCaptureLogger(2)
		runtime.LogFlowEvent(traceLog, "consumer.apply", "consumer", "con-1", "apply", "out", "", zs, []string{"doc-override@1"})
		Expect(*traceEntries).To(HaveLen(1))
		Expect((*traceEntries)[0].message).To(Equal("dbsp runtime event docs"))
		traceMap := kv((*traceEntries)[0].kv)
		Expect(traceMap["operator"]).To(Equal("consumer"))
		Expect(traceMap["docs"]).To(Equal([]string{"doc-override@1"}))

		debugLog, debugEntries := newCaptureLogger(1)
		runtime.LogFlowEvent(debugLog, "consumer.apply", "consumer", "con-1", "apply", "out", "", zs, []string{"doc-override@1"})
		Expect(*debugEntries).To(HaveLen(1))
		debugMap := kv((*debugEntries)[0].kv)
		_, hasDocs := debugMap["docs"]
		Expect(hasDocs).To(BeFalse())
	})

	It("applies the same verbosity policy for apply logs", func() {
		infoLog, infoEntries := newCaptureLogger(0)
		runtime.LogFlowApply(infoLog, "consumer.apply", "consumer", "con-1", "apply", "out", "", "default/a", 1, "doc")
		runtime.LogFlowApply(infoLog, "producer.emit", "producer", "prod-1", "output", "in", "", "default/a", 1, "doc")
		Expect(*infoEntries).To(HaveLen(1))
		Expect((*infoEntries)[0].message).To(Equal("dbsp runtime apply"))
		infoMap := kv((*infoEntries)[0].kv)
		Expect(infoMap["zset"]).To(Equal([]string{"default/a@1"}))
		_, hasDoc := infoMap["doc"]
		Expect(hasDoc).To(BeFalse())

		debugLog, debugEntries := newCaptureLogger(1)
		runtime.LogFlowApply(debugLog, "consumer.apply", "consumer", "con-1", "apply", "out", "", "default/a", 1, "doc")
		runtime.LogFlowApply(debugLog, "producer.emit", "producer", "prod-1", "output", "in", "", "default/a", 1, "doc")
		Expect(*debugEntries).To(HaveLen(2))
		for _, e := range *debugEntries {
			Expect(e.message).To(Equal("dbsp runtime apply"))
			em := kv(e.kv)
			_, docsPresent := em["doc"]
			Expect(docsPresent).To(BeFalse())
		}

		traceLog, traceEntries := newCaptureLogger(2)
		runtime.LogFlowApply(traceLog, "consumer.apply", "consumer", "con-1", "apply", "out", "", "default/a", 1, "full-doc")
		Expect(*traceEntries).To(HaveLen(1))
		Expect((*traceEntries)[0].message).To(Equal("dbsp runtime apply doc"))
		traceMap := kv((*traceEntries)[0].kv)
		Expect(traceMap["doc"]).To(Equal("full-doc"))
	})
})

type captureEntry struct {
	message string
	kv      []any
}

type captureSink struct {
	maxV   int
	prefix []any
	store  *[]captureEntry
}

func newCaptureLogger(maxV int) (logr.Logger, *[]captureEntry) {
	entries := []captureEntry{}
	return logr.New(&captureSink{maxV: maxV, store: &entries}), &entries
}

func (s *captureSink) Init(logr.RuntimeInfo) {}

func (s *captureSink) Enabled(level int) bool { return level <= s.maxV }

func (s *captureSink) Info(level int, msg string, keysAndValues ...any) {
	if !s.Enabled(level) {
		return
	}
	kvPairs := append(append([]any{}, s.prefix...), keysAndValues...)
	*s.store = append(*s.store, captureEntry{message: msg, kv: kvPairs})
}

func (s *captureSink) Error(_ error, _ string, _ ...any) {}

func (s *captureSink) WithValues(keysAndValues ...any) logr.LogSink {
	clone := *s
	clone.prefix = append(append([]any{}, s.prefix...), keysAndValues...)
	return &clone
}

func (s *captureSink) WithName(_ string) logr.LogSink {
	clone := *s
	return &clone
}

func kv(buf []any) map[string]any {
	out := map[string]any{}
	for i := 0; i+1 < len(buf); i += 2 {
		k, ok := buf[i].(string)
		if !ok {
			continue
		}
		out[k] = buf[i+1]
	}
	return out
}
