package producer

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// LogConfig configures a LogProducer.
type LogConfig struct {
	// Client is the Kubernetes client set. Required.
	Client kubernetes.Interface
	// Name is the unique component name for error reporting. Required.
	Name string
	// PodName is the pod to stream logs from. Required.
	PodName string
	// Namespace is the pod's namespace. Defaults to "default".
	Namespace string
	// Container is the container to stream logs from. Defaults to the first container.
	Container string
	// InputName is the runtime topic to publish to. Required.
	InputName string
	// Runtime is the engine runtime. Required.
	Runtime *dbspruntime.Runtime
	// Logger is a structured logger. Optional.
	Logger logr.Logger
}

// LogProducer streams pod logs and emits one {"line": "<raw>"} document per
// log line at weight +1.  It is format-agnostic; callers supply a producer
// callback (e.g. format.jsonl) to parse the raw lines into structured fields.
type LogProducer struct {
	*dbspruntime.BaseProducer

	client    kubernetes.Interface
	podName   string
	namespace string
	container string
	inputName string
	log       logr.Logger

	// streamFunc overrides the default log stream opener. Used for testing.
	streamFunc func(ctx context.Context) (io.ReadCloser, error)
}

var _ dbspruntime.Producer = (*LogProducer)(nil)

// NewLogProducer creates a LogProducer. It does not start streaming; call
// Start to begin.
func NewLogProducer(cfg LogConfig) (*LogProducer, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("log producer: client is required")
	}
	if cfg.Runtime == nil {
		return nil, fmt.Errorf("log producer: runtime is required")
	}

	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}
	ns := cfg.Namespace
	if ns == "" {
		ns = "default"
	}

	base, err := dbspruntime.NewBaseProducer(dbspruntime.BaseProducerConfig{
		Name:          cfg.Name,
		Publisher:     cfg.Runtime.NewPublisher(),
		ErrorReporter: cfg.Runtime,
		Logger:        log.WithName("log-producer").WithValues("pod", cfg.PodName, "namespace", ns),
		Topics:        []string{cfg.InputName},
	})
	if err != nil {
		return nil, err
	}

	return &LogProducer{
		BaseProducer: base,
		client:       cfg.Client,
		podName:      cfg.PodName,
		namespace:    ns,
		container:    cfg.Container,
		inputName:    cfg.InputName,
		log:          log.WithName("log-producer").WithValues("pod", cfg.PodName, "namespace", ns),
	}, nil
}

// Name returns the component name.
func (p *LogProducer) Name() string { return p.BaseProducer.Name() }

// String implements fmt.Stringer.
func (p *LogProducer) String() string {
	if p == nil {
		return "producer<log>{<nil>}"
	}
	return fmt.Sprintf("producer<log>{name=%q, pod=%q, namespace=%q, topic=%q}",
		p.Name(), p.podName, p.namespace, p.inputName)
}

// MarshalJSON provides a stable machine-readable representation.
func (p *LogProducer) MarshalJSON() ([]byte, error) {
	if p == nil {
		return json.Marshal(map[string]any{"component": "producer", "type": "log", "nil": true})
	}
	return json.Marshal(map[string]any{
		"component": "producer",
		"type":      "log",
		"name":      p.Name(),
		"pod":       p.podName,
		"namespace": p.namespace,
		"topic":     p.inputName,
	})
}

// Start streams pod logs and publishes one event per line. It reconnects with
// exponential backoff on stream errors. Returns nil when ctx is cancelled.
func (p *LogProducer) Start(ctx context.Context) error {
	backoff := time.Second
	const maxBackoff = 30 * time.Second

	for {
		if err := p.stream(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			p.HandleError(fmt.Errorf("log stream: %w", err))
		} else if ctx.Err() != nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

// newLogProducerWithStream creates a LogProducer with a custom stream opener.
// Used in tests to inject a prepared ReadCloser without a real kubelet.
func newLogProducerWithStream(cfg LogConfig, fn func(ctx context.Context) (io.ReadCloser, error)) (*LogProducer, error) {
	p, err := NewLogProducer(cfg)
	if err != nil {
		return nil, err
	}
	p.streamFunc = fn
	return p, nil
}

// stream opens one log stream and reads until EOF, error, or ctx cancellation.
func (p *LogProducer) stream(ctx context.Context) error {
	var rc io.ReadCloser
	var err error

	if p.streamFunc != nil {
		rc, err = p.streamFunc(ctx)
	} else {
		opts := &corev1.PodLogOptions{
			Follow:    true,
			Container: p.container,
		}
		rc, err = p.client.CoreV1().Pods(p.namespace).GetLogs(p.podName, opts).Stream(ctx)
	}
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer rc.Close()

	scanner := bufio.NewScanner(rc)
	for scanner.Scan() {
		line := scanner.Text()
		doc := dbspunstructured.New(map[string]any{"line": line}, nil)
		z := zset.New()
		z.Insert(doc, 1)
		if err := p.Publish(dbspruntime.Event{Name: p.inputName, Data: z}); err != nil {
			p.HandleError(fmt.Errorf("publish: %w", err))
		}
	}
	return scanner.Err()
}
