package consumer

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/l7mp/dbsp/dbsp/datamodel"
	dbunstructured "github.com/l7mp/dbsp/dbsp/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/dbsp/runtime"
	"github.com/l7mp/dbsp/dbsp/zset"
)

// Config configures a Kubernetes runtime consumer.
type Config struct {
	Client client.Client

	OutputName string
	TargetGVK  *runtime.GroupVersionKind

	Logger logr.Logger
}

// Consumer applies DBSP output deltas to Kubernetes via composite client writes.
type Consumer struct {
	client client.Client

	outputName string
	targetGVK  *runtime.GroupVersionKind

	log logr.Logger
}

var _ dbspruntime.Consumer = (*Consumer)(nil)

// New creates a Kubernetes consumer.
func New(cfg Config) (*Consumer, error) {
	if cfg.Client == nil {
		return nil, fmt.Errorf("consumer: nil client")
	}

	log := cfg.Logger
	if log.GetSink() == nil {
		log = logr.Discard()
	}

	return &Consumer{
		client:     cfg.Client,
		outputName: cfg.OutputName,
		targetGVK:  cfg.TargetGVK,
		log:        log.WithName("kubernetes-consumer").WithValues("output", cfg.OutputName),
	}, nil
}

// Start starts background consumer activity.
func (c *Consumer) Start(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

// Consume applies output Z-set deltas to Kubernetes objects.
func (c *Consumer) Consume(ctx context.Context, out dbspruntime.Output) error {
	if c.outputName != "" && out.Name != c.outputName {
		return nil
	}

	entries := out.Data.Entries()
	for i := range entries {
		e := entries[i]
		if err := c.applyEntry(ctx, e); err != nil {
			return err
		}
	}

	return nil
}

func (c *Consumer) applyEntry(ctx context.Context, e zset.Elem) error {
	u, err := toUnstructured(e.Document)
	if err != nil {
		return err
	}
	if c.targetGVK != nil {
		u.SetGroupVersionKind(*c.targetGVK)
	}

	if e.Weight < 0 {
		return c.client.Delete(ctx, u)
	}

	key := client.ObjectKeyFromObject(u)
	current := &unstructured.Unstructured{}
	current.SetGroupVersionKind(u.GroupVersionKind())
	current.SetNamespace(key.Namespace)
	current.SetName(key.Name)

	if err := c.client.Get(ctx, key, current); err != nil {
		return c.client.Create(ctx, u)
	}

	u.SetResourceVersion(current.GetResourceVersion())
	return c.client.Update(ctx, u)
}

func toUnstructured(doc datamodel.Document) (*unstructured.Unstructured, error) {
	udoc, ok := doc.(*dbunstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("consumer: unsupported document type %T", doc)
	}

	b, err := udoc.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("consumer: marshal document: %w", err)
	}

	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(b); err != nil {
		return nil, fmt.Errorf("consumer: decode document: %w", err)
	}

	return obj, nil
}
