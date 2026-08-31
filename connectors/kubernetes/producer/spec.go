package producer

import (
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kpredicate "github.com/l7mp/dbsp/connectors/kubernetes/runtime/predicate"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// Spec is the serialized configuration of the ingest producers, the one
// wire form every frontend funnels through (the JavaScript
// kubernetes.watch/list options object and a serialized controller's
// source fields deserialize into it alike). Labels is the matchLabels
// shorthand; LabelSelector carries the full selector, and the two merge.
type Spec struct {
	GVK           string                `json:"gvk"`
	Namespace     string                `json:"namespace,omitempty"`
	Labels        map[string]string     `json:"labels,omitempty"`
	LabelSelector *v1.LabelSelector     `json:"labelSelector,omitempty"`
	Predicate     *kpredicate.Predicate `json:"predicate,omitempty"`
	// Level switches from delta ingest to level ingest: every event
	// carries the full snapshot instead of the increment.
	Level bool `json:"level,omitempty"`
}

// Selector folds the matchLabels shorthand into the full selector.
func (s Spec) Selector() *v1.LabelSelector {
	sel := s.LabelSelector
	if len(s.Labels) == 0 {
		return sel
	}
	if sel == nil {
		sel = &v1.LabelSelector{}
	} else {
		sel = sel.DeepCopy()
	}
	if sel.MatchLabels == nil {
		sel.MatchLabels = map[string]string{}
	}
	for k, val := range s.Labels {
		sel.MatchLabels[k] = val
	}
	return sel
}

// Deps carries the non-serializable dependencies of a producer: the
// connector client and the GVK resolved from Spec.GVK by the caller's
// runtime (resolution needs the REST mapper and view discovery).
type Deps struct {
	Client  client.WithWatch
	GVK     schema.GroupVersionKind
	Runtime *dbspruntime.Runtime
	Logger  logr.Logger
}

// NewFromSpec builds the ingest producer publishing to topic: a Watcher,
// or a Lister with level: true.
func NewFromSpec(topic string, spec Spec, deps Deps) (dbspruntime.Runnable, error) {
	producerKind := "watcher"
	if spec.Level {
		producerKind = "lister"
	}
	cfg := Config{
		Client:        deps.Client,
		SourceGVK:     deps.GVK,
		Name:          fmt.Sprintf("kubernetes-producer-%s-%s-%s", producerKind, topic, strings.ToLower(deps.GVK.String())),
		InputName:     topic,
		Namespace:     spec.Namespace,
		LabelSelector: spec.Selector(),
		Predicate:     spec.Predicate,
		Runtime:       deps.Runtime,
		Logger:        deps.Logger,
	}
	if spec.Level {
		return NewLister(cfg)
	}
	return NewWatcher(cfg)
}
