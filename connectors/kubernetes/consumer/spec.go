package consumer

import (
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// Spec is the serialized configuration of the write-side consumers, the
// one wire form every frontend funnels through.
type Spec struct {
	GVK string `json:"gvk"`
}

// Deps carries the non-serializable dependencies of a consumer: the
// connector client and the GVK resolved from Spec.GVK by the caller's
// runtime.
type Deps struct {
	Client  client.Client
	GVK     schema.GroupVersionKind
	Runtime *dbspruntime.Runtime
	Logger  logr.Logger
}

// NewFromSpec builds the consumer subscribed to topic for the given verb
// ("update" or "patch"): a Patcher or an Updater.
func NewFromSpec(topic, verb string, spec Spec, deps Deps) (dbspruntime.Runnable, error) {
	consumerKind := verb + "r"
	if verb == "patch" {
		consumerKind = "patcher"
	}
	cfg := Config{
		Client:     deps.Client,
		Name:       fmt.Sprintf("kubernetes-consumer-%s-%s-%s", consumerKind, topic, strings.ToLower(deps.GVK.String())),
		OutputName: topic,
		TargetGVK:  deps.GVK,
		Runtime:    deps.Runtime,
		Logger:     deps.Logger,
	}
	switch verb {
	case "patch":
		return NewPatcher(cfg)
	case "update":
		return NewUpdater(cfg)
	default:
		return nil, fmt.Errorf("unknown consumer verb %q", verb)
	}
}
