package misc

import (
	"encoding/json"
	"fmt"

	"github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/spec"
)

// Group is the API group routing bindings to the misc connector.
const Group = "misc.connector.dcontroller.io"

// NewFactory returns the misc connector's binding factory: trigger
// sources (kind Timer, verbs Tick and Init) and no targets.
func NewFactory() runtime.ConnectorFactory {
	return runtime.ConnectorFactory{
		Name:   "misc",
		Groups: []string{Group},
		NewSource: func(rt *runtime.Runtime, s *spec.Source, topic string) (runtime.Runnable, error) {
			var ts TriggerSpec
			if s.Parameters != nil {
				if err := json.Unmarshal(*s.Parameters, &ts); err != nil {
					return nil, fmt.Errorf("parameters: %w", err)
				}
			}
			ts.Kind = s.Kind
			if s.Namespace != nil {
				ts.Namespace = *s.Namespace
			}
			deps := Deps{Runtime: rt, Logger: rt.Logger()}
			switch s.Type {
			case "", spec.Watcher, spec.Tick:
				return NewTick(topic, ts, deps)
			case spec.Init:
				return NewInit(topic, ts, deps)
			default:
				return nil, fmt.Errorf("unknown misc source type %q", s.Type)
			}
		},
	}
}
