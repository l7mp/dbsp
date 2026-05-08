package js

import (
	"fmt"

	"github.com/dop251/goja"

	k8sproducer "github.com/l7mp/dbsp/connectors/kubernetes/producer"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

type k8sLogOptions struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	Container string `json:"container"`
}

// k8sLog implements kubernetes.log("topic", {name, namespace, container}[, callback]).
// It starts a LogProducer that tails pod logs and publishes one {"line": "..."}
// entry per log line.  The optional callback has producer semantics: its return
// value is published to topic; returning nothing publishes an empty Z-set.
func (v *VM) k8sLog(call goja.FunctionCall) (goja.Value, error) {
	if len(call.Arguments) < 2 {
		return nil, fmt.Errorf("kubernetes.log(topic, {name, namespace, container}[, callback]) requires topic and options")
	}

	topic := call.Argument(0).String()
	if topic == "" {
		return nil, fmt.Errorf("kubernetes.log: empty topic")
	}

	var opts k8sLogOptions
	if err := decodeOptionValue(call.Argument(1), &opts); err != nil {
		return nil, fmt.Errorf("kubernetes.log options: %w", err)
	}
	if opts.Name == "" {
		return nil, fmt.Errorf("kubernetes.log: missing name")
	}

	var callback goja.Callable
	if len(call.Arguments) > 2 {
		arg := call.Argument(2)
		if !goja.IsUndefined(arg) && !goja.IsNull(arg) {
			cb, ok := goja.AssertFunction(arg)
			if !ok {
				return nil, fmt.Errorf("kubernetes.log: callback must be a function")
			}
			callback = cb
		}
	}

	krt, err := v.ensureK8sRuntime()
	if err != nil {
		return nil, fmt.Errorf("kubernetes.log: %w", err)
	}
	if !v.k8sNativeAvailable {
		return nil, fmt.Errorf("kubernetes.log: native Kubernetes unavailable (kubeconfig not found)")
	}

	restCfg := krt.GetRESTConfig()
	clientset, err := newKubernetesClientset(restCfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes.log: create clientset: %w", err)
	}

	ns := opts.Namespace
	if ns == "" {
		ns = "default"
	}

	publishTopic := topic
	if callback != nil {
		publishTopic = v.nextInternalTopic("kubernetes-log", topic)
		v.registerProducerCallback(publishTopic, topic, "kubernetes-log-callback", callback)
	}

	compName := fmt.Sprintf("kubernetes-log-%s-%s", ns, opts.Name)
	pub := v.runtime.NewPublisher()

	p, err := k8sproducer.NewLogProducer(k8sproducer.LogConfig{
		Client:    clientset,
		Name:      compName,
		PodName:   opts.Name,
		Namespace: ns,
		Container: opts.Container,
		InputName: publishTopic,
		Runtime:   v.runtime,
		Logger:    v.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("kubernetes.log: %w", err)
	}

	p.SetPublisher(dbspruntime.PublishFunc(func(e dbspruntime.Event) error {
		e.Name = publishTopic
		return pub.Publish(e)
	}))

	if err := v.runtime.Add(p); err != nil {
		return nil, fmt.Errorf("kubernetes.log: register: %w", err)
	}

	return goja.Undefined(), nil
}
