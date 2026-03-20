package runtime

// Runtime combines a shared PubSub and a runnable Manager.
type Runtime struct {
	*PubSub
	Manager
}

// NewRuntime creates a Runtime.
func NewRuntime() *Runtime {
	return &Runtime{PubSub: NewPubSub(), Manager: NewManager()}
}
