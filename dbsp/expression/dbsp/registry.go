package dbsp

import (
	"fmt"
	"sync"
)

// ExpressionFactory creates an expression from parsed arguments.
// args is one of: nil, a raw value (bool/int64/float64/string),
// an Expression (single sub-expression), []Expression, or map[string]Expression.
type ExpressionFactory func(args any) (Expression, error)

// Registry manages operator registration.
type Registry struct {
	mu        sync.RWMutex
	operators map[string]ExpressionFactory
}

// DefaultRegistry is the global default registry with built-in operators.
var DefaultRegistry = NewRegistry()

// NewRegistry creates a new empty registry.
func NewRegistry() *Registry {
	return &Registry{
		operators: make(map[string]ExpressionFactory),
	}
}

// Register adds an operator factory to the registry.
// Returns an error if the operator already exists.
func (r *Registry) Register(name string, factory ExpressionFactory) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(name) == 0 || name[0] != '@' {
		return fmt.Errorf("operator name must start with @: %q", name)
	}
	if _, exists := r.operators[name]; exists {
		return fmt.Errorf("operator %q already registered", name)
	}
	r.operators[name] = factory
	return nil
}

// MustRegister registers an operator, panicking on error.
func (r *Registry) MustRegister(name string, factory ExpressionFactory) {
	if err := r.Register(name, factory); err != nil {
		panic(err)
	}
}

// Override replaces an existing operator or adds a new one.
// Unlike Register, this does not error if the operator already exists.
func (r *Registry) Override(name string, factory ExpressionFactory) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(name) == 0 || name[0] != '@' {
		return fmt.Errorf("operator name must start with @: %q", name)
	}
	r.operators[name] = factory
	return nil
}

// Get returns an operator factory by name.
func (r *Registry) Get(name string) (ExpressionFactory, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	factory, ok := r.operators[name]
	return factory, ok
}

// Clone creates a copy of the registry for isolated customization.
func (r *Registry) Clone() *Registry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	clone := NewRegistry()
	for name, factory := range r.operators {
		clone.operators[name] = factory
	}
	return clone
}

// Has checks if an operator is registered.
func (r *Registry) Has(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.operators[name]
	return ok
}

// Names returns all registered operator names.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.operators))
	for name := range r.operators {
		names = append(names, name)
	}
	return names
}

// Register registers an operator with the default registry.
func Register(name string, factory ExpressionFactory) error {
	return DefaultRegistry.Register(name, factory)
}

// MustRegister registers with the default registry, panicking on error.
func MustRegister(name string, factory ExpressionFactory) {
	DefaultRegistry.MustRegister(name, factory)
}

// Override replaces an operator in the default registry.
func Override(name string, factory ExpressionFactory) error {
	return DefaultRegistry.Override(name, factory)
}

// AsExprList extracts a []Expression from factory args.
func AsExprList(args any) ([]Expression, error) {
	if list, ok := args.([]Expression); ok {
		return list, nil
	}
	return nil, fmt.Errorf("expected []Expression, got %T", args)
}

// AsSingleExpr extracts a single Expression from factory args.
func AsSingleExpr(args any) (Expression, error) {
	if e, ok := args.(Expression); ok {
		return e, nil
	}
	return nil, fmt.Errorf("expected Expression, got %T", args)
}

// AsLiteral extracts a raw literal value from factory args.
func AsLiteral(args any) any {
	return args
}

// AsExprMap extracts a map[string]Expression from factory args.
func AsExprMap(args any) (map[string]Expression, error) {
	if m, ok := args.(map[string]Expression); ok {
		return m, nil
	}
	return nil, fmt.Errorf("expected map[string]Expression, got %T", args)
}
