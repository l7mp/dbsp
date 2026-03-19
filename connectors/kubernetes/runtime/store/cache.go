package store

// Composite store is a store that serves views from the view store and the rest from the default
// Kubernetes store.

import (
	"context"

	"k8s.io/apimachinery/pkg/watch"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/l7mp/connectors/kubernetes/runtime/object"
)

// Re-export controller-runtime types for convenience.
// Users can use manager.Options and manager.Manager without importing controller-runtime.
type (
	Options      = ctrlcache.Options
	Cache        = ctrlcache.Cache
	NewCacheFunc = ctrlcache.NewCacheFunc
)

// ViewCacheInterface extends store.Cache with view-specific operations.
// Both ViewCache and DelegatingViewCache implement this interface.
type ViewCacheInterface interface {
	Cache
	// GetClient returns a client for this view store.
	GetClient() client.WithWatch
	// Add adds an object to the store.
	Add(obj object.Object) error
	// Update updates an object in the store.
	Update(oldObj, newObj object.Object) error
	// Delete removes an object from the store.
	Delete(obj object.Object) error
	// Watch watches for changes to objects.
	Watch(ctx context.Context, list client.ObjectList, opts ...client.ListOption) (watch.Interface, error)
}
