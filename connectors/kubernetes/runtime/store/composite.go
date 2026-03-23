package store

import (
	"context"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
)

var _ ctrlcache.Cache = &CompositeCache{}

// CompositeCache is a store for storing view objects. It delegates native objects to a default
// store.
type CompositeCache struct {
	defaultCache Cache
	viewCache    ViewCacheInterface
	logger, log  logr.Logger
}

// CacheOptions are generic caching options.
type CacheOptions struct {
	ctrlcache.Options
	// DefaultCache is the controller-runtime store used for anything that is not a view.
	DefaultCache Cache
	// ViewCache is the view store used for anything that is a view.
	ViewCache Cache
	// Logger is for logging. Currently only the viewcache generates log messages.
	Logger logr.Logger
}

// NewCompositeCache creates a new composite store. If the config is not nil it also creates a
// controller-runtime store for storing native resources.
func NewCompositeCache(config *rest.Config, opts CacheOptions) (*CompositeCache, error) {
	logger := opts.Logger
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}

	defaultCache := opts.DefaultCache
	if defaultCache == nil && config != nil {
		dc, err := ctrlcache.New(config, opts.Options)
		if err != nil {
			return nil, err
		}
		defaultCache = dc
	}

	var viewCache ViewCacheInterface
	if opts.ViewCache != nil {
		// Use the provided view store (can be ViewCache or DelegatingViewCache)
		if vc, ok := opts.ViewCache.(ViewCacheInterface); ok {
			viewCache = vc
		} else {
			// Fallback: create a new ViewCache if provided store doesn't implement the interface
			viewCache = NewViewCache(opts)
		}
	} else {
		// Create a new ViewCache if none provided
		viewCache = NewViewCache(opts)
	}

	return &CompositeCache{
		defaultCache: defaultCache,
		viewCache:    viewCache,
		logger:       logger,
		log:          logger.WithName("cache"),
	}, nil
}

// GetLogger returns the logger of the store.
func (cc *CompositeCache) GetLogger() logr.Logger {
	return cc.logger
}

// GetDefaultCache returns the store used for storing native objects.
func (cc *CompositeCache) GetDefaultCache() Cache {
	return cc.defaultCache
}

// GetViewCache returns the store used for storing view objects.
func (cc *CompositeCache) GetViewCache() ViewCacheInterface {
	return cc.viewCache
}

// NewClient creates a composite client bound to this cache.
func (cc *CompositeCache) NewClient(config *rest.Config, options ClientOptions) (*CompositeClient, error) {
	return NewCompositeClient(config, cc, options)
}

// GetInformer fetches or constructs an informer for the given object.
func (cc *CompositeCache) GetInformer(ctx context.Context, obj client.Object, opts ...ctrlcache.InformerGetOption) (ctrlcache.Informer, error) {
	gvk := obj.GetObjectKind().GroupVersionKind()

	cc.log.V(6).Info("get-informer", "gvk", gvk)

	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.GetInformer(ctx, obj)
	}
	return cc.defaultCache.GetInformer(ctx, obj)
}

// GetInformerForKind is similar to GetInformer, except that it takes a group-version-kind instead
// of the underlying object.
func (cc *CompositeCache) GetInformerForKind(ctx context.Context, gvk schema.GroupVersionKind, opts ...ctrlcache.InformerGetOption) (ctrlcache.Informer, error) {
	cc.log.V(6).Info("get-informer-for-kind", "gvk", gvk)

	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.GetInformerForKind(ctx, gvk)
	}
	return cc.defaultCache.GetInformerForKind(ctx, gvk)
}

// RemoveInformer removes an informer entry and stops it if it was running.
func (cc *CompositeCache) RemoveInformer(ctx context.Context, obj client.Object) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	cc.log.V(6).Info("remove-informer", "gvk", gvk)

	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.RemoveInformer(ctx, obj)
	}
	return cc.defaultCache.RemoveInformer(ctx, obj)
}

// Start runs all the informers known to this store until the context is closed. It blocks.
func (cc *CompositeCache) Start(ctx context.Context) error {
	cc.log.V(1).Info("starting")

	// we must run this in a goroutine, otherwise the default store cannot start up
	// ignore the returned error: viewcache.Start() never errs
	if cc.defaultCache != nil {
		go cc.defaultCache.Start(ctx) //nolint:errcheck
	}

	return cc.viewCache.Start(ctx)
}

// WaitForCacheSync waits for all the caches to sync. Returns false if it could not sync a store.c
func (cc *CompositeCache) WaitForCacheSync(ctx context.Context) bool {
	return cc.viewCache.WaitForCacheSync(ctx) && cc.defaultCache.WaitForCacheSync(ctx)
}

// IndexField adds an index with the given field name on the given object type.
func (cc *CompositeCache) IndexField(ctx context.Context, obj client.Object, field string, extractValue client.IndexerFunc) error {
	gvk := obj.GetObjectKind().GroupVersionKind()
	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.IndexField(ctx, obj, field, extractValue)
	}
	return cc.defaultCache.IndexField(ctx, obj, field, extractValue)
}

// Get retrieves an obj for the given object key from the store.
func (cc *CompositeCache) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	gvk := obj.GetObjectKind().GroupVersionKind()

	cc.log.V(5).Info("get", "gvk", gvk, "key", key)

	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.Get(ctx, key, obj, opts...)
	}
	return cc.defaultCache.Get(ctx, key, obj, opts...)
}

// List retrieves list of objects for a given namespace and list options.
func (cc *CompositeCache) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	gvk := list.GetObjectKind().GroupVersionKind()

	cc.log.V(5).Info("list", "gvk", gvk)

	if viewv1a1.IsViewKind(gvk) {
		return cc.viewCache.List(ctx, list, opts...)
	}
	return cc.defaultCache.List(ctx, list, opts...)
}
