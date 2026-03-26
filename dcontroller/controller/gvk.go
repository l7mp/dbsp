// Package controller wires a declarative DBSP pipeline from a Controller spec.
//
// It compiles the Pipeline spec into an incremental DBSP circuit via the
// aggregation compiler, creates producers for each source and consumers for
// each target, all connected through a shared runtime pub/sub bus.
package controller

import (
	"encoding/json"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"

	viewv1a1 "github.com/l7mp/dbsp/connectors/kubernetes/runtime/api/view/v1alpha1"
	opv1a1 "github.com/l7mp/dbsp/dcontroller/api/operator/v1alpha1"
)

// resolveGVK resolves the full GVK for a resource following the three-way rule:
//  1. Group nil or equal to the view group of the operator → view GVK with enforced version.
//  2. Group set, Version nil → standard Kubernetes resource resolved via REST mapper.
//  3. Group and Version both set → explicit GVK, used as-is.
//
// All source types (Watcher, OneShot, Periodic) and targets use the same resolution.
func resolveGVK(operatorName string, r opv1a1.Resource, mapper meta.RESTMapper) (schema.GroupVersionKind, error) {
	if r.Kind == "" {
		return schema.GroupVersionKind{}, fmt.Errorf("resource Kind is required")
	}

	viewGroup := viewv1a1.Group(operatorName)

	if r.Group == nil || *r.Group == viewGroup {
		// View resource: version is always enforced to viewv1a1.Version.
		return schema.GroupVersionKind{
			Group:   viewGroup,
			Version: viewv1a1.Version,
			Kind:    r.Kind,
		}, nil
	}

	if r.Version == nil {
		// Standard Kubernetes resource: resolve the version via the REST mapper.
		if mapper == nil {
			return schema.GroupVersionKind{},
				fmt.Errorf("REST mapper required to resolve GVK for group %q kind %q", *r.Group, r.Kind)
		}
		gvk, err := mapper.KindFor(schema.GroupVersionResource{Group: *r.Group, Resource: r.Kind})
		if err != nil {
			return schema.GroupVersionKind{},
				fmt.Errorf("cannot find GVK for %s/%s: %w", *r.Group, r.Kind, err)
		}
		return gvk, nil
	}

	// Explicit GVK: caller provided both Group and Version.
	return schema.GroupVersionKind{
		Group:   *r.Group,
		Version: *r.Version,
		Kind:    r.Kind,
	}, nil
}

// sourceGVK resolves the full GVK for a source. All source types (Watcher, OneShot,
// Periodic) use the same GVK resolution — there is no special "trigger" GVK.
func sourceGVK(operatorName string, s opv1a1.Source, mapper meta.RESTMapper) (schema.GroupVersionKind, error) {
	return resolveGVK(operatorName, s.Resource, mapper)
}

// targetGVK resolves the full GVK for a target.
func targetGVK(operatorName string, t opv1a1.Target, mapper meta.RESTMapper) (schema.GroupVersionKind, error) {
	return resolveGVK(operatorName, t.Resource, mapper)
}

// periodicParams holds the period field parsed from source parameters JSON.
type periodicParams struct {
	Period string `json:"period"`
}

// parsePeriod parses the period from source parameters JSON.
// Returns 0 and nil error if parameters are absent or the period field is empty.
func parsePeriod(s opv1a1.Source) (time.Duration, error) {
	if s.Parameters == nil {
		return 0, nil
	}

	var params periodicParams
	if err := json.Unmarshal(s.Parameters.Raw, &params); err != nil {
		return 0, err
	}

	if params.Period == "" {
		return 0, nil
	}

	return time.ParseDuration(params.Period)
}
