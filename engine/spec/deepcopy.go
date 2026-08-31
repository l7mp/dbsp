package spec

import (
	"encoding/json"

	"github.com/l7mp/dbsp/engine/transform"
)

// Hand-written deepcopy methods (the kubebuilder object generator does not
// reach into external packages): json.RawMessage fields are byte-copied.

func copyRaw(in *json.RawMessage) *json.RawMessage {
	if in == nil {
		return nil
	}
	cp := json.RawMessage(append([]byte{}, *in...))
	return &cp
}

func copyString(in *string) *string {
	if in == nil {
		return nil
	}
	cp := *in
	return &cp
}

// DeepCopyInto copies the receiver into out.
func (in *Resource) DeepCopyInto(out *Resource) {
	out.Group = copyString(in.Group)
	out.Version = copyString(in.Version)
	out.Kind = in.Kind
}

// DeepCopy returns a deep copy.
func (in *Resource) DeepCopy() *Resource {
	if in == nil {
		return nil
	}
	out := new(Resource)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out.
func (in *Source) DeepCopyInto(out *Source) {
	in.Resource.DeepCopyInto(&out.Resource)
	out.Type = in.Type
	out.Level = in.Level
	out.Namespace = copyString(in.Namespace)
	out.LabelSelector = copyRaw(in.LabelSelector)
	out.Predicate = copyRaw(in.Predicate)
	out.Parameters = copyRaw(in.Parameters)
}

// DeepCopy returns a deep copy.
func (in *Source) DeepCopy() *Source {
	if in == nil {
		return nil
	}
	out := new(Source)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out.
func (in *Target) DeepCopyInto(out *Target) {
	in.Resource.DeepCopyInto(&out.Resource)
	out.Type = in.Type
	out.Level = in.Level
	out.Parameters = copyRaw(in.Parameters)
}

// DeepCopy returns a deep copy.
func (in *Target) DeepCopy() *Target {
	if in == nil {
		return nil
	}
	out := new(Target)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out.
func (in *Controller) DeepCopyInto(out *Controller) {
	out.Name = in.Name
	out.Sources = nil
	for i := range in.Sources {
		out.Sources = append(out.Sources, *in.Sources[i].DeepCopy())
	}
	out.Pipeline = copyRaw(in.Pipeline)
	out.SQL = copyRaw(in.SQL)
	out.Circuit = copyRaw(in.Circuit)
	out.Targets = nil
	for i := range in.Targets {
		out.Targets = append(out.Targets, *in.Targets[i].DeepCopy())
	}
	out.Transforms = nil
	for _, t := range in.Transforms {
		cp := transform.TransformSpec{Name: t.Name, K: t.K}
		for _, p := range t.Pairs {
			cp.Pairs = append(cp.Pairs, append([]string{}, p...))
		}
		if t.Key != nil {
			cp.Key = append(json.RawMessage{}, t.Key...)
		}
		out.Transforms = append(out.Transforms, cp)
	}
}

// DeepCopy returns a deep copy.
func (in *Controller) DeepCopy() *Controller {
	if in == nil {
		return nil
	}
	out := new(Controller)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out.
func (in *OperatorSpec) DeepCopyInto(out *OperatorSpec) {
	out.Controllers = nil
	for i := range in.Controllers {
		out.Controllers = append(out.Controllers, *in.Controllers[i].DeepCopy())
	}
}

// DeepCopy returns a deep copy.
func (in *OperatorSpec) DeepCopy() *OperatorSpec {
	if in == nil {
		return nil
	}
	out := new(OperatorSpec)
	in.DeepCopyInto(out)
	return out
}
