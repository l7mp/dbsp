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
	out.As = in.As
	out.Type = in.Type
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
	out.As = in.As
	out.Type = in.Type
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
func (in *CircuitSpec) DeepCopyInto(out *CircuitSpec) {
	out.Name = in.Name
	out.Inputs = append([]string(nil), in.Inputs...)
	out.Outputs = append([]string(nil), in.Outputs...)
	out.Pipeline = copyRaw(in.Pipeline)
	out.SQL = copyRaw(in.SQL)
	out.Graph = copyRaw(in.Graph)
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
func (in *CircuitSpec) DeepCopy() *CircuitSpec {
	if in == nil {
		return nil
	}
	out := new(CircuitSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the receiver into out.
func (in *RuntimeSpec) DeepCopyInto(out *RuntimeSpec) {
	out.Sources = nil
	for i := range in.Sources {
		out.Sources = append(out.Sources, *in.Sources[i].DeepCopy())
	}
	out.Circuits = nil
	for i := range in.Circuits {
		out.Circuits = append(out.Circuits, *in.Circuits[i].DeepCopy())
	}
	out.Targets = nil
	for i := range in.Targets {
		out.Targets = append(out.Targets, *in.Targets[i].DeepCopy())
	}
}

// DeepCopy returns a deep copy.
func (in *RuntimeSpec) DeepCopy() *RuntimeSpec {
	if in == nil {
		return nil
	}
	out := new(RuntimeSpec)
	in.DeepCopyInto(out)
	return out
}
