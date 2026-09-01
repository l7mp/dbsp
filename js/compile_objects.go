package js

import (
	"github.com/dop251/goja"
)

// makeCompileObjects builds the sql, aggregate and circuit binding
// objects scoped to this runtime: every handle they produce installs
// into it on commit. The globals are the default runtime's objects; a
// created runtime's handle carries its own set.
func (inst *runtimeInstance) makeCompileObjects() (*goja.Object, *goja.Object, *goja.Object, error) {
	v := inst.vm
	rt := inst.rt
	sqlObj := v.rt.NewObject()
	if err := sqlObj.Set("table", v.wrap(v.sqlTable)); err != nil {
		return nil, nil, nil, err
	}
	if err := sqlObj.Set("compile", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.sqlCompile(rt, call)
	})); err != nil {
		return nil, nil, nil, err
	}

	aggObj := v.rt.NewObject()
	if err := aggObj.Set("compile", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.aggregateCompile(rt, call)
	})); err != nil {
		return nil, nil, nil, err
	}

	circuitObj := v.rt.NewObject()
	if err := circuitObj.Set("create", v.wrap(func(call goja.FunctionCall) (goja.Value, error) {
		return v.circuitCreate(rt, call)
	})); err != nil {
		return nil, nil, nil, err
	}

	return sqlObj, aggObj, circuitObj, nil
}
