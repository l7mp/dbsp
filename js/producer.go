package main

import (
	"fmt"

	"github.com/dop251/goja"
)

func (v *VM) jsonlProducer(call goja.FunctionCall) (goja.Value, error) {
	return nil, fmt.Errorf("producer.jsonl is not implemented yet")
}
