package transform

import (
	"encoding/json"

	"github.com/l7mp/dbsp/engine/operator"
	"github.com/l7mp/dbsp/engine/zset"
)

// testNonLinearOp is a minimal unary non-linear operator for transform tests.
// It behaves like identity at runtime, but reports NonLinear linearity so
// incrementalization uses the generic D ∘ O ∘ I pattern.
type testNonLinearOp struct{}

func newTestNonLinearOp() operator.Operator {
	return &testNonLinearOp{}
}

func (o *testNonLinearOp) Kind() operator.Kind {
	return operator.KindNoOp
}

func (o *testNonLinearOp) String() string {
	return "test-nonlinear"
}

func (o *testNonLinearOp) Arity() int {
	return 1
}

func (o *testNonLinearOp) Linearity() operator.Linearity {
	return operator.NonLinear
}

func (o *testNonLinearOp) Set(_ zset.ZSet) {}

func (o *testNonLinearOp) Apply(_ *operator.ExecContext, inputs ...zset.ZSet) (zset.ZSet, error) {
	if len(inputs) == 0 {
		return zset.New(), nil
	}
	return inputs[0], nil
}

func (o *testNonLinearOp) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"kind": "test-nonlinear"})
}

func (o *testNonLinearOp) UnmarshalJSON(_ []byte) error {
	return nil
}
