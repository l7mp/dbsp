package dbsp

import "time"

// NowOp implements @now - returns the current UTC timestamp.
type NowOp struct{}

func (o *NowOp) Name() string { return "@now" }

func (o *NowOp) Evaluate(ctx *Context, args Args) (any, error) {
	result := time.Now().UTC().Format(time.RFC3339)
	ctx.Logger().V(8).Info("eval", "op", o.Name(), "result", result)
	return result, nil
}

func init() {
	MustRegister("@now", func() Operator { return &NowOp{} })
}
