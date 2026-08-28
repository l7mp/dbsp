package operator

import "github.com/l7mp/dbsp/engine/expression"

// ExpressionOperator is implemented by the operators that evaluate
// expressions. The Incrementalizer inspects them: an expression that reads
// something other than its inputs (a clock, a random source) has no
// incremental form, because the retraction of a row would be computed by a
// different function than its insertion.
type ExpressionOperator interface {
	Expressions() []expression.Expression
}

func (o *Select) Expressions() []expression.Expression  { return []expression.Expression{o.predicate} }
func (o *Project) Expressions() []expression.Expression { return []expression.Expression{o.projection} }
func (o *GroupBy) Expressions() []expression.Expression {
	return []expression.Expression{o.keyExpr, o.valueExpr}
}
func (o *GroupByIncremental) Expressions() []expression.Expression {
	return []expression.Expression{o.keyExpr, o.valueExpr}
}
func (o *EquiJoin) Expressions() []expression.Expression {
	return []expression.Expression{o.leftKey, o.rightKey}
}
func (o *EquiJoinH) Expressions() []expression.Expression {
	return []expression.Expression{o.leftKey, o.rightKey}
}
