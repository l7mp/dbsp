package dbsp

// Typed constructors for building expression trees programmatically.

// NewNil creates a nil literal expression.
func NewNil() Expression { return &nilExpr{} }

// NewConst creates a constant value expression.
func NewConst(v any) Expression { return &constExpr{value: v} }

// NewBool creates a boolean literal expression.
func NewBool(v bool) Expression { return &boolExpr{operand: &constExpr{value: v}} }

// NewInt creates an integer literal expression.
func NewInt(v int64) Expression { return &intExpr{operand: &constExpr{value: v}} }

// NewFloat creates a float literal expression.
func NewFloat(v float64) Expression { return &floatExpr{operand: &constExpr{value: v}} }

// NewString creates a string literal expression.
func NewString(v string) Expression { return &stringExpr{operand: &constExpr{value: v}} }

// NewGet creates a field-get expression from a literal field name.
func NewGet(field string) Expression { return &getExpr{field: &constExpr{value: field}} }

// NewSet creates a field-set expression.
func NewSet(field, value Expression) Expression { return &setExpr{field: field, value: value} }

// NewList creates a list expression.
func NewList(elems ...Expression) Expression { return &listExpr{elements: elems} }

// NewDict creates a dict expression.
func NewDict(entries map[string]Expression) Expression { return &dictExpr{entries: entries} }

// NewAdd creates an addition expression.
func NewAdd(args ...Expression) Expression { return &addExpr{args: args} }

// NewSub creates a subtraction expression.
func NewSub(left, right Expression) Expression { return &subExpr{left: left, right: right} }

// NewMul creates a multiplication expression.
func NewMul(args ...Expression) Expression { return &mulExpr{args: args} }

// NewDiv creates a division expression.
func NewDiv(left, right Expression) Expression { return &divExpr{left: left, right: right} }

// NewMod creates a modulo expression.
func NewMod(left, right Expression) Expression { return &modExpr{left: left, right: right} }

// NewNeg creates a negation expression.
func NewNeg(operand Expression) Expression { return &negExpr{operand: operand} }

// NewEq creates an equality expression.
func NewEq(left, right Expression) Expression { return &eqExpr{left: left, right: right} }

// NewNeq creates a not-equal expression.
func NewNeq(left, right Expression) Expression { return &neqExpr{left: left, right: right} }

// NewGt creates a greater-than expression.
func NewGt(left, right Expression) Expression { return &gtExpr{left: left, right: right} }

// NewGte creates a greater-than-or-equal expression.
func NewGte(left, right Expression) Expression { return &gteExpr{left: left, right: right} }

// NewLt creates a less-than expression.
func NewLt(left, right Expression) Expression { return &ltExpr{left: left, right: right} }

// NewLte creates a less-than-or-equal expression.
func NewLte(left, right Expression) Expression { return &lteExpr{left: left, right: right} }

// NewAnd creates a logical AND expression.
func NewAnd(args ...Expression) Expression { return &andExpr{args: args} }

// NewOr creates a logical OR expression.
func NewOr(args ...Expression) Expression { return &orExpr{args: args} }

// NewNot creates a logical NOT expression.
func NewNot(operand Expression) Expression { return &notExpr{operand: operand} }

// NewIsNull creates an is-null check expression.
func NewIsNull(operand Expression) Expression { return &isNullExpr{operand: operand} }

// NewCond creates a conditional (if-then-else) expression.
func NewCond(cond, ifTrue, ifFalse Expression) Expression {
	return &condExpr{cond: cond, ifTrue: ifTrue, ifFalse: ifFalse}
}

// NewSqlBool creates a SQL bool normalization expression.
func NewSqlBool(operand Expression) Expression { return &sqlBoolExpr{operand: operand} }
