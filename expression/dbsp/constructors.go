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

// NewArg creates an @arg expression.
func NewArg() Expression { return &argExpr{nullaryOp{"@arg"}} }

// NewSet creates a field-set expression.
func NewSet(field, value Expression) Expression { return &setExpr{binaryOp{"@set", field, value}} }

// NewList creates a list expression.
func NewList(elems ...Expression) Expression { return &listExpr{elements: elems} }

// NewDict creates a dict expression.
func NewDict(entries map[string]Expression) Expression { return &dictExpr{entries: entries} }

// NewAdd creates an addition expression.
func NewAdd(args ...Expression) Expression { return &addExpr{variadicOp{"@add", args}} }

// NewSub creates a subtraction expression.
func NewSub(left, right Expression) Expression { return &subExpr{binaryOp{"@sub", left, right}} }

// NewMul creates a multiplication expression.
func NewMul(args ...Expression) Expression { return &mulExpr{variadicOp{"@mul", args}} }

// NewDiv creates a division expression.
func NewDiv(left, right Expression) Expression { return &divExpr{binaryOp{"@div", left, right}} }

// NewMod creates a modulo expression.
func NewMod(left, right Expression) Expression { return &modExpr{binaryOp{"@mod", left, right}} }

// NewNeg creates a negation expression.
func NewNeg(operand Expression) Expression { return &negExpr{unaryOp{"@neg", operand}} }

// NewEq creates an equality expression.
func NewEq(left, right Expression) Expression { return &eqExpr{binaryOp{"@eq", left, right}} }

// NewNeq creates a not-equal expression.
func NewNeq(left, right Expression) Expression { return &neqExpr{binaryOp{"@neq", left, right}} }

// NewGt creates a greater-than expression.
func NewGt(left, right Expression) Expression { return &gtExpr{binaryOp{"@gt", left, right}} }

// NewGte creates a greater-than-or-equal expression.
func NewGte(left, right Expression) Expression { return &gteExpr{binaryOp{"@gte", left, right}} }

// NewLt creates a less-than expression.
func NewLt(left, right Expression) Expression { return &ltExpr{binaryOp{"@lt", left, right}} }

// NewLte creates a less-than-or-equal expression.
func NewLte(left, right Expression) Expression { return &lteExpr{binaryOp{"@lte", left, right}} }

// NewAnd creates a logical AND expression.
func NewAnd(args ...Expression) Expression { return &andExpr{variadicOp{"@and", args}} }

// NewOr creates a logical OR expression.
func NewOr(args ...Expression) Expression { return &orExpr{variadicOp{"@or", args}} }

// NewNot creates a logical NOT expression.
func NewNot(operand Expression) Expression { return &notExpr{unaryOp{"@not", operand}} }

// NewSum creates a sum expression.
func NewSum(args ...Expression) Expression { return &sumExpr{variadicOp{"@sum", args}} }

// NewLexMin creates a lexicographic minimum expression.
func NewLexMin(args ...Expression) Expression { return &lexMinExpr{variadicOp{"@lexmin", args}} }

// NewLexMax creates a lexicographic maximum expression.
func NewLexMax(args ...Expression) Expression { return &lexMaxExpr{variadicOp{"@lexmax", args}} }

// NewLen creates a list length expression.
func NewLen(operand Expression) Expression { return &lenExpr{unaryOp{"@len", operand}} }

// NewIsNull creates an is-null check expression.
func NewIsNull(operand Expression) Expression { return &isNullExpr{unaryOp{"@isnull", operand}} }

// NewCond creates a conditional (if-then-else) expression.
func NewCond(cond, ifTrue, ifFalse Expression) Expression {
	return &condExpr{cond: cond, ifTrue: ifTrue, ifFalse: ifFalse}
}

// NewSqlBool creates a SQL bool normalization expression.
func NewSqlBool(operand Expression) Expression { return &sqlBoolExpr{unaryOp{"@sqlbool", operand}} }
