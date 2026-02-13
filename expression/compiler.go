package expression

// Compiler compiles expression source into an executable Expression.
type Compiler interface {
	// Compile parses source bytes and returns an executable Expression.
	Compile(source []byte) (Expression, error)

	// CompileString is a convenience wrapper for string input.
	CompileString(source string) (Expression, error)
}
