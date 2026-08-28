package expression

// Flags carries per-expression properties. Every flag is a taint: an
// expression tree carries the OR of the flags of the operators it was built
// from, so a flag set by one operator holds for every tree containing it.
type Flags uint32

const (
	// TimeVariant marks an expression whose value depends on when it is
	// evaluated rather than on its inputs: a clock read, a random draw.
	// Such an expression is legitimate in a snapshot circuit, where every
	// step recomputes every value, and unsound in an incremental one,
	// where a row is retracted by recomputing the function that inserted
	// it: a different clock at retraction time produces a different row,
	// and the retraction never cancels the insertion.
	TimeVariant Flags = 1 << iota
)

// Flagged is implemented by expressions that carry flags: an operator
// reports its own, a parsed tree the OR of its operators'. An expression
// that does not implement it carries none.
type Flagged interface {
	Flags() Flags
}

// Taint is a parsed expression tree together with the flags picked up
// while it was built, named after the first operator that set them. The
// parser wraps a root in it only when some flag was set.
type Taint struct {
	Expression
	flags   Flags
	culprit string
}

// NewTaint wraps e with flags set by the operator named culprit.
func NewTaint(e Expression, flags Flags, culprit string) *Taint {
	return &Taint{Expression: e, flags: flags, culprit: culprit}
}

// Flags implements Flagged.
func (t *Taint) Flags() Flags { return t.flags }

// Culprit names the operator that set the flags.
func (t *Taint) Culprit() string { return t.culprit }

// TimeInvariant reports whether e carries no TimeVariant flag.
func TimeInvariant(e Expression) bool {
	f, ok := e.(Flagged)
	return !ok || f.Flags()&TimeVariant == 0
}

// TimeVariantCulprit names the operator that makes e time-variant, when
// it is: the recorded culprit of a tainted tree, else the expression
// itself.
func TimeVariantCulprit(e Expression) (string, bool) {
	if TimeInvariant(e) {
		return "", false
	}
	if c, ok := e.(interface{ Culprit() string }); ok && c.Culprit() != "" {
		return c.Culprit(), true
	}
	return e.String(), true
}
