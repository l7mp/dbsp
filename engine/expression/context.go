package expression

import (
	"github.com/go-logr/logr"
	"github.com/l7mp/dbsp/engine/datamodel"
)

// EvalContext carries evaluation state through the expression tree.
// It is immutable - methods return new contexts with modifications.
type EvalContext struct {
	document datamodel.Document
	subject  any
	logger   logr.Logger
	now      string
	nowSet   bool
}

// NewContext creates a root evaluation context.
func NewContext(doc datamodel.Document) *EvalContext {
	return &EvalContext{
		document: doc,
		logger:   logr.Discard(),
	}
}

// WithLogger returns a new context with the given logger.
func (c *EvalContext) WithLogger(logger logr.Logger) *EvalContext {
	return &EvalContext{
		document: c.document,
		subject:  c.subject,
		logger:   logger,
		now:      c.now,
		nowSet:   c.nowSet,
	}
}

// WithSubject returns a new context with the given subject.
// Used by @map/@filter to pass iteration values.
func (c *EvalContext) WithSubject(subject any) *EvalContext {
	return &EvalContext{
		document: c.document,
		subject:  subject,
		logger:   c.logger,
		now:      c.now,
		nowSet:   c.nowSet,
	}
}

// WithNow returns a new context with a frozen timestamp.
func (c *EvalContext) WithNow(now string) *EvalContext {
	return &EvalContext{
		document: c.document,
		subject:  c.subject,
		logger:   c.logger,
		now:      now,
		nowSet:   true,
	}
}

// Document returns the primary document.
func (c *EvalContext) Document() datamodel.Document {
	return c.document
}

// Subject returns the current iteration subject (nil if not in iteration).
func (c *EvalContext) Subject() any {
	return c.subject
}

// Logger returns the logger.
func (c *EvalContext) Logger() logr.Logger {
	return c.logger
}

// Now returns the frozen timestamp, if one is set.
func (c *EvalContext) Now() (string, bool) {
	return c.now, c.nowSet
}
