// Package dbsp provides an extensible expression language for DBSP operators.
package dbsp

import (
	"github.com/go-logr/logr"
	"github.com/l7mp/dbsp/datamodel"
)

// Context carries evaluation state through the expression tree.
// It is immutable - methods return new contexts with modifications.
type Context struct {
	document datamodel.Document
	subject  any
	logger   logr.Logger
}

// NewContext creates a root evaluation context.
func NewContext(doc datamodel.Document) *Context {
	return &Context{
		document: doc,
		logger:   logr.Discard(),
	}
}

// WithLogger returns a new context with the given logger.
func (c *Context) WithLogger(logger logr.Logger) *Context {
	return &Context{
		document: c.document,
		subject:  c.subject,
		logger:   logger,
	}
}

// WithSubject returns a new context with the given subject.
// Used by @map/@filter to pass iteration values.
func (c *Context) WithSubject(subject any) *Context {
	return &Context{
		document: c.document,
		subject:  subject,
		logger:   c.logger,
	}
}

// Document returns the primary document.
func (c *Context) Document() datamodel.Document {
	return c.document
}

// Subject returns the current iteration subject (nil if not in iteration).
func (c *Context) Subject() any {
	return c.subject
}

// Logger returns the logger.
func (c *Context) Logger() logr.Logger {
	return c.logger
}
