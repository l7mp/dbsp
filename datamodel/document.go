package datamodel

import (
	"encoding/json"
	"errors"
	"fmt"
)

var ErrFieldNotFound = errors.New("field not found")

// Document is the interface that Z-set elements must implement.
type Document interface {
	fmt.Stringer
	json.Marshaler
	json.Unmarshaler

	// Hash returns a string identifier for equality checking. Two elements are equal iff their
	// hashes are equal. This is based on full content (like a hash of all fields).
	Hash() string

	// PrimaryKey returns the primary key for the element, like in SQL. Multiple elements with
	// different Hash() values may share the same PrimaryKey(). Returns an error if the primary
	// key is unavailable (e.g., lost during schemaless processing).
	PrimaryKey() (string, error)

	// Concat concatenates this document with another, producing a new document that contains
	// the fields of both. This is used by CartesianProduct to create flattened join results.
	// The implementation is type-specific: for SQL Rows, it merges schemas and data; for
	// other document types, it combines fields appropriately.
	Concat(other Document) Document

	// Copy returns a deep copy of the document when possible.
	Copy() Document

	// New returns a new empty document of the same type.
	New() Document

	// GetField returns the value for a field name.
	// It may return ErrFieldNotFound when the field is missing.
	GetField(key string) (any, error)

	// SetField sets the value for a field name.
	// It may return ErrFieldNotFound when the field is missing.
	SetField(key string, value any) error
}
