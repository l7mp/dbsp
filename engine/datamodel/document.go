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

	// Copy returns a deep copy of the document when possible.
	Copy() Document

	// Merge combines this document with another document and returns the merged result.
	Merge(other Document) Document

	// New returns a new empty document of the same type.
	New() Document

	// GetField returns the value for a field name.
	// It may return ErrFieldNotFound when the field is missing.
	GetField(key string) (any, error)

	// SetField sets the value for a field name.
	// It may return ErrFieldNotFound when the field is missing.
	SetField(key string, value any) error

	// Fields returns a deep copy of the document fields map.
	Fields() map[string]any
}
