package relation

import (
	"fmt"
	"strings"
)

// Database holds table schemas for query compilation.
type Database struct {
	name   string
	tables map[string]*Table
}

// NewDatabase creates a new empty catalog.
func NewDatabase(name string) *Database {
	return &Database{
		name:   name,
		tables: make(map[string]*Table),
	}
}

// GetName returns the name of the database.
func (d *Database) GetName() string { return d.name }

// RegisterTable adds a table schema to the catalog.
func (d *Database) RegisterTable(name string, table *Table) {
	d.tables[strings.ToLower(name)] = table
}

// GetTable returns the schema for a table, or an error if not found.
func (d *Database) GetTable(name string) (*Table, error) {
	table, ok := d.tables[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("table %s not found in database", name)
	}
	return table, nil
}

// Insert inserts a row into a table.
func (d *Database) Insert(table string, data []any) error {
	t, err := d.GetTable(table)
	if err != nil {
		return err
	}
	return t.Insert(data)
}

// Delete removes a row by primary key from a table.
func (d *Database) Delete(table string, pk string) (bool, error) {
	t, err := d.GetTable(table)
	if err != nil {
		return false, err
	}
	return t.Delete(pk), nil
}

// Update updates a row by primary key using the updater callback.
func (d *Database) Update(table string, pk string, fn func(row *Row) error) (*Row, error) {
	t, err := d.GetTable(table)
	if err != nil {
		return nil, err
	}
	return t.Update(pk, fn)
}
