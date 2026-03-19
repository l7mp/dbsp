package relation

type ColumnType int

const (
	TypeAny ColumnType = iota
	TypeInt
	TypeString
	TypeFloat
)

type Column struct {
	Name          string
	QualifiedName string
	Type          ColumnType
}

// Schema defines the table schemata
type Schema struct {
	Columns   []Column
	PKIndices []int // Indices of columns that make up the Primary Key
	Aliases   map[string]string
}
