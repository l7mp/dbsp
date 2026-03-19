package sql

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
)

// UnimplementedError indicates a SQL feature not yet supported.
type UnimplementedError struct {
	Feature string
}

func (e UnimplementedError) Error() string {
	if e.Feature == "" {
		return "unimplemented"
	}
	return fmt.Sprintf("unimplemented: %s", e.Feature)
}

func literalTypeName(t sqlparser.ValType) string {
	switch t {
	case sqlparser.StrVal:
		return "string"
	case sqlparser.IntVal:
		return "int"
	case sqlparser.FloatVal:
		return "float"
	case sqlparser.HexVal:
		return "hex"
	case sqlparser.ValArg:
		return "bindvar"
	case sqlparser.BitVal:
		return "bit"
	case sqlparser.HexNum:
		return "hexnum"
	default:
		return fmt.Sprintf("%d", t)
	}
}
