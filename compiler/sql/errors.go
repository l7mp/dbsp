package sql

import "fmt"

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
