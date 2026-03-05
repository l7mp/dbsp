package operator

import "fmt"

type jsonUnsupported struct{}

func (jsonUnsupported) MarshalJSON() ([]byte, error) {
	return nil, fmt.Errorf("operator JSON marshaling is not implemented")
}

func (*jsonUnsupported) UnmarshalJSON([]byte) error {
	return fmt.Errorf("operator JSON unmarshaling is not implemented")
}

