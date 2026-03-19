package aggregation

import (
	"encoding/json"
	"fmt"
)

// PipelineOp is a single pipeline stage serialized as {"@op": args}.
type PipelineOp struct {
	Op   string
	Args json.RawMessage
}

func (p PipelineOp) MarshalJSON() ([]byte, error) {
	if p.Op == "" {
		return nil, fmt.Errorf("pipeline op: empty operation")
	}
	return json.Marshal(map[string]json.RawMessage{p.Op: p.Args})
}

func (p *PipelineOp) UnmarshalJSON(data []byte) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw) != 1 {
		return fmt.Errorf("pipeline op: expected exactly one operation")
	}
	for op, args := range raw {
		p.Op = op
		p.Args = args
	}
	return nil
}

// Pipeline is an ordered list of pipeline stages.
type Pipeline []PipelineOp

func (p *Pipeline) UnmarshalJSON(data []byte) error {
	var many []PipelineOp
	if err := json.Unmarshal(data, &many); err == nil {
		*p = many
		return nil
	}

	var single PipelineOp
	if err := json.Unmarshal(data, &single); err != nil {
		return err
	}
	*p = Pipeline{single}
	return nil
}
