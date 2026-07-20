package aggregation

import (
	"encoding/json"
	"fmt"
	"strings"
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
	// A bare string is a zero-argument stage: "@distinct" means
	// {"@distinct": null}. Stage position is a closed grammar, so this
	// never collides with string literals (those exist only in expression
	// value position).
	var name string
	if err := json.Unmarshal(data, &name); err == nil {
		if !strings.HasPrefix(name, "@") {
			return fmt.Errorf("pipeline op: bare stage name must start with @: %q", name)
		}
		p.Op = name
		p.Args = json.RawMessage("null")
		return nil
	}

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
