package consumer

// ApplyResult classifies the result of one write command against the plant,
// split by whether the failure happened before the plant or at it. The
// contract: Unreachable failures MUST be retried with backoff, because the
// plant did not change and no feedback event will ever re-tick the loop on
// their account; Refused failures MUST NOT be retried, because the plant
// decided, so the command is reported and dropped. The consumer maps the
// apiserver's errors onto these classes.
type ApplyResult int

const (
	// Applied: the plant accepted the write.
	Applied ApplyResult = iota
	// Unreachable: the command never reached the plant (transport errors,
	// timeouts, throttling, server overload). Retry the same command.
	Unreachable
	// Refused: the plant received the command and rejected it. Report and
	// drop.
	Refused
)

// String implements fmt.Stringer.
func (o ApplyResult) String() string {
	switch o {
	case Applied:
		return "applied"
	case Unreachable:
		return "unreachable"
	case Refused:
		return "refused"
	}
	return "unknown"
}
