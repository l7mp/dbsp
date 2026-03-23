package operator

import (
	"errors"
)

type ErrorStack struct {
	buf   []error
	head  int // next index to write
	count int // number of stored elements, <= len(buf)
}

func NewErrorStack(capacity int) *ErrorStack {
	if capacity <= 0 {
		panic("errstack: capacity must be > 0")
	}
	return &ErrorStack{
		buf: make([]error, capacity),
	}
}

func (s *ErrorStack) Capacity() int { return len(s.buf) }

// Push stores err, dropping older ones when full.
// nil errors are ignored.
func (s *ErrorStack) Push(err error) {
	if err == nil {
		return
	}
	s.buf[s.head] = err
	s.head = (s.head + 1) % len(s.buf)
	if s.count < len(s.buf) {
		s.count++
	}
}

// Errors returns stored errors from oldest to newest.
func (s *ErrorStack) Errors() []error {
	out := make([]error, 0, s.count)
	if s.count == 0 {
		return out
	}
	start := (s.head - s.count + len(s.buf)) % len(s.buf)
	for i := 0; i < s.count; i++ {
		idx := (start + i) % len(s.buf)
		if s.buf[idx] != nil {
			out = append(out, s.buf[idx])
		}
	}
	return out
}

// Joined returns a single error that combines all stored errors.
// Uses errors.Join (Go 1.20+). Returns nil if there are no errors.
func (s *ErrorStack) Joined() error {
	errs := s.Errors()
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}
