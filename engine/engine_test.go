package engine_test

import (
	"testing"

	"github.com/l7mp/dbsp/compiler"
	"github.com/l7mp/dbsp/engine"
)

type stubCompiler struct {
	called bool
}

func (s *stubCompiler) Compile(source []byte) (*compiler.CompiledQuery, error) {
	s.called = true
	return nil, nil
}

func (s *stubCompiler) CompileString(source string) (*compiler.CompiledQuery, error) {
	return s.Compile([]byte(source))
}

func TestEngineCompileUsesCompiler(t *testing.T) {
	stub := &stubCompiler{}
	eng := engine.New(stub)

	if err := eng.Compile("select 1"); err == nil {
		t.Fatalf("expected error from nil compiled query")
	}
	if !stub.called {
		t.Fatalf("expected compiler to be called")
	}
}
