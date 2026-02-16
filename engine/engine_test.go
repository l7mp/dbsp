package engine_test

import (
	"github.com/l7mp/dbsp/compiler"
	"github.com/l7mp/dbsp/engine"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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

var _ = Describe("Engine", func() {
	It("calls compiler on Compile", func() {
		stub := &stubCompiler{}
		eng := engine.New(stub)

		err := eng.Compile("select 1")
		Expect(err).To(HaveOccurred())
		Expect(stub.called).To(BeTrue())
	})
})
