package engine_test

import (
	"github.com/l7mp/dbsp/dbsp/compiler"
	"github.com/l7mp/dbsp/dbsp/engine"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type stubCompiler struct {
	called bool
}

func (s *stubCompiler) Parse(source []byte) (compiler.IR, error) {
	s.called = true
	return nil, nil
}

func (s *stubCompiler) ParseString(source string) (compiler.IR, error) {
	return s.Parse([]byte(source))
}

func (s *stubCompiler) Compile(ir compiler.IR) (*compiler.Query, error) {
	s.called = true
	return nil, nil
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
