package sql

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSQLCompiler(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SQL Compiler Suite")
}
