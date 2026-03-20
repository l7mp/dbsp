package aggregation

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAggregationCompiler(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Aggregation Compiler Suite")
}
