package integration_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/connectors/kubernetes/integration/testsuite"
)

var suite *testsuite.Suite

func TestEnvtest(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Kubernetes Connector Envtest Suite")
}

var _ = BeforeSuite(func() {
	var err error
	suite, err = testsuite.New()
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	if suite != nil {
		suite.Close()
	}
})
