package operator_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/l7mp/dbsp/dcontroller/internal/testsuite"
)

var suite *testsuite.Suite

func TestMain(m *testing.M) {
	var err error
	suite, err = testsuite.NewSuite(
		filepath.Join("..", "..", "config", "crd", "resources"),
		".", // ConfigDeployment CRD lives alongside this test
	)
	if err != nil {
		panic("suite setup: " + err.Error())
	}
	code := m.Run()
	suite.Close()
	os.Exit(code)
}

func TestConfigmapDeployment(t *testing.T) {
	suite.RunJS(t, "operator.test.js")
}
