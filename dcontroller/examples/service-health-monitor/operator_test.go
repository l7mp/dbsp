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
	)
	if err != nil {
		panic("suite setup: " + err.Error())
	}
	code := m.Run()
	suite.Close()
	os.Exit(code)
}

func TestServiceHealthMonitor(t *testing.T) {
	port, err := testsuite.FreePort()
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	suite.RunJSWithAPIServer(t, "operator.test.js", port)
}
