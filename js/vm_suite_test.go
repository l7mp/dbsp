package js

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestJSRuntime(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DBSP JS Runtime Suite")
}
