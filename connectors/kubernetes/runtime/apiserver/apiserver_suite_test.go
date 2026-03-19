package apiserver

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAPIServer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Kubernetes API Server Suite")
}
