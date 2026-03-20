package main

import (
	"testing"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestCmds(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DBSP CLI Suite")
}

// newTestEnv returns a fresh appState and a run() function that executes a
// single command against it. The cobra tree is rebuilt on every call so
// commands are fully independent (cobra retains parsed-flag state between
// Execute calls on the same tree).
func newTestEnv() (*appState, func(...string) error) {
	state := newState(logr.Discard())
	run := func(args ...string) error {
		root := buildRootCmd(state)
		root.SetArgs(args)
		return root.Execute()
	}
	return state, run
}
