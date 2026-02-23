package main

import (
	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/dbsp/circuit"
	"github.com/l7mp/dbsp/internal/logger"
)

// appState holds all mutable shell state shared across commands.
type appState struct {
	circuits        map[string]*circuit.Circuit
	executors       map[string]*boundExecutor
	zsets           map[string]*boundZSet
	currentCircuit  string
	currentExecutor string
	currentZSet     string
	parentMenu      string
	logger          logr.Logger
}

// newState returns a fresh appState with all maps initialised and a discard logger.
func newState() *appState {
	return &appState{
		circuits:  make(map[string]*circuit.Circuit),
		executors: make(map[string]*boundExecutor),
		zsets:     make(map[string]*boundZSet),
		logger:    logger.DiscardLogger(),
	}
}
