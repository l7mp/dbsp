package main

import (
	"context"
	"sync"

	"github.com/go-logr/logr"

	"github.com/l7mp/dbsp/engine/circuit"
	"github.com/l7mp/dbsp/engine/compiler"
	"github.com/l7mp/dbsp/engine/datamodel/relation"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// appState holds all mutable shell state shared across commands.
type appState struct {
	circuits  map[string]*circuit.Circuit
	queries   map[string]*compiler.Query
	sql       map[string]sqlSpec
	aggregate map[string]aggregateSpec
	zsets     map[string]*boundZSet
	db        *relation.Database
	logger    logr.Logger

	runtime    *dbspruntime.Runtime
	runtimeCtx context.Context
	cancel     context.CancelFunc
	processors map[string]*dbspruntime.Circuit
	errMu      sync.Mutex
	runtimeErr error
}

type aggregateSpec struct {
	Source string
	Input  string
	Output string
}

type sqlSpec struct {
	Source string
	Output string
}

// newState returns a fresh appState with all maps initialised.
func newState(logger logr.Logger) *appState {
	ctx, cancel := context.WithCancel(context.Background())
	rt := dbspruntime.NewRuntime()

	state := &appState{
		circuits:   make(map[string]*circuit.Circuit),
		queries:    make(map[string]*compiler.Query),
		sql:        make(map[string]sqlSpec),
		aggregate:  make(map[string]aggregateSpec),
		zsets:      make(map[string]*boundZSet),
		db:         relation.NewDatabase("dbsp"),
		logger:     logger,
		runtime:    rt,
		runtimeCtx: ctx,
		cancel:     cancel,
		processors: make(map[string]*dbspruntime.Circuit),
	}

	go func() {
		if err := rt.Start(ctx); err != nil {
			state.errMu.Lock()
			state.runtimeErr = err
			state.errMu.Unlock()
		}
	}()

	return state
}

func (s *appState) close() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *appState) runtimeFailure() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.runtimeErr
}

func (s *appState) installProcessor(name string, p *dbspruntime.Circuit) {
	if prev, ok := s.processors[name]; ok {
		s.runtime.Stop(prev)
	}
	s.processors[name] = p
	s.runtime.Add(p)
}

func (s *appState) uninstallProcessor(name string) {
	if prev, ok := s.processors[name]; ok {
		s.runtime.Stop(prev)
		delete(s.processors, name)
	}
}
