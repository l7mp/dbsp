package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"strings"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	var (
		logLevel string
		verbose  bool
	)

	fs := pflag.NewFlagSet("dbsp", pflag.ContinueOnError)
	fs.SortFlags = false
	fs.SetOutput(os.Stderr)
	fs.StringVarP(&logLevel, "loglevel", "l", "error", "Log level: trace|debug|info|warn|error")
	fs.BoolVarP(&verbose, "verbose", "v", false, "Enable debug logs (same as --loglevel=debug)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if verbose {
		logLevel = "debug"
	}

	positionals := fs.Args()
	if len(positionals) > 1 {
		return fmt.Errorf("only one script path is allowed")
	}

	var script string
	if len(positionals) == 1 {
		script = positionals[0]
	}

	level, err := parseLogLevel(logLevel)
	if err != nil {
		return err
	}

	logger := newLogger(level)
	logger.V(1).Info("starting dbsp", "loglevel", level.String())

	vm, err := NewVM(logger)
	if err != nil {
		return err
	}
	defer vm.Close()

	sigch := make(chan os.Signal, 1)
	signal.Notify(sigch, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigch)

	go func() {
		<-sigch
		logger.Info("received termination signal")
		vm.Close()
	}()

	if script == "" {
		return fmt.Errorf("repl is not implemented yet; pass a .js script path")
	}

	err = vm.RunFile(script)
	if err != nil {
		return err
	}
	logger.V(1).Info("script completed")
	return nil
}

func parseLogLevel(raw string) (zapcore.Level, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "trace":
		return -10, nil
	case "debug":
		return zapcore.DebugLevel, nil
	case "info":
		return zapcore.InfoLevel, nil
	case "warn", "warning":
		return zapcore.WarnLevel, nil
	case "error", "":
		return zapcore.ErrorLevel, nil
	default:
		return zapcore.ErrorLevel, fmt.Errorf("invalid log level %q: use debug|info|warn|error", raw)
	}
}

func newLogger(level zapcore.Level) logr.Logger {
	zc := zap.NewDevelopmentConfig()
	zc.Level = zap.NewAtomicLevelAt(level)
	z, err := zc.Build()
	if err != nil {
		return logr.Discard()
	}
	return zapr.NewLogger(z)
}
