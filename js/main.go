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
	if len(positionals) < 1 {
		return fmt.Errorf("repl is not implemented yet; pass a .js script path")
	}

	script := positionals[0]
	scriptArgs := positionals[1:]

	level, err := parseLogLevel(logLevel)
	if err != nil {
		return err
	}

	logger := newLogger(level)
	logger.V(1).Info("starting dbsp", "loglevel", logLevel)

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

	processArgv := make([]string, 0, 2+len(scriptArgs))
	processArgv = append(processArgv, os.Args[0], script)
	processArgv = append(processArgv, scriptArgs...)
	if err := vm.SetProcessArgv(processArgv); err != nil {
		return fmt.Errorf("set process argv: %w", err)
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
		return zapcore.ErrorLevel, fmt.Errorf("invalid log level %q: use trace|debug|info|warn|error", raw)
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
