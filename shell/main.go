package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	root := newRootCommand()
	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	var level string
	var verbose bool

	root := &cobra.Command{
		Use:   "dbsp",
		Short: "DBSP interactive shell",
		Args:  cobra.MaximumNArgs(1),
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			if verbose {
				level = "debug"
			}
			_, err := parseLogLevel(level)
			return err
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ll, err := parseLogLevel(level)
			if err != nil {
				return err
			}
			log := newLogger(ll)

			if len(args) == 1 {
				file, err := os.Open(args[0])
				if err != nil {
					return err
				}
				defer file.Close()
				err = runScript(newState(log), file, args[0])
				if err != nil {
					cmd.SilenceUsage = true
				}
				return err
			}
			return runLineShell(newState(log))
		},
	}
	root.SilenceErrors = true
	root.PersistentFlags().StringVarP(&level, "loglevel", "l", "error", "Runtime log level: debug, info, warn, error")
	root.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose runtime logging (debug)")

	return root
}

func parseLogLevel(raw string) (zapcore.Level, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
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
