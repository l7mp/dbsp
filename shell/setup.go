package main

import (
	"bufio"
	"fmt"
	"github.com/spf13/cobra"
	"io"
	"os"
	"strings"
)

// buildRootCmd constructs the root command tree used by interactive shell and tests.
func buildRootCmd(state *appState) *cobra.Command {
	root := &cobra.Command{
		Use:                "dbsp",
		SilenceErrors:      true,
		SilenceUsage:       true,
		DisableFlagParsing: true,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}
	root.RunE = func(cmd *cobra.Command, args []string) error {
		if len(args) == 0 {
			return nil
		}
		return executeLine(state, args)
	}
	return root
}

// runLineShell runs a simple line-by-line interactive shell on stdin.
func runLineShell(state *appState) error {
	defer state.close()
	reader := bufio.NewScanner(os.Stdin)
	for {
		fmt.Fprint(os.Stdout, "dbsp > ")
		if !reader.Scan() {
			return reader.Err()
		}
		line := strings.TrimSpace(reader.Text())
		if line == "" {
			continue
		}
		if line == "exit" {
			return nil
		}
		args := strings.Fields(line)
		if err := executeLine(state, args); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
}

// runScript executes DBSP commands from an io.Reader, one per line.
// Blank lines and lines starting with '#' are skipped.
func runScript(state *appState, reader io.Reader, source string) error {
	defer state.close()
	scanner := bufio.NewScanner(reader)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if line == "exit" {
			return nil
		}
		args := strings.Fields(line)
		if err := executeLine(state, args); err != nil {
			return fmt.Errorf("%s:%d: %w", source, lineNumber, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("%s: %w", source, err)
	}
	return nil
}

func executeLine(state *appState, args []string) error {
	if len(args) == 0 {
		return nil
	}

	switch args[0] {
	case "zset":
		cmd := zsetRootCommand(state)
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true
		cmd.SetArgs(args[1:])
		return cmd.Execute()
	case "circuit":
		cmd := circuitRootCommand(state)
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true
		cmd.SetArgs(args[1:])
		return cmd.Execute()
	case "sql":
		cmd := sqlRootCommand(state)
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true
		cmd.SetArgs(args[1:])
		return cmd.Execute()
	case "aggregate":
		cmd := aggregateRootCommand(state)
		cmd.SilenceUsage = true
		cmd.SilenceErrors = true
		cmd.SetArgs(args[1:])
		return cmd.Execute()
	case "echo":
		fmt.Fprintln(os.Stdout, strings.Join(args[1:], " "))
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
