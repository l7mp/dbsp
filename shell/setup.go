package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/reeflective/console"
	"github.com/spf13/cobra"
)

// buildRootCmd constructs the non-interactive cobra command tree used by
// runScript, runLineShell, and tests alike. Errors and usage are silenced so
// callers control error presentation.
func buildRootCmd(state *appState) *cobra.Command {
	root := &cobra.Command{
		Use:           "dbsp",
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	root.AddCommand(circuitRootCommand(state))
	root.AddCommand(zsetRootCommand(state))
	root.AddCommand(executorRootCommand(state))
	root.AddCommand(sqlRootCommand(state))
	return root
}

// setupRootMenu configures the default (root) interactive menu.
func setupRootMenu(app *console.Console, state *appState) {
	menu := app.ActiveMenu()
	p := menu.Prompt()
	p.Primary = func() string { return "dbsp > " }
	menu.AddInterrupt(io.EOF, func(c *console.Console) {
		fmt.Fprintln(os.Stdout, "Exiting shell")
		os.Exit(0)
	})

	menu.SetCommands(func() *cobra.Command {
		root := &cobra.Command{}
		root.AddCommand(circuitRootCommand(state))
		root.AddCommand(zsetRootCommand(state))
		root.AddCommand(executorRootCommand(state))
		root.AddCommand(sqlRootCommand(state))
		root.AddCommand(&cobra.Command{
			Use:   "exit",
			Short: "Exit the shell",
			Run: func(cmd *cobra.Command, args []string) {
				os.Exit(0)
			},
		})
		return root
	})
}

// runLineShell runs a simple line-by-line interactive shell on stdin.
func runLineShell(state *appState) error {
	root := buildRootCmd(state)
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
		root.SetArgs(args)
		if err := root.Execute(); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
}

// runScript executes DBSP commands from an io.Reader, one per line.
// Blank lines and lines starting with '#' are skipped.
func runScript(state *appState, reader io.Reader, source string) error {
	root := buildRootCmd(state)
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
		root.SetArgs(args)
		if err := root.Execute(); err != nil {
			return fmt.Errorf("%s:%d: %w", source, lineNumber, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("%s: %w", source, err)
	}
	return nil
}
