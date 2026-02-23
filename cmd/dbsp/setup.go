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
		Use:          "dbsp",
		SilenceErrors: true,
		SilenceUsage:  true,
	}
	root.AddCommand(circuitRootCommand(nil, state))
	// Flat aliases give scripts and tests access to circuit sub-commands
	// without prefixing every line with "circuit".
	root.AddCommand(nodeCommand(state))
	root.AddCommand(edgeCommand(state))
	root.AddCommand(printCommand(state))
	root.AddCommand(validateCommand(state))
	root.AddCommand(incrementalizeCommand(state))
	root.AddCommand(zsetRootCommand(nil, state))
	root.AddCommand(executorRootCommand(nil, state))
	return root
}

// setupRootMenu configures the default (root) interactive menu.
func setupRootMenu(app *console.Console, state *appState) {
	menu := app.ActiveMenu()
	setupPrompt(menu, state)
	menu.AddInterrupt(io.EOF, func(c *console.Console) {
		fmt.Fprintln(os.Stdout, "Exiting shell")
		os.Exit(0)
	})

	menu.SetCommands(func() *cobra.Command {
		root := &cobra.Command{}
		root.AddCommand(circuitRootCommand(app, state))
		root.AddCommand(zsetRootCommand(app, state))
		root.AddCommand(executorRootCommand(app, state))
		root.AddCommand(newExitCommand(app, state))
		return root
	})
}

// setupCircuitMenu configures the "circuit" interactive sub-menu.
func setupCircuitMenu(app *console.Console, state *appState) {
	menu := app.NewMenu("circuit")
	setupPrompt(menu, state)
	menu.AddInterrupt(io.EOF, func(c *console.Console) {
		switchToParentMenu(app, state)
	})

	menu.SetCommands(func() *cobra.Command {
		root := &cobra.Command{}
		for _, cmd := range circuitMenuCommands(app, state) {
			root.AddCommand(cmd)
		}
		root.AddCommand(newExitCommand(app, state))
		return root
	})
}

// setupPrompt installs a context-aware prompt on any menu.
func setupPrompt(menu *console.Menu, state *appState) {
	p := menu.Prompt()
	p.Primary = func() string {
		switch {
		case state.currentCircuit != "":
			return fmt.Sprintf("dbsp circuit(%s) > ", state.currentCircuit)
		case state.currentExecutor != "":
			return fmt.Sprintf("dbsp executor(%s) > ", state.currentExecutor)
		case state.currentZSet != "":
			return fmt.Sprintf("dbsp zset(%s) > ", state.currentZSet)
		default:
			return "dbsp > "
		}
	}
}

// newExitCommand returns a cobra command that exits the current menu or the
// whole shell when already at the root.
func newExitCommand(app *console.Console, state *appState) *cobra.Command {
	return &cobra.Command{
		Use:   "exit",
		Short: "Exit current menu",
		Run: func(cmd *cobra.Command, args []string) {
			appMenu := app.ActiveMenu()
			if appMenu.Name() == "" {
				os.Exit(0)
			}
			switchToParentMenu(app, state)
		},
	}
}

// switchToParentMenu returns to the parent menu and clears all context fields.
func switchToParentMenu(app *console.Console, state *appState) {
	if state == nil || state.parentMenu == "" {
		app.SwitchMenu("")
		if state != nil {
			state.currentCircuit = ""
			state.currentExecutor = ""
			state.currentZSet = ""
		}
		return
	}
	app.SwitchMenu(state.parentMenu)
	state.parentMenu = ""
	state.currentCircuit = ""
	state.currentExecutor = ""
	state.currentZSet = ""
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
