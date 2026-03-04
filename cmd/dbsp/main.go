package main

import (
	"fmt"
	"os"

	"github.com/reeflective/console"
	"github.com/spf13/cobra"
)

func main() {
	root := newRootCommand()
	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	var noReadline bool

	root := &cobra.Command{
		Use:   "dbsp [script]",
		Short: "DBSP command-line tools",
		Long: `DBSP command-line tools.

With no arguments, starts an interactive shell.
With a script file argument, executes the script.
Subcommands (circuit, zset, executor, sql) are also available directly.`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				// Script mode: run commands from file.
				file, err := os.Open(args[0])
				if err != nil {
					return err
				}
				defer file.Close()
				return runScript(newState(), file, args[0])
			}
			// Interactive mode.
			state := newState()
			if noReadline {
				return runLineShell(state)
			}
			app := console.New("dbsp")
			app.NewlineBefore = true
			app.NewlineAfter = true
			setupRootMenu(app, state)
			return app.Start()
		},
	}

	root.Flags().BoolVar(&noReadline, "no-readline", false, "Disable readline (interactive mode only)")

	state := newState()
	root.AddCommand(circuitRootCommand(state))
	root.AddCommand(zsetRootCommand(state))
	root.AddCommand(executorRootCommand(state))
	root.AddCommand(sqlRootCommand(state))

	return root
}
