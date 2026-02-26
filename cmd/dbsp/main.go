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
	state := newState()

	root := &cobra.Command{
		Use:   "dbsp",
		Short: "DBSP command-line tools",
	}

	root.AddCommand(circuitRootCommand(nil, state))
	root.AddCommand(zsetRootCommand(nil, state))
	root.AddCommand(executorRootCommand(nil, state))
	root.AddCommand(sqlRootCommand(nil, state))
	root.AddCommand(newRunCommand())
	root.AddCommand(newShellCommand())

	return root
}

func newShellCommand() *cobra.Command {
	var noReadline bool

	cmd := &cobra.Command{
		Use:   "shell",
		Short: "Start interactive DBSP shell",
		RunE: func(cmd *cobra.Command, args []string) error {
			state := newState()

			if noReadline {
				return runLineShell(state)
			}

			app := console.New("dbsp")
			app.NewlineBefore = true
			app.NewlineAfter = true

			setupRootMenu(app, state)
			setupCircuitMenu(app, state)
			setupZSetMenu(app, state)
			setupExecutorMenu(app, state)
			setupSQLMenu(app, state)

			return app.Start()
		},
	}

	cmd.Flags().BoolVar(&noReadline, "no-readline", false, "Disable readline and run a basic line shell")

	return cmd
}

func newRunCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run <script>",
		Short: "Run DBSP commands from a script file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			file, err := os.Open(args[0])
			if err != nil {
				return err
			}
			defer file.Close()

			return runScript(newState(), file, args[0])
		},
	}

	return cmd
}
