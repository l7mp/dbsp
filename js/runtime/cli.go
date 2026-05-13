package runtime

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	dbspjs "github.com/l7mp/dbsp/js"
)

// Execute parses args, builds the root command, and runs it.
func Execute(args []string) error {
	root := newRootCommand()
	root.SetArgs(args)
	root.SilenceUsage = true
	return root.Execute()
}

// Main runs the runtime CLI and exits with status 1 on failure.
func Main() {
	if err := Execute(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type rootFlags struct {
	configFile string
	logLevel   string
	verbose    bool
	evalSource string
	stdlibPath string
}

func newRootCommand() *cobra.Command {
	flags := &rootFlags{}

	root := &cobra.Command{
		Use:   "dbsp [flags] <script.js> [script-args...] | dbsp [flags] -e <code> [args...]",
		Short: "DBSP runtime: run JavaScript scripts",
		Long: `dbsp runs DBSP JavaScript programs against an in-process incremental engine.
Without a subcommand, the first positional argument is the script path; the
remaining positionals are passed through to the script as process.argv.
Use -e/--eval to execute inline JavaScript source.`,
		Args:          cobra.ArbitraryArgs,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runScript(cmd, args, flags)
		},
	}
	root.Flags().SetInterspersed(false)
	root.PersistentFlags().SetInterspersed(false)

	pf := root.PersistentFlags()
	pf.StringVar(&flags.configFile, "config", "", "Config file (YAML); env: DBSP_CONFIG")
	pf.StringVarP(&flags.logLevel, "log-level", "l", "error", "Log level: trace|debug|info|warn|error")
	pf.BoolVarP(&flags.verbose, "verbose", "v", false, "Enable debug logs (same as --log-level=debug)")
	pf.StringVarP(&flags.evalSource, "eval", "e", "", "Evaluate JavaScript source and exit")
	pf.StringVar(&flags.stdlibPath, "stdlib", "", "Stdlib directory path; env: DBSP_STDLIB")

	cobra.OnInitialize(func() { initConfig(flags) })

	return root
}

// initConfig wires viper for env-var and config-file overrides. Precedence is
// flag > env (DBSP_*) > config file > default. It is invoked once via
// cobra.OnInitialize after pflag parsing.
func initConfig(flags *rootFlags) {
	viper.SetEnvPrefix("DBSP")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	viper.AutomaticEnv()

	if flags.configFile != "" {
		viper.SetConfigFile(flags.configFile)
		if err := viper.ReadInConfig(); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to read config %q: %v\n", flags.configFile, err)
		}
	}
}

// applyViper overrides any flag that was not set on the command line with the
// value from viper. Cobra/pflag has already populated defaults and explicit
// values; viper supplies env and config-file fallbacks for everything that
// matches a flag name.
func applyViper(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		applyViperToFlag(cmd, f)
	})
	if pf := cmd.PersistentFlags(); pf != nil {
		pf.VisitAll(func(f *pflag.Flag) { applyViperToFlag(cmd, f) })
	}
}

func applyViperToFlag(cmd *cobra.Command, f *pflag.Flag) {
	if f.Changed {
		return
	}
	if !viper.IsSet(f.Name) {
		return
	}
	if err := cmd.Flags().Set(f.Name, fmt.Sprintf("%v", viper.Get(f.Name))); err != nil {
		fmt.Fprintf(os.Stderr, "warning: viper override of %s failed: %v\n", f.Name, err)
	}
}

func runScript(cmd *cobra.Command, args []string, flags *rootFlags) error {
	applyViper(cmd)

	if flags.evalSource == "" && len(args) < 1 {
		return cmd.Help()
	}
	if flags.verbose {
		flags.logLevel = "debug"
	}

	level, err := parseLogLevel(flags.logLevel)
	if err != nil {
		return err
	}
	logger := newLogger(level)
	logger.V(1).Info("starting dbsp", "log-level", flags.logLevel)

	var stdlibPaths []string
	if flags.stdlibPath != "" {
		stdlibPaths = []string{flags.stdlibPath}
	}
	opts := dbspjs.Options{Logger: logger, StdlibPaths: stdlibPaths}

	vm, err := dbspjs.NewVMWithOptions(opts)
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

	script := ""
	scriptArgs := args
	if flags.evalSource == "" {
		script = args[0]
		scriptArgs = args[1:]
	}
	argvScript := script
	if flags.evalSource != "" {
		argvScript = "[eval]"
	}
	processArgv := make([]string, 0, 2+len(scriptArgs))
	processArgv = append(processArgv, os.Args[0], argvScript)
	processArgv = append(processArgv, scriptArgs...)
	if err := vm.SetProcessArgv(processArgv); err != nil {
		return fmt.Errorf("set process argv: %w", err)
	}

	if flags.evalSource != "" {
		source := flags.evalSource + "\nexit();\n"
		if err := vm.RunString(source); err != nil {
			return err
		}
		logger.V(1).Info("inline script completed")
		return nil
	}

	if err := vm.RunFile(script); err != nil {
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
