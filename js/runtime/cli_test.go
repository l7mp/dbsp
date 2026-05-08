package runtime

import (
	"os"
	"path/filepath"
	"testing"
)

// TestScriptArgvForwarding regression-tests that positional args after the
// script path land in process.argv inside the JS runtime.
func TestScriptArgvForwarding(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "dbsp-cli-argv-")
	if err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tmpDir) })

	scriptPath := filepath.Join(tmpDir, "argv.js")
	script := `
const args = (process && process.argv) ? process.argv.slice(2) : [];
if (args.join(",") !== "test,gwclass") {
  throw new Error("unexpected argv: " + JSON.stringify(args));
}
exit();
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		t.Fatalf("write script: %v", err)
	}

	if err := Execute([]string{scriptPath, "test", "gwclass"}); err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
}

// TestRootHelpOnNoArgs covers the "dbsp" (no subcommand, no positional)
// path. It must not error; help is the documented response.
func TestRootHelpOnNoArgs(t *testing.T) {
	if err := Execute([]string{}); err != nil {
		t.Fatalf("Execute([]) returned error: %v", err)
	}
}

// TestUnknownFlagErrors makes sure cobra still rejects bogus input.
func TestUnknownFlagErrors(t *testing.T) {
	err := Execute([]string{"--this-flag-does-not-exist"})
	if err == nil {
		t.Fatal("expected error for unknown flag, got nil")
	}
}

func TestEvalRunsInlineCodeAndExits(t *testing.T) {
	err := Execute([]string{"-e", `console.log("ok-from-eval")`})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
}

func TestEvalArgvForwarding(t *testing.T) {
	err := Execute([]string{"-e", `
const args = (process && process.argv) ? process.argv.slice(2) : [];
if (args.join(",") !== "x,y") {
  throw new Error("unexpected argv: " + JSON.stringify(args));
}
`, "x", "y"})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
}
