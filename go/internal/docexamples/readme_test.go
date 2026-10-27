package docexamples

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Run the root README's Go example as written, so the front page can't drift from the API.
func TestRootReadmeExample(t *testing.T) {
	readme, err := os.ReadFile("../../../README.md")
	require.NoError(t, err)

	_, rest, ok := strings.Cut(string(readme), "```go\npackage main\n")
	require.True(t, ok, "missing the README's Go example")
	body, _, ok := strings.Cut(rest, "\n```")
	require.True(t, ok, "unterminated README Go example")

	path := filepath.Join(t.TempDir(), "main.go")
	require.NoError(t, os.WriteFile(path, []byte("package main\n"+body+"\n"), 0o600)) //nolint:gosec // path is under t.TempDir, not user input

	cmd := exec.CommandContext(t.Context(), "go", "run", path) //nolint:gosec // path is a test-owned temporary Go source file
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
	require.Equal(t, "2 1\n", string(out))
}
