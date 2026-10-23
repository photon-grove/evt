package docexamples

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Compile the actual upgrade snippet, not a second copy that can drift from the README.
func TestReadmeGetRecentEvents(t *testing.T) {
	readme, err := os.ReadFile("../../../README.md")
	require.NoError(t, err)

	_, rest, ok := strings.Cut(string(readme), "```go\nfunc (r *Repository) GetRecentEvents(")
	require.True(t, ok, "missing Repository migration example")
	body, _, ok := strings.Cut(rest, "\n```")
	require.True(t, ok, "unterminated Repository migration example")

	source := `package example

import (
    "context"
    "errors"
    "testing"

    "github.com/photon-grove/evt/go"
)

type Repository struct {
    queryEvents func(context.Context, string, ...any) ([]evt.SerializedEvent, error)
}

func (r *Repository) GetRecentEvents(` + body + `

func TestMigration(t *testing.T) {
    ctx := context.Background()
    r := &Repository{queryEvents: func(_ context.Context, _ string, args ...any) ([]evt.SerializedEvent, error) {
        if len(args) != 2 || args[0] != "account-1" || args[1] != int64(2) {
            t.Fatalf("unexpected query arguments: %v", args)
        }
        return []evt.SerializedEvent{{Sequence: 3}, {Sequence: 2}}, nil
    }}

    for _, limit := range []int{0, -1} {
        _, err := r.GetRecentEvents(ctx, "account-1", limit)
        var invalid *evt.InvalidArgumentError
        if !errors.Is(err, evt.ErrInvalidArgument) || !errors.As(err, &invalid) || invalid.Argument != "limit" {
            t.Fatalf("expected typed invalid limit, got %v", err)
        }
    }

    events, err := r.GetRecentEvents(ctx, "account-1", 2)
    if err != nil || len(events) != 2 || events[0].Sequence != 3 || events[1].Sequence != 2 {
        t.Fatalf("unexpected recent events: %v, %v", events, err)
    }
}
`
	path := filepath.Join(t.TempDir(), "migration_test.go")
	require.NoError(t, os.WriteFile(path, []byte(source), 0o600)) //nolint:gosec // path is under t.TempDir, not user input

	cmd := exec.CommandContext(t.Context(), "go", "test", path) //nolint:gosec // path is a test-owned temporary Go source file
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
}
