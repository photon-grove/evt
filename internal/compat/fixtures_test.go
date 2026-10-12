package compat

import (
	"context"
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Regenerate the golden fixtures with: go test ./internal/compat -run TestFixtures -update
var update = flag.Bool("update", false, "rewrite compat/fixtures from the Go implementation")

func TestFixturesCodec(t *testing.T) {
	fixtures, err := GenerateCodecFixtures(context.Background())
	require.NoError(t, err)

	assertFixture(t, CodecFixtureFile, fixtures)
}

func TestFixturesScenarios(t *testing.T) {
	scenarios, err := LoadScenarios()
	require.NoError(t, err)

	fixtures, err := GenerateScenarioFixtures(context.Background(), scenarios)
	require.NoError(t, err)

	assertFixture(t, ScenarioFixtureFile, fixtures)
}

func assertFixture(t *testing.T, name string, value any) {
	t.Helper()

	data, err := MarshalFixture(value)
	require.NoError(t, err)

	path := FixturePath(name)
	if *update {
		require.NoError(t, os.WriteFile(path, data, 0o644))
		return
	}

	existing, err := os.ReadFile(path)
	require.NoError(t, err, "missing fixture; run go test ./internal/compat -run TestFixtures -update")
	require.Equal(t, string(existing), string(data),
		"Go output differs from %s; if the change is intentional, regenerate with -update and update the Rust side", name)
}

func TestForRunRewritesEntityIDs(t *testing.T) {
	snapshot := `{"entityType":"account","entityID":"compat-fixture-a"}`
	expectation := ScenarioExpectation{
		EntityID:    "compat-fixture-a",
		Events:      []string{`{"id":"compat-fixture-a:1","entityId":"compat-fixture-a"}`},
		Snapshot:    &snapshot,
		DynamoItems: []map[string]any{{"pk": map[string]any{"S": "compat-fixture-a"}}},
	}

	rewritten, err := expectation.ForRun("r42")
	require.NoError(t, err)

	require.Equal(t, "compat-r42-a", string(rewritten.EntityID))
	require.Equal(t, `{"id":"compat-r42-a:1","entityId":"compat-r42-a"}`, rewritten.Events[0])
	require.Equal(t, `{"entityType":"account","entityID":"compat-r42-a"}`, *rewritten.Snapshot)
	require.Equal(t, "compat-r42-a", rewritten.DynamoItems[0]["pk"].(map[string]any)["S"])
}
