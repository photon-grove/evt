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
