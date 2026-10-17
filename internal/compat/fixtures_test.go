package compat

import (
	"context"
	"flag"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
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

func TestFixturesFeatureMarker(t *testing.T) {
	fixtures, err := GenerateFeatureMarkerFixtures(context.Background())
	require.NoError(t, err)

	assertFixture(t, FeatureMarkerFixtureFile, fixtures)
}

func TestFixturesOutcomes(t *testing.T) {
	fixtures, err := GenerateOutcomeFixtures()
	require.NoError(t, err)

	assertFixture(t, OutcomeFixtureFile, fixtures)
}

func assertFixture(t *testing.T, name string, value any) {
	t.Helper()

	data, err := MarshalFixture(value)
	require.NoError(t, err)

	path := FixturePath(name)
	if *update {
		require.NoError(t, os.WriteFile(path, data, 0o644)) //nolint:gosec // fixtures are committed public files, not secrets
		return
	}

	existing, err := os.ReadFile(path) //nolint:gosec // path is FixturePath of a package-constant fixture name
	require.NoError(t, err, "missing fixture; run go test ./internal/compat -run TestFixtures -update")
	require.Equal(t, string(existing), string(data),
		"Go output differs from %s; if the change is intentional, regenerate with -update and update the Rust side", name)
}

// Every malformed outcome item must be refused as corrupt by the DynamoDB reader, never replayed.
func TestMalformedOutcomeRowsAreCorrupt(t *testing.T) {
	fixtures, err := GenerateOutcomeFixtures()
	require.NoError(t, err)
	require.NotEmpty(t, fixtures.MalformedRows)

	for _, row := range fixtures.MalformedRows {
		t.Run(row.Name, func(t *testing.T) {
			fake := NewFakeDynamo()
			item := map[string]types.AttributeValue{}

			for name, value := range row.DynamoItem {
				attribute, ok := value.(map[string]any)
				require.True(t, ok)

				if number, isNumber := attribute["N"].(string); isNumber {
					item[name] = &types.AttributeValueMemberN{Value: number}
				} else if text, isText := attribute["S"].(string); isText {
					item[name] = &types.AttributeValueMemberS{Value: text}
				} else if binary, isBinary := attribute["B"].(string); isBinary {
					item[name] = &types.AttributeValueMemberB{Value: []byte(binary)}
				}
			}

			_, err := fake.PutItem(context.Background(), &dynamodb.PutItemInput{Item: item})
			require.NoError(t, err)

			stored, err := dynamo.NewRepository(fake, fakeTable).GetOutcome(context.Background(), "bad")
			require.ErrorIs(t, err, evt.ErrCorruptOutcome)
			require.Nil(t, stored)
		})
	}
}
