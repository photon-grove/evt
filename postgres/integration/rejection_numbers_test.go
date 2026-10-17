//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/internal/compat"
)

// The canonical number text in compat/fixtures/outcomes.json is what PostgreSQL's jsonb prints:
// each fixture input, stored as jsonb and read back, decodes to the fixture's stored rejection and
// re-encodes to it byte for byte. An input evt refuses as unstorable is one jsonb refuses too, and
// one evt refuses only as too large is one jsonb would hold.
func Test_RejectionNumberFixturesMatchJSONB(t *testing.T) {
	ctx := context.Background()
	pool := newPool(ctx, t)

	data, err := os.ReadFile(compat.FixturePath(compat.OutcomeFixtureFile))
	require.NoError(t, err)

	var fixtures compat.OutcomeFixtures
	require.NoError(t, json.Unmarshal(data, &fixtures))
	require.NotEmpty(t, fixtures.RejectionNumbers)

	for _, vector := range fixtures.RejectionNumbers {
		t.Run(vector.Name, func(t *testing.T) {
			var text string

			queryErr := pool.QueryRow(ctx, `SELECT $1::jsonb::text`, vector.Input).Scan(&text)

			switch vector.Error {
			case "":
				require.NoError(t, queryErr)

				fromJSONB, err := evt.DecodeRejection([]byte(text))
				require.NoError(t, err)

				stored, err := evt.DecodeRejection([]byte(vector.Stored))
				require.NoError(t, err)
				require.Equal(t, stored, fromJSONB, "jsonb reads back the canonical numbers")

				reencoded, err := evt.EncodeRejection(fromJSONB)
				require.NoError(t, err)
				require.Equal(t, vector.Stored, string(reencoded))

			case "invalid-argument":
				var pgErr *pgconn.PgError
				require.True(t, errors.As(queryErr, &pgErr), "jsonb refuses the number too: %v", queryErr)
				require.Equal(t, "22003", pgErr.Code, "numeric value out of range")

			case "outcome-too-large":
				require.NoError(t, queryErr, "only evt's own bound refuses it")

			default:
				t.Fatalf("unknown fixture error %q", vector.Error)
			}
		})
	}
}
