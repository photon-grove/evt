//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/internal/compat"
)

// The canonical number text in compat/fixtures/outcomes.json is what PostgreSQL's jsonb prints: the
// stored text, cast to jsonb and read back, decodes to the same rejection and re-encodes to it byte
// for byte, and so does each input jsonb accepts. (evt sends jsonb the canonical text, never the
// input; jsonb's parser refuses some inputs, such as an exponent past its bound, whose canonical
// text it holds.) An input evt refuses as unstorable is one jsonb refuses too, and one evt refuses
// only as too large is one jsonb would hold.
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
				requireJSONBPrints(ctx, t, pool, vector.Stored, vector.Stored)

				if queryErr == nil {
					requireJSONBPrints(ctx, t, pool, vector.Input, vector.Stored)
				}

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

// requireJSONBPrints casts a rejection to jsonb and requires the text read back to decode to the
// stored rejection and re-encode to it byte for byte.
func requireJSONBPrints(ctx context.Context, t *testing.T, pool *pgxpool.Pool, input, stored string) {
	t.Helper()

	var text string
	require.NoError(t, pool.QueryRow(ctx, `SELECT $1::jsonb::text`, input).Scan(&text))

	fromJSONB, err := evt.DecodeRejection([]byte(text))
	require.NoError(t, err)

	want, err := evt.DecodeRejection([]byte(stored))
	require.NoError(t, err)
	require.Equal(t, want, fromJSONB, "jsonb reads back the canonical numbers")

	reencoded, err := evt.EncodeRejection(fromJSONB)
	require.NoError(t, err)
	require.Equal(t, stored, string(reencoded))
}
