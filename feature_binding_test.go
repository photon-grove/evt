package evt_test

import (
	"context"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/postgres"
	"github.com/stretchr/testify/require"
)

func TestFeatureMarkerTableBinding(t *testing.T) {
	for _, backend := range []string{"dynamo", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var repo interface {
				evt.Repository
				evt.Compactor
				PutSnapshot(context.Context, evt.EntityType, evt.EntityID, []byte, evt.EventSequence, evt.EventSequence) error
			}
			var attachWrong func()
			if backend == "dynamo" {
				r := dynamo.NewRepository(nil, "events")
				attachWrong = func() { r.WithFeatureMarker(dynamo.NewFeatureMarker(nil, "events", nil)) }
				r.EventsTable = "other_events"
				repo = r
			} else {
				r := postgres.NewRepository(nil, postgres.WithEventsTable("events"))
				attachWrong = func() { r.WithFeatureMarker(postgres.NewFeatureMarker(nil, "events", nil)) }
				postgres.WithEventsTable("other_events")(r)
				repo = r
			}

			// Recover the typed configuration panic; no database is available to hide a missed guard.
			func() {
				defer func() {
					mismatch, ok := recover().(*evt.FeatureMarkerTableMismatchError)
					require.True(t, ok, "attachment must reject a marker from another log")
					require.NotNil(t, mismatch)
				}()
				attachWrong()
			}()

			ctx := context.Background()
			var mismatch *evt.FeatureMarkerTableMismatchError
			err := repo.Commit(ctx, evt.SerializedResult{})
			require.ErrorAs(t, err, &mismatch)
			require.Equal(t, "other_events", mismatch.EventsTable)
			require.Equal(t, "events", mismatch.MarkerEventsTable)
			err = repo.CommitWithSnapshot(ctx, evt.SerializedResult{}, "account", "account", nil, 0)
			require.ErrorAs(t, err, &mismatch)
			err = repo.PutSnapshot(ctx, "account", "account", nil, 1, 1)
			require.ErrorAs(t, err, &mismatch)
			_, err = repo.CompactBelow(ctx, "account", 1)
			require.ErrorAs(t, err, &mismatch)
		})
	}
}
