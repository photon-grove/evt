package evt_test

import (
	"context"
	"testing"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/postgres"
	"github.com/photon-grove/evt/result"
	"github.com/stretchr/testify/require"
)

type bindingClient struct{ dynamo.Client }

type bindingDB struct{ postgres.DB }

// valueDB is a non-comparable DB value: == on it would panic at runtime.
type valueDB struct {
	postgres.DB
	tags []string
}

// valueClient is a non-comparable client value: == on it would panic at runtime.
type valueClient struct {
	dynamo.Client
	tags []string
}

func TestFeatureMarkerRejectsNonComparableMarkerClient(t *testing.T) {
	marker := dynamo.NewRepository(valueClient{tags: []string{"a"}}, "events").FeatureMarker(nil)
	repo := dynamo.NewRepository(&bindingClient{}, "events")

	defer func() {
		mismatch, ok := recover().(*evt.FeatureMarkerClientMismatchError)
		require.True(t, ok, "a non-comparable marker client must fail closed, not panic in ==")
		require.NotNil(t, mismatch)
	}()
	repo.WithFeatureMarker(marker)
}

func TestFeatureMarkerRejectsNonComparableMarkerDB(t *testing.T) {
	marker := postgres.NewRepository(valueDB{tags: []string{"a"}}).FeatureMarker(nil)
	repo := postgres.NewRepository(&bindingDB{})

	defer func() {
		mismatch, ok := recover().(*evt.FeatureMarkerClientMismatchError)
		require.True(t, ok, "a non-comparable marker DB must fail closed, not panic in ==")
		require.NotNil(t, mismatch)
	}()
	repo.WithFeatureMarker(marker)
}

func TestFeatureMarkerClientBinding(t *testing.T) {
	for _, backend := range []string{"dynamo", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var attachWrong func()
			if backend == "dynamo" {
				client := &bindingClient{}
				r := dynamo.NewRepository(client, "events")
				marker := r.FeatureMarker(nil)
				require.NotNil(t, r.WithFeatureMarker(marker))
				require.NotNil(t, dynamo.NewRepository(client, "events").WithFeatureMarker(marker))
				other := dynamo.NewRepository(&bindingClient{}, "events")
				attachWrong = func() { other.WithFeatureMarker(marker) }
			} else {
				db := &bindingDB{}
				r := postgres.NewRepository(db)
				marker := r.FeatureMarker(nil)
				require.NotNil(t, r.WithFeatureMarker(marker))
				require.NotNil(t, postgres.NewRepository(db).WithFeatureMarker(marker))
				other := postgres.NewRepository(&bindingDB{})
				attachWrong = func() { other.WithFeatureMarker(marker) }
			}

			// Equal table names do not imply equal physical stores. Reject before any I/O.
			defer func() {
				mismatch, ok := recover().(*evt.FeatureMarkerClientMismatchError)
				require.True(t, ok, "attachment must reject a marker from another client")
				require.NotNil(t, mismatch)
			}()
			attachWrong()
		})
	}
}

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
				attachWrong = func() { r.WithFeatureMarker(dynamo.NewRepository(nil, "events").FeatureMarker(nil)) }
				r.EventsTable = "other_events"
				repo = r
			} else {
				r := postgres.NewRepository(nil, postgres.WithEventsTable("events"))
				attachWrong = func() {
					r.WithFeatureMarker(postgres.NewRepository(nil, postgres.WithEventsTable("events")).FeatureMarker(nil))
				}
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
			stream := make(chan result.Result[evt.SerializedResult], 1)
			stream <- result.Ok(evt.SerializedResult{})
			close(stream)
			errs := repo.CommitStream(ctx, stream)
			require.Len(t, errs, 1)
			require.ErrorAs(t, errs[0], &mismatch)
		})
	}
}
