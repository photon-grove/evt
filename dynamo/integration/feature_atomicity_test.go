//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/result" //nolint:staticcheck // exercises the deprecated batch write contract
	"github.com/photon-grove/evt/test"
	"github.com/stretchr/testify/require"
)

// pausedWriteClient pauses after validation, immediately before the real atomic write.
type pausedWriteClient struct {
	dynamo.Client
	once   sync.Once
	paused chan struct{}
	resume chan struct{}
}

func (c *pausedWriteClient) TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	c.once.Do(func() {
		close(c.paused)
		select {
		case <-c.resume:
		case <-ctx.Done():
		}
	})

	return c.Client.TransactWriteItems(ctx, in, opts...)
}

func Test_CompactionFencesCompletedDeclaration(t *testing.T) {
	for _, tc := range []struct {
		existingMarker bool
		unsupported    bool
	}{{false, false}, {true, false}, {false, true}, {true, true}} {
		t.Run(fmt.Sprintf("existing=%t/unsupported=%t", tc.existingMarker, tc.unsupported), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			repo, marker := newIsolatedEventLog(ctx, t)
			id := evt.EntityID("account-1")
			events, err := evt.SerializeEvents([]evt.Event{&test.EntityCreated{ID: id, Value: "one"}}, 0, id, evt.Metadata{})
			require.NoError(t, err)
			require.NoError(t, repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: events}, test.EntityType, id, []byte(`{}`), 1))
			if tc.existingMarker {
				_, err = marker.Declare(ctx, func(m *evt.FeatureManifest) { m.Retention = []evt.EntityType{"session"} })
				require.NoError(t, err)
			}

			client := &pausedWriteClient{Client: repoClient(), paused: make(chan struct{}), resume: make(chan struct{})}
			compactor := dynamo.NewRepository(client, repo.EventsTable).WithLazyInitialization(true)
			done := make(chan error, 1)
			go func() {
				_, compactErr := compactor.CompactBelow(ctx, id, 1)
				done <- compactErr
			}()

			select {
			case <-client.paused:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			_, err = marker.Declare(ctx, func(m *evt.FeatureManifest) {
				if tc.unsupported {
					m.Features[evt.FeatureJournal] = 1
				} else {
					m.HistoryDependent = []evt.EntityType{test.EntityType}
				}
			})
			// Always resume, including on a failed assertion, to avoid leaking the paused writer.
			close(client.resume)
			require.NoError(t, err)
			compactErr := <-done
			if tc.unsupported {
				require.True(t, evt.IsUnsupportedFeatureErr(compactErr), "expected unsupported feature refusal, got %v", compactErr)
			} else {
				require.True(t, evt.IsHistoryDependentProjectionErr(compactErr), "expected history-dependent refusal, got %v", compactErr)
			}
			stored, err := repo.GetEvents(ctx, id)
			require.NoError(t, err)
			require.Len(t, stored, 1, "a completed declaration fences the paused compactor")
		})
	}
}

func Test_CommitAndDeclareShareMarker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	repo, marker := newIsolatedEventLog(ctx, t)
	writer := repo.WithFeatureMarker(marker)
	const commits = 24
	start := make(chan struct{})
	errs := make(chan error, commits+1)

	go func() {
		<-start
		for range commits {
			if _, err := marker.Declare(ctx, func(m *evt.FeatureManifest) {
				m.Retention = []evt.EntityType{"session"}
			}); err != nil {
				errs <- err
				return
			}
		}
		errs <- nil
	}()
	for i := range commits {
		go func() {
			<-start
			id := evt.EntityID(fmt.Sprintf("account-%d", i))
			events, err := evt.SerializeEvents([]evt.Event{&test.EntityCreated{ID: id, Value: "one"}}, 0, id, evt.Metadata{})
			if err != nil {
				errs <- err
				return
			}
			for {
				err = writer.Commit(ctx, evt.SerializedResult{Events: events})
				if !errors.Is(err, evt.ErrFeatureManifestChanged) || ctx.Err() != nil {
					errs <- err
					return
				}
			}
		}()
	}
	close(start)

	for range commits + 1 {
		require.NoError(t, <-errs, "concurrent declarations may require revalidation, never a permanent cache error")
	}
	for i := range commits {
		stored, err := repo.GetEvents(ctx, evt.EntityID(fmt.Sprintf("account-%d", i)))
		require.NoError(t, err)
		require.Len(t, stored, 1)
	}
}

func Test_SnapshotFencesCompletedDeclaration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	repo, marker := newIsolatedEventLog(ctx, t)
	client := &pausedWriteClient{Client: repoClient(), paused: make(chan struct{}), resume: make(chan struct{})}
	writer := dynamo.NewRepository(client, repo.EventsTable).WithLazyInitialization(true)
	done := make(chan error, 1)
	go func() { done <- writer.PutSnapshot(ctx, "account", "account-1", []byte(`{}`), 1, 1) }()

	select {
	case <-client.paused:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_, err := marker.Declare(ctx, func(m *evt.FeatureManifest) { m.Features[evt.FeatureJournal] = 1 })
	close(client.resume)
	require.NoError(t, err)
	require.True(t, evt.IsUnsupportedFeatureErr(<-done))
	snapshot, err := repo.GetSnapshot(ctx, "account-1")
	require.NoError(t, err)
	require.Nil(t, snapshot, "the unsupported maintenance write must not persist")
}

func Test_DefaultAndDeclaringWritersRefuseUnsupported(t *testing.T) {
	ctx := context.Background()
	repo, marker := newIsolatedEventLog(ctx, t)
	declaring := repo.WithFeatureMarker(marker)
	commitOne(ctx, t, repo, "before", 0)
	_, err := marker.Declare(ctx, func(m *evt.FeatureManifest) { m.Features[evt.FeatureJournal] = 1 })
	require.NoError(t, err)

	for _, writer := range []*dynamo.Repository{repo, declaring} {
		events, encodeErr := evt.SerializeEvents([]evt.Event{&test.EntityCreated{ID: "after", Value: "v"}}, 0, "after", evt.Metadata{})
		require.NoError(t, encodeErr)
		res := evt.SerializedResult{Events: events}
		require.True(t, evt.IsUnsupportedFeatureErr(writer.Commit(ctx, res)))
		require.True(t, evt.IsUnsupportedFeatureErr(writer.CommitWithSnapshot(ctx, res, test.EntityType, "after", []byte(`{}`), 1)))
		require.True(t, evt.IsUnsupportedFeatureErr(writer.PutSnapshot(ctx, test.EntityType, "after", []byte(`{}`), 1, 1)))
		ch := make(chan result.Result[evt.SerializedResult], 1)
		ch <- result.Ok(res)
		close(ch)
		errs := writer.CommitStream(ctx, ch) //nolint:staticcheck // the legacy batch path is refused outright (ADR 0007)
		require.Len(t, errs, 1)
		require.ErrorIs(t, errs[0], dynamo.ErrCommitStreamRefused)
	}

	// Changing only a durable set is still not writer validation.
	_, err = marker.Declare(ctx, func(m *evt.FeatureManifest) { m.Retention = []evt.EntityType{"session"} })
	require.NoError(t, err)
	require.True(t, evt.IsUnsupportedFeatureErr(declaring.PutSnapshot(ctx, "account", "after", []byte(`{}`), 1, 1)))
}
