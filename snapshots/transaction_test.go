package snapshots_test

// These tests exercise Store.Transaction (ADR 0004, C1/C2) over the in-memory repository: the
// engine-built read fences, caller-fence handling, duplicate-command detection across streams,
// single-use semantics, and the commit ID stamped once across every stream.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/mem"
	"github.com/photon-grove/evt/policy"
	"github.com/photon-grove/evt/snapshots"
	"github.com/photon-grove/evt/test"
)

// recordingRepository remembers every SerializedResult a transaction hands the repository.
type recordingRepository struct {
	evt.Repository

	committed []evt.SerializedResult
}

// AtomicTransactions forwards the wrapped repository's capability, which embedding the interface
// does not promote.
func (r *recordingRepository) AtomicTransactions() {}

func (r *recordingRepository) Commit(ctx context.Context, result evt.SerializedResult) error {
	r.committed = append(r.committed, result)

	return r.Repository.Commit(ctx, result)
}

type transactionSetup struct {
	repo  *recordingRepository
	store *snapshots.Store
}

func newTransactionSetup(snapshotSize int) transactionSetup {
	repo := &recordingRepository{Repository: mem.NewRepository()}

	return transactionSetup{repo: repo, store: snapshots.NewStore(repo, snapshotSize)}
}

// seed creates the stream with a created event followed by `updates` updated events.
func (s transactionSetup) seed(t *testing.T, id evt.EntityID, updates int, metadata evt.Metadata) {
	t.Helper()
	ctx := context.Background()

	entity := test.NewEntity(id)
	require.NoError(t, s.store.Execute(ctx, entity, id, &test.CreateEntity{Value: "created"}, metadata))

	for range updates {
		require.NoError(t, s.store.Execute(ctx, entity, id, &test.ReplaceEntity{Value: "updated"}, metadata))
	}

	s.repo.committed = nil
}

func (s transactionSetup) read(t *testing.T, tx *snapshots.Transaction, id evt.EntityID) {
	t.Helper()

	require.NoError(t, tx.Read(context.Background(), test.NewEntity(id), id))
}

func updated(id evt.EntityID) evt.Event {
	return &test.EntityUpdated{ID: id, Value: "transaction"}
}

func created(id evt.EntityID) evt.Event {
	return &test.EntityCreated{ID: id, Value: "transaction"}
}

func TestTransaction_EngineFillsFenceHeadsForUnwrittenReads(t *testing.T) {
	setup := newTransactionSetup(0)
	ctx := context.Background()
	metadata := newTestMetadata()

	setup.seed(t, "tx-read-only", 2, metadata)
	setup.seed(t, "tx-written", 0, metadata)

	tx := setup.store.Transaction()
	setup.read(t, tx, "tx-read-only")
	setup.read(t, tx, "tx-written")
	require.NoError(t, tx.Append("tx-written", updated("tx-written")))

	events, err := tx.Commit(ctx, metadata)
	require.NoError(t, err)
	require.Len(t, events, 1)

	require.Len(t, setup.repo.committed, 1, "one atomic repository commit")
	committed := setup.repo.committed[0]

	require.Equal(t, []evt.ReadFence{{
		EntityID:      "tx-read-only",
		EntityType:    test.EntityType,
		Head:          3,
		CallerStamped: false,
	}}, committed.Fences, "the unwritten read is fenced at the head the reader saw, engine-filled")
	require.Len(t, committed.Events, 1)
	require.Equal(t, evt.EntityID("tx-written"), committed.Events[0].EntityID)
	require.Equal(t, evt.EventSequence(2), committed.Events[0].Sequence)

	require.Len(t, getEvents(t, setup.repo, "tx-read-only"), 3, "a fenced read is never written")
}

func TestTransaction_FenceHeadCoversACompactedSnapshot(t *testing.T) {
	setup := newTransactionSetup(2)
	ctx := context.Background()
	metadata := newTestMetadata()

	setup.seed(t, "tx-compacted", 1, metadata)
	setup.seed(t, "tx-writer", 0, metadata)

	// The snapshot at event 2 covers both events, so compaction deletes them all.
	compactor, ok := setup.repo.Repository.(evt.Compactor)
	require.True(t, ok)
	deleted, err := compactor.CompactBelow(ctx, "tx-compacted", 2)
	require.NoError(t, err)
	require.Equal(t, 2, deleted)

	tx := setup.store.Transaction()
	setup.read(t, tx, "tx-compacted")
	setup.read(t, tx, "tx-writer")
	require.NoError(t, tx.Append("tx-writer", updated("tx-writer")))
	_, err = tx.Commit(ctx, metadata)
	require.NoError(t, err)

	require.Len(t, setup.repo.committed[0].Fences, 1)
	require.Equal(t, evt.EventSequence(2), setup.repo.committed[0].Fences[0].Head,
		"the head survives compaction because the snapshot's event sequence seeds the load")
}

func TestTransaction_StaleEngineFenceIsRetryableAndWritesNothing(t *testing.T) {
	setup := newTransactionSetup(0)
	ctx := context.Background()
	metadata := newTestMetadata()

	setup.seed(t, "tx-fenced", 0, metadata)

	tx := setup.store.Transaction()
	setup.read(t, tx, "tx-fenced")
	setup.read(t, tx, "tx-target")
	require.NoError(t, tx.Append("tx-target", created("tx-target")))

	// Another writer moves the fenced stream past the head the transaction read.
	other := test.NewEntity("tx-fenced")
	require.NoError(t, setup.store.Execute(ctx, other, "tx-fenced", &test.ReplaceEntity{Value: "moved"}, metadata))

	_, err := tx.Commit(ctx, metadata)
	require.Error(t, err)

	var conflict *evt.FenceConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, evt.EntityID("tx-fenced"), conflict.Fence.EntityID)
	require.Equal(t, evt.EventSequence(1), conflict.Fence.Head)
	require.False(t, conflict.Fence.CallerStamped)
	require.True(t, evt.IsConflictErr(err))
	require.True(t, policy.IsTransient(err), "an engine-filled fence conflict retries like an event-put conflict")

	require.Empty(t, getEvents(t, setup.repo, "tx-target"), "a failed transaction writes nothing")
}

func TestTransaction_CallerStampedFenceIsAStaleFenceRejection(t *testing.T) {
	setup := newTransactionSetup(0)
	ctx := context.Background()
	metadata := newTestMetadata()

	setup.seed(t, "tx-external", 1, metadata)

	tx := setup.store.Transaction()
	setup.read(t, tx, "tx-target")
	require.NoError(t, tx.Append("tx-target", created("tx-target")))
	// The caller believes the external stream is still at its first event.
	require.NoError(t, tx.Fence(evt.ReadFence{EntityID: "tx-external", EntityType: test.EntityType, Head: 1}))

	_, err := tx.Commit(ctx, metadata)

	var conflict *evt.FenceConflictError
	require.ErrorAs(t, err, &conflict)
	require.True(t, conflict.Fence.CallerStamped)
	require.False(t, policy.IsTransient(err), "a caller-stamped conflict is not retried")
	require.Empty(t, getEvents(t, setup.repo, "tx-target"))
}

func TestTransaction_DropsCallerFencesForStreamsItRead(t *testing.T) {
	for name, fenceFirst := range map[string]bool{"fence after read": false, "fence before read": true} {
		t.Run(name, func(t *testing.T) {
			setup := newTransactionSetup(0)
			ctx := context.Background()
			metadata := newTestMetadata()

			setup.seed(t, "tx-read", 1, metadata)

			tx := setup.store.Transaction()
			callerFence := evt.ReadFence{EntityID: "tx-read", EntityType: test.EntityType, Head: 99}

			if fenceFirst {
				require.NoError(t, tx.Fence(callerFence))
				setup.read(t, tx, "tx-read")
			} else {
				setup.read(t, tx, "tx-read")
				require.NoError(t, tx.Fence(callerFence))
			}
			setup.read(t, tx, "tx-target")
			require.NoError(t, tx.Append("tx-target", created("tx-target")))

			_, err := tx.Commit(ctx, metadata)
			require.NoError(t, err, "the engine's own fence supersedes the caller's")

			require.Equal(t, []evt.ReadFence{{
				EntityID:   "tx-read",
				EntityType: test.EntityType,
				Head:       2,
			}}, setup.repo.committed[0].Fences)
		})
	}
}

func TestTransaction_DetectsADuplicateCommandInAnyStream(t *testing.T) {
	setup := newTransactionSetup(0)
	ctx := context.Background()

	commandID := evt.CommandID("tx-command-1")
	metadata := newTestMetadata()
	metadata.CommandID = &commandID

	// The command already ran against one stream of the read set.
	setup.seed(t, "tx-earlier", 0, metadata)

	tx := setup.store.Transaction()
	setup.read(t, tx, "tx-earlier")
	setup.read(t, tx, "tx-later")
	require.NoError(t, tx.Append("tx-later", created("tx-later")))

	_, err := tx.Commit(ctx, metadata)
	require.True(t, evt.IsDuplicateCommandErr(err), "got %v", err)
	require.Empty(t, getEvents(t, setup.repo, "tx-later"), "a duplicate command writes nothing")
	require.Empty(t, setup.repo.committed)
}

func TestTransaction_IsSingleUse(t *testing.T) {
	setup := newTransactionSetup(0)
	ctx := context.Background()
	metadata := newTestMetadata()

	tx := setup.store.Transaction()
	setup.read(t, tx, "tx-once")
	require.NoError(t, tx.Append("tx-once", created("tx-once")))
	_, err := tx.Commit(ctx, metadata)
	require.NoError(t, err)

	_, err = tx.Commit(ctx, metadata)
	require.ErrorContains(t, err, "already consumed")
	require.ErrorContains(t, tx.Read(ctx, test.NewEntity("tx-other"), "tx-other"), "already consumed")
	require.ErrorContains(t, tx.Append("tx-once", updated("tx-once")), "already consumed")
	require.ErrorContains(t, tx.Fence(evt.ReadFence{EntityID: "tx-other"}), "already consumed")
	require.Len(t, getEvents(t, setup.repo, "tx-once"), 1)

	// A failed commit consumes the transaction too: it cannot be retried in place.
	failed := setup.store.Transaction()
	setup.read(t, failed, "tx-failed")
	require.NoError(t, failed.Append("tx-failed", created("tx-failed")))
	require.NoError(t, failed.Fence(evt.ReadFence{EntityID: "tx-once", EntityType: test.EntityType, Head: 0}))

	_, err = failed.Commit(ctx, metadata)
	require.Error(t, err)

	_, err = failed.Commit(ctx, metadata)
	require.ErrorContains(t, err, "already consumed")
}

func TestTransaction_ValidationErrorsAreRecoverableAndStateErrorsConsume(t *testing.T) {
	setup := newTransactionSetup(0)
	ctx := context.Background()

	// A nil entity, a duplicate read, and an append to an unread stream leave it usable.
	tx := setup.store.Transaction()
	setup.read(t, tx, "tx-known")
	require.Error(t, tx.Read(ctx, nil, "tx-nil"))
	require.Error(t, tx.Read(ctx, test.NewEntity("tx-known"), "tx-known"))
	require.Error(t, tx.Append("tx-unread", created("tx-unread")))
	require.NoError(t, tx.Append("tx-known", created("tx-known")))
	_, err := tx.Commit(ctx, newTestMetadata())
	require.NoError(t, err)
	require.Len(t, getEvents(t, setup.repo, "tx-known"), 1)

	// An event the entity cannot take consumes the transaction.
	broken := setup.store.Transaction()
	setup.read(t, broken, "tx-broken")
	require.Error(t, broken.Append("tx-broken", &foreignEvent{}))
	require.ErrorContains(t, broken.Append("tx-broken", created("tx-broken")), "already consumed")
}

func TestTransaction_StampsOneCommitIDAcrossEveryStream(t *testing.T) {
	ctx := context.Background()

	t.Run("generated once", func(t *testing.T) {
		setup := newTransactionSetup(0)

		tx := setup.store.Transaction()
		setup.read(t, tx, "tx-a")
		setup.read(t, tx, "tx-b")
		require.NoError(t, tx.Append("tx-a", created("tx-a")))
		require.NoError(t, tx.Append("tx-b", created("tx-b")))

		events, err := tx.Commit(ctx, newTestMetadata())
		require.NoError(t, err)
		require.Len(t, events, 2)

		require.NotEmpty(t, events[0].Metadata.CommitID)
		require.Equal(t, events[0].Metadata.CommitID, events[1].Metadata.CommitID)
	})

	t.Run("derived from the command ID", func(t *testing.T) {
		setup := newTransactionSetup(0)
		commandID := evt.CommandID("tx-command-2")
		metadata := newTestMetadata()
		metadata.CommandID = &commandID

		tx := setup.store.Transaction()
		setup.read(t, tx, "tx-a")
		setup.read(t, tx, "tx-b")
		require.NoError(t, tx.Append("tx-b", created("tx-b")))
		require.NoError(t, tx.Append("tx-a", created("tx-a")))

		events, err := tx.Commit(ctx, metadata)
		require.NoError(t, err)

		for _, event := range events {
			require.Equal(t, string(commandID), event.Metadata.CommitID)
		}
	})
}

// foreignEvent belongs to a different entity type than the test entity.
type foreignEvent struct{}

func (foreignEvent) EntityType() evt.EntityType { return "foreign" }
func (foreignEvent) EntityID() evt.EntityID     { return "tx-broken" }
func (foreignEvent) Type() evt.EventType        { return "foreign:happened" }
func (foreignEvent) Version() evt.EventVersion  { return 1 }

// plainRepository hides every optional capability of the repository it wraps.
type plainRepository struct{ evt.Repository }

func TestTransaction_RefusesARepositoryWithoutTheTransactionCapability(t *testing.T) {
	ctx := context.Background()
	recording := &recordingRepository{Repository: mem.NewRepository()}
	store := snapshots.NewStore(plainRepository{Repository: recording}, 0)

	tx := store.Transaction()
	err := tx.Read(ctx, test.NewEntity("tx-plain"), "tx-plain")

	var unsupported *evt.TransactionsUnsupportedError
	require.ErrorAs(t, err, &unsupported)
	require.ErrorIs(t, err, evt.ErrTransactionsUnsupported)
	require.Contains(t, unsupported.Repository, "plainRepository")
	require.Empty(t, recording.committed, "nothing is dispatched to the repository")

	// Commit fails closed on its own, too, for a transaction built without a read.
	_, err = store.Transaction().Commit(ctx, newTestMetadata())
	require.ErrorIs(t, err, evt.ErrTransactionsUnsupported)
	require.Empty(t, recording.committed)

	// The repositories this module ships declare the capability.
	require.NoError(t, evt.RequireTransactions(mem.NewRepository()))
}
