package snapshots_test

// These tests exercise the outcome-aware entry points of snapshots.Store (ADR 0004, C3/C4) over the
// in-memory repository: configuration errors, the custom digester, the legacy event window, the
// detached recovery read, and the mapping of handler errors. The backend-neutral behavior (replay,
// reuse, rejection, no-op, recovery, races) is covered by the shared conformance suite.

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/mem"
	"github.com/photon-grove/evt/snapshots"
	etest "github.com/photon-grove/evt/test"
)

// memOutcomes returns the in-memory repository as an outcome-capable one.
func memOutcomes(t *testing.T) evt.OutcomeRepository {
	t.Helper()

	repo, ok := mem.NewRepository().(evt.OutcomeRepository)
	require.True(t, ok)

	return repo
}

func commandMetadata(id string) evt.Metadata {
	commandID := evt.CommandID(id)

	return evt.Metadata{CommandID: &commandID}
}

func newOutcomeStore(t *testing.T, opts ...snapshots.OutcomeOption) (*snapshots.Store, evt.OutcomeRepository) {
	t.Helper()

	repo := memOutcomes(t)

	return snapshots.NewStore(repo, 0).WithOutcomes(opts...), repo
}

func createCommand() *etest.CreateEntity { return &etest.CreateEntity{Value: "created"} }

func TestOutcomes_DisabledStoreRefusesTheOutcomeEntryPoints(t *testing.T) {
	store := snapshots.NewStore(mem.NewRepository(), 0)
	ctx := context.Background()
	entity := etest.NewEntity("e-1")

	require.False(t, store.OutcomesEnabled())

	_, err := store.ExecuteWithOutcome(ctx, entity, "e-1", createCommand(), commandMetadata("cmd-1"))
	require.ErrorIs(t, err, evt.ErrInvalidArgument)

	_, _, err = store.BeginCommand(ctx, evt.CommandIdentity{Type: "t"}, commandMetadata("cmd-1"))
	require.ErrorIs(t, err, evt.ErrInvalidArgument)

	// A store without outcomes keeps today's behavior: no command ID is required.
	require.NoError(t, store.Execute(ctx, entity, "e-1", createCommand(), evt.Metadata{}))
}

// capabilityFreeRepository hides the outcome capability of the repository it wraps.
type capabilityFreeRepository struct{ evt.Repository }

func (capabilityFreeRepository) AtomicTransactions() {}

func TestOutcomes_RepositoryWithoutTheCapabilityIsRefusedBeforeAnyRead(t *testing.T) {
	store := snapshots.NewStore(capabilityFreeRepository{mem.NewRepository()}, 0).WithOutcomes()

	_, err := store.ExecuteWithOutcome(context.Background(), etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-1"))
	require.ErrorIs(t, err, evt.ErrOutcomesUnsupported)

	var unsupported *evt.OutcomesUnsupportedError
	require.ErrorAs(t, err, &unsupported)
	require.Contains(t, unsupported.Repository, "capabilityFreeRepository")
}

func TestOutcomes_BareCommitIsRefusedOnAnOutcomeStore(t *testing.T) {
	store, _ := newOutcomeStore(t)
	ctx := context.Background()
	entity := etest.NewEntity("e-1")

	eventContext, err := store.LoadEntity(ctx, entity, "e-1")
	require.NoError(t, err)

	_, err = store.Commit(ctx, evt.CommandResult{Events: []evt.Event{&etest.EntityCreated{ID: "e-1", Value: "v"}}}, eventContext, commandMetadata("cmd-1"))
	require.ErrorIs(t, err, evt.ErrInvalidArgument, "a bare commit has no command identity to digest")

	tx := store.Transaction()
	require.NoError(t, tx.Read(ctx, entity, "e-1"))

	_, err = tx.Commit(ctx, commandMetadata("cmd-2"))
	require.ErrorIs(t, err, evt.ErrInvalidArgument)
}

func TestOutcomes_CustomDigesterDefinesTheIdentity(t *testing.T) {
	// A digester that ignores the payload makes two different payloads the same command.
	store, repo := newOutcomeStore(t, snapshots.WithDigester(func(in evt.DigestInput) (string, error) {
		return "type:" + string(in.CommandType), nil
	}))
	ctx := context.Background()

	first, err := store.ExecuteWithOutcome(ctx, etest.NewEntity("e-1"), "e-1", &etest.CreateEntity{Value: "one"}, commandMetadata("cmd-1"))
	require.NoError(t, err)
	require.Equal(t, "type:test:create", first.Digest)

	second, err := store.ExecuteWithOutcome(ctx, etest.NewEntity("e-1"), "e-1", &etest.CreateEntity{Value: "two"}, commandMetadata("cmd-1"))
	require.NoError(t, err)
	require.True(t, second.Replayed, "the custom digest is equal, so this is a replay")

	events, err := repo.GetEvents(ctx, "e-1")
	require.NoError(t, err)
	require.Len(t, events, 1)

	// A digester error fails the command before anything is read or written.
	failing, _ := newOutcomeStore(t, snapshots.WithDigester(func(evt.DigestInput) (string, error) {
		return "", errors.New("no digest")
	}))
	_, err = failing.ExecuteWithOutcome(ctx, etest.NewEntity("e-2"), "e-2", createCommand(), commandMetadata("cmd-1"))
	require.ErrorContains(t, err, "no digest")
}

func TestOutcomes_LegacyEventWindowStillCatchesPreOutcomeCommands(t *testing.T) {
	repo := memOutcomes(t)
	ctx := context.Background()

	// A pre-v0.9 command: its events carry the command ID but no outcome row exists.
	legacy := snapshots.NewStore(repo, 0)
	require.NoError(t, legacy.Execute(ctx, etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("legacy")))

	store := snapshots.NewStore(repo, 0).WithOutcomes()
	err := store.Execute(ctx, etest.NewEntity("e-1"), "e-1", &etest.ReplaceEntity{Value: "again"}, commandMetadata("legacy"))
	require.True(t, evt.IsDuplicateCommandErr(err), "got %v", err)

	events, err := repo.GetEvents(ctx, "e-1")
	require.NoError(t, err)
	require.Len(t, events, 1)
}

func TestOutcomes_HandlerErrorsMapToRejectionsOrFailures(t *testing.T) {
	ctx := context.Background()

	store, repo := newOutcomeStore(t)

	// NotFoundError is a persisted rejection; the error-returning Execute keeps the original
	// error matchable on first execution.
	notFound := &MockEntity{handleError: evt.NewNotFoundError("gone")}
	notFound.ID = "e-1"

	err := store.Execute(ctx, notFound, "e-1", &MockCommand{}, commandMetadata("nf"))
	require.ErrorIs(t, err, evt.ErrRejected)
	require.True(t, evt.IsNotFoundErr(err))

	stored, err := repo.GetOutcome(ctx, "nf")
	require.NoError(t, err)
	require.Equal(t, evt.RejectionNotFound, stored.Rejection.Code)

	// On replay only the rejection is stored, so only it is returned.
	replayed := store.Execute(ctx, notFound, "e-1", &MockCommand{}, commandMetadata("nf"))
	require.ErrorIs(t, replayed, evt.ErrRejected)
	require.False(t, evt.IsNotFoundErr(replayed))

	// An arbitrary error is an internal failure: returned, and nothing recorded.
	broken := &MockEntity{handleError: errors.New("disk on fire")}
	broken.ID = "e-2"

	err = store.Execute(ctx, broken, "e-2", &MockCommand{}, commandMetadata("broken"))
	require.ErrorContains(t, err, "disk on fire")
	require.NotErrorIs(t, err, evt.ErrRejected)

	stored, err = repo.GetOutcome(ctx, "broken")
	require.NoError(t, err)
	require.Nil(t, stored)

	// A bad event while applying the handler's own events is an internal failure too.
	badApply := &MockEntity{applyError: evt.NewBadEventError(&etest.EntityCreated{})}
	badApply.ID = "e-3"

	err = store.Execute(ctx, badApply, "e-3", &MockCommand{}, commandMetadata("bad-apply"))
	require.Error(t, err)

	stored, err = repo.GetOutcome(ctx, "bad-apply")
	require.NoError(t, err)
	require.Nil(t, stored)

	// A NoOpError is an accepted outcome without events.
	noop := &MockEntity{handleError: evt.NewNoOpError("nothing to do")}
	noop.ID = "e-4"

	outcome, err := store.ExecuteWithOutcome(ctx, noop, "e-4", &MockCommand{}, commandMetadata("noop"))
	require.NoError(t, err)
	require.True(t, outcome.NoOp())
}

// cancelAwareRepository records whether GetOutcome saw a cancelled context.
type cancelAwareRepository struct {
	evt.OutcomeRepository

	cancel       context.CancelFunc
	sawCancelled bool
	reads        int
}

func (r *cancelAwareRepository) Commit(ctx context.Context, result evt.SerializedResult) error {
	if err := r.OutcomeRepository.Commit(ctx, result); err != nil {
		return err
	}

	// The commit applied, then the caller's context was cancelled while the result was lost.
	r.cancel()

	return &evt.AmbiguousWriteError{CommandID: result.Outcome.CommandID, Cause: context.Canceled}
}

func (r *cancelAwareRepository) GetOutcome(ctx context.Context, id evt.CommandID) (*evt.Outcome, error) {
	r.reads++
	if ctx.Err() != nil {
		r.sawCancelled = true
	}

	return r.OutcomeRepository.GetOutcome(ctx, id)
}

func TestOutcomes_RecoveryReadSurvivesTheCallersCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	repo := &cancelAwareRepository{OutcomeRepository: memOutcomes(t), cancel: cancel}
	store := snapshots.NewStore(repo, 0).WithOutcomes()

	outcome, err := store.ExecuteWithOutcome(ctx, etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-1"))
	require.NoError(t, err)
	require.True(t, outcome.Replayed)

	require.Equal(t, 2, repo.reads, "the pre-check and the recovery read")
	require.False(t, repo.sawCancelled, "the recovery read must not inherit the cancelled context")
}

func TestOutcomes_ResolveCommandReportsWhatHappened(t *testing.T) {
	store, _ := newOutcomeStore(t)
	ctx := context.Background()

	identity := evt.CommandIdentity{Type: "test:create", Streams: []evt.EntityID{"e-1"}, Payload: createCommand()}

	absent, err := store.ResolveCommand(ctx, identity, commandMetadata("cmd-1"))
	require.NoError(t, err)
	require.Nil(t, absent)

	_, err = store.ExecuteWithOutcome(ctx, etest.NewEntity("e-1"), "e-1", createCommand(), commandMetadata("cmd-1"))
	require.NoError(t, err)

	found, err := store.ResolveCommand(ctx, identity, commandMetadata("cmd-1"))
	require.NoError(t, err)
	require.True(t, found.Replayed)

	identity.Payload = &etest.CreateEntity{Value: "other"}
	_, err = store.ResolveCommand(ctx, identity, commandMetadata("cmd-1"))
	require.ErrorIs(t, err, evt.ErrIdempotencyKeyReuse)
}
