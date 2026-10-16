package snapshots

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
)

// recoveryTimeout bounds the outcome re-read that settles an ambiguous write. The re-read runs on
// a context detached from the caller's cancellation, because the usual reason for an ambiguous
// write is exactly a cancelled or timed-out context.
const recoveryTimeout = 10 * time.Second

// errOutcomesNeedCommand refuses a bare Commit on a store with outcomes enabled: the outcome row
// needs the command's identity (its type, declared streams, and payload), which only the
// command-level entry points have.
var errOutcomesNeedCommand = &evt.InvalidArgumentError{
	Argument: "commit",
	Message:  "this store records command outcomes; execute the command with Execute or ExecuteWithOutcome, or use a Transaction begun with BeginCommand",
}

// outcomeConfig is a store's command-outcome configuration.
type outcomeConfig struct {
	digester evt.Digester
}

// OutcomeOption configures command outcomes on a store.
type OutcomeOption func(*outcomeConfig)

// WithDigester replaces the default identity digest (SHA-256 over the canonical encoding of the
// command type, declared target streams, payload JSON, and principal). A custom digester returns
// its own opaque string; the engine compares digests for equality only, and a digester that is
// not deterministic turns every retry into an IdempotencyKeyReuse error.
func WithDigester(digester evt.Digester) OutcomeOption {
	return func(config *outcomeConfig) {
		if digester != nil {
			config.digester = digester
		}
	}
}

// WithOutcomes enables durable command outcomes on the store (ADR 0004, C3/C4). Every command then
// needs a command ID, reads its outcome row before it runs, and is recorded — accepted, accepted
// with no events, or rejected — in the same transaction as its events, so a retried command
// returns the original answer instead of running twice.
//
// The store's repository must implement evt.OutcomeRepository; the entry points return
// *evt.OutcomesUnsupportedError before reading or writing anything otherwise. Like the other With
// options it must be called during Store initialization, before concurrent use.
func (store *Store) WithOutcomes(options ...OutcomeOption) *Store {
	config := &outcomeConfig{digester: evt.DefaultDigest}
	for _, option := range options {
		option(config)
	}

	store.outcomes = config

	return store
}

// OutcomesEnabled reports whether the store records command outcomes.
func (store *Store) OutcomesEnabled() bool { return store.outcomes != nil }

// Outcome reads a command's stored outcome by ID with a strongly consistent read, or nil when
// the command has none (the prior_outcome readback).
func (store *Store) Outcome(ctx context.Context, id evt.CommandID) (*evt.Outcome, error) {
	repo, err := store.outcomeRepository()
	if err != nil {
		return nil, err
	}

	return repo.GetOutcome(ctx, id)
}

func (store *Store) outcomeRepository() (evt.OutcomeRepository, error) {
	if store.outcomes == nil {
		return nil, &evt.InvalidArgumentError{Argument: "store", Message: "command outcomes are not enabled; call WithOutcomes"}
	}

	return evt.RequireOutcomes(store.repo)
}

// ResolveCommand reads a command's outcome row and answers it for the given identity, without
// running or writing anything: the stored outcome (Replayed) when the digests match,
// *evt.IdempotencyKeyReuseError when they differ, and nil, nil when the command has no outcome.
// It is ambiguous-write recovery as a public call, for a caller that lost the result of a command
// it issued (a crash, a timeout it could not settle) and wants to learn what happened.
func (store *Store) ResolveCommand(
	ctx context.Context,
	identity evt.CommandIdentity,
	metadata evt.Metadata,
) (*evt.CommandOutcome, error) {
	_, replayed, err := store.beginCommand(ctx, identity, metadata)

	return replayed, err
}

// executed is the result of running a command through the outcome path: the outcome, plus the
// handler's own domain error when this call produced a rejection.
type executed struct {
	outcome    *evt.CommandOutcome
	handlerErr error
}

// ExecuteWithOutcome loads the entity, handles the command, and commits the result together with
// its outcome row. It requires WithOutcomes and a command ID in the metadata.
//
// Every call reads the outcome row first. A stored outcome with the same identity digest is
// returned (Replayed) without running the handler; one with a different digest is
// *evt.IdempotencyKeyReuseError. Otherwise the handler runs:
//
//   - events: committed with an accepted outcome carrying CommandResult.Response;
//   - no events, or a *evt.NoOpError: an accepted no-op outcome;
//   - an error that maps to a rejection (evt.RejectionOf): a rejected outcome, returned as a
//     CommandOutcome with a nil error;
//   - any other error: an internal failure, returned as an error with no outcome written.
//
// A lost race on the outcome row and an ambiguous commit result are resolved by re-reading the
// row, never by re-preparing the command; see evt.AmbiguousWriteError for the one case, a commit
// proven not to have applied, that the caller should retry.
//
// The single-aggregate shortcut fences nothing: a rejection or no-op it records was decided on the
// head the entity was loaded at.
func (store *Store) ExecuteWithOutcome(
	ctx context.Context,
	entity evt.Entity,
	entityID evt.EntityID,
	command evt.Command,
	metadata evt.Metadata,
) (*evt.CommandOutcome, error) {
	result, err := store.executeOutcome(ctx, entity, entityID, command, metadata)

	return result.outcome, err
}

// executeForError is Execute on a store with outcomes enabled.
func (store *Store) executeForError(
	ctx context.Context,
	entity evt.Entity,
	entityID evt.EntityID,
	command evt.Command,
	metadata evt.Metadata,
) error {
	result, err := store.executeOutcome(ctx, entity, entityID, command, metadata)
	if err != nil {
		return err
	}

	if result.outcome.Rejected() {
		return &evt.RejectionError{Rejection: *result.outcome.Rejection, Err: result.handlerErr}
	}

	return nil
}

func (store *Store) executeOutcome(
	ctx context.Context,
	entity evt.Entity,
	entityID evt.EntityID,
	command evt.Command,
	metadata evt.Metadata,
) (executed, error) {
	identity := evt.CommandIdentity{Type: command.Type(), Streams: []evt.EntityID{entityID}, Payload: command}

	cmd, replayed, err := store.beginCommand(ctx, identity, metadata)
	if err != nil {
		return executed{}, err
	}

	if replayed != nil {
		return executed{outcome: replayed}, nil
	}

	eventContext, err := store.LoadEntity(ctx, entity, entityID)
	if err != nil {
		return executed{}, err
	}

	// Keep the entity's internal ID aligned with the store key, as Execute does.
	if setter, ok := entity.(interface{ SetID(evt.EntityID) }); ok {
		setter.SetID(entityID)
	}

	// The event window is superseded by the outcome row, but pre-v0.9 commands have no row: an
	// event that carries this command ID without an outcome is one of those.
	if eventContext.HasCommandID(cmd.id) {
		return executed{}, evt.NewDuplicateCommandError(cmd.id)
	}

	result, err := entity.Handle(ctx, command)
	if err != nil {
		return store.handlerFailed(ctx, cmd, err)
	}

	for _, event := range result.Events {
		if err = entity.Apply(event); err != nil {
			return executed{}, err
		}
	}

	if len(result.Events) == 0 {
		// Projector operations without an event are never silently dropped: the commit keeps them
		// so the shared validation refuses them (ProjectorOnlyCommitError) before any write.
		outcome, cerr := cmd.commit(
			ctx,
			evt.SerializedResult{Transaction: result.Transaction}, //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
			evt.OutcomeAccepted,
			result.Response,
			nil,
		)

		return executed{outcome: outcome}, cerr
	}

	projectorTransaction, err := buildProjectorTransaction(ctx, entity, result.Events)
	if err != nil {
		return executed{}, err
	}

	serialized, snapshots, err := store.stage(entity, &eventContext, entityID, result.Events, evt.CommitMetadata(metadata))
	if err != nil {
		return executed{}, err
	}

	outcome, err := cmd.commit(ctx, evt.SerializedResult{
		Events:      serialized,
		Snapshots:   snapshots,
		Transaction: evt.MergeTransactions(result.Transaction, projectorTransaction), //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
	}, evt.OutcomeAccepted, result.Response, nil)

	return executed{outcome: outcome}, err
}

// handlerFailed records a handler error: a no-op and a domain rejection are durable outcomes; any
// other error is an internal failure that is returned and writes nothing.
func (store *Store) handlerFailed(ctx context.Context, cmd *command, handlerErr error) (executed, error) {
	if evt.IsNoOpErr(handlerErr) {
		outcome, err := cmd.commit(ctx, evt.SerializedResult{}, evt.OutcomeAccepted, nil, nil)

		return executed{outcome: outcome}, err
	}

	rejection, ok := evt.RejectionOf(handlerErr)
	if !ok {
		return executed{}, handlerErr
	}

	outcome, err := cmd.commit(ctx, evt.SerializedResult{}, evt.OutcomeRejected, nil, &rejection)

	return executed{outcome: outcome, handlerErr: handlerErr}, err
}

// stage serializes a stream's new events with the commit metadata and, when they reach the
// snapshot interval, captures the snapshot of the entity state they produced. The entity must
// already include the events.
func (store *Store) stage(
	entity evt.Entity,
	eventContext *evt.Context,
	entityID evt.EntityID,
	events []evt.Event,
	metadata evt.Metadata,
) ([]evt.SerializedEvent, []evt.SerializedSnapshot, error) {
	serialized, err := evt.SerializeEventsWithContext(events, eventContext, metadata)
	if err != nil {
		return nil, nil, err
	}

	if evt.CalculateAdditionalEvents(
		*eventContext.CurrentSequence, len(events), store.effectiveSnapshotSize(entity.Type()),
	) == 0 {
		return serialized, nil, nil
	}

	payload, err := store.updateSnapshotWithEvents(serialized, eventContext)
	if err != nil {
		return nil, nil, err
	}

	return serialized, []evt.SerializedSnapshot{{
		EntityType:    entity.Type(),
		EntityID:      entityID,
		Sequence:      *eventContext.CurrentSnapshot,
		EventSequence: serialized[len(serialized)-1].Sequence,
		Payload:       payload,
	}}, nil
}

// command is one command's outcome bookkeeping: its ID and its pre-execution identity digest.
type command struct {
	store  *Store
	repo   evt.OutcomeRepository
	id     evt.CommandID
	digest string

	metadata evt.Metadata
}

// beginCommand validates the command ID, computes the identity digest, and reads the outcome row
// first: a stored outcome with the same digest is returned (Replayed), one with a different digest
// is *evt.IdempotencyKeyReuseError, and an absent row returns the command to run.
func (store *Store) beginCommand(
	ctx context.Context,
	identity evt.CommandIdentity,
	metadata evt.Metadata,
) (*command, *evt.CommandOutcome, error) {
	repo, err := store.outcomeRepository()
	if err != nil {
		return nil, nil, err
	}

	if metadata.CommandID == nil {
		return nil, nil, evt.CheckCommandID("")
	}

	if err = evt.CheckCommandID(*metadata.CommandID); err != nil {
		return nil, nil, err
	}

	input, err := identity.DigestInput(metadata)
	if err != nil {
		return nil, nil, err
	}

	digest, err := store.outcomes.digester(input)
	if err != nil {
		return nil, nil, fmt.Errorf("compute command digest: %w", err)
	}

	cmd := &command{store: store, repo: repo, id: *metadata.CommandID, digest: digest, metadata: metadata}

	stored, err := repo.GetOutcome(ctx, cmd.id)
	if err != nil {
		return nil, nil, err
	}

	if stored == nil {
		return cmd, nil, nil
	}

	replayed, err := cmd.resolve(*stored)

	return nil, replayed, err
}

// resolve answers a command whose row exists: the stored outcome when the digests match, an
// IdempotencyKeyReuseError (carrying the stored outcome) when they differ.
func (c *command) resolve(stored evt.Outcome) (*evt.CommandOutcome, error) {
	if stored.Digest != c.digest {
		return nil, &evt.IdempotencyKeyReuseError{CommandID: c.id, Prior: stored}
	}

	return &evt.CommandOutcome{Outcome: stored, Replayed: true}, nil
}

// commit sends one repository commit that carries the command's outcome, and settles its result:
// success returns the recorded outcome; a lost outcome race, an ambiguous result, and a conflict
// another writer's outcome explains are resolved by reading the row.
func (c *command) commit(
	ctx context.Context,
	result evt.SerializedResult,
	status evt.OutcomeStatus,
	response []byte,
	rejection *evt.Rejection,
) (*evt.CommandOutcome, error) {
	result.Outcome = &evt.Outcome{
		CommandID: c.id,
		Status:    status,
		Digest:    c.digest,
		Response:  response,
		Rejection: rejection,
	}

	normalized, err := evt.NormalizeCommit(result)
	if err != nil {
		return nil, err
	}

	if err = c.store.repo.Commit(ctx, normalized); err != nil {
		return c.settle(ctx, err)
	}

	return &evt.CommandOutcome{Outcome: *normalized.Outcome, Events: normalized.Events}, nil
}

// settle resolves a failed commit that carried the outcome.
func (c *command) settle(ctx context.Context, commitErr error) (*evt.CommandOutcome, error) {
	switch {
	case errors.Is(commitErr, evt.ErrOutcomeConflict):
		// Another writer recorded this command ID first. Re-preparing can never resolve it; the
		// winner's row does.
		resolved, err := c.reread(ctx)
		if err != nil {
			return nil, err
		}

		if resolved == nil {
			return nil, fmt.Errorf("outcome put lost its condition but no outcome row exists: %w", commitErr)
		}

		return resolved, nil

	case errors.Is(commitErr, evt.ErrAmbiguousWrite):
		return c.recover(ctx, commitErr)

	case evt.IsConflictErr(commitErr) || policy.IsTransient(commitErr):
		// A concurrent writer of this same command may have won with its events and outcome. If a
		// row exists, it answers the command; if not, this is an ordinary conflict (or transient
		// refusal) to retry.
		resolved, err := c.reread(ctx)
		if errors.Is(err, evt.ErrIdempotencyKeyReuse) {
			return nil, err
		}

		if err == nil && resolved != nil {
			return resolved, nil
		}

		return nil, commitErr
	}

	return nil, commitErr
}

// recover resolves an ambiguous write by re-reading the outcome row (storage spec,
// Ambiguous-write recovery): the same digest means the commit won; a different digest is
// IdempotencyKeyReuse; an absent row proves the commit did not apply, so the same command ID is
// safe to run again. The handler never runs here.
func (c *command) recover(ctx context.Context, commitErr error) (*evt.CommandOutcome, error) {
	var ambiguous *evt.AmbiguousWriteError

	cause := commitErr
	if errors.As(commitErr, &ambiguous) {
		cause = ambiguous.Cause
	}

	resolved, err := c.reread(ctx)
	if errors.Is(err, evt.ErrIdempotencyKeyReuse) {
		return nil, err
	}

	if err != nil {
		return nil, &evt.AmbiguousWriteError{CommandID: c.id, Cause: errors.Join(cause, err)}
	}

	if resolved != nil {
		return resolved, nil
	}

	// Safe to retry: the caller can run the same command again, so policy.ExecuteWithRetry may.
	return nil, &policy.ClassifiedError{
		Class: policy.ClassTransient,
		Err:   &evt.AmbiguousWriteError{CommandID: c.id, Cause: cause, NotApplied: true},
	}
}

// reread reads the command's outcome row on a context detached from the caller's cancellation and
// resolves it against the command's digest. It returns nil, nil when the row is absent, and
// *evt.IdempotencyKeyReuseError when the stored digest differs.
func (c *command) reread(ctx context.Context) (*evt.CommandOutcome, error) {
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recoveryTimeout)
	defer cancel()

	stored, err := c.repo.GetOutcome(readCtx, c.id)
	if err != nil {
		return nil, err
	}

	if stored == nil {
		return nil, nil
	}

	return c.resolve(*stored)
}
