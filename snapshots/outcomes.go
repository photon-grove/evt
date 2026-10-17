package snapshots

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
)

// The outcome reads that settle an ambiguous write run on a context detached from the caller's
// cancellation, because the usual reason for an ambiguous write is exactly a cancelled or timed-out
// context. A single read is bounded by readTimeout; polling for a row that has not appeared yet
// backs off from recoveryFirstDelay to recoveryMaxDelay within the configured recovery wait.
const (
	readTimeout         = 10 * time.Second
	defaultRecoveryWait = 10 * time.Second
	recoveryFirstDelay  = 50 * time.Millisecond
	recoveryMaxDelay    = time.Second
)

// errOutcomesNeedCommand refuses a bare Commit on a store with outcomes enabled: the outcome row
// needs the command's identity (its type, target streams, and payload), which only the
// command-level entry points have.
var errOutcomesNeedCommand = &evt.InvalidArgumentError{
	Argument: "commit",
	Message:  "this store records command outcomes; execute the command with Execute or ExecuteWithOutcome, or use a Transaction begun with BeginCommand",
}

// outcomeConfig is a store's command-outcome configuration.
type outcomeConfig struct {
	digester     evt.Digester
	recoveryWait time.Duration
}

// OutcomeOption configures command outcomes on a store.
type OutcomeOption func(*outcomeConfig)

// WithDigester replaces the default identity digest (SHA-256 over the canonical encoding of the
// command type, target streams, payload JSON, and principal). A custom digester returns
// its own opaque string; the engine compares digests for equality only, and a digester that is
// not deterministic turns every retry into an IdempotencyKeyReuse error.
func WithDigester(digester evt.Digester) OutcomeOption {
	return func(config *outcomeConfig) {
		if digester != nil {
			config.digester = digester
		}
	}
}

// WithRecoveryWait bounds how long ambiguous-write recovery polls the outcome row for a commit
// whose result is unknown, 10 seconds by default. If the row has not appeared when the window ends
// the command fails with a retryable *evt.AmbiguousWriteError.
func WithRecoveryWait(wait time.Duration) OutcomeOption {
	return func(config *outcomeConfig) {
		config.recoveryWait = max(wait, 0)
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
	config := &outcomeConfig{digester: evt.DefaultDigest, recoveryWait: defaultRecoveryWait}
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

	// discarded reports that this call applied events to the caller's entity and then threw them
	// away without committing (a stale-fence rejection after the last attempt), so the entity no
	// longer reflects durable state.
	discarded bool
}

// attemptState holds the command that records the stale-fence rejection when the policy runs out of
// attempts.
type attemptState struct{ cmd *command }

// retriesExhausted records the stale-fence rejection of a command whose last attempt conflicted.
func (state *attemptState) retriesExhausted(ctx context.Context, attempts int) (executed, error) {
	if state.cmd == nil {
		return executed{}, errors.New("snapshots: retries were exhausted before the command began")
	}

	outcome, err := state.cmd.staleFence(ctx, evt.RetriesExhaustedRejection(attempts))

	return executed{outcome: outcome, discarded: true}, err
}

// ExecuteWithOutcome loads the entity, handles the command, and commits the result together with
// its outcome row. It requires WithOutcomes and a command ID in the metadata.
//
// Every attempt reads the outcome row first. A stored outcome with the same identity digest is
// returned (Replayed) without running the handler; one with a different digest is
// *evt.IdempotencyKeyReuseError. Otherwise the handler runs:
//
//   - events: committed with an accepted outcome carrying CommandResult.Response;
//   - no events, or a *evt.NoOpError: an accepted no-op outcome;
//   - an error that maps to a rejection (evt.RejectionOf): a rejected outcome, returned as a
//     CommandOutcome with a nil error;
//   - any other error: an internal failure, returned as an error with no outcome written.
//
// Retry is fence-aware (ADR 0004, C5). When the commit conflicts with another writer, the entity
// is reset, loaded again, and the handler runs again against the fresh state, after a backoff,
// up to the store's RetryPolicy (WithRetry). A single-aggregate command fences nothing, so every
// conflict can be re-prepared. When the last attempt also conflicts, the engine records a
// stale-fence rejection (evt.RetriesExhaustedRejection) as the command's outcome and returns it,
// so a replay of the command ID gets the same answer; the remedy is a new command ID. The handler
// may therefore run several times for one command, but at most one attempt commits.
//
// A lost race on the outcome row and an ambiguous commit result are resolved by re-reading the
// row, never by re-preparing the command; see evt.AmbiguousWriteError for the one case, a row
// that never appeared, that is retried the same way.
//
// The entity is reset between attempts: its exported fields (the state a snapshot persists) are
// restored from a deep copy, so events the failed attempt applied are not counted again. Its
// unexported fields are restored shallowly, so an unexported map, slice, or pointer is shared with
// the failed attempt and keeps what it accumulated: keep state that Apply changes in exported
// fields. The command value is reused by every attempt, so a handler must not mutate it. An entity
// that is not a pointer to a struct cannot be reset, so its command is attempted once and a
// conflict is returned as an error.
func (store *Store) ExecuteWithOutcome(
	ctx context.Context,
	entity evt.Entity,
	entityID evt.EntityID,
	command evt.Command,
	metadata evt.Metadata,
) (*evt.CommandOutcome, error) {
	result, err := store.run(ctx, entity, entityID, command, metadata)

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
	result, err := store.run(ctx, entity, entityID, command, metadata)
	if err != nil {
		return err
	}

	if result.outcome.Rejected() {
		return &evt.RejectionError{Rejection: *result.outcome.Rejection, Err: result.handlerErr}
	}

	return nil
}

// run is executeOutcome plus the contract Execute has always had: the entity the caller supplied
// reflects durable state when the call returns. A replayed outcome (a duplicate command, or a lost
// race settled by the winner's row) returns before, or after discarding, this call's own work, and
// so does a stale-fence rejection after the last attempt, so the entity is restored to what the
// caller handed in and loaded again from storage.
func (store *Store) run(
	ctx context.Context,
	entity evt.Entity,
	entityID evt.EntityID,
	command evt.Command,
	metadata evt.Metadata,
) (executed, error) {
	reset, resettable := captureEntity(entity)

	result, err := store.executeOutcome(ctx, entity, entityID, command, metadata, reset, resettable)
	if err != nil || result.outcome == nil || (!result.outcome.Replayed && !result.discarded) {
		return result, err
	}

	reset()

	if _, err = store.LoadEntity(ctx, entity, entityID); err != nil {
		return result, err
	}

	if setter, ok := entity.(interface{ SetID(evt.EntityID) }); ok {
		setter.SetID(entityID)
	}

	return result, nil
}

// captureEntity remembers a pointer-to-struct entity's value so a later reset puts back what the
// caller supplied, discarding events applied locally. The copy is deep over the entity's exported
// fields — every map, slice, pointer, and nested struct — because those are exactly the state a
// snapshot persists, so state a constructor initialized (a labels map, say) cannot be aliased and
// kept dirty by Apply. Unexported fields are copied shallowly. Any other entity kind cannot be
// reset: resettable is false and reset does nothing.
func captureEntity(entity evt.Entity) (reset func(), resettable bool) {
	value := reflect.ValueOf(entity)
	if !value.IsValid() || value.Kind() != reflect.Pointer || value.IsNil() || value.Elem().Kind() != reflect.Struct {
		return func() {}, false
	}

	saved := deepCopy(value.Elem(), map[uintptr]reflect.Value{})

	// Each reset installs a fresh copy, so a later attempt cannot alias what an earlier one saved.
	return func() { value.Elem().Set(deepCopy(saved, map[uintptr]reflect.Value{})) }, true
}

// deepCopy returns a copy of v that shares no map, slice, or pointer storage with it, through its
// exported fields. seen breaks pointer cycles.
func deepCopy(v reflect.Value, seen map[uintptr]reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return v
		}

		if copied, ok := seen[v.Pointer()]; ok {
			return copied
		}

		fresh := reflect.New(v.Type().Elem())
		seen[v.Pointer()] = fresh
		fresh.Elem().Set(deepCopy(v.Elem(), seen))

		return fresh

	case reflect.Map:
		if v.IsNil() {
			return v
		}

		fresh := reflect.MakeMapWithSize(v.Type(), v.Len())
		for iterator := v.MapRange(); iterator.Next(); {
			fresh.SetMapIndex(deepCopy(iterator.Key(), seen), deepCopy(iterator.Value(), seen))
		}

		return fresh

	case reflect.Slice:
		if v.IsNil() {
			return v
		}

		fresh := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := range v.Len() {
			fresh.Index(i).Set(deepCopy(v.Index(i), seen))
		}

		return fresh

	case reflect.Array:
		fresh := reflect.New(v.Type()).Elem()
		for i := range v.Len() {
			fresh.Index(i).Set(deepCopy(v.Index(i), seen))
		}

		return fresh

	case reflect.Interface:
		if v.IsNil() {
			return v
		}

		fresh := reflect.New(v.Type()).Elem()
		fresh.Set(deepCopy(v.Elem(), seen))

		return fresh

	case reflect.Struct:
		fresh := reflect.New(v.Type()).Elem()
		fresh.Set(v) // unexported fields, and every field's shallow value, first

		for i := range v.NumField() {
			if field := fresh.Field(i); field.CanSet() {
				field.Set(deepCopy(v.Field(i), seen))
			}
		}

		return fresh

	default:
		return v
	}
}

// executeOutcome runs the command through fence-aware retry: each attempt resets the entity,
// reads the outcome row, loads, handles, and commits; a conflict runs the next attempt, and the
// last conflict is recorded as a stale-fence rejection.
func (store *Store) executeOutcome(
	ctx context.Context,
	entity evt.Entity,
	entityID evt.EntityID,
	command evt.Command,
	metadata evt.Metadata,
	reset func(),
	resettable bool,
) (executed, error) {
	identity := evt.CommandIdentity{Type: command.Type(), Targets: []evt.EntityID{entityID}, Payload: command}

	// The identity and its digest are fixed once, before the first attempt.
	cmd, err := store.newCommand(identity, metadata)
	if err != nil {
		return executed{}, err
	}

	state := attemptState{cmd: cmd}
	loop := attemptLoop{retry: store.retry, once: !resettable}

	return loop.run(ctx, func(attempt int) (executed, error) {
		if attempt > 1 {
			reset()
		}

		return store.attemptOutcome(ctx, cmd, entity, entityID, command)
	}, state.retriesExhausted)
}

// attemptOutcome is one attempt of a single-aggregate command: the pre-check, the load, the
// handler, and the commit. It prepares everything from the entity as the caller supplied it.
func (store *Store) attemptOutcome(
	ctx context.Context,
	cmd *command,
	entity evt.Entity,
	entityID evt.EntityID,
	command evt.Command,
) (executed, error) {
	replayed, err := cmd.preCheck(ctx)
	if err != nil {
		return executed{}, err
	}

	if replayed != nil {
		return executed{outcome: replayed}, nil
	}

	eventContext, err := store.LoadEntity(ctx, entity, entityID)
	if err != nil {
		return executed{}, load(err)
	}

	// Keep the entity's internal ID aligned with the store key, as Execute does.
	if setter, ok := entity.(interface{ SetID(evt.EntityID) }); ok {
		setter.SetID(entityID)
	}

	// The event window is superseded by the outcome row, but pre-v0.9 commands have no row: an
	// event that carries this command ID without an outcome is one of those.
	if eventContext.HasCommandID(cmd.id) {
		return executed{}, internal(evt.NewDuplicateCommandError(cmd.id))
	}

	result, err := entity.Handle(ctx, command)
	if err != nil {
		return store.handlerFailed(ctx, cmd, err)
	}

	for _, event := range result.Events {
		if err = entity.Apply(event); err != nil {
			return executed{}, internal(err)
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
		return executed{}, internal(err)
	}

	serialized, snapshots, err := store.stage(entity, &eventContext, entityID, result.Events, evt.CommitMetadata(cmd.metadata))
	if err != nil {
		return executed{}, internal(err)
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
		// An internal failure is returned as is: whatever the handler returned, it is not a
		// concurrency conflict, so retry never re-runs it.
		return executed{}, internal(handlerErr)
	}

	outcome, err := cmd.commit(ctx, evt.SerializedResult{}, evt.OutcomeRejected, nil, &rejection)

	// The handler's own error belongs to the rejection only when this call committed it. A lost
	// race returns the winner's stored outcome, and with it nothing of this call's decision.
	if err != nil || outcome.Replayed {
		return executed{outcome: outcome}, err
	}

	return executed{outcome: outcome, handlerErr: handlerErr}, nil
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

// newCommand validates the command ID and computes the identity digest, once. The digest is a
// snapshot of the request's intent taken before anything is read or run: fence-aware retry
// prepares the command several times, and every attempt, a client retry, and a recovery read must
// compare equal, so nothing is recomputed from the live command or its metadata afterwards.
func (store *Store) newCommand(identity evt.CommandIdentity, metadata evt.Metadata) (*command, error) {
	repo, err := store.outcomeRepository()
	if err != nil {
		return nil, err
	}

	if metadata.CommandID == nil {
		return nil, evt.CheckCommandID("")
	}

	if err = evt.CheckCommandID(*metadata.CommandID); err != nil {
		return nil, err
	}

	input, err := identity.DigestInput(metadata)
	if err != nil {
		return nil, err
	}

	digest, err := store.outcomes.digester(input)
	if err != nil {
		return nil, fmt.Errorf("compute command digest: %w", err)
	}

	// A bad digest is a programmer error found before anything is read or run.
	if err = evt.CheckDigest(digest); err != nil {
		return nil, err
	}

	// The command's metadata is snapshotted here: the ID and digest are frozen, so a caller that
	// changes its metadata (or anything it points to) before Complete or Reject must not change what
	// they write.
	return &command{
		store:    store,
		repo:     repo,
		id:       *metadata.CommandID,
		digest:   digest,
		metadata: metadata.Clone(),
	}, nil
}

// preCheck reads the command's outcome row first, strongly consistent: a stored outcome with the
// same digest is returned (Replayed), one with a different digest is
// *evt.IdempotencyKeyReuseError, and an absent row returns nil, nil: the command runs.
func (c *command) preCheck(ctx context.Context) (*evt.CommandOutcome, error) {
	stored, err := c.repo.GetOutcome(ctx, c.id)
	if err != nil {
		return nil, err
	}

	if stored == nil {
		return nil, nil
	}

	return c.resolve(*stored)
}

// beginCommand is newCommand followed by preCheck: the command to run, or the stored outcome it
// resolves to.
func (store *Store) beginCommand(
	ctx context.Context,
	identity evt.CommandIdentity,
	metadata evt.Metadata,
) (*command, *evt.CommandOutcome, error) {
	cmd, err := store.newCommand(identity, metadata)
	if err != nil {
		return nil, nil, err
	}

	replayed, err := cmd.preCheck(ctx)
	if err != nil || replayed != nil {
		return nil, replayed, err
	}

	return cmd, nil, nil
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
	// The rejection is canonicalized through its stored encoding before it is committed and
	// returned, so a first execution and a replay expose the same Details on every backend.
	if rejection != nil {
		canonical, err := evt.CanonicalRejection(*rejection)
		if err != nil {
			return nil, err
		}

		rejection = &canonical
	}

	// The response is copied: the caller (or a handler) may keep and reuse its buffer, and neither
	// the stored row nor the returned outcome may change with it.
	result.Outcome = &evt.Outcome{
		CommandID: c.id,
		Status:    status,
		Digest:    c.digest,
		Response:  bytes.Clone(response),
		Rejection: rejection,
	}

	normalized, err := evt.NormalizeCommit(result)
	if err != nil {
		return nil, err
	}

	if err = c.store.repo.Commit(ctx, normalized); err != nil {
		return c.settle(ctx, err)
	}

	return &evt.CommandOutcome{Outcome: cloneOutcome(*normalized.Outcome), Events: normalized.Events}, nil
}

// cloneOutcome copies the byte slice an outcome shares with the commit that produced it.
func cloneOutcome(outcome evt.Outcome) evt.Outcome {
	outcome.Response = bytes.Clone(outcome.Response)

	return outcome
}

// settle resolves a failed commit that carried the outcome.
func (c *command) settle(ctx context.Context, commitErr error) (*evt.CommandOutcome, error) {
	switch {
	case errors.Is(commitErr, evt.ErrOutcomeConflict):
		// Another writer recorded this command ID first. Re-preparing can never resolve it; the
		// winner's row does.
		resolved, err := c.reread(context.WithoutCancel(ctx))
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
		resolved, err := c.reread(context.WithoutCancel(ctx))
		if errors.Is(err, evt.ErrIdempotencyKeyReuse) || errors.Is(err, evt.ErrCorruptOutcome) {
			return nil, err
		}

		if err == nil && resolved != nil {
			return resolved, nil
		}

		return nil, commitErr
	}

	return nil, commitErr
}

// recover resolves an ambiguous write by polling the outcome row (storage spec, Ambiguous-write
// recovery) with bounded exponential backoff on a context detached from the caller's cancellation:
// the same digest means the commit won; a different digest is IdempotencyKeyReuse. A row still
// absent when the window ends proves nothing — the transaction may yet land — so it is returned
// as a retryable *evt.AmbiguousWriteError, never as "not applied". The handler never runs here.
func (c *command) recover(ctx context.Context, commitErr error) (*evt.CommandOutcome, error) {
	var ambiguous *evt.AmbiguousWriteError

	cause := commitErr
	if errors.As(commitErr, &ambiguous) {
		cause = ambiguous.Cause
	}

	// Safe to retry: every attempt reads the outcome row first and every write is conditional, so
	// fence-aware retry runs the same command ID again and finds the outcome if it lands.
	retryable := func(cause error) error {
		return &policy.ClassifiedError{
			Class: policy.ClassTransient,
			Err:   &evt.AmbiguousWriteError{CommandID: c.id, Cause: cause},
		}
	}

	// The whole recovery, including a read that hangs, ends one grace period after the window: the
	// last poll gets recoveryMaxDelay to answer, and no per-read timeout can extend past it.
	waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.store.outcomes.recoveryWait+recoveryMaxDelay)
	defer cancel()

	delay, waited := recoveryFirstDelay, time.Duration(0)

	for {
		resolved, err := c.reread(waitCtx)
		if errors.Is(err, evt.ErrIdempotencyKeyReuse) || errors.Is(err, evt.ErrCorruptOutcome) {
			return nil, err
		}

		if err != nil {
			return nil, retryable(errors.Join(cause, err))
		}

		if resolved != nil {
			return resolved, nil
		}

		if waited >= c.store.outcomes.recoveryWait {
			return nil, retryable(cause)
		}

		step := min(delay, c.store.outcomes.recoveryWait-waited)

		select {
		case <-waitCtx.Done():
			return nil, retryable(cause)
		case <-time.After(step):
		}

		waited += step
		delay = min(delay*2, recoveryMaxDelay)
	}
}

// reread reads the command's outcome row on ctx, which the caller has detached from cancellation,
// and resolves it against the command's digest. It returns nil, nil when the row is absent, and
// *evt.IdempotencyKeyReuseError when the stored digest differs.
func (c *command) reread(ctx context.Context) (*evt.CommandOutcome, error) {
	// ctx is already detached from the caller's cancellation (and, during recovery, bounded by the
	// recovery window): the per-read timeout derives from it, so a read can never outlive the window.
	// A read is also never given longer than the configured recovery window plus its grace, so a
	// short WithRecoveryWait bounds the conflict rereads as well.
	readCtx, cancel := context.WithTimeout(ctx, min(readTimeout, c.store.outcomes.recoveryWait+recoveryMaxDelay))
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
