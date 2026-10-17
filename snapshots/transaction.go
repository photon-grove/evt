package snapshots

import (
	"context"
	"errors"
	"fmt"

	"github.com/photon-grove/evt"
)

// Transaction prepares one atomic multi-stream commit (ADR 0004, C1/C2). A caller loads a typed
// read set with Read, appends events to any subset with Append, adds caller-stamped fences with
// Fence, and commits once with Commit. Streams that were read but never written contribute
// engine-filled read fences, so the commit fails when another writer moved them past the head
// the reader saw.
//
// The single-aggregate shortcut stays: Store.Execute remains a load, handle, and commit of one
// entity, implemented over the same repository commit path.
//
// A Transaction commits at most once: Commit consumes it whether it succeeds or fails, and a
// consumed Transaction refuses every later call. Errors from the staging calls split in two.
// Input validation errors leave the Transaction unchanged and usable: a nil entity or a duplicate
// stream passed to Read, and an Append to a stream that was not read. Errors that can leave the
// staged state inconsistent consume it: a failed load in Read, and an event of the wrong entity
// type or one the entity cannot apply in Append. Build a new Transaction after those.
//
// The store's repository must implement evt.TransactionalRepository. Read and Commit refuse one
// that does not with *evt.TransactionsUnsupportedError (errors.Is evt.ErrTransactionsUnsupported)
// before anything is loaded or written, and consume the Transaction.
//
// On a store with outcomes enabled (WithOutcomes) begin the transaction with BeginCommand and
// finish it with Complete or Reject instead of Commit, so the command is recorded as an outcome
// in the same atomic commit.
type Transaction struct {
	store   *Store
	streams map[evt.EntityID]*transactionStream
	order   []evt.EntityID
	fences  []evt.ReadFence
	used    bool

	// command is set by BeginCommand on a store with outcomes enabled.
	command *command
}

// transactionStream is one read aggregate and the events appended to it since the read.
type transactionStream struct {
	entity  evt.Entity
	context evt.Context
	pending []evt.Event
}

// Transaction begins a new multi-stream transaction on the store.
func (store *Store) Transaction() *Transaction {
	return &Transaction{
		store:   store,
		streams: make(map[evt.EntityID]*transactionStream),
	}
}

// Read loads an aggregate into the transaction's read set. The entity instance is filled from
// the snapshot and events like Store.LoadEntity, so appended events keep its snapshot payload
// consistent. A stream that receives no events before Commit is fenced at the head it was read
// at; a stream that receives events is written.
func (tx *Transaction) Read(ctx context.Context, entity evt.Entity, entityID evt.EntityID) error {
	if tx.used {
		return fmt.Errorf("snapshots: transaction is already consumed")
	}
	if err := evt.RequireTransactions(tx.store.repo); err != nil {
		tx.used = true

		return err
	}
	if entity == nil {
		return fmt.Errorf("snapshots: transaction read of %s has a nil entity", entityID)
	}
	if _, exists := tx.streams[entityID]; exists {
		return fmt.Errorf("snapshots: duplicate read of %s", entityID)
	}

	context, err := tx.store.LoadEntity(ctx, entity, entityID)
	if err != nil {
		tx.used = true

		return err
	}

	// Keep the entity's internal ID aligned with the store key, as Execute does, so projectors
	// write views to the right partition.
	if setter, ok := entity.(interface{ SetID(evt.EntityID) }); ok {
		setter.SetID(entityID)
	}

	tx.streams[entityID] = &transactionStream{entity: entity, context: context}
	tx.order = append(tx.order, entityID)

	return nil
}

// Append applies domain events to an already-read stream, preparing its post-commit state. The
// events are committed only when Commit succeeds.
func (tx *Transaction) Append(entityID evt.EntityID, events ...evt.Event) error {
	if tx.used {
		return fmt.Errorf("snapshots: transaction is already consumed")
	}

	stream, ok := tx.streams[entityID]
	if !ok {
		return fmt.Errorf("snapshots: stream %s was not read by this transaction", entityID)
	}

	// A command records the identity digest of the streams it declared. Writing any other stream
	// would commit an effect that digest omits, so a later call with the declared identity would
	// replay an outcome for it. Nothing is changed, so the transaction stays usable.
	if tx.command != nil && len(events) > 0 {
		if _, declared := tx.command.declared[entityID]; !declared {
			return &evt.InvalidCommitError{Reason: fmt.Sprintf(
				"stream %s is written but was not declared by the command's identity", entityID,
			)}
		}
	}

	for _, event := range events {
		if event.EntityType() != stream.entity.Type() {
			tx.used = true

			return fmt.Errorf("snapshots: event type %s does not belong to stream %s", event.EntityType(), entityID)
		}
		if err := stream.entity.Apply(event); err != nil {
			tx.used = true

			return err
		}
	}

	stream.pending = append(stream.pending, events...)

	return nil
}

// Fence adds a caller-stamped read fence for a stream the engine did not load. The fence fails
// the commit when another writer moved the stream past the given head. A fence for a stream
// this transaction read is dropped, whether Fence is called before or after Read: the engine's
// own fence provenance supersedes it.
func (tx *Transaction) Fence(fence evt.ReadFence) error {
	if tx.used {
		return fmt.Errorf("snapshots: transaction is already consumed")
	}
	if _, exists := tx.streams[fence.EntityID]; exists {
		return nil
	}

	fence.CallerStamped = true
	tx.fences = append(tx.fences, fence)

	return nil
}

// Commit stamps the commit ID once, prepares every written stream's events, projector
// operations, and snapshot, fences every unwritten read, and persists exactly one atomic
// repository commit. On failure nothing is written.
//
// A store with outcomes enabled refuses Commit: use BeginCommand, then Complete or Reject.
func (tx *Transaction) Commit(ctx context.Context, metadata evt.Metadata) ([]evt.SerializedEvent, error) {
	if tx.used {
		return nil, fmt.Errorf("snapshots: transaction is already consumed")
	}
	tx.used = true

	if tx.store.outcomes != nil {
		return nil, errOutcomesNeedCommand
	}

	// Fail closed before dispatch: a repository that has not declared atomic transactions could
	// drop the fences and snapshots this result carries, or write only some streams.
	if err := evt.RequireTransactions(tx.store.repo); err != nil {
		return nil, err
	}

	metadata = evt.CommitMetadata(metadata)

	result, err := tx.prepare(ctx, metadata)
	if err != nil {
		return nil, err
	}

	normalized, err := evt.NormalizeCommit(result)
	if err != nil {
		return nil, err
	}

	if len(normalized.Events) == 0 && len(normalized.Snapshots) == 0 && len(normalized.Fences) == 0 {
		return nil, nil
	}

	if err := tx.store.repo.Commit(ctx, normalized); err != nil {
		return nil, err
	}

	return normalized.Events, nil
}

// BeginCommand starts a transaction for one command on a store with outcomes enabled. It checks
// the command ID, computes the identity digest from the declared command (its type, the streams it
// declares it will write, and its payload) and the metadata's principal, and reads the outcome row
// before anything is loaded:
//
//   - a stored outcome with the same digest is returned and the transaction is nil — the command
//     already ran, and nothing should run again;
//   - a stored outcome with a different digest is *evt.IdempotencyKeyReuseError;
//   - otherwise the returned transaction is ready for Read, Append, and Fence, and must be finished
//     with Complete or Reject.
func (store *Store) BeginCommand(
	ctx context.Context,
	identity evt.CommandIdentity,
	metadata evt.Metadata,
) (*Transaction, *evt.CommandOutcome, error) {
	cmd, replayed, err := store.beginCommand(ctx, identity, metadata)
	if err != nil || replayed != nil {
		return nil, replayed, err
	}

	tx := store.Transaction()
	tx.command = cmd

	return tx, nil, nil
}

// Complete commits the transaction and records an accepted outcome carrying the response, in the
// same atomic commit as the staged events, snapshots, and fences. A transaction that appended
// nothing records an accepted no-op: the fences of its reads are still checked.
//
// A failed caller-stamped fence is a stale-fence rejection: the engine records it as the command's
// outcome through an outcome-only transaction and returns it. Every other conflict, including a
// failed engine-filled fence, is returned for the caller to retry with a fresh BeginCommand. A lost
// race on the outcome row and an ambiguous commit result are resolved by re-reading the row.
func (tx *Transaction) Complete(ctx context.Context, response []byte) (*evt.CommandOutcome, error) {
	return tx.finish(ctx, evt.OutcomeAccepted, response, nil)
}

// Reject records the command as rejected, with the given domain rejection, in an outcome-only
// commit. No staged events are written. The fences of the transaction's reads are still checked, so
// a rejection is only recorded if the state it was decided on still holds. Use evt.RejectionOf to
// map a handler error to a rejection and return any other error to the caller instead.
func (tx *Transaction) Reject(ctx context.Context, rejection evt.Rejection) (*evt.CommandOutcome, error) {
	return tx.finish(ctx, evt.OutcomeRejected, nil, &rejection)
}

func (tx *Transaction) finish(
	ctx context.Context,
	status evt.OutcomeStatus,
	response []byte,
	rejection *evt.Rejection,
) (*evt.CommandOutcome, error) {
	if tx.used {
		return nil, fmt.Errorf("snapshots: transaction is already consumed")
	}
	tx.used = true

	if tx.command == nil {
		return nil, &evt.InvalidArgumentError{
			Argument: "transaction",
			Message:  "begin the transaction with Store.BeginCommand to record a command outcome",
		}
	}

	if err := evt.RequireTransactions(tx.store.repo); err != nil {
		return nil, err
	}

	// A pre-v0.9 event in a stream this command read already carries its ID without an outcome
	// row: the command ran, so no outcome — accepted, rejected, or stale-fence — is recorded for it.
	if err := tx.checkLegacyDuplicate(); err != nil {
		return nil, err
	}

	metadata := evt.CommitMetadata(tx.command.metadata)

	var (
		result evt.SerializedResult
		err    error
	)

	if status == evt.OutcomeRejected {
		// Rejections write no events, but keep checking the fences of everything the decision read.
		result = tx.fencedReads()
	} else if result, err = tx.prepare(ctx, metadata); err != nil {
		return nil, err
	}

	outcome, err := tx.command.commit(ctx, result, status, response, rejection)

	var conflict *evt.FenceConflictError
	if errors.As(err, &conflict) && conflict.Fence.CallerStamped {
		return tx.command.staleFence(ctx)
	}

	return outcome, err
}

// checkLegacyDuplicate applies the load-time duplicate window of the streams the transaction read.
func (tx *Transaction) checkLegacyDuplicate() error {
	id := tx.command.id

	for _, entityID := range tx.order {
		if tx.streams[entityID].context.HasCommandID(id) {
			return evt.NewDuplicateCommandError(id)
		}
	}

	return nil
}

// staleFence records the engine's stale-fence rejection: a caller-stamped fence went stale, so the
// engine cannot know the caller's decision still holds. The outcome-only transaction carries none
// of the failed commit's other items — no fences (they are stale by definition), events, or
// snapshots — and the old command ID stays terminal; the remedy is a new command ID.
func (c *command) staleFence(ctx context.Context) (*evt.CommandOutcome, error) {
	rejection := evt.Rejection{Code: evt.RejectionStaleFence, Message: "a caller-stamped fence went stale"}

	return c.commit(ctx, evt.SerializedResult{}, evt.OutcomeRejected, nil, &rejection)
}

// fencedReads returns an event-free result that carries a read fence for every stream the
// transaction read, plus the caller-stamped fences.
func (tx *Transaction) fencedReads() evt.SerializedResult {
	result := evt.SerializedResult{}

	for _, fence := range tx.fences {
		if _, read := tx.streams[fence.EntityID]; !read {
			result.Fences = append(result.Fences, fence)
		}
	}

	for _, entityID := range tx.order {
		stream := tx.streams[entityID]

		result.Fences = append(result.Fences, evt.ReadFence{
			EntityID:   entityID,
			EntityType: stream.entity.Type(),
			Head:       streamHead(stream.context),
		})
	}

	return result
}

// prepare serializes every written stream's events, projector operations, and snapshot, and fences
// every unwritten read, into one repository result.
func (tx *Transaction) prepare(ctx context.Context, metadata evt.Metadata) (evt.SerializedResult, error) {
	// A caller-stamped fence for a stream this transaction read is superseded by the engine's own
	// fence (or by the write), whichever order Fence and Read were called in.
	result := evt.SerializedResult{}
	for _, fence := range tx.fences {
		if _, read := tx.streams[fence.EntityID]; !read {
			result.Fences = append(result.Fences, fence)
		}
	}

	for _, entityID := range tx.order {
		stream := tx.streams[entityID]

		if metadata.CommandID != nil && stream.context.HasCommandID(*metadata.CommandID) {
			return evt.SerializedResult{}, evt.NewDuplicateCommandError(*metadata.CommandID)
		}

		if len(stream.pending) == 0 {
			// An unwritten read is fenced at the head it saw: the larger of its highest event
			// sequence and its snapshot's event sequence.
			result.Fences = append(result.Fences, evt.ReadFence{
				EntityID:   entityID,
				EntityType: stream.entity.Type(),
				Head:       streamHead(stream.context),
			})

			continue
		}

		serialized, snapshots, err := tx.store.stage(stream.entity, &stream.context, entityID, stream.pending, metadata)
		if err != nil {
			return evt.SerializedResult{}, err
		}

		projectorTransaction, err := buildProjectorTransaction(ctx, stream.entity, stream.pending)
		if err != nil {
			return evt.SerializedResult{}, err
		}

		result.Transaction = evt.MergeTransactions(result.Transaction, projectorTransaction)
		result.Events = append(result.Events, serialized...)
		result.Snapshots = append(result.Snapshots, snapshots...)
	}

	return result, nil
}

// streamHead returns the head a fenced read observed: the loaded context's current event
// sequence. LoadEntity seeds it from the snapshot's event sequence and advances it over later
// events, so it is already the larger of the stream's highest event sequence and its snapshot's
// event sequence — including a compacted stream, whose events a snapshot covers.
func streamHead(context evt.Context) evt.EventSequence {
	if context.CurrentSequence == nil {
		return 0
	}

	return *context.CurrentSequence
}
