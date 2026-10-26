package snapshots

import (
	"context"
	"errors"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/policy"
)

// Prepare is one attempt's decision step of a multi-stream command (ExecuteTransaction). It
// receives a fresh Transaction already begun for the command and does what a handler does: Read
// the aggregates it needs, decide, Append events to any of them, and add caller-stamped fences
// with Fence. It returns the response to store with the accepted outcome, or an error:
//
//   - an error that maps to a rejection (evt.RejectionOf) records a rejected outcome, fenced on
//     everything it read, so the command is only rejected on state that still holds. The events
//     staged with Append are discarded, unless the error is marked with evt.RejectWithEvents: then
//     they are committed in the same atomic commit (Transaction.RejectWithEvents);
//   - any other error is an internal failure, returned with no outcome written.
//
// Returning nil with nothing appended records an accepted no-op.
//
// It runs again, from scratch, whenever the engine re-prepares the command, so it must be safe to
// repeat: it decides from the Transaction's reads, not from state captured by an earlier call, and
// its external side effects must be idempotent. Only the attempt that commits writes anything.
// The streams it reads and writes may differ between attempts, because they may depend on the
// state it reads; they are not part of the command's identity.
type Prepare func(ctx context.Context, tx *Transaction) (response []byte, err error)

// ExecuteTransaction runs a multi-stream command on a store with outcomes enabled under
// fence-aware retry (ADR 0004, C5). Each attempt begins the command (BeginCommand: it validates
// the command ID, computes the identity digest from identity and the metadata's principal, and
// reads the outcome row), runs prepare against a fresh Transaction, and commits it with
// Complete or Reject. A stored outcome with the same digest is returned without running prepare.
//
// When the commit conflicts with another writer, the command is re-prepared — prepare runs again
// against freshly loaded state, after a backoff — up to the store's RetryPolicy (WithRetry),
// provided the failed item is one the engine can re-derive: an event put, a floor or snapshot
// condition, a continuity check, or an engine-filled fence. A failed caller-stamped fence is not
// retried: the engine records the stale-fence rejection at once (Transaction.Complete). When the
// last attempt also conflicts, it records a stale-fence rejection (evt.RetriesExhaustedRejection)
// instead of returning the conflict, so a replay of the command ID gets the same answer; the
// remedy is a new command ID.
//
// The identity is the caller's intent, fixed before anything is read: its Targets are the streams
// the request names, not the streams prepare ends up writing, so a command whose write set
// depends on state — a rename releases whichever handle the entity currently holds — keeps one
// identity across attempts, client retries, and recovery.
func (store *Store) ExecuteTransaction(
	ctx context.Context,
	identity evt.CommandIdentity,
	metadata evt.Metadata,
	prepare Prepare,
) (*evt.CommandOutcome, error) {
	if prepare == nil {
		return nil, &evt.InvalidArgumentError{Argument: "prepare", Message: "a prepare function is required"}
	}

	// The identity and its digest are fixed once, before the first attempt.
	cmd, err := store.newCommand(identity, metadata)
	if err != nil {
		return nil, err
	}

	state := attemptState{cmd: cmd}
	loop := attemptLoop{retry: store.retry}

	result, err := loop.run(ctx, func(int) (executed, error) {
		replayed, perr := cmd.preCheck(ctx)
		if perr != nil || replayed != nil {
			return executed{outcome: replayed}, perr
		}

		tx := store.Transaction()
		tx.command = cmd

		response, rerr := prepare(ctx, tx)
		if rerr != nil {
			return prepareFailed(ctx, tx, rerr)
		}

		outcome, cerr := tx.Complete(ctx, response)

		return executed{outcome: outcome}, cerr
	}, state.retriesExhausted)

	return result.outcome, err
}

// prepareFailed records or returns the error a Prepare function returned.
func prepareFailed(ctx context.Context, tx *Transaction, err error) (executed, error) {
	if rejection, ok := evt.RejectionOf(err); ok {
		reject := tx.Reject
		if evt.RejectsWithEvents(err) {
			reject = tx.RejectWithEvents
		}

		outcome, rerr := reject(ctx, rejection)

		return executed{outcome: outcome, handlerErr: err}, rerr
	}

	// A failed Read the function passed on: a load that raced a compaction found a sequence gap
	// (reloading resolves it), or the backend was momentarily unavailable. It is retried like a
	// transient failure but never exhausts into a rejection: it says the read failed, not that the
	// decision went stale.
	if tx.readErr != nil && errors.Is(err, tx.readErr) &&
		(evt.IsOptimisticConflict(tx.readErr) || policy.IsTransient(tx.readErr)) {
		return executed{}, load(err)
	}

	return executed{}, internal(err)
}
