# Command outcomes and retries

A plain command ID catches a duplicate while the original events are still in the log. Command
outcomes go further. Enable them on a store with `WithOutcomes`, and every command needs a command
ID and gets recorded in the same transaction as its events. The record is a reserved
`command-outcome#<id>` row, written once, in its final state. A retried command then returns the
original answer instead of running twice, including the response bytes your handler set in
`CommandResult.Response`. And the check still covers IDs that were folded into a snapshot long ago.

```go
store := snapshots.NewStore(repo, 5).WithOutcomes()

outcome, err := store.ExecuteWithOutcome(ctx, account, id, &Deposit{Cents: 500},
    evt.NewMetadata(ctx, nil, evt.WithCommandID("req-123")))
switch {
case errors.Is(err, evt.ErrIdempotencyKeyReuse):
    // "req-123" already belongs to a different command (a different identity digest)
case err != nil:
    // an internal failure: nothing was recorded, so the same call can be retried
case outcome.Rejected():
    _ = outcome.Rejection // {Code, Message, Details}, replayed verbatim on a retry
default:
    _ = outcome.Response // the stored response, whether just produced or replayed
    _ = outcome.Replayed // true when the stored outcome was returned and nothing ran
}
```

A repository opts in by implementing `evt.OutcomeRepository`. The in-memory, DynamoDB and PostgreSQL
repositories all do. PostgreSQL keeps the rows in `{events}__evt_command_outcomes`, which
`EnsureSchema` creates. On an upgraded log, command IDs recorded before outcomes were enabled keep
only the old window check (see the [storage
format](../reference/storage-format.md#command-outcomes)). The bare `Store.Commit` doesn't record
outcomes, and it refuses them.

## Identity

Before a command runs, the store computes its identity digest: SHA-256 over the command's type, the
target streams the request names, the canonical payload JSON and the principal. The same ID with the
same digest replays. The same ID with a different digest is `*evt.IdempotencyKeyReuseError`, which
carries the stored outcome. You can supply your own digest with `snapshots.WithDigester`.

The identity is the caller's intent. The streams a command goes on to read and write aren't part of
it, because they can depend on state and change between attempts. A rename that releases whichever
handle the aggregate currently holds keeps one identity across attempts, because only the targets in
its `evt.CommandIdentity` are digested.

The flip side is a contract. Everything that distinguishes the request has to be in `Targets` or the
payload. A transfer whose destination is chosen only inside `Prepare` has the same identity whichever
destination it picks. So a second request with the same command ID and a different destination
replays the first one's outcome instead of running. Put the destination in the payload, or in the
targets.

## Rejections

A handler error that implements `evt.Rejectioner` is persisted as a rejected outcome.
`NotFoundError` and `BadCommandError` both implement it, with the engine codes `not-found` and
`bad-command`. Any other error (a bad event, an encoding or storage failure) is an internal failure.
It's returned and nothing is recorded, so a retry after the fix starts clean. The error-returning
`Execute` reports a rejection as `*evt.RejectionError`, which matches `errors.Is(err,
evt.ErrRejected)`.

Some refusals still have to be recorded in the domain. A proposal whose apply finds its base
revision stale should answer the client with a rejection and also leave an `apply-failed` event on
the proposal. That way the aggregate is no longer "applicable" and the history shows the attempt.
Return the events beside the error and mark the error with `evt.RejectWithEvents`. They commit in
the same atomic commit as the rejected row, and the row lists them.

```go
func (p *Proposal) Handle(ctx context.Context, command evt.Command) (evt.CommandResult, error) {
    // ... the base revision is stale:
    return evt.CommandResult{Events: []evt.Event{ApplyFailed{Base: apply.Base}}},
        evt.RejectWithEvents(&BaseStale{Base: apply.Base}) // a Rejectioner
}
```

In a `Prepare` function, stage the events with `tx.Append` and return the marked error, or finish a
transaction by hand with `Transaction.RejectWithEvents`. A plain rejection still discards whatever
was staged. Everything an accepted commit enforces applies here too: continuity and fences, reserved
streams, projectors, snapshots and the commit budget. A replay returns the stored rejection without
running the handler or writing the events again. A conflict re-prepares the command, which may then
decide differently and record no failure at all. If the attempts run out, the `stale-fence`
rejection is recorded without your events. A rejection stores no response.

A command that produces no events, or returns `NoOpError`, is recorded as accepted with no events.
That's a durable outcome you can observe, not an error.

## Ambiguous writes

Sometimes a commit's result is unknown, after a timeout or a lost connection. The store then reads
the outcome row again instead of running the command again. A stored outcome is returned. An absent
row is polled for a bounded window (`snapshots.WithRecoveryWait`, 10 s by default, on a context
detached from the caller's cancellation). A row that never appears becomes a retryable
`*evt.AmbiguousWriteError` (`errors.Is(err, evt.ErrAmbiguousWrite)`, transient). It's never reported
as "not applied", because a transaction can land after any single read. So retrying the same command
ID is safe. `Store.ResolveCommand` runs the same read for a caller that lost the result entirely.

A lost race on the outcome row is resolved the same way and never surfaces as a generic conflict.
The loser returns only the winner's stored outcome. A first execution returns its rejection in the
stored (decoded) form, so its `Details` equal a replay's.

## Limits

A few inputs are checked before anything is read or written. A command ID, principal and command
type must be valid UTF-8, and a custom digest must be non-empty. U+0000 is refused on every backend
in a command ID, a digest, and a rejection's code, message and details. A command ID is at most 512
UTF-8 bytes (`*evt.CommandIDTooLongError`). The response and the encoded rejection are each at most
64 KiB (`*evt.OutcomeTooLargeError`), measured with numbers in canonical text. The outcome row counts
in the commit budget.

Numbers in a rejection's details are stored in the canonical text PostgreSQL's `jsonb` prints (no
exponent, no sign on zero), so they read back the same on every backend. A number PostgreSQL's
numeric type can't hold is refused as an invalid argument.

## Multi-stream commands

A command that touches several streams uses `Store.ExecuteTransaction`, which runs your decision step
(`snapshots.Prepare`) under fence-aware retry. The lower-level alternative is `Store.BeginCommand`.
It reads the outcome row first, then you use the usual `Read`, `Append` and `Fence`, and then finish
with `Transaction.Complete` (accepted, or a no-op when nothing was appended) or `Transaction.Reject`.
That path attempts once. A failed caller-stamped fence is persisted as a `stale-fence` rejection, and
the remedy is a new command ID.

```go
identity := evt.CommandIdentity{
    Type: "profile:rename", Targets: []evt.EntityID{profileID}, Payload: rename,
}

outcome, err := store.ExecuteTransaction(ctx, identity, metadata,
    func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
        profile := NewProfile(profileID)
        if err := tx.Read(ctx, profile, profileID); err != nil {
            return nil, err
        }

        // The handle this profile holds now decides which claim stream is released.
        released, taken := ClaimID(profile.Handle), ClaimID(rename.Handle)
        // ... Read both claims, check the new handle is free (return a Rejectioner if not),
        // then Append to the profile and to both claims.
        return nil, nil
    })
```

## Fence-aware retry

`ExecuteWithOutcome`, `Execute` on an outcomes store, and `ExecuteTransaction` all retry a command
that conflicts with another writer. The policy is `snapshots.RetryPolicy`, set with
`Store.WithRetry`. The default is 5 attempts, with delays starting at 25 ms and doubling up to 1 s,
jittered. Unset delays take those defaults, `snapshots.ImmediateRetryPolicy` skips the waiting for
tests, and `Deadline` bounds the whole command. With the defaults the waits add up to about 0.4 s.
But an ambiguous write that never resolves costs a recovery window per attempt.

Fences come in two kinds. An engine-filled fence is one the engine adds for a stream the command read
but didn't write, at the head it loaded. A caller-stamped fence is one your `Prepare` adds itself
with `Fence`. A conflict on anything the engine can derive again (an event put, a snapshot or
continuity check, an engine-filled fence) re-prepares the command, meaning it runs the decision
again from fresh state. The aggregate is reset and loaded again, the handler
or your `Prepare` runs again against fresh state, and only the attempt that commits writes anything.
A failed caller-stamped fence fails fast with the `stale-fence` rejection instead, because the engine
can't know whether your decision still holds.

When the last attempt also conflicts, the engine persists a `stale-fence` rejection
(`evt.RetriesExhaustedRejection`) as the command's outcome, so a replay of the ID gets the same
answer. Submit a new command ID to try again. A transient failure that isn't a conflict (throttling,
or an ambiguous write that never resolved) is retried the same way but returned as an error when the
attempts run out.

This means handlers and `Prepare` functions can run more than once, and they have to be safe to
repeat. The store resets the aggregate between attempts from a deep copy of its exported fields. So
keep the state `Apply` changes in exported fields, and don't mutate the command in a handler.
