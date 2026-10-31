# Projections and rebuilds

A projection is a read model derived from the event log. Its rows are deterministic: given the same
events, a projector writes the same rows, so any view table can be wiped and rebuilt by replaying
the log. This page covers the two ways evt writes views (per-entity projectors and commit-wide
projectors), how to rebuild them safely, and how to read them back without buffering.

You'd reach for `evt.RebuildProjections` when a projector bug wrote bad rows, when you add a new
view over streams that already exist, when a view's payload schema changes, or when you want to
check a view against the log. During a rebuild the repository streams entities, your replay
function reconstitutes each aggregate's state, and the projectors produce transaction groups. In
dry-run mode evt reports the work without writing anything. And the writes always go through the
`CommitGroup` you supply, so you choose the commit strategy that's safest for your backend.

## Fencing a rebuild

A rebuild rewrites view rows while live commits keep writing theirs, and the two can interleave
badly. A live commit can put a row that the rebuild's wipe then deletes, or recreate one the replay
hasn't reached yet. So evt fences rebuilds with a durable lock. A rebuild worker takes the lock for
the whole run, and while it's held every commit is refused with `*evt.RebuildInProgressError`
(`errors.Is(err, evt.ErrRebuildInProgress)`). It's a framework guarantee rather than a flag your
jobs pass around, and every repository provides it through `evt.RebuildFenceProvider`.

Here's the whole lifecycle:

```go
fence := repo.(evt.RebuildFenceProvider).RebuildFence()

lease, err := fence.Begin(ctx, "rebuild-2026-10", 5*time.Minute) // acquire, or take over an expired lease
// ... wipe and replay, renewing the lease while it runs:
lease, err = fence.Renew(ctx, lease, 5*time.Minute)
// ... write view batches under the lease guard (at most 99 operations each):
err = fence.WriteViews(ctx, lease, batch) // *evt.RebuildLeaseLostError: stop, you were replaced
// ... and release it:
_, err = fence.End(ctx, lease)
```

And here it is wrapped around `RebuildProjections`, with the failure path:

```go
fence := repo.(evt.RebuildFenceProvider).RebuildFence()

lease, err := fence.Begin(ctx, owner, 5*time.Minute)
if err != nil {
    // *evt.RebuildLockHeldError: another rebuild holds an unexpired lease.
    // *evt.RebuildBeginIndeterminateError: the begin may have taken the lock (see below).
    return err
}

result, err := evt.RebuildProjections(ctx, repo, applyEvent, cfg)

switch {
case err == nil && len(result.Errors) == 0:
    _, err = fence.End(ctx, lease) // the generation is even again: readers trust the views
default:
    // The views may be half-written. Releasing the lock lets the next run begin without waiting
    // for the lease, but readers stay fenced and commits stay refused until a rebuild completes.
    _, _ = fence.ForceRelease(ctx, owner) //nolint:errcheck // best effort; the lease also expires
}
```

The fence keeps a generation counter that's odd while a rebuild is in progress and even when the
views are settled. `Begin` raises it by one (by two for a takeover or after a force-release), and
`End` raises it by one more. A lease is both the owner token and the generation `Begin` returned,
which makes it a fencing token, meaning a value every later write must present so a stale holder
can't act after it's been replaced. `Renew`, `End` and `WriteViews` take the whole `evt.RebuildLease`,
so a worker whose lease lapsed can't renew, write for, or end the lease of a later incarnation
that took over under the same owner token. It gets `*evt.RebuildLeaseLostError` instead.

A `Begin` that times out, loses its response, or hits a server fault may have taken the lock
anyway. It returns `*evt.RebuildBeginIndeterminateError`, never a plain failure, with the `Lease`
the begin would have returned and the generation it `Observed`. Don't treat it as "not taken": a
lock nobody ends keeps every commit refused until it lapses and a later rebuild takes it over and
finishes. Read `fence.State(ctx)` and call `state.HoldsBegun(err.Lease, err.Observed)`. Held means
the begin applied, so carry on under that lease or `End` it, provided the owner token is your
run's own (two concurrent begins under one token can write the same lease). Settled but not held
means it can never apply, so begin again. Unsettled means it may still land. A throttled or cancelled `Begin` wrote nothing and keeps its
usual classification.

Renew well inside the lease you asked for, because a worker that pauses past its lease can be
replaced by a takeover. Write view batches through `fence.WriteViews` to have each one conditioned
transactionally on the lease, so a worker that lost the lock writes nothing. Writes you make
through your own `CommitGroup` aren't guarded that way.

A refused commit is transient, never a conflict. Fence-aware retry waits with a backoff and tries
again, then returns `*evt.RebuildInProgressError` when the attempts run out. It's never exhausted
into a `stale-fence` rejection, it records no outcome (a rejection decided during a rebuild isn't
recorded either), and the same command ID runs again once the rebuild ends. `policy.Classify`
reports it as transient.

Now the tradeoff. A crashed worker's lock keeps refusing commits after its lease expires, until
another rebuild takes it over with `Begin` and runs to its end. That's the cost of never letting a
commit write beside a rebuild. `ForceRelease(ctx, owner)`
is the operator's escape hatch for a wedged worker. It ends that worker's lease at once, so a new
rebuild can begin without waiting for it to lapse. But it leaves the generation odd and flagged,
and readers stay fenced and commits stay refused until a later rebuild begins and ends. The
force-released rebuild may have written only some of its batches, and a commit (a journaled one
above all) could build on that half-written state. To resume writes, run the rebuild again to its
end.

Readers fence their decisions against the generation too. A command that decides on a derived view
reads under an epoch, so a decision made on a half-rebuilt view fails instead of being recorded:

```go
err := evt.ReadAtRebuildEpoch(ctx, fence, func(ctx context.Context) error {
    row, err = views.Get(ctx, key) // the derived-view read
    return err
})
// *evt.RebuildUnavailableError: a rebuild is running (or a force-release left the views untrustworthy)
// *evt.RebuildEpochChangedError: a rebuild began and ended while the read ran; read again
```

`ReadAtRebuildEpoch` reads the generation before and after the read. `ObserveRebuildEpoch` and
`epoch.Verify` do the same in two steps if you need them apart. Return either error from your
handler. The engine returns it at once and records no outcome, and both are transient
(`policy.IsTransient`), so retry the command later. A rebuild that begins after the read is
stopped by the commit's lock check instead.

Leases are whole seconds on the repository's clock (`WithClock` in tests). The owner token is any
string up to 512 bytes without U+0000. On DynamoDB the fence lives in the reserved `SYSTEM#REBUILD`
and `SYSTEM#REBUILD#GENERATION` items, and on PostgreSQL in the `{events}__evt_rebuild_lock` and
`{events}__evt_rebuild_generation` tables, which `EnsureSchema` creates. The
[storage format](../reference/storage-format.md#rebuild-fence) has the exact rows and transactions.

## Commit-wide projectors

An `EventProjector` sees one entity at a time. A commit-wide projector (`evt.CommitProjector`) is a
pure function over everything one commit wrote, and it returns view operations that commit in the
same transaction as the events. That's what you want for read models that span entities: a
directory of unique names, a per-tenant ledger, an index that must never disagree with the streams
behind it. Register projectors with `snapshots.Store.WithCommitProjectors`.

The example is a directory of unique handles. Each handle name is a stream of its own,
`handle:<name>`, and that stream owns the row for its name for good. A command that claims a name
reads the handle stream and appends a `handle:claimed` event to it, in the same transaction as the
account's event. Two accounts racing for one name both read the handle stream at the same
sequence, so one loses to optimistic concurrency and retries against the new state. Releasing a
name appends `handle:released`, and the row is rewritten with a tombstone rather than deleted.

```go
type handles struct{}

func (handles) Name() string { return "handles" }

// The directory reads the events of handle streams, so it is history-dependent on that type.
func (handles) History() []evt.EntityType { return []evt.EntityType{"handle"} }

func (handles) ProjectCommit(_ context.Context, commit evt.CommitView) ([]evt.ViewOperation, error) {
    var operations []evt.ViewOperation

    for _, stream := range commit.Streams { // ascending by entity ID bytes
        if stream.EntityType != "handle" {
            continue // an account stream appears by ID and type only: it is not declared
        }

        latest := stream.Events[len(stream.Events)-1] // this commit's events, ascending by sequence

        // The row's owner is the handle stream. A release is a tombstone put, not a delete: the
        // row stays where the stream put it, so the next claimant, a later commit of the same
        // stream, rewrites it.
        operations = append(operations,
            evt.PutView("handles", nameOf(stream.EntityID), stream.EntityID, stream.EntityType, holderOf(latest)))
    }

    return operations, nil
}

store := snapshots.NewStore(repo, 5).WithCommitProjectors(handles{})
```

### What a projector sees

A projector is given an `evt.CommitView`: the commit ID and every stream the commit wrote,
ascending by entity-ID bytes. Each stream carries its entity type, the commit's events for it, and
its state after the commit (the JSON its snapshot would carry). Streams that were only read are
fenced rather than written, so they aren't in the view. A command that writes no event, such as a
plain rejection or an accepted no-op, runs no projector. A rejection that carries events projects
them just like an accepted command's. The view has no outcome context on purpose, because a
rebuild has no outcome row to give it.

`History()` decides which of two kinds a projector is, and each kind is given only what a rebuild
of that kind could give it again. A projector that declares no types is current-state. It gets
each stream's state after the commit and nothing else: no events and an empty commit ID. So it
can't key a row by commit or count a commit's events, and it suits rows that are a function of
current state, like a per-entity summary.

A projector that declares types is history-dependent. It gets the commit ID, plus the events and
state of the streams of those types. Every other stream appears by entity ID and type with neither,
because its history may have been compacted and the declaration doesn't cover it. Before the first
commit that runs it, the store durably adds those types to the feature manifest's
`historyDependent` set, so `CompactBelow` refuses them in every process. The commits a rebuild
replays have to stay. The repository must be able to make that declaration (`evt.HistoryDeclarer`,
which the in-memory, DynamoDB and PostgreSQL repositories implement), or the commit fails with
`*evt.HistoryDeclarationUnsupportedError`.

### Owners

Every operation names the stream that owns its row. The owner is an argument of
`PutView(pk, sk, owner, ...)` and `DeleteView(pk, sk, owner)`, and it must be a stream the commit
wrote. For a history-dependent projector it must also be of a type the projector declares in
`History()`, so the row can be rebuilt from the history the declaration protects. Otherwise the
operation fails with `*evt.InvalidViewOperationError`. The repository stores the owner as the row's
`entityID` and conditions every write to the row on it, puts and deletes alike, with or without a
presence condition. So a write by any other stream fails the commit with a
`*evt.ProjectorOperationError` (reason `ConditionalCheckFailed`) when it happens, not at some later
rebuild.

A row's owner must never change. The log keeps only one commit order, which is each stream's
sequence order. The writes to a row can be put back in order only while they all come from commits
that wrote the same stream. That's why a key never changes hands by a delete from one stream and a
put from another. The delete leaves the row absent, so the second stream's put would pass the
owner condition live, and a rebuild has no order between the two streams to tell which came last.
`evt.ReplayViews` refuses that case with `*evt.SharedViewKeyError`. To reuse a key, keep it with
its owner and rewrite the row as a tombstone, or make the key belong to a stream every claimant
writes, as the handle stream does above. Don't rely on commit IDs for ordering. They aren't causal.

Even a delete that finds nothing counts as a claim on the key. A delete by owner A of a row that
isn't there, followed by a put by owner B, is accepted live (the row is absent, so the owner
condition holds) but refused on replay, because the replay sees two owners touch one row.

### Operations and where they go

A projector returns puts and deletes of rows at any `pk`/`sk`, in the order they should be written.
A put overwrites unless it's `IfAbsent`, and a delete of a missing row succeeds unless it's
`IfPresent`. `OnConflict(err)` names the error to return when the condition fails. Projectors run
in the order `WithCommitProjectors` was called, each one's operations in emission order. They land
after the feature-marker and rebuild-lock checks and before the written streams
([layout](../reference/storage-format.md#writes)), and they count against the shared commit budget.
A single-stream commit has room for 97 events and view operations together, or 96 with a snapshot.

The engine checks every operation before any write, on every backend:

| Problem | Error |
| --- | --- |
| an empty, non-UTF-8 or NUL-bearing key | `*evt.InvalidViewOperationError` |
| a `pk` over 2,048 or an `sk` over 1,024 UTF-8 bytes | `*evt.ViewKeyTooLongError` |
| two operations on the same row | `*evt.DuplicateViewKeyError` |
| operations with no event behind them | `*evt.ProjectorOnlyCommitError` |
| a repository with nowhere to write them | `*evt.ViewsNotConfiguredError` |

Operations are never dropped. The last two rules follow from views being projections of committed
events: a commit that carries operations but no event isn't a projection of anything.

The repository writes the rows. Give the DynamoDB repository a views table with
`repo.WithViewsTable("views")` (string `pk` and `sk`, the same table `NewViewRepository` reads). Give
the PostgreSQL repository a writer with `repo.WithViewWriter(fn)`. evt defines no view schema
there, so your `fn` turns each operation into SQL for your own table, and it must enforce the owner
in that SQL (see [PostgreSQL](postgres.md#views)). The in-memory repository keeps the rows for
tests (`mem.Repository.View`, `Views`).

A repository has to declare that it writes views. It implements `evt.ViewWritingRepository`, whose
`WritesViews()` reports the capability (a decorator reports its wrapped repository's), and
`evt.HistoryDeclarer` for history-dependent projectors, or the commit is refused. A custom
`Repository` receives the operations as `SerializedResult.Views`. One that can't write them in the
commit's transaction must return an error rather than ignore them.

### When a condition fails

The commit writes nothing and returns `*evt.ProjectorOperationError`. It names the projector and
the operation and wraps the operation's mapped error, so `errors.Is(err, ErrHandleTaken)` works.
It isn't an optimistic-concurrency conflict and it isn't transient. Re-preparing the command can't
fix it, so fence-aware retry returns it at once and records no outcome. It also ranks last among
the ways a commit can fail. A lost outcome row, stream-head drift, a sequence gap, and any conflict
on a written stream or a fence are all reported ahead of it. A projector function's own error is
a different type, `*evt.ProjectorError`.

### Purity and rebuilds

A projector's rows must be a deterministic function of its `CommitView`. That rules out clocks,
random sources, the environment, store reads, state kept between calls, and any dependence on map
iteration order. Projectors run again on every fence-aware re-preparation of a command, so they
have to be safe to repeat anyway. Decisions and outside signals get recorded as events first, as
[keeping the log](concepts.md#keeping-the-log) explains.

For commit-wide projectors, today the only rebuild is an in-memory replay. You regroup the stored events with
`evt.GroupCommits` (ordered consistently with each stream's sequence), fold each stream's state
after its commit yourself, and run the projectors over the groups with `evt.ReplayViews`. For a
history-dependent projector that is the contract: the rows it returns equal the live rows. It's
also how to test one, and the end-to-end test in `go/snapshots/projectors_test.go` does exactly
that.

```go
groups, err := evt.GroupCommits(storedEvents) // ordered consistently with every stream's sequence
for _, group := range groups {
    for i, stream := range group.Streams {
        group.Streams[i].State = foldState(storedEvents, stream) // the state after this commit
    }
}

rows, err := evt.ReplayViews(ctx, projectors, groups)
// rows must equal the live rows, or err is *evt.SharedViewKeyError: the replay refuses a row two
// streams write instead of guessing the order.
```

For a current-state projector the same replay works, because the state is yours to supply. But
there's no engine entry point yet that rebuilds current-state rows from the streams' current state
alone. That state-based rebuild is specified in the
[storage format](../reference/storage-format.md#views) and lands with the rebuild ports. Until then,
treat a current-state projector's rows as reproducible only by replaying with folded state.
Nothing rebuilds the rows of a durable view table by itself today.

## Scanning the event log

`RebuildProjections` reads the whole log through `StreamEntities`, which is backed by a DynamoDB
`Scan`. A scan makes no ordering promises, so `StreamEntities` groups every matched event by entity
and applies each entity's events in sequence order before yielding it. That means it buffers the
matched events in memory for the length of the scan. It's a rebuild and diagnostic path, not a hot
read path.

For large tables, give the DynamoDB repository a parallel scan before you hand it to
`RebuildProjections`:

```go
repo := dynamo.NewRepository(client, eventsTable).WithScanSegments(8)
res, err := evt.RebuildProjections(ctx, repo, applyEvent, cfg)
```

`WithScanSegments(n)` sweeps the table with `n` parallel segments. It costs more read throughput and
consumed capacity, and in exchange the rebuild finishes faster. For small tables, leave it at the
default single sequential scan.

### Large tables and entity heads

`StreamEntities` sits behind both `evt.RebuildProjections` and `evt.RebuildProjectionsFromStream`,
and the library has no bounded-memory counterpart for a from-events rebuild. For a very large
table, page your own DynamoDB Query or Scan with `LastEvaluatedKey` and feed
`evt.RebuildProjectionsFromStream` yourself. One partition query per entity keeps memory bounded.
For a snapshot-seeded rebuild, `StreamEntitiesFromSnapshots` is bounded by the entity count rather
than the event count (see the next section).

A full rebuild reprocesses every entity even when only a few changed. To find the ones that moved,
`Repository.StreamEntityHeads` returns a `map[EntityID]EventSequence` of each entity's highest
event sequence, derived by scanning and folding the log. The head accounts for a compacted stream's
snapshot floor, so it stays correct after `CompactBelow`. Store a per-entity checkpoint beside your
views, compare each head against it, and reproject only the entities whose head moved:

```go
current, err := repo.StreamEntityHeads(ctx, "") // all entity types; scans the event log
// for id, head := range current:
//   if head > checkpoint[id] { reproject(id); checkpoint[id] = head }
```

Compare with `>`, not `!=`, because heads only advance. `StreamEntityHeads` is a scan, so it costs
read capacity comparable to the log, and the library keeps no separate heads table. The
`evt.EntityHeadStreamer` interface is backend-neutral, and the in-memory and PostgreSQL
repositories implement it too (PostgreSQL with `SELECT entity_id, MAX(sequence) ...`). If you'd
rather have a visitor than a map, use `evt.EntityHeadVisitor`, which those backends also implement.

## Rebuilding compacted streams

By default a rebuild replays each stream from event sequence 1. But compaction
(`evt.Compactor.CompactBelow`) deletes events that a durable snapshot already captures, so on a
compacted stream those low events are gone, and a replay from event 1 would build the wrong state.

For compacted streams, set `RebuildConfig.SeedAggregate`. The rebuild then seeds each entity from
its snapshot and applies only the events after it (through `evt.SnapshotStreamer`), falling back to
full replay for streams with no snapshot. It's safe on uncompacted data too, so if you plan to
compact, switch your rebuilds to `SeedAggregate` first:

```go
res, err := evt.RebuildProjections(ctx, repo, applyEvent, evt.RebuildConfig{
    Projectors: projectors,
    CommitGroup: commitGroup,
    SeedAggregate: func(ctx context.Context, snap evt.SerializedSnapshot) (evt.Aggregate, error) {
        aggregate, err := newAggregateForType(snap.EntityType) // your factory
        if err != nil {
            return nil, err
        }
        return aggregate, json.Unmarshal(snap.Payload, aggregate)
    },
})
```

The snapshot-aware path enumerates entity IDs with a key-only scan and a dedup set, then queries
each partition, seeding from the `sk=0` snapshot and applying the events after it. It's bounded by
the entity count and doesn't depend on scan ordering. If `SeedAggregate` is set but the repository
doesn't implement `evt.SnapshotStreamer`, `RebuildProjections` returns an error rather than quietly
producing partial state. [ADR 0001](../adr/0001-event-compaction-and-snapshot-truncation.md) has
the reasoning.

To drive it yourself, the DynamoDB repository exposes `StreamEntitiesFromSnapshotsWithOptions`:

```go
stream := repo.StreamEntitiesFromSnapshotsWithOptions(ctx, dynamo.StreamFromSnapshotsOptions{
    EntityType: entityType,
    Workers:    8,
}, seedAggregate, applyEvent)

res, err := evt.RebuildProjectionsFromStream(ctx, stream, evt.RebuildConfig{
    Projectors:  projectors,
    CommitGroup: commitGroup,
})
```

`Workers` sets how many entity partitions are rebuilt at once, and `Skip` lets you resume an
interrupted run. Rebuilds are idempotent, so a full restart is always safe.

## Reading views without buffering

The view repository's `ListViewsByEntityType` and `ListViewsByPK` buffer their whole result set.
For large results, use the cursor-based `*Paged` variants, or the streaming iterators on the
optional `evt.ViewStreamer` interface (`ListViewsByEntityTypeEach` and `ListViewsByPKEach`). Those
call a callback per view and stop early when it returns an error. The DynamoDB repository
implements the interface, so type-assert a `ViewRepository` to reach it:

```go
if streamer, ok := repo.(evt.ViewStreamer); ok {
    err := streamer.ListViewsByEntityTypeEach(ctx, entityType, func(v *evt.SerializedView) error {
        // handle v without buffering the whole result set
        return nil
    })
}
```
