# Projections and Rebuilds

Projection rows are deterministic read models derived from immutable events.

Use `evt.RebuildProjections` when:

- a projector bug wrote incorrect view rows
- a new view is added for existing aggregate streams
- a view payload schema changes
- an operator wants to validate projection health against the event log

During a rebuild, the repository streams entities, the caller-supplied replay
function reconstitutes aggregate state, and projectors produce transaction
groups. In dry-run mode, `evt` reports the work without writing rows.

The rebuild contract deliberately makes writes explicit through `CommitGroup` so
adopters can choose the safest commit strategy for their storage backend.

## Fencing a rebuild

A rebuild rewrites view rows while live commits keep writing theirs, so the two can interleave
unsafely: a live commit can put a row the rebuild's wipe then deletes, or recreate one the replay
has not reached. The framework fences rebuilds durably (v0.9, C10): take the rebuild lock for the
whole run, and every commit is refused with `*evt.RebuildInProgressError` until it is released.

```go
fence := repo.(evt.RebuildFenceProvider).RebuildFence()

lease, err := fence.Begin(ctx, owner, 5*time.Minute)
if err != nil {
    return err // *evt.RebuildLockHeldError: another rebuild holds an unexpired lease
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

- Renew the lease (`lease, err = fence.Renew(ctx, lease, d)`) while the run goes on, well inside the
  lease you asked for: a worker that pauses past its lease can be replaced by a takeover. The lease
  (owner and generation) is a fencing token, so the replaced worker cannot renew, write, or end
  its successor's lease even if both use one owner token. Write view batches through
  `fence.WriteViews(ctx, lease, batch)` (at most 99 operations) to have each batch conditioned
  transactionally on the lease: a worker that lost the lock gets `*evt.RebuildLeaseLostError` and
  writes nothing. Writes you make through your own `CommitGroup` are not guarded; a crashed worker's
  lock simply blocks commits until it expires and a rebuild that takes it over (`Begin` does that)
  ends. A force-release does not unblock commits: it leaves the generation unsettled, and every
  commit is refused with `*evt.RebuildInProgressError` until a later rebuild runs to its end.
- Refused commits are transient: fence-aware retry waits and tries again, then returns the error,
  and records no outcome, so the same command ID runs once the rebuild is over.
- **Readers fence a decision against the generation.** A command that decides on a derived view
  reads under an epoch, so a decision made on a half-rebuilt view fails instead of being recorded:

```go
err := evt.ReadAtRebuildEpoch(ctx, fence, func(ctx context.Context) error {
    row, err = views.Get(ctx, key) // the derived-view read
    return err
})
// *evt.RebuildUnavailableError: a rebuild is running (or a force-release left the views untrustworthy)
// *evt.RebuildEpochChangedError: a rebuild began and ended while the read ran; read again
```

  Return either error from the handler: the engine returns it at once and records no outcome, and
  both are transient (`policy.IsTransient`), so retry the command later. A rebuild that begins
  after the read is stopped by the commit's lock check instead.

See the [storage format](spec/storage-format.md#rebuild-fence) for the rows, the transactions, and
the PostgreSQL equivalent.

## Commit-wide projectors

An `EventProjector` sees one entity at a time. A **commit-wide projector** (v0.9, C7) is a pure
function over everything one commit wrote, and it emits **view operations** that commit in the same
transaction as the events. Use it for read models that span entities: a directory of unique names,
a per-tenant ledger, an index that must never disagree with the streams behind it.

The example is a directory of unique handles. Each handle name is a stream of its own,
`handle:<name>`, and the handle stream owns the row for that name for good. A command that claims a
name reads the handle stream and appends a `handle:claimed` event to it in the same transaction as
the account's event; two accounts racing for one name both read the handle stream at one sequence,
so one loses to optimistic concurrency and retries against the new state. Releasing a name appends
`handle:released`, and the row is rewritten with a tombstone, never deleted.

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

**What a projector sees.** An `evt.CommitView`: the commit ID and every stream the commit wrote,
ascending by entity-ID bytes, each with its entity type, the commit's events for it, and its state
after the commit (the JSON its snapshot would carry). Streams that were only read are fenced, not
written, so they are not in the view. A command that writes no event — a plain rejection, an
accepted no-op — runs no projector; a rejection that carries events projects them like an accepted
command's. The view has no outcome context on purpose: a rebuild has no outcome row.

**Current-state or history-dependent.** `History()` names the entity types whose *events* the
projector reads, and the view each kind is given is what a rebuild of that kind can give it again:

- A projector that returns none is **current-state**. It is given each stream's state after the
  commit, and nothing else: no events and an **empty commit ID**, so it cannot key a row by commit,
  count a commit's events, or otherwise depend on anything beyond the state the streams reach. Use
  it for rows that are a function of current state, such as a per-entity summary.
- A projector that returns types is **history-dependent**. It is given the commit ID, and the events
  *and state* of the streams of those types only; every other stream appears by entity ID and type
  with neither (its history may have been compacted, and the declaration does not cover it). Before
  the first commit that runs it the store durably adds those types to the feature manifest's
  `historyDependent` set, so `CompactBelow` refuses them in every process — the commits a rebuild
  replays must stay. The repository must be able to declare (`evt.HistoryDeclarer`; the in-memory,
  DynamoDB, and PostgreSQL repositories are) or the commit fails with
  `*evt.HistoryDeclarationUnsupportedError`.

**Owners.** Every operation of every projector names the stream that **owns** its row (the owner is
an argument of `PutView` and `DeleteView`). It must be a stream the commit wrote; for a
history-dependent projector it must also be of a type the projector declares in `History()`, so the
row can be rebuilt from the history the declaration protects (`*evt.InvalidViewOperationError`
otherwise). The repository stores the owner as the row's `entityID` and conditions **every** write
to the row on it — puts and deletes, with or without a presence condition — so a write by any
other stream fails the commit with a `*evt.ProjectorOperationError` (reason
`ConditionalCheckFailed`) at the time it is made, not at a later rebuild.

**A row's owner must never change.** The log keeps one commit order only: each stream's sequence
order. The writes to a row can be put back in order only while they all come from commits that
wrote the same stream. A key therefore never changes hands by delete-then-put from another stream:
the delete leaves the row absent, so the second stream's put would pass the owner condition live,
and a rebuild — which has no order between the two streams — could not tell which came last
(`evt.ReplayViews` refuses it with `*evt.SharedViewKeyError`). To reuse a key, keep it with its
owner and rewrite the row as a tombstone, or make the key belong to a stream every claimant writes,
as the handle stream above does. Do not rely on commit IDs for ordering; they are not causal.

Even a delete that finds nothing counts as a claim on the key. A delete by owner A of a row that is
not there, followed by a put by owner B, is accepted live (the row is absent, so the owner
condition holds) but refused on replay, because the replay sees two owners touch one row. Treat
every delete as a claim, whether or not it removes a row.

**What it returns.** Puts and deletes of rows at any `pk`/`sk`, in the order they should be
written. A put overwrites unless it is `IfAbsent`; a delete of a missing row succeeds unless it is
`IfPresent`. `OnConflict(err)` names the error to return when the condition fails. Projectors are
appended in the order `WithCommitProjectors` was called, each one's operations in emission order,
after the marker and rebuild-lock checks and before the written streams ([layout](spec/storage-format.md#writes)).
Before any write, on every backend, the engine refuses an operation with an empty or non-UTF-8 key,
a `pk` over 2,048 or an `sk` over 1,024 UTF-8 bytes (`*evt.ViewKeyTooLongError`), and two
operations on the same row (`*evt.DuplicateViewKeyError`). Operations count against the
`CommitTooLarge` budget, and a commit that carries operations but no event is refused
(`*evt.ProjectorOnlyCommitError`): views are projections of committed events.

**Where the rows go.** The repository writes them. Give the DynamoDB repository a views table
with `repo.WithViewsTable("views")` (string `pk` and `sk`, the same table `NewViewRepository`
reads); give the PostgreSQL repository a writer with `repo.WithViewWriter(fn)`, since evt defines
no view schema — your `fn` turns each operation into the SQL for your own table (see
[PostgreSQL](postgres.md#views)). The in-memory repository keeps them for tests
(`mem.Repository.View`, `Views`). Without a place to write, the commit fails with
`*evt.ViewsNotConfiguredError` before any write; operations are never dropped.

**When a condition fails.** The commit writes nothing and returns `*evt.ProjectorOperationError`,
which names the projector and the operation (and wraps the operation's mapped error, so
`errors.Is(err, ErrHandleTaken)` works). It is not an optimistic-concurrency conflict and not
transient: re-preparing the command cannot fix it, so fence-aware retry returns it at once and
records no outcome. It ranks last among the ways a commit can fail: a lost outcome row, stream-head
drift, a sequence gap, and any conflict on a written stream or a fence are reported first.

**Purity is the rebuild contract.** A projector's rows must be a deterministic function of its
`CommitView`: no clock, random source, environment, or store reads; no state kept between calls; no
dependence on map iteration order. Mutable decisions, externally observed signals, and verdicts are
event-sourced first and projected second; a view row that exists only because something happened
outside the log cannot be rebuilt, and a rebuild would silently lose it.

**What a rebuild reproduces today.** Only an in-memory replay exists: regroup the stored events
with `evt.GroupCommits`, fold each stream's state after its commit yourself, and run the projectors
over the groups with `evt.ReplayViews`. For a **history-dependent** projector that is the contract
— the rows it returns equal the live rows — and it is how to test one (see the end-to-end test in
`go/snapshots/projectors_test.go`). For a **current-state** projector the same replay works because
the state is yours to supply, but there is no engine entry point yet that rebuilds current-state
rows from the current state of the streams alone (the state-based rebuild is specified in the
[storage format](spec/storage-format.md#views) and lands with the rebuild ports); until then,
treat a current-state projector's rows as reproducible only by replaying with folded state.
Nothing rebuilds the rows of a durable view table by itself today.

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

## Scanning the event log

`RebuildProjections` reads the whole event log through `StreamEntities`, which is
backed by a DynamoDB `Scan`. A `Scan` makes no ordering guarantees, so
`StreamEntities` groups every matched event by entity and applies each entity's
events in sequence order before yielding it. As a result it buffers the matched
events in memory for the duration of the scan — it is a rebuild/diagnostic path,
not a hot read path.

For large tables, configure a parallel scan on the DynamoDB repository before
passing it to `RebuildProjections`:

```go
repo := dynamo.NewRepository(client, eventsTable).WithScanSegments(8)
res, err := evt.RebuildProjections(ctx, repo, applyEvent, cfg)
```

`WithScanSegments(n)` sweeps the table with `n` parallel segments, trading higher
read throughput (and consumed capacity) for a faster rebuild. Leave it at the
default (a single sequential scan) for small tables.

### Large tables and entity heads

`StreamEntities` (the source behind `evt.RebuildProjections` and `evt.RebuildProjectionsFromStream`)
buffers every matched event, and the library has no bounded-memory counterpart for a from-events
rebuild. For a very large table, page your own DynamoDB Query or Scan with `LastEvaluatedKey` (one
partition query per entity keeps memory bounded) and feed `evt.RebuildProjectionsFromStream`
yourself. For a snapshot-seeded rebuild, `StreamEntitiesFromSnapshots` is bounded by the entity
count rather than the event count (see below).

A full rebuild reprocesses every entity even when only a few changed. To find the ones that moved,
`Repository.StreamEntityHeads` returns a `map[EntityID]EventSequence` of each entity's highest
event sequence, derived by scanning and folding the event log. The head accounts for a compacted
stream's snapshot floor, so it stays correct after `CompactBelow`. Compare each head against a
per-entity checkpoint you store beside your views, and reproject only the entities whose head
moved:

```go
current, err := repo.StreamEntityHeads(ctx, "") // all entity types; scans the event log
// for id, head := range current:
//   if head > checkpoint[id] { reproject(id); checkpoint[id] = head }
```

Compare with `>`, not `!=`, because heads only advance. `StreamEntityHeads` is a scan, so it costs
read capacity comparable to the log, and the library keeps no separate heads table. The
`evt.EntityHeadStreamer` interface is backend-neutral, so the in-memory and PostgreSQL repositories
implement it too (PostgreSQL with `SELECT entity_id, MAX(sequence) ...`). A caller that wants a
visitor instead of a map can use `evt.EntityHeadVisitor`, which those backends also implement.

## Rebuilding compacted streams

By default a rebuild replays each stream from event sequence 1. If a stream has
been **compacted** (`evt.Compactor.CompactBelow` truncates events that a durable
snapshot already captures), those low events no longer exist, so a from-event-1
replay would reconstruct incorrect state.

For compacted streams, set `RebuildConfig.SeedEntity`. The rebuild then seeds each
entity from its snapshot before applying only the post-snapshot events (via
`evt.SnapshotStreamer`), and falls back to full replay for streams that have no
snapshot. This is safe on uncompacted data too, so adopters that plan to compact
should switch rebuilds to `SeedEntity` first:

```go
res, err := evt.RebuildProjections(ctx, repo, applyEvent, evt.RebuildConfig{
    Projectors: projectors,
    CommitGroup: commitGroup,
    SeedEntity: func(ctx context.Context, snap evt.SerializedSnapshot) (evt.Entity, error) {
        entity, err := newEntityForType(snap.EntityType) // your factory
        if err != nil {
            return nil, err
        }
        return entity, json.Unmarshal(snap.Payload, entity)
    },
})
```

The snapshot-aware path enumerates entity IDs with a key-only event-log scan and a dedup set, then
queries each partition (seeding from the `sk=0` snapshot and applying events after it). It is
bounded by the entity count, and it does not depend on scan ordering. If `SeedEntity` is set but
the repository does not implement `evt.SnapshotStreamer`, `RebuildProjections` returns an error
rather than silently producing partial state. See
[ADR 0001](adr/0001-event-compaction-and-snapshot-truncation.md).

To drive it yourself, the DynamoDB repository exposes `StreamEntitiesFromSnapshotsWithOptions`:

```go
stream := repo.StreamEntitiesFromSnapshotsWithOptions(ctx, dynamo.StreamFromSnapshotsOptions{
    EntityType: entityType,
    Workers:    8,
}, seedEntity, applyEvent)

res, err := evt.RebuildProjectionsFromStream(ctx, stream, evt.RebuildConfig{
    Projectors:  projectors,
    CommitGroup: commitGroup,
})
```

`Workers` sets how many entity partitions are reconstructed at once, and `Skip` lets you resume an
interrupted run. Rebuilds are idempotent, so a full restart is always safe.

## Reading views without buffering

The view repository's `ListViewsByEntityType` and `ListViewsByPK` buffer their
full result set. For large result sets prefer the cursor-based `*Paged` variants,
or the streaming iterators on the optional `evt.ViewStreamer` interface
(`ListViewsByEntityTypeEach` / `ListViewsByPKEach`), which invoke a callback per
view and stop early when it returns an error. Type-assert a `ViewRepository` to
`evt.ViewStreamer` to reach them (the DynamoDB repository implements it):

```go
if streamer, ok := repo.(evt.ViewStreamer); ok {
    err := streamer.ListViewsByEntityTypeEach(ctx, entityType, func(v *evt.SerializedView) error {
        // handle v without buffering the whole result set
        return nil
    })
}
```
