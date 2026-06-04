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

### Bounded-memory rebuilds for large tables

Because `StreamEntities` must regroup an unordered scan, it holds the matched
events in memory until the scan finishes. For large event logs, use the dynamo
repository's `StreamEntitiesByQuery`, which first enumerates the distinct entity
IDs with a key-only scan (`ProjectionExpression` on `pk`) and then queries each
entity's partition — returning its events already in order — folding and emitting
one entity at a time. Memory is bounded to the set of entity IDs plus a worker
pool of in-flight aggregates, and entities stream out as they are rebuilt. Pass
the resulting stream to `RebuildProjectionsFromStream`:

```go
repo := dynamo.NewRepository(client, eventsTable)
stream := repo.StreamEntitiesByQuery(ctx, dynamo.StreamByQueryOptions{
    EntityType: cfg.EntityType, // optional filter
    Workers:    8,              // partitions queried concurrently
    // Skip lets you resume an interrupted run; rebuilds are idempotent, so a
    // full restart is also always safe.
    Skip: func(id evt.EntityID) bool { return false },
}, applyEvent)

res, err := evt.RebuildProjectionsFromStream(ctx, stream, cfg)
```

`RebuildProjections` is the convenience wrapper that builds the scan-based stream
for you; `RebuildProjectionsFromStream` accepts any entity stream so you can pick
the strategy that fits your table size.

Note on cost: enumeration is still a table `Scan`. A `ProjectionExpression`
reduces the data returned over the wire but **not** the read capacity consumed —
DynamoDB charges scan RCUs by the size of the items read, not the attributes
projected. So enumeration consumes read capacity comparable to scanning the full
log; the win here is bounded memory, incremental output, and parallel per-entity
queries, not lower read cost. For genuinely cheaper enumeration, back it with a
dedicated per-entity index or registry.

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
