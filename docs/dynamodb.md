# DynamoDB Integration

## Event Log Table

The event log table uses:

- `pk` (`S`) as the entity ID
- `sk` (`N`) as the event sequence
- `sk = 0` as the inline snapshot row for that entity
- DynamoDB Streams with `NEW_IMAGE` when using async projectors or publishers

Event rows are append-only and protected by conditional writes.

### Compaction

Event rows are retained in full by default. A stream may opt into compaction with
`evt.Compactor.CompactBelow(ctx, entityID, throughSequence)`, which deletes events
in `[1, throughSequence]` — but only after confirming the inline `sk = 0` snapshot
covers them (`snapshot.EventSequence >= throughSequence`), and never the snapshot
row itself. Uncovered requests return `evt.ErrCompactionUncovered` and delete
nothing. Rebuild compacted streams via the snapshot-aware path
(`RebuildConfig.SeedEntity`). The raw `dynamo.Delete` is snapshot-unsafe, for
local/staging fixtures only, and excluded from production builds (`-tags prod`).
See [ADR 0001](adr/0001-event-compaction-and-snapshot-truncation.md).

## Entity Views Table

The view table uses:

- `pk` (`S`) as the projection-owned lookup key
- `sk` (`S`) as the view sort key, defaulting to `VIEW`
- `entityType` (`S`) for `entityType-index`
- `ttl` (`N`) when expiring derived rows is useful

Views are rebuildable cache, not source of truth.

## Local Tables

The `infra/local` Terraform stack creates emulator tables that match the test
suite. Apply it before running integration tests.
