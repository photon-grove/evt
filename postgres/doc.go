// Package postgres implements the evt.Repository and evt.Store contracts on top of PostgreSQL.
//
// It is a durable, backend-neutral peer of the DynamoDB backend: the same aggregates, projectors,
// and snapshot logic run unchanged against either store. The event log is an append-only relational
// table keyed by (entity_id, sequence); durable snapshots live in a sibling table keyed by
// entity_id. The package honors the storage invariants documented in BEHAVIORAL_INVARIANTS.md and
// verified by the backend-neutral conformance suite (conformance.RunRepositorySuite):
//
//   - per-entity sequence uniqueness — a PRIMARY KEY on (entity_id, sequence);
//   - optimistic concurrency on commit — a duplicate (entity_id, sequence) insert fails the unique
//     constraint and is surfaced as an evt.ConflictError;
//   - stable event ordering — GetEvents returns events ascending; GetLatestEvents returns
//     sequence > N ascending;
//   - snapshot consistency — a written snapshot is read back intact, and after compaction the
//     snapshot is the authoritative floor for replay;
//   - atomic event/snapshot writes — every SerializedResult is committed inside one SQL transaction.
//
// The optional capabilities the framework detects by type assertion are all implemented:
// evt.Compactor (CompactBelow), evt.SnapshotStreamer (StreamEntitiesFromSnapshots), and
// evt.EntityHeadStreamer / evt.EntityHeadVisitor (a streamed cursor over the event log that yields
// one head per entity with constant memory).
//
// Table DDL is owned by the Repository (EnsureSchema applies idempotent CREATE TABLE IF NOT EXISTS
// statements) rather than by external infrastructure, because PostgreSQL table schemas are an
// application concern that must stay in lockstep with the Go types that read and write them. The
// local Terraform stack under infra/local-postgres provisions the database itself; the Repository
// owns the tables inside it.
package postgres
