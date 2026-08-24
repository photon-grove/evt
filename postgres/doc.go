// Package postgres implements evt.Repository and evt.Store with PostgreSQL.
//
// Events are append-only and unique per (entity_id, sequence); duplicate commits return
// evt.ConflictError. Reads are sequence-ordered, event and snapshot writes are atomic, and durable
// snapshots become the replay floor after compaction.
//
// Repository implements evt.Compactor, evt.SnapshotStreamer, evt.EntityHeadStreamer, and
// evt.EntityHeadVisitor. EnsureSchema owns the idempotent table DDL.
package postgres
