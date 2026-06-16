package evt

import "context"

// EntityHeadStreamer is an optional capability implemented by Repositories that can report every
// entity's current head sequence in a single enumeration pass. It is the change-detection
// primitive for incremental projection rebuilds: a caller compares each head against a per-entity
// projection checkpoint (the sequence a view was last built from) to find the entities whose
// projections are stale, and reprojects only those.
//
// It is designed to preserve the event log's write scaling: detection reads from the existing
// partition key and sort key, so it needs no secondary index and no global sequence counter — the
// two structures that would reintroduce a hot write-path partition. Each append still touches only
// its own entity partition.
//
// Like Compactor and SnapshotStreamer, it is kept off the core Repository interface for backward
// compatibility and detected via a type assertion (repo.(EntityHeadStreamer)).
type EntityHeadStreamer interface {
	// StreamEntityHeads enumerates the distinct entities in the event log and returns each
	// entity's current head sequence. The head is the larger of (a) the highest event sort key in
	// the partition and (b) the EventSequence recorded by the entity's durable snapshot (the sk=0
	// row), so the result stays correct for streams whose early events have been compacted away
	// (where the snapshot, not event 1, is the authoritative floor). entityType, when non-empty,
	// restricts enumeration to entities of that type.
	//
	// Cost note: for scan-backed stores this is a single key-only Scan. DynamoDB charges read
	// capacity by item size, not by projected attributes, so it reads capacity comparable to a
	// full scan; the win is one pass yielding one bounded entry per entity (the changed set), not
	// lower read cost. It replaces re-reading and re-projecting every stream on every rebuild.
	StreamEntityHeads(ctx context.Context, entityType EntityType) (map[EntityID]EventSequence, error)
}
