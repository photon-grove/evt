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

// EntityHeadVisitor is an optional streaming variant of EntityHeadStreamer. Where StreamEntityHeads
// materializes every head into a map (O(entities) memory), StreamEntityHeadsFunc delivers heads one
// at a time to a visitor callback and never accumulates them — so a rebuild's enumeration memory
// ceiling stays constant no matter how many entities exist.
//
// This is only constant-memory when the underlying store holds one row per entity (a registry such
// as a heads table): such a source is naturally unique, needs no dedup set, and is resumable from
// the last key it paged. It is deliberately backend-neutral — no DynamoDB types appear here — so a
// future SQL backend can satisfy it with a streamed cursor over SELECT entity_id, MAX(sequence) ….
// Like EntityHeadStreamer, it is kept off the core Repository interface and detected via a type
// assertion (source.(EntityHeadVisitor)).
type EntityHeadVisitor interface {
	// StreamEntityHeadsFunc enumerates entity heads and invokes visit once per entity, in whatever
	// order the store pages them, without holding the full set in memory. The head sequence has the
	// same meaning as StreamEntityHeads. entityType, when non-empty, restricts enumeration to that
	// type. If visit returns an error, enumeration stops and that error is returned; a paging error
	// is returned as-is. A nil error means every entity was visited exactly once.
	StreamEntityHeadsFunc(
		ctx context.Context,
		entityType EntityType,
		visit func(EntityID, EventSequence) error,
	) error
}
