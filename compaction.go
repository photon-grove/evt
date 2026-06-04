package evt

import (
	"context"
	"errors"

	"github.com/photon-grove/evt/result"
)

// ErrCompactionUncovered is returned by a Compactor when a compaction request cannot be
// proven safe: either the stream has no durable snapshot, or its latest snapshot does not
// cover the requested sequence. Compaction must never delete an event that is not already
// captured by a durable snapshot, so the operation is refused rather than risking data loss.
var ErrCompactionUncovered = errors.New("compaction refused: no durable snapshot covers the requested range")

// Compactor is an optional capability implemented by Repositories that can truncate an
// entity's event log below a durable snapshot. It is intentionally NOT part of the core
// Repository interface so that adding it remains backward compatible for the shared module:
// callers detect support with a type assertion (repo.(Compactor)).
//
// Compaction forfeits the "replay from sequence 1" property for the affected stream. After a
// successful CompactBelow, the stream's authoritative starting point is its latest durable
// snapshot, not its first event. Rebuilds must therefore use a SnapshotStreamer (see
// RebuildConfig.SeedEntity) rather than assuming events 1..N are present.
type Compactor interface {
	// CompactBelow deletes events for the given entity whose sequence is in the range
	// [1, throughSequence], but only after verifying that a durable snapshot exists whose
	// recorded EventSequence is >= throughSequence (i.e. the deleted range is fully captured
	// by the snapshot). The sk=0 snapshot row is never deleted.
	//
	// It returns the number of events deleted. If no covering snapshot exists, it deletes
	// nothing and returns ErrCompactionUncovered. A throughSequence < 1 is a no-op.
	CompactBelow(ctx context.Context, entityID EntityID, throughSequence EventSequence) (int, error)
}

// SnapshotSeeder reconstructs a fresh entity instance from a durable snapshot payload. It is
// the snapshot-aware counterpart to the applyEvent callback used during replay: applyEvent
// builds an entity from its events, while a SnapshotSeeder builds one from the JSON state a
// snapshot captured. Callers typically look up the concrete type by snapshot.EntityType and
// json.Unmarshal(snapshot.Payload, entity).
type SnapshotSeeder func(ctx context.Context, snapshot SerializedSnapshot) (Entity, error)

// SnapshotStreamer is an optional capability implemented by Repositories that can rebuild
// entities starting from each stream's durable snapshot instead of from sequence 1. It is the
// replay path that stays correct after CompactBelow has removed events below a snapshot.
//
// Like Compactor, it is kept off the core Repository interface for backward compatibility and
// detected via a type assertion (repo.(SnapshotStreamer)).
type SnapshotStreamer interface {
	// StreamEntitiesFromSnapshots streams completed entities, seeding each from its latest durable
	// snapshot (the sk=0 row) via seedEntity before applying only the events recorded after that
	// snapshot. Events at or below the snapshot's EventSequence are skipped because the snapshot
	// already captures them. Streams with no snapshot fall back to full replay from sequence 1 via
	// applyEvent. entityType, when non-empty, restricts the rebuild to entities of that type.
	//
	// Implementations should reconstruct each entity from its own partition (a per-entity query),
	// not from scan order, so the result is correct regardless of how the underlying store returns
	// rows.
	StreamEntitiesFromSnapshots(
		ctx context.Context,
		entityType EntityType,
		seedEntity SnapshotSeeder,
		applyEvent func(context.Context, SerializedEvent, Entity) (Entity, error),
	) <-chan result.Result[Entity]
}
