package dynamo

import (
	"context"
	"fmt"

	"github.com/photon-grove/evt"
)

// RebuildProjectionsByQuery runs a bounded-memory projection rebuild: it builds the
// enumerate-then-query entity stream described by opts and feeds it to
// evt.RebuildProjectionsFromStream. It is the convenience wrapper for the registry-backed,
// constant-memory rebuild that StreamByQueryOptions.HeadSource enables — equivalent to wiring
// StreamEntitiesByQuery and evt.RebuildProjectionsFromStream by hand, so callers no longer
// hand-assemble the two.
//
// Use it as a drop-in alternative to evt.RebuildProjections for large tables. evt.RebuildProjections
// stays the scan-based default that buffers the whole log; this path queries one partition at a time
// and emits each entity as soon as it is rebuilt, bounding memory to the enumerated IDs plus
// opts.Workers in-flight aggregates. Set opts.HeadSource to enumerate IDs from a heads registry (one
// row per entity) instead of a key-only event-log scan — the opt-in, constant-memory path — and
// leave it nil to keep the scan-and-dedup enumeration. The default stays unchanged either way.
//
// Entity-type filtering: opts.EntityType scopes enumeration and cfg.EntityType is the rebuild's
// defensive per-entity check, so the two must agree. When opts.EntityType is empty it defaults to
// cfg.EntityType, so a caller can set the type once in cfg (as with evt.RebuildProjections) and have
// it scope enumeration too. When both are set and differ, this returns an error rather than silently
// enumerating one type while the rebuild skips it as the wrong type.
//
// cfg is validated up front — applyEvent, Projectors, and CommitGroup are checked before the stream
// starts, mirroring evt.RebuildProjections, so an invalid config fails fast without launching the
// enumeration scan and per-entity queries (which would otherwise consume read capacity and invoke
// applyEvent after the caller already had an error).
//
// This wires the scan-vs-registry choice only. The snapshot-seeded path (cfg.SeedEntity, for streams
// truncated by CompactBelow) is NOT combinable here: StreamEntitiesByQuery reads each partition with
// GetEvents and never consults the seeder, so a compacted stream would replay only its surviving
// events and commit projections built from truncated history. A non-nil cfg.SeedEntity is therefore
// rejected — use evt.RebuildProjections (which routes through evt.SnapshotStreamer) for compacted
// streams.
func (repo *Repository) RebuildProjectionsByQuery(
	ctx context.Context,
	opts StreamByQueryOptions,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
	cfg evt.RebuildConfig,
) (*evt.RebuildResult, error) {
	if applyEvent == nil {
		return nil, fmt.Errorf("applyEvent callback is required")
	}

	// Validate cfg before starting the stream so an invalid config does not strand a producer or burn
	// read capacity. evt.RebuildProjectionsFromStream re-checks these and drains on error, but by then
	// the stream is already running.
	if len(cfg.Projectors) == 0 {
		return nil, fmt.Errorf("at least one projector is required")
	}

	if !cfg.DryRun && cfg.CommitGroup == nil {
		return nil, fmt.Errorf("CommitGroup is required when DryRun is false")
	}

	// The query path cannot honor a snapshot seeder, so accepting one would silently rebuild compacted
	// streams from truncated history. Reject it rather than ignore it.
	if cfg.SeedEntity != nil {
		return nil, fmt.Errorf("cfg.SeedEntity is not supported by RebuildProjectionsByQuery; use evt.RebuildProjections for snapshot-seeded (compacted) streams")
	}

	if opts.EntityType == "" {
		opts.EntityType = cfg.EntityType
	} else if cfg.EntityType != "" && opts.EntityType != cfg.EntityType {
		return nil, fmt.Errorf(
			"entity type mismatch: opts.EntityType %q scopes enumeration but cfg.EntityType %q drives the rebuild's type check; set them equal or leave opts.EntityType empty",
			opts.EntityType, cfg.EntityType,
		)
	}

	stream := repo.StreamEntitiesByQuery(ctx, opts, applyEvent)

	return evt.RebuildProjectionsFromStream(ctx, stream, cfg)
}
