package dynamo

import (
	"context"
	"fmt"

	"github.com/photon-grove/evt"
)

// RebuildProjectionsByQuery runs a bounded-memory projection rebuild by combining
// StreamEntitiesByQuery with evt.RebuildProjectionsFromStream.
//
// Unlike evt.RebuildProjections, this path queries one partition at a time and emits each rebuilt
// entity. Memory is bounded to the enumerated IDs plus opts.Workers in-flight aggregates. Set
// opts.HeadSource to enumerate IDs from a heads registry instead of a key-only event-log scan.
//
// Entity-type filtering: opts.EntityType scopes enumeration and cfg.EntityType is the rebuild's
// defensive per-entity check, so the two must agree. When opts.EntityType is empty it defaults to
// cfg.EntityType, so a caller can set the type once in cfg (as with evt.RebuildProjections) and have
// it scope enumeration too. When both are set and differ, this returns an error rather than silently
// enumerating one type while the rebuild skips it as the wrong type.
//
// The method validates applyEvent and cfg before starting enumeration.
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
