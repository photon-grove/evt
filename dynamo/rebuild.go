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
// cfg is validated by evt.RebuildProjectionsFromStream, which drains the already-started stream on a
// config error so its producer never strands. applyEvent is required.
//
// This wires the scan-vs-registry choice; the snapshot-seeded path (cfg.SeedEntity, for streams
// truncated by CompactBelow) remains on evt.RebuildProjections via evt.SnapshotStreamer and is not
// combinable with opts here.
func (repo *Repository) RebuildProjectionsByQuery(
	ctx context.Context,
	opts StreamByQueryOptions,
	applyEvent func(context.Context, evt.SerializedEvent, evt.Entity) (evt.Entity, error),
	cfg evt.RebuildConfig,
) (*evt.RebuildResult, error) {
	if applyEvent == nil {
		return nil, fmt.Errorf("applyEvent callback is required")
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
