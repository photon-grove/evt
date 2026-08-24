package dynamo

import (
	"context"
	"fmt"

	"github.com/photon-grove/evt"
)

// RebuildProjectionsByQuery rebuilds with per-entity queries and bounded in-flight aggregates.
// HeadSource can enumerate IDs from a heads registry.
//
// opts.EntityType defaults to cfg.EntityType; if both are set, they must match.
// This path rejects SeedEntity because it cannot safely rebuild compacted streams. Use
// evt.RebuildProjections for snapshot-seeded replay.
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
