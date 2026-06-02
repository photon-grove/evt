package evt

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"

	"github.com/photon-grove/evt/logging"
)

// RebuildConfig configures a projection rebuild run.
type RebuildConfig struct {
	// EntityType filters entities to rebuild. Only entities matching this type are processed.
	EntityType EntityType

	// Projectors are the projection functions to run against each entity.
	Projectors []EventProjector

	// CommitGroup writes a TransactionGroup produced by a projector. Required when DryRun is false.
	// For DynamoDB-backed views this typically executes the ViewPutGroup's write items via
	// BatchWriteItem or TransactWriteItems.
	CommitGroup func(ctx context.Context, group TransactionGroup) error

	// DryRun when true skips writing views and only reports what would change.
	DryRun bool

	// MaxErrors caps the number of per-entity errors collected before the rebuild stops early.
	// Zero means no limit.
	MaxErrors int

	// OnProgress, if non-nil, is called after each entity is successfully processed
	// by the projectors or when an error is encountered (stream, projector, or commit).
	// Skipped entities (nil or wrong type) do not trigger this callback.
	// The processed argument is the count of successfully processed entities so far,
	// and errors is the cumulative number of errors encountered.
	OnProgress func(processed, errors int)
}

// RebuildResult summarizes a projection rebuild run.
//
// Note: Processed counts only entities where all projectors succeeded.
// Entities that produced errors are counted in Errors, not Processed or Skipped.
type RebuildResult struct {
	// Processed is the number of entities successfully replayed through all projectors.
	Processed int

	// Skipped is the number of entities skipped (wrong type or nil).
	Skipped int

	// Errors collects per-entity errors that did not halt the run.
	// Capped by MaxErrors if configured.
	Errors []error
}

// RebuildProjections replays all entities from the repository through the configured projectors,
// regenerating view data. Entities are streamed via StreamEntities, so this works with any
// Repository implementation.
//
// The applyEvent callback is the same function used by StreamEntities to reconstitute entities
// from serialized events. Callers typically pass the same function they use for normal event
// replay (deserialize + apply).
func RebuildProjections(
	ctx context.Context,
	repo Repository,
	applyEvent func(context.Context, SerializedEvent, Entity) (Entity, error),
	cfg RebuildConfig,
) (*RebuildResult, error) {
	if applyEvent == nil {
		return nil, fmt.Errorf("applyEvent callback is required")
	}
	if len(cfg.Projectors) == 0 {
		return nil, fmt.Errorf("at least one projector is required")
	}
	if !cfg.DryRun && cfg.CommitGroup == nil {
		return nil, fmt.Errorf("CommitGroup is required when DryRun is false")
	}

	logger := logging.GetLogger(ctx)

	// Build an optional filter expression for the entity type.
	var expr *expression.Expression
	if cfg.EntityType != "" {
		builder := expression.NewBuilder().WithFilter(
			expression.Name("entityType").Equal(expression.Value(string(cfg.EntityType))),
		)
		built, err := builder.Build()
		if err != nil {
			return nil, fmt.Errorf("building filter expression: %w", err)
		}
		expr = &built
	}

	res := &RebuildResult{}

	for entityResult := range repo.StreamEntities(ctx, expr, applyEvent) {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}

		entity, err := entityResult.Unwrap()
		if err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("streaming entity: %w", err))
			reportProgress(cfg.OnProgress, res.Processed, len(res.Errors))
			if cfg.MaxErrors > 0 && len(res.Errors) >= cfg.MaxErrors {
				return res, fmt.Errorf("rebuild stopped: reached MaxErrors limit (%d)", cfg.MaxErrors)
			}
			continue
		}

		if entity == nil {
			res.Skipped++
			continue
		}

		// Double-check entity type in case the storage-level filter is approximate
		// (e.g., in-memory repo ignores expressions).
		if cfg.EntityType != "" && entity.Type() != cfg.EntityType {
			res.Skipped++
			continue
		}

		rebuildLogger := logger.With(
			slog.String("entity_id", string(entity.GetID())),
			slog.String("entity_type", string(entity.Type())),
		)

		if err := projectEntity(ctx, entity, cfg, rebuildLogger); err != nil {
			res.Errors = append(res.Errors, fmt.Errorf("entity %s: %w", entity.GetID(), err))
			reportProgress(cfg.OnProgress, res.Processed, len(res.Errors))
			if cfg.MaxErrors > 0 && len(res.Errors) >= cfg.MaxErrors {
				return res, fmt.Errorf("rebuild stopped: reached MaxErrors limit (%d)", cfg.MaxErrors)
			}
			continue
		}

		res.Processed++
		reportProgress(cfg.OnProgress, res.Processed, len(res.Errors))
	}

	logger.Info("Projection rebuild complete",
		slog.Int("processed", res.Processed),
		slog.Int("skipped", res.Skipped),
		slog.Int("errors", len(res.Errors)),
	)

	return res, nil
}

// projectEntity runs all projectors for a single entity and commits the resulting view writes.
func projectEntity(
	ctx context.Context,
	entity Entity,
	cfg RebuildConfig,
	logger *slog.Logger,
) error {
	for _, projector := range cfg.Projectors {
		// Projectors receive nil events during rebuild — the entity already has its final state
		// applied. Passing nil signals "full rebuild" rather than incremental projection.
		txnGroup, err := projector.Project(ctx, entity, nil)
		if err != nil {
			return fmt.Errorf("projector error: %w", err)
		}
		if txnGroup == nil {
			continue
		}

		if cfg.DryRun {
			logger.Info("Dry run: would write view",
				slog.Int("operations", txnGroup.Len()),
			)
			continue
		}

		if err := cfg.CommitGroup(ctx, txnGroup); err != nil {
			return fmt.Errorf("committing view: %w", err)
		}
	}

	return nil
}

func reportProgress(fn func(processed, errors int), processed, errors int) {
	if fn != nil {
		fn(processed, errors)
	}
}
