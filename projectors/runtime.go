package projectors

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
)

// Projector processes events from a DynamoDB Stream and maintains read models.
type Projector interface {
	// Process handles a batch of stream records, returning identifiers of records
	// that should be retried (partial batch failure).
	Process(ctx context.Context, records []StreamRecord) ([]BatchItemFailure, error)

	// Name returns a stable identifier for this projector, used in idempotency
	// keys and telemetry labels.
	Name() string
}

// StreamRecord represents a single event from a DynamoDB Streams trigger,
// converted from the raw DynamoDB record into a domain-friendly shape.
type StreamRecord struct {
	EventID                     string
	EntityID                    string
	EntityType                  string
	EventType                   string
	Version                     int
	Sequence                    int
	Payload                     []byte
	Metadata                    json.RawMessage
	ApproximateCreationDateTime time.Time
}

// BatchItemFailure identifies a single record that Lambda should retry.
// The ItemIdentifier corresponds to the DynamoDB stream record's EventID.
type BatchItemFailure struct {
	ItemIdentifier string `json:"itemIdentifier"`
}

// Runtime wraps a Projector with idempotency checking, retry classification,
// and structured logging/telemetry.
type Runtime struct {
	projector   Projector
	idempotency IdempotencyGuard
	logger      *slog.Logger
}

// NewRuntime creates a Runtime that decorates the given projector with the
// provided idempotency guard and logger.
func NewRuntime(projector Projector, idempotency IdempotencyGuard, logger *slog.Logger) *Runtime {
	if logger == nil {
		logger = slog.Default()
	}

	return &Runtime{
		projector:   projector,
		idempotency: idempotency,
		logger:      logger,
	}
}

// Logger returns the runtime's logger, falling back to slog.Default() when unset.
func (r *Runtime) Logger() *slog.Logger {
	if r == nil || r.logger == nil {
		return slog.Default()
	}

	return r.logger
}

// Process runs the projector over the given records, skipping already-processed
// events (idempotency) and classifying errors for retry vs DLQ routing.
//
// On a batch-level projector error, the error is classified:
//   - Permanent: the batch's records are reported as partial-batch failures
//     ([]BatchItemFailure). Lambda retries them up to the event source mapping's
//     configured limit and then routes them to its on-failure destination (DLQ);
//     there is no handler-initiated "send straight to DLQ" in Lambda.
//   - Transient or unknown: the error is returned so Lambda fails and retries the
//     whole invocation.
//
// Per-record failures returned by the projector itself (with a nil error) are
// passed through as partial-batch failures and the successful records are marked
// processed for idempotency.
func (r *Runtime) Process(ctx context.Context, records []StreamRecord) ([]BatchItemFailure, error) {
	logger := r.Logger().With("projector", r.projector.Name())

	logger.Info("Processing batch", "recordCount", len(records))

	pending := make([]StreamRecord, 0, len(records))
	for _, rec := range records {
		processed, err := r.idempotency.IsProcessed(ctx, r.projector.Name(), rec.EventID)
		if err != nil {
			logger.Warn("Idempotency check failed, proceeding with record",
				"eventID", rec.EventID, "error", err)
			pending = append(pending, rec)
			continue
		}
		if processed {
			logger.Debug("Skipping already-processed event", "eventID", rec.EventID)
			continue
		}
		pending = append(pending, rec)
	}

	if len(pending) == 0 {
		logger.Info("All records already processed")
		return nil, nil
	}

	failures, err := r.projector.Process(ctx, pending)
	if err != nil {
		classification := ClassifyError(err)
		logger.Error("Projector batch error",
			"error", err,
			"classification", classification,
		)
		if classification == RetryPermanent {
			allFailures := make([]BatchItemFailure, 0, len(pending))
			for _, rec := range pending {
				allFailures = append(allFailures, BatchItemFailure{ItemIdentifier: rec.EventID})
			}
			return allFailures, nil
		}
		return nil, err
	}

	failedSet := make(map[string]struct{}, len(failures))
	for _, f := range failures {
		failedSet[f.ItemIdentifier] = struct{}{}
	}
	for _, rec := range pending {
		if _, failed := failedSet[rec.EventID]; failed {
			continue
		}
		if err := r.idempotency.MarkProcessed(ctx, r.projector.Name(), rec.EventID); err != nil {
			logger.Warn("Failed to mark event as processed",
				"eventID", rec.EventID, "error", err)
		}
	}

	logger.Info("Batch complete",
		"total", len(records),
		"pending", len(pending),
		"failures", len(failures),
	)

	return failures, nil
}
