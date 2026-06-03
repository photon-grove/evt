package stream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
)

// GroupIDExtractor maps a serialized domain event to an SNS FIFO
// MessageGroupId. Returning ("", false) skips FIFO publish for the event,
// leaving the standard topic as the sole delivery path.
type GroupIDExtractor func(event evt.SerializedEvent) (groupID string, ok bool)

// fifoTarget holds the optional FIFO companion topic configuration.
type fifoTarget struct {
	topicARN  string
	extractor GroupIDExtractor
}

// SNSPublisher publishes DynamoDB stream records to an SNS topic.
// Each INSERT event from the event log is converted to an EventBridge-style
// CloudWatchEvent envelope carrying the serialized domain event as Detail,
// then sent as the SNS message body. This preserves compatibility with
// existing consumers (e.g. the audit log projector) that expect the
// EventBridge event shape.
//
// When a FIFO companion topic is configured via WithFIFOTarget, eligible
// events (as decided by the GroupIDExtractor) are also published to the FIFO
// topic with MessageGroupId for per-group ordering and MessageDeduplicationId
// set to the serialized event ID for SNS-side idempotency across retries.
type SNSPublisher struct {
	client   snsPublishClient
	topicARN string
	source   string
	fifo     *fifoTarget
	logger   *slog.Logger
}

type snsPublishClient interface {
	PublishBatch(ctx context.Context, params *sns.PublishBatchInput, optFns ...func(*sns.Options)) (*sns.PublishBatchOutput, error)
}

// Option customizes SNSPublisher construction.
type Option func(*SNSPublisher) error

// WithFIFOTarget configures an SNS FIFO companion topic. Events for which the
// extractor returns ok=true are published to the FIFO topic with the returned
// MessageGroupId; the serialized event ID is used as MessageDeduplicationId.
// Events where the extractor returns ok=false are not forwarded to FIFO.
func WithFIFOTarget(topicARN string, extractor GroupIDExtractor) Option {
	return func(p *SNSPublisher) error {
		arn := strings.TrimSpace(topicARN)
		if arn == "" {
			return fmt.Errorf("fifo topic ARN is required")
		}
		if !strings.HasSuffix(arn, ".fifo") {
			return fmt.Errorf("fifo topic ARN must end in .fifo: %q", arn)
		}
		if extractor == nil {
			return fmt.Errorf("fifo group ID extractor is required")
		}
		p.fifo = &fifoTarget{topicARN: arn, extractor: extractor}
		return nil
	}
}

// WithLogger configures the logger used by the publisher. If unset, slog.Default() is used.
func WithLogger(logger *slog.Logger) Option {
	return func(p *SNSPublisher) error {
		p.logger = logger

		return nil
	}
}

// NewSNSPublisher creates a new SNS publisher.
// source is the logical event source (e.g. "example.events").
func NewSNSPublisher(client snsPublishClient, topicARN string, source string, opts ...Option) (*SNSPublisher, error) {
	if client == nil {
		return nil, fmt.Errorf("client is required")
	}
	if topicARN == "" {
		return nil, fmt.Errorf("topicARN is required")
	}
	if source == "" {
		return nil, fmt.Errorf("source is required")
	}

	pub := &SNSPublisher{
		client:   client,
		topicARN: topicARN,
		source:   source,
	}
	for _, opt := range opts {
		if err := opt(pub); err != nil {
			return nil, err
		}
	}
	return pub, nil
}

func (p *SNSPublisher) loggerOrDefault() *slog.Logger {
	if p == nil || p.logger == nil {
		return slog.Default()
	}

	return p.logger
}

// PublishResult contains the outcome of publishing a batch of records.
type PublishResult struct {
	// FailedIndices contains the indices of records that failed to publish.
	// These can be used to construct DynamoDB batch item failures.
	FailedIndices []int
	// PublishedCount is the number of records successfully published.
	PublishedCount int
	// SkippedCount is the number of records skipped (e.g., empty NewImage).
	SkippedCount int
	// DroppedMalformedCount is the number of malformed records explicitly dropped.
	DroppedMalformedCount int
}

// maxBatchSize is the maximum number of messages per SNS PublishBatch call.
const maxBatchSize = 10

// batchEntry holds a prepared SNS batch entry along with the original record index.
type batchEntry struct {
	entry snstypes.PublishBatchRequestEntry
	index int
}

// fifoBatchEntry extends batchEntry with the FIFO-specific routing attributes.
// The standard topic batch entry is built once; the FIFO sibling reuses the
// same message body/attributes plus MessageGroupId and MessageDeduplicationId.
type fifoBatchEntry struct {
	entry snstypes.PublishBatchRequestEntry
	index int
}

// Publish converts DynamoDB records to domain events and publishes them to SNS
// using batched PublishBatch calls (up to 10 messages per call).
// Returns a PublishResult with indices of failed records for batch failure handling.
func (p *SNSPublisher) Publish(ctx context.Context, records []events.DynamoDBEventRecord) (*PublishResult, error) {
	result := &PublishResult{
		FailedIndices: make([]int, 0),
	}

	if len(records) == 0 {
		return result, nil
	}

	logger := p.loggerOrDefault()
	logger.Info("Publishing records to SNS", "count", len(records), "topic", p.topicARN)

	batch := make([]batchEntry, 0, min(len(records), maxBatchSize))
	var fifoBatch []fifoBatchEntry
	if p.fifo != nil {
		fifoBatch = make([]fifoBatchEntry, 0, min(len(records), maxBatchSize))
	}

	for i, record := range records {
		// Skip records with empty NewImage
		if len(record.Change.NewImage) == 0 {
			logger.Warn("Skipping record with empty NewImage", "index", i)
			result.SkippedCount++
			continue
		}

		domainEvent, eventJSON, err := dynamo.MarshalJSON(record.Change.NewImage)
		if err != nil {
			var invalidEventErr *dynamo.InvalidEventError
			if errors.As(err, &invalidEventErr) {
				logger.Warn("Dropping malformed domain event",
					"index", i,
					"field", invalidEventErr.Field,
					"reason", invalidEventErr.Reason,
					"error", invalidEventErr.Err,
					"metric", "MalformedEventsDropped",
					"metricValue", 1,
				)
				result.DroppedMalformedCount++
				continue
			}

			logger.Error("Failed to marshal domain event",
				"index", i,
				"error", err,
			)
			result.FailedIndices = append(result.FailedIndices, i)
			continue
		}

		// Wrap the serialized event in a CloudWatchEvent-style envelope to
		// match the existing EventBridge-based consumers.
		envelope := events.CloudWatchEvent{
			ID:         string(domainEvent.ID),
			Source:     p.source,
			DetailType: string(domainEvent.Type),
			Detail:     eventJSON,
		}

		envelopeJSON, err := json.Marshal(envelope)
		if err != nil {
			logger.Error("Failed to marshal CloudWatchEvent envelope",
				"index", i,
				"error", err,
			)
			result.FailedIndices = append(result.FailedIndices, i)
			continue
		}

		attrs := map[string]snstypes.MessageAttributeValue{
			"entityType": {
				DataType:    aws.String("String"),
				StringValue: aws.String(string(domainEvent.EntityType)),
			},
			"eventType": {
				DataType:    aws.String("String"),
				StringValue: aws.String(string(domainEvent.Type)),
			},
		}
		for key, value := range correlationAttributes(domainEvent.Metadata) {
			attrs[key] = value
		}

		batch = append(batch, batchEntry{
			entry: snstypes.PublishBatchRequestEntry{
				Id:                aws.String(fmt.Sprintf("%d", i)),
				Message:           aws.String(string(envelopeJSON)),
				MessageAttributes: attrs,
			},
			index: i,
		})

		if p.fifo != nil {
			if groupID, ok := p.fifo.extractor(domainEvent); ok && groupID != "" {
				fifoBatch = append(fifoBatch, fifoBatchEntry{
					entry: snstypes.PublishBatchRequestEntry{
						Id:                     aws.String(fmt.Sprintf("%d", i)),
						Message:                aws.String(string(envelopeJSON)),
						MessageAttributes:      attrs,
						MessageGroupId:         aws.String(groupID),
						MessageDeduplicationId: aws.String(string(domainEvent.ID)),
					},
					index: i,
				})
			}
		}

		// Flush FIFO first so a FIFO failure that marks records as failed
		// drives retries back through *both* topics; the FIFO dedup window
		// (keyed on MessageDeduplicationId = event ID) absorbs the duplicate
		// FIFO publish, while the standard topic sees the publish exactly
		// once. If we flushed standard first, a subsequent FIFO failure
		// would make the whole index fail, and retries would re-publish to
		// the standard topic — which has no dedup — producing duplicate
		// deliveries to the feed projector and webhook dispatcher.
		if p.fifo != nil && len(fifoBatch) >= maxBatchSize {
			p.flushFIFOBatch(ctx, fifoBatch, result, logger)
			fifoBatch = fifoBatch[:0]
		}
		if len(batch) >= maxBatchSize {
			p.flushBatch(ctx, batch, result, logger)
			batch = batch[:0]
		}
	}

	// Flush remaining entries — same order rationale as above.
	if p.fifo != nil && len(fifoBatch) > 0 {
		p.flushFIFOBatch(ctx, fifoBatch, result, logger)
	}
	if len(batch) > 0 {
		p.flushBatch(ctx, batch, result, logger)
	}

	failedCount := len(result.FailedIndices)
	if failedCount > 0 || result.SkippedCount > 0 || result.DroppedMalformedCount > 0 {
		logger.Warn("Publishing to SNS complete with issues",
			"total", len(records),
			"published", result.PublishedCount,
			"failed", failedCount,
			"skipped", result.SkippedCount,
			"malformedDropped", result.DroppedMalformedCount,
		)
	} else {
		logger.Info("Publishing to SNS complete",
			"total", len(records),
			"published", result.PublishedCount,
		)
	}

	return result, nil
}

// flushBatch sends a batch of entries via PublishBatch and updates the result.
func (p *SNSPublisher) flushBatch(ctx context.Context, batch []batchEntry, result *PublishResult, logger *slog.Logger) {
	// Build an index map from batch entry Id to original record index.
	indexByID := make(map[string]int, len(batch))
	entries := make([]snstypes.PublishBatchRequestEntry, 0, len(batch))
	for _, b := range batch {
		indexByID[*b.entry.Id] = b.index
		entries = append(entries, b.entry)
	}

	output, err := p.client.PublishBatch(ctx, &sns.PublishBatchInput{
		TopicArn:                   aws.String(p.topicARN),
		PublishBatchRequestEntries: entries,
	})
	if err != nil {
		// Whole batch failed — mark every entry as failed.
		logger.Error("PublishBatch call failed",
			"error", err,
			"batchSize", len(entries),
		)
		for _, b := range batch {
			result.FailedIndices = append(result.FailedIndices, b.index)
		}
		return
	}

	result.PublishedCount += len(output.Successful)

	for _, failed := range output.Failed {
		idx, ok := indexByID[*failed.Id]
		if !ok {
			logger.Error("PublishBatch returned unknown failed entry Id", "id", *failed.Id)
			continue
		}
		logger.Error("PublishBatch partial failure",
			"index", idx,
			"code", *failed.Code,
			"message", aws.ToString(failed.Message),
			"senderFault", failed.SenderFault,
		)
		result.FailedIndices = append(result.FailedIndices, idx)
	}
}

// flushFIFOBatch sends a batch of FIFO entries to the companion FIFO topic.
// Failures are recorded on the shared result so batch-item-failure semantics
// cover both the standard and FIFO publishes; duplicates caused by retries
// are absorbed by the FIFO dedup window keyed on MessageDeduplicationId.
func (p *SNSPublisher) flushFIFOBatch(ctx context.Context, batch []fifoBatchEntry, result *PublishResult, logger *slog.Logger) {
	if p.fifo == nil || len(batch) == 0 {
		return
	}

	indexByID := make(map[string]int, len(batch))
	entries := make([]snstypes.PublishBatchRequestEntry, 0, len(batch))
	for _, b := range batch {
		indexByID[*b.entry.Id] = b.index
		entries = append(entries, b.entry)
	}

	output, err := p.client.PublishBatch(ctx, &sns.PublishBatchInput{
		TopicArn:                   aws.String(p.fifo.topicARN),
		PublishBatchRequestEntries: entries,
	})
	if err != nil {
		logger.Error("FIFO PublishBatch call failed",
			"error", err,
			"batchSize", len(entries),
			"topic", p.fifo.topicARN,
		)
		for _, b := range batch {
			result.FailedIndices = appendUnique(result.FailedIndices, b.index)
		}
		return
	}

	for _, failed := range output.Failed {
		idx, ok := indexByID[*failed.Id]
		if !ok {
			logger.Error("FIFO PublishBatch returned unknown failed entry Id", "id", *failed.Id)
			continue
		}
		logger.Error("FIFO PublishBatch partial failure",
			"index", idx,
			"code", *failed.Code,
			"message", aws.ToString(failed.Message),
			"senderFault", failed.SenderFault,
		)
		result.FailedIndices = appendUnique(result.FailedIndices, idx)
	}
}

// appendUnique appends idx to indices only if it is not already present.
// Callers may report the same record index as failed via both the standard
// and FIFO paths; we only need one batch-item-failure entry per record.
func appendUnique(indices []int, idx int) []int {
	for _, existing := range indices {
		if existing == idx {
			return indices
		}
	}
	return append(indices, idx)
}

func correlationAttributes(metadata evt.Metadata) map[string]snstypes.MessageAttributeValue {
	attrs := map[string]snstypes.MessageAttributeValue{}

	if metadata.CommandID != nil && *metadata.CommandID != "" {
		commandID := string(*metadata.CommandID)
		attrs["commandId"] = snstypes.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(commandID),
		}
		// Use commandId as the canonical cross-hop correlation key when available.
		attrs["correlationId"] = snstypes.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(commandID),
		}
	}

	if metadata.Trace != nil {
		if traceParent, ok := (*metadata.Trace)["traceparent"]; ok && traceParent != "" {
			attrs["traceparent"] = snstypes.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(traceParent),
			}
			if _, hasCorrelationID := attrs["correlationId"]; !hasCorrelationID {
				attrs["correlationId"] = snstypes.MessageAttributeValue{
					DataType:    aws.String("String"),
					StringValue: aws.String(traceParent),
				}
			}
		}
	}

	return attrs
}
