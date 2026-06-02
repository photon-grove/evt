package stream

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/photon-grove/evt"
	"github.com/stretchr/testify/require"
)

type mockSNSPublishClient struct {
	publishBatchErr    error
	publishBatchInputs []*sns.PublishBatchInput
	// failedIDs allows tests to simulate partial batch failures by entry Id.
	failedIDs map[string]snstypes.BatchResultErrorEntry
}

func (m *mockSNSPublishClient) PublishBatch(_ context.Context, input *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	m.publishBatchInputs = append(m.publishBatchInputs, input)
	if m.publishBatchErr != nil {
		return nil, m.publishBatchErr
	}

	output := &sns.PublishBatchOutput{}
	for _, entry := range input.PublishBatchRequestEntries {
		if failed, ok := m.failedIDs[*entry.Id]; ok {
			output.Failed = append(output.Failed, failed)
		} else {
			output.Successful = append(output.Successful, snstypes.PublishBatchResultEntry{
				Id: entry.Id,
			})
		}
	}
	return output, nil
}

func TestSNSPublisher_Publish_DropsMalformedMetadata(t *testing.T) {
	client := &mockSNSPublishClient{}
	publisher, err := NewSNSPublisher(client, "arn:aws:sns:us-east-1:123456789012:test-topic", "example.events")
	require.NoError(t, err)

	records := []events.DynamoDBEventRecord{
		buildRecord(`{"region":"us-east-1"}`),
		buildRecord(`{"region":"us-east-1"`), // malformed metadata JSON
	}

	result, err := publisher.Publish(context.Background(), records)
	require.NoError(t, err)
	require.Len(t, client.publishBatchInputs, 1)
	require.Equal(t, 1, result.PublishedCount)
	require.Equal(t, 1, result.DroppedMalformedCount)
	require.Empty(t, result.FailedIndices)
}

func TestSNSPublisher_Publish_BatchCallFailureMarksAllFailed(t *testing.T) {
	client := &mockSNSPublishClient{publishBatchErr: errors.New("batch failed")}
	publisher, err := NewSNSPublisher(client, "arn:aws:sns:us-east-1:123456789012:test-topic", "example.events")
	require.NoError(t, err)

	result, err := publisher.Publish(context.Background(), []events.DynamoDBEventRecord{
		buildRecord(`{"region":"us-east-1"}`),
	})
	require.NoError(t, err)
	require.Len(t, client.publishBatchInputs, 1)
	require.Equal(t, 0, result.PublishedCount)
	require.Equal(t, []int{0}, result.FailedIndices)
	require.Equal(t, 0, result.DroppedMalformedCount)
}

func TestSNSPublisher_Publish_PartialBatchFailure(t *testing.T) {
	client := &mockSNSPublishClient{
		failedIDs: map[string]snstypes.BatchResultErrorEntry{
			"1": {Id: strPtr("1"), Code: strPtr("InternalError"), SenderFault: false},
		},
	}
	publisher, err := NewSNSPublisher(client, "arn:aws:sns:us-east-1:123456789012:test-topic", "example.events")
	require.NoError(t, err)

	records := []events.DynamoDBEventRecord{
		buildRecord(`{"region":"us-east-1"}`),
		buildRecord(`{"region":"us-east-1"}`),
		buildRecord(`{"region":"us-east-1"}`),
	}

	result, err := publisher.Publish(context.Background(), records)
	require.NoError(t, err)
	require.Equal(t, 2, result.PublishedCount)
	require.Equal(t, []int{1}, result.FailedIndices)
}

func TestSNSPublisher_Publish_EmitsCorrelationAttributes(t *testing.T) {
	client := &mockSNSPublishClient{}
	publisher, err := NewSNSPublisher(client, "arn:aws:sns:us-east-1:123456789012:test-topic", "example.events")
	require.NoError(t, err)

	records := []events.DynamoDBEventRecord{
		buildRecord(`{"region":"us-east-1","commandId":"cmd-123","trace":{"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"}}`),
	}

	result, err := publisher.Publish(context.Background(), records)
	require.NoError(t, err)
	require.Equal(t, 1, result.PublishedCount)
	require.Len(t, client.publishBatchInputs, 1)

	entries := client.publishBatchInputs[0].PublishBatchRequestEntries
	require.Len(t, entries, 1)
	attrs := entries[0].MessageAttributes
	require.Contains(t, attrs, "commandId")
	require.Contains(t, attrs, "correlationId")
	require.Contains(t, attrs, "traceparent")
	require.Equal(t, "cmd-123", *attrs["commandId"].StringValue)
	require.Equal(t, "cmd-123", *attrs["correlationId"].StringValue)
	require.Equal(t, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00", *attrs["traceparent"].StringValue)
}

func TestSNSPublisher_Publish_FallsBackCorrelationToTraceparent(t *testing.T) {
	client := &mockSNSPublishClient{}
	publisher, err := NewSNSPublisher(client, "arn:aws:sns:us-east-1:123456789012:test-topic", "example.events")
	require.NoError(t, err)

	records := []events.DynamoDBEventRecord{
		buildRecord(`{"region":"us-east-1","trace":{"traceparent":"00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"}}`),
	}

	result, err := publisher.Publish(context.Background(), records)
	require.NoError(t, err)
	require.Equal(t, 1, result.PublishedCount)
	require.Len(t, client.publishBatchInputs, 1)

	entries := client.publishBatchInputs[0].PublishBatchRequestEntries
	require.Len(t, entries, 1)
	attrs := entries[0].MessageAttributes
	require.NotContains(t, attrs, "commandId")
	require.Contains(t, attrs, "correlationId")
	require.Contains(t, attrs, "traceparent")
	require.Equal(t, "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01", *attrs["correlationId"].StringValue)
}

func TestSNSPublisher_Publish_BatchesByMaxSize(t *testing.T) {
	client := &mockSNSPublishClient{}
	publisher, err := NewSNSPublisher(client, "arn:aws:sns:us-east-1:123456789012:test-topic", "example.events")
	require.NoError(t, err)

	// 12 valid records should produce 2 batch calls (10 + 2)
	records := make([]events.DynamoDBEventRecord, 12)
	for i := range records {
		records[i] = buildRecord(`{"region":"us-east-1"}`)
	}

	result, err := publisher.Publish(context.Background(), records)
	require.NoError(t, err)
	require.Len(t, client.publishBatchInputs, 2)
	require.Len(t, client.publishBatchInputs[0].PublishBatchRequestEntries, 10)
	require.Len(t, client.publishBatchInputs[1].PublishBatchRequestEntries, 2)
	require.Equal(t, 12, result.PublishedCount)
	require.Empty(t, result.FailedIndices)
}

func TestSNSPublisher_Publish_EmptyRecords(t *testing.T) {
	client := &mockSNSPublishClient{}
	publisher, err := NewSNSPublisher(client, "arn:aws:sns:us-east-1:123456789012:test-topic", "example.events")
	require.NoError(t, err)

	result, err := publisher.Publish(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, 0, result.PublishedCount)
	require.Empty(t, result.FailedIndices)
	require.Empty(t, client.publishBatchInputs)
}

func TestSNSPublisher_Publish_DualPublishesEligibleEventsToFIFO(t *testing.T) {
	client := &mockSNSPublishClient{}
	publisher, err := NewSNSPublisher(
		client,
		"arn:aws:sns:us-east-1:123456789012:std-topic",
		"example.events",
		WithFIFOTarget("arn:aws:sns:us-east-1:123456789012:fifo-topic.fifo",
			func(_ evt.SerializedEvent) (string, bool) { return "world-a:loc-1", true }),
	)
	require.NoError(t, err)

	result, err := publisher.Publish(context.Background(), []events.DynamoDBEventRecord{
		buildRecord(`{"region":"us-east-1"}`),
	})
	require.NoError(t, err)
	require.Equal(t, 1, result.PublishedCount)
	require.Empty(t, result.FailedIndices)

	require.Len(t, client.publishBatchInputs, 2)
	// FIFO is flushed first so a FIFO failure does not force a retry that
	// would duplicate standard-topic publishes (standard has no dedup).
	fifoInput := client.publishBatchInputs[0]
	stdInput := client.publishBatchInputs[1]
	require.Equal(t, "arn:aws:sns:us-east-1:123456789012:fifo-topic.fifo", *fifoInput.TopicArn)
	require.Equal(t, "arn:aws:sns:us-east-1:123456789012:std-topic", *stdInput.TopicArn)

	require.Len(t, fifoInput.PublishBatchRequestEntries, 1)
	fifoEntry := fifoInput.PublishBatchRequestEntries[0]
	require.NotNil(t, fifoEntry.MessageGroupId)
	require.Equal(t, "world-a:loc-1", *fifoEntry.MessageGroupId)
	require.NotNil(t, fifoEntry.MessageDeduplicationId)
	require.NotEmpty(t, *fifoEntry.MessageDeduplicationId)
}

func TestSNSPublisher_Publish_SkipsFIFOWhenExtractorReturnsFalse(t *testing.T) {
	client := &mockSNSPublishClient{}
	publisher, err := NewSNSPublisher(
		client,
		"arn:aws:sns:us-east-1:123456789012:std-topic",
		"example.events",
		WithFIFOTarget("arn:aws:sns:us-east-1:123456789012:fifo-topic.fifo",
			func(_ evt.SerializedEvent) (string, bool) { return "", false }),
	)
	require.NoError(t, err)

	result, err := publisher.Publish(context.Background(), []events.DynamoDBEventRecord{
		buildRecord(`{"region":"us-east-1"}`),
	})
	require.NoError(t, err)
	require.Equal(t, 1, result.PublishedCount)
	require.Empty(t, result.FailedIndices)

	// Only the standard topic should be hit.
	require.Len(t, client.publishBatchInputs, 1)
	require.Equal(t, "arn:aws:sns:us-east-1:123456789012:std-topic", *client.publishBatchInputs[0].TopicArn)
}

func TestSNSPublisher_Publish_FIFOFailureMarksRecordFailed(t *testing.T) {
	client := &mockSNSPublishClient{
		failedIDs: map[string]snstypes.BatchResultErrorEntry{
			// The single entry Id "0" is reused across standard and FIFO batches;
			// mark FIFO-side failures by inspecting TopicArn in a custom client.
		},
	}
	// Custom client: standard succeeds, FIFO fails entirely.
	wrapped := &topicScopedMockClient{
		inner:          client,
		failForTopicIn: "fifo",
	}
	publisher, err := NewSNSPublisher(
		wrapped,
		"arn:aws:sns:us-east-1:123456789012:std-topic",
		"example.events",
		WithFIFOTarget("arn:aws:sns:us-east-1:123456789012:fifo-topic.fifo",
			func(_ evt.SerializedEvent) (string, bool) { return "world-a:loc-1", true }),
	)
	require.NoError(t, err)

	result, err := publisher.Publish(context.Background(), []events.DynamoDBEventRecord{
		buildRecord(`{"region":"us-east-1"}`),
	})
	require.NoError(t, err)
	require.Equal(t, 1, result.PublishedCount) // standard succeeded
	require.Equal(t, []int{0}, result.FailedIndices)
}

func TestNewSNSPublisher_RejectsNonFIFOARN(t *testing.T) {
	client := &mockSNSPublishClient{}
	_, err := NewSNSPublisher(
		client,
		"arn:aws:sns:us-east-1:123456789012:std-topic",
		"example.events",
		WithFIFOTarget("arn:aws:sns:us-east-1:123456789012:not-fifo",
			func(_ evt.SerializedEvent) (string, bool) { return "g", true }),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), ".fifo")
}

func TestNewSNSPublisher_RejectsNilFIFOExtractor(t *testing.T) {
	client := &mockSNSPublishClient{}
	_, err := NewSNSPublisher(
		client,
		"arn:aws:sns:us-east-1:123456789012:std-topic",
		"example.events",
		WithFIFOTarget("arn:aws:sns:us-east-1:123456789012:fifo-topic.fifo", nil),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "extractor")
}

// topicScopedMockClient lets tests fail publishes for the FIFO topic only.
type topicScopedMockClient struct {
	inner          *mockSNSPublishClient
	failForTopicIn string // substring of TopicARN that should fail
}

func (t *topicScopedMockClient) PublishBatch(_ context.Context, input *sns.PublishBatchInput, _ ...func(*sns.Options)) (*sns.PublishBatchOutput, error) {
	t.inner.publishBatchInputs = append(t.inner.publishBatchInputs, input)
	if input.TopicArn != nil && t.failForTopicIn != "" && strings.Contains(*input.TopicArn, t.failForTopicIn) {
		return nil, errors.New("simulated FIFO publish failure")
	}
	output := &sns.PublishBatchOutput{}
	for _, entry := range input.PublishBatchRequestEntries {
		output.Successful = append(output.Successful, snstypes.PublishBatchResultEntry{Id: entry.Id})
	}
	return output, nil
}

func strPtr(s string) *string { return &s }

func buildRecord(metadata string) events.DynamoDBEventRecord {
	return events.DynamoDBEventRecord{
		Change: events.DynamoDBStreamRecord{
			NewImage: map[string]events.DynamoDBAttributeValue{
				"pk":         events.NewStringAttribute("entity-1"),
				"sk":         events.NewNumberAttribute("1"),
				"entityType": events.NewStringAttribute("portfolio"),
				"type":       events.NewStringAttribute("OrderPlaced"),
				"version":    events.NewNumberAttribute("1"),
				"payload":    events.NewStringAttribute(`{"orderId":"ord-1"}`),
				"metadata":   events.NewStringAttribute(metadata),
			},
		},
	}
}
