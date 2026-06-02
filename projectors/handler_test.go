package projectors_test

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/photon-grove/evt/logging"
	"github.com/photon-grove/evt/projectors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestRuntime(proj *stubProjector) *projectors.Runtime {
	guard := projectors.NewInMemoryIdempotencyGuard()
	return projectors.NewRuntime(proj, guard, testLogger())
}

func TestLambdaHandler_ProcessesINSERTRecords(t *testing.T) {
	proj := &stubProjector{name: "handler-test"}
	rt := newTestRuntime(proj)
	handler := projectors.NewLambdaHandler(rt)

	ctx := logging.WithLogger(context.Background(), testLogger())
	event := events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				EventID:   "shardId-001:12345",
				EventName: "INSERT",
				Change: events.DynamoDBStreamRecord{
					NewImage: map[string]events.DynamoDBAttributeValue{
						"pk":         events.NewStringAttribute("entity-1"),
						"sk":         events.NewNumberAttribute("1"),
						"entityType": events.NewStringAttribute("universe"),
						"type":       events.NewStringAttribute("universe:created"),
						"version":    events.NewNumberAttribute("2"),
						"payload":    events.NewStringAttribute(`{"name":"Test"}`),
						"metadata":   events.NewStringAttribute(`{}`),
					},
				},
			},
		},
	}

	resp, err := handler(ctx, event)
	require.NoError(t, err)
	assert.Empty(t, resp.BatchItemFailures)
	assert.Equal(t, 1, proj.called)
	require.Len(t, proj.lastBatch, 1)
	assert.Equal(t, "shardId-001:12345", proj.lastBatch[0].EventID)
	assert.Equal(t, "entity-1", proj.lastBatch[0].EntityID)
	assert.Equal(t, "universe", proj.lastBatch[0].EntityType)
	assert.Equal(t, "universe:created", proj.lastBatch[0].EventType)
	assert.Equal(t, 1, proj.lastBatch[0].Sequence)
	assert.Equal(t, 2, proj.lastBatch[0].Version)
}

func TestLambdaHandler_SkipsNonINSERTRecords(t *testing.T) {
	proj := &stubProjector{name: "handler-test"}
	rt := newTestRuntime(proj)
	handler := projectors.NewLambdaHandler(rt)

	ctx := logging.WithLogger(context.Background(), testLogger())
	event := events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				EventID:   "shardId-001:11111",
				EventName: "MODIFY",
				Change: events.DynamoDBStreamRecord{
					NewImage: map[string]events.DynamoDBAttributeValue{
						"pk": events.NewStringAttribute("entity-1"),
					},
				},
			},
			{
				EventID:   "shardId-001:22222",
				EventName: "REMOVE",
				Change:    events.DynamoDBStreamRecord{},
			},
		},
	}

	resp, err := handler(ctx, event)
	require.NoError(t, err)
	assert.Empty(t, resp.BatchItemFailures)
	assert.Equal(t, 0, proj.called, "projector should not be called for non-INSERT events")
}

func TestLambdaHandler_ReturnsPartialBatchFailures(t *testing.T) {
	proj := &stubProjector{
		name: "handler-test",
		failures: []projectors.BatchItemFailure{
			{ItemIdentifier: "shardId-001:22222"},
		},
	}
	rt := newTestRuntime(proj)
	handler := projectors.NewLambdaHandler(rt)

	ctx := logging.WithLogger(context.Background(), testLogger())
	event := events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				EventID:   "shardId-001:11111",
				EventName: "INSERT",
				Change: events.DynamoDBStreamRecord{
					NewImage: map[string]events.DynamoDBAttributeValue{
						"pk":         events.NewStringAttribute("entity-1"),
						"sk":         events.NewNumberAttribute("1"),
						"type":       events.NewStringAttribute("universe:created"),
						"entityType": events.NewStringAttribute("universe"),
					},
				},
			},
			{
				EventID:   "shardId-001:22222",
				EventName: "INSERT",
				Change: events.DynamoDBStreamRecord{
					NewImage: map[string]events.DynamoDBAttributeValue{
						"pk":         events.NewStringAttribute("entity-2"),
						"sk":         events.NewNumberAttribute("1"),
						"type":       events.NewStringAttribute("universe:updated"),
						"entityType": events.NewStringAttribute("universe"),
					},
				},
			},
		},
	}

	resp, err := handler(ctx, event)
	require.NoError(t, err)
	require.Len(t, resp.BatchItemFailures, 1)
	assert.Equal(t, "shardId-001:22222", resp.BatchItemFailures[0].ItemIdentifier)
}

func TestLambdaHandler_SkipsRecordsMissingRequiredFields(t *testing.T) {
	proj := &stubProjector{name: "handler-test"}
	rt := newTestRuntime(proj)
	handler := projectors.NewLambdaHandler(rt)

	ctx := logging.WithLogger(context.Background(), testLogger())
	event := events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				EventID:   "shardId-001:33333",
				EventName: "INSERT",
				Change: events.DynamoDBStreamRecord{
					NewImage: map[string]events.DynamoDBAttributeValue{
						"pk": events.NewStringAttribute("entity-1"),
						// Missing sk and type — should be skipped.
					},
				},
			},
		},
	}

	resp, err := handler(ctx, event)
	require.NoError(t, err)
	assert.Empty(t, resp.BatchItemFailures)
	assert.Equal(t, 0, proj.called, "projector should not be called for records missing required fields")
}
