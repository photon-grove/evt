package publishers_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/photon-grove/evt/ingressbudget"
	"github.com/photon-grove/evt/logging"
	"github.com/photon-grove/evt/publishers"
	"github.com/photon-grove/evt/stream"
	"github.com/stretchr/testify/require"
)

type stubPublisher struct {
	result *stream.PublishResult
	err    error
	calls  int
}

func (s *stubPublisher) Publish(_ context.Context, _ []events.DynamoDBEventRecord) (*stream.PublishResult, error) {
	s.calls++
	return s.result, s.err
}

type stubBudget struct {
	eventDecision ingressbudget.Decision
	retryDecision ingressbudget.Decision
}

func (s *stubBudget) AllowEvent(_ time.Time) ingressbudget.Decision {
	if s.eventDecision == "" {
		return ingressbudget.DecisionAllow
	}
	return s.eventDecision
}

func (s *stubBudget) AllowRetry(_ time.Time) ingressbudget.Decision {
	if s.retryDecision == "" {
		return ingressbudget.DecisionAllow
	}
	return s.retryDecision
}

func newInsertEvent(id string) events.DynamoDBEvent {
	return events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{
				EventID:   id,
				EventName: "INSERT",
				Change: events.DynamoDBStreamRecord{
					SequenceNumber: "123",
					NewImage: map[string]events.DynamoDBAttributeValue{
						"pk": events.NewStringAttribute("entity-1"),
					},
				},
			},
		},
	}
}

func TestHandleDynamoDBEvent_Success(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), logging.NewLogger(true))
	pub := &stubPublisher{result: &stream.PublishResult{}}
	budget := &stubBudget{}

	resp, err := publishers.HandleDynamoDBEvent(ctx, newInsertEvent("evt-1"), pub, budget)
	require.NoError(t, err)
	require.Len(t, resp.BatchItemFailures, 0)
	require.Equal(t, 1, pub.calls)
}

func TestHandleDynamoDBEvent_DropsOnIngressBudget(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), logging.NewLogger(true))
	pub := &stubPublisher{result: &stream.PublishResult{}}
	budget := &stubBudget{eventDecision: ingressbudget.DecisionDrop}

	resp, err := publishers.HandleDynamoDBEvent(ctx, newInsertEvent("evt-1"), pub, budget)
	require.NoError(t, err)
	require.Len(t, resp.BatchItemFailures, 1)
	require.Equal(t, "123", resp.BatchItemFailures[0].ItemIdentifier)
	require.Equal(t, 0, pub.calls)
}

func TestHandleDynamoDBEvent_FailureReturnsBatchItemFailure(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), logging.NewLogger(true))
	pub := &stubPublisher{err: errors.New("publish failed")}
	budget := &stubBudget{}

	resp, err := publishers.HandleDynamoDBEvent(ctx, newInsertEvent("evt-1"), pub, budget)
	require.NoError(t, err)
	require.Len(t, resp.BatchItemFailures, 1)
	require.Equal(t, "123", resp.BatchItemFailures[0].ItemIdentifier)
	require.Equal(t, 1, pub.calls)
}

func TestHandleDynamoDBEvent_FailedIndicesReturnBatchItemFailure(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), logging.NewLogger(true))
	pub := &stubPublisher{
		result: &stream.PublishResult{
			FailedIndices: []int{0},
		},
	}
	budget := &stubBudget{}

	resp, err := publishers.HandleDynamoDBEvent(ctx, newInsertEvent("evt-1"), pub, budget)
	require.NoError(t, err)
	require.Len(t, resp.BatchItemFailures, 1)
	require.Equal(t, "123", resp.BatchItemFailures[0].ItemIdentifier)
	require.Equal(t, 1, pub.calls)
}

func TestHandleDynamoDBEvent_DropsMalformedRecordWithoutRetryFailure(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), logging.NewLogger(true))
	pub := &stubPublisher{
		result: &stream.PublishResult{
			DroppedMalformedCount: 1,
		},
	}
	budget := &stubBudget{}

	resp, err := publishers.HandleDynamoDBEvent(ctx, newInsertEvent("evt-1"), pub, budget)
	require.NoError(t, err)
	require.Empty(t, resp.BatchItemFailures)
	require.Equal(t, 1, pub.calls)
}

func TestHandleDynamoDBEvent_IgnoresNonInsertRecords(t *testing.T) {
	ctx := logging.WithLogger(context.Background(), logging.NewLogger(true))
	pub := &stubPublisher{result: &stream.PublishResult{}}
	budget := &stubBudget{}

	event := events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{EventName: "MODIFY"},
			{EventName: "REMOVE"},
		},
	}

	resp, err := publishers.HandleDynamoDBEvent(ctx, event, pub, budget)
	require.NoError(t, err)
	require.Empty(t, resp.BatchItemFailures)
	require.Equal(t, 0, pub.calls)
}
