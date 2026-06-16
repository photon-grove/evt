package projectors_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/projectors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// envelopeFor builds the CloudWatchEvent envelope JSON exactly as
// stream.SNSPublisher does: the serialized domain event is the Detail. This is
// the body a SNS->SQS (raw delivery) message or an SNS->Lambda record carries.
func envelopeFor(t *testing.T, event evt.SerializedEvent) []byte {
	t.Helper()

	detail, err := json.Marshal(event)
	require.NoError(t, err)

	envelope := events.CloudWatchEvent{
		ID:         string(event.ID),
		Source:     "test.events",
		DetailType: string(event.Type),
		Detail:     detail,
	}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)

	return body
}

func sampleEvent(id, entity string) evt.SerializedEvent {
	return evt.SerializedEvent{
		ID:         evt.EventID(id),
		EntityID:   evt.EntityID(entity),
		EntityType: evt.EntityType("account"),
		Sequence:   evt.EventSequence(3),
		Type:       evt.EventType("account:opened"),
		Version:    evt.EventVersion(2),
		Payload:    []byte(`{"balance":100}`),
	}
}

func TestStreamRecordFromEnvelope_DecodesDomainEvent(t *testing.T) {
	body := envelopeFor(t, sampleEvent("account-1/3", "account-1"))

	record, err := projectors.StreamRecordFromEnvelope(body)
	require.NoError(t, err)

	assert.Equal(t, "account-1/3", record.EventID)
	assert.Equal(t, "account-1", record.EntityID)
	assert.Equal(t, "account", record.EntityType)
	assert.Equal(t, "account:opened", record.EventType)
	assert.Equal(t, 2, record.Version)
	assert.Equal(t, 3, record.Sequence)
	assert.JSONEq(t, `{"balance":100}`, string(record.Payload))
}

func TestStreamRecordFromEnvelope_RejectsMalformed(t *testing.T) {
	_, err := projectors.StreamRecordFromEnvelope([]byte("not json"))
	require.Error(t, err)

	// Envelope is valid JSON but its Detail has no entity id.
	noEntity := envelopeFor(t, evt.SerializedEvent{ID: evt.EventID("x")})
	_, err = projectors.StreamRecordFromEnvelope(noEntity)
	require.Error(t, err)

	// ...or no event id, which would otherwise collide on an empty idempotency key.
	noID := envelopeFor(t, evt.SerializedEvent{EntityID: evt.EntityID("acct-1")})
	_, err = projectors.StreamRecordFromEnvelope(noID)
	require.Error(t, err)
}

func TestSQSHandler_ProcessesAndReportsPartialFailures(t *testing.T) {
	// The projector fails the second event by its event ID; the handler must map
	// that back to the second SQS message's id, not the first.
	proj := &stubProjector{
		name:     "sqs-test",
		failures: []projectors.BatchItemFailure{{ItemIdentifier: "account-2/3"}},
	}
	rt := newTestRuntime(proj)
	handler := projectors.NewSQSHandler(rt)

	event := events.SQSEvent{
		Records: []events.SQSMessage{
			{MessageId: "msg-a", Body: string(envelopeFor(t, sampleEvent("account-1/3", "account-1")))},
			{MessageId: "msg-b", Body: string(envelopeFor(t, sampleEvent("account-2/3", "account-2")))},
		},
	}

	resp, err := handler(context.Background(), event)
	require.NoError(t, err)
	require.Len(t, resp.BatchItemFailures, 1)
	assert.Equal(t, "msg-b", resp.BatchItemFailures[0].ItemIdentifier)
	assert.Equal(t, 1, proj.called)
	assert.Len(t, proj.lastBatch, 2)
}

func TestSQSHandler_SkipsUndecodableMessage(t *testing.T) {
	proj := &stubProjector{name: "sqs-skip"}
	rt := newTestRuntime(proj)
	handler := projectors.NewSQSHandler(rt)

	event := events.SQSEvent{
		Records: []events.SQSMessage{
			{MessageId: "bad", Body: "not an envelope"},
			{MessageId: "good", Body: string(envelopeFor(t, sampleEvent("account-1/3", "account-1")))},
		},
	}

	resp, err := handler(context.Background(), event)
	require.NoError(t, err)
	assert.Empty(t, resp.BatchItemFailures)
	// Only the decodable record reaches the projector.
	require.Len(t, proj.lastBatch, 1)
	assert.Equal(t, "account-1", proj.lastBatch[0].EntityID)
}

func TestSNSHandler_ProcessesRecords(t *testing.T) {
	proj := &stubProjector{name: "sns-test"}
	rt := newTestRuntime(proj)
	handler := projectors.NewSNSHandler(rt)

	event := events.SNSEvent{
		Records: []events.SNSEventRecord{
			{SNS: events.SNSEntity{MessageID: "m1", Message: string(envelopeFor(t, sampleEvent("account-1/3", "account-1")))}},
		},
	}

	require.NoError(t, handler(context.Background(), event))
	assert.Equal(t, 1, proj.called)
}

func TestSNSHandler_ReturnsErrorOnFailures(t *testing.T) {
	// SNS has no partial-batch protocol, so any reported failure fails the whole
	// invocation for redrive.
	proj := &stubProjector{
		name:     "sns-fail",
		failures: []projectors.BatchItemFailure{{ItemIdentifier: "account-1/3"}},
	}
	rt := newTestRuntime(proj)
	handler := projectors.NewSNSHandler(rt)

	event := events.SNSEvent{
		Records: []events.SNSEventRecord{
			{SNS: events.SNSEntity{MessageID: "m1", Message: string(envelopeFor(t, sampleEvent("account-1/3", "account-1")))}},
		},
	}

	require.Error(t, handler(context.Background(), event))
}
