package projectors_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-lambda-go/events"
	"github.com/photon-grove/evt/go/projectors"
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

	ctx := context.Background()
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

	ctx := context.Background()
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

// orderedProjector records every event it is handed, in order, and fails the ones named in failing
// (with failErr as a batch error when set, otherwise as a per-record failure).
type orderedProjector struct {
	failing map[string]bool
	failErr error
	seen    []string
}

func (p *orderedProjector) Name() string { return "ordered" }

func (p *orderedProjector) Process(_ context.Context, records []projectors.StreamRecord) ([]projectors.BatchItemFailure, error) {
	var failures []projectors.BatchItemFailure
	for _, record := range records {
		p.seen = append(p.seen, record.EventID)
		if !p.failing[record.EventID] {
			continue
		}
		if p.failErr != nil {
			return nil, p.failErr
		}
		failures = append(failures, projectors.BatchItemFailure{ItemIdentifier: record.EventID})
	}

	return failures, nil
}

// insertRecord is the stream record of an event INSERT, with distinct event ID and sequence number.
func insertRecord(eventID, sequenceNumber, entityID string) events.DynamoDBEventRecord {
	return events.DynamoDBEventRecord{
		EventID:   eventID,
		EventName: "INSERT",
		Change: events.DynamoDBStreamRecord{
			SequenceNumber: sequenceNumber,
			NewImage: map[string]events.DynamoDBAttributeValue{
				"pk":         events.NewStringAttribute(entityID),
				"sk":         events.NewNumberAttribute("1"),
				"type":       events.NewStringAttribute("universe:created"),
				"entityType": events.NewStringAttribute("universe"),
			},
		},
	}
}

func streamEvent(records ...events.DynamoDBEventRecord) events.DynamoDBEvent {
	return events.DynamoDBEvent{Records: records}
}

// Lambda resumes a stream from the lowest reported sequence number, so a mid-batch failure reports
// that record's SequenceNumber (not its EventID) and stops there: the records after it are not
// attempted, and their redelivery projects them after the failed one.
func TestLambdaHandler_FailureReportsSequenceNumberAndStopsTheBatch(t *testing.T) {
	proj := &orderedProjector{failing: map[string]bool{"evt-b": true}}
	handler := projectors.NewLambdaHandler(projectors.NewRuntime(proj, projectors.NewInMemoryIdempotencyGuard(), testLogger()))
	a := insertRecord("evt-a", "100", "entity-a")
	b := insertRecord("evt-b", "101", "entity-b")
	c := insertRecord("evt-c", "102", "entity-c")

	resp, err := handler(context.Background(), streamEvent(a, b, c))
	require.NoError(t, err)

	body, err := json.Marshal(resp)
	require.NoError(t, err)
	assert.JSONEq(t, `{"batchItemFailures":[{"itemIdentifier":"101"}]}`, string(body))
	assert.Equal(t, []string{"evt-a", "evt-b"}, proj.seen, "records after the failure are not attempted")

	// Lambda delivers the failed record and every later one again.
	delete(proj.failing, "evt-b")
	resp, err = handler(context.Background(), streamEvent(b, c))
	require.NoError(t, err)
	assert.Empty(t, resp.BatchItemFailures)
	assert.Equal(t, []string{"evt-a", "evt-b", "evt-b", "evt-c"}, proj.seen)
}

func TestLambdaHandler_PermanentErrorReportsThatRecordsSequenceNumber(t *testing.T) {
	proj := &orderedProjector{
		failing: map[string]bool{"evt-b": true},
		failErr: projectors.NewPermanentError(errors.New("bad payload")),
	}
	handler := projectors.NewLambdaHandler(projectors.NewRuntime(proj, projectors.NewInMemoryIdempotencyGuard(), testLogger()))

	resp, err := handler(context.Background(), streamEvent(
		insertRecord("evt-a", "100", "entity-a"),
		insertRecord("evt-b", "101", "entity-b"),
		insertRecord("evt-c", "102", "entity-c"),
	))
	require.NoError(t, err)
	assert.Equal(t, []projectors.BatchItemFailure{{ItemIdentifier: "101"}}, resp.BatchItemFailures)
	assert.Equal(t, []string{"evt-a", "evt-b"}, proj.seen)
}

// A transient error fails the whole invocation; the records that succeeded are skipped on the retry.
func TestLambdaHandler_TransientErrorFailsTheInvocation(t *testing.T) {
	proj := &orderedProjector{failing: map[string]bool{"evt-b": true}, failErr: errors.New("timeout")}
	handler := projectors.NewLambdaHandler(projectors.NewRuntime(proj, projectors.NewInMemoryIdempotencyGuard(), testLogger()))
	batch := streamEvent(
		insertRecord("evt-a", "100", "entity-a"),
		insertRecord("evt-b", "101", "entity-b"),
		insertRecord("evt-c", "102", "entity-c"),
	)

	_, err := handler(context.Background(), batch)
	require.Error(t, err)
	assert.Equal(t, []string{"evt-a", "evt-b"}, proj.seen)

	proj.failing = nil
	resp, err := handler(context.Background(), batch)
	require.NoError(t, err)
	assert.Empty(t, resp.BatchItemFailures)
	assert.Equal(t, []string{"evt-a", "evt-b", "evt-b", "evt-c"}, proj.seen)
}

func TestLambdaHandler_SkipsRecordsMissingRequiredFields(t *testing.T) {
	proj := &stubProjector{name: "handler-test"}
	rt := newTestRuntime(proj)
	handler := projectors.NewLambdaHandler(rt)

	ctx := context.Background()
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

// A stream's first commit inserts its head item (sk = -1, ADR 0007); it is not an event, and a
// projector never sees it.
func TestLambdaHandler_SkipsHeadItems(t *testing.T) {
	proj := &stubProjector{name: "handler-test"}
	handler := projectors.NewLambdaHandler(newTestRuntime(proj))

	image := map[string]events.DynamoDBAttributeValue{
		"pk":   events.NewStringAttribute("entity-1"),
		"sk":   events.NewNumberAttribute("-1"),
		"head": events.NewNumberAttribute("1"),
		// Even an image that looks like an event is skipped by its sort key.
		"type": events.NewStringAttribute("universe:created"),
	}

	resp, err := handler(context.Background(), events.DynamoDBEvent{Records: []events.DynamoDBEventRecord{
		{EventID: "head", EventName: "INSERT", Change: events.DynamoDBStreamRecord{Keys: image, NewImage: image}},
	}})
	require.NoError(t, err)
	assert.Empty(t, resp.BatchItemFailures)
	assert.Equal(t, 0, proj.called)
}
