package stream

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

func record(name, sk string) events.DynamoDBEventRecord {
	keys := map[string]events.DynamoDBAttributeValue{
		"pk": events.NewStringAttribute("acct-1"),
		"sk": events.NewNumberAttribute(sk),
	}

	return events.DynamoDBEventRecord{
		EventID:   name,
		EventName: "INSERT",
		Change:    events.DynamoDBStreamRecord{Keys: keys, NewImage: keys},
	}
}

// The first commit of a stream inserts its head item; the handler passes events and drops it.
func TestDynamoDBHandler_SkipsHeadItems(t *testing.T) {
	got, err := NewDynamoDBHandler().Handle(context.Background(), events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{record("head", "-1"), record("event-1", "1"), record("snapshot", "0")},
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if len(got) != 1 || got[0].EventID != "event-1" {
		t.Fatalf("expected only the event record, got %+v", got)
	}
}

// The SNS publisher counts a head item as skipped, not as a malformed event to drop.
func TestSNSPublisher_SkipsHeadItems(t *testing.T) {
	publisher, err := NewSNSPublisher(&mockSNSPublishClient{}, "arn:aws:sns:us-west-2:000000000000:test", "test") //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
	if err != nil {
		t.Fatal(err)
	}

	result, err := publisher.Publish(context.Background(), []events.DynamoDBEventRecord{record("head", "-1")})
	if err != nil {
		t.Fatal(err)
	}

	if result.SkippedCount != 1 || result.DroppedMalformedCount != 0 || result.PublishedCount != 0 || len(result.FailedIndices) != 0 {
		t.Fatalf("expected one skipped record, got %+v", result)
	}
}
