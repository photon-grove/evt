package stream

import (
	"context"
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

func TestNewDynamoDBHandler_DefaultLoggerCompatibility(t *testing.T) {
	handler := NewDynamoDBHandler()

	got, err := handler.Handle(context.Background(), events.DynamoDBEvent{
		Records: []events.DynamoDBEventRecord{
			{EventName: "MODIFY"},
			{EventID: "evt-1", EventName: "INSERT"},
		},
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected one INSERT record, got %d", len(got))
	}
	if got[0].EventID != "evt-1" {
		t.Fatalf("expected evt-1, got %s", got[0].EventID)
	}
}
