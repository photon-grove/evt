package stream

import (
	"encoding/json"
	"testing"
)

func TestNormalizeSNSEvent_SNSPassthrough(t *testing.T) {
	payload := `{
		"Records": [{
			"EventSource": "aws:sns",
			"Sns": {
				"Message": "{\"detail\": \"test-payload\"}"
			}
		}]
	}`

	result, err := NormalizeSNSEvent(json.RawMessage(payload))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(result.Records))
	}
	if result.Records[0].EventSource != "aws:sns" {
		t.Errorf("expected EventSource aws:sns, got %s", result.Records[0].EventSource)
	}
	if result.Records[0].SNS.Message != `{"detail": "test-payload"}` {
		t.Errorf("unexpected message: %s", result.Records[0].SNS.Message)
	}
}

func TestNormalizeSNSEvent_SQSRawMessageDelivery(t *testing.T) {
	payload := `{
		"Records": [{
			"messageId": "msg-1",
			"receiptHandle": "handle-1",
			"body": "{\"detail\": \"test-payload\"}",
			"eventSource": "aws:sqs",
			"eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue"
		}]
	}`

	result, err := NormalizeSNSEvent(json.RawMessage(payload))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(result.Records))
	}
	if result.Records[0].EventSource != "aws:sns" {
		t.Errorf("expected synthetic EventSource aws:sns, got %s", result.Records[0].EventSource)
	}
	if result.Records[0].SNS.Message != `{"detail": "test-payload"}` {
		t.Errorf("unexpected message: %s", result.Records[0].SNS.Message)
	}
}

func TestNormalizeSNSEvent_MultipleSQSRecords(t *testing.T) {
	payload := `{
		"Records": [
			{"messageId": "1", "body": "{\"a\":1}", "eventSource": "aws:sqs", "eventSourceARN": "arn"},
			{"messageId": "2", "body": "{\"b\":2}", "eventSource": "aws:sqs", "eventSourceARN": "arn"}
		]
	}`

	result, err := NormalizeSNSEvent(json.RawMessage(payload))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(result.Records))
	}
	if result.Records[0].SNS.Message != `{"a":1}` {
		t.Errorf("record 0 message: %s", result.Records[0].SNS.Message)
	}
	if result.Records[1].SNS.Message != `{"b":2}` {
		t.Errorf("record 1 message: %s", result.Records[1].SNS.Message)
	}
}

func TestNormalizeSNSEvent_EmptyPayload(t *testing.T) {
	_, err := NormalizeSNSEvent(nil)
	if err == nil {
		t.Fatal("expected error for empty payload")
	}
}

func TestNormalizeSNSEvent_InvalidJSON(t *testing.T) {
	_, err := NormalizeSNSEvent(json.RawMessage(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestNormalizeSNSEvent_EmptyRecords(t *testing.T) {
	_, err := NormalizeSNSEvent(json.RawMessage(`{"Records": []}`))
	if err == nil {
		t.Fatal("expected error for empty records")
	}
}
