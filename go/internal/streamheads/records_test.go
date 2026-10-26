package streamheads

import (
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

func record(sk string) events.DynamoDBEventRecord {
	keys := map[string]events.DynamoDBAttributeValue{
		"pk": events.NewStringAttribute("acct-1"),
		"sk": events.NewNumberAttribute(sk),
	}

	return events.DynamoDBEventRecord{EventName: "INSERT", Change: events.DynamoDBStreamRecord{Keys: keys, NewImage: keys}}
}

func TestIsNonEventRecord(t *testing.T) {
	tests := map[string]struct {
		record events.DynamoDBEventRecord
		want   bool
	}{
		"a head item":     {record("-1"), true},
		"a snapshot":      {record("0"), true},
		"the first event": {record("1"), false},
		"a later event":   {record("42"), false},
		"keys only": {events.DynamoDBEventRecord{Change: events.DynamoDBStreamRecord{
			Keys: map[string]events.DynamoDBAttributeValue{"sk": events.NewNumberAttribute("-1")},
		}}, true},
		"new image only": {events.DynamoDBEventRecord{Change: events.DynamoDBStreamRecord{
			NewImage: map[string]events.DynamoDBAttributeValue{"sk": events.NewNumberAttribute("-1")},
		}}, true},
		"no sort key":      {events.DynamoDBEventRecord{EventName: "INSERT"}, false},
		"a non-number key": {events.DynamoDBEventRecord{Change: events.DynamoDBStreamRecord{Keys: map[string]events.DynamoDBAttributeValue{"sk": events.NewStringAttribute("-1")}}}, false},
		"an unparsable":    {record("NaN"), false},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if got := IsNonEventRecord(test.record); got != test.want {
				t.Fatalf("IsNonEventRecord() = %v, want %v", got, test.want)
			}
		})
	}
}
