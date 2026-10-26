package streamheads

import (
	"strconv"

	"github.com/aws/aws-lambda-go/events"
)

// IsNonEventRecord reports whether a DynamoDB Streams record belongs to a row that is not an event:
// the inline snapshot (sk = 0) or a per-stream head item (sk = -1, ADR 0007). Every written stream
// produces a head-item record per commit, and the head migration produces one per stream, so a
// consumer of event records skips them. The sort key is read from the record's keys, which every
// stream view type carries, and from the new image as a fallback. A record whose sort key is
// absent or not a number is left to the caller to judge.
func IsNonEventRecord(record events.DynamoDBEventRecord) bool {
	for _, attributes := range []map[string]events.DynamoDBAttributeValue{record.Change.Keys, record.Change.NewImage} {
		sk, ok := attributes["sk"]
		if !ok || sk.DataType() != events.DataTypeNumber {
			continue
		}

		parsed, err := strconv.ParseInt(sk.Number(), 10, 64)
		if err != nil {
			return false
		}

		return parsed < 1
	}

	return false
}
