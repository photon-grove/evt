// Package publishers provides the Lambda helper for the single consumer of the
// DynamoDB event-log stream: it reads INSERT records and publishes each event to
// an SNS topic for fan-out.
//
// This is the front of the blessed delivery path. One publisher Lambda is
// triggered by the event-log stream; HandleDynamoDBEvent wraps each event in a
// CloudWatchEvent envelope (via the SNS publisher in the stream package) and
// publishes it to the events topic, optionally with a FIFO companion for ordered
// consumers. Downstream projectors and other consumers subscribe to that topic
// (see the projectors package) rather than reading the stream themselves, so the
// stream keeps a single cheap reader and consumers scale independently.
package publishers
