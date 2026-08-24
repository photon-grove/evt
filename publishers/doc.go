// Package publishers provides the Lambda handler for DynamoDB event-log INSERT records.
// It publishes CloudWatchEvent envelopes to SNS for fan-out and can also publish eligible
// events to a FIFO companion topic.
package publishers
