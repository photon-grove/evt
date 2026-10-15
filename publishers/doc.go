// Package publishers provides the Lambda handler for DynamoDB event-log INSERT records.
// It publishes CloudWatchEvent envelopes to SNS for fan-out and can also publish eligible
// events to a FIFO companion topic.
//
// Deprecated: Cut in ADR-0006 (K5): removed without an in-library replacement. Consume the
// DynamoDB Stream with your own handler and publish onward yourself (projector-style consumers
// can use projectors.NewLambdaHandler directly); command outcomes are planned, not shipped.
// The envelope format stays as the cross-language wire format. It will be removed in the next
// minor release.
package publishers
