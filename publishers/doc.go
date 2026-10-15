// Package publishers provides the Lambda handler for DynamoDB event-log INSERT records.
// It publishes CloudWatchEvent envelopes to SNS for fan-out and can also publish eligible
// events to a FIFO companion topic.
//
// Deprecated: Cut in ADR-0006 (K5): the publisher runtime is superseded by command outcomes and
// the stream envelope (which remains the cross-language wire format). It will be removed in the
// next minor release.
package publishers
