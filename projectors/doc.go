// Package projectors provides an async projector runtime for building read
// models from committed events. It wraps individual Projector implementations
// with idempotency guards, retry classification, and structured telemetry.
//
// # Where records come from
//
// The blessed path is the SNS fan-out: a single publisher (see the publishers
// package) consumes the DynamoDB event-log stream and publishes each event to an
// SNS topic; every projector subscribes to that topic — usually over SNS->SQS
// with raw message delivery — and runs independently. Use NewSQSHandler (or
// NewSNSHandler for a direct subscription) to drive the Runtime from those
// deliveries; both decode the CloudWatchEvent envelope into a StreamRecord via
// StreamRecordFromEnvelope.
//
// Fan-out keeps the DynamoDB stream's single consumer (the publisher) cheap and
// lets projectors, change-detection heads, search indexers, feeds, and webhooks
// scale and fail independently.
//
// NewLambdaHandler is a supported alternative for wiring a projector directly to
// the DynamoDB stream, for the occasional consumer that should not go through the
// topic. Prefer the SNS/SQS handlers unless you have a specific reason not to.
package projectors
