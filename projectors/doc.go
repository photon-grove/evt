// Package projectors provides an async projector runtime for building read
// models from committed events. It wraps individual Projector implementations
// with idempotency guards, retry classification, and structured telemetry.
//
// # Where records come from
//
// Projectors commonly consume events through SNS fan-out. A publisher (see the
// publishers package) consumes the DynamoDB event-log stream and publishes each
// event to an SNS topic. Projectors subscribe to that topic, usually through
// SNS->SQS with raw message delivery. NewSQSHandler and NewSNSHandler decode the
// CloudWatchEvent envelope into a StreamRecord via StreamRecordFromEnvelope.
//
// Fan-out leaves the publisher as the DynamoDB stream's sole consumer. Projectors,
// change-detection heads, search indexers, feeds, and webhooks run independently.
//
// NewLambdaHandler supports projectors that consume the DynamoDB stream directly.
package projectors
