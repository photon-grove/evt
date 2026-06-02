// Package projectors provides an async projector runtime for processing
// DynamoDB Streams events. It wraps individual Projector implementations with
// idempotency guards, retry classification, and structured telemetry.
package projectors
