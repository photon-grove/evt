# Streams, Projectors, and Publishers

`evt/projectors` and `evt/publishers` provide Lambda-oriented helpers for
DynamoDB Streams.

## Projectors

The projector runtime adds:

- idempotency checks by projector name and event ID
- retry classification
- partial-batch failure responses
- structured logging with caller-provided `*slog.Logger` values

## Publishers

The publisher handler accepts event-log `INSERT` records and sends serialized
domain events downstream. The included SNS publisher wraps events in a
CloudWatchEvent-shaped envelope and can publish to an optional FIFO companion
topic for ordered consumers.

Malformed rows should be dropped deliberately and measured; retryable failures
should return batch item failures so Lambda retries only the affected records.
