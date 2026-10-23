# Streams and Projectors

Once events land in the [DynamoDB event log](dynamodb.md), they fan out to
asynchronous consumers (read-model projectors, search indexers, feeds, webhooks) through a
single hop:

```text
event log ──DynamoDB Stream──▶ your handler ──▶ SNS topic ──┬──▶ SNS→SQS ──▶ projector
              (NEW_IMAGE)      (one reader)                 ├──▶ SNS→SQS ──▶ search / …
                                                            └──▶ SNS→Lambda ──▶ feeds / webhooks
```

One handler reads the DynamoDB Stream and republishes each **`INSERT`** of an event to SNS.
Consumers subscribe, usually through SNS→SQS with raw delivery, and can scale and fail
independently while that handler remains the only stream reader. The library doesn't ship the SNS
publisher, so you write that handler yourself (see [Publishing onward](#publishing-onward)). The
`projectors` package provides the consumer-side Lambda runtimes, and it can also read the stream
directly with no topic hop.

### Head items are not events

Every written stream also has a **head item** at `sk = -1` (ADR 0007, [storage
format](../reference/storage-format.md#stream-heads)), updated by every commit that writes the stream. The
stream therefore carries one `INSERT` for it with a stream's first commit, one `MODIFY` per later
commit, and one record per stream from the head migration. They are concurrency metadata, not
events, and a consumer skips them. The consumers in this module do: `stream.DynamoDBHandler` drops
such `INSERT` records, and `projectors.NewLambdaHandler` skips any record whose sort key is below 1 (as it does the
snapshot at `sk = 0`). A consumer you write decodes `NEW_IMAGE`, so skip `sk < 1` the same way.

An event source mapping or EventBridge Pipe filter can drop them before your function runs. The
keys are on every record, including `KEYS_ONLY` views:

```json
{ "dynamodb": { "Keys": { "sk": { "N": [{ "anything-but": ["-1", "0"] }] } } } }
```

(`"0"` also drops the snapshot, which consumers already skip.) A `KEYS_ONLY` consumer that wakes on
every record, such as a journal reader, fires on every head update unless it filters, so give it the
filter above. Deploy the filter or the skip **before** the head migration: the backfill emits one
record per stream (see the [migration runbook](migrating-to-stream-heads.md)).

Every consumer follows the same reliability contract: process records
independently, return **partial-batch failures** so only affected records retry,
and stay **idempotent** because the same event can be delivered more than once.

## Projectors

A projector consumes events from the SNS fan-out and maintains a read model. You
implement the `Projector` interface, wrap it in a runtime, and serve it with the
SQS handler — the topic delivered over SNS→SQS with raw message delivery:

```go
type Projector interface {
    Process(ctx context.Context, records []projectors.StreamRecord) ([]projectors.BatchItemFailure, error)
    Name() string // stable identity for idempotency keys and telemetry
}

func main() {
    runtime := projectors.NewRuntime(
        myProjector,
        idempotencyGuard,   // projectors.IdempotencyGuard
        slog.Default(),
    )
    lambda.Start(projectors.NewSQSHandler(runtime))
}
```

`NewSQSHandler` decodes each SQS message — the CloudWatchEvent envelope that your own
handler emitted — into a
`StreamRecord` with `StreamRecordFromEnvelope`, runs the batch through the runtime, and returns
an `events.SQSEventResponse` so Lambda redelivers only the failed messages. For a
latency-sensitive consumer wired as a **direct** SNS→Lambda subscription, use `NewSNSHandler`
instead; SNS has no partial-batch protocol, so a failure retries the whole invocation — give
that subscription a redrive policy. (`NewLambdaHandler` reads the DynamoDB Stream directly, with no topic hop.)

The runtime adds:

- **idempotency** keyed by `(projector name, event ID)` — provide a durable
  `IdempotencyGuard` in production (a DynamoDB-backed one);
  `projectors.NewInMemoryIdempotencyGuard()` is fine for tests and single-process
  runs. The projector **name must be stable**: renaming it resets dedup history.
- **retry classification** — wrap unrecoverable errors with
  `projectors.NewPermanentError(err)` to route them to partial-batch failures
  (and ultimately a DLQ); transient errors (timeouts, context deadlines) retry the
  whole invocation.
- **partial-batch failure responses** — `NewSQSHandler` returns an
  `events.SQSEventResponse{BatchItemFailures: […]}` (and `NewLambdaHandler` a
  `DynamoDBStreamResponse{…}`) so Lambda re-delivers only the failed records.
- **structured logging** through the `*slog.Logger` you pass in (falling back to
  `slog.Default()` when nil).

## Publishing onward

The library no longer ships an SNS publisher or ingress budgets ([ADR
0006](../adr/0006-parity-policy-and-feature-cuts.md), K5), so there are two ways to feed consumers.

Projector-style consumers can read the DynamoDB Stream directly with
`projectors.NewLambdaHandler`, which skips the SNS hop entirely.

For anything else, subscribe your own Lambda or worker to the stream and publish onward from it.
`stream.DynamoDBHandler` does the filtering half of that job. It keeps only `INSERT` records and
drops head items and other non-event rows, and you hand the survivors to whatever publishes them:

```go
handler := stream.NewDynamoDBHandler(logger)

inserts, err := handler.Handle(ctx, event) // []events.DynamoDBEventRecord
if err != nil {
    return err
}
// Encode each record as the envelope below and publish it with your own SNS, SQS or
// EventBridge client. Apply your own backpressure here if a burst could overrun a topic.
```

To keep downstream subscribers working, emit the stream envelope: a CloudWatchEvent-shaped
`{ID, Source, DetailType, Detail}` object with the full serialized event in `Detail`. The envelope
is the cross-language wire format, so it stays. Rust encodes it with
`photon_evt::wire::envelope::encode`, and Go decodes it with `projectors.StreamRecordFromEnvelope`.
If you publish to SNS, add message attributes such as `entityType`, `eventType`, `commandId` and
`correlationId` so subscribers can filter without parsing the body.

Command outcomes ([C3/C4](../../README.md#command-outcomes-and-rejections-v09-c3c4)) make commands
idempotent and record their results, but they don't consume the stream or publish anything.

## Reliability checklist

- **Drop malformed rows deliberately and measure them.** A record that can't be
  decoded should be counted and skipped, not retried forever.
- **Return batch item failures, not whole-batch errors,** for retryable problems,
  so successful records in the same batch aren't reprocessed.
- **Keep handlers idempotent.** At-least-once delivery means every projector and
  subscriber must tolerate seeing an event twice.

For rebuilding read models offline (after a projector bug, a schema change, or a
[compaction](dynamodb.md#compaction)), use the synchronous rebuild path instead of
the stream — see [Projections and rebuilds](projections.md).
