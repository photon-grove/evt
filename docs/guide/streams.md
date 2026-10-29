# Streams and projectors

Once events land in the [DynamoDB event log](dynamodb.md), they reach asynchronous consumers
(read-model projectors, search indexers, feeds, webhooks) through the table's DynamoDB Stream. The
usual shape is a single hop:

```text
event log ──DynamoDB Stream──▶ your handler ──▶ SNS topic ──┬──▶ SNS→SQS ──▶ projector
              (NEW_IMAGE)      (one reader)                 ├──▶ SNS→SQS ──▶ search / …
                                                            └──▶ SNS→Lambda ──▶ feeds / webhooks
```

One handler reads the stream and republishes each `INSERT` of an event to SNS. Consumers
subscribe, usually through SNS→SQS with raw delivery, so they can scale and fail independently
while that handler stays the only stream reader. The library doesn't ship the SNS publisher, so you
write that handler yourself (see [Publishing onward](#publishing-onward)). The `projectors` package
provides the consumer-side Lambda runtimes, and it can also read the stream directly with no topic
in between.

Every consumer follows the same reliability contract. It returns partial-batch failures so retries
skip the records that already succeeded, and stays idempotent, because the same event can be
delivered more than once.

### Head items are not events

Every written stream also has a head item at `sk = -1`, updated by every commit that writes the
stream ([ADR 0007](../adr/0007-per-stream-head-item.md),
[storage format](../reference/storage-format.md#stream-heads)). So the DynamoDB Stream carries one
`INSERT` for it with a stream's first commit, one `MODIFY` per later commit, and one record per
stream from the head migration. These records are concurrency metadata, not events, and consumers
skip them. The consumers in this module already do. `stream.DynamoDBHandler` drops those `INSERT`
records, and `projectors.NewLambdaHandler` skips any record whose sort key is below 1, just as it
skips the snapshot at `sk = 0`. A consumer you write decodes `NEW_IMAGE`, so skip `sk < 1` the same
way.

You can also drop them before your function runs, with an event source mapping or EventBridge Pipe
filter. The keys are on every record, including `KEYS_ONLY` views:

```json
{ "dynamodb": { "Keys": { "sk": { "N": [{ "anything-but": ["-1", "0"] }] } } } }
```

The `"0"` drops the snapshot too, which consumers skip anyway. A `KEYS_ONLY` consumer that wakes on
every record, such as a journal reader, fires on every head update unless it filters, so give it
this filter. And deploy the filter or the skip before the head migration, because the backfill
emits one record per stream (see the [migration runbook](migrating-to-stream-heads.md)).

## Projectors

A projector consumes events from the SNS fan-out and maintains a read model. You implement the
`Projector` interface, wrap it in a runtime, and serve it with the SQS handler, with the topic
delivered over SNS→SQS using raw message delivery:

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

`NewSQSHandler` decodes each SQS message, which is the CloudWatchEvent envelope your own handler
emitted, into a `StreamRecord` with `StreamRecordFromEnvelope`. It runs the batch through the
runtime and returns an `events.SQSEventResponse`, so Lambda redelivers only the failed messages.
For a latency-sensitive consumer wired as a direct SNS→Lambda subscription, use `NewSNSHandler`
instead. SNS has no partial-batch protocol, so a failure retries the whole invocation, and that
subscription needs a redrive policy. `NewLambdaHandler` reads the DynamoDB Stream directly, with no
topic at all.

The runtime takes care of four things around your `Process`:

- Idempotency, keyed by `(projector name, event ID)`. Provide a durable `IdempotencyGuard` in
  production, such as a DynamoDB-backed one. `projectors.NewInMemoryIdempotencyGuard()` is fine for
  tests and single-process runs. The projector name must be stable, because renaming it resets the
  dedup history.
- Retry classification. Wrap unrecoverable errors with `projectors.NewPermanentError(err)` to route
  them to partial-batch failures and, eventually, a DLQ. Transient errors (timeouts, context
  deadlines) retry the whole invocation.
- Partial-batch failure responses. `NewSQSHandler` returns an
  `events.SQSEventResponse{BatchItemFailures: […]}` that names the failed messages' IDs, so Lambda
  redelivers only those messages. `NewLambdaHandler` returns a `DynamoDBStreamResponse{…}` that
  names a stream record's `SequenceNumber` (see [Reading the stream directly](#reading-the-stream-directly)).
- Structured logging through the `*slog.Logger` you pass in, falling back to `slog.Default()` when
  it's nil.

### Reading the stream directly

`NewLambdaHandler` needs `FunctionResponseTypes: ["ReportBatchItemFailures"]` on the event source
mapping. It hands records to the runtime one at a time, in stream order, so each `Process` call gets
a single record, and the first record that fails stops the batch. The response reports that record's `SequenceNumber`, the identifier Lambda
expects for a stream:

```json
{ "batchItemFailures": [{ "itemIdentifier": "4421584500000000017450439091" }] }
```

Lambda checkpoints before the lowest reported sequence number: it treats the earlier records as
done and delivers the failed record and every record after it again. The handler doesn't attempt
the later records, so they're projected after the failed one and per-item order holds. Your
`Process` still reports a failure by the record's `EventID`, and the handler translates it. A
transient error fails the whole invocation instead, and on the retry the idempotency guard skips
the records that already succeeded.

An `itemIdentifier` Lambda doesn't recognize, including an empty one, fails the whole batch, and
without `ReportBatchItemFailures` Lambda ignores the response. Bound the retries on the mapping
(`MaximumRetryAttempts` or `MaximumRecordAgeInSeconds`) and give it an on-failure destination, or
one poison record stops its shard until it leaves the stream.

## Publishing onward

The library no longer ships an SNS publisher or ingress budgets
([ADR 0006](../adr/0006-parity-policy-and-feature-cuts.md)), so there are two ways to feed
consumers. Projector-style consumers can read the stream directly with
`projectors.NewLambdaHandler` and skip SNS entirely. For anything else, subscribe your own Lambda
or worker to the stream and publish onward from it.

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

To keep downstream subscribers working, emit the stream envelope. It's a CloudWatchEvent-shaped
`{ID, Source, DetailType, Detail}` object with the full serialized event in `Detail`, and it's the
cross-language wire format, which is why it stayed when the publisher went. Rust encodes it with
`photon_evt::wire::envelope::encode`, and Go decodes it with `projectors.StreamRecordFromEnvelope`.
If you publish to SNS, add message attributes such as `entityType`, `eventType`, `commandId` and
`correlationId` so subscribers can filter without parsing the body.

[Command outcomes](commands.md) make commands idempotent and record their results, but they don't
consume the stream or publish anything.

## Keeping consumers reliable

A record that can't be decoded should be counted and skipped deliberately, not retried forever. For
retryable problems, return batch item failures rather than a whole-batch error, so the successful
records in the same batch aren't processed again. And keep handlers idempotent. Delivery is
at-least-once, so every projector and subscriber has to tolerate seeing an event twice.

To rebuild read models offline, after a projector bug, a schema change or a
[compaction](dynamodb.md#compaction), use the synchronous rebuild path instead of the stream. That's
covered in [Projections and rebuilds](projections.md).
