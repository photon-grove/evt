# Streams, Projectors, and Publishers

Once events land in the [DynamoDB event log](dynamodb.md), they fan out to
asynchronous consumers — read-model projectors, change-detection
[heads](projections.md), search indexers, feeds, webhooks — through a single hop:

```text
event log ──DynamoDB Stream──▶ publisher ──▶ SNS topic ──┬──▶ SNS→SQS ──▶ projector
              (NEW_IMAGE)      (one reader)               ├──▶ SNS→SQS ──▶ heads / search / …
                                                          └──▶ SNS→Lambda ──▶ feeds / webhooks
```

Use SNS fan-out: one publisher reads the DynamoDB Stream and republishes each
**`INSERT`** to SNS. Consumers subscribe, usually through SNS→SQS with raw delivery,
and can scale and fail independently while the publisher remains the only stream
reader. The `publishers` and `projectors` packages provide the Lambda runtimes.

Every consumer follows the same reliability contract: process records
independently, return **partial-batch failures** so only affected records retry,
and stay **idempotent** because the same event can be delivered more than once.

*SNS fan-out via `evt/publishers` is deprecated in v0.9.0*
([ADR 0006](adr/0006-parity-policy-and-feature-cuts.md), K5). The shipped, supported paths for
new consumers are reading the DynamoDB Stream **directly** with
`projectors.NewLambdaHandler` (no topic hop), or consuming the stream with your own handler and
publishing onward yourself — see [Publishers](#publishers) for the full migration notes. The
consumer-side guidance below (projectors, idempotency, retry classification) is unaffected.

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

`NewSQSHandler` decodes each SQS message — the CloudWatchEvent envelope the
(deprecated) publisher emitted, or that your own handler emitted — into a
`StreamRecord` with `StreamRecordFromEnvelope`, runs the batch through the runtime, and returns
an `events.SQSEventResponse` so Lambda redelivers only the failed messages. For a
latency-sensitive consumer wired as a **direct** SNS→Lambda subscription, use `NewSNSHandler`
instead; SNS has no partial-batch protocol, so a failure retries the whole invocation — give
that subscription a redrive policy. (`NewLambdaHandler` reads the DynamoDB Stream directly —
the shipped path now that the publisher is deprecated.)

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

## Publishers

*Deprecated in v0.9.0 ([ADR 0006](adr/0006-parity-policy-and-feature-cuts.md), K5):*
`evt/publishers`, `BudgetController`, and the SNS/FIFO publisher are removed in the next minor
release **without an in-library replacement**. Command outcomes ship in v0.9.0
([C3/C4](../README.md#command-outcomes-and-rejections-v09-c3c4)), but they make commands
idempotent and record their results; they are not a publisher replacement. They do not consume
the stream or publish anything, and the envelope is only the wire format — it neither consumes
the stream nor publishes to SNS. What to do today:

- **Projector-style consumers** — switch to the direct-from-stream handler
  (`projectors.NewLambdaHandler`), which is shipped and skips the SNS hop entirely.
- **Anything else** — consume the DynamoDB Stream with your own handler (a stream-targeted
  Lambda or a worker) and publish onward yourself. To keep the existing envelope shape for
  downstream subscribers, emit the documented CloudWatchEvent-style envelope — the format stays
  as the cross-language wire format (Rust encodes it with `photon_evt::wire::envelope::encode`;
  Go decodes it with `projectors.StreamRecordFromEnvelope`).
- **Ingress/retry budgets** — implement backpressure around your own handler; nothing ships.

The rest of this section describes the pre-v0.9 runtime for readers still on it.

`evt/publishers` is the single DynamoDB-Stream consumer: it reads committed
events and republishes them to the SNS topic the consumers above subscribe to.
The handler takes a `StreamPublisher` and a `BudgetController`:

```go
func HandleDynamoDBEvent(
    ctx context.Context,
    event events.DynamoDBEvent,
    publisher StreamPublisher,
    budget BudgetController,
    loggers ...*slog.Logger,
) (events.DynamoDBEventResponse, error)
```

The included **SNS publisher** (from the `stream` package) wraps each event in a
CloudWatchEvent-shaped envelope — `{ID, Source, DetailType, Detail}`, with the full
serialized event in `Detail` — and adds SNS message attributes (`entityType`,
`eventType`, `commandId`, `correlationId`) so subscribers can filter without
parsing the body:

```go
publisher, err := stream.NewSNSPublisher(snsClient, topicARN, "account-service",
    // Optional: also publish to a FIFO companion topic for ordered consumers,
    // deduplicated by event ID, grouped by whatever key you extract.
    stream.WithFIFOTarget(fifoTopicARN, groupIDForEvent),
)
```

A `BudgetController` (`publishers.NewBudgetController(eventsPerMinute,
retriesPerMinute, now)`) is a soft per-minute rate limit on ingress and retries,
so a flood of events can't overrun downstream topics. `publishers.LoadConfigFromEnv`
reads the topic ARNs and budgets from the environment for a typical Lambda
deployment.

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
