# Streams, Projectors, and Publishers

Once events land in the [DynamoDB event log](dynamodb.md), a DynamoDB Stream
(configured with `NEW_IMAGE`) can drive asynchronous work: updating read models
out of band, or fanning events out to other systems. The `projectors` and
`publishers` packages are Lambda-oriented runtimes for exactly that, and both
process **only `INSERT` records** — the append of a new event — skipping `MODIFY`
and `REMOVE` silently.

Both follow the same reliability contract: process records independently, return
**partial-batch failures** so Lambda retries only the affected records, and stay
**idempotent** because the same record can be delivered more than once.

## Projectors

`evt/projectors` runs an idempotent, retry-classified handler over a stream batch.
You implement the `Projector` interface and wrap it in a runtime:

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
    lambda.Start(projectors.NewLambdaHandler(runtime))
}
```

The runtime adds:

- **idempotency** keyed by `(projector name, event ID)` — provide a durable
  `IdempotencyGuard` in production (a DynamoDB-backed one);
  `projectors.NewInMemoryIdempotencyGuard()` is fine for tests and single-process
  runs. The projector **name must be stable**: renaming it resets dedup history.
- **retry classification** — wrap unrecoverable errors with
  `projectors.NewPermanentError(err)` to route them to partial-batch failures
  (and ultimately a DLQ); transient errors (timeouts, context deadlines) retry the
  whole invocation.
- **partial-batch failure responses** — the handler returns a
  `DynamoDBStreamResponse{BatchItemFailures: […]}` so Lambda re-delivers only the
  failed records.
- **structured logging** through the `*slog.Logger` you pass in (falling back to
  `slog.Default()` when nil).

## Publishers

`evt/publishers` forwards committed events to downstream consumers. The handler
takes a `StreamPublisher` and a `BudgetController`:

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
  decoded should be counted and skipped, not retried forever — the publisher
  reports `DroppedMalformedCount` for this.
- **Return batch item failures, not whole-batch errors,** for retryable problems,
  so successful records in the same batch aren't reprocessed.
- **Keep handlers idempotent.** At-least-once delivery means every projector and
  subscriber must tolerate seeing an event twice.

For rebuilding read models offline (after a projector bug, a schema change, or a
[compaction](dynamodb.md#compaction)), use the synchronous rebuild path instead of
the stream — see [Projections and rebuilds](projections.md).
