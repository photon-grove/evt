# @photon-grove/evt-lambda

The consumer side of an [`@photon-grove/evt`](https://github.com/photon-grove/evt/blob/main/packages/evt/README.md) event log on AWS Lambda. It reads
the DynamoDB Streams records an event-log table (or its views table) produces, writes the event
source mapping filter patterns that keep unrelated records from invoking your function, and builds
the partial batch failure response Lambda resumes from. It's the TypeScript port of the Rust crate
[`photon-evt-lambda`](https://github.com/photon-grove/evt/blob/main/crates/evt-lambda/README.md), held to the same Rust-generated fixture,
`compat/fixtures/lambda.json`, so both write the same filter patterns byte for byte and decode,
skip, or refuse the same records. The design is in
[ADR 0005](https://github.com/photon-grove/evt/blob/main/docs/adr/0005-rust-first-extensions-journal-and-lambda.md).

The package is a private workspace package for now: it isn't published to npm yet.

It has no runtime dependencies beyond `@photon-grove/evt` (a peer dependency), and no AWS SDK or
Lambda runtime dependency. `StreamEvent` is the payload Lambda delivers, typed structurally, so the
parsed JSON passes in as it is, and so does a `DynamoDBStreamEvent` typed with `@types/aws-lambda`.
`BatchResponse` is the response Lambda expects. Item images are DynamoDB JSON, decoded with the
same `@photon-grove/evt/dynamodb` codecs the repository uses. Like `@photon-grove/evt`, it runs on
Node 22.12+, Bun, Deno, and edge runtimes.

The pieces are small. `decodeEvent` reads the committed events from an event-log table's stream
(`NEW_IMAGE`) and skips the engine's own items: stream heads (`sk = -1`), snapshots (`sk = 0`), the
feature marker and rebuild fence (`SYSTEM#…`), and command outcomes (`command-outcome#…`).
`EventFilter` and `JournalFilter` write filter patterns by event type, entity type or journal scope,
and `matches` applies the same test in the handler. `decodeWakeup` reads a views-table stream record
(`KEYS_ONLY` is enough) and returns the journal scope to read. And `processEvents`,
`processWakeups` and `processRecords` run a batch in order, stop at the first failure, and return
the `batchItemFailures` response.

## Projectors and fan-out

Turn on the event-log table's stream with `NEW_IMAGE` (or `NEW_AND_OLD_IMAGES`). Then map it to the
function with a filter, partial batch failures, and bounded retries:

```ts
import {EventFilter, type StreamEvent, processEvents} from '@photon-grove/evt-lambda'

const filter = new EventFilter().entityType('account').eventTypePrefix('account:')

// Deploy this text as the mapping's filter: FilterCriteria.Filters[].Pattern.
const pattern = filter.pattern()

export async function handler(event: StreamEvent) {
  const outcome = await processEvents(event, filter, async (streamed) => {
    // Make this idempotent, keyed on streamed.idempotencyKey ("acct-1:3").
    await project(streamed.event)
  })

  if (outcome.failure !== undefined) {
    console.error(`stopped at ${outcome.failure.sequenceNumber}`, outcome.failure.error)
  }

  // Return this from the function.
  return outcome.response()
}
```

`processEvents` applies the filter again in the handler. A function therefore stays correct when its
mapping has no filter, or an outdated one. Upcasting is the consumer's job: run your upcasters on
`streamed.event` before you decode its payload. A filter is immutable, so one module-level filter is
safe to share across invocations.

## Batch failure semantics

These rules assume `FunctionResponseTypes: ["ReportBatchItemFailures"]` on the mapping.

Records go to the handler in stream order, one at a time, and the first record that fails stops the
batch, whether it failed to decode or the handler threw (or returned a rejected promise). The
response reports that record's `SequenceNumber`. Lambda checkpoints before the lowest reported
sequence number, treats the earlier records as done, and delivers the failed record and every
record after it again. The helpers don't attempt the later records, so they come back behind the
failed one and per-item order holds.

So handlers must be idempotent. The failed record and anything after it can arrive more than once,
and a handler that failed halfway may already have run some of its side effects. Key event handlers
on the event ID (`StreamedEvent.idempotencyKey`), and key projectors on the last sequence they
applied per stream.

A record that doesn't decode is not skipped. It fails the batch like a handler error, with a
`DecodeError` whose `kind` is `missingNewImage` (an event insert on a `KEYS_ONLY` or `OLD_IMAGE`
stream) or `invalidRecord` (an event item, or its DynamoDB JSON, that doesn't decode). So set
`MaximumRetryAttempts` (or `MaximumRecordAgeInSeconds`) and an on-failure destination on the
mapping, or a poison record stops its shard until it ages out of the stream. For a stream source the
on-failure destination receives the batch's shard and sequence-number range, not the records, so
replaying from it means reading the stream or the table again. An empty `itemIdentifier` makes
Lambda fail the whole batch, and that's the response for a record with no sequence number. Without
`ReportBatchItemFailures` Lambda ignores the response, and only a thrown error fails (and retries)
the batch. A `ParallelizationFactor` above 1 keeps order per partition key, which is per stream for
event items.

## Journal wake-ups

The sync journal ([storage format](https://github.com/photon-grove/evt/blob/main/docs/reference/storage-format.md#journal)) writes its rows (`journal#<scope>`, 20-digit position)
and counters (`journal-counter#<scope>`, `COUNTER`) as view rows, in the views table, in the same
transaction as the events, whether a Rust or a TypeScript writer (`@photon-grove/evt/journal`)
committed them. A consumer wakes on those writes in three steps:

1. Turn on the views table's stream. `KEYS_ONLY` is enough: wake-ups decode from the keys, so any
   view type works.
2. Map the stream to the function with a `JournalFilter` pattern, so only journal writes invoke it.
3. Read each woken scope from its own cursor until it is caught up, then stores the cursor.

```ts
import {JournalFilter, type StreamEvent, processWakeups} from '@photon-grove/evt-lambda'

// One wake-up per scope per commit: the commit puts each touched scope's counter once.
const counters = JournalFilter.counters().scopePrefix('world:')
// {"eventName":["INSERT","MODIFY"],"dynamodb":{"Keys":{"pk":{"S":[{"prefix":"journal-counter#world:"}]}}}}
counters.pattern()

// One wake-up per row, carrying the row's position.
const rows = JournalFilter.rows().scope('world:7')

export async function handler(event: StreamEvent) {
  const outcome = await processWakeups(event, counters, async (wakeup) => {
    // wakeup.position is a bigint, or undefined for a KEYS_ONLY counter record.
    await catchUp(wakeup.scope)
  })

  return outcome.response()
}
```

A stream record is a wake-up, not a delivery. DynamoDB Streams orders records per item, not per
scope, and a batch can hold several records of one scope. `processWakeups` merges them into one call
per scope, with the highest position seen. A redelivered or merged wake-up only costs a read that
finds nothing new, so journal consumers need no other idempotency. Removals (a compaction deleting
rows) are not wake-ups, and neither are stray keys in the journal's partitions: `decodeWakeup` never
throws, so no views-table record can stop a shard, and a corrupt journal shows up where it is read.

A wake-up can be lost. A failed scope blocks the scopes behind it in its shard until it succeeds,
and when the mapping's retries run out Lambda moves past the batch, so the scopes it named are read
only on their next write. Size the mapping's retry budget for your longest rebuild, or leave it
unbounded (`-1`), and a consumer that can't fall behind a quiet scope should also sweep its scopes
from their cursors on a schedule. The
[crate README](https://github.com/photon-grove/evt/blob/main/crates/evt-lambda/README.md#journal-wake-ups) has the full reasoning, why the
wake-ups read the views table's stream, and a Terraform sketch of the event source mapping.

## Differences from the Rust crate

- The Rust crate deserializes the whole payload before the handler runs, so malformed DynamoDB JSON
  anywhere in it fails the invocation. This package reads the payload as it is and checks only what
  a decoder reads: malformed JSON in an event image is that record's `invalidRecord`, and an unknown
  `eventName` or `StreamViewType` reads as `UNKNOWN`, as in Rust.
- `JournalFilter.scope` takes the scope as a string and throws a `RangeError` for one that isn't
  1 to 512 bytes of UTF-8 without U+0000, the rule Rust's `Scope::new` enforces.
- Journal positions are `bigint`, and handlers fail by throwing instead of returning an error.
- A counter payload that repeats a field the counter doesn't have (which the journal never writes)
  wakes its scope with no position, where Rust ignores the repeated field and reads the position.

## Development

```sh
moon run evt-lambda:check evt-lambda:test evt-lambda:runtimes
```

`evt-lambda:check` type-checks `src/` with no runtime types, runs oxlint, and rejects any import but
`@photon-grove/evt`. `evt-lambda:test` runs the Vitest tests and every `lambda.json` vector through
the built package on Node, and `evt-lambda:runtimes` runs the vectors on Bun and Deno. The vector
runner is `packages/evt-compat/src/lambda.ts`.

## License

Apache-2.0.
