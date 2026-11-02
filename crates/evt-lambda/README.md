# photon-evt-lambda

This crate is the consumer side of a [`photon-evt`](https://crates.io/crates/photon-evt) event log on
AWS Lambda. It reads the DynamoDB Streams records an event-log table (or its views table) produces,
writes the event source mapping filter patterns that keep unrelated records from invoking your
function, and builds the partial batch failure response Lambda resumes from. The design is in
[ADR 0005](https://github.com/photon-grove/evt/blob/main/docs/adr/0005-rust-first-extensions-journal-and-lambda.md).

```toml
[dependencies]
# Add features = ["journal"] for journal wake-ups.
photon-evt-lambda = "0.2"
```

Its API hands you `photon-evt` types (decoded events, journal wake-ups), so pick the release built
against the `photon-evt` you use, and Cargo resolves both to one copy: `photon-evt-lambda` 0.2 for
`photon-evt` 0.3, and 0.1 for `photon-evt` 0.2.

There is no Lambda runtime or AWS SDK dependency. `StreamEvent` deserializes from the payload Lambda
delivers and `BatchResponse` serializes to the response it expects, so you can wire both into
`lambda_runtime` or anything else. Item images are DynamoDB JSON, decoded with the same
`photon_evt::wire::dynamo` codecs the repositories use.

The pieces are small. `decode_event` reads the committed events from an event-log table's stream
(`NEW_IMAGE`) and skips the engine's own items: stream heads (`sk = -1`), snapshots (`sk = 0`), the
feature marker and rebuild fence (`SYSTEM#…`), and command outcomes (`command-outcome#…`).
`EventFilter` and `JournalFilter` write filter patterns by event type, entity type or journal scope,
and `matches` applies the same test in the handler. With the `journal` feature, `decode_wakeup`
reads a views-table stream record (`KEYS_ONLY` is enough) and returns the journal scope to read.
And `process_events`, `process_wakeups` and `process_records` run a batch in order, stop at the
first failure, and return the `batchItemFailures` response.

The guide's [streams page](https://github.com/photon-grove/evt/blob/main/docs/guide/streams.md) covers the event-log stream from the Go side
too, and the [storage format](https://github.com/photon-grove/evt/blob/main/docs/reference/storage-format.md) pins every item these decoders
read. Releases are tagged `photon-evt-lambda-vX.Y.Z` and published to crates.io, separately from
`photon-evt`.

## Projectors and fan-out

Turn on the event-log table's stream with `NEW_IMAGE` (or `NEW_AND_OLD_IMAGES`). Then map it to
the function with a filter, partial batch failures, and bounded retries:

```rust
use photon_evt::BoxError;
use photon_evt_lambda::{EventFilter, StreamEvent, StreamedEvent, process_events};

# tokio::runtime::Runtime::new().unwrap().block_on(async {
let filter = EventFilter::new()
    .entity_type("account")
    .event_type_prefix("account:");

// Deploy this text as the mapping's filter: FilterCriteria.Filters[].Pattern.
let pattern = filter.pattern().unwrap();
assert!(pattern.starts_with(r#"{"eventName":["INSERT"]"#));

let batch: StreamEvent = serde_json::from_str(r#"{"Records":[]}"#).unwrap();
let outcome = process_events(&batch, &filter, async |streamed: StreamedEvent| {
    // Make this idempotent, keyed on streamed.idempotency_key() ("acct-1:3").
    let _ = streamed;
    Ok::<_, BoxError>(())
})
.await;

// Return this from the function.
let response = outcome.response();
assert!(response.is_success());
# });
```

`process_events` applies the filter again in the handler. A function therefore stays correct when
its mapping has no filter, or an outdated one. Upcasting is the consumer's job: run your upcasters
on `streamed.event` before you decode its payload.

[`examples/stream_handler.rs`](https://github.com/photon-grove/evt/blob/main/crates/evt-lambda/examples/stream_handler.rs)
runs a sample batch, inline as the JSON Lambda delivers it, through an idempotent projector: one
record fails, the response names it, and the redelivered records and a duplicate delivery are
applied once. Run it with `cargo run -p photon-evt-lambda --example stream_handler`.

## Batch failure semantics

These rules assume `FunctionResponseTypes: ["ReportBatchItemFailures"]` on the mapping.

Records go to the handler in stream order, one at a time, and the first record that fails stops
the batch, whether it failed to decode or the handler returned an error. The response reports that
record's `SequenceNumber`. Lambda checkpoints before the lowest reported sequence number, treats
the earlier records as done, and delivers the failed record and every record after it again. The
helpers don't attempt the later records, so they come back behind the failed one and per-item
order holds.

So handlers must be idempotent. The failed record and anything after it can arrive more than once,
and a handler that failed halfway may already have run some of its side effects. Key event
handlers on the event ID (`StreamedEvent::idempotency_key`), and key projectors on the last
sequence they applied per stream.

A record that doesn't decode is not skipped. It fails the batch like a handler error, so set
`MaximumRetryAttempts` (or `MaximumRecordAgeInSeconds`) and an on-failure destination on the
mapping, or a poison record stops its shard until it ages out of the stream. For a stream source
the on-failure destination receives the batch's shard and sequence-number range, not the records,
so replaying from it means reading the stream or the table again. An empty `itemIdentifier` makes
Lambda fail the whole batch, and that's the response for a record with no sequence number. Without
`ReportBatchItemFailures` Lambda ignores the response, and only a returned error fails (and
retries) the batch. A `ParallelizationFactor` above 1 keeps order per partition key, which is per
stream for event items.

## Journal wake-ups

The [sync journal](https://docs.rs/photon-evt/latest/photon_evt/#the-sync-journal) writes its rows (`journal#<scope>`,
20-digit position) and counters (`journal-counter#<scope>`, `COUNTER`) as view rows. They go in
the repository's views table, in the same transaction as the events. A consumer that wakes on
journal writes does three things:

1. Turns on the views table's stream. `KEYS_ONLY` is enough: wake-ups decode from the keys, so any
   view type works.
2. Maps the stream to the function with a `JournalFilter` pattern, so only journal writes invoke
   it.
3. Reads each woken scope with `photon_evt::journal::page_at_epoch` from its own cursor until it
   is caught up, then stores the cursor.

```rust
# #[cfg(feature = "journal")]
# {
use photon_evt::journal::Scope;
use photon_evt_lambda::JournalFilter;

// One wake-up per scope per commit: the commit puts each touched scope's counter once.
let counters = JournalFilter::counters().scope_prefix("world:");
assert_eq!(
    counters.pattern().unwrap(),
    r#"{"eventName":["INSERT","MODIFY"],"dynamodb":{"Keys":{"pk":{"S":[{"prefix":"journal-counter#world:"}]}}}}"#,
);

// One wake-up per row, carrying the row's position.
let rows = JournalFilter::rows().scope(&Scope::new("world:7").unwrap());
assert!(rows.pattern().is_ok());
# }
```

A stream record is a wake-up, not a delivery. DynamoDB Streams orders records per item, not
per scope, and a batch can hold several records of one scope. `process_wakeups` merges them into
one call per scope, with the highest position seen. The handler reads the page, and the page is
ordered and reports gaps. A redelivered or merged wake-up only costs a read that finds nothing new,
so journal consumers need no other idempotency. During a rebuild, `page_at_epoch` fails with
`RebuildUnavailable` or `RebuildEpochChanged`. Return that error: the batch fails at that scope and
retries. Removals (a compaction deleting rows) are not wake-ups, and neither are stray keys in the
journal's partitions: `decode_wakeup` never fails, so no views-table record can stop a shard, and a
corrupt journal shows up where it is read, in the page.

A wake-up can be lost. A failed scope blocks the scopes behind it in its shard until it
succeeds, and when the mapping's retries run out (`MaximumRetryAttempts`,
`MaximumRecordAgeInSeconds`), Lambda moves past the batch. The scopes it named are then read only
on their next write. Two things keep that from mattering:

So size the mapping's retry budget for your longest rebuild, or leave it unbounded (`-1`), since
rebuild errors are transient and the only other failures are the consumer's own. The cost is that
a scope that keeps failing holds back its shard that long, and the sample below allows a day. A
consumer that can't fall behind a quiet scope should also sweep its scopes from their cursors on a
schedule, which is the same read a wake-up triggers. The sweep catches any wake-up that was lost.

### Why the views-table stream

This crate supports one stream on the views table, filtered to the journal's keys. The journal
already stores its rows and counters there, through the repository's view operations, in the
commit, under the same owner rules as every other view row. A separate journal table would change
the commit path and the storage format (a second table in every journaled transaction and in the
rebuild lock's guarded writes), and it would give consumers nothing the filter doesn't. The event
source mapping drops non-journal records before invoking the function, and
`JournalFilter::matches` repeats the test in the handler.

There are costs. The views table has one stream, and its view type is set per table. A wake-up
consumer works with any view type, so if another consumer of the same table needs images, choose
`NEW_IMAGE` and both work. Every view write appears in that stream, and every mapping on it reads
every shard. AWS recommends at most two readers per shard, so keep to two mappings on the views
table and fan out from a function when more consumers need the wake-ups. Filtered-out records cost
no invocations. The decoders don't look at the table name (`eventSourceARN`), so the same filters
and decoders would work unchanged on a dedicated journal table's stream.

### The `journal` feature

Enabling `journal` here enables `photon-evt`'s `journal` feature, which lets the build model a
journal table (ADR 0005). Only a repository wrapped by `JournalRepository`, and its later clones,
writes to one: any other repository refuses a table that declares the journal with
`UnsupportedFeature`, on commits and snapshot writes. A function that only consumes events does not
need the feature. A function that consumes wake-ups and also commits events to a journal table
must commit them through `JournalRepository`.

## Deployment sketch (Terraform)

```hcl
resource "aws_lambda_event_source_mapping" "journal_wakeups" {
  event_source_arn                   = aws_dynamodb_table.views.stream_arn
  function_name                      = aws_lambda_function.consumer.arn
  starting_position                  = "LATEST"
  function_response_types            = ["ReportBatchItemFailures"]
  # Wake-ups retry through a rebuild; a scheduled sweep covers any that are still lost.
  maximum_retry_attempts             = -1
  maximum_record_age_in_seconds      = 86400

  filter_criteria {
    filter {
      # JournalFilter::counters().pattern()
      pattern = jsonencode({
        eventName = ["INSERT", "MODIFY"]
        dynamodb  = { Keys = { pk = { S = [{ prefix = "journal-counter#" }] } } }
      })
    }
  }

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.consumer_failures.arn
    }
  }
}
```

## Compatibility fixture and Rust version

`compat/fixtures/lambda.json` pins the filter patterns, the records the decoders accept, skip, or
refuse (the event decoder only), the wake-ups they produce, and the batch responses. It is generated by this crate
(Rust-first, per ADR 0005):

```sh
EVT_UPDATE_FIXTURES=1 cargo test -p photon-evt-lambda --all-features --test fixtures
```

The minimum Rust version is 1.88, like `photon-evt`. The batch helpers take async closures (`AsyncFnMut`).

## License

Apache-2.0.
