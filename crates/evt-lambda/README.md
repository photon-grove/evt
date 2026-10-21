# photon-evt-lambda

AWS Lambda helpers for [`photon-evt`](../evt) on DynamoDB: the consumer side of an event log
([ADR 0005](../../docs/adr/0005-rust-first-extensions-journal-and-lambda.md), X3).

- **Event decode.** `decode_event` reads the committed events from an event-log table's stream
  (`NEW_IMAGE`). The engine's own items are skipped: stream heads (`sk = -1`), snapshots (`sk = 0`),
  the feature marker and rebuild fence (`SYSTEM#…`), and command outcomes (`command-outcome#…`).
- **Filter patterns.** `EventFilter` and `JournalFilter` write event source mapping filter patterns
  by event type, entity type, or journal scope. `matches` applies the same test in the handler.
- **Journal wake-ups** (feature `journal`). `decode_wakeup` reads a views-table stream record
  (`KEYS_ONLY` is enough) and returns the journal scope to read.
- **Partial batch failures.** `process_events`, `process_wakeups` and `process_records` process a
  batch in order, stop at the first failure, and return the `batchItemFailures` response that
  Lambda resumes from.

The crate has no Lambda runtime or AWS SDK dependency. `StreamEvent` deserializes from the payload
Lambda delivers, and `BatchResponse` serializes to the response Lambda expects. You can wire both
into `lambda_runtime` or any other runtime. Item images are DynamoDB JSON, decoded with the same
`photon_evt::wire::dynamo` codecs the repositories use.

```toml
[dependencies]
# Add features = ["journal"] for journal wake-ups.
photon-evt-lambda = "0.1"
```

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

## Batch failure semantics

These rules assume `FunctionResponseTypes: ["ReportBatchItemFailures"]` on the mapping.

- Records go to the handler in stream order, one at a time. The first record that fails stops the
  batch, whether it failed to decode or the handler returned an error. The response reports that
  record's `SequenceNumber`.
- Lambda checkpoints before the lowest reported sequence number. It treats the earlier records as
  done and delivers the failed record, and every record after it, again. The helpers do not attempt
  the later records: they come back behind the failed one, so per-item order holds.
- **Handlers must be idempotent.** The failed record, and any record after it, can be delivered
  more than once. A handler that failed halfway can also have run some of its side effects already.
  Key event handlers on the event ID (`StreamedEvent::idempotency_key`). Key projectors on the last
  sequence they applied per stream.
- A record that does not decode is never skipped. It fails the batch like a handler error. Set
  `MaximumRetryAttempts` (or `MaximumRecordAgeInSeconds`) and an on-failure destination on the
  mapping. Otherwise a poison record stops its shard until it ages out of the stream. For a stream
  source, the on-failure destination receives the batch's shard and sequence-number range, not the
  records, so replaying from it means reading the stream or the table again.
- An empty `itemIdentifier` makes Lambda fail the whole batch. That is the response for a record
  with no sequence number. Without `ReportBatchItemFailures`, Lambda ignores the response, and only
  a returned error fails (and retries) the batch.
- `ParallelizationFactor` above 1 keeps order per partition key, which is per stream for event
  items.

## Journal wake-ups

The [sync journal](../evt/README.md#the-sync-journal) (X1) writes its rows (`journal#<scope>`,
20-digit position) and counters (`journal-counter#<scope>`, `COUNTER`) as view rows. They go in
the repository's **views table**, in the same transaction as the events. A consumer that wakes on
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

A stream record is a **wake-up, not a delivery**. DynamoDB Streams orders records per item, not
per scope, and a batch can hold several records of one scope. `process_wakeups` merges them into
one call per scope, with the highest position seen. The handler reads the page, and the page is
ordered and reports gaps. A redelivered or merged wake-up only costs a read that finds nothing new,
so journal consumers need no other idempotency. During a rebuild, `page_at_epoch` fails with
`RebuildUnavailable` or `RebuildEpochChanged`. Return that error: the batch fails at that scope and
retries. Removals (a compaction deleting rows) are not wake-ups, and neither are stray keys in the
journal's partitions: `decode_wakeup` never fails, so no views-table record can stop a shard, and a
corrupt journal shows up where it is read, in the page.

**A wake-up can be lost.** A failed scope blocks the scopes behind it in its shard until it
succeeds, and when the mapping's retries run out (`MaximumRetryAttempts`,
`MaximumRecordAgeInSeconds`), Lambda moves past the batch. The scopes it named are then read only
on their next write. Two things keep that from mattering:

- **Retry for longer than a rebuild.** Rebuild errors are transient, and the only other failures
  are the consumer's own. Size the retry budget (or leave it unbounded, `-1`) for the longest
  rebuild you run, rather than a handful of attempts. The cost is that a scope that keeps failing
  holds back its shard for as long: the sample below allows a day.
- **Sweep.** A consumer that must not fall behind a quiet scope also reads its scopes from their
  cursors on a schedule, the same read a wake-up triggers. Wake-ups make the consumer prompt; the
  sweep makes it complete.

### Why the views-table stream, not a separate journal table

This crate supports **one stream on the views table, filtered to the journal's keys**. Two reasons:

- **X1 already stores the rows there.** The journal writes rows and counters through the
  repository's view operations, in the commit, under the same owner rules as every other view row.
  A separate journal table would change the commit path and the storage format (a second table in
  every journaled transaction and in the rebuild lock's guarded writes). It would gain nothing for
  consumers that the filter does not already give them.
- **The filter does the separation.** The event source mapping drops non-journal records before
  invoking the function. Only journal writes run the consumer, and `JournalFilter::matches` repeats
  the test in the handler.

The costs, and how to live with them:

- The views table has one stream, and its view type is set per table. A wake-up consumer works
  with any view type. If another consumer of the same table needs images, choose `NEW_IMAGE` and
  both work.
- Every view write appears in that stream, and every mapping on it reads every shard. AWS
  recommends at most two readers per shard, so keep to two mappings on the views table and fan out
  from a function when more consumers need the wake-ups. Filtered-out records cost no
  invocations.
- The decoders do not look at the table name (`eventSourceARN`). If a dedicated journal table holds
  the rows later, the same filters and decoders work on its stream unchanged. That is the hook for
  the alternative.

### Feature `journal` is build-wide

Enabling `journal` here enables `photon-evt`'s `journal` feature. That feature is build-wide (ADR
0005): every repository in the build then accepts a table that declares the journal. A function
that only consumes events does not need the feature. A function that consumes wake-ups and also
commits events to a journal table must commit them through `JournalRepository`.

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

## Compatibility fixture

`compat/fixtures/lambda.json` pins the filter patterns, the records the decoders accept, skip, or
refuse (the event decoder only), the wake-ups they produce, and the batch responses. It is generated by this crate
(Rust-first, ADR 0005):

```sh
EVT_UPDATE_FIXTURES=1 cargo test -p photon-evt-lambda --all-features --test fixtures
```

## Minimum Rust version

1.88, like `photon-evt`. The batch helpers take async closures (`AsyncFnMut`).

## License

Apache-2.0.
