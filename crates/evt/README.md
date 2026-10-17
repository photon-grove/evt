# photon-evt

Event sourcing for Rust with DynamoDB and PostgreSQL event logs that are wire-compatible with the
Go [`evt`](https://github.com/photon-grove/evt) framework.

Events written by this crate and by Go's `evt` are interchangeable: the same DynamoDB items,
PostgreSQL rows, serialized JSON, and stream envelopes, byte for byte. Services in either language
can append to and load the same streams. The
[storage format](https://github.com/photon-grove/evt/blob/main/docs/spec/storage-format.md) is
documented, and shared fixtures plus a live cross-language test run enforce it.

- Define events with `domain_events!` and state with the `Aggregate` trait.
- A `Store` loads aggregates from snapshots, handles commands, and commits events with optimistic
  concurrency and duplicate-command detection.
- Backends: `MemRepository` for tests, DynamoDB (feature `dynamodb`), and PostgreSQL (feature
  `postgres`).
- `testing` feature: given/when/then `Scenario` and multi-stream `Fixture` over `MemRepository`
  (see [Testing your domain](#testing-your-domain)).
- Upcasters migrate old payload versions on read. `compact_below` deletes only events a snapshot
  covers.
- In-band projectors write views in the same transaction as the events.
- `latest(n)` reads a stream's last `n` events newest-first. The `SYSTEM#` and `command-outcome#`
  namespaces are reserved for engine rows: writes to them are refused, entity heads skip them,
  and a `SYSTEM#FEATURES` marker row declares the extensions a table uses — a writer that does
  not support a declared feature refuses to write, including maintenance snapshots. Declaration
  invalidates the writer's cache; it never grants feature support. Durable history-dependent
  declarations fence each compaction batch under a marker check (DynamoDB) or all deletes under
  one transaction lock (PostgreSQL). DynamoDB has no whole-operation lease: a declaration racing
  multi-batch compaction can leave a partially compacted stream, whose history-dependent rebuild
  reports `CompactionUncovered`. Declare history dependence before writing data of that type.
- `EventLog` walks the whole log lazily as a `futures_core::Stream`: `stream_all_events` yields
  batches of events and `stream_entities` yields folded aggregates, both optionally filtered by
  entity type (see [Enumerating the log](#enumerating-the-log)).
- `Store::transaction` loads several aggregates, appends decided events to any of them, and
  commits once: all streams land or none does. Reads that were not written are fenced at the head
  they saw, and every event of the commit carries one `commitId` (the command ID when present,
  else a generated ULID). The shared 100-item / 3.8 MB / 380 KB budget fails an oversized commit
  with `CommitTooLarge` before any write, identically across backends and languages.
- `Store::with_outcomes` records every command as a durable outcome in the same transaction as
  its events, so a retried command returns the stored answer instead of running twice
  ([command outcomes](#command-outcomes)).

## Example

```rust
use photon_evt::{Aggregate, MemRepository, Metadata, RetryPolicy, Store, domain_events};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Deposited {
    pub amount_cents: i64,
}

domain_events! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum AccountEvent {
        Deposited(Deposited) => ("account:deposited", 1),
    }
}

#[derive(Clone)]
pub enum AccountCommand {
    Deposit { amount_cents: i64 },
}

#[derive(Debug, thiserror::Error)]
#[error("amount must be positive")]
pub struct InvalidAmount;

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Account {
    pub balance_cents: i64,
}

impl Aggregate for Account {
    const TYPE: &'static str = "account";

    type Command = AccountCommand;
    type Event = AccountEvent;
    type Error = InvalidAmount;
    type Services = ();

    async fn handle(&self, command: AccountCommand, _: &()) -> Result<Vec<AccountEvent>, InvalidAmount> {
        match command {
            AccountCommand::Deposit { amount_cents } if amount_cents > 0 => {
                Ok(vec![Deposited { amount_cents }.into()])
            }
            AccountCommand::Deposit { .. } => Err(InvalidAmount),
        }
    }

    fn apply(&mut self, event: AccountEvent) {
        match event {
            AccountEvent::Deposited(e) => self.balance_cents += e.amount_cents,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::<Account, _>::new(MemRepository::new(), ());

    let metadata = Metadata::new("us-west-2").with_command_id("cmd-1");
    let deposit = AccountCommand::Deposit { amount_cents: 250 };
    let executed = store.execute("acct-1", deposit, metadata.clone()).await?;
    assert_eq!(executed.state.aggregate().balance_cents, 250);

    // The same command ID is rejected as a duplicate.
    let again = AccountCommand::Deposit { amount_cents: 250 };
    let duplicate = store.execute("acct-1", again, metadata).await;
    assert!(duplicate.is_err_and(|err| err.is_duplicate_command()));

    // Conflicts and transient backend failures can be retried with backoff.
    let policy = RetryPolicy::default();
    let deposit = AccountCommand::Deposit { amount_cents: 100 };
    let metadata = Metadata::new("us-west-2").with_command_id("cmd-2");
    let executed = store
        .execute_with_retry("acct-1", deposit, metadata, &policy, tokio::time::sleep)
        .await?;
    assert_eq!(executed.state.aggregate().balance_cents, 350);

    Ok(())
}
```

## Command outcomes

A store built with `with_outcomes` writes one `command-outcome#<commandId>` row per command,
once and in its final state, inside the commit transaction. Every command needs a command ID
(at most 512 UTF-8 bytes) and a type that implements `OutcomeCommand`, and `Aggregate::Error`
implements `ToRejection`:

- A repeat of a command (same ID, same identity digest) returns the stored outcome and does not
  run the handler or write. The dedupe is global, so it survives snapshots and compaction. The
  digest is a SHA-256 over the command's type, declared streams, canonical payload JSON, and
  principal, and `OutcomeConfig::with_digester` replaces it. The same ID with another digest is
  `Error::IdempotencyKeyReuse`, carrying the stored `prior_outcome`; it is never stored.
- An error from `handle` that `ToRejection` maps to `Some` is a domain rejection: it is persisted
  as a `rejected` outcome (`Rejection { code, message, details }`) and a retried command replays it
  verbatim. A `None` is an internal failure the handler could not decide: it is returned as
  `Error::Handler` and records nothing, like every other internal failure, so a retry after the fix
  starts clean.
- A command that decides no events is an accepted no-op, and a response (at most 65536 bytes) is
  stored with the outcome. See `Store::execute_with_response`.
- A commit whose result is unknown (`Error::Indeterminate`: a timeout, a dropped connection, a server fault; a definitive refusal such as a validation or authorization error stays `Error::Backend` and is not recovered) is settled by polling the outcome row for the recovery window
  (`OutcomeConfig::with_recovery_wait`, 10 s), never by running the handler again: the stored outcome
  comes back with `replayed` set, or a row that never appears is the retryable `Error::AmbiguousWrite`
  (never "not applied": a transaction can land after any single read).
- **Backend authors:** a commit that carries an outcome and whose result is genuinely unknown
  (timeout, lost connection or response, server fault, a dropped `COMMIT`) must fail with
  `Error::Indeterminate`, the only error the store recovers by re-reading the outcome row; a
  definitive failure (validation, missing table, authorization, a constraint error) must stay
  `Error::Backend`. `Repository::backoff` is the timer recovery polling sleeps on (override it if your
  runtime has one). Decoded outcome rows must pass `Outcome::validate_stored`.
- `Store::transaction` records multi-stream commands with `Transaction::begin_command`, which must
  be the first call on the transaction (`Error::CommandOrder` after any read, append, or fence; a
  replayed result finishes the transaction), then
  `commit_with_outcome` or `reject`. A failed caller-stamped fence persists a `stale-fence`
  rejection instead of retrying; the remedy is a new command ID.

```rust
use photon_evt::{
    Aggregate, BoxError, MemRepository, Metadata, OutcomeCommand, OutcomeConfig, Rejection,
    Store, ToRejection, canonical_json, domain_events,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Counted {
    pub by: i64,
}

domain_events! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum CounterEvent {
        Counted(Counted) => ("counter:counted", 1),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("count must be positive")]
pub struct NotPositive;

impl ToRejection for NotPositive {
    fn to_rejection(&self) -> Option<Rejection> {
        Some(Rejection::new("not-positive", self.to_string()))
    }
}

pub struct Count(pub i64);

impl OutcomeCommand for Count {
    fn command_type(&self) -> String {
        "counter:count".into()
    }

    fn canonical_payload(&self) -> Result<Option<Vec<u8>>, BoxError> {
        Ok(Some(canonical_json(&self.0)?))
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Counter {
    pub total: i64,
}

impl Aggregate for Counter {
    const TYPE: &'static str = "counter";

    type Command = Count;
    type Event = CounterEvent;
    type Error = NotPositive;
    type Services = ();

    async fn handle(&self, Count(by): Count, _: &()) -> Result<Vec<CounterEvent>, NotPositive> {
        if by <= 0 {
            return Err(NotPositive);
        }

        Ok(vec![Counted { by }.into()])
    }

    fn apply(&mut self, event: CounterEvent) {
        match event {
            CounterEvent::Counted(counted) => self.total += counted.by,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::<Counter, _>::new(MemRepository::new(), ())
        .with_outcomes(OutcomeConfig::new());
    let metadata = Metadata::new("us-west-2").with_command_id("cmd-1");

    let first = store.execute_with_outcome("c-1", Count(5), metadata.clone()).await?;
    assert!(first.is_accepted() && !first.replayed);

    // The retry returns the stored outcome; the handler does not run again.
    let again = store.execute_with_outcome("c-1", Count(5), metadata.clone()).await?;
    assert!(again.replayed);
    assert_eq!(store.load("c-1").await?.aggregate().total, 5);

    // The same ID for a different command is refused.
    let reused = store.execute_with_outcome("c-1", Count(6), metadata).await;
    assert!(matches!(reused, Err(photon_evt::Error::IdempotencyKeyReuse { .. })));

    // A domain error is a durable rejection.
    let rejected = store
        .execute_with_outcome("c-1", Count(0), Metadata::new("us-west-2").with_command_id("cmd-2"))
        .await?;
    assert_eq!(rejected.rejection().map(|r| r.code.as_str()), Some("not-positive"));

    Ok(())
}
```

`photon_evt::conformance::FaultInjectingRepository` wraps a repository to make a commit apply and
then report an indeterminate failure, and `outcome_cases` runs the outcome conformance cases
against a repository that stores them. With outcomes enabled, the plain `Store::execute`,
`Store::commit`, and `Transaction::commit` refuse to run, so no outcome is skipped silently. The
older duplicate-command window is kept as a pre-check for IDs written before outcomes existed.

## Events and versions

Each `domain_events!` variant declares its stored type and version. Loading a stored event whose
version, after upcasting, is not the declared one fails with `PayloadError::VersionMismatch`
rather than decoding it into the wrong shape. The macro implements `From<Payload>` for each
variant. Add `no_from` to skip it when two variants share a payload type:

```rust,ignore
domain_events! {
    pub enum AccountEvent {
        Frozen(Empty) => ("account:frozen", 1, no_from),
        Unfrozen(Empty) => ("account:unfrozen", 1, no_from),
    }
}
```

## Async runtime and sharing

The core crate does not depend on an async runtime. The `dynamodb` and `postgres` features use
Tokio through the AWS SDK and `tokio-postgres`. `execute_with_retry` takes the sleep function from
the caller, such as `tokio::time::sleep`.

A `Store` is `Send + Sync` when its repository and services are, so share it with `Arc`. It is
`Clone` when they are `Clone`. `Repository` uses `async fn` in traits, so it cannot be used as
`dyn Repository`: pick the backend with a generic parameter, or wrap the backends in an enum.

`Store::execute` is not cancellation safe. If its future is dropped while the commit is in flight,
the events may or may not have been stored. Reload before deciding what to do. A command ID only
catches the repeat while the first run's events are after the latest snapshot: if that commit also
wrote a snapshot, the reloaded entity no longer sees the ID, and running the command again applies
it twice.

## DynamoDB

Enable the `dynamodb` feature and bring your own `aws_sdk_dynamodb::Client`. The crate disables the
SDK's default HTTP and runtime features, so enable them in your application (for example, by
depending on `aws-config` with its defaults).

```rust,ignore
use photon_evt::dynamodb::{DynamoRepository, ViewProjector};

let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
let repository = DynamoRepository::new(aws_sdk_dynamodb::Client::new(&config), "event-log");

let store = Store::<Account, _>::new(repository, ())
    .with_snapshot_interval(25)
    .with_projector(ViewProjector::new("entity-views"));
```

The table needs a string partition key `pk` and a number sort key `sk`, the same table Go's
`dynamo.Repository` uses.

## PostgreSQL

Enable the `postgres` feature. The repository uses a `deadpool_postgres::Pool`, and
`ensure_schema` runs the same idempotent DDL as Go's `EnsureSchema`. Engine relations use the
reserved `__evt_` infix; schema setup migrates the old `{events}_entity_type_idx` index to
`{events}__evt_entity_type_idx`. Configured unquoted names are lowercase-canonicalized, with a
34-byte events-table limit and a 58-byte snapshots-table limit. Both names must avoid `__evt_`
and any collision among the tables and derived relations.

```rust,ignore
use photon_evt::postgres::PostgresRepository;

let repository = PostgresRepository::new(pool);
repository.ensure_schema().await?;

let store = Store::<Account, _>::new(repository, ());
```

## Enumerating the log

`EventLog`, implemented by all three backends, is the Rust counterpart of Go's `StreamAllEvents`
and `StreamEntities`. Rebuilds and diagnostic tools use it to walk the log without loading it:

```rust,ignore
use futures_util::StreamExt; // any `futures_core::Stream` consumer works
use photon_evt::{EventLog, StreamFilter};

// Batches of events, narrowed to one entity type.
let mut batches = std::pin::pin!(repository.stream_all_events(StreamFilter::of_type("account")));
while let Some(batch) = batches.next().await {
    for event in batch? {
        // ...
    }
}

// Aggregates folded from their events, one `Loaded<Account>` at a time.
let mut accounts = std::pin::pin!(repository.stream_entities::<Account>(StreamFilter::all()));
```

- **Lazy and cancellable.** Nothing is read until the stream is polled, and dropping it cancels the
  read. A PostgreSQL stream returns its pooled connection when dropped.
- **Ordering.** PostgreSQL and the in-memory repository yield one batch per entity, entities
  ascending by ID and each entity's events ascending by sequence. DynamoDB yields one batch per
  Scan page: no order, and an entity's events can span pages. `stream_entities` yields entities in
  the order their first event was seen and folds each in sequence order.
- **Memory.** One entity (PostgreSQL, in memory) or one page (DynamoDB) for `stream_all_events`.
  On DynamoDB `stream_entities` buffers every matched event until the Scan completes, as Go does,
  because a Scan does not group an entity's events. `DynamoRepository::with_scan_page_size` caps
  the page.
- **Filter.** `StreamFilter::all()` matches everything (Go's zero filter). `of_type("")` matches
  only entities stored with an empty type. `stream_entities::<A>` narrows to `A::TYPE` unless the
  filter names a type.
- **Skipped.** Entities in the reserved `SYSTEM#` and `command-outcome#` namespaces, and snapshot
  items, are never yielded.
- **Errors.** A backend error ends the stream as its last item. An entity that fails to fold (an
  unknown event type, a failed upcast, a sequence gap from compaction) is yielded as an error and
  the stream continues. Entities fold from events only, as in Go, so a compacted stream reports
  `SequenceGap`: load it with `Store::load` instead.

## Testing your domain

The `testing` feature adds `photon_evt::testing`: given/when/then tests over `MemRepository`, the
counterpart of Go's `test` package. `Scenario` seeds a stream, runs a command through a real
`Store`, and asserts the emitted events, the rejection, a store error, or the resulting state:

```rust,ignore
use photon_evt::testing::Scenario;

#[tokio::test]
async fn withdrawing_more_than_the_balance_is_rejected() {
    Scenario::<Account>::given([opened("acct-1", 10)])
        .when(AccountCommand::Withdraw { amount: 11 })
        .then_rejected(|error| matches!(error, AccountError::InsufficientFunds { .. }))
        .await;
}
```

`Fixture` is the multi-stream piece: `given::<A>(id, events)` seeds (or, on an existing stream,
stands in for a concurrent writer), `transaction()` starts a `Store::transaction`-style commit
over the same data, and `events::<A>(id)` / `assert_events` / `load::<A>(id)` inspect each
stream. `Scenario::on(&fixture)` runs a single-stream scenario against it.

[`examples/banking.rs`](examples/banking.rs) puts it together: `domain_events!` enums, two
aggregates, a balance read model fed only from committed events, and a two-account transfer as a multi-stream transaction with a
read-fenced `bank` stream. Run it with
`cargo run -p photon-evt --example banking --features testing`; its tests run with
`cargo test -p photon-evt --features testing --example banking`.

## Testing a backend

The `conformance` feature adds the backend conformance suite that this crate runs against its own
backends:

```rust,ignore
photon_evt::conformance::Suite::new(&my_repository).run().await;
photon_evt::conformance::outcome_cases(&my_repository, &second_handle).await;
// For a backend that implements `EventLog`:
photon_evt::conformance::enumeration_cases(&my_repository).await;
```

## Compatibility with Go

- Keep payload numbers integral (for example, cents): Go and Rust format some floats differently.
- Declare payload and aggregate fields in the same order as the Go structs, and use `BTreeMap` for
  maps, so the JSON bytes match. Go sorts map keys as strings, so a map with integer keys needs
  `#[serde(serialize_with = "photon_evt::json::go_key_order")]`.
- Use `photon_evt::json::to_vec` for any JSON you want byte-identical to Go's `json.Marshal`.
- Go re-encodes a `time.Time` field in its own RFC 3339 form (`05.100Z` becomes `05.1Z`). If a Go
  struct parses a timestamp that a Rust struct keeps as a string, store timestamps in Go's form.
- Rust refuses to decode an event at a version it does not declare. Go decodes it anyway. Deploy
  readers before writers when you bump an event version.

The known differences are listed in the
[storage format](https://github.com/photon-grove/evt/blob/main/docs/spec/storage-format.md#known-differences).

The [feature parity matrix](https://github.com/photon-grove/evt/blob/main/docs/parity/feature-matrix.md)
tracks every capability across languages. The vNext plan's core reshape (multi-stream
transactions, command outcomes, fences —
[ADR 0004](https://github.com/photon-grove/evt/blob/main/docs/adr/0004-core-reshape-multi-stream-transactions-and-outcomes.md)),
Rust-first extensions such as the journal and the Lambda toolkit
([ADR 0005](https://github.com/photon-grove/evt/blob/main/docs/adr/0005-rust-first-extensions-journal-and-lambda.md)),
and the port/defer/cut parity policy
([ADR 0006](https://github.com/photon-grove/evt/blob/main/docs/adr/0006-parity-policy-and-feature-cuts.md))
decide what lands here and when. Core changes land in Go and Rust together; extensions are
Rust-first, and Go ports them from the spec and fixtures rather than hand-rolling them in apps.

## Minimum Rust version

Rust 1.88 for the core crate and the `postgres` and `conformance` features. CI checks this.

The `dynamodb` feature follows the AWS SDK's minimum Rust version, which rises over time. On Rust
1.84 and later, Cargo's MSRV-aware resolver (the default for edition 2024, or `resolver = "3"`)
picks an SDK release that supports your `rust-version`. `aws-sdk-dynamodb` 1.104 is the latest
release that supports 1.88.

## License

Apache-2.0.
