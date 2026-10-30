# photon-evt

Event sourcing for Rust, with DynamoDB and PostgreSQL event logs that are wire-compatible with the
Go module, [`github.com/photon-grove/evt/go`](https://github.com/photon-grove/evt/tree/main/go).
Event sourcing means the log of events is the record of truth, and an aggregate's state is whatever
you get by folding its events in order. This crate isn't a binding to the Go code. It's a native
implementation of the same format, so a Go service and a Rust service can append to and load the
same streams: the same DynamoDB items, PostgreSQL rows, JSON and stream envelopes, byte for byte.
The [storage format](https://github.com/photon-grove/evt/blob/main/docs/reference/storage-format.md) is written down, and shared fixtures plus a
live cross-language run hold both languages to it.

```sh
cargo add photon-evt                       # the core and the in-memory backend
cargo add photon-evt --features dynamodb   # or postgres, journal, conformance, testing
```

You define events with `domain_events!` and state with the `Aggregate` trait. A `Store` loads an
aggregate (from its latest snapshot when there is one), runs a command against it, and commits the
decided events with optimistic concurrency, meaning the commit fails if another writer got to the
stream first. `MemRepository` backs tests, and the `dynamodb` and `postgres` features add the
durable backends. Upcasters migrate old payload versions as they're read, `compact_below` deletes
only events a snapshot already covers, and in-band projectors write views in the same transaction
as the events. The [concepts guide](https://github.com/photon-grove/evt/blob/main/docs/guide/concepts.md) walks through each of these, and the
[getting started guide](https://github.com/photon-grove/evt/blob/main/docs/guide/getting-started.md) builds one end to end in Rust, then in Go.

`Store::transaction` goes further. It loads several aggregates, appends decided events to any of
them, and commits once, so all the streams land or none does. Streams it read but didn't write are
fenced at the head it saw. That check is a read fence: the commit fails if a stream it only read has moved since. Every event of the commit carries one `commitId` (the command ID when
there is one, else a generated ULID). Every commit, in both languages and on every backend, fits a
shared budget of 100 items, 3.8 MB and 380 KB, or it fails with `CommitTooLarge` before anything is
written. Each commit also spends two items on control checks (the feature marker and the rebuild
lock), so a single-stream commit holds at most 97 events or projector operations, 96 with a
snapshot, and one fewer with an outcome.

Each stream has one small head item (DynamoDB `sk = -1`, or a row in `{events}__evt_stream_heads`
on PostgreSQL) recording the highest sequence committed to it. A commit raises the head with an
update conditioned on the head it saw, and a read fence is one check on that same item. A stream
whose head disagrees with its events or snapshot fails with `Error::StreamHeadDrift`, which is never
retried and never recorded as a rejection. Every writer requires the `stream-heads` feature in the
feature marker. A table written before stream heads is refused until `evt-doctor migrate-heads` has
backfilled it and declared the feature (`FeatureMarker::declare_stream_heads` is the hook for that
call). A fresh store is initialized once, before writers start, with `FeatureMarker::initialize` (or
`evt-doctor init`), which verifies the store holds no data and is idempotent. `initialize` is a new
required trait method, so a custom `FeatureMarker` implementation must add it. A DynamoDB writer never
scans: it refuses a table that doesn't declare the feature with `Error::StoreNotInitialized`, so its
role needs no `dynamodb:Scan`, and `with_lazy_initialization(true)` restores first-write
initialization for tests and local use. PostgreSQL and in-memory writers initialize a fresh store on
their first write and refuse a populated undeclared one with `Error::StreamHeadsNotMigrated`. See
the [migration runbook](https://github.com/photon-grove/evt/blob/main/docs/guide/migrating-to-stream-heads.md) and
[ADR 0007](https://github.com/photon-grove/evt/blob/main/docs/adr/0007-per-stream-head-item.md). DynamoDB scanners skip the head item
(`sk < 1`), and a Streams consumer has to skip its records too.

The `SYSTEM#` and `command-outcome#` namespaces are reserved for engine rows. Writes to them are
refused and entity heads skip them. A `SYSTEM#FEATURES` marker row declares the extensions a table
uses, and a writer that doesn't support a declared feature refuses to write, maintenance snapshots
included. Declaring a feature invalidates the writer's cache, but it never grants support for it.
Durable history-dependent declarations fence each compaction batch under a marker check on DynamoDB,
or all deletes under one transaction lock on PostgreSQL. DynamoDB has no whole-operation lease, so a
declaration that races a multi-batch compaction can leave a stream partly compacted, and its
history-dependent rebuild then reports `CompactionUncovered`. Declare history dependence before you
write data of that type.

The rest of this page covers the larger features. [Command outcomes](#command-outcomes) make a
retried command return its stored answer instead of running twice. Then come
[commit-wide projectors](#commit-wide-projectors), the [sync journal](#the-sync-journal), the
[rebuild fence](#rebuild-fence), [rebuilding projections](#rebuilding-projections), and
[enumerating the log](#enumerating-the-log) with `EventLog`.
There's also `latest(n)`, which reads a stream's last `n` events newest-first. The guide pages on
[commands and retries](https://github.com/photon-grove/evt/blob/main/docs/guide/commands.md), [projections](https://github.com/photon-grove/evt/blob/main/docs/guide/projections.md),
[DynamoDB](https://github.com/photon-grove/evt/blob/main/docs/guide/dynamodb.md) and [PostgreSQL](https://github.com/photon-grove/evt/blob/main/docs/guide/postgres.md) explain the same
ideas from the Go side.

This crate is versioned on its own and tagged `photon-evt-vX.Y.Z` on crates.io releases. The Go
module is versioned separately and tagged `go/vX.Y.Z`. The two share a storage format, not a
version number.

## Quick start

Here's a whole program against the in-memory backend. The account folds `Deposited` events into a
balance, the store refuses a second command with the same command ID as a duplicate, and
`execute_with_retry` retries conflicts and transient backend failures with backoff, sleeping on the
function you pass it.

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

A store built with `with_outcomes` writes one `command-outcome#<commandId>` row per command, once
and in its final state, inside the commit transaction. Every command needs a command ID of at most
512 UTF-8 bytes with no U+0000, which PostgreSQL can't store. The same rule covers digests and
rejection codes, messages and details. The command type implements `OutcomeCommand`, and
`Aggregate::Error` implements `ToRejection`. The
[commands guide](https://github.com/photon-grove/evt/blob/main/docs/guide/commands.md) explains the model in more depth.

A repeat of a command, meaning the same ID with the same identity digest, returns the stored
outcome without running the handler or writing anything. The dedupe is global, so it survives
snapshots and compaction. The digest is a SHA-256 over the command's type, the target streams the
request names, canonical payload JSON and the principal, and `OutcomeConfig::with_digester`
replaces it. The same ID with another digest is `Error::IdempotencyKeyReuse`, which carries the
stored `prior_outcome` and is not itself stored.

An error from `handle` that `ToRejection` maps to `Some` is a domain rejection. It's persisted as a
`rejected` outcome (`Rejection { code, message, details }`), and a retry replays it verbatim. A
`None` is an internal failure: it comes back as `Error::Handler` and records nothing, so a retry
after the fix starts clean. A rejection can also carry events, for example an `apply-failed` event
left on a proposal whose base revision went stale. Implement `Aggregate::rejection_events(&self,
error)` to return them (the default is none), or in a transaction stage them with `append` and
return `Decision::reject_with_events` (or call `Transaction::reject_with_events`). They commit
atomically with the rejected row and are applied, snapshotted and projected like accepted events.
`Decision::reject` and `Transaction::reject` still discard what was staged. A rejection stores no
response. A command that decides no events is an accepted no-op, and a response of at most 65536
bytes can be stored with any accepted outcome (see `Store::execute_with_response`).

A commit whose result is unknown (`Error::Indeterminate`, from a timeout, a dropped connection or a
server fault) is settled by polling the outcome row for the recovery window
(`OutcomeConfig::with_recovery_wait`, 10 s), not by running the handler again. The stored outcome
comes back with `replayed` set, or a row that never appears becomes the retryable
`Error::AmbiguousWrite`. A definitive refusal, such as a validation or authorization error, stays
`Error::Backend`. So a backend author has one rule to follow: fail a commit whose result is truly
unknown with `Error::Indeterminate`, and every definitive failure with `Error::Backend`.
`Repository::backoff` is the timer recovery polling sleeps on and `Repository::deadline` bounds each
recovery read, so override both if your runtime has one, or bound your own reads if it has none.
Decoded outcome rows must pass `Outcome::validate_stored`.

`execute_with_outcome`, `execute_with_response` and `Store::execute_transaction` retry a command
that conflicts with another writer, up to the store's `RetryPolicy` (`OutcomeConfig::with_retry`).
The default is 5 attempts, 25 ms doubling to 1 s, jittered, waiting on `Repository::backoff`.
`RetryPolicy::immediate` skips the waits in tests, and `with_deadline` bounds the whole command. A
conflict the engine can re-derive re-prepares the command, meaning it reloads the aggregate and
runs the handler or your `prepare` closure again against fresh state. So handlers and closures must
be safe to repeat, and a command passed to `execute_with_outcome` must be `Clone`. When the last
attempt also conflicts, the engine records `Rejection::retries_exhausted(attempts)` (code
`stale-fence`), and a new command ID is the way to try again. For multi-stream commands,
`Transaction::begin_command` must be the first call on the transaction (`Error::CommandOrder`
otherwise), followed by `commit_with_outcome` or `reject`, attempting once. The
[commands guide](https://github.com/photon-grove/evt/blob/main/docs/guide/commands.md) covers
identity, ambiguous writes and fence-aware retry in full, and the rules are the same in both
languages.

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

#[derive(Clone)]
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

A multi-stream command runs under the same retry with `Store::execute_transaction`. The closure is
the command's decision step and runs again from scratch whenever the engine re-prepares it. Its
identity names only what the request names (`with_targets`), so the digest stays the same however
the write set changes. That means everything that distinguishes the request has to be in the
targets or the payload, as the
[commands guide](https://github.com/photon-grove/evt/blob/main/docs/guide/commands.md#fence-aware-retry)
explains:

```rust
use photon_evt::{
    Aggregate, CommandIdentity, Decision, MemRepository, Metadata, OutcomeConfig, Store,
    domain_events,
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

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Counter {
    pub total: i64,
}

impl Aggregate for Counter {
    const TYPE: &'static str = "counter";

    type Command = ();
    type Event = CounterEvent;
    type Error = std::convert::Infallible;
    type Services = ();

    async fn handle(&self, _: (), _: &()) -> Result<Vec<CounterEvent>, Self::Error> {
        Ok(Vec::new())
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

    // The request names counter "a". Which counter it also writes comes from the state it reads.
    let identity = CommandIdentity::new("counter:spill").with_targets(["a"]);
    let metadata = Metadata::new("us-west-2").with_command_id("cmd-1");

    let outcome = store
        .execute_transaction(&identity, &metadata, |tx| {
            Box::pin(async move {
                let a = tx.read::<Counter>("a").await?; // read only: an engine-filled fence
                let spill = if a.aggregate().total > 0 { "b" } else { "c" };

                let mut destination = tx.read::<Counter>(spill).await?;
                tx.append(&mut destination, vec![Counted { by: 1 }.into()])?;

                Ok(Decision::accept())
            })
        })
        .await?;
    assert!(outcome.is_accepted());

    // The same command again replays the stored outcome; the closure does not run.
    let again = store
        .execute_transaction(&identity, &metadata, |_| unreachable!("a replay prepares nothing"))
        .await?;
    assert!(again.replayed);

    Ok(())
}
```

`photon_evt::conformance::FaultInjectingRepository` wraps a repository to make a commit apply and
then report an indeterminate failure, and `outcome_cases` runs the outcome conformance cases
against a repository that stores them. With outcomes enabled, the plain `Store::execute`,
`Store::commit`, and `Transaction::commit` refuse to run, so no outcome is skipped silently. The
older duplicate-command window is kept as a pre-check for IDs written before outcomes existed.

## Events, runtimes and sharing

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

## Snapshots and load telemetry

A commit snapshots when it reaches the next multiple of the snapshot interval, 5 by default.
`Store::with_snapshot_interval` sets the store's interval, which its transactions inherit. An
aggregate type can declare its own, which wins wherever that type is committed, like Go's
per-type `WithSnapshotSize`:

```rust,ignore
impl Aggregate for Account {
    const TYPE: &'static str = "account";
    const SNAPSHOT_INTERVAL: Option<usize> = Some(50); // Some(0) never snapshots this type
    // ...
}
```

A stream that is read far more often than written can still replay a long tail on every load.
`Store::with_catch_up_snapshot_threshold(n)` (off by default, like Go's
`WithCatchUpSnapshotThreshold`) has a load that replays at least `n` events write the state it
folded as the next snapshot. The write never lowers a stored snapshot, and it is best effort: a
failure is a `tracing` warning, and the load still returns. The store's loads, its execute paths and
its transactions' reads all catch up.

A snapshot is a single item, so keep an aggregate's state bounded and its unbounded history in
events. A snapshot whose item would be over the commit's per-item budget,
`photon_evt::MAX_COMMIT_ITEM_BYTES` (measured by `snapshot_commit_size`), is skipped with a
`tracing` warning instead of failing the commit with `Error::CommitTooLarge`, on commits,
transactions and catch-up loads alike. Loads then replay from the last snapshot that fit
([Oversized snapshots](https://github.com/photon-grove/evt/blob/main/docs/reference/storage-format.md#oversized-snapshots)).

Every load runs in a `DEBUG` `tracing` span named `load` with `entity_type`, `entity_id`,
`sequence`, `snapshot_seq` (the snapshot the load started from, 0 without one) and
`events_replayed`. Alert on `events_replayed` to find streams that need a smaller interval or a
catch-up threshold.

## Backends

For DynamoDB, enable the `dynamodb` feature and bring your own `aws_sdk_dynamodb::Client`. The crate disables the
SDK's default HTTP and runtime features, so enable them in your application (for example, by
depending on `aws-config` with its defaults).

```rust,ignore
use photon_evt::dynamodb::{DynamoRepository, ViewProjector};

let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
let repository = DynamoRepository::new(aws_sdk_dynamodb::Client::new(&config), "event-log");

// Once, at deploy time, with a role that may Scan the table (writers never scan):
repository.initialize().await?;

let store = Store::<Account, _>::new(repository, ())
    .with_snapshot_interval(25)
    .with_projector(ViewProjector::new("entity-views"));
```

The table needs a string partition key `pk` and a number sort key `sk`, the same table Go's
`dynamo.Repository` uses.

For PostgreSQL, enable the `postgres` feature. The repository uses a `deadpool_postgres::Pool`, and
`ensure_schema` runs the same idempotent DDL as Go's `EnsureSchema`, plus the Rust-only views table
(`{events}__evt_views`, below). Engine relations use the
reserved `__evt_` infix. Schema setup migrates the old `{events}_entity_type_idx` index to
`{events}__evt_entity_type_idx`, and it creates the rebuild fence's `{events}__evt_rebuild_lock`
and `{events}__evt_rebuild_generation` (a schema created before them treats a commit's lock check
as passing until `ensure_schema` runs again). Configured unquoted names are lowercase-canonicalized,
with a 34-byte events-table limit and a 58-byte snapshots-table limit. Both names must avoid `__evt_`
and any collision among the tables and derived relations.

```rust,ignore
use photon_evt::postgres::PostgresRepository;

let repository = PostgresRepository::new(pool);
repository.ensure_schema().await?;

let store = Store::<Account, _>::new(repository, ());
```

## Commit-wide projectors

A `CommitProjector` is a pure function over everything one commit wrote, across every stream and
aggregate type. It returns `ViewOperation`s, puts and deletes of rows at any `pk`/`sk`, optionally
conditioned (`if_absent` on a put, `if_present` on a delete) and mapped to your own error
(`on_conflict`). The rows commit in the same transaction as the events, so a read model that spans
entities is atomic with the commit. Register projectors with `Store::with_commit_projector` (a
store's transactions inherit them) or `Transaction::with_commit_projector`.

```rust,ignore
use std::sync::Arc;
use photon_evt::{BoxError, CommitProjector, CommitView, ViewOperation};

struct Directory;

impl CommitProjector for Directory {
    fn name(&self) -> &str {
        "directory"
    }

    fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
        Ok(commit
            .streams
            .iter()
            .map(|stream| {
                ViewOperation::put(
                    "directory",
                    stream.entity_id.as_str(),
                    stream.entity_id.clone(), // the owner: the stream that owns the row
                    stream.entity_type.as_str(),
                    stream.state.clone(), // the aggregate's JSON after the commit
                )
            })
            .collect())
    }
}

let repository = DynamoRepository::new(client, "event-log").with_views_table("views");
let store = Store::<Account, _>::new(repository, ()).with_commit_projector(Arc::new(Directory));
```

The repository decides where the rows go: `DynamoRepository::with_views_table`, a
`PostgresRepository::with_view_writer` that turns each operation into a statement for your own
table, or `MemRepository` (read back with `view` and `views`). A repository with nowhere to write
them refuses the commit with `Error::ViewsNotConfigured`. A failed condition is
`Error::ProjectorOperation`, naming the projector and the operation, with your mapped error as its
`source()`. It's never retried and records no outcome. Keys over 2,048 bytes (`pk`) or 1,024 bytes
(`sk`), a repeated key, and operations with no event are refused before any write.

Every operation names an owner: the stream the row belongs to, which is stored as the row's
`entityID` attribute. The owner must be a stream the commit wrote. Every write to a row is
conditioned on its owner, so a stream never overwrites or deletes a row another stream owns: the
operation fails with `Error::ProjectorOperation` (reason `ConditionalCheckFailed`), and nothing of
the commit is written. A row that does not exist yet can be written by anyone.

A row's owner must never change. Never delete a row to hand its key to another stream: keep the
row and rewrite it, as its owner, with a tombstone payload. A rebuild orders the writes to a row by
its owner's sequence, the only commit order the log keeps, and `replay_views` refuses a row that
two streams write (`Error::SharedViewKey`).

A PostgreSQL view writer has to enforce the owner in SQL, because the engine can't. The statements
it returns must have these semantics (see `PostgresRepository::with_view_writer`):

- a put with `if_absent` is a plain `INSERT` (no `ON CONFLICT`), and the statement names the
  table's `(pk, sk)` key constraint with `PgStatement::with_key_constraint`: only a violation of
  that constraint is a failed `if_absent` condition.
- any other put is an upsert that only the owner may overwrite:
  `INSERT ... ON CONFLICT (pk, sk) DO UPDATE SET ... WHERE <table>.entity_id = EXCLUDED.entity_id`.
  A put that affects no row (the row is another stream's) fails its condition.
- a delete with `if_present` removes the owner's row only:
  `DELETE ... WHERE pk = $1 AND sk = $2 AND entity_id = $3`, and a delete that affects no row fails
  its condition.
- a delete without a condition succeeds on an absent row but refuses another stream's row, so its
  statement reports the foreign rows it found rather than the rows it deleted:
  `WITH gone AS (DELETE ... WHERE pk = $1 AND sk = $2 AND entity_id = $3) SELECT 1 FROM <table>
  WHERE pk = $1 AND sk = $2 AND entity_id <> $3`. An affected-row count above zero fails its
  condition (a plain `DELETE` would silently do nothing to another stream's row).

A projector says which entity types it reads the *events* of with `history()`. The default, none,
is a current-state projector. It's given an empty commit ID and streams without events, and
the state of every stream, so a rebuild reproduces its rows from the final states alone. One that
names types is history-dependent. It's given the commit ID, and the events and state of the
streams of those types, and every other stream arrives without events and without state. A
history-dependent projector may only own rows by streams of the types it names. The store durably
adds those types to the manifest's `historyDependent` set before the projector's first commit
(`compact_below` then refuses them), so the repository must implement `declare_history_dependent`
and `writes_views` (the in-memory, DynamoDB, and PostgreSQL ones do). A row keyed by the commit ID
therefore needs a history-dependent projector.

A projector must be deterministic: `group_commits` regroups stored events by commit ID in an order
every stream agrees with, so a rebuild, or a test, can run the projector again over each commit
(`replay_views`) and compare the rows.

## The sync journal

Enable the `journal` feature for per-scope ordered, replayable delivery rows written in the commit
that wrote the events ([ADR 0005](https://github.com/photon-grove/evt/blob/main/docs/adr/0005-rust-first-extensions-journal-and-lambda.md),
Rust only for now, and Go refuses a table that declares it). A scope is a named, independently
ordered sequence (a world, or a place in it). You map each event of the journaled entity types to
zero or more scopes with a pure function. The event takes the next position in each, and a client
polls a scope with `high_watermark` and `page`. The example below is [`examples/journal.rs`](https://github.com/photon-grove/evt/blob/main/crates/evt/examples/journal.rs),
which `cargo test --all-features` compiles and runs, and a test fails if the two drift apart.

```rust,ignore
use photon_evt::conformance::{Add, Ledger};
use photon_evt::journal::{Journal, JournalRepository, Scope};
use photon_evt::{Error, MemRepository, Metadata, SerializedEvent, Store};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Each event goes to its ledger's scope and to a scope shared by every ledger. The resolver is
    // a pure function of the stored event; it may return a `Result` and use `?`.
    let journal = Journal::new(
        ["ledger"],
        |event: &SerializedEvent| -> Result<Vec<Scope>, Error> {
            Ok(vec![
                Scope::new(format!("ledger:{}", event.entity_id))?,
                Scope::new("all")?,
            ])
        },
    )?
    .with_max_row_bytes(30_000)? // the default
    .with_retry(photon_evt::RetryPolicy::new(5)); // the default

    // Wrap a repository that writes views: in memory here, DynamoDB with a views table in
    // production.
    let repository = JournalRepository::new(MemRepository::new(), journal.clone());
    let store = Store::<Ledger, _>::new(&repository, ());

    store.execute("a", Add(5), Metadata::new("local")).await?;
    store.execute("b", Add(7), Metadata::new("local")).await?;
    store.execute("a", Add(1), Metadata::new("local")).await?;

    // A poller keeps the last position it saw per scope.
    let scope = Scope::new("all")?;
    let mut after = 0;
    loop {
        let page = repository.page(&scope, after, 100).await?;
        // ... deliver page.rows (each row.event is the stored event) ...
        after = page.next_after();

        if let Some(gap) = page.gap {
            // Positions gap.from..=gap.to are missing: retry the page, escalate if it persists.
            return Err(format!("journal gap {gap:?}").into());
        }
        if !page.has_more() {
            break; // caught up with repository.high_watermark(&scope)
        }
    }
    assert_eq!(after, 3);

    Ok(())
}
```

Behind that call, every commit that writes a journaled event reads the scopes' counters, allocates
positions, stores them in the event's metadata (`evt.journal`), and adds the journal rows and the
counters to the commit's view operations, so they are atomic with the events. Because the
allocations are on the events, `journal::replay` and `journal::rebuild` rebuild the rows and counters
from stored events in any order. The journaled entity types are declared history-dependent in the
feature marker with the `journal` feature (`journal: 1`), so they are never compacted and builds
without the feature refuse the table.

- Row-size guard. A row over the cap (default 30,000 bytes of stored payload, at most 370,000)
  fails the commit before any write with `Error::JournalRowTooLarge`. A store with outcomes records
  that as the command's `journal-row-too-large` rejection. `Journal::pre_check` (or
  `pre_check_events` for typed events) measures the same rows without a counter, at the widest
  position. It is exact for the metadata you give it and advisory otherwise (a handler does not see
  the commit's principal, extensions, or command ID), so pass the commit's metadata or add
  `with_pre_check_headroom`. Call it in your command handler and turn the error into a recorded
  rejection with `RowTooLarge::to_rejection`.
- Contention. Writers racing for a scope lose positions to each other. The journal re-reads the
  counters and allocates again under its `RetryPolicy` (and retries transient failures). When
  attempts run out on taken positions it returns `Error::Conflict`. A conflict on the stream itself
  is yours to re-prepare. A hot scope serializes on its counter item. A counter that falls behind its
  rows (a restore, a rebuild beside writers) is healed from the highest row.
- A cursor ahead of the journal. `page` flags `cursor_ahead` when `after` is above the high
  watermark (after a restore, say). The page isn't caught up, and the consumer should resync.
- Every writer of the table must commit through a `JournalRepository`. The marker stops builds
  without the feature, not a writer that bypasses the journal, and the `journal` cargo feature is
  build-wide, so a build that enables it accepts a journal table through every repository,
  wrapped or not. A bypassing writer's events would be missing from the scopes. Enable the journal
  before writing the events it covers: earlier events have no row and no gap shows it. A commit's
  journal rows and counters count toward the commit budget (97 event and projector operations, 96 with a snapshot).
- Rebuilds run under the [rebuild lock](#rebuild-fence) and write the rows and counters through
  `write_rebuild_views`, fenced on the lease's owner and fencing token. To rebuild the journal with
  your commit-wide views under one lease, use `ProjectionRebuild::with_journal`
  ([Rebuilding projections](#rebuilding-projections)). `rebuild_with_lock` takes the
  lock for the run; `rebuild_with_lease` runs under a lease you took and end, so one lease can cover
  the journal and your other derived state; `rebuild` writes under a lease without renewing it. The
  first two renew the lease before a batch whenever half of it has passed, so a long rebuild is not
  taken over, and a renewal that finds the lock gone stops the run before another write. Live
  commits fail with `Error::RebuildInProgress` while the lock is held, so a rebuild cannot lower a
  counter under a writer. A failed run releases the lock only when it attempted no write and its
  begin acquired the lock from a settled generation; otherwise it keeps the lock (the rows may be
  half-written). Under your own lease, apply the same rule: `rebuild_with_lease` reports the batches
  it attempted (`RebuildFailure`), and `RebuildLease::acquired_from` tells, from the generation you
  read just before your begin, whether the lease was acquired from a settled one. Count every write
  you made under the lease, not just this call's. Readers that can overlap a rebuild use
  `page_at_epoch` / `high_watermark_at_epoch`: they fail with `RebuildUnavailable` or
  `RebuildEpochChanged` (both transient) instead of showing half-written rows. Keep the cursor, back
  off, and read the same page again.
- Backends. DynamoDB and the in-memory repository. The Rust PostgreSQL repository does not write
  commit-wide views yet.
- Waking consumers. On DynamoDB, a consumer learns of new rows from the views table's stream,
  filtered to the journal's keys, and then reads `page` from its cursor. The
  [`photon-evt-lambda`](https://github.com/photon-grove/evt/tree/main/crates/evt-lambda) crate
  (feature `journal`) has the filter patterns, the wake-up decode, and the batch handling.

### Compaction and `410 Gone`

A scope's journal grows without bound, so an operator (or a scheduled job) compacts its oldest rows
into a snapshot. This is journal compaction:

```rust,ignore
use std::time::Duration;

use photon_evt::journal::{compact_with_lease, page, read_chunk, read_manifest};

// Store rows 1..=4000 as a snapshot, then delete them. Safe to run again.
let report = compact_with_lease(&repository, &scope, 4000, "compactor-1", Duration::from_secs(60)).await?;

match page(&repository, &scope, after, 100).await {
    Ok(page) => { /* as before */ }
    // The reader is below the floor: respond 410 with `gone`, and the client installs the snapshot.
    Err(Error::JournalGone(gone)) => {
        let manifest = read_manifest(&repository, &scope).await?.expect("a compacted scope");
        for index in 0..manifest.chunks.len() {
            let chunk = read_chunk(&repository, &manifest, index).await?; // digest-checked
            // ... apply chunk.rows
        }
        after = manifest.checkpoint; // resume paging after the checkpoint
    }
    Err(other) => return Err(other.into()),
}
```

- Order makes it safe without stopping writers. A run writes the canonical chunks, swaps the
  manifest (its checkpoint becomes the scope's floor), then deletes the rows, then writes an audit
  row. A `page` that meets a gap reads the floor after the rows, so a reader racing a compaction
  gets `Error::JournalGone` (map it to HTTP 410 with `JournalGone::HTTP_STATUS`), never a gap. Rows
  below the floor that are still there are served. A chunk read for a manifest that has since been
  replaced is the transient `Error::JournalSnapshotMoved`: read the manifest again.
- One run per scope. `begin_compaction` takes a lease with a fencing token
  (`CompactionLeaseHeld` while another holds it unexpired, and a lapsed one is taken over),
  `renew_compaction` and `end_compaction` keep or release it, and `force_release_compaction` is the
  operator's release of a named owner. Every write checks the lease and the rebuild epoch, so a
  worker that lost its lease or met a rebuild stops with `CompactionLeaseLost` or
  `RebuildEpochChanged` and has written nothing more. `compact_with_lease` does all of it.
- Rebuilds restore compacted rows and leave the manifest. Compact again at the same checkpoint
  to prune them. Compaction never deletes events, and adds nothing to a commit's budget. A commit's
  counter put is a compare-and-set on the counter it read, so a writer whose read went stale cannot
  claim positions whose rows were compacted, and a commit never allocates at or below a scope's
  floor, even after a lost counter. `compact` renews its lease in place during a long run.
- Reading the floor takes `JournalCompactionStore` (an extension of `JournalStore`, which is
  unchanged). `page`, `JournalRepository`, and compaction require it. Compaction rows
  that are not the engine's (`SYSTEM#JOURNAL`) are refused with `Error::ForeignJournalRow`, and a
  preflight refuses a gapped range or an oversized manifest before anything is written.
- The first lease declares `journal-compaction` on the feature marker, so a build that predates
  compaction refuses the table. `photon_evt::conformance::compaction_cases` checks a backend
  (`CompactionStore`, implemented by the in-memory and DynamoDB repositories).

The stored shapes are in the [storage format](https://github.com/photon-grove/evt/blob/main/docs/reference/storage-format.md#journal),
and `photon_evt::conformance::journal_cases` (features `journal` and `conformance`) checks a backend.

## Rebuild fence

A projection rebuild wipes and replays derived state, so it must not interleave with live writers
or be read half-done. `RebuildFence` (the in-memory, DynamoDB, and PostgreSQL repositories) keeps
a lock row and a generation counter, the same rows in both languages:

```rust,ignore
use std::time::Duration;

use photon_evt::{Error, RebuildFence, read_at_rebuild_epoch};

// The worker: take the lock (or an expired lease), write views in guarded batches, release.
let lease = repository.begin_rebuild("rebuild-7", Duration::from_secs(300)).await?;
repository.write_rebuild_views(&lease, &batch).await?; // at most 99 operations
let lease = repository.renew_rebuild(&lease, Duration::from_secs(300)).await?;
repository.end_rebuild(&lease).await?;

// A reader of derived state: observe the generation, read, verify.
let balance = read_at_rebuild_epoch(&repository, |_| async { read_balance().await }).await?;
```

- While the lock exists, an expired one included, every commit fails with
  `Error::RebuildInProgress`, which is transient. Fence-aware retry runs the command again after a
  backoff and returns the error when attempts run out. It is never a conflict, never a
  `stale-fence` rejection, and records no outcome.
- `begin_rebuild` takes over a lease that expired (the generation grows by two) and refuses one
  that is held with `Error::RebuildLockHeld`. The `RebuildLease` it returns carries a fencing
  token, the odd generation it produced, which the lock row stores: `renew_rebuild`,
  `end_rebuild`, and `write_rebuild_views` take the lease and act only while the lock names both
  its owner and its token. A worker that lost its lock, including a stale incarnation that reuses
  its owner token after a takeover, gets `Error::RebuildLeaseLost` and writes nothing.
- `force_release_rebuild` is the operator's escape hatch for a wedged worker. It ends the lease at
  once, so a new rebuild can begin, but leaves the generation unsettled, so readers stay fenced
  (`Error::RebuildUnavailable`) and every commit is still refused (`Error::RebuildInProgress`) until
  a later rebuild begins and ends. A failed rebuild may have cleared journal counters, and a
  journaled commit would otherwise allocate positions from them. To resume writes, run the rebuild
  again to its end (for the journal, `rebuild_with_lock` or a `ProjectionRebuild` `with_journal`).
- A command handler that reads a view under the epoch check returns `RebuildUnavailable` or
  `RebuildEpochChanged` as its own error. The engine returns it at once and records nothing.
- Leases are whole seconds read from the repository's clock, and `with_clock` injects one in tests.

## Rebuilding projections

Views are derived state, so they can be rebuilt from the log. `ProjectionRebuild` replays
commit-wide projectors over the aggregate types you register and writes their rows under the
rebuild fence:

```rust,ignore
use std::time::Duration;

use photon_evt::ProjectionRebuild;

let report = ProjectionRebuild::new(&repository)
    .with_projector(directory.clone())   // the same Arc<dyn CommitProjector>s the store runs
    .with_projector(activity_feed.clone())
    .aggregate::<Account>()
    .aggregate::<Team>()
    .run("rebuild-7", Duration::from_secs(300))
    .await?;
```

- It takes the lock for the whole run, so commits fail with `RebuildInProgress` while it reads and
  writes. It computes and checks every row first, then writes them in guarded batches (at most 99
  operations within the commit byte budget), renews the lease, and releases the lock when it's done.
- A current-state projector runs once per stream, over that stream's final state. Final states
  come from `EventLog::stream_entities_from_snapshots`, so compacted streams start from their
  snapshot. Only rebuild projectors whose rows depend on their owner's state alone: a row that
  reads another stream's state from the same commit is recomputed from one stream.
- A history-dependent projector replays every commit of the types it names (`group_commits`).
  Streams of other types in a commit are left out of the replayed view. A stream of those types
  whose history doesn't run from sequence 1 to its head (compacted, or a tail that retention's TTL
  deleted) fails the run with `CompactionUncovered` before anything is written.
- Conditions aren't evaluated, so the run can go over existing rows, and running it twice is safe.
  It doesn't delete rows nothing produces any more, so wipe them first to rebuild from scratch.
- It holds every row in memory, and needs strongly consistent reads of the log.
- `operations()` is a dry run that returns what `run` would write, without taking the lock.
- `run_with_lease` rebuilds under a lease you took and end, renewing it in place. A failure
  (`RebuildFailure`) says how many guarded batches it attempted, and `RebuildLease::acquired_from`
  says whether your begin acquired the lock from a settled generation, so you can apply the release
  rule below to your own lease (counting every write made under it).
- A failure before the first write leaves the rows alone, and releases the lock if the run acquired
  it from a settled generation. Once a write was attempted, a failure keeps the lock until a retry
  takes over the lapsed lease. A run that took over a lapsed lease, or acquired after a
  force-release, keeps the lock on any failure, since the rows may still be half-written from the
  earlier run; only a rebuild that completes releases it.
- `with_journal(journal)` rebuilds the sync journal in the same run, under the same lease: its rows
  and counters come from the events of the journal's types, read under the lock, and go out in the
  same guarded batches, so readers see one rebuild (one generation) for the views and the journal.
  A key both would write fails the run before anything is written. With a journal and no
  projectors, aggregate types are optional. It needs the `journal` cargo feature. The journal's
  projector itself is refused as a projector here.

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

- Lazy and cancellable. Nothing is read until the stream is polled, and dropping it cancels the
  read. A PostgreSQL stream returns its pooled connection when dropped.
- Ordering. PostgreSQL and the in-memory repository yield one batch per entity, entities
  ascending by ID and each entity's events ascending by sequence. DynamoDB yields one batch per
  Scan page: no order, and an entity's events can span pages. `stream_entities` yields entities in
  the order their first event was seen and folds each in sequence order.
- Memory. One entity (PostgreSQL, in memory) or one page (DynamoDB) for `stream_all_events`.
  On DynamoDB `stream_entities` buffers every matched event until the Scan completes, as Go does,
  because a Scan does not group an entity's events. `DynamoRepository::with_scan_page_size` caps
  the page.
- Filter. `StreamFilter::all()` matches everything (Go's zero filter). `of_type("")` matches
  only entities stored with an empty type. `stream_entities::<A>` narrows to `A::TYPE` unless the
  filter names a type.
- Skipped. Entities in the reserved `SYSTEM#` and `command-outcome#` namespaces, and snapshot
  items, are never yielded.
- Errors. A backend error ends the stream as its last item. An entity that fails to fold (an
  unknown event type, a failed upcast, a sequence gap from compaction) is yielded as an error and
  the stream continues. Entities fold from events only, as in Go, so a compacted stream reports
  `SequenceGap`. Use `stream_entities_from_snapshots` instead, which lists entities with
  `entity_heads` and loads each from its snapshot plus later events, like `Store::load`.

## Reading and writing views

`ViewRepository` is the Rust counterpart of Go's `evt.ViewRepository`: it reads and writes the rows
of the views table outside the commit path, with the same five attributes commit-wide view
operations write (the owner stream is the row's `entityID`) plus an optional `ttl`. `MemRepository`
implements it over the rows commits write, and `DynamoViewRepository` (feature `dynamodb`) over
the views table, which needs a string `pk` and `sk` and the `entityType-index` index on
`entityType`. `DynamoRepository::view_repository` builds one for the table
`with_views_table` configured. On PostgreSQL (feature `postgres`), evt owns the views table:
`ensure_schema` creates `{events}__evt_views`, and `PostgresRepository::view_repository` returns a
`PgViewRepository` over it. Its keys order by UTF-8 bytes like DynamoDB's, its payloads keep their
exact bytes, every read (entity-type lists included) sees every committed transaction, and
`purge_expired(now)` deletes rows whose `ttl` has passed, which PostgreSQL does not do by itself. It
needs a `UTF8` database. `PostgresRepository::with_views_table()` makes it the commit's view
target in place of `with_view_writer`: evt builds the statements with DynamoDB's owner rules,
applies the expected-payload condition, and declares the `postgres-views` marker feature so no
other writer puts the store's views elsewhere.

```rust,ignore
use photon_evt::{ViewRepository, ViewRow};

let views = repository.view_repository()?; // or DynamoViewRepository::new(client, "views")

// Writes take no owner check: use them for rows no commit-wide projector owns, and for rebuilds.
views.put_view(&ViewRow::new("USER#1#teams", "t1", "user-1", "team", r#"{"name":"core"}"#)).await?;
views.put_views(&rows).await?; // DynamoDB: BatchWriteItem in chunks of 25 (Go's PutViews)

let one = views.get_view("account#acct-1").await?; // the row at sk "VIEW"
let at = views.get_view_at("USER#1#teams", "t1").await?;
let found = views.batch_get_views(&["account#a", "account#b"]).await?; // BatchGetItem

// Lists: by partition (ascending by sk bytes) or by entity type (the index, unordered).
let mut cursor = None;
loop {
    let page = views.list_views_by_pk_paged("USER#1#teams", Some(50), cursor.as_deref()).await?;
    // ... page.views ...
    match page.next_cursor { Some(next) => cursor = Some(next), None => break }
}
```

A cursor is Go's: base64url JSON of the last key, so it moves between a Go and a Rust process. A
page can come back empty with a cursor on DynamoDB, so keep going until the cursor is `None`.

Point reads and partition listings see every commit that has returned, so a handler can commit and
read its own write. `DynamoViewRepository` gets that with strongly consistent reads of the base
table, at twice the read capacity of an eventual read. `with_consistent_read(false)` opts the
partition listings out when a stale page is acceptable. Entity-type listings read the
`entityType-index` global secondary index, which DynamoDB only reads eventually consistently, so
they can briefly miss a row a commit just wrote.

`ViewCodec<T, R>` (Go's `viewcodec.Codec`, `viewcodec.Single`, and the `GetJSONView` family) binds a
repository, an entity type and a serde type. It writes `T` as the payload with `json::to_vec`, so
the bytes are Go's `json.Marshal` of the same shape, and reads it back. `with_decoder` stands in
for Go's factory when a value needs constructor-injected dependencies.

```rust,ignore
use photon_evt::ViewCodec;

let profiles = ViewCodec::<Profile, _>::new(&views, "profile");

profiles.put_at("profile#ada", "ada", &profile).await?;
let ada: Option<Profile> = profiles.get("profile#ada").await?;

// Lists skip a payload that does not decode and report it; the _strict forms fail instead.
let all = profiles.list_all(|row, err| eprintln!("skipped {}/{}: {err}", row.pk, row.sk)).await?;
```

Where Go's decoder differs from serde's, serde wins. A JSON `null` does not decode into a struct
and field names match exactly, so mark fields `#[serde(default)]` that older or Go-written rows may
omit. Views are projections, so replaying the events must be able to rebuild every row, so keep the
mutable state behind these writes in events.

## Testing

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

[`examples/banking.rs`](https://github.com/photon-grove/evt/blob/main/crates/evt/examples/banking.rs) puts it together: `domain_events!` enums, two
aggregates, a balance read model fed only from committed events, and a two-account transfer as a multi-stream transaction with a
read-fenced `bank` stream. Run it with
`cargo run -p photon-evt --example banking --features testing`, and its tests run with
`cargo test -p photon-evt --features testing --example banking`.

[`examples/todo.rs`](https://github.com/photon-grove/evt/blob/main/crates/evt/examples/todo.rs) is
the commit-wide projector end to end: a current-state `CommitProjector` writes each todo list's
summary row in the commit, and `group_commits` and `replay_views` rebuild the rows from the log
into an empty set that equals the live one. Run it with
`cargo run -p photon-evt --example todo`. The
[examples guide](https://github.com/photon-grove/evt/blob/main/docs/guide/examples.md) lists every
Go and Rust example.

The `conformance` feature adds the backend conformance suite that this crate runs against its own
backends:

```rust,ignore
photon_evt::conformance::Suite::new(&my_repository).run().await;
photon_evt::conformance::outcome_cases(&my_repository, &second_handle).await;
// For a backend that implements `RebuildFence` (and reads `clock` for its leases):
photon_evt::conformance::RebuildSuite::new(&my_repository, &clock).run().await;
// For a backend that implements `EventLog`:
photon_evt::conformance::enumeration_cases(&my_repository).await;
```

## Compatibility with Go

- Keep payload numbers integral (cents, for example), because Go and Rust format some floats differently.
- Declare payload and aggregate fields in the same order as the Go structs, and use `BTreeMap` for
  maps, so the JSON bytes match. Go sorts map keys as strings, so a map with integer keys needs
  `#[serde(serialize_with = "photon_evt::json::go_key_order")]`.
- Use `photon_evt::json::to_vec` for any JSON you want byte-identical to Go's `json.Marshal`.
- Go re-encodes a `time.Time` field in its own RFC 3339 form (`05.100Z` becomes `05.1Z`). If a Go
  struct parses a timestamp that a Rust struct keeps as a string, store timestamps in Go's form.
- Rust refuses to decode an event at a version it does not declare. Go decodes it anyway. Deploy
  readers before writers when you bump an event version.

The known differences are listed in the
[storage format](https://github.com/photon-grove/evt/blob/main/docs/reference/storage-format.md#known-differences).

The [feature parity matrix](https://github.com/photon-grove/evt/blob/main/docs/reference/feature-matrix.md) tracks what each language
implements. Core changes land in Go and Rust together. Extensions such as the journal and the Lambda
toolkit land in Rust first, and Go ports them from the spec and fixtures. The reasoning is in
[ADR 0004](https://github.com/photon-grove/evt/blob/main/docs/adr/0004-core-reshape-multi-stream-transactions-and-outcomes.md) (multi-stream
transactions, command outcomes and fences),
[ADR 0005](https://github.com/photon-grove/evt/blob/main/docs/adr/0005-rust-first-extensions-journal-and-lambda.md) (Rust-first extensions) and
[ADR 0006](https://github.com/photon-grove/evt/blob/main/docs/adr/0006-parity-policy-and-feature-cuts.md) (the parity policy).

## Features and Rust version

No feature is on by default. `dynamodb` adds the DynamoDB event log through the AWS SDK, and you bring
your own `aws_sdk_dynamodb::Client`. `postgres` adds the PostgreSQL event log through
`tokio-postgres` and a deadpool pool. Both run on Tokio. `journal` adds the sync journal and lets
this build write to a table that declares it, while a build without it refuses such a table.
`conformance` adds the backend conformance suite, and `testing` adds the given/when/then helpers
(it turns on `conformance`, whose fixtures back its examples).

The minimum Rust version is 1.88 for the core crate and the `postgres`, `conformance`, and `journal` features. CI checks this.

The `dynamodb` feature follows the AWS SDK's minimum Rust version, which rises over time. On Rust
1.84 and later, Cargo's MSRV-aware resolver (the default for edition 2024, or `resolver = "3"`)
picks an SDK release that supports your `rust-version`. `aws-sdk-dynamodb` 1.104 is the latest
release that supports 1.88.

## License

Apache-2.0.
