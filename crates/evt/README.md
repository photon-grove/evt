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
- **Stream heads (ADR 0007).** Every stream has one small head item (DynamoDB `sk = -1`, or a row
  in `{events}__evt_stream_heads` on PostgreSQL) recording the highest event sequence committed to
  it. A commit raises the head with an `Update` conditioned on the head it saw, and a read fence is
  one check on it, so appends no longer carry a continuation or floor check and fences cost one
  action instead of two. A stream whose head disagrees with its events or snapshot fails with
  `Error::StreamHeadDrift`, which is never retried and never recorded as a rejection. Every writer
  requires the `stream-heads` feature in the marker: a table written before stream heads is
  refused with `Error::StreamHeadsNotMigrated` until `evt-doctor migrate-heads` has backfilled it
  and declared the feature (`FeatureMarker::declare_stream_heads` is the hook for that call), and
  a fresh table is declared by its first writer. DynamoDB scanners skip the head item (`sk < 1`),
  and a Streams consumer must skip its records (see the storage format spec).
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
(at most 512 UTF-8 bytes, no U+0000, which PostgreSQL cannot store; the same goes for digests and
rejection codes, messages, and details) and a type that implements `OutcomeCommand`, and `Aggregate::Error`
implements `ToRejection`:

- A repeat of a command (same ID, same identity digest) returns the stored outcome and does not
  run the handler or write. The dedupe is global, so it survives snapshots and compaction. The
  digest is a SHA-256 over the command's type, the target streams the request names, canonical
  payload JSON, and principal (the caller's intent, never the streams the command ends up writing), and `OutcomeConfig::with_digester` replaces it. The same ID with another digest is
  `Error::IdempotencyKeyReuse`, carrying the stored `prior_outcome`; it is never stored.
- An error from `handle` that `ToRejection` maps to `Some` is a domain rejection: it is persisted
  as a `rejected` outcome (`Rejection { code, message, details }`) and a retried command replays it
  verbatim. A `None` is an internal failure the handler could not decide: it is returned as
  `Error::Handler` and records nothing, like every other internal failure, so a retry after the fix
  starts clean.
- A rejection can carry events. A proposal whose apply finds its base revision stale should answer
  the client with a rejection *and* leave an `apply-failed` event on the proposal. Implement
  `Aggregate::rejection_events(&self, error)` to return those events for the errors that map to a
  rejection (the default is none): they commit in the same atomic transaction as the rejected row,
  which lists them, and a replay returns the stored rejection without writing them again. They are
  applied, snapshotted, and projected like an accepted command's events. In a transaction, stage
  the events with `append` and return `Decision::reject_with_events` (or call
  `Transaction::reject_with_events`); `Decision::reject` and `Transaction::reject` still discard
  what was staged. Every rule of an accepted commit applies (continuity, fences, reserved streams,
  the commit budget). A conflict re-prepares the command, which may then decide differently; when
  attempts run out, the `stale-fence` rejection is recorded without these events. A rejection
  stores no response.
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
  `Error::Backend`. `Repository::backoff` is the timer recovery polling sleeps on and `Repository::deadline` the timer
  that bounds each recovery read (override both if your runtime has one; a backend with no timer must
  bound its own reads). Decoded outcome rows must pass `Outcome::validate_stored`.
- `Store::transaction` records multi-stream commands with `Transaction::begin_command`, which must
  be the first call on the transaction (`Error::CommandOrder` after any read, append, or fence; a
  replayed result finishes the transaction), then
  `commit_with_outcome` or `reject`, attempting once. A failed caller-stamped fence persists a
  `stale-fence` rejection instead of retrying; the remedy is a new command ID.
- **Fence-aware retry (C5).** `execute_with_outcome`, `execute_with_response`, and
  `Store::execute_transaction` retry a command that conflicts with another writer, up to the
  store's `RetryPolicy` (`OutcomeConfig::with_retry`; the default is 5 attempts, 25 ms doubling to
  1 s, jittered, waiting on `Repository::backoff`; `RetryPolicy::immediate` opts out of waiting for
  tests and `with_deadline` bounds the whole command; with the defaults the waits total about
  0.4 s, but an ambiguous write that never resolves costs a recovery window per attempt). A conflict on anything the engine can
  re-derive (an event put, a snapshot or continuity check, an engine-filled fence)
  **re-prepares** the command: the aggregate is loaded again, the handler (or your `prepare`
  closure) runs again against fresh state, and only the attempt that commits writes anything. A
  failed **caller-stamped** fence fails fast with the `stale-fence` rejection instead, because the
  engine cannot know your decision still holds. When the last attempt also conflicts, the engine
  persists `Rejection::retries_exhausted(attempts)` (code `stale-fence`) as the command's outcome,
  so a replay of the ID gets the same answer; submit a new command ID to try again. Handlers and
  `prepare` closures may therefore run more than once and must be safe to repeat, and a command
  passed to `execute_with_outcome` must be `Clone`. A transient failure that is not a conflict
  (throttling, an ambiguous write that never resolved) is retried the same way but returned as an
  error when attempts run out.

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

A multi-stream command runs under the same retry with `Store::execute_transaction`: the closure is
the command's decision step, run again from scratch whenever the engine re-prepares it, and the
streams it reads and writes can depend on what it reads. The identity names only what the request
names (`with_targets`), so the digest is the same on every attempt however the write set changes.
The flip side is a contract: **everything that distinguishes the request must be in the targets or
the payload.** A transfer whose destination is chosen only inside the closure has the same identity
whichever destination it picks, so a second request with the same command ID and a different
destination replays the first's outcome instead of running; put the destination in the payload (or
the targets):

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
`source()`; it is never retried and records no outcome. Keys over 2,048 bytes (`pk`) or 1,024 bytes
(`sk`), a repeated key, and operations with no event are refused before any write.

### Row owners

Every operation names an **owner**: the stream the row belongs to, which is stored as the row's
`entityID` attribute. The owner must be a stream the commit wrote. Every write to a row is
conditioned on its owner, so a stream never overwrites or deletes a row another stream owns: the
operation fails with `Error::ProjectorOperation` (reason `ConditionalCheckFailed`), and nothing of
the commit is written. A row that does not exist yet can be written by anyone.

**A row's owner must never change.** Never delete a row to hand its key to another stream: keep the
row and rewrite it, as its owner, with a tombstone payload. A rebuild orders the writes to a row by
its owner's sequence, the only commit order the log keeps, and `replay_views` refuses a row that
two streams write (`Error::SharedViewKey`).

**A PostgreSQL view writer must enforce the owner in SQL**; the engine cannot. The statements it
returns must have these semantics (see `PostgresRepository::with_view_writer`):

- a put with `if_absent` is a plain `INSERT` (no `ON CONFLICT`), and the statement names the
  table's `(pk, sk)` key constraint with `PgStatement::with_key_constraint`: only a violation of
  that constraint is a failed `if_absent` condition;
- any other put is an upsert that only the owner may overwrite:
  `INSERT ... ON CONFLICT (pk, sk) DO UPDATE SET ... WHERE <table>.entity_id = EXCLUDED.entity_id`.
  A put that affects no row (the row is another stream's) fails its condition;
- a delete with `if_present` removes the owner's row only:
  `DELETE ... WHERE pk = $1 AND sk = $2 AND entity_id = $3`, and a delete that affects no row fails
  its condition;
- a delete without a condition succeeds on an absent row but refuses another stream's row, so its
  statement reports the foreign rows it found rather than the rows it deleted:
  `WITH gone AS (DELETE ... WHERE pk = $1 AND sk = $2 AND entity_id = $3) SELECT 1 FROM <table>
  WHERE pk = $1 AND sk = $2 AND entity_id <> $3`. An affected-row count above zero fails its
  condition (a plain `DELETE` would silently do nothing to another stream's row).

### Current-state and history-dependent projectors

A projector says which entity types it reads the *events* of with `history()`. The default, none,
is a **current-state** projector: it is given an empty commit ID and streams without events, and
the state of every stream, so a rebuild reproduces its rows from the final states alone. One that
names types is **history-dependent**: it is given the commit ID, and the events and state of the
streams of those types; every other stream arrives without events and without state. A
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
X1; Rust only, Go refuses a table that declares it). A **scope** is a named, independently ordered
sequence (a world, a place in it). You map each event of the journaled entity types to zero or more
scopes with a pure function; the event takes the next **position** in each, and a client polls a
scope with `high_watermark` and `page`.

```rust,ignore
use photon_evt::journal::{Journal, JournalRepository, Scope};

// Each event goes to its world's scope and to the scope of the place it happened in.
let journal = Journal::new(["match"], |event: &SerializedEvent| {
    let (world, place) = locate(event); // a pure function of the stored event
    vec![Scope::new(format!("world:{world}"))?, Scope::new(format!("world:{world}/place:{place}"))?]
})?
.with_max_row_bytes(30_000) // the default
.with_retry(RetryPolicy::new(5)); // the default

// Wrap a repository that writes views (in memory, or DynamoDB with a views table).
let repository = JournalRepository::new(
    DynamoRepository::new(client, "event-log").with_views_table("entity-views"),
    journal.clone(),
);
let store = Store::<Match, _>::new(&repository, journal.clone());

// A poller keeps the last position it saw per scope.
let scope = Scope::new("world:7")?;
let mut after = 0;
loop {
    let page = repository.page(&scope, after, 100).await?;
    // ... deliver page.rows (each row.event is the stored event) ...
    after = page.next_after();

    if let Some(gap) = page.gap {
        // Positions gap.from..=gap.to are missing: retry the page, escalate if it persists.
        break;
    }
    if !page.has_more() { break } // caught up with repository.high_watermark(&scope)
}
```

Behind that call, every commit that writes a journaled event reads the scopes' counters, allocates
positions, stores them in the event's metadata (`evt.journal`), and adds the journal rows and the
counters to the commit's view operations, so they are atomic with the events. Because the
allocations are on the events, `journal::replay` and `journal::rebuild` rebuild the rows and counters
from stored events in any order. The journaled entity types are declared history-dependent in the
feature marker with the `journal` feature (`journal: 1`), so they are never compacted and builds
without the feature refuse the table.

- **Row-size guard.** A row over the cap (default 30,000 bytes of stored payload, at most 370,000)
  fails the commit before any write with `Error::JournalRowTooLarge`; a store with outcomes records
  that as the command's `journal-row-too-large` rejection. `Journal::pre_check` (or
  `pre_check_events` for typed events) measures the same rows without a counter, at the widest
  position. It is exact for the metadata you give it and advisory otherwise (a handler does not see
  the commit's principal, extensions, or command ID), so pass the commit's metadata or add
  `with_pre_check_headroom`. Call it in your command handler and turn the error into a recorded
  rejection with `RowTooLarge::to_rejection`.
- **Contention.** Writers racing for a scope lose positions to each other. The journal re-reads the
  counters and allocates again under its `RetryPolicy` (and retries transient failures); when
  attempts run out on taken positions it returns `Error::Conflict`. A conflict on the stream itself
  is yours to re-prepare. A hot scope serializes on its counter item. A counter that falls behind its
  rows (a restore, a rebuild beside writers) is healed from the highest row.
- **A cursor ahead of the journal.** `page` flags `cursor_ahead` when `after` is above the high
  watermark (after a restore, say); the page is not caught up, and the consumer should resync.
- **Every writer of the table must commit through a `JournalRepository`.** The marker stops builds
  without the feature, not a writer that bypasses the journal, and the `journal` cargo feature is
  build-wide, so a build that enables it accepts a journal table through every repository,
  wrapped or not; a bypassing writer's events would be missing from the scopes. Enable the journal
  before writing the events it covers: earlier events have no row and no gap shows it. A commit's
  journal rows and counters count toward the 100-action transaction budget.
- **Rebuilds** (`journal::rebuild`) rewrite rows and counters unconditionally and take a
  `WritersQuiesced` acknowledgement: do not run one beside live writers.
- **Backends.** DynamoDB and the in-memory repository. The Rust PostgreSQL repository does not write
  commit-wide views yet.

The stored shapes are in the [storage format](https://github.com/photon-grove/evt/blob/main/docs/spec/storage-format.md#journal),
and `photon_evt::conformance::journal_cases` (features `journal` and `conformance`) checks a backend.

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

## Reading and writing views

`ViewRepository` is the Rust counterpart of Go's `evt.ViewRepository`: it reads and writes the rows
of the views table outside the commit path, with the same five attributes commit-wide view
operations write (the owner stream is the row's `entityID`) plus an optional `ttl`. `MemRepository`
implements it over the rows commits write, and `DynamoViewRepository` (feature `dynamodb`) over
the views table, which needs a string `pk` and `sk` and the `entityType-index` index on
`entityType`. `DynamoRepository::view_repository` builds one for the table
`with_views_table` configured.

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
page can come back empty with a cursor on DynamoDB; keep going until the cursor is `None`.

`ViewCodec<T, R>` (Go's `viewstore.Codec`, `viewstore.Single`, and the `GetJSONView` family) binds a
repository, an entity type and a serde type. It writes `T` as the payload with `json::to_vec`, so
the bytes are Go's `json.Marshal` of the same shape, and reads it back; `with_decoder` stands in
for Go's factory when a value needs constructor-injected dependencies.

```rust,ignore
use photon_evt::ViewCodec;

let profiles = ViewCodec::<Profile, _>::new(&views, "profile");

profiles.put_at("profile#ada", "ada", &profile).await?;
let ada: Option<Profile> = profiles.get("profile#ada").await?;

// Lists skip a payload that does not decode and report it; the _strict forms fail instead.
let all = profiles.list_all(|row, err| eprintln!("skipped {}/{}: {err}", row.pk, row.sk)).await?;
```

Where Go's decoder differs from serde's, serde wins: a JSON `null` does not decode into a struct
and field names match exactly, so mark fields `#[serde(default)]` that older or Go-written rows may
omit. Views are projections: replaying the events must be able to rebuild every row, so keep the
mutable state behind these writes in events.

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

Rust 1.88 for the core crate and the `postgres`, `conformance`, and `journal` features. CI checks this.

The `dynamodb` feature follows the AWS SDK's minimum Rust version, which rises over time. On Rust
1.84 and later, Cargo's MSRV-aware resolver (the default for edition 2024, or `resolver = "3"`)
picks an SDK release that supports your `rust-version`. `aws-sdk-dynamodb` 1.104 is the latest
release that supports 1.88.

## License

Apache-2.0.
