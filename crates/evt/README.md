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
- Upcasters migrate old payload versions on read. `compact_below` deletes only events a snapshot
  covers.
- In-band projectors write views in the same transaction as the events.

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
the events may or may not have been stored. Reload before deciding what to do, and set a command
ID so running the command again is safe.

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
`ensure_schema` runs the same idempotent DDL as Go's `EnsureSchema`.

```rust,ignore
use photon_evt::postgres::PostgresRepository;

let repository = PostgresRepository::new(pool);
repository.ensure_schema().await?;

let store = Store::<Account, _>::new(repository, ());
```

## Testing a backend

The `conformance` feature adds the backend conformance suite that this crate runs against its own
backends:

```rust,ignore
photon_evt::conformance::Suite::new(&my_repository).run().await;
```

## Compatibility with Go

- Keep payload numbers integral (for example, cents): Go and Rust format some floats differently.
- Declare payload and aggregate fields in the same order as the Go structs, and use `BTreeMap` for
  maps, so the JSON bytes match.
- Use `photon_evt::json::to_vec` for any JSON you want byte-identical to Go's `json.Marshal`.
- Rust refuses to decode an event at a version it does not declare. Go decodes it anyway. Deploy
  readers before writers when you bump an event version.

The known differences are listed in the
[storage format](https://github.com/photon-grove/evt/blob/main/docs/spec/storage-format.md#known-differences).

## Minimum Rust version

Rust 1.88.

## License

Apache-2.0.
