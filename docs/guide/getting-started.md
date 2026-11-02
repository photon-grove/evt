# Getting started

This guide builds a bank account aggregate in Rust, tests it against the in-memory backend, makes
retries safe, and moves it to DynamoDB or PostgreSQL without changing the aggregate. The same model
in Go and in TypeScript comes after. Every Rust snippet here is copied from
[`crates/evt/examples/getting_started.rs`](https://github.com/photon-grove/evt/blob/main/crates/evt/examples/getting_started.rs),
which CI builds and tests, and a test fails if the two drift apart. From a checkout of this
repository, run the whole program with:

```sh
cargo run -p photon-evt --example getting_started --features testing
```

## Install

```sh
cargo add photon-evt
cargo add serde --features derive
cargo add thiserror
cargo add tokio --features macros,rt-multi-thread
cargo add photon-evt --dev --features testing
```

The default build is the core plus the in-memory backend, with no cloud dependencies, so unit tests
stay fast and offline. The `dynamodb` and `postgres` features add the durable backends (step 6),
and `testing` adds the given/when/then helpers (step 4).

## 1. Declare the events

The snippets in this guide share these imports:

```rust
use photon_evt::{
    Aggregate, BoxError, ExecuteError, MemRepository, Metadata, OutcomeCommand, OutcomeConfig,
    Rejection, Store, ToRejection, canonical_json, domain_events,
};
use serde::{Deserialize, Serialize};
```

Events are plain `serde` structs. `domain_events!` gathers them into one enum and gives each a type
string and a version, which is how a stored event finds its way back to a Rust type on replay.

```rust
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountOpened {
    pub account_id: String,
    pub balance: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoneyDeposited {
    pub account_id: String,
    pub amount: i64,
}

domain_events! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum AccountEvent {
        Opened(AccountOpened) => ("bank_account.opened", 1),
        Deposited(MoneyDeposited) => ("bank_account.money_deposited", 1),
    }
}
```

The type string, the version and the payload's JSON fields are your durable contract. Start every
event at version `1`, and read about [schema evolution](concepts.md#upcaster) before you change a
payload's shape.

## 2. Write the aggregate

An aggregate is the state that commands act on. It names its command, event and error types, and
it can take shared services (a clock, a client) through `Services`. The state derives `Default`,
because an entity with no events loads as the default, and `serde`, because snapshots store it.
`#[serde(default)]` lets a snapshot written before you added a field still load. It also means a
renamed field loads as its default instead of failing, so treat the state's field names as part of
the contract too.

```rust
#[derive(Clone, Debug, PartialEq, Serialize)]
pub enum AccountCommand {
    Open {
        account_id: String,
        initial_balance: i64,
    },
    Deposit {
        amount: i64,
    },
}

#[derive(Debug, PartialEq, thiserror::Error)]
pub enum AccountError {
    #[error("account already opened")]
    AlreadyOpened,
    #[error("account not opened")]
    NotOpened,
    #[error("amount must be positive")]
    InvalidAmount,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Account {
    pub id: String,
    pub balance: i64,
    pub opened: bool,
}
```

`handle` validates a command against the current state and returns the events it decides on. It
never changes the aggregate. `apply` changes the state and never validates, because the event
already happened. Keep each side to its job.

```rust
impl Aggregate for Account {
    const TYPE: &'static str = "bank_account";

    type Command = AccountCommand;
    type Event = AccountEvent;
    type Error = AccountError;
    type Services = ();

    async fn handle(
        &self,
        command: AccountCommand,
        _: &(),
    ) -> Result<Vec<AccountEvent>, AccountError> {
        match command {
            AccountCommand::Open {
                account_id,
                initial_balance,
            } => {
                if self.opened {
                    return Err(AccountError::AlreadyOpened);
                }

                Ok(vec![
                    AccountOpened {
                        account_id,
                        balance: initial_balance,
                    }
                    .into(),
                ])
            }

            AccountCommand::Deposit { amount } => {
                if !self.opened {
                    return Err(AccountError::NotOpened);
                }
                if amount <= 0 {
                    return Err(AccountError::InvalidAmount);
                }

                let account_id = self.id.clone();

                Ok(vec![MoneyDeposited { account_id, amount }.into()])
            }
        }
    }

    fn apply(&mut self, event: AccountEvent) {
        match event {
            AccountEvent::Opened(e) => {
                self.id = e.account_id;
                self.balance = e.balance;
                self.opened = true;
            }
            AccountEvent::Deposited(e) => self.balance += e.amount,
        }
    }
}
```

## 3. Run it in memory

A `Store` puts the aggregate on a repository. `execute` loads the stream, runs `handle`, and commits
the events in one conditional write on the stream's head, so a writer that raced you gets a
conflict instead of a silent overwrite.

```rust
fn open(account_id: &str, initial_balance: i64) -> AccountCommand {
    AccountCommand::Open {
        account_id: account_id.to_owned(),
        initial_balance,
    }
}

async fn open_and_deposit() -> Result<(), Box<dyn std::error::Error>> {
    let store = Store::<Account, _>::new(MemRepository::new(), ());

    store
        .execute("acct-1", open("acct-1", 100), Metadata::default())
        .await?;
    store
        .execute(
            "acct-1",
            AccountCommand::Deposit { amount: 25 },
            Metadata::default(),
        )
        .await?;

    // Each load folds the stream (from its latest snapshot, if any) into a fresh Account.
    let loaded = store.load("acct-1").await?;
    assert_eq!(loaded.aggregate().balance, 125);

    // A rejected command commits nothing and returns the aggregate's error.
    let rejected = store
        .execute(
            "acct-1",
            AccountCommand::Deposit { amount: 0 },
            Metadata::default(),
        )
        .await;
    assert!(matches!(
        rejected,
        Err(ExecuteError::Rejected(AccountError::InvalidAmount))
    ));

    Ok(())
}
```

`execute` returns `ExecuteError::Rejected` when the aggregate refused the command and
`ExecuteError::Store` when loading or committing failed. `is_conflict()` and `is_retryable()` tell
the store errors apart, and `execute_with_retry` reloads and re-handles a command after a conflict.
The store writes a snapshot every five events by default (`with_snapshot_interval` changes that),
so a load reads the latest snapshot plus the tail instead of the whole stream.

## 4. Test it with given/when/then

The `testing` feature adds `Scenario`: seed a stream with events, run one command through a real
`Store` over `MemRepository`, and assert the events, the resulting state, or the rejection. Keep
these tests under `#[cfg(test)]`, since the Install step enables `testing` only for dev builds.

```rust
#[cfg(test)]
mod tests {
    use photon_evt::testing::Scenario;

    use super::*;

    fn opened(account_id: &str, balance: i64) -> AccountEvent {
        AccountOpened {
            account_id: account_id.to_owned(),
            balance,
        }
        .into()
    }

    #[tokio::test]
    async fn opening_emits_an_opened_event() {
        Scenario::<Account>::given([])
            .when(open("acct-1", 100))
            .then_events([opened("acct-1", 100)])
            .await;
    }

    #[tokio::test]
    async fn depositing_adds_to_the_balance() {
        Scenario::<Account>::given([opened("acct-1", 100)])
            .when(AccountCommand::Deposit { amount: 25 })
            .then_state(|account| assert_eq!(account.balance, 125))
            .await;
    }

    #[tokio::test]
    async fn depositing_before_opening_is_rejected() {
        Scenario::<Account>::given([])
            .when(AccountCommand::Deposit { amount: 10 })
            .then_rejected(|error| *error == AccountError::NotOpened)
            .await;
    }
}
```

`Fixture` is the multi-stream version, for tests of transactions that touch several aggregates.
The [`photon-evt` README](https://github.com/photon-grove/evt/blob/main/crates/evt/README.md#testing)
covers both.

## 5. Make retries safe with command outcomes

A client that times out will send the same command again. Put a command ID in `Metadata` (a request
ID, job ID or message ID) and plain `execute` reports a repeat as
`ExecuteError::Store(Error::DuplicateCommand)` (`is_duplicate_command()`), meaning the command
already ran. It only sees command IDs recorded since the stream's latest snapshot, though. Command
outcomes make the check durable. Turn them on with `with_outcomes`, and every command gets a stored
outcome row, written in the same commit as its events.

The store needs two things from your types. `OutcomeCommand` names the command and gives its
canonical payload. Together with the entity ID and the caller's principal, they identify the
request. Command IDs are global, so reusing one for any other request, on this account or another,
fails with `Error::IdempotencyKeyReuse`. `ToRejection` decides which errors are stored as
rejections. Return `None` for an internal failure that shouldn't be recorded.

```rust
impl OutcomeCommand for AccountCommand {
    fn command_type(&self) -> String {
        match self {
            AccountCommand::Open { .. } => "bank_account.open".into(),
            AccountCommand::Deposit { .. } => "bank_account.deposit".into(),
        }
    }

    fn canonical_payload(&self) -> Result<Option<Vec<u8>>, BoxError> {
        Ok(Some(canonical_json(self)?))
    }
}

impl ToRejection for AccountError {
    fn to_rejection(&self) -> Option<Rejection> {
        let code = match self {
            AccountError::AlreadyOpened => "already-opened",
            AccountError::NotOpened => "not-opened",
            AccountError::InvalidAmount => "invalid-amount",
        };

        Some(Rejection::new(code, self.to_string()))
    }
}
```

Then call `execute_with_outcome` with a command ID on every command. A store with outcomes enabled
refuses the plain `execute`, so no command can skip its outcome.

```rust
async fn deposit_with_outcomes() -> Result<(), Box<dyn std::error::Error>> {
    let store =
        Store::<Account, _>::new(MemRepository::new(), ()).with_outcomes(OutcomeConfig::new());

    let metadata = Metadata::default().with_command_id("open-acct-1");
    store
        .execute_with_outcome("acct-1", open("acct-1", 100), metadata)
        .await?;

    let deposit = AccountCommand::Deposit { amount: 25 };
    let metadata = Metadata::default().with_command_id("deposit-7f3a");
    let first = store
        .execute_with_outcome("acct-1", deposit.clone(), metadata.clone())
        .await?;
    assert!(first.is_accepted() && !first.replayed);

    // The retry returns the stored outcome. The handler doesn't run, so nothing is credited twice.
    let retry = store
        .execute_with_outcome("acct-1", deposit, metadata)
        .await?;
    assert!(retry.replayed);
    assert_eq!(store.load("acct-1").await?.aggregate().balance, 125);

    // A rejection is stored too, so a retry of this command ID gets the same answer.
    let metadata = Metadata::default().with_command_id("deposit-zero");
    let rejected = store
        .execute_with_outcome("acct-1", AccountCommand::Deposit { amount: 0 }, metadata)
        .await?;
    assert_eq!(
        rejected.rejection().map(|r| r.code.as_str()),
        Some("invalid-amount")
    );

    Ok(())
}
```

[Command outcomes and retries](commands.md) covers stored responses, multi-stream commands with
`execute_transaction`, and what happens when two retries race.

## 6. Move it to DynamoDB or PostgreSQL

The aggregate doesn't change. Swap the repository behind the store. For DynamoDB, enable the
`dynamodb` feature and bring your own client:

```sh
cargo add photon-evt --features dynamodb
cargo add aws-config aws-sdk-dynamodb
```

A new table is initialized once, before any writer starts, by a deploy step with a role that may
Scan it. You can call `initialize` from that step, or run `evt-doctor init` from the Go CLI:

```sh
go install github.com/photon-grove/evt/go/cmd/evt-doctor@latest
```

```rust
async fn initialize_table(client: aws_sdk_dynamodb::Client) -> Result<(), photon_evt::Error> {
    use photon_evt::dynamodb::DynamoRepository;

    // A deploy step: run it once, before any writer starts, with a role that may Scan the table.
    DynamoRepository::new(client, "event-log")
        .initialize()
        .await?;

    Ok(())
}
```

Writers refuse an uninitialized table instead of scanning it, so their roles need no
`dynamodb:Scan`. For tests and a local emulator, `with_lazy_initialization(true)` lets the first
write initialize the table instead.

```rust
async fn open_on_dynamodb(client: aws_sdk_dynamodb::Client) -> Result<(), photon_evt::Error> {
    use photon_evt::dynamodb::DynamoRepository;

    // A writer never scans, so its role needs no dynamodb:Scan.
    let repository = DynamoRepository::new(client, "event-log");
    let store = Store::<Account, _>::new(repository, ())
        .with_snapshot_interval(25)
        .with_outcomes(OutcomeConfig::new());

    let metadata = Metadata::default().with_command_id("open-acct-1");
    store
        .execute_with_outcome("acct-1", open("acct-1", 100), metadata)
        .await?;

    Ok(())
}
```

The table has a string partition key `pk` and a number sort key `sk`, and the
[DynamoDB guide](dynamodb.md) describes it and the views table.

For PostgreSQL, enable the `postgres` feature and pass a `deadpool_postgres::Pool` from the same
`deadpool-postgres` release that `photon-evt` uses:

```sh
cargo add photon-evt --features postgres
cargo add deadpool-postgres@0.14
```

`ensure_schema` creates the tables if they don't exist. It needs DDL rights, so run it from a deploy
or startup step rather than in every writer.

```rust
async fn prepare_database(pool: deadpool_postgres::Pool) -> Result<(), photon_evt::Error> {
    use photon_evt::postgres::PostgresRepository;

    // A deploy or startup step: it creates the tables if they don't exist, so it needs DDL rights.
    PostgresRepository::new(pool).ensure_schema().await
}
```

A writer on a fresh database declares the storage features on its first write.

```rust
async fn open_on_postgres(pool: deadpool_postgres::Pool) -> Result<(), photon_evt::Error> {
    use photon_evt::postgres::PostgresRepository;

    // On a fresh database, the first write declares the storage features itself.
    let repository = PostgresRepository::new(pool);
    let store = Store::<Account, _>::new(repository, ()).with_outcomes(OutcomeConfig::new());

    let metadata = Metadata::default().with_command_id("open-acct-1");
    store
        .execute_with_outcome("acct-1", open("acct-1", 100), metadata)
        .await?;

    Ok(())
}
```

The [PostgreSQL guide](postgres.md) has the schema and local setup.

## Project views deliberately

Views are derived from the log, so record anything that can't be rebuilt, like a human decision or
a publish flag, as an event first ([keeping the log](concepts.md#keeping-the-log) says why).
[Projections and rebuilds](projections.md) covers how to write view rows in the same commit as the
events, and what can rebuild them today.

## The same model in Go

The Go module reads and writes the same storage format, and the complete runnable Go version lives
in [`go/examples/banking`](https://github.com/photon-grove/evt/tree/main/go/examples/banking). It's
a separate model from the Rust one above, not a drop-in partner on the same streams. For a Rust and
a Go service to share a stream, the events need the same type strings, versions and payload JSON,
and the aggregate state needs the same JSON too, because both languages read each other's
snapshots. Command outcomes also need the same command type and canonical payload for a retry to
match across languages. The
[compatibility notes](https://github.com/photon-grove/evt/blob/main/crates/evt/README.md#compatibility-with-go)
list what to line up.

### Install the Go module

```sh
go get github.com/photon-grove/evt/go
```

The core `evt` and `mem` packages have no cloud dependencies, so unit tests stay fast and offline.
The AWS SDK comes in only through `backends/dynamo`, and the `stream` and `projectors` packages
pull in the AWS Lambda event types.

### Define an aggregate

An aggregate implements [`evt.Aggregate`](concepts.md#aggregate). It does three jobs. It handles
commands into new events, applies events to its own state, and deserializes historical events back
into current Go types.

```go
const entityType evt.EntityType = "bank_account"

type Account struct {
    ID      evt.EntityID
    Balance int
    Opened  bool
}

func NewAccount(id evt.EntityID) *Account {
    return &Account{ID: id}
}

func (a *Account) Type() evt.EntityType { return entityType }
func (a *Account) GetID() evt.EntityID  { return a.ID }
```

Keep command validation and invariant checks in `Handle`, and keep state mutation in `Apply`.
`Handle` never mutates the aggregate. It only decides which facts to record. `Apply` never
validates, because it trusts that the fact already happened.

```go
func (a *Account) Handle(_ context.Context, command evt.Command) (evt.CommandResult, error) {
    switch cmd := command.(type) {
    case *OpenAccount:
        if a.Opened {
            return evt.CommandResult{}, evt.NewConflictError("account already opened")
        }
        return evt.CommandResult{Events: []evt.Event{AccountOpened{
            AccountID: cmd.AccountID,
            Balance:   cmd.InitialBalance,
            At:        time.Now().UTC(),
        }}}, nil

    case *Deposit:
        if !a.Opened {
            return evt.CommandResult{}, evt.NewNotFoundError("account not opened")
        }
        return evt.CommandResult{Events: []evt.Event{MoneyDeposited{
            AccountID: cmd.AccountID,
            Amount:    cmd.Amount,
            At:        time.Now().UTC(),
        }}}, nil

    default:
        return evt.CommandResult{}, evt.NewBadCommandError(command)
    }
}

func (a *Account) Apply(event evt.Event) error {
    switch e := event.(type) {
    case AccountOpened:
        a.ID, a.Balance, a.Opened = e.AccountID, e.Balance, true
    case MoneyDeposited:
        a.Balance += e.Amount
    default:
        return evt.NewBadEventError(event)
    }
    return nil
}
```

`DeserializeEvent` is how stored JSON becomes a typed event again on replay. It keys off the event's
`Type` string, so that string is part of your durable contract. Read about
[schema evolution](concepts.md#upcaster) before you ever change it.

```go
func (a *Account) DeserializeEvent(s evt.SerializedEvent) (evt.Event, error) {
    switch s.Type {
    case "bank_account.opened":
        var e AccountOpened
        return e, json.Unmarshal(s.Payload, &e)
    case "bank_account.money_deposited":
        var e MoneyDeposited
        return e, json.Unmarshal(s.Payload, &e)
    default:
        return nil, fmt.Errorf("unrecognized event: %s", s.Type)
    }
}

// No schema migrations or in-band projectors yet.
func (a *Account) EventUpcasters() []evt.EventUpcaster { return nil }
func (a *Account) Projectors() []evt.EventProjector    { return nil }
```

Commands implement [`evt.Command`](concepts.md#command) (`Type` and `EntityType`), and events
implement [`evt.Event`](concepts.md#event) (`Type`, `Version`, `EntityID` and `EntityType`). Start
every event at a `Version()` of `1`. That number is what lets you [upcast](concepts.md#upcaster) old
payloads later.

### Drive it with `mem` in a test

`mem.NewStore()` gives you the full [`evt.Store`](concepts.md#store) contract with no
infrastructure, so behavior tests stay fast. The store loads a fresh aggregate from the log on every
`Execute`, so give each command its own instance. Reusing an aggregate that already applied events
would apply them twice.

```go
func TestOpenThenDepositAndReplay(t *testing.T) {
    ctx := context.Background()
    store := mem.NewStore()

    account := NewAccount("acct-1")
    if err := store.Execute(ctx, account, "acct-1",
        &OpenAccount{AccountID: "acct-1", InitialBalance: 100}, evt.Metadata{}); err != nil {
        t.Fatalf("open: %v", err)
    }

    account = NewAccount("acct-1")
    if err := store.Execute(ctx, account, "acct-1",
        &Deposit{AccountID: "acct-1", Amount: 25}, evt.Metadata{}); err != nil {
        t.Fatalf("deposit: %v", err)
    }

    // Replay from the log into a fresh instance to rebuild the state.
    reloaded := NewAccount("acct-1")
    if _, err := store.LoadAggregate(ctx, reloaded, "acct-1"); err != nil {
        t.Fatalf("reload: %v", err)
    }
    if reloaded.Balance != 125 {
        t.Fatalf("balance = %d, want 125", reloaded.Balance)
    }
}
```

The error constructors round-trip through typed predicates, so tests assert on intent instead of
strings: `evt.IsNotFoundErr(err)`, `evt.IsConflictErr(err)` and `evt.IsDuplicateCommandErr(err)`.

### Make retries safe with metadata

[`evt.Metadata`](concepts.md#metadata) rides along with every command. It carries an optional
command ID, trace context, origin, caller identity (`PrincipalID`), causation (`CausationID`) and
application-specific `Extensions`. The command ID is what makes retries idempotent. Set it from a
request ID, job ID or message ID, and a duplicate attempt fails as a
[`DuplicateCommandError`](concepts.md#command) and appends nothing. For durable
idempotency that also stores the response, see [command outcomes](commands.md).

```go
md := evt.NewMetadata(ctx, nil,
    evt.WithCommandID("idempotency-key-123"),
    evt.WithPrincipalID("user:42"),
    evt.WithExtension("requestSource", "cli"),
)
err := store.Execute(ctx, account, "acct-1", &Deposit{AccountID: "acct-1", Amount: 25}, md)
```

This example leaves `CausationID` unset, so the stored `causationId` defaults to
`"idempotency-key-123"`. For a command triggered by another command, add
`evt.WithCausationID("upstream-command-456")` to override it. With neither a command ID nor a
causation ID, `causationId` encodes as `null`. An unset principal also encodes as `null`, and nil or
empty extensions encode as `{}`. Call `evt.WithExtension(key, value)` once for each
application-specific string entry.

Use `evt.WithTrace(ctx)` to carry an OpenTelemetry context along, and `evt.WithAddress(addr)` when
the caller has parsed a client address worth recording.

### Move the same model to DynamoDB

The aggregate does not change. Swap the store's backing repository:

```go
repo := dynamo.NewRepository(dynamoClient, "evt-local-event-log")
store := snapshots.NewStore(repo, 25) // write a snapshot every ~25 events
```

Initialize a new event-log table once, before its writers start, with `evt-doctor init` (or
`repo.Initialize(ctx)` in a deploy step). Writers refuse an uninitialized table instead of scanning
it, so their roles need no `dynamodb:Scan`. For a local emulator you can skip the step with
`.WithLazyInitialization(true)`, and the first write initializes the table itself.

`snapshots.NewStore` keeps replay fast by checkpointing aggregate state inline in the event log. So
loading a long-lived stream reads the latest snapshot plus the tail, not every event from sequence
1. You provision two tables, an event log and a views table, and the [DynamoDB guide](dynamodb.md)
describes both. Run them locally against the Moto emulator with the `infra/local` Terraform stack
before you touch real AWS.

The same swap works for PostgreSQL: `postgres.NewStore` (or `postgres.NewRepository` with
`snapshots.NewStore`) over a `pgxpool.Pool`. The aggregate still doesn't change. The
[PostgreSQL guide](postgres.md) has the schema and local setup.

## The same model in TypeScript

[`@photon-grove/evt`](https://github.com/photon-grove/evt/blob/main/packages/evt/README.md) is the
TypeScript implementation of the same storage format. So far it has the core, the in-memory backend
and the given/when/then helpers. The DynamoDB and PostgreSQL backends and command outcomes come in
later releases, and the [feature matrix](../reference/feature-matrix.md) tracks them. This section
builds the Rust bank account again, with the same event types and payload JSON. Every TypeScript
snippet is copied from
[`packages/evt/examples/getting-started.ts`](https://github.com/photon-grove/evt/blob/main/packages/evt/examples/getting-started.ts)
and its test file, which CI type-checks and runs.

### Install the package

```sh
npm install @photon-grove/evt
```

It's ESM only, with no runtime dependencies. CI runs it on Node 22.12 and later, Bun and Deno, and
it uses only web-standard globals (`TextEncoder`, Web Crypto, `AbortSignal`), so it doesn't need
Node's built-ins. CommonJS code on Node can still `require()` it.

### Declare the events and the aggregate

The snippets share these imports:

```ts
import {
  type EventOf,
  type Loaded,
  Store,
  defineAggregate,
  defineEvents,
  json,
} from '@photon-grove/evt'
import {MemRepository} from '@photon-grove/evt/memory'
```

`defineEvents` gives each event type a version and a codec. `json()` writes plain objects with
evt's own JSON writer, in property order, and it takes an optional
[Standard Schema](https://standardschema.dev) validator such as Zod or Valibot. `EventOf` is the
union of `{type, data}` objects the aggregate decides on and applies.

```ts
export const accountEvents = defineEvents({
  'bank_account.opened': {
    version: 1,
    codec: json<{accountId: string; balance: number}>(),
  },
  'bank_account.money_deposited': {
    version: 1,
    codec: json<{accountId: string; amount: number}>(),
  },
})

export type AccountEvent = EventOf<typeof accountEvents>
```

Commands are whatever your handler takes. The state is plain data, because snapshots store it as
JSON, and its field names are part of the contract, as they are in Rust.

```ts
export type AccountCommand =
  | {type: 'open'; accountId: string; initialBalance: number}
  | {type: 'deposit'; amount: number}

export interface Account {
  id: string
  balance: number
  opened: boolean
}
```

`defineAggregate` ties them together. `handle` decides on a command against the current state and
returns a decision: accept with events, or reject with a code and a message. `apply` folds one event
into a new state and never validates. Return a new object from `apply` rather than changing the one
you're given.

```ts
export const account = defineAggregate({
  type: 'bank_account',
  events: accountEvents,
  state: json<Account>(),
  initial: (): Account => ({id: '', balance: 0, opened: false}),

  handle(state, command: AccountCommand) {
    switch (command.type) {
      case 'open':
        if (state.opened) {
          return reject('already-opened', 'account already opened')
        }
        return accept({
          type: 'bank_account.opened',
          data: {accountId: command.accountId, balance: command.initialBalance},
        })

      case 'deposit':
        if (!state.opened) {
          return reject('not-opened', 'account not opened')
        }
        if (command.amount <= 0) {
          return reject('invalid-amount', 'amount must be positive')
        }
        return accept({
          type: 'bank_account.money_deposited',
          data: {accountId: state.id, amount: command.amount},
        })
    }
  },

  apply(state, event) {
    switch (event.type) {
      case 'bank_account.opened':
        return {id: event.data.accountId, balance: event.data.balance, opened: true}
      case 'bank_account.money_deposited':
        return {...state, balance: state.balance + event.data.amount}
    }
  },
})
```

The decisions are plain objects, so two small helpers keep `handle` readable:

```ts
function accept(...events: AccountEvent[]) {
  return {kind: 'accept', events} as const
}

function reject(code: string, message: string) {
  return {kind: 'reject', rejection: {code, message}} as const
}
```

### Run it in memory

A `Store` puts aggregates on a repository. `execute` loads the stream, runs `handle`, and commits
the events with a conditional write on the stream's head, so a writer that raced you gets a
`ConflictError` instead of a silent overwrite. It resolves to what the command did: `committed`,
`rejected` with the rejection, or `duplicate` when an event since the latest snapshot already
carries the metadata's command ID.

```ts
export async function openAndDeposit(): Promise<Loaded<Account>> {
  const store = new Store(new MemRepository())

  await store.execute(account, 'acct-1', {type: 'open', accountId: 'acct-1', initialBalance: 100})
  await store.execute(account, 'acct-1', {type: 'deposit', amount: 25})

  // A rejected command commits nothing, and its result carries the rejection.
  const result = await store.execute(account, 'acct-1', {type: 'deposit', amount: 0})
  if (result.kind !== 'rejected' || result.rejection.code !== 'invalid-amount') {
    throw new Error(`expected a rejection, got ${result.kind}`)
  }

  // Each load folds the stream (from its latest snapshot, if any) into a fresh state.
  return store.load(account, 'acct-1')
}
```

`executeWithRetry` runs a command again after a conflict, waiting a jittered backoff between
attempts. The store writes a snapshot every five events by default (the `snapshotInterval` option
changes that), so a load reads the latest snapshot plus the tail. Sequences are `bigint`s
(the loaded `sequence` is `2n` here), because Go and Rust store them as 64-bit integers.

### Test it with given/when/then

`@photon-grove/evt/testing` adds `scenario`: seed a stream with events, run one command through a
real `Store` over `MemRepository`, and assert the events, the state or the rejection. A failure
throws an `AssertionError` with the expected and actual values, so it works in Vitest, Jest,
`node:test` or any other runner.

```ts
import {scenario} from '@photon-grove/evt/testing'
import {expect, test} from 'vitest'

import {type AccountEvent, account} from './getting-started.ts'

function opened(accountId: string, balance: number): AccountEvent {
  return {type: 'bank_account.opened', data: {accountId, balance}}
}

test('opening emits an opened event', async () => {
  await scenario(account)
    .when({type: 'open', accountId: 'acct-1', initialBalance: 100})
    .thenEvents(opened('acct-1', 100))
})

test('depositing adds to the balance', async () => {
  await scenario(account)
    .given(opened('acct-1', 100))
    .when({type: 'deposit', amount: 25})
    .thenState((state) => {
      expect(state.balance).toBe(125)
    })
})

test('depositing before opening is rejected', async () => {
  await scenario(account).when({type: 'deposit', amount: 10}).thenRejected('not-opened')
})
```

`Fixture` seeds and reads several streams, for tests that span aggregates. The
[package README](https://github.com/photon-grove/evt/blob/main/packages/evt/README.md#testing)
covers both.

## Where to go next

[Concepts](concepts.md) covers the vocabulary and the contract behind each type. The
[DynamoDB](dynamodb.md) and [PostgreSQL](postgres.md) guides cover table shapes, snapshots,
compaction and local setup. [Projections and rebuilds](projections.md) is about building read
models and rebuilding them, [Command outcomes and retries](commands.md) is about idempotency, and
[Streams and projectors](streams.md) is about async fan-out on Lambda. The
[`photon-evt` README](https://github.com/photon-grove/evt/blob/main/crates/evt/README.md) covers
the rest of the Rust API: transactions, commit-wide projectors, the sync journal and Lambda
handlers. The
[`@photon-grove/evt` README](https://github.com/photon-grove/evt/blob/main/packages/evt/README.md)
does the same for TypeScript.
