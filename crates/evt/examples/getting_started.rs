//! The program `docs/guide/getting-started.md` builds, step by step: a bank account aggregate on
//! the in-memory backend, its given/when/then tests, the same aggregate behind durable command
//! outcomes, and the DynamoDB and PostgreSQL setup (compiled with those features, never run here).
//!
//! The guide's Rust code blocks are copied from this file, and `tests/getting_started_guide.rs`
//! fails if they drift apart.
//!
//! Run it with `cargo run -p photon-evt --example getting_started --features testing`, and its
//! tests with `cargo test -p photon-evt --features testing --example getting_started`.

use photon_evt::{
    Aggregate, BoxError, ExecuteError, MemRepository, Metadata, OutcomeCommand, OutcomeConfig,
    Rejection, Store, ToRejection, canonical_json, domain_events,
};
use serde::{Deserialize, Serialize};

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

// The DynamoDB and PostgreSQL functions are compiled with those features and never run here,
// because they need a real table or database.

#[cfg(feature = "dynamodb")]
#[allow(dead_code)]
async fn initialize_table(client: aws_sdk_dynamodb::Client) -> Result<(), photon_evt::Error> {
    use photon_evt::dynamodb::DynamoRepository;

    // A deploy step: run it once, before any writer starts, with a role that may Scan the table.
    DynamoRepository::new(client, "event-log")
        .initialize()
        .await?;

    Ok(())
}

#[cfg(feature = "dynamodb")]
#[allow(dead_code)]
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

#[cfg(feature = "postgres")]
#[allow(dead_code)]
async fn prepare_database(pool: deadpool_postgres::Pool) -> Result<(), photon_evt::Error> {
    use photon_evt::postgres::PostgresRepository;

    // A deploy or startup step: it creates the tables if they don't exist, so it needs DDL rights.
    PostgresRepository::new(pool).ensure_schema().await
}

#[cfg(feature = "postgres")]
#[allow(dead_code)]
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    open_and_deposit().await?;
    deposit_with_outcomes().await?;
    println!("acct-1: opened with 100, deposited 25 once, rejected a deposit of 0");

    Ok(())
}

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

// Runs the walkthrough itself, so `cargo test` covers the code the guide shows.
#[cfg(test)]
mod walkthrough {
    #[tokio::test]
    async fn the_walkthrough_runs() {
        super::open_and_deposit().await.unwrap();
        super::deposit_with_outcomes().await.unwrap();
    }
}
