//! A runnable banking example: the Rust port of Go's `go/examples/banking`, extended to show the
//! multi-stream reshape surface.
//!
//! - `domain_events!` declares each aggregate's events.
//! - `Account` and `Bank` are aggregates; `BalanceView` is a read model folded from committed
//!   events (projectors are pure: never side-effect in `project`).
//! - `transfer` moves money between two accounts as one multi-stream transaction. It also reads
//!   the `bank` stream without writing it, so the commit is fenced on the bank not having been
//!   frozen in the meantime.
//! - The tests use `photon_evt::testing` (given/when/then) over `MemRepository`.
//!
//! Run it with `cargo run -p photon-evt --example banking --features testing`.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use photon_evt::{
    Aggregate, DomainEvent, Error, MemRepository, Metadata, SerializedEvent, Store, domain_events,
};
use serde::{Deserialize, Serialize};

const BANK_ID: &str = "bank";

// ---- Events ----

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MoneyWithdrawn {
    pub account_id: String,
    pub amount: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferSent {
    pub account_id: String,
    pub to: String,
    pub amount: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferReceived {
    pub account_id: String,
    pub from: String,
    pub amount: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Marker {}

domain_events! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum AccountEvent {
        Opened(AccountOpened) => ("bank_account.opened", 1),
        Deposited(MoneyDeposited) => ("bank_account.money_deposited", 1),
        Withdrawn(MoneyWithdrawn) => ("bank_account.money_withdrawn", 1),
        TransferSent(TransferSent) => ("bank_account.transfer_sent", 1),
        TransferReceived(TransferReceived) => ("bank_account.transfer_received", 1),
    }
}

domain_events! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum BankEvent {
        Frozen(Marker) => ("bank.frozen", 1, no_from),
        Unfrozen(Marker) => ("bank.unfrozen", 1, no_from),
    }
}

// ---- Account ----

#[derive(Clone, Debug, PartialEq)]
pub enum AccountCommand {
    Open {
        account_id: String,
        initial_balance: i64,
    },
    Deposit {
        amount: i64,
    },
    Withdraw {
        amount: i64,
    },
    SendTransfer {
        to: String,
        amount: i64,
    },
    ReceiveTransfer {
        from: String,
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
    #[error("insufficient funds: balance {balance}, requested {requested}")]
    InsufficientFunds { balance: i64, requested: i64 },
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Account {
    pub id: String,
    pub balance: i64,
    pub opened: bool,
}

impl Account {
    fn check_open(&self, amount: i64) -> Result<(), AccountError> {
        if !self.opened {
            return Err(AccountError::NotOpened);
        }
        if amount <= 0 {
            return Err(AccountError::InvalidAmount);
        }

        Ok(())
    }

    fn check_funds(&self, amount: i64) -> Result<(), AccountError> {
        self.check_open(amount)?;
        if self.balance < amount {
            return Err(AccountError::InsufficientFunds {
                balance: self.balance,
                requested: amount,
            });
        }

        Ok(())
    }
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
        let account_id = self.id.clone();

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
                self.check_open(amount)?;

                Ok(vec![MoneyDeposited { account_id, amount }.into()])
            }

            AccountCommand::Withdraw { amount } => {
                self.check_funds(amount)?;

                Ok(vec![MoneyWithdrawn { account_id, amount }.into()])
            }

            AccountCommand::SendTransfer { to, amount } => {
                self.check_funds(amount)?;

                Ok(vec![
                    TransferSent {
                        account_id,
                        to,
                        amount,
                    }
                    .into(),
                ])
            }

            AccountCommand::ReceiveTransfer { from, amount } => {
                self.check_open(amount)?;

                Ok(vec![
                    TransferReceived {
                        account_id,
                        from,
                        amount,
                    }
                    .into(),
                ])
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
            AccountEvent::Withdrawn(e) => self.balance -= e.amount,
            AccountEvent::TransferSent(e) => self.balance -= e.amount,
            AccountEvent::TransferReceived(e) => self.balance += e.amount,
        }
    }
}

// ---- Bank (a stream that transfers read but never write) ----

#[derive(Clone, Debug, PartialEq)]
pub enum BankCommand {
    Freeze,
    Unfreeze,
}

#[derive(Debug, thiserror::Error)]
#[error("the bank is already in that state")]
pub struct NoChange;

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Bank {
    pub frozen: bool,
}

impl Aggregate for Bank {
    const TYPE: &'static str = "bank";

    type Command = BankCommand;
    type Event = BankEvent;
    type Error = NoChange;
    type Services = ();

    async fn handle(&self, command: BankCommand, _: &()) -> Result<Vec<BankEvent>, NoChange> {
        match (command, self.frozen) {
            (BankCommand::Freeze, false) => Ok(vec![BankEvent::Frozen(Marker {})]),
            (BankCommand::Unfreeze, true) => Ok(vec![BankEvent::Unfrozen(Marker {})]),
            _ => Err(NoChange),
        }
    }

    fn apply(&mut self, event: BankEvent) {
        self.frozen = matches!(event, BankEvent::Frozen(_));
    }
}

// ---- Balance view ----

/// A read model of balances, folded from committed events.
///
/// Projectors are pure: `Projector::project` returns operations that the repository applies only
/// if the commit succeeds, so it must never side-effect. `MemRepository` has no view storage (its
/// operation type is uninhabited), so this view is fed from the events a commit returns, after
/// `commit` is `Ok`. A rejected, conflicted, or duplicate command therefore leaves it untouched.
/// On DynamoDB or PostgreSQL, implement `Projector` and return the backend's write operations
/// instead; they commit atomically with the events.
#[derive(Clone, Debug, Default)]
pub struct BalanceView(Arc<Mutex<BTreeMap<String, i64>>>);

impl BalanceView {
    pub fn balance(&self, account_id: &str) -> Option<i64> {
        self.0.lock().unwrap().get(account_id).copied()
    }

    /// Folds committed account events into the view. Events of other aggregates are ignored.
    pub fn record(&self, committed: &[SerializedEvent]) {
        let mut balances = self.0.lock().unwrap();

        for stored in committed.iter().filter(|e| e.entity_type == Account::TYPE) {
            let Ok(event) = AccountEvent::from_payload(
                stored.event_type.as_str(),
                stored.version,
                &stored.payload,
            ) else {
                continue;
            };
            let balance = balances.entry(stored.entity_id.to_string()).or_insert(0);

            match event {
                AccountEvent::Opened(e) => *balance = e.balance,
                AccountEvent::Deposited(e) => *balance += e.amount,
                AccountEvent::Withdrawn(e) => *balance -= e.amount,
                AccountEvent::TransferSent(e) => *balance -= e.amount,
                AccountEvent::TransferReceived(e) => *balance += e.amount,
            }
        }
    }
}

// ---- Transfer: one transaction over three streams ----

#[derive(Debug, thiserror::Error)]
pub enum TransferError {
    #[error("the bank is frozen")]
    Frozen,
    #[error("account {id}: {source}")]
    Account {
        id: String,
        #[source]
        source: AccountError,
    },
    #[error(transparent)]
    Store(#[from] Error),
}

type Accounts = Store<Account, MemRepository>;

/// Reads both accounts and the bank, stages the debit and credit, and leaves the transaction
/// ready to commit. The bank is read but not written, so the commit fails if it changes first.
pub async fn stage_transfer<'a>(
    accounts: &'a Accounts,
    from: &str,
    to: &str,
    amount: i64,
) -> Result<photon_evt::Transaction<'a, MemRepository>, TransferError> {
    let mut tx = accounts.transaction();

    let bank = tx.read::<Bank>(BANK_ID).await?;
    if bank.aggregate().frozen {
        return Err(TransferError::Frozen);
    }

    let mut source = tx.read::<Account>(from).await?;
    let mut target = tx.read::<Account>(to).await?;

    let debit = source
        .aggregate()
        .handle(
            AccountCommand::SendTransfer {
                to: to.to_owned(),
                amount,
            },
            accounts.services(),
        )
        .await
        .map_err(|source| TransferError::Account {
            id: from.to_owned(),
            source,
        })?;
    let credit = target
        .aggregate()
        .handle(
            AccountCommand::ReceiveTransfer {
                from: from.to_owned(),
                amount,
            },
            accounts.services(),
        )
        .await
        .map_err(|source| TransferError::Account {
            id: to.to_owned(),
            source,
        })?;

    // `append_to` runs the store's projectors and snapshot interval for each account.
    accounts.append_to(&mut tx, &mut source, debit)?;
    accounts.append_to(&mut tx, &mut target, credit)?;

    Ok(tx)
}

/// Commits a staged transfer and, only once the commit succeeds, folds its events into the view.
pub async fn finish_transfer(
    tx: photon_evt::Transaction<'_, MemRepository>,
    view: &BalanceView,
    metadata: &Metadata,
) -> Result<(), Error> {
    let committed = tx.commit(metadata).await?;
    view.record(&committed);

    Ok(())
}

/// Moves `amount` from one account to the other atomically, retrying when another writer moved
/// one of the streams in between. A command ID makes a repeated request a duplicate-command error.
pub async fn transfer(
    accounts: &Accounts,
    view: &BalanceView,
    from: &str,
    to: &str,
    amount: i64,
    command_id: &str,
) -> Result<(), TransferError> {
    let metadata = Metadata::default().with_command_id(command_id);
    let mut attempts = 0;

    loop {
        let tx = stage_transfer(accounts, from, to, amount).await?;

        match finish_transfer(tx, view, &metadata).await {
            Err(error) if error.is_retryable() && attempts < 3 => attempts += 1,
            result => return result.map_err(TransferError::from),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let view = BalanceView::default();
    let accounts: Accounts = Store::new(MemRepository::new(), ());

    for (id, initial_balance) in [("acct-1", 100), ("acct-2", 20)] {
        let executed = accounts
            .execute(
                id,
                AccountCommand::Open {
                    account_id: id.to_owned(),
                    initial_balance,
                },
                Metadata::default(),
            )
            .await?;
        view.record(&executed.events);
    }
    let executed = accounts
        .execute(
            "acct-1",
            AccountCommand::Deposit { amount: 25 },
            Metadata::default(),
        )
        .await?;
    view.record(&executed.events);

    transfer(&accounts, &view, "acct-1", "acct-2", 50, "transfer-1").await?;

    for id in ["acct-1", "acct-2"] {
        let loaded = accounts.load(id).await?;
        println!(
            "{id}: balance={} sequence={} view={:?}",
            loaded.aggregate().balance,
            loaded.sequence(),
            view.balance(id),
        );
    }

    let overdraft = transfer(&accounts, &view, "acct-2", "acct-1", 1_000, "transfer-2").await;
    println!("overdraft: {}", overdraft.unwrap_err());

    let bank: Store<Bank, MemRepository> = Store::new(accounts.repository().clone(), ());
    bank.execute(BANK_ID, BankCommand::Freeze, Metadata::default())
        .await?;
    let frozen = transfer(&accounts, &view, "acct-1", "acct-2", 1, "transfer-3").await;
    println!("frozen: {}", frozen.unwrap_err());

    Ok(())
}

#[cfg(test)]
mod tests {
    use photon_evt::testing::{Fixture, Scenario};
    use photon_evt::{EntityId, Repository};

    use super::*;

    fn opened(id: &str, balance: i64) -> AccountEvent {
        AccountOpened {
            account_id: id.to_owned(),
            balance,
        }
        .into()
    }

    #[tokio::test]
    async fn open_emits_an_opened_event() {
        Scenario::<Account>::given([])
            .when(AccountCommand::Open {
                account_id: "acct-1".into(),
                initial_balance: 100,
            })
            .then_events([opened("acct-1", 100)])
            .await;
    }

    #[tokio::test]
    async fn opening_twice_is_rejected() {
        Scenario::<Account>::given([opened("acct-1", 100)])
            .when(AccountCommand::Open {
                account_id: "acct-1".into(),
                initial_balance: 1,
            })
            .then_rejected(|e| *e == AccountError::AlreadyOpened)
            .await;
    }

    #[tokio::test]
    async fn depositing_before_opening_is_rejected() {
        Scenario::<Account>::given([])
            .when(AccountCommand::Deposit { amount: 10 })
            .then_rejected(|e| *e == AccountError::NotOpened)
            .await;
    }

    #[tokio::test]
    async fn deposit_adds_to_the_balance() {
        Scenario::<Account>::given([opened("acct-1", 100)])
            .entity("acct-1")
            .when(AccountCommand::Deposit { amount: 25 })
            .then_state(|account| assert_eq!(account.balance, 125))
            .await;
    }

    #[tokio::test]
    async fn withdrawing_more_than_the_balance_is_rejected() {
        Scenario::<Account>::given([opened("acct-1", 10)])
            .when(AccountCommand::Withdraw { amount: 11 })
            .then_rejected(|e| {
                *e == AccountError::InsufficientFunds {
                    balance: 10,
                    requested: 11,
                }
            })
            .await;
    }

    async fn seeded() -> (Fixture, Accounts, BalanceView) {
        let fixture = Fixture::new();
        fixture
            .given::<Account>("acct-1", [opened("acct-1", 100)])
            .await;
        fixture
            .given::<Account>("acct-2", [opened("acct-2", 20)])
            .await;

        let view = BalanceView::default();
        let accounts = fixture.store::<Account>(());
        for id in ["acct-1", "acct-2"] {
            view.record(
                &fixture
                    .repository()
                    .events(&EntityId::new(id))
                    .await
                    .unwrap(),
            );
        }

        (fixture, accounts, view)
    }

    #[tokio::test]
    async fn a_transfer_commits_both_sides_atomically() {
        let (fixture, accounts, view) = seeded().await;

        transfer(&accounts, &view, "acct-1", "acct-2", 30, "transfer-1")
            .await
            .unwrap();

        fixture
            .assert_events::<Account>(
                "acct-1",
                [
                    opened("acct-1", 100),
                    TransferSent {
                        account_id: "acct-1".into(),
                        to: "acct-2".into(),
                        amount: 30,
                    }
                    .into(),
                ],
            )
            .await;
        assert_eq!(
            fixture.load::<Account>("acct-1").await.aggregate().balance,
            70
        );
        assert_eq!(
            fixture.load::<Account>("acct-2").await.aggregate().balance,
            50
        );
        assert_eq!(view.balance("acct-2"), Some(50));
    }

    #[tokio::test]
    async fn a_rejected_leg_writes_nothing() {
        let (fixture, accounts, view) = seeded().await;

        let error = transfer(&accounts, &view, "acct-2", "acct-1", 500, "transfer-1")
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TransferError::Account {
                source: AccountError::InsufficientFunds { .. },
                ..
            }
        ));
        assert_eq!(fixture.events::<Account>("acct-1").await.len(), 1);
        assert_eq!(fixture.events::<Account>("acct-2").await.len(), 1);
        assert_eq!(
            (view.balance("acct-1"), view.balance("acct-2")),
            (Some(100), Some(20))
        );
    }

    #[tokio::test]
    async fn a_repeated_command_id_does_not_transfer_twice() {
        let (fixture, accounts, view) = seeded().await;

        transfer(&accounts, &view, "acct-1", "acct-2", 30, "transfer-1")
            .await
            .unwrap();
        let again = transfer(&accounts, &view, "acct-1", "acct-2", 30, "transfer-1")
            .await
            .unwrap_err();

        assert!(matches!(again, TransferError::Store(e) if e.is_duplicate_command()));
        // The duplicate left the view where the first transfer put it.
        assert_eq!(
            (view.balance("acct-1"), view.balance("acct-2")),
            (Some(70), Some(50))
        );
        assert_eq!(
            fixture.load::<Account>("acct-1").await.aggregate().balance,
            70
        );
    }

    #[tokio::test]
    async fn a_frozen_bank_refuses_transfers() {
        let (fixture, accounts, view) = seeded().await;
        fixture
            .given::<Bank>(BANK_ID, [BankEvent::Frozen(Marker {})])
            .await;

        let error = transfer(&accounts, &view, "acct-1", "acct-2", 1, "transfer-1")
            .await
            .unwrap_err();

        assert!(matches!(error, TransferError::Frozen));
    }

    #[tokio::test]
    async fn freezing_the_bank_mid_transfer_fails_the_read_fence() {
        let (fixture, accounts, view) = seeded().await;

        // The transfer reads the bank (unfrozen) and stages both legs...
        let tx = stage_transfer(&accounts, "acct-1", "acct-2", 30)
            .await
            .unwrap();

        // ...then the bank freezes before the commit.
        fixture
            .given::<Bank>(BANK_ID, [BankEvent::Frozen(Marker {})])
            .await;

        let error = finish_transfer(tx, &view, &Metadata::default())
            .await
            .unwrap_err();
        assert!(error.is_conflict(), "{error:?}");
        // The failed commit never reached the view.
        assert_eq!(
            (view.balance("acct-1"), view.balance("acct-2")),
            (Some(100), Some(20))
        );
        assert_eq!(
            fixture.load::<Account>("acct-1").await.aggregate().balance,
            100
        );
    }
}
