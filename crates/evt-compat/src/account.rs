//! The shared account domain. It mirrors `internal/compat/account.go` field for field and rule
//! for rule, so both languages write identical payloads and snapshots.

use std::collections::BTreeMap;

use photon_evt::{Aggregate, BoxError, JsonUpcaster, Upcaster, domain_events, json::null_default};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Go's zero `time.Time` encoding, used for an account that was never opened.
pub const GO_ZERO_TIME: &str = "0001-01-01T00:00:00Z";

/// The account aggregate. Field order is the snapshot's JSON key order and matches the Go struct.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Account {
    pub owner: String,
    pub status: String,
    pub balance_cents: i64,
    pub deposits: i64,
    pub opened_at: String,
    pub tags: Option<Vec<String>>,
    pub labels: Option<BTreeMap<String, String>>,
    pub last_memo: Option<String>,
}

impl Default for Account {
    fn default() -> Self {
        Self {
            owner: String::new(),
            status: String::new(),
            balance_cents: 0,
            deposits: 0,
            opened_at: GO_ZERO_TIME.to_owned(),
            tags: None,
            labels: None,
            last_memo: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct AccountOpened {
    pub owner: String,
    pub opened_at: String,
}

impl Default for AccountOpened {
    fn default() -> Self {
        Self {
            owner: String::new(),
            opened_at: GO_ZERO_TIME.to_owned(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct MoneyDeposited {
    pub amount_cents: i64,
    pub memo: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct MoneyWithdrawn {
    pub amount_cents: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AccountTagged {
    #[serde(deserialize_with = "null_default")]
    pub tags: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AccountLabeled {
    #[serde(deserialize_with = "null_default")]
    pub labels: BTreeMap<String, String>,
}

/// account:renamed v2. Version 1 stored the owner under `name`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AccountRenamed {
    pub owner: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct AccountClosed {
    pub reason: Option<String>,
}

domain_events! {
    #[derive(Clone, Debug, PartialEq)]
    pub enum AccountEvent {
        Opened(AccountOpened) => ("account:opened", 1),
        Deposited(MoneyDeposited) => ("account:deposited", 1),
        Withdrawn(MoneyWithdrawn) => ("account:withdrawn", 1),
        Tagged(AccountTagged) => ("account:tagged", 1),
        Labeled(AccountLabeled) => ("account:labeled", 1),
        Renamed(AccountRenamed) => ("account:renamed", 2),
        Closed(AccountClosed) => ("account:closed", 1),
    }
}

/// Account commands, decoded from the same scenario JSON the Go harness reads.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum AccountCommand {
    Open {
        owner: String,
        #[serde(default)]
        opened_at: Option<String>,
    },
    Deposit {
        amount_cents: i64,
        #[serde(default)]
        memo: Option<String>,
    },
    BatchDeposit {
        amounts: Vec<i64>,
    },
    Withdraw {
        amount_cents: i64,
    },
    Tag {
        tags: Vec<String>,
    },
    SetLabels {
        labels: BTreeMap<String, String>,
    },
    Rename {
        owner: String,
    },
    Close {
        #[serde(default)]
        reason: Option<String>,
    },
}

/// A business-rule rejection.
#[derive(Debug, thiserror::Error)]
#[error("rejected: {0}")]
pub struct Rejected(pub &'static str);

fn rename_name_to_owner(payload: &mut Value) -> Result<(), BoxError> {
    let object = payload
        .as_object_mut()
        .ok_or("account:renamed v1 payload is not an object")?;
    let name = object
        .remove("name")
        .ok_or("account:renamed v1 payload has no name")?;
    object.insert("owner".to_owned(), name);

    Ok(())
}

static RENAMED_V1: JsonUpcaster = JsonUpcaster::new("account:renamed", 1, 2, rename_name_to_owner);
static UPCASTERS: [&dyn Upcaster; 1] = [&RENAMED_V1];

impl Aggregate for Account {
    const TYPE: &'static str = "account";

    type Command = AccountCommand;
    type Event = AccountEvent;
    type Error = Rejected;
    type Services = ();

    async fn handle(&self, command: AccountCommand, _: &()) -> Result<Vec<AccountEvent>, Rejected> {
        use AccountCommand as C;

        if let C::Open { owner, opened_at } = command {
            if !self.status.is_empty() {
                return Err(Rejected("account already opened"));
            }

            let opened_at = opened_at
                .filter(|at| !at.is_empty())
                .unwrap_or_else(|| GO_ZERO_TIME.to_owned());

            return Ok(vec![AccountOpened { owner, opened_at }.into()]);
        }

        if self.status != "open" {
            return Err(Rejected("account is not open"));
        }

        match command {
            C::Open { .. } => unreachable!("handled above"),
            C::Deposit { amount_cents, memo } => {
                if amount_cents <= 0 {
                    return Err(Rejected("amount must be positive"));
                }
                Ok(vec![MoneyDeposited { amount_cents, memo }.into()])
            }
            C::BatchDeposit { amounts } => {
                if amounts.is_empty() {
                    return Err(Rejected("no amounts"));
                }
                amounts
                    .into_iter()
                    .map(|amount_cents| {
                        if amount_cents <= 0 {
                            return Err(Rejected("amount must be positive"));
                        }
                        Ok(MoneyDeposited {
                            amount_cents,
                            memo: None,
                        }
                        .into())
                    })
                    .collect()
            }
            C::Withdraw { amount_cents } => {
                if amount_cents <= 0 {
                    return Err(Rejected("amount must be positive"));
                }
                if amount_cents > self.balance_cents {
                    return Err(Rejected("insufficient funds"));
                }
                Ok(vec![MoneyWithdrawn { amount_cents }.into()])
            }
            C::Tag { tags } => {
                if tags.is_empty() {
                    return Err(Rejected("no tags"));
                }
                Ok(vec![AccountTagged { tags }.into()])
            }
            C::SetLabels { labels } => {
                if labels.is_empty() {
                    return Err(Rejected("no labels"));
                }
                Ok(vec![AccountLabeled { labels }.into()])
            }
            C::Rename { owner } => {
                if owner.is_empty() {
                    return Err(Rejected("owner is required"));
                }
                Ok(vec![AccountRenamed { owner }.into()])
            }
            C::Close { reason } => Ok(vec![AccountClosed { reason }.into()]),
        }
    }

    fn apply(&mut self, event: AccountEvent) {
        match event {
            AccountEvent::Opened(e) => {
                self.owner = e.owner;
                self.opened_at = e.opened_at;
                self.status = "open".to_owned();
            }
            AccountEvent::Deposited(e) => {
                self.balance_cents += e.amount_cents;
                self.deposits += 1;
                if e.memo.is_some() {
                    self.last_memo = e.memo;
                }
            }
            AccountEvent::Withdrawn(e) => self.balance_cents -= e.amount_cents,
            AccountEvent::Tagged(e) => self.tags.get_or_insert_with(Vec::new).extend(e.tags),
            AccountEvent::Labeled(e) => self
                .labels
                .get_or_insert_with(BTreeMap::new)
                .extend(e.labels),
            AccountEvent::Renamed(e) => self.owner = e.owner,
            AccountEvent::Closed(_) => self.status = "closed".to_owned(),
        }
    }

    fn upcasters() -> &'static [&'static dyn Upcaster] {
        &UPCASTERS
    }
}
