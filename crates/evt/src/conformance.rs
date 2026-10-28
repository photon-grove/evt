//! A backend-neutral conformance suite for [`Repository`] implementations, the Rust counterpart of
//! Go's `conformance` package.
//!
//! It checks the contract documented on [`Repository`] (ordering, isolation, optimistic
//! concurrency, snapshots, compaction, entity heads) and the [`Store`] behavior built on it
//! (snapshot-aware loads, stale-commit conflicts, retries, and in-band projector operations that
//! commit atomically with events). Every case namespaces its entity IDs, so durable backends can
//! share one table across runs. The repository cases also namespace entity types. The [`Store`]
//! cases use the [`Ledger`] type, so a shared table accumulates `ledger` rows.
//!
//! ```
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! use photon_evt::MemRepository;
//! use photon_evt::conformance::Suite;
//!
//! Suite::new(&MemRepository::new()).run().await;
//! # });
//! ```
//!
//! Enable the `conformance` feature in `dev-dependencies` to run it against your own backend.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::ops::RangeInclusive;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::aggregate::{Aggregate, Projector};
use crate::enumerate::{EventLog, StreamFilter};
use crate::error::{BoxError, Error};
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::features::{self, FeatureMarker};
use crate::ids::{
    CommandId, EntityId, EntityType, EventId, EventSequence, EventType, EventVersion,
};
use crate::mem::MemRepository;
use crate::metadata::Metadata;
use crate::outcome::Outcome;
use crate::outcome::{
    CommandIdentity, DigestInput, OutcomeCommand, OutcomeConfig, Rejection, ToRejection,
    canonical_json, default_digest,
};
use crate::repository::{
    Commit, FenceProvenance, NewSnapshot, ReadFence, Repository, TransactionCommit,
};
use crate::retry::RetryPolicy;
use crate::store::{Decision, Store, serialize_events};
use crate::transaction::Begin;

/// The aggregate the [`Store`] cases use: a running total of positive amounts.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Ledger {
    /// The sum of every added amount.
    pub total: i64,
    /// The number of additions.
    pub entries: i64,
}

/// The payload of `ledger:added`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Added {
    /// The added amount.
    pub amount: i64,
}

crate::domain_events! {
    /// The [`Ledger`] events.
    #[derive(Clone, Debug, PartialEq)]
    pub enum LedgerEvent {
        /// An amount was added.
        Added(Added) => ("ledger:added", 1),
    }
}

/// The [`Ledger`] command: add a positive amount.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Add(pub i64);

/// The [`Ledger`] rejection for a non-positive amount.
#[derive(Debug, thiserror::Error)]
#[error("amount must be positive")]
pub struct InvalidAmount;

impl OutcomeCommand for Add {
    fn command_type(&self) -> String {
        "ledger:add".into()
    }

    fn canonical_payload(&self) -> Result<Option<Vec<u8>>, BoxError> {
        Ok(Some(canonical_json(&self.0)?))
    }
}

impl ToRejection for InvalidAmount {
    fn to_rejection(&self) -> Option<Rejection> {
        Some(Rejection::new("invalid-amount", self.to_string()))
    }
}

impl Aggregate for Ledger {
    const TYPE: &'static str = "ledger";

    type Command = Add;
    type Event = LedgerEvent;
    type Error = InvalidAmount;
    type Services = ();

    async fn handle(&self, Add(amount): Add, _: &()) -> Result<Vec<LedgerEvent>, InvalidAmount> {
        if amount <= 0 {
            return Err(InvalidAmount);
        }

        Ok(vec![Added { amount }.into()])
    }

    fn apply(&mut self, event: LedgerEvent) {
        match event {
            LedgerEvent::Added(added) => {
                self.total += added.amount;
                self.entries += 1;
            }
        }
    }
}

/// An aggregate whose handler always fails with an internal error that is not a domain rejection
/// ([`ToRejection`] returns `None`), for the outcome cases that prove nothing is recorded.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Flaky;

crate::domain_events! {
    /// The [`Flaky`] events (it never emits one).
    #[derive(Clone, Debug, PartialEq)]
    pub enum FlakyEvent {
        /// Never emitted.
        Tick(Tick) => ("flaky:tick", 1),
    }
}

/// The payload of `flaky:tick`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tick;

/// The [`Flaky`] command.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Flake;

/// The internal failure [`Flaky`] returns: a dependency was unavailable.
#[derive(Debug, thiserror::Error)]
#[error("the dependency is unavailable")]
pub struct Unavailable;

impl OutcomeCommand for Flake {
    fn command_type(&self) -> String {
        "flaky:flake".into()
    }

    fn canonical_payload(&self) -> Result<Option<Vec<u8>>, BoxError> {
        Ok(None)
    }
}

impl ToRejection for Unavailable {
    fn to_rejection(&self) -> Option<Rejection> {
        None
    }
}

impl Aggregate for Flaky {
    const TYPE: &'static str = "flaky";

    type Command = Flake;
    type Event = FlakyEvent;
    type Error = Unavailable;
    type Services = ();

    async fn handle(&self, _: Flake, _: &()) -> Result<Vec<FlakyEvent>, Unavailable> {
        Err(Unavailable)
    }

    fn apply(&mut self, _: FlakyEvent) {}
}

/// A second aggregate type for transactions that mix entity types: a claim on a unique name,
/// held by at most one holder.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Claim {
    /// Who holds the claim, once taken.
    pub holder: Option<String>,
}

/// The payload of `claim:taken`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Taken {
    /// The new holder.
    pub holder: String,
}

crate::domain_events! {
    /// The [`Claim`] events.
    #[derive(Clone, Debug, PartialEq)]
    pub enum ClaimEvent {
        /// The claim was taken.
        Taken(Taken) => ("claim:taken", 1),
    }
}

/// The [`Claim`] command: take the claim for a holder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Take(pub String);

/// The [`Claim`] rejection when it is already held.
#[derive(Debug, thiserror::Error)]
#[error("the claim is already held")]
pub struct AlreadyHeld;

impl OutcomeCommand for Take {
    fn command_type(&self) -> String {
        "claim:take".into()
    }

    fn canonical_payload(&self) -> Result<Option<Vec<u8>>, BoxError> {
        Ok(Some(canonical_json(&self.0)?))
    }
}

impl ToRejection for AlreadyHeld {
    fn to_rejection(&self) -> Option<Rejection> {
        Some(Rejection::new("already-held", self.to_string()))
    }
}

impl Aggregate for Claim {
    const TYPE: &'static str = "claim";

    type Command = Take;
    type Event = ClaimEvent;
    type Error = AlreadyHeld;
    type Services = ();

    async fn handle(&self, Take(holder): Take, _: &()) -> Result<Vec<ClaimEvent>, AlreadyHeld> {
        if self.holder.is_some() {
            return Err(AlreadyHeld);
        }

        Ok(vec![Taken { holder }.into()])
    }

    fn apply(&mut self, event: ClaimEvent) {
        match event {
            ClaimEvent::Taken(taken) => self.holder = Some(taken.holder),
        }
    }
}

/// A third aggregate type for rejections that carry events: a wallet that records a refused
/// spend in its history, in the same commit as the rejection.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Wallet {
    /// Funds added minus funds spent.
    pub balance: i64,
    /// The number of refused spends recorded in the history.
    pub refusals: i64,
}

/// The payload of `wallet:funded`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Funded {
    /// The amount.
    pub amount: i64,
}

/// The payload of `wallet:spent`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Spent {
    /// The amount.
    pub amount: i64,
}

/// The payload of `wallet:refused`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Refused {
    /// The amount that was asked for.
    pub amount: i64,
}

crate::domain_events! {
    /// The [`Wallet`] events.
    #[derive(Clone, Debug, PartialEq)]
    pub enum WalletEvent {
        /// Funds were added.
        Funded(Funded) => ("wallet:funded", 1),
        /// Funds were spent.
        Spent(Spent) => ("wallet:spent", 1),
        /// A spend was refused. Recorded beside the rejection.
        Refused(Refused) => ("wallet:refused", 1),
    }
}

/// The [`Wallet`] commands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalletCommand {
    /// Add funds.
    Fund(i64),
    /// Spend funds. Over the balance it is rejected, and the refusal is recorded as an event.
    Spend(i64),
}

/// The [`Wallet`] rejections.
#[derive(Debug, thiserror::Error)]
pub enum WalletError {
    /// A non-positive amount. Recorded as a plain rejection with no events.
    #[error("amount must be positive")]
    InvalidAmount,
    /// A spend over the balance. Recorded with a `wallet:refused` event.
    #[error("balance is too low")]
    Overdrawn {
        /// The amount asked for.
        requested: i64,
        /// The balance it was decided on.
        balance: i64,
    },
}

impl OutcomeCommand for WalletCommand {
    fn command_type(&self) -> String {
        match self {
            Self::Fund(_) => "wallet:fund".into(),
            Self::Spend(_) => "wallet:spend".into(),
        }
    }

    fn canonical_payload(&self) -> Result<Option<Vec<u8>>, BoxError> {
        let (Self::Fund(amount) | Self::Spend(amount)) = self;

        Ok(Some(canonical_json(amount)?))
    }
}

impl ToRejection for WalletError {
    fn to_rejection(&self) -> Option<Rejection> {
        Some(match self {
            Self::InvalidAmount => Rejection::new("invalid-amount", self.to_string()),
            Self::Overdrawn { requested, balance } => {
                Rejection::new("insufficient-funds", self.to_string())
                    .with_details(serde_json::json!({ "balance": balance, "requested": requested }))
            }
        })
    }
}

impl Aggregate for Wallet {
    const TYPE: &'static str = "wallet";

    type Command = WalletCommand;
    type Event = WalletEvent;
    type Error = WalletError;
    type Services = ();

    async fn handle(
        &self,
        command: WalletCommand,
        _: &(),
    ) -> Result<Vec<WalletEvent>, WalletError> {
        match command {
            WalletCommand::Fund(amount) if amount > 0 => Ok(vec![Funded { amount }.into()]),
            WalletCommand::Spend(amount) if amount > 0 && amount <= self.balance => {
                Ok(vec![Spent { amount }.into()])
            }
            WalletCommand::Spend(requested) if requested > 0 => Err(WalletError::Overdrawn {
                requested,
                balance: self.balance,
            }),
            _ => Err(WalletError::InvalidAmount),
        }
    }

    fn apply(&mut self, event: WalletEvent) {
        match event {
            WalletEvent::Funded(funded) => self.balance += funded.amount,
            WalletEvent::Spent(spent) => self.balance -= spent.amount,
            WalletEvent::Refused(_) => self.refusals += 1,
        }
    }

    fn rejection_events(&self, error: &WalletError) -> Vec<WalletEvent> {
        match error {
            WalletError::Overdrawn { requested, .. } => vec![Refused { amount: *requested }.into()],
            WalletError::InvalidAmount => Vec::new(),
        }
    }
}

/// A backend's view storage, for the case that checks projector operations commit atomically
/// with events.
pub trait ViewTarget<R: Repository>: Send + Sync {
    /// The projector type.
    type Projector: Projector<Ledger, R::Operation> + 'static;

    /// Returns a projector that stores the [`Ledger`] state as the entity's view.
    fn projector(&self) -> Self::Projector;

    /// Reads the view the projector wrote, if any.
    fn view(
        &self,
        entity_id: &EntityId,
    ) -> impl Future<Output = Result<Option<Ledger>, BoxError>> + Send;
}

/// The placeholder [`ViewTarget`] for suites without view storage. Its cases are skipped.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoViews;

impl<Op> Projector<Ledger, Op> for NoViews {
    fn project(&self, _: &EntityId, _: &Ledger, _: &[LedgerEvent]) -> Result<Vec<Op>, BoxError> {
        Ok(Vec::new())
    }
}

impl<R: Repository> ViewTarget<R> for NoViews {
    type Projector = NoViews;

    fn projector(&self) -> NoViews {
        NoViews
    }

    async fn view(&self, _: &EntityId) -> Result<Option<Ledger>, BoxError> {
        Ok(None)
    }
}

/// Backend-specific access to a stream's head item (ADR 0007), for the cases that must put a
/// stream in a state no conforming writer can: an event appended **without** its head update, as a
/// writer that predates stream heads would, and a stream whose head item is gone. Implement it
/// for a backend with a raw handle on its storage and pass it to [`Suite::with_head_fixtures`];
/// the cases that need it are skipped without one.
pub trait HeadFixtures<R: Repository>: Send + Sync {
    /// Stores the event in its stream **without** touching the stream's head item: a DynamoDB
    /// event item with no head update, or a PostgreSQL event row with no heads row.
    fn append_bypassing_head(
        &self,
        event: &SerializedEvent,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Deletes the stream's head item or row, leaving its events and snapshot.
    fn delete_head(&self, entity_id: &EntityId) -> impl Future<Output = Result<(), Error>> + Send;

    /// Reads the stream's stored head, `None` when it has no head item or row.
    fn stored_head(
        &self,
        entity_id: &EntityId,
    ) -> impl Future<Output = Result<Option<EventSequence>, Error>> + Send;
}

/// The placeholder [`HeadFixtures`] for suites without raw storage access. Its cases are skipped.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoHeadFixtures;

impl<R: Repository> HeadFixtures<R> for NoHeadFixtures {
    async fn append_bypassing_head(&self, _: &SerializedEvent) -> Result<(), Error> {
        Err(Error::InvalidConfig("no head fixtures".into()))
    }

    async fn delete_head(&self, _: &EntityId) -> Result<(), Error> {
        Err(Error::InvalidConfig("no head fixtures".into()))
    }

    async fn stored_head(&self, _: &EntityId) -> Result<Option<EventSequence>, Error> {
        Err(Error::InvalidConfig("no head fixtures".into()))
    }
}

impl HeadFixtures<crate::MemRepository> for crate::MemRepository {
    async fn append_bypassing_head(&self, event: &SerializedEvent) -> Result<(), Error> {
        MemRepository::append_bypassing_head(self, event.clone());

        Ok(())
    }

    async fn delete_head(&self, entity_id: &EntityId) -> Result<(), Error> {
        MemRepository::delete_head(self, entity_id);

        Ok(())
    }

    async fn stored_head(&self, entity_id: &EntityId) -> Result<Option<EventSequence>, Error> {
        Ok(MemRepository::stored_head(self, entity_id))
    }
}

/// Runs the conformance cases against one repository.
pub struct Suite<'a, R, V = NoViews, H = NoHeadFixtures> {
    repository: &'a R,
    snapshot_sequence_conflicts: bool,
    views: Option<V>,
    head_fixtures: Option<H>,
}

impl<'a, R: Repository> Suite<'a, R> {
    /// Creates a suite over a repository. The repository may hold data from earlier runs.
    pub fn new(repository: &'a R) -> Self {
        Self {
            repository,
            snapshot_sequence_conflicts: true,
            views: None,
            head_fixtures: None,
        }
    }
}

impl<'a, R: Repository, V: ViewTarget<R>, H: HeadFixtures<R>> Suite<'a, R, V, H> {
    /// Whether a commit whose snapshot sequence is not the stored one plus one is rejected
    /// (default `true`). PostgreSQL relies on the event-sequence conflict instead, so it opts out.
    pub fn snapshot_sequence_conflicts(mut self, enforced: bool) -> Self {
        self.snapshot_sequence_conflicts = enforced;
        self
    }

    /// Adds the view storage for the projector case.
    pub fn with_views<W: ViewTarget<R>>(self, views: W) -> Suite<'a, R, W, H> {
        Suite {
            repository: self.repository,
            snapshot_sequence_conflicts: self.snapshot_sequence_conflicts,
            views: Some(views),
            head_fixtures: self.head_fixtures,
        }
    }

    /// Adds raw access to the stream heads, for the drift, ranking, and head-guard cases that put
    /// a stream in a state no conforming writer can.
    pub fn with_head_fixtures<G: HeadFixtures<R>>(self, fixtures: G) -> Suite<'a, R, V, G> {
        Suite {
            repository: self.repository,
            snapshot_sequence_conflicts: self.snapshot_sequence_conflicts,
            views: self.views,
            head_fixtures: Some(fixtures),
        }
    }

    /// Runs every case, printing each result.
    ///
    /// # Panics
    ///
    /// After running every case, if any failed, listing each failure.
    pub async fn run(self) {
        let repo = self.repository;
        let mut failures = Vec::new();

        let mut record = |name: &str, result: Result<(), Failure>| match result {
            Ok(()) => println!("ok   {name}"),
            Err(Failure(message)) => {
                println!("FAIL {name}");
                failures.push(format!("{name}: {message}"));
            }
        };

        record("commit and read in order", commit_and_read(repo).await);
        record("metadata round trip", metadata_round_trip(repo).await);
        record("isolate entities", isolate_entities(repo).await);
        record(
            "reject events for another entity or of mixed types",
            foreign_events(repo).await,
        );
        record(
            "reject events out of order, with gaps, below 1, or with wrong IDs or snapshot sequences",
            out_of_order_events(repo).await,
        );
        record("an empty commit is a no-op", empty_commit(repo).await);
        record(
            "reject a duplicate sequence",
            duplicate_sequence(repo).await,
        );
        record("snapshot round trip", snapshot_round_trip(repo).await);
        if self.snapshot_sequence_conflicts {
            record(
                "reject a stale snapshot sequence",
                snapshot_sequence_conflict(repo).await,
            );
        }
        record("put snapshot is monotonic", put_snapshot(repo).await);
        record(
            "an out-of-band snapshot must be covered by the stream's head",
            Box::pin(snapshot_guard(repo)).await,
        );
        record("compact below a snapshot", compact_below(repo).await);
        record(
            "compaction never moves the head and never conflicts with a current writer",
            Box::pin(head_survives_compaction(repo)).await,
        );
        record(
            "reject a commit below the compacted floor",
            compacted_floor(repo).await,
        );
        record(
            "a multi-stream commit appends to every stream in pinned order",
            multi_stream_commit(repo).await,
        );
        record(
            "a transactional first snapshot must have sequence 1",
            transaction_first_snapshot(repo).await,
        );
        record(
            "a transaction mixes aggregate types and fences read-only streams of any type",
            mixed_type_transaction(repo).await,
        );
        record(
            "a commit must continue from the stored head without a sequence gap",
            sequence_gaps(repo).await,
        );
        record(
            "a multi-stream commit is all-or-nothing",
            multi_stream_atomicity(repo).await,
        );
        record(
            "read fences pass at the head and fail once another writer moves the stream",
            read_fences(repo).await,
        );
        record(
            "an oversized commit is refused with CommitTooLarge before any write",
            commit_too_large(repo).await,
        );
        record(
            "append, snapshot, compact must fail a stale fence",
            fence_compaction_aba(repo).await,
        );
        record("entity heads", entity_heads(repo).await);
        record(
            "racing writers lose with conflicts, never with drift",
            Box::pin(racing_writers_never_read_as_drift(repo)).await,
        );
        if let Some(fixtures) = &self.head_fixtures {
            record(
                "a stream whose head disagrees with its log is drift, which is never retried or recorded",
                Box::pin(stream_head_drift(repo, fixtures)).await,
            );
            record(
                "failures rank outcome conflict, drift, gap, then conflict",
                Box::pin(failure_ranking(repo, fixtures)).await,
            );
        }
        record(
            "latest(n) reads the newest events first",
            latest_n(repo).await,
        );
        record(
            "refuse reserved namespaces on every write path",
            reserved_namespaces(repo).await,
        );
        record("store loads from a snapshot", store_snapshots(repo).await);
        record(
            "store rejects a stale commit",
            store_stale_commit(repo).await,
        );
        record("store retries a conflict", store_retry(repo).await);
        record(
            "store reloads when compaction races a load",
            store_load_during_compaction(repo).await,
        );
        if let Some(views) = &self.views {
            record(
                "projector operations commit with events",
                store_projector(repo, views).await,
            );
            record(
                "a transaction projects each stream once from its final state",
                transaction_projector(repo, views).await,
            );
        }

        if !failures.is_empty() {
            panic!(
                "{} conformance case(s) failed:\n\n{}",
                failures.len(),
                failures.join("\n\n")
            );
        }
    }
}

/// A failed check.
struct Failure(String);

impl<E: std::error::Error> From<E> for Failure {
    fn from(err: E) -> Self {
        Self(format!("unexpected error: {err}"))
    }
}

type CaseResult = Result<(), Failure>;

macro_rules! ensure {
    ($cond:expr, $($message:tt)+) => {
        if !$cond {
            return Err(Failure(format!($($message)+)));
        }
    };
}

macro_rules! ensure_eq {
    ($got:expr, $want:expr, $what:expr) => {{
        let (got, want) = (&$got, &$want);
        if got != want {
            return Err(Failure(format!(
                "{}\n  got:  {got:?}\n  want: {want:?}",
                $what
            )));
        }
    }};
}

fn expect_err<T: fmt::Debug>(
    result: Result<T, Error>,
    what: &str,
    matches: impl Fn(&Error) -> bool,
) -> CaseResult {
    match result {
        Err(err) if matches(&err) => Ok(()),
        other => Err(Failure(format!("{what}: got {other:?}"))),
    }
}

fn expect_conflict<T: fmt::Debug>(result: Result<T, Error>, what: &str) -> CaseResult {
    expect_err(result, what, Error::is_conflict)
}

/// Returns a token unique to this process and call, for entity IDs and types.
fn namespace() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default();

    // Zero-padded, so namespaces made in one nanosecond sort in the order they were made: cases
    // that expect a stream order (the first of two streams by ID bytes) must not flip when the
    // counter passes 9.
    format!(
        "conf-{nanos}-{:06}",
        COUNTER.fetch_add(1, Ordering::Relaxed) + 1
    )
}

struct Stream {
    id: EntityId,
    entity_type: EntityType,
}

impl Stream {
    fn new(ns: &str, name: &str, entity_type: &str) -> Self {
        Self {
            id: EntityId::new(format!("{ns}-{name}")),
            entity_type: EntityType::new(format!("{ns}-{entity_type}")),
        }
    }

    /// Builds stored events whose payload records their sequence: `{"amount":<sequence>}`.
    fn events(&self, sequences: RangeInclusive<i64>) -> Vec<SerializedEvent> {
        sequences
            .map(|sequence| self.event(sequence, sequence))
            .collect()
    }

    fn event(&self, sequence: i64, amount: i64) -> SerializedEvent {
        let sequence = EventSequence(sequence);

        SerializedEvent {
            id: EventId::for_event(&self.id, sequence),
            sequence,
            event_type: EventType::new("ledger:added"),
            version: EventVersion(1),
            entity_id: self.id.clone(),
            entity_type: self.entity_type.clone(),
            payload: format!(r#"{{"amount":{amount}}}"#).into_bytes(),
            metadata: Metadata::default()
                .with_origin("conformance", "suite")
                .with_trace([("traceparent", "00-trace-span-01")])
                .with_timestamp("2026-01-02T03:04:05Z"),
        }
    }

    fn new_snapshot(&self, sequence: i64, payload: &str) -> NewSnapshot {
        NewSnapshot {
            entity_type: self.entity_type.clone(),
            sequence: EventSequence(sequence),
            payload: payload.as_bytes().to_vec(),
        }
    }

    fn snapshot(&self, sequence: i64, event_sequence: i64, payload: &str) -> SerializedSnapshot {
        SerializedSnapshot {
            entity_type: self.entity_type.clone(),
            entity_id: self.id.clone(),
            sequence: EventSequence(sequence),
            event_sequence: EventSequence(event_sequence),
            payload: payload.as_bytes().to_vec(),
        }
    }

    async fn commit<R: Repository>(
        &self,
        repo: &R,
        events: Vec<SerializedEvent>,
        snapshot: Option<NewSnapshot>,
    ) -> Result<(), Error> {
        repo.commit(Commit {
            entity_id: self.id.clone(),
            events,
            snapshot,
            operations: Vec::new(),
            views: Vec::new(),
            outcome: None,
        })
        .await
    }
}

async fn isolate_entities<R: Repository>(repo: &R) -> CaseResult {
    let ns = namespace();
    let a = Stream::new(&ns, "a", "ledger");
    let b = Stream::new(&ns, "b", "ledger");
    let missing = Stream::new(&ns, "missing", "ledger");

    a.commit(repo, a.events(1..=2), None).await?;
    b.commit(repo, b.events(1..=1), None).await?;

    ensure_eq!(repo.events(&a.id).await?, a.events(1..=2), "events(a)");
    ensure_eq!(repo.events(&b.id).await?, b.events(1..=1), "events(b)");
    ensure!(
        repo.events(&missing.id).await?.is_empty(),
        "a missing entity has no events"
    );
    ensure!(
        repo.snapshot(&missing.id).await?.is_none(),
        "a missing entity has no snapshot"
    );

    Ok(())
}

async fn commit_and_read<R: Repository>(repo: &R) -> CaseResult {
    let stream = Stream::new(&namespace(), "1", "ledger");
    stream.commit(repo, stream.events(1..=3), None).await?;
    stream.commit(repo, stream.events(4..=5), None).await?;

    ensure_eq!(
        repo.events(&stream.id).await?,
        stream.events(1..=5),
        "events() returns every event as written, ascending"
    );

    for (after, want) in [
        (2, stream.events(3..=5)),
        (5, Vec::new()),
        (9, Vec::new()),
        (i64::MAX, Vec::new()),
    ] {
        ensure_eq!(
            repo.events_after(&stream.id, EventSequence(after)).await?,
            want,
            format!("events_after({after})")
        );
    }

    Ok(())
}

async fn metadata_round_trip<R: Repository>(repo: &R) -> CaseResult {
    let stream = Stream::new(&namespace(), "1", "ledger");
    let mut events = stream.events(1..=2);
    for (i, event) in events.iter_mut().enumerate() {
        event.metadata = event
            .metadata
            .clone()
            .with_command_id(format!("command-{}", i + 1))
            .with_principal_id(format!("principal-{}", i + 1))
            .with_causation_id(format!("cause-{}", i + 1))
            .with_extension("source", format!("source-{}", i + 1))
            .with_extension("empty", "");
    }

    stream.commit(repo, events.clone(), None).await?;

    let stored = repo.events(&stream.id).await?;
    ensure_eq!(stored.len(), 2, "metadata event count");
    for (got, want) in stored.iter().zip(&events) {
        ensure_eq!(
            got.metadata.command_id,
            want.metadata.command_id,
            "command ID survives commit and read"
        );
        ensure_eq!(
            got.metadata.principal_id,
            want.metadata.principal_id,
            "principal ID survives commit and read"
        );
        ensure_eq!(
            got.metadata.causation_id,
            want.metadata.causation_id,
            "explicit causation ID survives commit and read"
        );
        ensure_eq!(
            got.metadata.extensions,
            want.metadata.extensions,
            "extensions, including an empty value, survive commit and read"
        );
    }

    Ok(())
}

async fn foreign_events<R: Repository>(repo: &R) -> CaseResult {
    let ns = namespace();
    let a = Stream::new(&ns, "a", "ledger");
    let b = Stream::new(&ns, "b", "ledger");

    expect_err(
        a.commit(repo, b.events(1..=1), None).await,
        "a commit of entity b's event under entity a",
        |err| matches!(err, Error::InvalidCommit(_)),
    )?;

    // Typed scans read each row's entity type, so one commit must not mix them.
    let mixed = vec![
        a.event(1, 1),
        SerializedEvent {
            entity_type: EntityType::new(format!("{}-other", a.entity_type)),
            ..a.event(2, 2)
        },
    ];
    expect_err(
        a.commit(repo, mixed, None).await,
        "a commit whose events have two entity types",
        |err| matches!(err, Error::InvalidCommit(_)),
    )?;
    let other_type = NewSnapshot::new(format!("{}-other", a.entity_type), EventSequence(1), "{}");
    expect_err(
        a.commit(repo, a.events(1..=1), Some(other_type)).await,
        "a snapshot whose entity type differs from the events'",
        |err| matches!(err, Error::InvalidCommit(_)),
    )?;

    ensure!(
        repo.events(&a.id).await?.is_empty() && repo.events(&b.id).await?.is_empty(),
        "the rejected commits write nothing"
    );

    Ok(())
}

async fn out_of_order_events<R: Repository>(repo: &R) -> CaseResult {
    let ns = namespace();

    // Loads reject a stream with a gap, so a commit must not create one.
    let gapped = Stream::new(&ns, "gap", "ledger");
    expect_err(
        gapped
            .commit(repo, vec![gapped.event(1, 1), gapped.event(3, 3)], None)
            .await,
        "a commit that skips a sequence",
        |err| matches!(err, Error::InvalidCommit(_)),
    )?;
    ensure!(
        repo.events(&gapped.id).await?.is_empty(),
        "the rejected gap writes nothing"
    );

    // DynamoDB keeps the snapshot at sk = 0 and reads after it, so sequences start at 1.
    let low = Stream::new(&ns, "low", "ledger");
    for sequence in [0, -1] {
        expect_err(
            low.commit(repo, vec![low.event(sequence, 1)], None).await,
            &format!("a commit at sequence {sequence}"),
            |err| matches!(err, Error::InvalidCommit(_)),
        )?;
    }

    // DynamoDB rebuilds the ID from the key, so a stored ID must already be canonical.
    let renamed = SerializedEvent {
        id: EventId::new("custom-id"),
        ..low.event(1, 1)
    };
    expect_err(
        low.commit(repo, vec![renamed], None).await,
        "an event whose ID is not {entity_id}:{sequence}",
        |err| matches!(err, Error::InvalidCommit(_)),
    )?;
    // Snapshot sequences start at 1, and the DynamoDB condition subtracts one from them.
    for sequence in [0, i64::MIN] {
        expect_err(
            low.commit(
                repo,
                low.events(1..=1),
                Some(low.new_snapshot(sequence, "{}")),
            )
            .await,
            &format!("a snapshot at sequence {sequence}"),
            |err| matches!(err, Error::InvalidCommit(_)),
        )?;
    }

    ensure!(
        repo.events(&low.id).await?.is_empty() && repo.snapshot(&low.id).await?.is_none(),
        "the rejected commits write nothing"
    );

    let stream = Stream::new(&ns, "1", "ledger");
    stream
        .commit(
            repo,
            stream.events(1..=5),
            Some(stream.new_snapshot(1, "{}")),
        )
        .await?;
    repo.compact_below(&stream.id, EventSequence(5)).await?;

    // Only the first event is checked against the floor, so [6, 5] would recreate compacted 5.
    let reversed = vec![stream.event(6, 6), stream.event(5, 5)];
    expect_err(
        stream.commit(repo, reversed, None).await,
        "a commit whose sequences descend",
        |err| matches!(err, Error::InvalidCommit(_)),
    )?;
    ensure!(
        repo.events(&stream.id).await?.is_empty(),
        "the rejected commit writes nothing"
    );

    Ok(())
}

async fn empty_commit<R: Repository>(repo: &R) -> CaseResult {
    let stream = Stream::new(&namespace(), "1", "ledger");

    // The snapshot sequences are wrong for a new stream, and 0 is invalid, but an empty commit
    // ignores its snapshot rather than validating it.
    for sequence in [2, 0] {
        stream
            .commit(repo, Vec::new(), Some(stream.new_snapshot(sequence, "{}")))
            .await?;
    }
    ensure!(
        repo.snapshot(&stream.id).await?.is_none(),
        "an empty commit ignores its snapshot"
    );
    ensure!(
        repo.entity_heads(Some(&stream.entity_type))
            .await?
            .is_empty(),
        "an empty commit creates no stream"
    );

    Ok(())
}

async fn duplicate_sequence<R: Repository>(repo: &R) -> CaseResult {
    let stream = Stream::new(&namespace(), "1", "ledger");
    stream.commit(repo, stream.events(1..=2), None).await?;

    expect_conflict(
        stream.commit(repo, vec![stream.event(1, 100)], None).await,
        "re-committing sequence 1",
    )?;
    expect_conflict(
        stream.commit(repo, stream.events(2..=3), None).await,
        "a commit overlapping the stored head",
    )?;
    ensure_eq!(
        repo.events(&stream.id).await?,
        stream.events(1..=2),
        "a rejected commit writes nothing"
    );

    stream.commit(repo, stream.events(3..=3), None).await?;
    ensure_eq!(
        repo.events(&stream.id).await?,
        stream.events(1..=3),
        "the next sequence still commits"
    );

    Ok(())
}

async fn snapshot_round_trip<R: Repository>(repo: &R) -> CaseResult {
    let stream = Stream::new(&namespace(), "1", "ledger");

    stream
        .commit(
            repo,
            stream.events(1..=3),
            Some(stream.new_snapshot(1, r#"{"total":6}"#)),
        )
        .await?;
    ensure_eq!(
        repo.snapshot(&stream.id).await?,
        Some(stream.snapshot(1, 3, r#"{"total":6}"#)),
        "the snapshot covers the commit's last event"
    );

    stream
        .commit(
            repo,
            stream.events(4..=5),
            Some(stream.new_snapshot(2, r#"{"total":15}"#)),
        )
        .await?;
    stream.commit(repo, stream.events(6..=6), None).await?;
    ensure_eq!(
        repo.snapshot(&stream.id).await?,
        Some(stream.snapshot(2, 5, r#"{"total":15}"#)),
        "a later snapshot replaces it; a commit without one leaves it"
    );

    Ok(())
}

async fn snapshot_sequence_conflict<R: Repository>(repo: &R) -> CaseResult {
    let ns = namespace();
    let first = Stream::new(&ns, "first", "ledger");
    expect_conflict(
        first
            .commit(repo, first.events(1..=2), Some(first.new_snapshot(2, "{}")))
            .await,
        "a first snapshot whose sequence is not 1",
    )?;
    ensure!(
        repo.snapshot(&first.id).await?.is_none() && repo.events(&first.id).await?.is_empty(),
        "the rejected first snapshot writes nothing"
    );

    let stream = Stream::new(&ns, "1", "ledger");
    stream
        .commit(
            repo,
            stream.events(1..=2),
            Some(stream.new_snapshot(1, "{}")),
        )
        .await?;

    expect_conflict(
        stream
            .commit(
                repo,
                stream.events(3..=4),
                Some(stream.new_snapshot(1, "{}")),
            )
            .await,
        "a snapshot that does not follow the stored one",
    )?;
    ensure_eq!(
        repo.events(&stream.id).await?,
        stream.events(1..=2),
        "the rejected commit's events are not written"
    );
    ensure_eq!(
        repo.snapshot(&stream.id).await?,
        Some(stream.snapshot(1, 2, "{}")),
        "the stored snapshot is unchanged"
    );

    stream
        .commit(
            repo,
            stream.events(3..=4),
            Some(stream.new_snapshot(2, "{}")),
        )
        .await?;

    Ok(())
}

async fn put_snapshot<R: Repository>(repo: &R) -> CaseResult {
    let stream = Stream::new(&namespace(), "1", "ledger");

    // An out-of-band snapshot must be covered by the stream's head (H3), so the stream has events
    // up to the highest snapshot below.
    stream.commit(repo, stream.events(1..=7), None).await?;

    ensure!(
        repo.put_snapshot(&stream.snapshot(1, 5, r#"{"v":1}"#))
            .await?,
        "the first snapshot is written"
    );
    ensure!(
        !repo
            .put_snapshot(&stream.snapshot(2, 3, r#"{"v":2}"#))
            .await?,
        "an older snapshot is not written"
    );
    ensure_eq!(
        repo.snapshot(&stream.id).await?,
        Some(stream.snapshot(1, 5, r#"{"v":1}"#)),
        "the newer snapshot stays"
    );

    ensure!(
        repo.put_snapshot(&stream.snapshot(3, 5, r#"{"v":3}"#))
            .await?,
        "a snapshot at the same event sequence replaces it"
    );
    ensure!(
        repo.put_snapshot(&stream.snapshot(4, 7, r#"{"v":4}"#))
            .await?,
        "a newer snapshot replaces it"
    );
    ensure_eq!(
        repo.snapshot(&stream.id).await?,
        Some(stream.snapshot(4, 7, r#"{"v":4}"#)),
        "the latest snapshot is stored"
    );

    Ok(())
}

fn is_drift(error: &Error, id: &EntityId, stored: Option<i64>, derived: i64) -> bool {
    matches!(
        error,
        Error::StreamHeadDrift { entity_id, stored: found_stored, derived: found_derived }
            if entity_id == id
                && *found_stored == stored.map(EventSequence)
                && *found_derived == EventSequence(derived)
    )
}

fn expect_drift<T: fmt::Debug>(
    result: Result<T, Error>,
    what: &str,
    id: &EntityId,
    stored: Option<i64>,
    derived: i64,
) -> CaseResult {
    match result {
        Err(error) if is_drift(&error, id, stored, derived) => {
            ensure!(
                !error.is_retryable() && !error.is_conflict(),
                "{what}: drift is neither retryable nor a conflict: {error:?}"
            );

            Ok(())
        }
        other => Err(Failure(format!(
            "{what}: want StreamHeadDrift (stored {stored:?}, derived {derived}) on {id}, got {other:?}"
        ))),
    }
}

/// An out-of-band snapshot write conditions on the stream's head (ADR 0007, H3).
async fn snapshot_guard<R: Repository>(repo: &R) -> CaseResult {
    let ns = namespace();
    let a = Stream::new(&ns, "a", "ledger");
    a.commit(repo, a.events(1..=3), None).await?;

    // A snapshot ahead of the stream would let a writer that loaded earlier pass `head = 3` and
    // append events 4 and 5 beneath a snapshot that already covers them.
    expect_drift(
        repo.put_snapshot(&a.snapshot(1, 5, "{}")).await,
        "a snapshot ahead of the head",
        &a.id,
        Some(3),
        5,
    )?;
    ensure!(
        repo.snapshot(&a.id).await?.is_none(),
        "the refused snapshot is not written"
    );

    // One at the head, and below it, is covered.
    ensure!(
        repo.put_snapshot(&a.snapshot(1, 3, "{}")).await?,
        "a snapshot at the head is written"
    );

    // A stream that was never written has no head item: a snapshot above 0 is drift with an
    // absent stored head, but a snapshot of the empty stream (event sequence 0) is allowed.
    let empty = Stream::new(&ns, "empty", "ledger");
    expect_drift(
        repo.put_snapshot(&empty.snapshot(1, 2, "{}")).await,
        "a snapshot of a stream with no head item",
        &empty.id,
        None,
        2,
    )?;
    ensure!(
        repo.put_snapshot(&empty.snapshot(1, 0, "{}")).await?,
        "a snapshot at event sequence 0 needs no head item"
    );
    ensure_eq!(
        repo.snapshot(&empty.id).await?,
        Some(empty.snapshot(1, 0, "{}")),
        "the empty-stream snapshot is stored"
    );

    // The stream then writes its first events normally.
    empty.commit(repo, empty.events(1..=2), None).await?;
    ensure_eq!(
        repo.events(&empty.id).await?,
        empty.events(1..=2),
        "the stream with an empty-stream snapshot accepts its first events"
    );

    Ok(())
}

/// Compaction deletes events but never touches the head (ADR 0007): a writer whose load is
/// current still appends, and a negative `after` is clamped to the events.
async fn head_survives_compaction<R: Repository>(repo: &R) -> CaseResult {
    let ns = namespace();
    let a = Stream::new(&ns, "a", "ledger");
    let c = Stream::new(&ns, "c", "ledger");

    a.commit(repo, a.events(1..=4), Some(a.new_snapshot(1, "{}")))
        .await?;
    ensure_eq!(
        repo.compact_below(&a.id, EventSequence(4)).await?,
        4,
        "the head event is compacted away with the rest"
    );

    // The writer that loaded at head 4 before the compaction is still current.
    a.commit(repo, a.events(5..=5), None).await?;
    ensure_eq!(
        repo.events(&a.id).await?,
        a.events(5..=5),
        "the append after compaction"
    );
    ensure_eq!(
        repo.entity_heads(Some(&a.entity_type)).await?,
        BTreeMap::from([(a.id.clone(), EventSequence(5))]),
        "compaction did not move the head"
    );

    // A fence at the head passes, and a fence at the compacted head fails: heads only increase.
    let fence = |head| {
        let mut commit = TransactionCommit::new(vec![write(&c, 1..=1)]);
        commit.fences = vec![ReadFence::new(
            a.id.clone(),
            a.entity_type.clone(),
            EventSequence(head),
            FenceProvenance::Engine,
        )];

        commit
    };
    expect_conflict(
        repo.commit_transaction(fence(4)).await,
        "a fence at the head before the append",
    )?;
    repo.commit_transaction(fence(5)).await?;

    // A negative lower bound never reaches the snapshot or the head item.
    ensure_eq!(
        repo.events_after(&a.id, EventSequence(-5)).await?,
        a.events(5..=5),
        "a negative `after` returns the events only"
    );

    Ok(())
}

/// A stream whose head disagrees with its log is drift (ADR 0007): never retried, never an
/// outcome or a rejection, and ranked above a gap and an ordinary conflict.
async fn stream_head_drift<R: Repository, H: HeadFixtures<R>>(
    repo: &R,
    fixtures: &H,
) -> CaseResult {
    let ns = namespace();
    let a = Stream::new(&ns, "a", "ledger");
    let c = Stream::new(&ns, "c", "ledger");

    // Head 2, then an event 3 stored without its head update.
    a.commit(repo, a.events(1..=2), None).await?;
    fixtures.append_bypassing_head(&a.event(3, 3)).await?;
    ensure_eq!(
        fixtures.stored_head(&a.id).await?,
        Some(EventSequence(2)),
        "the bypassed event left the head behind"
    );

    // The head update passes (`head = 2`) and the event put fails: certain drift.
    expect_drift(
        a.commit(repo, vec![a.event(3, 9)], None).await,
        "an append whose head update passes over an event already above the head",
        &a.id,
        Some(2),
        3,
    )?;

    // The head update fails (`head = 3` against 2) and the probe finds the log ahead: drift, not
    // a gap and not a conflict.
    expect_drift(
        a.commit(repo, vec![a.event(4, 9)], None).await,
        "an append past a drifted head",
        &a.id,
        Some(2),
        3,
    )?;
    ensure_eq!(
        repo.events(&a.id).await?.len(),
        3,
        "the refused appends wrote nothing"
    );

    // A fence at the stored head still passes, which is why writers must be stopped before a
    // migration; a fence at the derived head fails the check and the probe reports the drift.
    let fenced = |head| {
        let mut commit = TransactionCommit::new(vec![write(&c, 1..=1)]);
        commit.fences = vec![ReadFence::new(
            a.id.clone(),
            a.entity_type.clone(),
            EventSequence(head),
            FenceProvenance::Engine,
        )];

        commit
    };
    expect_drift(
        repo.commit_transaction(fenced(3)).await,
        "a fence at the derived head of a drifted stream",
        &a.id,
        Some(2),
        3,
    )?;
    repo.commit_transaction(fenced(2)).await?;

    // An out-of-band snapshot at the derived head is ahead of the stored head.
    expect_drift(
        repo.put_snapshot(&a.snapshot(1, 3, "{}")).await,
        "a snapshot over a drifted stream",
        &a.id,
        Some(2),
        3,
    )?;

    // Drift writes no outcome row and no rejection, even with retries available.
    let command = command_id();
    let result = a
        .commit_with_outcome(
            repo,
            vec![a.event(4, 9)],
            Outcome::accepted(command.clone(), "digest"),
        )
        .await;
    expect_drift(
        result,
        "a drifted commit that carries an outcome",
        &a.id,
        Some(2),
        3,
    )?;
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "drift records no outcome"
    );

    let id = ledger_id();
    let store = outcome_store(repo);
    store
        .execute_with_outcome(&id, Add(1), command_metadata(&command_id()))
        .await?;
    store
        .execute_with_outcome(&id, Add(1), command_metadata(&command_id()))
        .await?;
    fixtures
        .append_bypassing_head(&SerializedEvent {
            id: EventId::for_event(&id, EventSequence(3)),
            sequence: EventSequence(3),
            event_type: EventType::new("ledger:added"),
            version: EventVersion(1),
            entity_id: id.clone(),
            entity_type: EntityType::new(Ledger::TYPE),
            payload: br#"{"amount":1}"#.to_vec(),
            metadata: Metadata::default(),
        })
        .await?;
    let command = command_id();
    let result = store
        .execute_with_outcome(&id, Add(1), command_metadata(&command))
        .await;
    expect_drift(
        result,
        "a store command over a drifted stream",
        &id,
        Some(2),
        3,
    )?;
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "drift is never exhausted into a stale-fence rejection"
    );

    // A head item that is gone while the log remains is drift too, with an absent stored head.
    let b = Stream::new(&ns, "b", "ledger");
    b.commit(repo, b.events(1..=2), None).await?;
    fixtures.delete_head(&b.id).await?;
    ensure_eq!(fixtures.stored_head(&b.id).await?, None, "the head is gone");
    expect_drift(
        b.commit(repo, vec![b.event(3, 3)], None).await,
        "an append to a stream with no head item",
        &b.id,
        None,
        2,
    )?;
    expect_drift(
        b.commit(repo, b.events(1..=1), None).await,
        "a create over a stream with events and no head item",
        &b.id,
        None,
        2,
    )?;

    Ok(())
}

/// Several failed items rank, identically on every backend: an outcome conflict above drift,
/// drift above a gap, a gap above any other conflict (ADR 0007).
async fn failure_ranking<R: Repository, H: HeadFixtures<R>>(repo: &R, fixtures: &H) -> CaseResult {
    let ns = namespace();
    // Sorted by ID, the conflict stream comes first, then the gap, then the drift.
    let conflict = Stream::new(&ns, "a-conflict", "ledger");
    let gap = Stream::new(&ns, "b-gap", "ledger");
    let drift = Stream::new(&ns, "c-drift", "ledger");

    conflict.commit(repo, conflict.events(1..=2), None).await?;
    gap.commit(repo, gap.events(1..=1), None).await?;
    drift.commit(repo, drift.events(1..=2), None).await?;
    fixtures.append_bypassing_head(&drift.event(3, 3)).await?;

    // Each commit below fails on every stream it names.
    let stale = || write(&conflict, 2..=2); // head is 2, not 1
    let skipping = || write(&gap, 3..=3); // head is 1, not 2
    let drifted = || write(&drift, 4..=4); // head is 2 but the log reaches 3

    expect_conflict(
        repo.commit_transaction(TransactionCommit::new(vec![stale()]))
            .await,
        "a stale append alone",
    )?;

    let result = repo
        .commit_transaction(TransactionCommit::new(vec![stale(), skipping()]))
        .await;
    ensure!(
        matches!(&result, Err(Error::SequenceGap { entity_id, after: EventSequence(1), found: EventSequence(3) }) if *entity_id == gap.id),
        "a gap outranks a conflict that comes first: {result:?}"
    );

    expect_drift(
        repo.commit_transaction(TransactionCommit::new(vec![stale(), skipping(), drifted()]))
            .await,
        "drift outranks a gap and a conflict",
        &drift.id,
        Some(2),
        3,
    )?;

    // A stale fence is a conflict like a stale append, and ranks below a gap.
    let mut fenced = TransactionCommit::new(vec![skipping()]);
    fenced.fences = vec![ReadFence::new(
        conflict.id.clone(),
        conflict.entity_type.clone(),
        EventSequence(1),
        FenceProvenance::Engine,
    )];
    let result = repo.commit_transaction(fenced).await;
    ensure!(
        matches!(&result, Err(Error::SequenceGap { entity_id, .. }) if *entity_id == gap.id),
        "a gap outranks a stale fence: {result:?}"
    );

    // Among conflicts, the first failed item in transaction order is reported: written streams
    // in byte order, then the fences.
    let mut fenced = TransactionCommit::new(vec![stale()]);
    fenced.fences = vec![ReadFence::new(
        gap.id.clone(),
        gap.entity_type.clone(),
        EventSequence(0),
        FenceProvenance::Engine,
    )];
    let result = repo.commit_transaction(fenced).await;
    ensure!(
        matches!(&result, Err(Error::Conflict { entity_id, .. }) if *entity_id == conflict.id),
        "a stale append is reported before a stale fence: {result:?}"
    );

    // A recorded outcome outranks all of it: the command already ran.
    let command = command_id();
    repo.commit_transaction(
        TransactionCommit::<R::Operation>::new(Vec::new())
            .with_outcome(Outcome::accepted(command.clone(), "digest")),
    )
    .await?;
    let result = repo
        .commit_transaction(
            TransactionCommit::new(vec![stale(), skipping(), drifted()])
                .with_outcome(Outcome::accepted(command.clone(), "digest")),
        )
        .await;
    ensure!(
        matches!(&result, Err(Error::OutcomeConflict { command_id }) if *command_id == command),
        "an existing outcome outranks drift, gap, and conflict: {result:?}"
    );

    Ok(())
}

/// Writers that race on one stream lose with conflicts only: the failure-path probe never reads a
/// commit that landed between its reads as drift.
async fn racing_writers_never_read_as_drift<R: Repository>(repo: &R) -> CaseResult {
    let a = Stream::new(&namespace(), "race", "ledger");

    for round in 0..4 {
        let first = round * 6 + 1;
        // Six writers compete for the same sequence of the same stream, all at once.
        let attempts: Vec<_> = (0..6)
            .map(|writer| {
                let event = a.event(first, 100 + writer);

                Box::pin(a.commit(repo, vec![event], None))
                    as std::pin::Pin<Box<dyn Future<Output = Result<(), Error>> + '_>>
            })
            .collect();
        let results = join_all(attempts).await;

        let winners = results.iter().filter(|result| result.is_ok()).count();
        ensure_eq!(winners, 1, "exactly one racing writer commits");
        for result in results {
            if let Err(error) = result {
                ensure!(
                    error.is_conflict(),
                    "a losing writer sees a conflict, not drift: {error:?}"
                );
            }
        }

        // The next round continues from the winner.
        a.commit(repo, vec![a.event(first + 1, 1)], None).await?;
        for tail in 2..6 {
            a.commit(repo, vec![a.event(first + tail, 1)], None).await?;
        }
    }

    ensure_eq!(
        repo.events(&a.id).await?.len(),
        24,
        "every round committed six events"
    );

    Ok(())
}

/// Drives futures concurrently on the current task and returns their outputs in order.
async fn join_all<'f, T>(
    mut futures: Vec<std::pin::Pin<Box<dyn Future<Output = T> + 'f>>>,
) -> Vec<T> {
    let mut outputs: Vec<Option<T>> = futures.iter().map(|_| None).collect();

    std::future::poll_fn(|cx| {
        let mut pending = false;
        for (future, output) in futures.iter_mut().zip(outputs.iter_mut()) {
            if output.is_none() {
                match future.as_mut().poll(cx) {
                    std::task::Poll::Ready(value) => *output = Some(value),
                    std::task::Poll::Pending => pending = true,
                }
            }
        }

        if pending {
            std::task::Poll::Pending
        } else {
            std::task::Poll::Ready(())
        }
    })
    .await;

    outputs
        .into_iter()
        .map(|output| output.expect("polled to completion"))
        .collect()
}

async fn compact_below<R: Repository>(repo: &R) -> CaseResult {
    let ns = namespace();
    let stream = Stream::new(&ns, "1", "ledger");
    stream
        .commit(
            repo,
            stream.events(1..=4),
            Some(stream.new_snapshot(1, "{}")),
        )
        .await?;
    stream.commit(repo, stream.events(5..=6), None).await?;

    for through in [0, -1] {
        ensure_eq!(
            repo.compact_below(&stream.id, EventSequence(through))
                .await?,
            0,
            format!("compact_below({through}) is a no-op")
        );
    }

    expect_err(
        repo.compact_below(&stream.id, EventSequence(5)).await,
        "compacting past the snapshot",
        |err| {
            matches!(
                err,
                Error::CompactionUncovered {
                    covered: Some(EventSequence(4)),
                    ..
                }
            )
        },
    )?;
    ensure_eq!(
        repo.events(&stream.id).await?,
        stream.events(1..=6),
        "a refused compaction deletes nothing"
    );

    ensure_eq!(
        repo.compact_below(&stream.id, EventSequence(3)).await?,
        3,
        "compact_below(3) deletes events 1-3"
    );
    ensure_eq!(
        repo.compact_below(&stream.id, EventSequence(4)).await?,
        1,
        "compact_below(4) deletes the remaining covered event"
    );
    ensure_eq!(
        repo.compact_below(&stream.id, EventSequence(4)).await?,
        0,
        "compaction is idempotent"
    );
    ensure_eq!(
        repo.events(&stream.id).await?,
        stream.events(5..=6),
        "uncovered events remain"
    );
    ensure_eq!(
        repo.snapshot(&stream.id).await?,
        Some(stream.snapshot(1, 4, "{}")),
        "compaction never deletes the snapshot"
    );

    let unsnapshotted = Stream::new(&ns, "2", "ledger");
    unsnapshotted
        .commit(repo, unsnapshotted.events(1..=2), None)
        .await?;
    let missing = Stream::new(&ns, "missing", "ledger");
    for (stream, what) in [(&unsnapshotted, "no snapshot"), (&missing, "no stream")] {
        expect_err(
            repo.compact_below(&stream.id, EventSequence(1)).await,
            &format!("compacting a stream with {what}"),
            |err| matches!(err, Error::CompactionUncovered { covered: None, .. }),
        )?;
    }

    Ok(())
}

async fn compacted_floor<R: Repository>(repo: &R) -> CaseResult {
    let stream = Stream::new(&namespace(), "1", "ledger");
    stream
        .commit(
            repo,
            stream.events(1..=4),
            Some(stream.new_snapshot(1, "{}")),
        )
        .await?;
    repo.compact_below(&stream.id, EventSequence(4)).await?;

    // A writer that loaded before the snapshot commits sequence 3 again. Its key was compacted
    // away, so only the snapshot floor rejects it.
    expect_conflict(
        stream.commit(repo, stream.events(3..=3), None).await,
        "a commit below the floor",
    )?;
    expect_conflict(
        stream
            .commit(
                repo,
                stream.events(4..=5),
                Some(stream.new_snapshot(2, "{}")),
            )
            .await,
        "a snapshot commit that starts at the floor",
    )?;
    ensure!(
        repo.events(&stream.id).await?.is_empty(),
        "rejected commits write nothing"
    );

    stream.commit(repo, stream.events(5..=5), None).await?;
    ensure_eq!(
        repo.events(&stream.id).await?,
        stream.events(5..=5),
        "a commit above the floor succeeds"
    );

    Ok(())
}

/// A one-stream write, for transactions that append to a single stream.
fn write<Op>(entity: &Stream, sequences: RangeInclusive<i64>) -> Commit<Op> {
    Commit {
        entity_id: entity.id.clone(),
        events: entity.events(sequences),
        snapshot: None,
        operations: Vec::new(),
        views: Vec::new(),
        outcome: None,
    }
}

async fn multi_stream_commit<R: Repository>(repo: &R) -> CaseResult {
    let ns = namespace();
    let a = Stream::new(&ns, "a", "ledger");
    let b = Stream::new(&ns, "b", "ledger");

    // Deliberately declared out of bytewise stream order: the commit pins the order itself, and
    // each stream keeps its own ascending sequences.
    let commit = TransactionCommit::new(vec![write(&b, 1..=1), write(&a, 1..=2)]);
    repo.commit_transaction(commit).await?;

    ensure_eq!(repo.events(&a.id).await?, a.events(1..=2), "events(a)");
    ensure_eq!(repo.events(&b.id).await?, b.events(1..=1), "events(b)");

    Ok(())
}

async fn transaction_first_snapshot<R: Repository>(repo: &R) -> CaseResult {
    let ns = namespace();
    let a = Stream::new(&ns, "a", "ledger");
    let b = Stream::new(&ns, "b", "ledger");

    // A stream's first snapshot is sequence 1 on every backend, also in a multi-stream commit
    // that creates the stream.
    let skipping = Commit {
        snapshot: Some(a.new_snapshot(3, "{}")),
        ..write(&a, 1..=2)
    };
    expect_conflict(
        repo.commit_transaction(TransactionCommit::new(vec![write(&b, 1..=1), skipping]))
            .await,
        "a first snapshot whose sequence is not 1",
    )?;
    ensure!(
        repo.events(&a.id).await?.is_empty()
            && repo.events(&b.id).await?.is_empty()
            && repo.snapshot(&a.id).await?.is_none(),
        "the rejected commit writes nothing, in any stream"
    );

    let accepted = Commit {
        snapshot: Some(a.new_snapshot(1, "{}")),
        ..write(&a, 1..=2)
    };
    repo.commit_transaction(TransactionCommit::new(vec![write(&b, 1..=1), accepted]))
        .await?;
    ensure_eq!(
        repo.snapshot(&a.id).await?,
        Some(a.snapshot(1, 2, "{}")),
        "sequence 1 is accepted"
    );

    Ok(())
}

async fn mixed_type_transaction<R: Repository>(repo: &R) -> CaseResult {
    use crate::Transaction;

    let ns = namespace();
    let ledger_id = EntityId::new(format!("{ns}-ledger"));
    let claim_id = EntityId::new(format!("{ns}-claim"));
    let metadata = Metadata::new("conformance");

    // One commit appends to a ledger and to a claim, two different aggregate types.
    let mut tx = Transaction::new(repo).with_snapshot_interval(0);
    let mut ledger = tx.read::<Ledger>(ledger_id.clone()).await?;
    let mut claim = tx.read::<Claim>(claim_id.clone()).await?;
    tx.append(&mut ledger, vec![Added { amount: 5 }.into()])?;
    tx.append(
        &mut claim,
        vec![
            Taken {
                holder: "ada".to_owned(),
            }
            .into(),
        ],
    )?;
    let committed = tx.commit(&metadata).await?;

    ensure_eq!(committed.len(), 2, "both streams are committed");
    let ledger_events = repo.events(&ledger_id).await?;
    let claim_events = repo.events(&claim_id).await?;
    ensure!(
        ledger_events.len() == 1
            && ledger_events[0].entity_type == EntityType::new(Ledger::TYPE)
            && claim_events.len() == 1
            && claim_events[0].entity_type == EntityType::new(Claim::TYPE),
        "each stream is stored under its own aggregate type"
    );
    ensure_eq!(
        ledger_events[0].metadata.commit_id,
        claim_events[0].metadata.commit_id,
        "one commit ID across both aggregate types"
    );

    // A read-only stream of another type is fenced with engine provenance: once another writer
    // moves the claim, the transaction that read it fails with a retryable fence conflict.
    let mut tx = Transaction::new(repo).with_snapshot_interval(0);
    let mut ledger = tx.read::<Ledger>(ledger_id.clone()).await?;
    tx.read::<Claim>(claim_id.clone()).await?;
    tx.append(&mut ledger, vec![Added { amount: 1 }.into()])?;

    // Another writer appends to the claim at its next sequence.
    let stream = Stream::new(&ns, "claim", "claim");
    stream.commit(repo, vec![stream.event(2, 1)], None).await?;

    let result = tx.commit(&metadata).await;
    ensure!(
        matches!(
            &result,
            Err(Error::FenceConflict { entity_id, head: EventSequence(1), provenance: FenceProvenance::Engine })
                if *entity_id == claim_id
        ),
        "the claim moved under a transaction that read it: {result:?}"
    );
    ensure_eq!(
        repo.events(&ledger_id).await?.len(),
        1,
        "the fenced transaction wrote nothing"
    );

    Ok(())
}

async fn sequence_gaps<R: Repository>(repo: &R) -> CaseResult {
    let ns = namespace();
    let gap = |result: Result<(), Error>, id: &EntityId, after: i64, found: i64| match result {
        Err(Error::SequenceGap {
            entity_id,
            after: stored,
            found: first,
        }) if entity_id == *id
            && stored == EventSequence(after)
            && first == EventSequence(found) =>
        {
            Ok(())
        }
        other => Err(Failure(format!(
            "want a sequence gap on {id} after {after} found {found}, got {other:?}"
        ))),
    };

    // A first event above sequence 1 on a stream with no events is a gap.
    let new = Stream::new(&ns, "new", "ledger");
    gap(
        new.commit(repo, new.events(3..=3), None).await,
        &new.id,
        0,
        3,
    )?;
    ensure!(
        repo.events(&new.id).await?.is_empty(),
        "a refused gap writes nothing"
    );

    // Skipping a sequence on an existing stream is a gap; continuing from the head is not.
    let a = Stream::new(&ns, "a", "ledger");
    let b = Stream::new(&ns, "b", "ledger");
    a.commit(repo, a.events(1..=1), None).await?;
    gap(a.commit(repo, a.events(3..=3), None).await, &a.id, 1, 3)?;

    // One stream of a multi-stream commit with a gap fails the whole commit.
    let mixed = TransactionCommit::new(vec![write(&b, 1..=1), write(&a, 3..=3)]);
    gap(repo.commit_transaction(mixed).await, &a.id, 1, 3)?;
    ensure!(
        repo.events(&b.id).await?.is_empty(),
        "the clean stream of the failed commit is untouched"
    );

    a.commit(repo, a.events(2..=2), None).await?;

    // Compaction can delete every event a snapshot covers, including the head: the snapshot's
    // event sequence is then the head, so the stream still continues from it and still refuses a
    // gap.
    let c = Stream::new(&ns, "c", "ledger");
    c.commit(repo, c.events(1..=3), Some(c.new_snapshot(1, "{}")))
        .await?;
    ensure_eq!(
        repo.compact_below(&c.id, EventSequence(3)).await?,
        3,
        "the head event is compacted away"
    );
    gap(c.commit(repo, c.events(5..=5), None).await, &c.id, 3, 5)?;
    c.commit(repo, c.events(4..=4), None).await?;

    Ok(())
}

async fn multi_stream_atomicity<R: Repository>(repo: &R) -> CaseResult {
    let ns = namespace();
    let a = Stream::new(&ns, "a", "ledger");
    let b = Stream::new(&ns, "b", "ledger");

    a.commit(repo, a.events(1..=2), None).await?;

    // The sibling stream's event collides with nothing; stream a's reuses sequence 1.
    let collision = Commit {
        events: vec![a.event(1, 99)],
        ..write(&a, 1..=1)
    };
    let commit = TransactionCommit::new(vec![write(&b, 1..=1), collision]);
    let result = repo.commit_transaction(commit).await;

    ensure!(
        matches!(&result, Err(Error::Conflict { entity_id, .. }) if *entity_id == a.id),
        "a commit that collides on one stream must fail with a conflict"
    );

    // All-or-nothing: the sibling stream that shared the failed commit must be untouched.
    ensure!(
        repo.events(&b.id).await?.is_empty(),
        "the sibling stream of a failed commit is untouched"
    );

    // The conflict names the stream whose action failed, not the first stream of the commit: the
    // collision here is on `d`, which sorts after the clean streams `c` and `e`.
    let c = Stream::new(&ns, "c", "ledger");
    let d = Stream::new(&ns, "d", "ledger");
    let e = Stream::new(&ns, "e", "ledger");
    d.commit(repo, d.events(1..=1), None).await?;

    let collision = Commit {
        events: vec![d.event(1, 99)],
        ..write(&d, 1..=1)
    };
    let commit = TransactionCommit::new(vec![write(&c, 1..=1), collision, write(&e, 1..=1)]);
    let result = repo.commit_transaction(commit).await;

    ensure!(
        matches!(&result, Err(Error::Conflict { entity_id, .. }) if *entity_id == d.id),
        "a conflict on a later stream is attributed to that stream, got {result:?}"
    );
    ensure!(
        repo.events(&c.id).await?.is_empty() && repo.events(&e.id).await?.is_empty(),
        "the clean streams of the failed commit are untouched"
    );

    Ok(())
}

async fn read_fences<R: Repository>(repo: &R) -> CaseResult {
    let ns = namespace();
    let a = Stream::new(&ns, "a", "ledger");
    let c = Stream::new(&ns, "c", "ledger");
    let absent = Stream::new(&ns, "absent", "ledger");

    a.commit(repo, a.events(1..=2), None).await?;

    // A fence at the observed head passes while the transaction writes another stream. A fence
    // on an absent stream (head 0) passes too.
    let fence = ReadFence::new(
        a.id.clone(),
        a.entity_type.clone(),
        EventSequence(2),
        FenceProvenance::Engine,
    );
    let mut commit = TransactionCommit::new(vec![write(&c, 1..=1)]);
    commit.fences = vec![
        fence.clone(),
        ReadFence::new(
            absent.id.clone(),
            absent.entity_type.clone(),
            EventSequence::ZERO,
            FenceProvenance::Engine,
        ),
    ];
    repo.commit_transaction(commit).await?;

    // Another writer moves the fenced stream past the head.
    a.commit(repo, vec![a.event(3, 3)], None).await?;

    // A stale fence now fails the whole commit, and the conflict names the fenced stream.
    let before = repo.events(&c.id).await?.len();
    let mut commit = TransactionCommit::new(vec![write(&c, 2..=2)]);
    commit.fences = vec![fence];
    let Err(err) = repo.commit_transaction(commit).await else {
        return Err(Failure("a stale fence must fail the commit".into()));
    };

    ensure!(
        matches!(
            &err,
            Error::FenceConflict { entity_id, head: EventSequence(2), provenance: FenceProvenance::Engine }
                if *entity_id == a.id
        ),
        "a stale fence fails with a fence conflict naming the fenced stream and its fenced head"
    );
    ensure!(
        err.is_conflict(),
        "a fence conflict is an optimistic-concurrency conflict"
    );
    ensure!(
        err.is_retryable(),
        "an engine-filled fence conflict retries like an event-put conflict"
    );

    // Nothing from the failed commit was written.
    ensure_eq!(
        repo.events(&c.id).await?.len(),
        before,
        "a failed fenced commit writes nothing"
    );

    // A fence at head 0 is `attribute_not_exists(head)`: it holds while the stream has never been
    // written and fails once anyone creates it.
    absent.commit(repo, absent.events(1..=1), None).await?;
    let mut commit = TransactionCommit::new(vec![write(&c, 2..=2)]);
    commit.fences = vec![ReadFence::new(
        absent.id.clone(),
        absent.entity_type.clone(),
        EventSequence::ZERO,
        FenceProvenance::Engine,
    )];
    let result = repo.commit_transaction(commit).await;
    ensure!(
        matches!(&result, Err(Error::FenceConflict { entity_id, head: EventSequence(0), .. }) if *entity_id == absent.id),
        "a fence at head 0 fails once the stream exists: {result:?}"
    );
    ensure_eq!(
        repo.events(&c.id).await?.len(),
        before,
        "the commit with the stale head-0 fence wrote nothing"
    );

    Ok(())
}

async fn commit_too_large<R: Repository>(repo: &R) -> CaseResult {
    let stream = Stream::new(&namespace(), "large", "ledger");

    // An append costs the same as a create (ADR 0007): the feature-marker check, the rebuild-lock
    // check, the head update, and 97 event puts are exactly 100 actions. There is no continuation
    // check.
    let grows = Stream::new(&namespace(), "grows", "ledger");
    grows.commit(repo, grows.events(1..=1), None).await?;
    let result = grows.commit(repo, grows.events(2..=99), None).await;
    ensure!(
        matches!(&result, Err(Error::CommitTooLarge { actions: 101, .. })),
        "98 appended events are one action too many: {result:?}"
    );
    grows.commit(repo, grows.events(2..=98), None).await?;
    ensure_eq!(
        repo.events(&grows.id).await?.len(),
        98,
        "97 appended events fit"
    );

    // The same 97 events fit when the commit creates the stream. Only emitted actions count.
    let fits = Stream::new(&namespace(), "fits", "ledger");
    fits.commit(repo, fits.events(1..=97), None).await?;
    ensure_eq!(
        repo.events(&fits.id).await?.len(),
        97,
        "a commit that fills the budget exactly is accepted"
    );

    // A snapshot put is one more action beside the head update, so a commit that creates a stream
    // and writes its first snapshot holds 96 events, not 97: the one case that costs more.
    let snapshotted = Stream::new(&namespace(), "snapshotted", "ledger");
    let result = snapshotted
        .commit(
            repo,
            snapshotted.events(1..=97),
            Some(snapshotted.new_snapshot(1, "{}")),
        )
        .await;
    ensure!(
        matches!(&result, Err(Error::CommitTooLarge { actions: 101, .. })),
        "97 events and a snapshot are one action too many: {result:?}"
    );
    snapshotted
        .commit(
            repo,
            snapshotted.events(1..=96),
            Some(snapshotted.new_snapshot(1, "{}")),
        )
        .await?;

    // 101 event puts plus the control rows and the head update exceed the 100-item budget.
    let result = stream.commit(repo, stream.events(1..=101), None).await;

    ensure!(
        matches!(&result, Err(Error::CommitTooLarge { .. })),
        "an oversized commit is refused with CommitTooLarge"
    );
    ensure!(
        repo.events(&stream.id).await?.is_empty(),
        "an oversized commit is refused before any write"
    );

    Ok(())
}

async fn fence_compaction_aba<R: Repository>(repo: &R) -> CaseResult {
    let ns = namespace();
    let a = Stream::new(&ns, "a", "ledger");
    let c = Stream::new(&ns, "c", "ledger");

    a.commit(repo, a.events(1..=3), Some(a.new_snapshot(1, "{}")))
        .await?;

    // Compaction deletes the events the snapshot covers, which once emptied the slot a stale
    // fence checked. The fence is now a condition on the stream's head, which only increases and
    // which compaction never touches, so there is nothing to mitigate: the stale fence fails.
    repo.compact_below(&a.id, EventSequence(3)).await?;

    let mut commit = TransactionCommit::new(vec![write(&c, 1..=1)]);
    commit.fences = vec![ReadFence::new(
        a.id.clone(),
        a.entity_type.clone(),
        EventSequence(1),
        FenceProvenance::Engine,
    )];

    ensure!(
        matches!(
            repo.commit_transaction(commit).await,
            Err(Error::FenceConflict { .. })
        ),
        "append, snapshot, compact must fail a stale fence"
    );

    Ok(())
}

async fn entity_heads<R: Repository>(repo: &R) -> CaseResult {
    let ns = namespace();
    let a1 = Stream::new(&ns, "alpha-1", "alpha");
    let a2 = Stream::new(&ns, "alpha-2", "alpha");
    let b1 = Stream::new(&ns, "beta-1", "beta");

    a1.commit(repo, a1.events(1..=3), None).await?;
    a2.commit(repo, a2.events(1..=2), Some(a2.new_snapshot(1, "{}")))
        .await?;
    repo.compact_below(&a2.id, EventSequence(2)).await?;
    b1.commit(repo, b1.events(1..=1), None).await?;

    let want_alpha = BTreeMap::from([
        (a1.id.clone(), EventSequence(3)),
        (a2.id.clone(), EventSequence(2)),
    ]);
    ensure_eq!(
        repo.entity_heads(Some(&a1.entity_type)).await?,
        want_alpha,
        "heads filtered by type, including a fully compacted stream"
    );

    let all = repo.entity_heads(None).await?;
    for (id, head) in want_alpha.iter().chain([(&b1.id, &EventSequence(1))]) {
        ensure_eq!(all.get(id), Some(head), format!("unfiltered head of {id}"));
    }

    // An explicit empty type matches only streams stored with an empty type, not every stream.
    let untyped = Stream {
        id: EntityId::new(format!("{ns}-untyped")),
        entity_type: EntityType::default(),
    };
    untyped.commit(repo, untyped.events(1..=1), None).await?;
    let empty = repo.entity_heads(Some(&EntityType::default())).await?;
    ensure_eq!(
        empty.get(&untyped.id),
        Some(&EventSequence(1)),
        "the empty-type filter includes the untyped stream"
    );
    ensure!(
        !empty.contains_key(&a1.id) && !empty.contains_key(&b1.id),
        "the empty-type filter excludes typed streams"
    );

    let unknown = EntityType::new(format!("{ns}-unknown"));
    ensure!(
        repo.entity_heads(Some(&unknown)).await?.is_empty(),
        "an unknown type has no heads"
    );

    Ok(())
}

async fn latest_n<R: Repository>(repo: &R) -> CaseResult {
    let stream = Stream::new(&namespace(), "latest", "ledger");

    // A snapshot and compaction in the middle of the stream: `latest` reads the stored events
    // only, never the snapshot item, whether or not earlier events were compacted away.
    stream
        .commit(
            repo,
            stream.events(1..=3),
            Some(stream.new_snapshot(1, "{}")),
        )
        .await?;
    stream.commit(repo, stream.events(4..=6), None).await?;
    repo.compact_below(&stream.id, EventSequence(3)).await?;

    let descending = |sequences: &[i64]| -> Vec<SerializedEvent> {
        sequences
            .iter()
            .rev()
            .map(|sequence| stream.event(*sequence, *sequence))
            .collect()
    };

    ensure_eq!(
        repo.latest(&stream.id, 3).await?,
        descending(&[4, 5, 6]),
        "latest(3) over a compacted stream"
    );
    ensure_eq!(
        repo.latest(&stream.id, 2).await?,
        descending(&[5, 6]),
        "latest(2)"
    );
    ensure_eq!(
        repo.latest(&stream.id, 1).await?,
        descending(&[6]),
        "latest(1)"
    );
    ensure_eq!(
        repo.latest(&stream.id, 10).await?,
        descending(&[4, 5, 6]),
        "latest(10) returns the whole remaining stream"
    );

    for count in [i32::MAX as usize + 1, usize::MAX] {
        ensure_eq!(
            repo.latest(&stream.id, count).await?,
            descending(&[4, 5, 6]),
            "latest with an oversized count returns stored events without overflowing"
        );
    }

    let missing = Stream::new(&namespace(), "latest-missing", "ledger");
    ensure!(
        repo.latest(&missing.id, 3).await?.is_empty(),
        "a missing entity has no latest events"
    );

    expect_err(repo.latest(&stream.id, 0).await, "latest(0)", |err| {
        matches!(err, Error::InvalidArgument(_))
    })?;

    Ok(())
}

async fn reserved_namespaces<R: Repository>(repo: &R) -> CaseResult {
    let ns = namespace();

    // The reserved prefix starts the entity ID itself, so these are not namespaced like the
    // other cases' IDs. They are refused before any write, so nothing collides across runs.
    let reserved: Vec<EntityId> = ["SYSTEM#", "command-outcome#"]
        .into_iter()
        .map(|prefix| EntityId::new(format!("{prefix}{ns}-acct")))
        .collect();

    for id in &reserved {
        let template = Stream::new(&ns, "template", "ledger");
        let events = vec![SerializedEvent {
            entity_id: id.clone(),
            id: EventId::for_event(id, EventSequence(1)),
            ..template.event(1, 1)
        }];
        expect_err(
            repo.commit(Commit {
                entity_id: id.clone(),
                events,
                snapshot: None,
                operations: Vec::new(),
                views: Vec::new(),
                outcome: None,
            })
            .await,
            &format!("committing to {id}"),
            |err| matches!(err, Error::ReservedEntityId { .. }),
        )?;
        ensure!(
            repo.events(id).await?.is_empty() && repo.snapshot(id).await?.is_none(),
            "the refused commit wrote nothing to {id}"
        );

        expect_err(
            repo.put_snapshot(&SerializedSnapshot {
                entity_type: EntityType::new(format!("{ns}-ledger")),
                entity_id: id.clone(),
                sequence: EventSequence(1),
                event_sequence: EventSequence(1),
                payload: b"{}".to_vec(),
            })
            .await,
            &format!("put_snapshot at {id}"),
            |err| matches!(err, Error::ReservedEntityId { .. }),
        )?;

        for through in [-1, 0, 1] {
            expect_err(
                repo.compact_below(id, EventSequence(through)).await,
                &format!("compact_below({through}) at {id}"),
                |err| matches!(err, Error::ReservedEntityId { .. }),
            )?;
        }
    }

    let heads = repo.entity_heads(None).await?;
    for id in &reserved {
        ensure!(
            !heads.contains_key(id),
            "the refused writes left no head for {id}"
        );
    }

    Ok(())
}

fn metadata() -> Metadata {
    Metadata::new("local").with_origin("conformance", "suite")
}

fn ledger_id() -> EntityId {
    EntityId::new(format!("{}-ledger", namespace()))
}

async fn store_snapshots<R: Repository>(repo: &R) -> CaseResult {
    let store = Store::<Ledger, _>::new(repo, ()).with_snapshot_interval(2);
    let id = ledger_id();

    for amount in 1..=3 {
        store.execute(&id, Add(amount), metadata()).await?;
    }

    let loaded = store.load(&id).await?;
    ensure_eq!(
        loaded.aggregate(),
        &Ledger {
            total: 6,
            entries: 3
        },
        "loaded state"
    );
    ensure_eq!(loaded.sequence(), EventSequence(3), "loaded sequence");
    ensure_eq!(
        loaded.snapshot_sequence(),
        EventSequence(1),
        "loaded snapshot sequence"
    );

    let snapshot = repo
        .snapshot(&id)
        .await?
        .ok_or(Failure("no snapshot".into()))?;
    ensure_eq!(
        (snapshot.sequence, snapshot.event_sequence),
        (EventSequence(1), EventSequence(2)),
        "the snapshot taken at the interval"
    );
    ensure_eq!(
        serde_json::from_slice::<Ledger>(&snapshot.payload)?,
        Ledger {
            total: 3,
            entries: 2
        },
        "the snapshot state"
    );

    repo.compact_below(&id, EventSequence(2)).await?;
    let reloaded = store.load(&id).await?;
    ensure_eq!(
        (reloaded.aggregate(), reloaded.sequence()),
        (loaded.aggregate(), loaded.sequence()),
        "a compacted stream loads the same state from its snapshot"
    );

    let executed = store.execute(&id, Add(4), metadata()).await?;
    ensure_eq!(
        (
            executed.state.sequence(),
            executed.state.snapshot_sequence()
        ),
        (EventSequence(4), EventSequence(2)),
        "the next interval snapshots again"
    );

    Ok(())
}

async fn store_stale_commit<R: Repository>(repo: &R) -> CaseResult {
    let store = Store::<Ledger, _>::new(repo, ());
    let id = ledger_id();
    store.execute(&id, Add(1), metadata()).await?;

    let first = store.load(&id).await?;
    let second = store.load(&id).await?;

    store
        .commit(first, vec![Added { amount: 2 }.into()], &metadata())
        .await?;
    expect_conflict(
        store
            .commit(second, vec![Added { amount: 3 }.into()], &metadata())
            .await,
        "committing from a stale load",
    )?;

    let loaded = store.load(&id).await?;
    ensure_eq!(
        (loaded.aggregate().total, loaded.sequence()),
        (3, EventSequence(2)),
        "only the first writer's event is stored"
    );

    Ok(())
}

/// Lets another writer act once, at a moment chosen by `interference`.
struct Interfering<'a, R> {
    inner: &'a R,
    armed: AtomicBool,
    interference: Interference,
}

enum Interference {
    /// Commit the same sequences just before the first commit.
    CompetingCommit,
    /// Between the snapshot read and the event read of the first load: append `appended` events
    /// with a snapshot every five, and compact through the new snapshot.
    CompactDuringLoad { appended: usize },
}

impl<R: Repository> Interfering<'_, R> {
    fn fires(&self, interference: &Interference) -> bool {
        std::mem::discriminant(&self.interference) == std::mem::discriminant(interference)
            && self.armed.swap(false, Ordering::SeqCst)
    }

    async fn compact_ahead(&self, entity_id: &EntityId, appended: usize) -> Result<(), Error> {
        let other = Store::<Ledger, _>::new(self.inner, ()).with_snapshot_interval(5);

        for _ in 0..appended {
            other
                .execute(entity_id, Add(1), metadata())
                .await
                .map_err(|err| Error::backend(err.to_string()))?;
        }

        let covered = self
            .inner
            .snapshot(entity_id)
            .await?
            .map_or(EventSequence::ZERO, |snapshot| snapshot.event_sequence);
        self.inner.compact_below(entity_id, covered).await?;

        Ok(())
    }
}

impl<R: Repository> Repository for Interfering<'_, R> {
    type Operation = R::Operation;

    fn writes_views(&self) -> bool {
        self.inner.writes_views()
    }

    fn applies_expected_payload(&self) -> bool {
        self.inner.applies_expected_payload()
    }

    fn declare_history_dependent(
        &self,
        types: &[EntityType],
    ) -> impl Future<Output = Result<(), Error>> + Send {
        self.inner.declare_history_dependent(types)
    }

    async fn commit(&self, commit: Commit<R::Operation>) -> Result<(), Error> {
        if self.fires(&Interference::CompetingCommit) {
            let competing = commit
                .events
                .iter()
                .map(|event| SerializedEvent {
                    payload: br#"{"amount":100}"#.to_vec(),
                    metadata: Metadata::default(),
                    ..event.clone()
                })
                .collect();

            self.inner
                .commit(Commit {
                    entity_id: commit.entity_id.clone(),
                    events: competing,
                    snapshot: None,
                    operations: Vec::new(),
                    views: Vec::new(),
                    outcome: None,
                })
                .await?;
        }

        self.inner.commit(commit).await
    }

    async fn outcome(&self, command_id: &CommandId) -> Result<Option<Outcome>, Error> {
        self.inner.outcome(command_id).await
    }

    async fn events(&self, entity_id: &EntityId) -> Result<Vec<SerializedEvent>, Error> {
        self.inner.events(entity_id).await
    }

    async fn events_after(
        &self,
        entity_id: &EntityId,
        after: EventSequence,
    ) -> Result<Vec<SerializedEvent>, Error> {
        if let Interference::CompactDuringLoad { appended } = self.interference
            && self.fires(&self.interference)
        {
            self.compact_ahead(entity_id, appended).await?;
        }

        self.inner.events_after(entity_id, after).await
    }

    async fn latest(&self, entity_id: &EntityId, n: usize) -> Result<Vec<SerializedEvent>, Error> {
        self.inner.latest(entity_id, n).await
    }

    async fn snapshot(&self, entity_id: &EntityId) -> Result<Option<SerializedSnapshot>, Error> {
        self.inner.snapshot(entity_id).await
    }

    async fn put_snapshot(&self, snapshot: &SerializedSnapshot) -> Result<bool, Error> {
        self.inner.put_snapshot(snapshot).await
    }

    async fn compact_below(
        &self,
        entity_id: &EntityId,
        through: EventSequence,
    ) -> Result<usize, Error> {
        self.inner.compact_below(entity_id, through).await
    }

    async fn entity_heads(
        &self,
        entity_type: Option<&EntityType>,
    ) -> Result<BTreeMap<EntityId, EventSequence>, Error> {
        self.inner.entity_heads(entity_type).await
    }
}

async fn store_load_during_compaction<R: Repository>(repo: &R) -> CaseResult {
    let racing = |appended| Interfering {
        inner: repo,
        armed: AtomicBool::new(true),
        interference: Interference::CompactDuringLoad { appended },
    };
    let prepare = || async {
        let id = ledger_id();
        let store = Store::<Ledger, _>::new(repo, ()).with_snapshot_interval(5);
        for _ in 0..5 {
            store.execute(&id, Add(1), metadata()).await?;
        }

        Ok::<_, Failure>(id)
    };

    // The load reads the snapshot at 5, then another writer reaches 11 and compacts through 10.
    // Reading events after 5 returns only 11, which must not be applied to the state at 5.
    let id = prepare().await?;
    let loaded = Store::<Ledger, _>::new(racing(6), ()).load(&id).await?;
    ensure_eq!(
        (loaded.aggregate().total, loaded.sequence()),
        (11, EventSequence(11)),
        "the load starts over from the newer snapshot"
    );

    // With five appended, the other writer stops at 10 and compacts through it, so the event read
    // is empty and the load returns the older state. A commit from it is below the new floor.
    let id = prepare().await?;
    let store = Store::<Ledger, _>::new(racing(5), ()).with_snapshot_interval(5);
    let stale = store.load(&id).await?;
    ensure_eq!(stale.sequence(), EventSequence(5), "the stale load");
    expect_conflict(
        store
            .commit(stale, vec![Added { amount: 1 }.into()], &metadata())
            .await,
        "a commit from a load that fell behind a compaction",
    )?;

    Ok(())
}

async fn no_sleep(_: Duration) {}

async fn store_retry<R: Repository>(repo: &R) -> CaseResult {
    let interfering = |inner| Interfering {
        inner,
        armed: AtomicBool::new(true),
        interference: Interference::CompetingCommit,
    };

    let store = Store::<Ledger, _>::new(interfering(repo), ()).with_snapshot_interval(0);
    let id = ledger_id();
    let executed = store
        .execute_with_retry(
            &id,
            Add(5),
            metadata(),
            &RetryPolicy::immediate(2),
            no_sleep,
        )
        .await?;
    ensure_eq!(
        (executed.state.aggregate().total, executed.state.sequence()),
        (105, EventSequence(2)),
        "the retry reloads the competing event and commits after it"
    );

    let store = Store::<Ledger, _>::new(interfering(repo), ()).with_snapshot_interval(0);
    let result = store
        .execute_with_retry(
            &ledger_id(),
            Add(5),
            metadata(),
            &RetryPolicy::immediate(1),
            no_sleep,
        )
        .await;
    ensure!(
        result.as_ref().is_err_and(|err| err.is_conflict()),
        "a single attempt returns the conflict: got {result:?}"
    );

    Ok(())
}

async fn transaction_projector<R: Repository, V: ViewTarget<R>>(repo: &R, views: &V) -> CaseResult {
    use crate::Transaction;
    use std::sync::Arc;

    let id = ledger_id();
    let projectors: Vec<Arc<dyn Projector<Ledger, R::Operation>>> =
        vec![Arc::new(views.projector())];

    // Two appends to one handle write the stream's view row once, from the final state. Writing
    // the row per append would touch one item twice in the transaction, which DynamoDB refuses.
    let mut tx = Transaction::new(repo).with_snapshot_interval(0);
    let mut ledger = tx.read::<Ledger>(id.clone()).await?;
    tx.append_with(&mut ledger, vec![Added { amount: 1 }.into()], &projectors)?;
    tx.append_with(&mut ledger, vec![Added { amount: 2 }.into()], &projectors)?;
    tx.commit(&metadata()).await?;

    ensure_eq!(
        views
            .view(&id)
            .await
            .map_err(|err| Failure(err.to_string()))?,
        Some(Ledger {
            total: 3,
            entries: 2
        }),
        "the view reflects both appends"
    );
    ensure_eq!(
        repo.events(&id).await?.len(),
        2,
        "both appends are committed"
    );

    Ok(())
}

async fn store_projector<R: Repository, V: ViewTarget<R>>(repo: &R, views: &V) -> CaseResult {
    let store = Store::<Ledger, _>::new(repo, ()).with_projector(views.projector());
    let id = ledger_id();

    store.execute(&id, Add(5), metadata()).await?;
    ensure_eq!(
        views
            .view(&id)
            .await
            .map_err(|err| Failure(err.to_string()))?,
        Some(Ledger {
            total: 5,
            entries: 1
        }),
        "the view is written with the events"
    );

    let first = store.load(&id).await?;
    let second = store.load(&id).await?;
    store
        .commit(first, vec![Added { amount: 1 }.into()], &metadata())
        .await?;
    expect_conflict(
        store
            .commit(second, vec![Added { amount: 100 }.into()], &metadata())
            .await,
        "committing from a stale load",
    )?;
    ensure_eq!(
        views
            .view(&id)
            .await
            .map_err(|err| Failure(err.to_string()))?,
        Some(Ledger {
            total: 6,
            entries: 2
        }),
        "a rejected commit does not write its view"
    );

    Ok(())
}

/// Runs the enumeration cases (P3) against a repository that implements [`EventLog`]: filtering by
/// entity type, ordering within and across batches, folding entities, reserved namespaces, and
/// cancellation. Like [`Suite`], it tolerates a table that holds data from earlier runs: it filters
/// to entity types unique to this run.
///
/// Reserved-namespace rows cannot be written through the [`Repository`], so a backend that can
/// seed them out of band should do so before calling this and assert they are skipped; the cases
/// here check that no yielded event is in a reserved namespace.
pub async fn enumeration_cases<R>(repo: &R)
where
    R: Repository + EventLog,
{
    let mut failures = Vec::new();

    let mut record = |name: &str, result: CaseResult| match result {
        Ok(()) => println!("ok   {name}"),
        Err(Failure(message)) => {
            println!("FAIL {name}");
            failures.push(format!("{name}: {message}"));
        }
    };

    record(
        "stream_all_events filters by entity type and orders each entity's events",
        enumerate_events(repo).await,
    );
    record(
        "stream_entities folds each entity and reports a gap without ending the stream",
        enumerate_entities(repo).await,
    );
    record(
        "stream_entities_from_snapshots seeds each entity from its snapshot, compacted or not",
        enumerate_from_snapshots(repo).await,
    );
    record(
        "dropping an enumeration stream releases its resources",
        enumerate_cancellation(repo).await,
    );

    if !failures.is_empty() {
        panic!(
            "{} enumeration case(s) failed:\n\n{}",
            failures.len(),
            failures.join("\n\n")
        );
    }
}

async fn drain<S: futures_core::Stream>(stream: S) -> Vec<S::Item> {
    let mut stream = std::pin::pin!(stream);
    let mut items = Vec::new();
    while let Some(item) =
        std::future::poll_fn(|cx| futures_core::Stream::poll_next(stream.as_mut(), cx)).await
    {
        items.push(item);
    }

    items
}

/// Groups streamed events by entity, however the backend batched them.
fn by_entity(
    batches: Vec<Result<Vec<SerializedEvent>, Error>>,
) -> Result<BTreeMap<EntityId, Vec<SerializedEvent>>, Failure> {
    let mut grouped = BTreeMap::<EntityId, Vec<SerializedEvent>>::new();
    for batch in batches {
        let batch = batch?;
        ensure!(!batch.is_empty(), "a batch is never empty");
        for event in batch {
            ensure!(
                event.sequence >= EventSequence(1),
                "an inline snapshot leaked into the enumeration: {}",
                event.id
            );
            ensure!(
                !features::is_reserved(&event.entity_id),
                "a reserved-namespace event leaked into the enumeration: {}",
                event.id
            );
            grouped
                .entry(event.entity_id.clone())
                .or_default()
                .push(event);
        }
    }

    Ok(grouped)
}

async fn enumerate_events<R: Repository + EventLog>(repo: &R) -> CaseResult {
    let ns = namespace();
    let a1 = Stream::new(&ns, "alpha-1", "alpha");
    let a2 = Stream::new(&ns, "alpha-2", "alpha");
    let b1 = Stream::new(&ns, "beta-1", "beta");

    a1.commit(repo, a1.events(1..=3), None).await?;
    // A snapshot item is not an event, and a compacted stream starts above sequence 1.
    a2.commit(repo, a2.events(1..=4), Some(a2.new_snapshot(1, "{}")))
        .await?;
    repo.compact_below(&a2.id, EventSequence(2)).await?;
    b1.commit(repo, b1.events(1..=2), None).await?;

    let alpha = by_entity(
        drain(repo.stream_all_events(StreamFilter::of_type(a1.entity_type.clone()))).await,
    )?;
    ensure_eq!(
        alpha,
        BTreeMap::from([
            (a1.id.clone(), a1.events(1..=3)),
            (a2.id.clone(), a2.events(3..=4)),
        ]),
        "events of one entity type, each entity ascending by sequence"
    );

    let unknown = EntityType::new(format!("{ns}-unknown"));
    ensure!(
        drain(repo.stream_all_events(StreamFilter::of_type(unknown)))
            .await
            .is_empty(),
        "an unknown type streams nothing"
    );

    let all = by_entity(drain(repo.stream_all_events(StreamFilter::all())).await)?;
    for (id, events) in [
        (&a1.id, a1.events(1..=3)),
        (&a2.id, a2.events(3..=4)),
        (&b1.id, b1.events(1..=2)),
    ] {
        ensure_eq!(
            all.get(id),
            Some(&events),
            format!("unfiltered events of {id}")
        );
    }

    // Backends that batch whole entities yield them contiguously, one batch each.
    if R::BATCHES_BY_ENTITY {
        let batches =
            drain(repo.stream_all_events(StreamFilter::of_type(a1.entity_type.clone()))).await;
        let ids: Vec<_> = batches
            .iter()
            .map(|batch| batch.as_ref().map(|events| events[0].entity_id.clone()))
            .collect::<Result<_, _>>()?;
        ensure_eq!(
            ids,
            vec![a1.id.clone(), a2.id.clone()],
            "one batch per entity, ascending by entity ID"
        );
    }

    Ok(())
}

async fn enumerate_entities<R: Repository + EventLog>(repo: &R) -> CaseResult {
    let ns = namespace();
    let whole = Stream::new(&ns, "whole", "ledger");
    let second = Stream::new(&ns, "second", "ledger");
    let compacted = Stream::new(&ns, "compacted", "ledger");
    let other = Stream::new(&ns, "other", "unrelated");

    whole.commit(repo, whole.events(1..=3), None).await?;
    second.commit(repo, second.events(1..=1), None).await?;
    compacted
        .commit(
            repo,
            compacted.events(1..=3),
            Some(compacted.new_snapshot(1, "{}")),
        )
        .await?;
    repo.compact_below(&compacted.id, EventSequence(3)).await?;
    other.commit(repo, other.events(1..=2), None).await?;

    // The ledger's entity type is namespaced, so name it: the filter would default to "ledger".
    let entities =
        drain(repo.stream_entities::<Ledger>(StreamFilter::of_type(whole.entity_type.clone())))
            .await;

    let mut folded = BTreeMap::new();
    let mut gaps = Vec::new();
    for entity in entities {
        match entity {
            Ok(loaded) => {
                folded.insert(
                    loaded.entity_id().clone(),
                    (loaded.aggregate().total, loaded.sequence()),
                );
            }
            Err(Error::SequenceGap { entity_id, .. }) => gaps.push(entity_id),
            Err(err) => return Err(Failure(format!("unexpected entity error: {err}"))),
        }
    }

    ensure_eq!(
        folded,
        BTreeMap::from([
            (whole.id.clone(), (6, EventSequence(3))),
            (second.id.clone(), (1, EventSequence(1))),
        ]),
        "folded entities (the payload amount is the sequence)"
    );
    ensure_eq!(
        gaps,
        Vec::<EntityId>::new(),
        "a fully compacted stream has no events to fold"
    );

    // A stream compacted part-way has a gap at its start: the error does not end the stream.
    let partial = Stream::new(&ns, "partial", "ledger");
    partial
        .commit(
            repo,
            partial.events(1..=4),
            Some(partial.new_snapshot(1, "{}")),
        )
        .await?;
    repo.compact_below(&partial.id, EventSequence(2)).await?;
    let entities =
        drain(repo.stream_entities::<Ledger>(StreamFilter::of_type(whole.entity_type.clone())))
            .await;
    let gap_ids: Vec<_> = entities
        .iter()
        .filter_map(|entity| match entity {
            Err(Error::SequenceGap { entity_id, .. }) => Some(entity_id.clone()),
            _ => None,
        })
        .collect();
    ensure_eq!(
        gap_ids,
        vec![partial.id.clone()],
        "the gap names the compacted entity"
    );
    ensure!(
        entities.iter().filter(|entity| entity.is_ok()).count() >= 2,
        "the other entities still stream after the gap"
    );

    Ok(())
}

async fn enumerate_from_snapshots<R: Repository + EventLog>(repo: &R) -> CaseResult {
    let ns = namespace();
    let whole = Stream::new(&ns, "whole", "ledger");
    let partial = Stream::new(&ns, "partial", "ledger");
    let at_head = Stream::new(&ns, "at-head", "ledger");
    let other = Stream::new(&ns, "other", "unrelated");

    whole.commit(repo, whole.events(1..=3), None).await?;
    // Compacted part-way: the snapshot covers 1..=2 (amounts 1 and 2), event 3 remains.
    partial
        .commit(
            repo,
            partial.events(1..=2),
            Some(partial.new_snapshot(1, r#"{"total":3,"entries":2}"#)),
        )
        .await?;
    partial.commit(repo, partial.events(3..=3), None).await?;
    repo.compact_below(&partial.id, EventSequence(2)).await?;
    // Compacted down to the snapshot: no events left.
    at_head
        .commit(
            repo,
            at_head.events(1..=2),
            Some(at_head.new_snapshot(1, r#"{"total":3,"entries":2}"#)),
        )
        .await?;
    repo.compact_below(&at_head.id, EventSequence(2)).await?;
    other.commit(repo, other.events(1..=1), None).await?;

    let entities = drain(
        repo.stream_entities_from_snapshots::<Ledger>(StreamFilter::of_type(
            whole.entity_type.clone(),
        )),
    )
    .await;
    let loaded = entities
        .into_iter()
        .map(|entity| {
            entity.map(|loaded| {
                (
                    loaded.entity_id().clone(),
                    (
                        loaded.aggregate().total,
                        loaded.sequence(),
                        loaded.snapshot_sequence(),
                    ),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    ensure_eq!(
        loaded,
        vec![
            (at_head.id.clone(), (3, EventSequence(2), EventSequence(1))),
            (partial.id.clone(), (6, EventSequence(3), EventSequence(1))),
            (whole.id.clone(), (6, EventSequence(3), EventSequence::ZERO)),
        ],
        "every entity of the type, ascending by ID, from its snapshot plus the events after it"
    );

    Ok(())
}

async fn enumerate_cancellation<R: Repository + EventLog>(repo: &R) -> CaseResult {
    let ns = namespace();
    let stream = Stream::new(&ns, "cancel", "alpha");
    stream.commit(repo, stream.events(1..=2), None).await?;

    // A pooled backend has a handful of connections: leaking one per dropped stream would hang
    // before the loop ends.
    for _ in 0..16 {
        let events = repo.stream_all_events(StreamFilter::of_type(stream.entity_type.clone()));
        let mut events = std::pin::pin!(events);
        let first =
            std::future::poll_fn(|cx| futures_core::Stream::poll_next(events.as_mut(), cx)).await;
        ensure!(first.is_some(), "the first batch arrives before the drop");
    }

    stream.commit(repo, stream.events(3..=3), None).await?;
    ensure_eq!(
        repo.events(&stream.id).await?.len(),
        3,
        "the repository works after dropped streams"
    );

    Ok(())
}

/// Runs the feature-marker cases (v0.9, C9) against a repository that stores the
/// `SYSTEM#FEATURES` marker row. Like [`Suite`], it tolerates a table that holds data from
/// earlier runs: it resets the manifest first, so features a previous run left behind cannot
/// brick the table, and it leaves the manifest declaring only `stream-heads` at the end. The table
/// must already be head-aware (ADR 0007): a table whose data predates stream heads is refused by
/// every writer until it is migrated.
///
/// The cases use two writers — `first` and `second` — because the refusal binds a writer whose
/// cached validation predates a declaration made by another writer. Pass two handles over the
/// same storage that each keep their own state, such as a repository and its clone (mem) or two
/// repositories built on the same client or pool (the live backends).
pub async fn feature_marker_cases<R>(first: &R, second: &R)
where
    R: FeatureMarker + Repository,
{
    let repo = first;
    let other = second;
    let mut failures = Vec::new();

    let mut record = |name: &str, result: CaseResult| match result {
        Ok(()) => println!("ok   {name}"),
        Err(Failure(message)) => {
            println!("FAIL {name}");
            failures.push(format!("{name}: {message}"));
        }
    };

    record(
        "the manifest starts with stream-heads alone",
        clear_manifest(repo, other).await,
    );
    record(
        "a table that does not declare stream-heads refuses every write path until it is declared",
        unmigrated_store(repo, other).await,
    );
    record(
        "a declaration refuses commits and snapshots from both writers, then revalidates",
        declaration_bumps_and_refuses(repo, other).await,
    );
    record(
        "the durable historyDependent set refuses compaction across writers",
        history_dependent_refusal(repo, other).await,
    );
    record(
        "the retention set round-trips and entity heads skip the marker row",
        retention_and_heads(repo, other).await,
    );
    record(
        "the durable retention set refuses transaction fences",
        fence_retention_refusal(repo, other).await,
    );
    record(
        "one declaration transaction bumps the version once",
        one_bump_per_declaration(repo, other).await,
    );

    // Leave the manifest empty even when a case failed, so leftovers cannot brick the table's
    // writers after this run.
    if let Err(Failure(message)) = clear_manifest(repo, other).await {
        println!("FAIL cleanup");
        failures.push(format!("cleanup: {message}"));
    }

    if !failures.is_empty() {
        panic!(
            "{} feature-marker case(s) failed:\n\n{}",
            failures.len(),
            failures.join("\n\n")
        );
    }
}

/// Clears every declaration except `stream-heads`, so the run starts and ends with a manifest no
/// writer refuses. The feature is required of every writer (ADR 0007), and every table these
/// cases write to is head-aware.
async fn clear_manifest<R: FeatureMarker>(repo: &R, other: &R) -> CaseResult {
    for repository in [repo, other] {
        repository
            .update_manifest(|manifest| {
                manifest.features.clear();
                manifest.declare_stream_heads();
                manifest.history_dependent.clear();
                manifest.retention.clear();
            })
            .await?;
    }

    let manifest = repo.read_manifest().await?;
    ensure!(
        manifest.is_some_and(|manifest| {
            manifest.features_json() == r#"{"stream-heads":1}"#
                && manifest.history_dependent.is_empty()
                && manifest.retention.is_empty()
        }),
        "the cleared manifest declares stream-heads and nothing else"
    );

    Ok(())
}

async fn declaration_bumps_and_refuses<R>(repo: &R, other: &R) -> CaseResult
where
    R: FeatureMarker + Repository,
{
    let ns = namespace();
    let stream = Stream::new(&ns, "marker", "ledger");
    stream
        .commit(
            repo,
            stream.events(1..=4),
            Some(stream.new_snapshot(1, "{}")),
        )
        .await?;

    // The commit caches the version it validated; nothing declares a feature yet.
    stream.commit(repo, stream.events(5..=5), None).await?;

    other
        .update_manifest(|manifest| {
            manifest.features.insert("journal-compaction".to_owned(), 2);
        })
        .await?;
    let manifest = other
        .read_manifest()
        .await?
        .ok_or(Failure("no manifest".into()))?;
    ensure_eq!(
        manifest.features.get("journal-compaction"),
        Some(&2),
        "the declaration"
    );

    // Declaring never counts as validation, either on the declaring handle or one whose
    // cached validation predates the declaration. Maintenance snapshots obey the same refusal.
    let before_snapshot = repo.snapshot(&stream.id).await?;
    for writer in [other, repo] {
        let unsupported = |err: &Error| matches!(err, Error::UnsupportedFeature { feature, version: 2 } if feature == "journal-compaction");
        expect_err(
            stream.commit(writer, stream.events(6..=6), None).await,
            "committing to a table that declares an unsupported extension",
            unsupported,
        )?;
        expect_err(
            writer.put_snapshot(&stream.snapshot(2, 5, "{}")).await,
            "writing a snapshot to a table that declares an unsupported extension",
            unsupported,
        )?;
    }
    ensure_eq!(
        repo.events(&stream.id).await?,
        stream.events(1..=5),
        "the refused commits wrote nothing"
    );
    ensure_eq!(
        repo.snapshot(&stream.id).await?,
        before_snapshot,
        "the refused snapshots did not advance the floor"
    );

    // No historyDependent declaration is needed: an unsupported extension alone must fence
    // deletion. Exercise both the declaring handle and a previously validated writer.
    ensure!(
        manifest.history_dependent.is_empty(),
        "the unsupported-feature regression has no historyDependent set"
    );
    for compactor in [other, repo] {
        expect_err(
            compactor.compact_below(&stream.id, EventSequence(2)).await,
            "compacting a log declaring an unsupported extension without historyDependent",
            |err| matches!(err, Error::UnsupportedFeature { .. }),
        )?;
        ensure_eq!(
            repo.events(&stream.id).await?,
            stream.events(1..=5),
            "unsupported-feature compaction deleted nothing"
        );
        ensure_eq!(
            repo.snapshot(&stream.id).await?,
            before_snapshot,
            "unsupported-feature compaction preserved the snapshot"
        );
    }

    // A declaration touching only a durable set must not bless existing unsupported features.
    let retained = stream.entity_type.as_str().to_owned();
    repo.update_manifest(|manifest| {
        manifest.retention.insert(retained.clone());
    })
    .await?;
    expect_err(
        stream.commit(repo, stream.events(6..=6), None).await,
        "committing after a durable-set-only declaration on an unsupported log",
        |err| matches!(err, Error::UnsupportedFeature { .. }),
    )?;

    // Removing the feature revalidates the writer, which then commits again.
    other
        .update_manifest(|manifest| {
            manifest.features.remove("journal-compaction");
            manifest.retention.remove(&retained);
        })
        .await?;
    stream.commit(repo, stream.events(6..=6), None).await?;
    ensure_eq!(
        repo.events(&stream.id).await?,
        stream.events(1..=6),
        "the writer commits after the feature is removed"
    );

    Ok(())
}

async fn history_dependent_refusal<R>(repo: &R, other: &R) -> CaseResult
where
    R: FeatureMarker + Repository,
{
    let ns = namespace();
    let stream = Stream::new(&ns, "history", "history-ledger");
    stream
        .commit(
            repo,
            stream.events(1..=4),
            Some(stream.new_snapshot(1, "{}")),
        )
        .await?;

    // The set is durable, so a writer that never registered the projector refuses all the same.
    let entity_type = stream.entity_type.as_str().to_owned();
    other
        .update_manifest(|manifest| {
            manifest.history_dependent.insert(entity_type.clone());
        })
        .await?;
    expect_err(
        repo.compact_below(&stream.id, EventSequence(2)).await,
        "compacting a history-dependent entity type",
        |err| {
            matches!(
                err,
                Error::HistoryDependentProjection { entity_type, .. }
                    if *entity_type == stream.entity_type
            )
        },
    )?;
    ensure_eq!(
        repo.events(&stream.id).await?,
        stream.events(1..=4),
        "the refused compaction deleted nothing"
    );

    let entity_type = stream.entity_type.as_str().to_owned();
    other
        .update_manifest(|manifest| {
            manifest.history_dependent.remove(&entity_type);
        })
        .await?;
    ensure_eq!(
        repo.compact_below(&stream.id, EventSequence(2)).await?,
        2,
        "compaction proceeds once the type leaves the set"
    );

    Ok(())
}

async fn retention_and_heads<R>(repo: &R, other: &R) -> CaseResult
where
    R: FeatureMarker + Repository,
{
    let ns = namespace();
    let retained = format!("{ns}-retained");

    other
        .update_manifest(|manifest| {
            manifest.retention.insert(retained.clone());
        })
        .await?;
    let manifest = other
        .read_manifest()
        .await?
        .ok_or(Failure("no manifest".into()))?;
    ensure!(
        manifest.is_retained(&EntityType::new(&retained)),
        "the declared retention set"
    );

    other
        .update_manifest(|manifest| {
            manifest.retention.remove(&retained);
        })
        .await?;
    let manifest = other
        .read_manifest()
        .await?
        .ok_or(Failure("no manifest".into()))?;
    ensure!(
        !manifest.is_retained(&EntityType::new(&retained)),
        "the removed retention declaration"
    );

    // The marker row lives in the reserved SYSTEM# namespace, which entity heads skip.
    let heads = repo.entity_heads(None).await?;
    ensure!(
        !heads.contains_key(&EntityId::new(features::FEATURES_ID)),
        "entity heads skip the feature-marker row"
    );

    Ok(())
}

async fn one_bump_per_declaration<R>(repo: &R, other: &R) -> CaseResult
where
    R: FeatureMarker + Repository,
{
    let _ = repo;
    let before = other
        .read_manifest()
        .await?
        .map_or(0, |manifest| manifest.version);

    // Two features in one declaration still bump the version by exactly one, so every writer
    // cached at the old version revalidates once, not once per feature.
    let ns = namespace();
    other
        .update_manifest(|manifest| {
            manifest.features.insert(format!("{ns}-one"), 1);
            manifest.features.insert(format!("{ns}-two"), 1);
        })
        .await?;
    let manifest = other
        .read_manifest()
        .await?
        .ok_or(Failure("no manifest".into()))?;
    ensure_eq!(
        manifest.version,
        before + 1,
        "the version after a two-feature declaration"
    );

    Ok(())
}

/// A table whose manifest does not declare `stream-heads` is refused by every write path that
/// validates the marker, however populated it is (ADR 0007), and reads keep working. Declaring
/// the feature unconditionally (the migration's hook) lifts the refusal.
async fn unmigrated_store<R>(repo: &R, other: &R) -> CaseResult
where
    R: FeatureMarker + Repository,
{
    let ns = namespace();
    let stream = Stream::new(&ns, "unmigrated", "ledger");
    stream
        .commit(
            repo,
            stream.events(1..=3),
            Some(stream.new_snapshot(1, "{}")),
        )
        .await?;

    // A stream with no snapshot: its compaction is uncovered once the table is migrated, but
    // an unmigrated table refuses before the snapshot is read.
    let plain = Stream::new(&ns, "unmigrated-plain", "ledger");
    plain.commit(repo, plain.events(1..=2), None).await?;

    // As if the data had been written by a build without stream heads: the feature is not
    // declared. Both writers' cached validations predate the change.
    other
        .update_manifest(|manifest| {
            manifest.features.remove(features::STREAM_HEADS_FEATURE);
        })
        .await?;

    let not_migrated = |err: &Error| matches!(err, Error::StreamHeadsNotMigrated);
    for (name, writer) in [("the declaring writer", other), ("the cached writer", repo)] {
        expect_err(
            stream.commit(writer, stream.events(4..=4), None).await,
            &format!("a commit by {name}"),
            not_migrated,
        )?;
        expect_err(
            writer.put_snapshot(&stream.snapshot(2, 3, "{}")).await,
            &format!("a snapshot write by {name}"),
            not_migrated,
        )?;
        expect_err(
            writer.compact_below(&stream.id, EventSequence(3)).await,
            &format!("compaction by {name}"),
            not_migrated,
        )?;

        expect_err(
            writer.compact_below(&plain.id, EventSequence(1)).await,
            &format!("compaction of an uncovered stream by {name}"),
            not_migrated,
        )?;

        // Outcome-only and fence-only commits validate the marker too.
        expect_err(
            writer
                .commit(
                    Commit::new(stream.id.clone(), Vec::new())
                        .with_outcome(Outcome::accepted(command_id(), "digest")),
                )
                .await,
            &format!("an outcome-only commit by {name}"),
            not_migrated,
        )?;
        let mut rejected = TransactionCommit::<R::Operation>::new(Vec::new())
            .with_outcome(Outcome::accepted(command_id(), "digest"));
        rejected.fences = vec![ReadFence::new(
            stream.id.clone(),
            stream.entity_type.clone(),
            EventSequence(3),
            FenceProvenance::Engine,
        )];
        expect_err(
            writer.commit_transaction(rejected).await,
            &format!("an outcome-only transaction by {name}"),
            not_migrated,
        )?;
    }

    // Reads need nothing, and the refused writes changed nothing.
    ensure_eq!(
        writer_events(repo, &stream).await?,
        stream.events(1..=3),
        "the refused writes left the events alone"
    );
    ensure_eq!(
        repo.snapshot(&stream.id).await?,
        Some(stream.snapshot(1, 3, "{}")),
        "the refused writes left the snapshot alone"
    );
    let heads = repo.entity_heads(Some(&stream.entity_type)).await?;
    ensure_eq!(
        heads.get(&stream.id),
        Some(&EventSequence(3)),
        "entity heads are readable before the migration"
    );

    // The unconditional declaration is the hook a migration performs after it has backfilled and
    // verified the heads.
    other.declare_stream_heads().await?;
    ensure!(
        other
            .read_manifest()
            .await?
            .is_some_and(|manifest| manifest.declares_stream_heads()),
        "the declaration"
    );
    stream.commit(repo, stream.events(4..=4), None).await?;
    stream.commit(other, stream.events(5..=5), None).await?;
    ensure_eq!(
        writer_events(repo, &stream).await?,
        stream.events(1..=5),
        "both writers commit once the feature is declared"
    );
    expect_err(
        repo.compact_below(&plain.id, EventSequence(1)).await,
        "compaction of an uncovered stream once the feature is declared",
        |err| matches!(err, Error::CompactionUncovered { .. }),
    )?;

    Ok(())
}

async fn writer_events<R: Repository>(
    repo: &R,
    stream: &Stream,
) -> Result<Vec<SerializedEvent>, Failure> {
    Ok(repo.events(&stream.id).await?)
}

/// Runs against two handles over an **empty** store that has never been written to: no events, no
/// snapshots, no marker row. The first write declares `stream-heads` itself (ADR 0007), because
/// the store holds nothing a writer without heads could have written; later writers, including a
/// second handle, find it declared.
///
/// The store must be fresh: a durable backend needs its own empty table or schema for this case.
pub async fn fresh_store_cases<R>(fresh: &R, other: &R)
where
    R: FeatureMarker + Repository,
{
    let result: CaseResult = async {
        ensure!(
            fresh.read_manifest().await?.is_none(),
            "the store starts with no marker row"
        );

        // An outcome-only commit writes no stream, and an empty store may still declare.
        fresh
            .commit(
                Commit::new("fresh-outcome-only", Vec::new())
                    .with_outcome(Outcome::accepted(command_id(), "digest")),
            )
            .await?;
        let manifest = fresh
            .read_manifest()
            .await?
            .ok_or(Failure("the first write declared nothing".into()))?;
        ensure_eq!(
            manifest.version,
            1,
            "the declaration creates the row at version 1"
        );
        ensure_eq!(
            manifest.features_json(),
            r#"{"stream-heads":1}"#.to_owned(),
            "the declared features"
        );

        let ns = namespace();
        let stream = Stream::new(&ns, "fresh", "ledger");
        stream.commit(fresh, stream.events(1..=2), None).await?;
        stream.commit(other, stream.events(3..=3), None).await?;
        ensure_eq!(
            fresh
                .read_manifest()
                .await?
                .map(|manifest| manifest.version),
            Some(1),
            "a second writer finds the feature declared and declares nothing"
        );
        ensure_eq!(
            fresh.events(&stream.id).await?,
            stream.events(1..=3),
            "both writers wrote"
        );

        Ok(())
    }
    .await;

    if let Err(Failure(message)) = result {
        panic!("fresh store case failed: {message}");
    }
    println!("ok   a fresh store declares stream-heads on the first write");
}

/// Runs against two handles over an **empty** store that has never been written to (no events,
/// no snapshots, no marker row): explicit store initialization ([`FeatureMarker::initialize`],
/// issue #188).
///
/// With `refuses_uninitialized` (the handles are writers that do not initialize a store
/// themselves, the DynamoDB default), every write path first refuses the uninitialized store with
/// [`Error::StoreNotInitialized`] and writes nothing. Then, on every backend: two concurrent
/// initializations let exactly one handle write, a second initialization by either handle declares
/// nothing, both handles write, and initializing a store that holds data but does not declare
/// the feature is refused with [`Error::StreamHeadsNotMigrated`] and declares nothing, as the
/// first writer's verification always did. `fixtures`, when it has raw storage access, adds a
/// stream with no head item to that store.
///
/// The store must be fresh: a durable backend needs its own empty table or schema for this case.
pub async fn initialization_cases<R, F>(
    fresh: &R,
    other: &R,
    fixtures: &F,
    refuses_uninitialized: bool,
) where
    R: FeatureMarker + Repository,
    F: HeadFixtures<R>,
{
    let result: CaseResult = async {
        ensure!(
            fresh.read_manifest().await?.is_none(),
            "the store starts with no marker row"
        );

        let ns = namespace();
        let stream = Stream::new(&ns, "initialized", "ledger");
        if refuses_uninitialized {
            uninitialized_store_refuses_writes(fresh, &stream).await?;
        }

        // Two deployment jobs initializing the same empty store at once: however their scans and
        // reads interleave, exactly one of them writes, the manifest version bumps once, and the
        // loser reports that it declared nothing. A second write by the loser would bump the
        // version again, and every writer that cached the winner's would then fail its next
        // commit's manifest check.
        let declared = join_all(vec![
            Box::pin(fresh.initialize()),
            Box::pin(other.initialize()),
        ])
        .await;
        ensure!(
            declared.iter().all(Result::is_ok),
            "both concurrent initializations succeed: {declared:?}"
        );
        ensure_eq!(
            declared
                .iter()
                .filter(|declared| matches!(declared, Ok(true)))
                .count(),
            1,
            "exactly one of two concurrent initializations declares"
        );

        let manifest = fresh
            .read_manifest()
            .await?
            .ok_or(Failure("the initialization declared nothing".into()))?;
        ensure_eq!(
            (manifest.version, manifest.features_json()),
            (1, r#"{"stream-heads":1}"#.to_owned()),
            "the concurrent initializations create the row at version 1 declaring stream-heads"
        );

        ensure!(
            !fresh.initialize().await? && !other.initialize().await?,
            "a second initialization, by either handle, declares nothing"
        );

        stream.commit(fresh, stream.events(1..=2), None).await?;
        stream.commit(other, stream.events(3..=3), None).await?;
        ensure_eq!(
            fresh.events(&stream.id).await?,
            stream.events(1..=3),
            "both writers write to the initialized store"
        );
        ensure!(
            !fresh.initialize().await?,
            "initializing a store that holds data and declares the feature declares nothing"
        );
        ensure_eq!(
            fresh
                .read_manifest()
                .await?
                .map(|manifest| manifest.version),
            Some(1),
            "idempotent initialization leaves the manifest version alone"
        );

        initializing_a_populated_store_is_refused(fresh, other, fixtures, &ns).await?;

        // Declared again (the migration's hook), the store takes writes.
        other.declare_stream_heads().await?;
        stream.commit(fresh, stream.events(4..=4), None).await?;
        ensure_eq!(
            fresh.events(&stream.id).await?,
            stream.events(1..=4),
            "the writer writes once the feature is declared again"
        );

        Ok(())
    }
    .await;

    if let Err(Failure(message)) = result {
        panic!("store initialization case failed: {message}");
    }
    println!("ok   explicit store initialization");
}

/// Every write path refuses an uninitialized store with [`Error::StoreNotInitialized`], and none
/// of them writes anything: no event, snapshot, outcome, or marker row.
async fn uninitialized_store_refuses_writes<R>(writer: &R, stream: &Stream) -> CaseResult
where
    R: FeatureMarker + Repository,
{
    let not_initialized = |err: &Error| matches!(err, Error::StoreNotInitialized);
    let command = command_id();

    expect_err(
        stream
            .commit(
                writer,
                stream.events(1..=1),
                Some(stream.new_snapshot(1, "{}")),
            )
            .await,
        "a commit to an uninitialized store",
        not_initialized,
    )?;
    expect_err(
        writer
            .commit(
                Commit::new(stream.id.clone(), Vec::new())
                    .with_outcome(Outcome::accepted(command.clone(), "digest")),
            )
            .await,
        "an outcome-only commit to an uninitialized store",
        not_initialized,
    )?;
    let mut fenced = TransactionCommit::<R::Operation>::new(Vec::new())
        .with_outcome(Outcome::accepted(command_id(), "digest"));
    fenced.fences = vec![ReadFence::new(
        stream.id.clone(),
        stream.entity_type.clone(),
        EventSequence::ZERO,
        FenceProvenance::Engine,
    )];
    expect_err(
        writer.commit_transaction(fenced).await,
        "a fenced transaction on an uninitialized store",
        not_initialized,
    )?;
    expect_err(
        writer.put_snapshot(&stream.snapshot(1, 1, "{}")).await,
        "a snapshot write to an uninitialized store",
        not_initialized,
    )?;
    expect_err(
        writer.compact_below(&stream.id, EventSequence(1)).await,
        "compaction on an uninitialized store",
        not_initialized,
    )?;

    ensure!(
        writer.read_manifest().await?.is_none(),
        "a refused write declares nothing"
    );
    ensure!(
        writer.events(&stream.id).await?.is_empty() && writer.snapshot(&stream.id).await?.is_none(),
        "a refused write stores no event or snapshot"
    );
    ensure!(
        writer.outcome(&command).await?.is_none(),
        "a refused write records no outcome"
    );

    Ok(())
}

/// A store that holds data without declaring `stream-heads` (here: the declaration taken back, and,
/// with raw storage access, a stream with no head item) is refused, and nothing is declared.
async fn initializing_a_populated_store_is_refused<R, F>(
    fresh: &R,
    other: &R,
    fixtures: &F,
    ns: &str,
) -> CaseResult
where
    R: FeatureMarker + Repository,
    F: HeadFixtures<R>,
{
    other
        .update_manifest(|manifest| {
            manifest.features.remove(features::STREAM_HEADS_FEATURE);
        })
        .await?;

    let headless = Stream::new(ns, "headless", "ledger");
    match fixtures.append_bypassing_head(&headless.event(1, 1)).await {
        Ok(()) | Err(Error::InvalidConfig(_)) => {}
        Err(err) => return Err(err.into()),
    }

    let before = fresh.read_manifest().await?;
    expect_err(
        fresh.initialize().await,
        "initializing a populated store that does not declare stream-heads",
        |err| matches!(err, Error::StreamHeadsNotMigrated),
    )?;
    ensure_eq!(
        fresh.read_manifest().await?,
        before,
        "a refused initialization declares nothing"
    );

    Ok(())
}

/// The refusal must consult the manifest, not the calling instance's configuration: retention is
/// declared on one writer and the fenced transaction is committed by the other. This case runs
/// before the unsupported-feature declaration case, whose leftover manifest would refuse every
/// later commit, and the suite's final cleanup clears the retention entry.
async fn fence_retention_refusal<R>(repo: &R, other: &R) -> CaseResult
where
    R: FeatureMarker + Repository,
{
    let ns = namespace();
    let retained = format!("{ns}-retained");

    other
        .update_manifest(|manifest| {
            manifest.retention.insert(retained.clone());
        })
        .await?;

    let mut commit = TransactionCommit::<R::Operation>::new(Vec::new());
    commit.fences = vec![ReadFence::new(
        EntityId::new(format!("{ns}-a")),
        EntityType::new(&retained),
        EventSequence::ZERO,
        FenceProvenance::Engine,
    )];

    ensure!(
        matches!(
            repo.commit_transaction(commit).await,
            Err(Error::RetentionConflict { .. })
        ),
        "a fence for a retention-enabled type is refused with RetentionConflict"
    );

    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Command outcomes and rejections (C3, C4)
// ---------------------------------------------------------------------------------------------

/// Wraps a repository to simulate the failures that make a commit's result unknown, for the
/// command-outcome cases. It forwards everything, and can be armed to:
///
/// - [apply the next outcome-carrying commit and then fail it](Self::arm_after_apply) with
///   [`Error::Indeterminate`], the way a timeout or a lost response looks to the caller while the
///   transaction really committed;
/// - [fail the next outcome-carrying commit without applying it](Self::arm_before_apply);
/// - [commit a rival commit first](Self::race_with), as another writer winning a race between a
///   command's outcome pre-check and its commit;
/// - [fail an outcome read](Self::fail_outcome_read_after), [return a malformed row for
///   one](Self::corrupt_outcome_read_after), or [stall one forever](Self::stall_outcome_read_after)
///   (its read deadline then fires at once, standing in for the passage of time).
///
/// Each arming fires once, on the next commit that carries an outcome (or, for the read, on the
/// chosen outcome read).
pub struct FaultInjectingRepository<R: Repository> {
    inner: R,
    state: Mutex<FaultState<R::Operation>>,
}

struct FaultState<Op> {
    fault: Option<Fault>,
    /// A held commit that lands after a number of outcome reads (see `arm_land_after_reads`).
    delayed: Option<(Commit<Op>, usize)>,
    rival: Option<Commit<Op>>,
    reads_before_failure: Option<(usize, ReadFault)>,
    /// A stalled read is armed or running: the read deadline fires at once.
    stalling: bool,
}

#[derive(Clone, Copy)]
enum ReadFault {
    Backend,
    Corrupt,
    Stall,
}

#[derive(Clone, Copy)]
enum Fault {
    AfterApply,
    BeforeApply,
    Definitive,
    LandAfterReads(usize),
}

impl<R: Repository> FaultInjectingRepository<R> {
    /// Wraps a repository with nothing armed.
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            state: Mutex::new(FaultState {
                fault: None,
                delayed: None,
                rival: None,
                reads_before_failure: None,
                stalling: false,
            }),
        }
    }

    /// The wrapped repository.
    pub fn inner(&self) -> &R {
        &self.inner
    }

    fn state(&self) -> std::sync::MutexGuard<'_, FaultState<R::Operation>> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The next outcome-carrying commit really applies, then fails with [`Error::Indeterminate`].
    pub fn arm_after_apply(&self) {
        self.state().fault = Some(Fault::AfterApply);
    }

    /// The next outcome-carrying commit fails with [`Error::Indeterminate`] without applying.
    pub fn arm_before_apply(&self) {
        self.state().fault = Some(Fault::BeforeApply);
    }

    /// The next outcome-carrying commit (through [`Repository::commit`]) is held and fails with
    /// [`Error::Indeterminate`]; it really applies just before the `reads`-th outcome read after
    /// that, the way a transaction that was still executing when the caller gave up lands a moment
    /// later. Recovery must poll for it, not call the first miss "not applied".
    pub fn arm_land_after_reads(&self, reads: usize) {
        self.state().fault = Some(Fault::LandAfterReads(reads));
    }

    /// The next outcome-carrying commit fails with a plain [`Error::Backend`] without applying: a
    /// definitive refusal (validation, a missing table, authorization), not an unknown result.
    /// The store must return it as is, never recover it as an ambiguous write.
    pub fn arm_definitive_failure(&self) {
        self.state().fault = Some(Fault::Definitive);
    }

    /// Commits `rival` through the wrapped repository just before the next outcome-carrying
    /// commit, so that commit finds the rival's rows already written.
    pub fn race_with(&self, rival: Commit<R::Operation>) {
        self.state().rival = Some(rival);
    }

    /// Fails an outcome read with [`Error::Backend`]: the read after `reads` successful ones.
    pub fn fail_outcome_read_after(&self, reads: usize) {
        self.state().reads_before_failure = Some((reads, ReadFault::Backend));
    }

    /// Makes an outcome read fail with [`Error::InvalidRecord`], as a malformed stored row does:
    /// the read after `reads` successful ones.
    pub fn corrupt_outcome_read_after(&self, reads: usize) {
        self.state().reads_before_failure = Some((reads, ReadFault::Corrupt));
    }

    /// Makes an outcome read never resolve, the way a backend waiting forever for a connection
    /// does: the read after `reads` successful ones. The read deadline fires at once from now.
    pub fn stall_outcome_read_after(&self, reads: usize) {
        let mut state = self.state();

        state.reads_before_failure = Some((reads, ReadFault::Stall));
        state.stalling = true;
    }

    fn take_armed(&self) -> (Option<Commit<R::Operation>>, Option<Fault>) {
        let mut state = self.state();

        (state.rival.take(), state.fault.take())
    }

    fn injected() -> Error {
        Error::Indeterminate("injected fault: the commit's result is unknown".into())
    }
}

impl<R: Repository> Repository for FaultInjectingRepository<R> {
    type Operation = R::Operation;

    fn writes_views(&self) -> bool {
        self.inner.writes_views()
    }

    fn applies_expected_payload(&self) -> bool {
        self.inner.applies_expected_payload()
    }

    fn declare_history_dependent(
        &self,
        types: &[EntityType],
    ) -> impl Future<Output = Result<(), Error>> + Send {
        self.inner.declare_history_dependent(types)
    }

    async fn commit(&self, commit: Commit<R::Operation>) -> Result<(), Error> {
        if commit.outcome.is_some() {
            let (rival, fault) = self.take_armed();

            if let Some(rival) = rival {
                self.inner.commit(rival).await?;
            }

            match fault {
                Some(Fault::AfterApply) => {
                    self.inner.commit(commit).await?;

                    return Err(Self::injected());
                }
                Some(Fault::BeforeApply) => return Err(Self::injected()),
                Some(Fault::Definitive) => {
                    return Err(Error::backend("injected fault: the request was refused"));
                }
                Some(Fault::LandAfterReads(reads)) => {
                    self.state().delayed = Some((commit, reads));

                    return Err(Self::injected());
                }
                None => {}
            }
        }

        self.inner.commit(commit).await
    }

    async fn commit_transaction(
        &self,
        commit: TransactionCommit<R::Operation>,
    ) -> Result<(), Error> {
        if commit.outcome.is_some() {
            let (rival, fault) = self.take_armed();

            if let Some(rival) = rival {
                self.inner.commit(rival).await?;
            }

            match fault {
                Some(Fault::AfterApply) => {
                    self.inner.commit_transaction(commit).await?;

                    return Err(Self::injected());
                }
                Some(Fault::BeforeApply) => return Err(Self::injected()),
                Some(Fault::Definitive) => {
                    return Err(Error::backend("injected fault: the request was refused"));
                }
                Some(Fault::LandAfterReads(_)) => {
                    unreachable!("arm_land_after_reads holds single-stream commits only")
                }
                None => {}
            }
        }

        self.inner.commit_transaction(commit).await
    }

    fn backoff(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        self.inner.backoff(duration)
    }

    fn deadline(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        let stalling = self.state().stalling;
        let inner = self.inner.deadline(duration);

        async move {
            if !stalling {
                inner.await;
            }
        }
    }

    async fn outcome(&self, command_id: &CommandId) -> Result<Option<Outcome>, Error> {
        // A held commit lands just before the read it was waiting for.
        let landing = {
            let mut state = self.state();

            match state.delayed.take() {
                Some((held, 0 | 1)) => Some(held),
                Some((held, remaining)) => {
                    state.delayed = Some((held, remaining - 1));

                    None
                }
                None => None,
            }
        };
        if let Some(held) = landing {
            self.inner.commit(held).await?;
        }

        let fail = {
            let mut state = self.state();

            match state.reads_before_failure {
                Some((0, fault)) => {
                    state.reads_before_failure = None;
                    state.stalling = false;

                    Some(fault)
                }
                Some((remaining, fault)) => {
                    state.reads_before_failure = Some((remaining - 1, fault));

                    None
                }
                None => None,
            }
        };

        match fail {
            Some(ReadFault::Backend) => {
                return Err(Error::backend("injected fault: the outcome read failed"));
            }
            Some(ReadFault::Corrupt) => {
                return Err(Error::InvalidRecord(
                    "injected fault: the stored outcome row is malformed".into(),
                ));
            }
            Some(ReadFault::Stall) => std::future::pending().await,
            None => {}
        }

        self.inner.outcome(command_id).await
    }

    async fn events(&self, entity_id: &EntityId) -> Result<Vec<SerializedEvent>, Error> {
        self.inner.events(entity_id).await
    }

    async fn events_after(
        &self,
        entity_id: &EntityId,
        after: EventSequence,
    ) -> Result<Vec<SerializedEvent>, Error> {
        self.inner.events_after(entity_id, after).await
    }

    async fn latest(&self, entity_id: &EntityId, n: usize) -> Result<Vec<SerializedEvent>, Error> {
        self.inner.latest(entity_id, n).await
    }

    async fn snapshot(&self, entity_id: &EntityId) -> Result<Option<SerializedSnapshot>, Error> {
        self.inner.snapshot(entity_id).await
    }

    async fn put_snapshot(&self, snapshot: &SerializedSnapshot) -> Result<bool, Error> {
        self.inner.put_snapshot(snapshot).await
    }

    async fn compact_below(
        &self,
        entity_id: &EntityId,
        through: EventSequence,
    ) -> Result<usize, Error> {
        self.inner.compact_below(entity_id, through).await
    }

    async fn entity_heads(
        &self,
        entity_type: Option<&EntityType>,
    ) -> Result<BTreeMap<EntityId, EventSequence>, Error> {
        self.inner.entity_heads(entity_type).await
    }
}

/// Runs the command-outcome and rejection cases (C3, C4) against a repository that supports
/// outcomes. `other` must be a second handle over the same storage: the cases check that a second
/// writer sees the first one's outcomes. Like [`feature_marker_cases`], it panics after running
/// every case if any failed.
pub async fn outcome_cases<R: Repository>(repo: &R, other: &R) {
    let mut failures = Vec::new();

    let mut record = |name: &str, result: CaseResult| match result {
        Ok(()) => println!("ok   {name}"),
        Err(Failure(message)) => {
            println!("FAIL {name}");
            failures.push(format!("{name}: {message}"));
        }
    };

    record(
        "a repeated command returns its stored outcome, across writers and snapshots",
        Box::pin(outcome_replay(repo, other)).await,
    );
    record(
        "a reused command ID with another identity is IdempotencyKeyReuse",
        Box::pin(outcome_key_reuse(repo)).await,
    );
    record(
        "a domain rejection is persisted once and replayed verbatim",
        Box::pin(outcome_rejection(repo)).await,
    );
    record(
        "a pre-outcome command ID is a duplicate, never a recorded rejection",
        Box::pin(outcome_legacy_duplicate(repo)).await,
    );
    record(
        "an internal handler failure is an error and writes no outcome",
        Box::pin(outcome_internal_failure(repo)).await,
    );
    record(
        "a zero-event command is an accepted no-op that stores its response",
        Box::pin(outcome_noop(repo)).await,
    );
    record(
        "a transaction persists a rejection after checking its fences",
        Box::pin(outcome_transaction_reject(repo)).await,
    );
    record(
        "a rejection's numbers read back the same on every backend",
        Box::pin(outcome_rejection_numbers(repo)).await,
    );
    record(
        "command ID and outcome bounds are refused before any write",
        Box::pin(outcome_limits(repo)).await,
    );
    record(
        "ambiguous writes are settled by re-reading the outcome row",
        Box::pin(outcome_ambiguous(repo)).await,
    );
    record(
        "a lost outcome put resolves through the winner's row and writes nothing",
        Box::pin(outcome_race(repo)).await,
    );
    record(
        "an outcome read validates the command ID like a write",
        Box::pin(outcome_read_validation(repo)).await,
    );
    record(
        "U+0000 is refused in digests, command IDs, and rejections before any write",
        Box::pin(outcome_nul_refused(repo)).await,
    );
    record(
        "a command may not declare a reserved target stream",
        Box::pin(outcome_reserved_targets(repo)).await,
    );
    record(
        "a winner of the same command that moved a fenced stream resolves to its outcome",
        Box::pin(outcome_fence_race(repo)).await,
    );
    record(
        "a failed caller-stamped fence persists a stale-fence rejection",
        Box::pin(outcome_stale_fence(repo)).await,
    );
    record(
        "a transaction may write streams its identity does not name",
        Box::pin(outcome_write_set_is_not_identity(repo)).await,
    );
    record(
        "an engine-filled conflict re-prepares the command and only the committing attempt writes",
        Box::pin(retry_engine_filled(repo)).await,
    );
    record(
        "a caller-stamped fence conflict fails fast without preparing again",
        Box::pin(retry_caller_fenced(repo)).await,
    );
    record(
        "exhausted retries persist a stale-fence rejection that replays identically",
        Box::pin(retry_exhausted(repo)).await,
    );
    record(
        "a write set that changes between attempts is still one command identity",
        Box::pin(retry_write_set_changes(repo)).await,
    );
    record(
        "the single-aggregate shortcut re-prepares and records a stale-fence rejection",
        Box::pin(retry_single_aggregate(repo)).await,
    );
    record(
        "the identity digest is computed once, however many attempts a command takes",
        Box::pin(retry_digest_once(repo)).await,
    );
    record(
        "a deadline ends the retries and is treated as exhausted attempts",
        Box::pin(retry_deadline(repo)).await,
    );
    record(
        "a stream outside the identity is still refused when it is reserved",
        Box::pin(retry_reserved_stream(repo)).await,
    );
    record(
        "a failed commit leaves no outcome row",
        Box::pin(outcome_atomicity(repo)).await,
    );
    record(
        "the outcome row counts against the commit budget",
        Box::pin(outcome_budget(repo)).await,
    );
    record(
        "a rejection that carries events commits them atomically with its row and replays without them",
        Box::pin(rejection_events(repo)).await,
    );
    record(
        "a transaction rejects with its staged events, or discards them",
        Box::pin(rejection_events_transaction(repo)).await,
    );
    record(
        "a rejection with events obeys the commit rules of an accepted commit",
        Box::pin(rejection_events_rules(repo)).await,
    );
    record(
        "a conflict re-prepares a rejection with events, and exhaustion records none of its events",
        Box::pin(rejection_events_retry(repo)).await,
    );
    record(
        "a stale caller-stamped fence beside a rejection's events persists the stale-fence rejection alone",
        Box::pin(rejection_events_caller_fence(repo)).await,
    );

    if !failures.is_empty() {
        panic!(
            "{} command-outcome case(s) failed:\n\n{}",
            failures.len(),
            failures.join("\n\n")
        );
    }
}

/// The outcome configuration the cases use: a short recovery window, long enough to catch a commit
/// that lands a moment after the caller gave up, short enough that a row that never appears does
/// not stall the suite.
fn outcome_config() -> OutcomeConfig {
    OutcomeConfig::new().with_recovery_wait(Duration::from_millis(300))
}

fn outcome_store<R: Repository>(repo: R) -> Store<Ledger, R> {
    Store::new(repo, ())
        .with_snapshot_interval(0)
        .with_outcomes(outcome_config())
}

/// A store that runs each command once: the cases that pin what a single attempt returns (an
/// ambiguous write whose row never appears, say) must not have the engine run the command again on
/// its own. Fence-aware retry has its own cases.
fn single_attempt_store<R: Repository>(repo: R) -> Store<Ledger, R> {
    Store::new(repo, ())
        .with_snapshot_interval(0)
        .with_outcomes(outcome_config().with_retry(RetryPolicy::immediate(1)))
}

fn command_id() -> CommandId {
    CommandId::new(format!("{}-cmd", namespace()))
}

fn command_metadata(command_id: &CommandId) -> Metadata {
    metadata().with_command_id(command_id.clone())
}

/// The default digest of `Add(amount)` on a ledger, as the store computes it.
fn ledger_digest(entity_id: &EntityId, amount: i64) -> Result<String, Failure> {
    let command = Add(amount);
    let input = DigestInput {
        command_type: command.command_type(),
        targets: vec![entity_id.clone()],
        payload: command
            .canonical_payload()
            .map_err(|err| Failure(err.to_string()))?
            .unwrap_or_default(),
        principal: String::new(),
    };

    default_digest(&input).map_err(|err| Failure(err.to_string()))
}

/// A commit another writer would make for `Add(amount)` on `entity_id` under `command_id`, whose
/// outcome carries the digest of `Add(digest_amount)`.
fn rival_commit<Op>(
    entity_id: &EntityId,
    command_id: &CommandId,
    amount: i64,
    digest_amount: i64,
) -> Result<Commit<Op>, Failure> {
    let events = serialize_events::<Ledger>(
        entity_id,
        EventSequence::ZERO,
        &[LedgerEvent::from(Added { amount })],
        &Metadata::default().with_command_id(command_id.clone()),
    )?;

    Ok(
        Commit::new(entity_id.clone(), events).with_outcome(Outcome::accepted(
            command_id.clone(),
            ledger_digest(entity_id, digest_amount)?,
        )),
    )
}

fn expect_reuse(result: Result<impl fmt::Debug, Error>, what: &str) -> Result<Outcome, Failure> {
    match result {
        Err(Error::IdempotencyKeyReuse { prior_outcome, .. }) => Ok(*prior_outcome),
        other => Err(Failure(format!(
            "{what}: want IdempotencyKeyReuse, got {other:?}"
        ))),
    }
}

async fn outcome_replay<R: Repository>(repo: &R, other: &R) -> CaseResult {
    let store = outcome_store(repo);
    let second = outcome_store(other);
    let id = ledger_id();
    let command = command_id();

    let first = store
        .execute_with_outcome(&id, Add(5), command_metadata(&command))
        .await?;
    ensure!(
        first.is_accepted() && !first.is_noop() && !first.replayed,
        "the first execution is a fresh accepted outcome: {first:?}"
    );
    ensure_eq!(
        first.outcome.event_ids,
        vec![EventId::for_event(&id, EventSequence(1))],
        "the outcome names the event the commit wrote"
    );
    ensure_eq!(
        (first.events.len(), first.typed_events.len()),
        (1, 1),
        "the fresh result carries the committed events"
    );

    // The same command again, from the same writer and from a second one, replays the stored
    // outcome without running the handler or writing.
    for (what, store) in [("same writer", &store), ("second writer", &second)] {
        let again = store
            .execute_with_outcome(&id, Add(5), command_metadata(&command))
            .await?;
        ensure!(again.replayed, "{what}: the repeat is a replay");
        ensure!(again.events.is_empty(), "{what}: a replay commits nothing");
        ensure_eq!(
            again.outcome,
            first.outcome,
            format!("{what}: stored outcome")
        );
    }
    let loaded = store.load(&id).await?;
    ensure_eq!(
        loaded.aggregate(),
        &Ledger {
            total: 5,
            entries: 1
        },
        "the command applied once"
    );
    ensure_eq!(
        repo.outcome(&command).await?,
        Some(first.outcome.clone()),
        "the stored row"
    );

    // The plain entry points would skip the outcome row, so they refuse.
    let refused = store.execute(&id, Add(1), metadata()).await;
    ensure!(
        matches!(
            refused.as_ref().err().and_then(|err| err.as_store_error()),
            Some(Error::InvalidConfig(_))
        ),
        "execute on a store with outcomes is refused: {refused:?}"
    );

    // A command ID folded into a snapshot, whose events are compacted, is still deduplicated:
    // the outcome row is global, unlike the event window.
    let snapshots = Store::<Ledger, _>::new(repo, ())
        .with_snapshot_interval(1)
        .with_outcomes(outcome_config());
    let id = ledger_id();
    let commands: Vec<CommandId> = (0..3).map(|_| command_id()).collect();
    for (amount, command) in (1..=3).zip(&commands) {
        snapshots
            .execute_with_outcome(&id, Add(amount), command_metadata(command))
            .await?;
    }
    repo.compact_below(&id, EventSequence(3)).await?;

    let repeated = snapshots
        .execute_with_outcome(&id, Add(1), command_metadata(&commands[0]))
        .await?;
    ensure!(
        repeated.replayed,
        "a command folded into a snapshot is still deduplicated"
    );
    let total = snapshots.load(&id).await?.aggregate().total;
    ensure_eq!(total, 6, "the compacted command did not apply twice");

    Ok(())
}

async fn outcome_key_reuse<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);
    let id = ledger_id();
    let command = command_id();

    let first = store
        .execute_with_outcome(&id, Add(5), command_metadata(&command))
        .await?;

    // Another payload, another stream, and another principal are each another command.
    let prior = expect_reuse(
        store
            .execute_with_outcome(&id, Add(6), command_metadata(&command))
            .await,
        "another payload",
    )?;
    ensure_eq!(prior, first.outcome, "the error carries the stored outcome");

    let elsewhere = ledger_id();
    expect_reuse(
        store
            .execute_with_outcome(&elsewhere, Add(5), command_metadata(&command))
            .await,
        "another stream",
    )?;
    ensure!(
        repo.events(&elsewhere).await?.is_empty(),
        "the refused command wrote nothing"
    );

    expect_reuse(
        store
            .execute_with_outcome(
                &id,
                Add(5),
                command_metadata(&command).with_principal_id("user:2"),
            )
            .await,
        "another principal",
    )?;

    ensure_eq!(
        repo.outcome(&command).await?,
        Some(first.outcome),
        "the reuse is never stored: the first command's row stands"
    );
    let entries = store.load(&id).await?.aggregate().entries;
    ensure_eq!(entries, 1, "only the first command applied");

    Ok(())
}

async fn outcome_legacy_duplicate<R: Repository>(repo: &R) -> CaseResult {
    // An upgraded log: a command recorded before outcomes existed left its ID in an event and no
    // outcome row. A store without outcomes writes exactly that.
    let legacy: Store<Ledger, &R> = Store::new(repo, ()).with_snapshot_interval(0);
    let id = ledger_id();
    let command = command_id();
    legacy
        .execute(&id, Add(5), command_metadata(&command))
        .await
        .map_err(|err| Failure(format!("the legacy command: {err}")))?;
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "a legacy command has no outcome row"
    );

    let store = outcome_store(repo);

    // The single-aggregate path: the window check precedes the handler, so a command that would
    // be rejected now is a duplicate, and nothing is recorded.
    let direct = store
        .execute_with_outcome(&id, Add(0), command_metadata(&command))
        .await;
    ensure!(
        matches!(direct, Err(Error::DuplicateCommand(_))),
        "the window check precedes the rejection: {direct:?}"
    );
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "no terminal rejection was recorded"
    );

    // The transaction path: a rejection of a command whose ID the read stream already carries.
    let mut transaction = store.transaction();
    let identity = CommandIdentity::new("ledger:add");
    ensure!(
        matches!(
            transaction
                .begin_command(&identity, &command_metadata(&command))
                .await?,
            Begin::Proceed
        ),
        "no outcome row, so the command proceeds"
    );
    transaction.read::<Ledger>(&id).await?;
    let rejected = transaction
        .reject(Rejection::new("invalid-amount", "amount must be positive"))
        .await;
    ensure!(
        matches!(rejected, Err(Error::DuplicateCommand(_))),
        "reject applies the window check: {rejected:?}"
    );
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "no terminal rejection was recorded for a command that already ran"
    );

    Ok(())
}

async fn outcome_internal_failure<R: Repository>(repo: &R) -> CaseResult {
    let store: Store<Flaky, &R> = Store::new(repo, ())
        .with_snapshot_interval(0)
        .with_outcomes(outcome_config());
    let id = ledger_id();
    let command = command_id();

    // `ToRejection` returned `None`: the handler could not decide, which is not a domain refusal.
    let failed = store
        .execute_with_outcome(&id, Flake, command_metadata(&command))
        .await;
    ensure!(
        matches!(failed, Err(Error::Handler(_))),
        "an internal failure is returned as Error::Handler: {failed:?}"
    );
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "no outcome row is written for an internal failure"
    );
    ensure!(
        repo.events(&id).await?.is_empty(),
        "an internal failure writes no events"
    );

    // Nothing was recorded, so the retry decides again instead of replaying a durable failure.
    let again = store
        .execute_with_outcome(&id, Flake, command_metadata(&command))
        .await;
    ensure!(
        matches!(again, Err(Error::Handler(_))) && repo.outcome(&command).await?.is_none(),
        "the retry under the same ID starts clean: {again:?}"
    );

    Ok(())
}

async fn outcome_rejection<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);
    let id = ledger_id();
    let command = command_id();

    let rejected = store
        .execute_with_outcome(&id, Add(0), command_metadata(&command))
        .await?;
    ensure!(
        rejected.is_rejected() && !rejected.replayed && rejected.events.is_empty(),
        "a domain error is a fresh rejected outcome: {rejected:?}"
    );
    let rejection = rejected
        .rejection()
        .ok_or_else(|| Failure("a rejected outcome carries its rejection".into()))?
        .clone();
    ensure_eq!(
        rejection.code.as_str(),
        "invalid-amount",
        "the rejection code"
    );
    ensure_eq!(
        rejection.message.as_str(),
        "amount must be positive",
        "message"
    );
    ensure_eq!(rejection.details, None, "details");

    let stored = repo
        .outcome(&command)
        .await?
        .ok_or_else(|| Failure("the rejection was not persisted".into()))?;
    ensure!(
        stored.is_rejected() && stored.event_ids.is_empty(),
        "stored status"
    );
    ensure_eq!(
        stored.rejection,
        Some(rejection.clone()),
        "stored rejection"
    );
    ensure!(
        repo.events(&id).await?.is_empty(),
        "a rejection writes no events"
    );

    // Terminal for the digest: the retry replays the original rejection verbatim.
    let again = store
        .execute_with_outcome(&id, Add(0), command_metadata(&command))
        .await?;
    ensure!(
        again.replayed && again.is_rejected(),
        "the retry replays: {again:?}"
    );
    ensure_eq!(
        again.rejection(),
        Some(&rejection),
        "the replayed rejection"
    );

    // Another command ID with the same input is a fresh decision.
    let fresh = store
        .execute_with_outcome(&id, Add(0), command_metadata(&command_id()))
        .await?;
    ensure!(
        !fresh.replayed && fresh.is_rejected(),
        "a new ID decides again"
    );

    Ok(())
}

async fn outcome_noop<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);
    let id = ledger_id();
    let command = command_id();
    let identity = CommandIdentity::new("ledger:review").with_targets([id.clone()]);
    let metadata = command_metadata(&command);
    let response = br#"{"reviewed":true}"#.to_vec();

    // A transaction that appends nothing is an accepted no-op that still stores its response.
    let mut tx = store.transaction();
    ensure!(
        matches!(
            tx.begin_command(&identity, &metadata).await?,
            Begin::Proceed
        ),
        "an unseen command proceeds"
    );
    tx.read::<Ledger>(&id).await?;
    let noop = tx.commit_with_outcome(Some(response.clone())).await?;
    ensure!(
        noop.is_accepted() && noop.is_noop() && !noop.replayed && noop.events.is_empty(),
        "an accepted no-op: {noop:?}"
    );
    ensure_eq!(
        noop.response(),
        Some(response.as_slice()),
        "the stored response"
    );

    let stored = repo
        .outcome(&command)
        .await?
        .ok_or_else(|| Failure("the no-op was not persisted".into()))?;
    ensure_eq!(stored, noop.outcome.clone(), "the stored row");
    ensure!(stored.event_ids.is_empty(), "a no-op names no events");

    // A retry returns the original answer.
    let mut tx = store.transaction();
    match tx.begin_command(&identity, &metadata).await? {
        Begin::Replayed(replayed) => {
            ensure!(replayed.replayed && replayed.is_noop(), "the no-op replays");
            ensure_eq!(replayed.outcome, noop.outcome, "the replayed outcome");
        }
        Begin::Proceed => return Err(Failure("a recorded no-op must replay".into())),
    }

    // The no-op still checks the read set's fences: a stream that moved fails it, retryably, and
    // records nothing.
    let command = command_id();
    let identity = CommandIdentity::new("ledger:review").with_targets([id.clone()]);
    let mut tx = store.transaction();
    tx.begin_command(&identity, &command_metadata(&command))
        .await?;
    tx.read::<Ledger>(&id).await?;
    outcome_store(repo)
        .execute_with_outcome(&id, Add(1), command_metadata(&command_id()))
        .await?;
    let result = tx.commit_with_outcome(None).await;
    ensure!(
        matches!(
            &result,
            Err(err @ Error::FenceConflict { provenance: FenceProvenance::Engine, .. }) if err.is_retryable()
        ),
        "a no-op over a stale read fails its fence: {result:?}"
    );
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "the failed no-op recorded nothing"
    );

    // The repository-level outcome-only commit is the same row, written once.
    let command = command_id();
    let accepted = Outcome::accepted(command.clone(), "digest");
    repo.commit(Commit::<R::Operation>::new(id.clone(), Vec::new()).with_outcome(accepted.clone()))
        .await?;
    ensure_eq!(
        repo.outcome(&command).await?,
        Some(accepted.clone()),
        "outcome-only row"
    );
    let again = repo
        .commit(Commit::<R::Operation>::new(id.clone(), Vec::new()).with_outcome(accepted))
        .await;
    ensure!(
        matches!(&again, Err(Error::OutcomeConflict { command_id }) if *command_id == command),
        "a second outcome for the ID is OutcomeConflict, not a generic conflict: {again:?}"
    );

    // A rejected outcome may carry events: they commit with the row, and the row lists them.
    let stream = Stream::new(&namespace(), "rejected", "ledger");
    let command = command_id();
    let rejected = Outcome::rejected(command.clone(), "digest", Rejection::new("c", "m"));
    stream
        .commit_with_outcome(repo, stream.events(1..=1), rejected.clone())
        .await?;
    ensure_eq!(
        repo.events(&stream.id).await?.len(),
        1,
        "the rejection's event was written"
    );
    ensure_eq!(
        repo.outcome(&command)
            .await?
            .map(|stored| (stored.is_rejected(), stored.event_ids)),
        Some((true, vec![EventId::for_event(&stream.id, EventSequence(1))])),
        "the rejected row lists it"
    );

    Ok(())
}

impl Stream {
    async fn commit_with_outcome<R: Repository>(
        &self,
        repo: &R,
        events: Vec<SerializedEvent>,
        outcome: Outcome,
    ) -> Result<(), Error> {
        repo.commit(Commit::new(self.id.clone(), events).with_outcome(outcome))
            .await
    }
}

async fn outcome_transaction_reject<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);
    let ext = ledger_id();
    let plain = Store::<Ledger, _>::new(repo, ()).with_snapshot_interval(0);
    plain.execute(&ext, Add(1), metadata()).await?;
    let command = command_id();
    let identity = CommandIdentity::new("username:claim").with_targets([ext.clone()]);
    let rejection = Rejection::new("username-taken", "handle ada is taken")
        .with_details(serde_json::json!({ "handle": "ada" }));

    // A rejection decided from a read that is still current is persisted, with its details.
    let mut tx = store.transaction();
    tx.begin_command(&identity, &command_metadata(&command))
        .await?;
    tx.read::<Ledger>(&ext).await?;
    let rejected = tx.reject(rejection.clone()).await?;
    ensure!(
        rejected.is_rejected() && !rejected.replayed && rejected.events.is_empty(),
        "a fresh rejection: {rejected:?}"
    );
    ensure_eq!(
        rejected.rejection(),
        Some(&rejection),
        "the returned rejection"
    );
    ensure_eq!(
        repo.outcome(&command)
            .await?
            .and_then(|stored| stored.rejection),
        Some(rejection.clone()),
        "the stored rejection"
    );

    let mut tx = store.transaction();
    match tx
        .begin_command(&identity, &command_metadata(&command))
        .await?
    {
        Begin::Replayed(replayed) => {
            ensure_eq!(replayed.outcome, rejected.outcome, "replayed verbatim");
        }
        Begin::Proceed => return Err(Failure("a recorded rejection must replay".into())),
    }

    // A rejection decided from a stream that has since moved is not persisted: the engine-filled
    // fence fails the outcome-only transaction as a retryable conflict.
    let command = command_id();
    let mut tx = store.transaction();
    tx.begin_command(&identity, &command_metadata(&command))
        .await?;
    tx.read::<Ledger>(&ext).await?;
    plain.execute(&ext, Add(1), metadata()).await?;
    let result = tx.reject(rejection.clone()).await;
    ensure!(
        matches!(
            &result,
            Err(err @ Error::FenceConflict { provenance: FenceProvenance::Engine, .. }) if err.is_retryable()
        ),
        "a rejection over a stale read fails its fence: {result:?}"
    );
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "the stale rejection recorded nothing"
    );

    Ok(())
}

/// A rejection's numbers are stored in their canonical text, so a first execution equals a replay
/// even on a backend that rewrites the stored text (PostgreSQL's `jsonb` prints numbers without an
/// exponent and drops the sign of zero).
async fn outcome_rejection_numbers<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);
    let ext = ledger_id();
    let command = command_id();
    let identity = CommandIdentity::new("numbers:reject").with_targets([ext.clone()]);
    let rejection = Rejection::new("numbers", "numbers in every form").with_details(
        serde_json::json!({ "big": 1e21, "huge": 6.805598656397509e25, "tiny": 1.5e-7, "negzero": -0.0, "nested": [100.0, { "zero": -0.0 }] }),
    );

    let mut tx = store.transaction();
    tx.begin_command(&identity, &command_metadata(&command))
        .await?;
    let rejected = tx.reject(rejection).await?;

    let mut tx = store.transaction();
    match tx
        .begin_command(&identity, &command_metadata(&command))
        .await?
    {
        Begin::Replayed(replayed) => {
            ensure_eq!(
                replayed.outcome,
                rejected.outcome,
                "a first execution equals a replay"
            );
        }
        Begin::Proceed => return Err(Failure("a recorded rejection must replay".into())),
    }

    let stored = repo
        .outcome(&command)
        .await?
        .and_then(|stored| stored.rejection)
        .ok_or_else(|| Failure("the rejection is stored".into()))?;
    ensure_eq!(
        stored.encode()?,
        r#"{"code":"numbers","message":"numbers in every form","details":{"big":1000000000000000000000,"huge":68055986563975090000000000,"negzero":0.0,"nested":[100.0,{"zero":0.0}],"tiny":0.00000015}}"#,
        "every backend stores and returns the same numeric text"
    );

    Ok(())
}

async fn outcome_limits<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);
    let id = ledger_id();

    // A missing command ID is a programmer error before any read or write.
    let result = store.execute_with_outcome(&id, Add(1), metadata()).await;
    ensure!(
        matches!(&result, Err(Error::InvalidArgument(_))),
        "a missing command ID: {result:?}"
    );
    let result = store
        .execute_with_outcome(&id, Add(1), metadata().with_command_id(""))
        .await;
    ensure!(
        matches!(&result, Err(Error::InvalidArgument(_))),
        "an empty command ID is absent: {result:?}"
    );

    // 512 UTF-8 bytes is the limit, counted in bytes, not characters.
    let too_long = CommandId::new("c".repeat(513));
    let result = store
        .execute_with_outcome(&id, Add(1), command_metadata(&too_long))
        .await;
    ensure!(
        matches!(&result, Err(Error::CommandIdTooLong { length: 513 })),
        "a 513-byte command ID: {result:?}"
    );
    let multibyte = CommandId::new("é".repeat(257));
    let result = store
        .execute_with_outcome(&id, Add(1), command_metadata(&multibyte))
        .await;
    ensure!(
        matches!(&result, Err(Error::CommandIdTooLong { length: 514 })),
        "257 two-byte characters are 514 bytes: {result:?}"
    );
    ensure!(
        repo.events(&id).await?.is_empty(),
        "a refused command ID wrote nothing"
    );

    let exact = CommandId::new(format!("{:x<512}", namespace()));
    ensure_eq!(exact.as_str().len(), 512, "test setup");
    let accepted = store
        .execute_with_outcome(&id, Add(1), command_metadata(&exact))
        .await?;
    ensure!(accepted.is_accepted(), "a 512-byte command ID is accepted");
    ensure!(
        store
            .execute_with_outcome(&id, Add(1), command_metadata(&exact))
            .await?
            .replayed,
        "and replays"
    );

    // Oversized responses and rejections are refused before any write, events included.
    let stream = Stream::new(&namespace(), "limits", "ledger");
    let oversized = Outcome::accepted(command_id(), "digest").with_response(vec![7; 65537]);
    let result = stream
        .commit_with_outcome(repo, stream.events(1..=1), oversized)
        .await;
    ensure!(
        matches!(
            &result,
            Err(Error::OutcomeTooLarge {
                what: "response",
                size: 65537
            })
        ),
        "a 65537-byte response: {result:?}"
    );
    ensure!(
        repo.events(&stream.id).await?.is_empty(),
        "no event was written"
    );

    let at_limit = command_id();
    stream
        .commit_with_outcome(
            repo,
            stream.events(1..=1),
            Outcome::accepted(at_limit.clone(), "digest").with_response(vec![7; 65536]),
        )
        .await?;
    ensure_eq!(
        repo.outcome(&at_limit)
            .await?
            .and_then(|stored| stored.response)
            .map(|r| r.len()),
        Some(65536),
        "a 65536-byte response round-trips"
    );

    // The command ID check comes first.
    let both = Outcome::accepted(too_long.clone(), "digest").with_response(vec![7; 65537]);
    let result = Stream::new(&namespace(), "both", "ledger")
        .commit_with_outcome(repo, Vec::new(), both)
        .await;
    ensure!(
        matches!(&result, Err(Error::CommandIdTooLong { .. })),
        "the ID bound precedes the size bounds: {result:?}"
    );

    let command = command_id();
    let identity = CommandIdentity::new("ledger:review").with_targets([id.clone()]);
    let mut tx = store.transaction();
    tx.begin_command(&identity, &command_metadata(&command))
        .await?;
    let result = tx.reject(Rejection::new("c", "x".repeat(65536))).await;
    ensure!(
        matches!(
            &result,
            Err(Error::OutcomeTooLarge {
                what: "rejection",
                ..
            })
        ),
        "an oversized rejection: {result:?}"
    );
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "an oversized rejection writes no outcome"
    );

    Ok(())
}

async fn outcome_ambiguous<R: Repository>(repo: &R) -> CaseResult {
    let fault = FaultInjectingRepository::new(repo);
    let store = single_attempt_store(&fault);

    // The commit applied but the caller saw a failure: recovery returns the stored outcome and
    // the command does not run again.
    let id = ledger_id();
    let command = command_id();
    fault.arm_after_apply();
    let recovered = store
        .execute_with_outcome(&id, Add(5), command_metadata(&command))
        .await?;
    ensure!(
        recovered.replayed && recovered.is_accepted(),
        "an applied ambiguous write returns the stored outcome: {recovered:?}"
    );
    ensure_eq!(
        recovered.outcome.event_ids,
        vec![EventId::for_event(&id, EventSequence(1))],
        "the recovered outcome names the committed event"
    );
    ensure_eq!(repo.events(&id).await?.len(), 1, "the command applied once");

    // The commit did not apply: the row is absent, so retrying the same command ID is safe.
    let id = ledger_id();
    let command = command_id();
    fault.arm_before_apply();
    let error = store
        .execute_with_outcome(&id, Add(5), command_metadata(&command))
        .await
        .err();
    ensure!(
        matches!(&error, Some(err @ Error::AmbiguousWrite { .. }) if err.is_retryable()),
        "an absent row after the whole window is a retryable AmbiguousWrite, never \"not applied\": {error:?}"
    );
    ensure!(repo.events(&id).await?.is_empty(), "nothing was written");
    let retried = store
        .execute_with_outcome(&id, Add(5), command_metadata(&command))
        .await?;
    ensure!(
        retried.is_accepted() && !retried.replayed,
        "the retry commits: {retried:?}"
    );

    // Another command claimed the ID with a different identity before the ambiguous commit.
    let id = ledger_id();
    let command = command_id();
    fault.race_with(rival_commit(&id, &command, 6, 6)?);
    fault.arm_before_apply();
    let prior = expect_reuse(
        store
            .execute_with_outcome(&id, Add(5), command_metadata(&command))
            .await,
        "ambiguous write whose ID was claimed by another command",
    )?;
    ensure_eq!(
        prior.digest,
        ledger_digest(&id, 6)?,
        "the prior outcome's digest"
    );

    // The commit lands a moment after the caller gave up: recovery polls for the row and finds it
    // instead of calling the first miss "not applied".
    let id = ledger_id();
    let command = command_id();
    fault.arm_land_after_reads(3);
    let late = store
        .execute_with_outcome(&id, Add(5), command_metadata(&command))
        .await?;
    ensure!(
        late.replayed && late.is_accepted(),
        "a delayed commit is found by polling: {late:?}"
    );
    ensure_eq!(repo.events(&id).await?.len(), 1, "the command applied once");

    // A definitive refusal is not an unknown result: it is returned as is, with no recovery read
    // and nothing that would make the command run again.
    let id = ledger_id();
    let command = command_id();
    fault.arm_definitive_failure();
    let error = store
        .execute_with_outcome(&id, Add(5), command_metadata(&command))
        .await
        .err();
    ensure!(
        matches!(&error, Some(err @ Error::Backend(_)) if !err.is_retryable()),
        "a definitive backend error is returned as is, not recovered: {error:?}"
    );
    ensure!(repo.events(&id).await?.is_empty(), "nothing was written");
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "no outcome row exists for a refused commit"
    );

    // Recovery that cannot read the row cannot tell: the original failure, not known unapplied.
    let id = ledger_id();
    let command = command_id();
    fault.arm_after_apply();
    fault.fail_outcome_read_after(1);
    let error = store
        .execute_with_outcome(&id, Add(5), command_metadata(&command))
        .await
        .err();
    ensure!(
        matches!(&error, Some(err @ Error::AmbiguousWrite { .. }) if err.is_retryable()),
        "an unreadable recovery is a retryable AmbiguousWrite: {error:?}"
    );
    ensure_eq!(repo.events(&id).await?.len(), 1, "the commit did apply");
    let later = store
        .execute_with_outcome(&id, Add(5), command_metadata(&command))
        .await?;
    ensure!(later.replayed, "a later attempt finds the row");

    // A recovery read that never resolves ends at its deadline: still a retryable ambiguous write.
    let id = ledger_id();
    let command = command_id();
    fault.arm_after_apply();
    fault.stall_outcome_read_after(1);
    let error = store
        .execute_with_outcome(&id, Add(5), command_metadata(&command))
        .await
        .err();
    ensure!(
        matches!(&error, Some(err @ Error::AmbiguousWrite { .. }) if err.is_retryable()),
        "a stalled recovery read is a retryable AmbiguousWrite: {error:?}"
    );

    // A malformed row found during recovery is corruption, never masked as an ambiguous write.
    let id = ledger_id();
    let command = command_id();
    fault.arm_after_apply();
    fault.corrupt_outcome_read_after(1);
    let error = store
        .execute_with_outcome(&id, Add(5), command_metadata(&command))
        .await
        .err();
    ensure!(
        matches!(&error, Some(Error::InvalidRecord(_))),
        "a malformed row during recovery is corruption: {error:?}"
    );

    Ok(())
}

async fn outcome_race<R: Repository>(repo: &R) -> CaseResult {
    let fault = FaultInjectingRepository::new(repo);
    let store = outcome_store(&fault);

    // Another writer commits the same command, with the same identity, between this command's
    // pre-check and its commit. The loser returns the winner's outcome and applies nothing.
    let id = ledger_id();
    let command = command_id();
    fault.race_with(rival_commit(&id, &command, 5, 5)?);
    let result = store
        .execute_with_outcome(&id, Add(5), command_metadata(&command))
        .await?;
    ensure!(
        result.replayed && result.is_accepted(),
        "the race loser replays the winner: {result:?}"
    );
    ensure_eq!(repo.events(&id).await?.len(), 1, "the command applied once");

    // The same race with another identity is IdempotencyKeyReuse.
    let id = ledger_id();
    let command = command_id();
    fault.race_with(rival_commit(&id, &command, 6, 6)?);
    expect_reuse(
        store
            .execute_with_outcome(&id, Add(5), command_metadata(&command))
            .await,
        "a race lost to another command",
    )?;
    ensure_eq!(
        repo.events(&id).await?.len(),
        1,
        "only the winner's event is stored"
    );

    // The winner wrote only the outcome and another stream: the loser's events do not conflict,
    // so only its outcome put fails. That is OutcomeConflict, never a generic conflict, and it
    // rolls back the loser's events with it.
    let id = ledger_id();
    let winner_stream = ledger_id();
    let command = command_id();
    let mut rival = rival_commit::<R::Operation>(&winner_stream, &command, 5, 5)?;
    rival.outcome = Some(Outcome::accepted(command.clone(), ledger_digest(&id, 5)?));
    fault.race_with(rival);
    let result = store
        .execute_with_outcome(&id, Add(5), command_metadata(&command))
        .await?;
    ensure!(
        result.replayed,
        "an outcome-only race resolves through the row: {result:?}"
    );
    ensure!(
        repo.events(&id).await?.is_empty(),
        "the loser's events were rolled back with its outcome"
    );

    // The loser returns ONLY the winner's stored outcome. Its own decision here is a rejection
    // (`Add(0)` is refused), but the winner recorded an accepted outcome under the same digest: the
    // result is the winner's accepted outcome, with nothing of the loser's local decision.
    let id = ledger_id();
    let command = command_id();
    fault.race_with(rival_commit(&id, &command, 5, 0)?);
    let result = store
        .execute_with_outcome(&id, Add(0), command_metadata(&command))
        .await?;
    ensure!(
        result.replayed && result.is_accepted() && result.rejection().is_none(),
        "the loser returns the winner's accepted outcome, not its own rejection: {result:?}"
    );
    ensure_eq!(
        result.outcome.event_ids,
        vec![EventId::for_event(&id, EventSequence(1))],
        "the winner's events"
    );
    ensure_eq!(repo.events(&id).await?.len(), 1, "only the winner wrote");

    // The repository reports the lost put as the typed error.
    let command = command_id();
    let accepted = Outcome::accepted(command.clone(), "digest");
    let stream = Stream::new(&namespace(), "race", "ledger");
    stream
        .commit_with_outcome(repo, stream.events(1..=1), accepted.clone())
        .await?;
    let other = Stream::new(&namespace(), "race-other", "ledger");
    let result = other
        .commit_with_outcome(repo, other.events(1..=1), accepted)
        .await;
    ensure!(
        matches!(&result, Err(Error::OutcomeConflict { command_id }) if *command_id == command),
        "a lost outcome put is OutcomeConflict: {result:?}"
    );
    ensure!(
        repo.events(&other.id).await?.is_empty(),
        "the failed commit's events were not written"
    );

    Ok(())
}

/// PostgreSQL text and jsonb cannot hold U+0000, so every backend refuses it alike, before any
/// write: in a custom digest, a command ID, and a rejection's code, message, and details.
async fn outcome_nul_refused<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);
    let ext = ledger_id();

    let digested = Store::<Ledger, _>::new(repo, ())
        .with_snapshot_interval(0)
        .with_outcomes(outcome_config().with_digester(|_| Ok("digest\0nul".into())));
    let command = command_id();
    let result = digested
        .execute_with_outcome(&ext, Add(1), command_metadata(&command))
        .await;
    ensure!(
        matches!(result, Err(Error::InvalidArgument(_))),
        "a digest with U+0000 is refused: {result:?}"
    );
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "the refused digest recorded nothing"
    );

    let nul_id = CommandId::new(format!("{}\0nul", namespace()));
    let result = store
        .execute_with_outcome(&ext, Add(1), command_metadata(&nul_id))
        .await;
    ensure!(
        matches!(result, Err(Error::InvalidArgument(_))),
        "a command ID with U+0000 is refused: {result:?}"
    );

    // Every other persisted metadata string is held to the same rule, before the handler runs.
    type Tweak = fn(&mut Metadata);
    let tweaks: [(&str, Tweak); 6] = [
        ("principal", |m| m.principal_id = Some("who\0".into())),
        ("causation", |m| {
            m.causation_id = Some(CommandId::new("cause\0"))
        }),
        ("extension key", |m| {
            m.extensions.insert("k\0".into(), "v".into());
        }),
        ("extension value", |m| {
            m.extensions.insert("k".into(), "v\0".into());
        }),
        ("region", |m| m.region = "us\0".into()),
        ("origin", |m| {
            m.origin = Some(crate::Origin::new("s\0", "e"))
        }),
    ];
    for (name, tweak) in tweaks {
        let command = CommandId::new(format!("{}-{}", namespace(), name.replace(' ', "-")));
        let mut metadata = command_metadata(&command);
        tweak(&mut metadata);

        let result = store.execute_with_outcome(&ext, Add(1), metadata).await;
        ensure!(
            matches!(result, Err(Error::InvalidArgument(_))),
            "{name} with U+0000 is refused: {result:?}"
        );
        ensure!(
            repo.outcome(&command).await?.is_none(),
            "{name}: nothing was recorded"
        );
    }

    let rejections = [
        Rejection::new("c\0", "m"),
        Rejection::new("c", "m\0"),
        Rejection::new("c", "m").with_details(serde_json::json!({ "key": "va\0lue" })),
        Rejection::new("c", "m").with_details(serde_json::json!({ "ke\0y": "value" })),
        Rejection::new("c", "m").with_details(serde_json::json!({ "n": [{ "deep": "x\0" }] })),
    ];
    let identity = CommandIdentity::new("ledger:nul").with_targets([ext.clone()]);
    for rejection in rejections {
        let command = command_id();
        let mut tx = store.transaction();
        tx.begin_command(&identity, &command_metadata(&command))
            .await?;
        let result = tx.reject(rejection.clone()).await;
        ensure!(
            matches!(result, Err(Error::InvalidArgument(_))),
            "{rejection:?} is refused: {result:?}"
        );
        ensure!(
            repo.outcome(&command).await?.is_none(),
            "{rejection:?} recorded nothing"
        );
    }

    Ok(())
}

async fn outcome_read_validation<R: Repository>(repo: &R) -> CaseResult {
    let empty = repo.outcome(&CommandId::new("")).await;
    ensure!(
        matches!(empty, Err(Error::InvalidArgument(_))),
        "an empty ID is a programmer error, not an absent row: {empty:?}"
    );

    let long = repo
        .outcome(&CommandId::new(
            "a".repeat(crate::outcome::MAX_COMMAND_ID_BYTES + 1),
        ))
        .await;
    ensure!(
        matches!(long, Err(Error::CommandIdTooLong { .. })),
        "an oversized ID is refused: {long:?}"
    );

    Ok(())
}

async fn outcome_reserved_targets<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);

    for reserved in ["SYSTEM#target", "command-outcome#target"] {
        let command = command_id();
        let metadata = command_metadata(&command);

        // A no-op or a rejection aimed at a reserved stream would otherwise record an outcome.
        for amount in [0, 5] {
            let result = store
                .execute_with_outcome(reserved, Add(amount), metadata.clone())
                .await;
            ensure!(
                matches!(result, Err(Error::ReservedEntityId { .. })),
                "{reserved}: the single-aggregate path refuses it: {result:?}"
            );
        }

        let resolved = store.resolve_outcome(reserved, &Add(5), &metadata).await;
        ensure!(
            matches!(resolved, Err(Error::ReservedEntityId { .. })),
            "{reserved}: resolving refuses it: {resolved:?}"
        );

        let identity = CommandIdentity::new("ledger:any")
            .with_targets([ledger_id().to_string(), reserved.to_owned()]);
        let mut transaction = store.transaction();
        let begun = transaction.begin_command(&identity, &metadata).await;
        ensure!(
            matches!(begun, Err(Error::ReservedEntityId { .. })),
            "{reserved}: begin_command refuses it: {begun:?}"
        );

        ensure!(
            repo.outcome(&command).await?.is_none(),
            "{reserved}: no outcome is recorded"
        );
    }

    Ok(())
}

/// The winner of a race for the same command ID wrote a stream the loser fenced at the head it
/// read. A backend that asserts fences before inserting the outcome (PostgreSQL) reports the stale
/// fence, not the lost outcome put; the loser must still resolve to the winner's outcome (or key
/// reuse), and only fall back to a fence-aware retry when no winner's row exists.
async fn outcome_fence_race<R: Repository>(repo: &R) -> CaseResult {
    let fault = FaultInjectingRepository::new(repo);
    let store = outcome_store(&fault);
    let plain = Store::<Ledger, _>::new(repo, ()).with_snapshot_interval(0);

    for (name, same_identity) in [("same identity", true), ("another identity", false)] {
        let watched = ledger_id();
        let target = ledger_id();
        plain.execute(&watched, Add(1), metadata()).await?;

        let command = command_id();
        // Both writers name both streams: the winner writes the watched one.
        let identity =
            CommandIdentity::new("ledger:copy").with_targets([target.clone(), watched.clone()]);
        let metadata = command_metadata(&command);

        // The loser reads the watched stream (an engine fence at head 1) and writes the target.
        let mut tx = store.transaction();
        tx.begin_command(&identity, &metadata).await?;
        tx.read::<Ledger>(&watched).await?;
        let mut ledger = tx.read::<Ledger>(&target).await?;
        tx.append(&mut ledger, vec![Added { amount: 1 }.into()])?;

        // The winner, between the loser's load and commit, moves the watched stream and records
        // the same command ID.
        let winner_identity = if same_identity {
            identity.clone()
        } else {
            CommandIdentity::new("ledger:other").with_targets([target.clone(), watched.clone()])
        };
        let digest = default_digest(&winner_identity.digest_input(&metadata))
            .map_err(|err| Failure(err.to_string()))?;
        let events = serialize_events::<Ledger>(
            &watched,
            EventSequence(1),
            &[LedgerEvent::from(Added { amount: 1 })],
            &Metadata::default().with_command_id(command.clone()),
        )?;
        fault.race_with(
            Commit::new(watched.clone(), events)
                .with_outcome(Outcome::accepted(command.clone(), digest)),
        );

        let result = tx.commit_with_outcome(None).await;

        if same_identity {
            let resolved = result.map_err(|err| Failure(format!("{name}: {err}")))?;
            ensure!(
                resolved.replayed && resolved.is_accepted(),
                "{name}: the loser returns the winner's stored outcome: {resolved:?}"
            );
        } else {
            expect_reuse(result, name)?;
        }

        ensure!(
            repo.events(&target).await?.is_empty(),
            "{name}: the loser wrote nothing"
        );
        ensure_eq!(
            repo.events(&watched).await?.len(),
            2,
            "only the winner moved the watched stream"
        );
    }

    Ok(())
}

async fn outcome_stale_fence<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);
    let plain = Store::<Ledger, _>::new(repo, ()).with_snapshot_interval(0);
    let external = ledger_id();
    plain.execute(&external, Add(1), metadata()).await?;
    plain.execute(&external, Add(1), metadata()).await?;

    let target = ledger_id();
    let command = command_id();
    let identity = CommandIdentity::new("ledger:copy").with_targets([target.clone()]);
    let metadata = command_metadata(&command);

    // The caller stamped a fence at head 1; the stream is at 2.
    let mut tx = store.transaction();
    tx.begin_command(&identity, &metadata).await?;
    let mut ledger = tx.read::<Ledger>(&target).await?;
    tx.append(&mut ledger, vec![Added { amount: 1 }.into()])?;
    tx.fence(ReadFence::new(
        external.clone(),
        Ledger::TYPE,
        EventSequence(1),
        FenceProvenance::Engine,
    ))?;
    let stale = tx.commit_with_outcome(None).await?;

    ensure!(
        stale.is_rejected() && !stale.replayed && stale.events.is_empty(),
        "a stale caller-stamped fence is a persisted rejection: {stale:?}"
    );
    let rejection = stale
        .rejection()
        .cloned()
        .unwrap_or_else(|| Rejection::new("", ""));
    ensure_eq!(
        rejection,
        Rejection::stale_fence(),
        "the stale-fence rejection"
    );
    ensure_eq!(rejection.code.as_str(), "stale-fence", "code");
    ensure_eq!(
        rejection.message.as_str(),
        "a caller-stamped fence went stale",
        "message"
    );
    ensure!(
        repo.events(&target).await?.is_empty(),
        "the command wrote nothing"
    );
    ensure_eq!(
        repo.outcome(&command).await?,
        Some(stale.outcome.clone()),
        "the rejection is the command's stored outcome"
    );

    // The old command ID is terminal: it replays the rejection verbatim. The remedy is a new ID.
    let mut tx = store.transaction();
    match tx.begin_command(&identity, &metadata).await? {
        Begin::Replayed(replayed) => {
            ensure_eq!(replayed.outcome, stale.outcome, "the replayed stale-fence");
        }
        Begin::Proceed => return Err(Failure("the old ID must replay its rejection".into())),
    }

    // An engine-filled fence conflict is an ordinary retryable conflict: no rejection.
    let command = command_id();
    let mut tx = store.transaction();
    tx.begin_command(&identity, &command_metadata(&command))
        .await?;
    tx.read::<Ledger>(&external).await?;
    let mut ledger = tx.read::<Ledger>(&target).await?;
    tx.append(&mut ledger, vec![Added { amount: 1 }.into()])?;
    plain
        .execute(&external, Add(1), metadata_for_plain())
        .await?;
    let result = tx.commit_with_outcome(None).await;
    ensure!(
        matches!(
            &result,
            Err(err @ Error::FenceConflict { provenance: FenceProvenance::Engine, .. }) if err.is_retryable()
        ),
        "an engine-filled fence conflict stays a retryable conflict: {result:?}"
    );
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "no rejection was persisted for it"
    );

    // A passing caller-stamped fence says nothing about an event conflict: it re-prepares.
    let command = command_id();
    let head = plain.load(&external).await?.sequence();
    let mut tx = store.transaction();
    tx.begin_command(&identity, &command_metadata(&command))
        .await?;
    let mut ledger = tx.read::<Ledger>(&target).await?;
    tx.append(&mut ledger, vec![Added { amount: 1 }.into()])?;
    tx.fence(ReadFence::new(
        external.clone(),
        Ledger::TYPE,
        head,
        FenceProvenance::Engine,
    ))?;
    plain.execute(&target, Add(1), metadata_for_plain()).await?;
    let result = tx.commit_with_outcome(None).await;
    ensure!(
        matches!(&result, Err(err) if err.is_conflict() && !matches!(err, Error::FenceConflict { .. })),
        "an event conflict beside a passing caller fence is an ordinary conflict: {result:?}"
    );
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "no rejection was persisted for an event conflict"
    );

    // The stale caller-stamped fence also wins when the transaction rejects.
    let command = command_id();
    let mut tx = store.transaction();
    tx.begin_command(&identity, &command_metadata(&command))
        .await?;
    tx.fence(ReadFence::new(
        external.clone(),
        Ledger::TYPE,
        EventSequence(1),
        FenceProvenance::Engine,
    ))?;
    let outcome = tx.reject(Rejection::new("denied", "not allowed")).await?;
    ensure_eq!(
        outcome
            .rejection()
            .map(|rejection| rejection.code.as_str().to_owned()),
        Some("stale-fence".to_owned()),
        "a stale fence replaces the rejection it would have recorded"
    );

    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Fence-aware retry (C5)
// ---------------------------------------------------------------------------------------------

/// Which commits a [`Meddling`] repository interferes with.
#[derive(Clone, Copy)]
enum Trigger {
    /// Commits that carry read fences.
    Fences,
    /// Commits that write events.
    Events,
}

/// Wraps a repository so another writer moves a stream between a command's reads and its commit:
/// just before each commit its [`Trigger`] selects, up to a number of times, it appends one event
/// to the stream. It also records the backoffs the engine asks for, returning at once.
struct Meddling<'a, R> {
    inner: &'a R,
    stream: EntityId,
    trigger: Trigger,
    moves: Mutex<u32>,
    delays: Mutex<Vec<Duration>>,
    /// Moves the stream by funding a [`Wallet`] with this amount instead of adding to a ledger.
    wallet: Option<i64>,
}

impl<'a, R: Repository> Meddling<'a, R> {
    /// A repository that moves `stream` before the next `moves` matching commits (`u32::MAX` for
    /// every one).
    fn new(inner: &'a R, stream: &EntityId, trigger: Trigger, moves: u32) -> Self {
        Self {
            inner,
            stream: stream.clone(),
            trigger,
            moves: Mutex::new(moves),
            delays: Mutex::new(Vec::new()),
            wallet: None,
        }
    }

    /// Moves a [`Wallet`] stream, by funding it with `amount`, instead of a ledger.
    fn funding(mut self, amount: i64) -> Self {
        self.wallet = Some(amount);
        self
    }

    fn moves(&self) -> std::sync::MutexGuard<'_, u32> {
        self.moves
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn disarm(&self) {
        *self.moves() = 0;
    }

    fn delays(&self) -> Vec<Duration> {
        self.delays
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    async fn meddle(&self, fenced: bool, writes_events: bool) -> Result<(), Error> {
        let matches = match self.trigger {
            Trigger::Fences => fenced,
            Trigger::Events => writes_events,
        };
        if !matches {
            return Ok(());
        }

        {
            let mut moves = self.moves();

            match *moves {
                0 => return Ok(()),
                u32::MAX => {}
                remaining => *moves = remaining - 1,
            }
        }

        if let Some(amount) = self.wallet {
            Store::<Wallet, _>::new(self.inner, ())
                .with_snapshot_interval(0)
                .execute(
                    &self.stream,
                    WalletCommand::Fund(amount),
                    Metadata::default(),
                )
                .await
                .map_err(|err| Error::backend(err.to_string()))?;

            return Ok(());
        }

        Store::<Ledger, _>::new(self.inner, ())
            .with_snapshot_interval(0)
            .execute(&self.stream, Add(1), Metadata::default())
            .await
            .map_err(|err| Error::backend(err.to_string()))?;

        Ok(())
    }
}

impl<R: Repository> Repository for Meddling<'_, R> {
    type Operation = R::Operation;

    fn writes_views(&self) -> bool {
        self.inner.writes_views()
    }

    fn applies_expected_payload(&self) -> bool {
        self.inner.applies_expected_payload()
    }

    fn declare_history_dependent(
        &self,
        types: &[EntityType],
    ) -> impl Future<Output = Result<(), Error>> + Send {
        self.inner.declare_history_dependent(types)
    }

    async fn commit(&self, commit: Commit<R::Operation>) -> Result<(), Error> {
        self.meddle(false, !commit.events.is_empty()).await?;

        self.inner.commit(commit).await
    }

    async fn commit_transaction(
        &self,
        commit: TransactionCommit<R::Operation>,
    ) -> Result<(), Error> {
        let writes_events = commit
            .streams
            .iter()
            .any(|stream| !stream.events.is_empty());
        self.meddle(!commit.fences.is_empty(), writes_events)
            .await?;

        self.inner.commit_transaction(commit).await
    }

    fn backoff(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        self.delays
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(duration);

        async {}
    }

    async fn outcome(&self, command_id: &CommandId) -> Result<Option<Outcome>, Error> {
        self.inner.outcome(command_id).await
    }

    async fn events(&self, entity_id: &EntityId) -> Result<Vec<SerializedEvent>, Error> {
        self.inner.events(entity_id).await
    }

    async fn events_after(
        &self,
        entity_id: &EntityId,
        after: EventSequence,
    ) -> Result<Vec<SerializedEvent>, Error> {
        self.inner.events_after(entity_id, after).await
    }

    async fn latest(&self, entity_id: &EntityId, n: usize) -> Result<Vec<SerializedEvent>, Error> {
        self.inner.latest(entity_id, n).await
    }

    async fn snapshot(&self, entity_id: &EntityId) -> Result<Option<SerializedSnapshot>, Error> {
        self.inner.snapshot(entity_id).await
    }

    async fn put_snapshot(&self, snapshot: &SerializedSnapshot) -> Result<bool, Error> {
        self.inner.put_snapshot(snapshot).await
    }

    async fn compact_below(
        &self,
        entity_id: &EntityId,
        through: EventSequence,
    ) -> Result<usize, Error> {
        self.inner.compact_below(entity_id, through).await
    }

    async fn entity_heads(
        &self,
        entity_type: Option<&EntityType>,
    ) -> Result<BTreeMap<EntityId, EventSequence>, Error> {
        self.inner.entity_heads(entity_type).await
    }
}

/// A store over a [`Meddling`] repository whose retry policy has `attempts` attempts.
fn retry_store<'m, R: Repository + 'm>(
    meddling: &'m Meddling<'_, R>,
    attempts: u32,
) -> Store<Ledger, &'m Meddling<'m, R>> {
    Store::new(meddling, ())
        .with_snapshot_interval(0)
        .with_outcomes(outcome_config().with_retry(RetryPolicy::new(attempts)))
}

fn added(amount: i64) -> Vec<LedgerEvent> {
    vec![Added { amount }.into()]
}

async fn seed<R: Repository>(repo: &R, id: &EntityId, amount: i64) -> CaseResult {
    Store::<Ledger, _>::new(repo, ())
        .with_snapshot_interval(0)
        .execute(id, Add(amount), metadata())
        .await?;

    Ok(())
}

/// The engine-filled copy command: reads `watched` (an engine-filled fence), writes its total into
/// `target`, and records how often it ran and what it saw.
struct CopyCommand<'a> {
    watched: &'a EntityId,
    target: &'a EntityId,
    calls: AtomicUsize,
    saw: Mutex<Vec<i64>>,
}

impl<'a> CopyCommand<'a> {
    fn new(watched: &'a EntityId, target: &'a EntityId) -> Self {
        Self {
            watched,
            target,
            calls: AtomicUsize::new(0),
            saw: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn saw(&self) -> Vec<i64> {
        self.saw
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    async fn prepare<R: Repository>(
        &self,
        tx: &mut crate::Transaction<'_, R>,
    ) -> Result<Decision, Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);

        let source = tx.read::<Ledger>(self.watched).await?;
        let mut destination = tx.read::<Ledger>(self.target).await?;
        let total = source.aggregate().total;
        self.saw
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(total);

        tx.append(&mut destination, added(total))?;

        Ok(Decision::accept_with(total.to_string()))
    }
}

async fn retry_engine_filled<R: Repository>(repo: &R) -> CaseResult {
    let watched = ledger_id();
    let target = ledger_id();
    seed(repo, &watched, 1).await?;

    // Another writer moves the watched stream between the command's reads and its first commit.
    let meddling = Meddling::new(repo, &watched, Trigger::Fences, 1);
    let store = retry_store(&meddling, 3);
    let copy = CopyCommand::new(&watched, &target);
    let identity = CommandIdentity::new("ledger:copy").with_targets([target.clone()]);
    let command = command_id();

    let outcome = store
        .execute_transaction(&identity, &command_metadata(&command), |tx| {
            Box::pin(copy.prepare(tx))
        })
        .await?;
    ensure!(
        outcome.is_accepted() && !outcome.replayed,
        "the re-prepared command committed: {outcome:?}"
    );
    ensure_eq!(
        copy.calls(),
        2,
        "the failed fence re-prepared the command once"
    );
    ensure_eq!(
        copy.saw(),
        vec![1, 2],
        "the second attempt decided on the fresh state"
    );
    ensure_eq!(outcome.response(), Some(&b"2"[..]), "the response");
    ensure_eq!(
        meddling.delays().len(),
        1,
        "one backoff between the two attempts"
    );

    // The failed attempt wrote nothing: the target holds exactly the committing attempt's event.
    let events = repo.events(&target).await?;
    ensure_eq!(events.len(), 1, "one event in the target");
    ensure_eq!(
        String::from_utf8_lossy(&events[0].payload).into_owned(),
        r#"{"amount":2}"#.to_owned(),
        "the committing attempt's decision"
    );
    ensure_eq!(
        repo.outcome(&command).await?,
        Some(outcome.outcome.clone()),
        "exactly one outcome row, the committed one"
    );

    Ok(())
}

/// A command that decided on a caller-supplied head of `referral`, stamped as a caller fence.
struct StampedCommand<'a> {
    referral: &'a EntityId,
    target: &'a EntityId,
    calls: AtomicUsize,
}

impl StampedCommand<'_> {
    async fn prepare<R: Repository>(
        &self,
        tx: &mut crate::Transaction<'_, R>,
        head: EventSequence,
    ) -> Result<Decision, Error> {
        self.calls.fetch_add(1, Ordering::SeqCst);

        let mut ledger = tx.read::<Ledger>(self.target).await?;
        tx.append(&mut ledger, added(1))?;
        tx.fence(ReadFence::new(
            self.referral.clone(),
            Ledger::TYPE,
            head,
            FenceProvenance::Caller,
        ))?;

        Ok(Decision::accept())
    }
}

async fn retry_caller_fenced<R: Repository>(repo: &R) -> CaseResult {
    let referral = ledger_id();
    let target = ledger_id();
    seed(repo, &referral, 1).await?;

    let identity = CommandIdentity::new("ledger:rename")
        .with_targets([target.clone()])
        .with_payload(&"Grace")?;
    let stamped = StampedCommand {
        referral: &referral,
        target: &target,
        calls: AtomicUsize::new(0),
    };

    // The caller decided on the referral at head 1; another writer moves it before the commit.
    let meddling = Meddling::new(repo, &referral, Trigger::Events, 1);
    let store = retry_store(&meddling, 3);
    let command = command_id();

    let outcome = store
        .execute_transaction(&identity, &command_metadata(&command), |tx| {
            Box::pin(stamped.prepare(tx, EventSequence(1)))
        })
        .await?;
    ensure!(
        outcome.is_rejected() && !outcome.replayed,
        "a stale caller-stamped fence is a persisted rejection, not an error: {outcome:?}"
    );
    ensure_eq!(
        outcome.rejection().cloned(),
        Some(Rejection::stale_fence()),
        "the caller-fence stale-fence rejection"
    );
    ensure_eq!(
        stamped.calls.load(Ordering::SeqCst),
        1,
        "the command was not prepared again"
    );
    ensure!(meddling.delays().is_empty(), "and nothing waited");
    ensure!(
        repo.events(&target).await?.is_empty(),
        "the failed commit wrote none of its events"
    );

    // The old command ID is terminal: it replays the stored rejection without preparing anything.
    let replayed = store
        .execute_transaction(&identity, &command_metadata(&command), |tx| {
            Box::pin(stamped.prepare(tx, EventSequence(1)))
        })
        .await?;
    ensure!(replayed.replayed, "the old ID replays: {replayed:?}");
    ensure_eq!(replayed.outcome, outcome.outcome, "the replayed rejection");
    ensure_eq!(
        stamped.calls.load(Ordering::SeqCst),
        1,
        "a replay prepares nothing"
    );

    // A caller-stamped fence that held says nothing about an event conflict beside it: the failed
    // item is the event put, an ordinary conflict, so the command re-prepares and succeeds.
    let head = Store::<Ledger, _>::new(repo, ())
        .load(&referral)
        .await?
        .sequence();
    let rival = Meddling::new(repo, &target, Trigger::Events, 1);
    let store = retry_store(&rival, 3);
    stamped.calls.store(0, Ordering::SeqCst);

    let retried = store
        .execute_transaction(&identity, &command_metadata(&command_id()), |tx| {
            Box::pin(stamped.prepare(tx, head))
        })
        .await?;
    ensure!(
        retried.is_accepted(),
        "a caller fence that held does not turn a conflict into a rejection: {retried:?}"
    );
    ensure_eq!(
        stamped.calls.load(Ordering::SeqCst),
        2,
        "the event conflict re-prepared"
    );
    ensure_eq!(
        repo.events(&target).await?.len(),
        2,
        "the rival's event and the committing attempt's"
    );

    Ok(())
}

async fn retry_exhausted<R: Repository>(repo: &R) -> CaseResult {
    let watched = ledger_id();
    let target = ledger_id();
    seed(repo, &watched, 1).await?;

    // The watched stream moves before every commit that fences it. The rejection's own outcome-only
    // commit carries no fences, so it is left alone.
    let meddling = Meddling::new(repo, &watched, Trigger::Fences, u32::MAX);
    let store = retry_store(&meddling, 3);
    let copy = CopyCommand::new(&watched, &target);
    let identity = CommandIdentity::new("ledger:copy").with_targets([target.clone()]);
    let command = command_id();

    let outcome = store
        .execute_transaction(&identity, &command_metadata(&command), |tx| {
            Box::pin(copy.prepare(tx))
        })
        .await?;
    ensure!(
        outcome.is_rejected() && !outcome.replayed,
        "exhausted retries are a persisted rejection, not an error: {outcome:?}"
    );
    ensure_eq!(
        outcome.rejection().cloned(),
        Some(Rejection::retries_exhausted(3)),
        "the exhausted stale-fence rejection"
    );
    ensure_eq!(copy.calls(), 3, "prepared once per attempt and no more");
    ensure_eq!(copy.saw(), vec![1, 2, 3], "each attempt saw fresh state");

    let delays = meddling.delays();
    ensure_eq!(
        delays.len(),
        2,
        "a backoff between attempts, none after the last"
    );
    ensure!(delays[1] >= delays[0], "the delays grow: {delays:?}");
    ensure!(
        outcome.outcome.event_ids.is_empty(),
        "no event was committed"
    );
    ensure!(
        repo.events(&target).await?.is_empty(),
        "no attempt committed"
    );
    ensure_eq!(
        repo.outcome(&command).await?,
        Some(outcome.outcome.clone()),
        "the rejection is the command's stored outcome"
    );

    // A replay of the same command gets the same answer and prepares nothing.
    meddling.disarm();
    let replayed = store
        .execute_transaction(&identity, &command_metadata(&command), |tx| {
            Box::pin(copy.prepare(tx))
        })
        .await?;
    ensure!(replayed.replayed, "the command replays: {replayed:?}");
    ensure_eq!(replayed.outcome, outcome.outcome, "the replayed rejection");
    ensure_eq!(copy.calls(), 3, "a replay prepares nothing");

    // A different command under the same ID is still a reuse, never a replay of the rejection.
    let other = CommandIdentity::new("ledger:other").with_targets([target.clone()]);
    let reuse = store
        .execute_transaction(&other, &command_metadata(&command), |tx| {
            Box::pin(copy.prepare(tx))
        })
        .await;
    expect_reuse(reuse, "another identity under the exhausted command's ID")?;

    // The remedy is a new command ID, decided on the fresh state.
    let fresh = store
        .execute_transaction(&identity, &command_metadata(&command_id()), |tx| {
            Box::pin(copy.prepare(tx))
        })
        .await?;
    ensure!(fresh.is_accepted(), "a new ID commits: {fresh:?}");
    ensure_eq!(
        repo.events(&target).await?.len(),
        1,
        "the new command's event"
    );

    Ok(())
}

/// A rename: it releases the slot the index currently names, so the stream it writes comes from
/// state it reads, and records which slot each attempt released.
struct RenameCommand<'a> {
    ns: &'a str,
    index: &'a EntityId,
    profile: &'a EntityId,
    released: Mutex<Vec<EntityId>>,
}

impl RenameCommand<'_> {
    fn slot(&self, n: i64) -> EntityId {
        EntityId::new(format!("{}-slot-{n}", self.ns))
    }

    fn released(&self) -> Vec<EntityId> {
        self.released
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    async fn prepare<R: Repository>(
        &self,
        tx: &mut crate::Transaction<'_, R>,
    ) -> Result<Decision, Error> {
        let holder = tx.read::<Ledger>(self.index).await?;
        let released = self.slot(holder.aggregate().total);
        self.released
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(released.clone());

        let mut profile = tx.read::<Ledger>(self.profile).await?;
        let mut slot = tx.read::<Ledger>(&released).await?;
        tx.append(&mut profile, added(1))?;
        tx.append(&mut slot, added(1))?;

        Ok(Decision::accept())
    }
}

async fn retry_write_set_changes<R: Repository>(repo: &R) -> CaseResult {
    let ns = namespace();
    let index = EntityId::new(format!("{ns}-index"));
    let profile = EntityId::new(format!("{ns}-profile"));
    seed(repo, &index, 1).await?;

    // The holder changes under the first attempt, so the second derives another write set.
    let meddling = Meddling::new(repo, &index, Trigger::Fences, 1);
    let store = retry_store(&meddling, 3);
    let rename = RenameCommand {
        ns: &ns,
        index: &index,
        profile: &profile,
        released: Mutex::new(Vec::new()),
    };

    // The request names the profile; the slot it releases is not part of its identity.
    let identity = CommandIdentity::new("ledger:rename")
        .with_targets([profile.clone()])
        .with_payload(&"grace")?;
    let command = command_id();
    let metadata = command_metadata(&command);

    let outcome = store
        .execute_transaction(&identity, &metadata, |tx| Box::pin(rename.prepare(tx)))
        .await?;
    ensure!(
        outcome.is_accepted(),
        "the re-prepared command committed: {outcome:?}"
    );
    ensure_eq!(
        rename.released(),
        vec![rename.slot(1), rename.slot(2)],
        "the write set changed between attempts"
    );
    ensure!(
        repo.events(&rename.slot(1)).await?.is_empty(),
        "the first attempt's write set was never committed"
    );
    ensure_eq!(
        repo.events(&rename.slot(2)).await?.len(),
        1,
        "the second attempt's slot"
    );
    ensure_eq!(repo.events(&profile).await?.len(), 1, "the profile");
    ensure_eq!(
        outcome.outcome.event_ids,
        vec![
            EventId::for_event(&profile, EventSequence(1)),
            EventId::for_event(&rename.slot(2), EventSequence(1)),
        ],
        "the outcome names every stream the commit wrote"
    );

    // The recorded digest is the identity's: the request's targets, never the streams written.
    ensure_eq!(
        outcome.outcome.digest,
        default_digest(&identity.digest_input(&metadata))
            .map_err(|err| Failure(err.to_string()))?,
        "the identity digest"
    );

    // A client retry that arrives after the rename landed is the same command, not a key reuse.
    let again = store
        .execute_transaction(&identity, &metadata, |tx| Box::pin(rename.prepare(tx)))
        .await?;
    ensure!(again.replayed, "the retry replays: {again:?}");
    ensure_eq!(again.outcome, outcome.outcome, "the replayed outcome");
    ensure_eq!(rename.released().len(), 2, "a replay prepares nothing");

    Ok(())
}

async fn retry_single_aggregate<R: Repository>(repo: &R) -> CaseResult {
    let target = ledger_id();
    seed(repo, &target, 1).await?;

    let responses = AtomicUsize::new(0);
    let respond = |ledger: &Ledger, _: &[LedgerEvent]| {
        responses.fetch_add(1, Ordering::SeqCst);

        Ok(Some(ledger.total.to_string().into_bytes()))
    };

    // Another writer appends between the command's load and its commit, once: a single-aggregate
    // command fences nothing, so the conflict re-prepares.
    let meddling = Meddling::new(repo, &target, Trigger::Events, 1);
    let store = retry_store(&meddling, 3);
    let outcome = store
        .execute_with_response(&target, Add(2), command_metadata(&command_id()), respond)
        .await?;
    ensure!(outcome.is_accepted(), "the retry committed: {outcome:?}");
    ensure_eq!(
        responses.load(Ordering::SeqCst),
        2,
        "the handler ran again against the fresh state"
    );
    ensure_eq!(outcome.response(), Some(&b"4"[..]), "1 + the rival's 1 + 2");
    ensure_eq!(repo.events(&target).await?.len(), 3, "no event twice");

    // Every attempt loses: the rejection is recorded.
    let meddling = Meddling::new(repo, &target, Trigger::Events, u32::MAX);
    let store = retry_store(&meddling, 3);
    let command = command_id();
    responses.store(0, Ordering::SeqCst);

    let outcome = store
        .execute_with_response(&target, Add(2), command_metadata(&command), respond)
        .await?;
    ensure!(outcome.is_rejected(), "the attempts ran out: {outcome:?}");
    ensure_eq!(
        outcome.rejection().cloned(),
        Some(Rejection::retries_exhausted(3)),
        "the exhausted stale-fence rejection"
    );
    ensure_eq!(responses.load(Ordering::SeqCst), 3, "one run per attempt");
    ensure_eq!(
        repo.events(&target).await?.len(),
        6,
        "three rival events; the command committed none"
    );

    meddling.disarm();
    let replayed = store
        .execute_with_response(&target, Add(2), command_metadata(&command), respond)
        .await?;
    ensure!(replayed.replayed, "the command replays: {replayed:?}");
    ensure_eq!(replayed.outcome, outcome.outcome, "the replayed rejection");
    ensure_eq!(responses.load(Ordering::SeqCst), 3, "a replay runs nothing");

    Ok(())
}

async fn retry_digest_once<R: Repository>(repo: &R) -> CaseResult {
    // A digester with state: it returns a different value on every call, so a digest recomputed
    // per attempt would drift from the one the first attempt computed.
    let calls = std::sync::Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let config = outcome_config()
        .with_retry(RetryPolicy::immediate(3))
        .with_digester(move |_| Ok(format!("digest-{}", counted.fetch_add(1, Ordering::SeqCst))));

    // A multi-stream command that loses its first attempt to a moved fence.
    let watched = ledger_id();
    let target = ledger_id();
    seed(repo, &watched, 1).await?;

    let meddling = Meddling::new(repo, &watched, Trigger::Fences, 1);
    let store = Store::<Ledger, _>::new(&meddling, ())
        .with_snapshot_interval(0)
        .with_outcomes(config.clone());
    let copy = CopyCommand::new(&watched, &target);
    let identity = CommandIdentity::new("ledger:copy").with_targets([target.clone()]);

    let outcome = store
        .execute_transaction(&identity, &command_metadata(&command_id()), |tx| {
            Box::pin(copy.prepare(tx))
        })
        .await?;
    ensure!(outcome.is_accepted(), "the retry committed: {outcome:?}");
    ensure_eq!(copy.calls(), 2, "the command was prepared twice");
    ensure_eq!(calls.load(Ordering::SeqCst), 1, "the digester ran once");
    ensure_eq!(
        outcome.outcome.digest,
        "digest-0".to_owned(),
        "the stored digest is the first value"
    );

    // The single-aggregate path, with a rival append that costs it one attempt.
    calls.store(0, Ordering::SeqCst);
    let single = ledger_id();
    seed(repo, &single, 1).await?;

    let meddling = Meddling::new(repo, &single, Trigger::Events, 1);
    let store = Store::<Ledger, _>::new(&meddling, ())
        .with_snapshot_interval(0)
        .with_outcomes(config);
    let outcome = store
        .execute_with_outcome(&single, Add(2), command_metadata(&command_id()))
        .await?;
    ensure!(outcome.is_accepted(), "the retry committed: {outcome:?}");
    ensure_eq!(calls.load(Ordering::SeqCst), 1, "the digester ran once");
    ensure_eq!(
        outcome.outcome.digest,
        "digest-0".to_owned(),
        "the stored digest is the first value"
    );

    Ok(())
}

async fn retry_deadline<R: Repository>(repo: &R) -> CaseResult {
    let target = ledger_id();
    seed(repo, &target, 1).await?;

    // Every attempt loses, and the deadline has passed once the first has run: the retries stop
    // there, and the attempts made are what the rejection records.
    let meddling = Meddling::new(repo, &target, Trigger::Events, u32::MAX);
    let store = Store::<Ledger, _>::new(&meddling, ())
        .with_snapshot_interval(0)
        .with_outcomes(
            outcome_config()
                .with_retry(RetryPolicy::immediate(5).with_deadline(Duration::from_nanos(1))),
        );
    let runs = AtomicUsize::new(0);

    let outcome = store
        .execute_with_response(&target, Add(2), command_metadata(&command_id()), |_, _| {
            runs.fetch_add(1, Ordering::SeqCst);

            Ok(None)
        })
        .await?;
    ensure!(
        outcome.is_rejected(),
        "a conflict past the deadline is recorded like exhausted attempts: {outcome:?}"
    );
    ensure_eq!(
        outcome.rejection().cloned(),
        Some(Rejection::retries_exhausted(1)),
        "the attempts made"
    );
    ensure_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "no attempt started past the deadline"
    );

    Ok(())
}

async fn retry_reserved_stream<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);
    let named = ledger_id();

    // The identity no longer limits the write set, so the commit is what refuses a reserved
    // stream, before any write and without retrying.
    for reserved in ["SYSTEM#retry-case", "command-outcome#retry-case"] {
        let command = command_id();
        let calls = AtomicUsize::new(0);
        let identity = CommandIdentity::new("ledger:copy").with_targets([named.clone()]);

        let result = store
            .execute_transaction(&identity, &command_metadata(&command), |tx| {
                calls.fetch_add(1, Ordering::SeqCst);

                Box::pin(async move {
                    let mut stream = tx.read::<Ledger>(reserved).await?;
                    tx.append(&mut stream, added(1))?;

                    Ok(Decision::accept())
                })
            })
            .await;
        ensure!(
            matches!(&result, Err(Error::ReservedEntityId { .. })),
            "{reserved}: a reserved stream is refused: {result:?}"
        );
        ensure_eq!(calls.load(Ordering::SeqCst), 1, "and not retried");
        ensure!(
            repo.outcome(&command).await?.is_none(),
            "{reserved}: no outcome was recorded"
        );
    }

    Ok(())
}

async fn outcome_write_set_is_not_identity<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);
    let named = ledger_id();
    let derived = ledger_id();
    let command = command_id();
    let identity = CommandIdentity::new("ledger:copy").with_targets([named.clone()]);
    let metadata = command_metadata(&command);

    let mut tx = store.transaction();
    tx.begin_command(&identity, &metadata).await?;
    let mut first = tx.read::<Ledger>(&named).await?;
    let mut second = tx.read::<Ledger>(&derived).await?;
    tx.append(&mut first, added(1))?;
    tx.append(&mut second, added(1))?;
    let outcome = tx.commit_with_outcome(None).await?;
    ensure!(
        outcome.is_accepted(),
        "a stream derived from state needs no declaration: {outcome:?}"
    );
    ensure_eq!(repo.events(&named).await?.len(), 1, "the named stream");
    ensure_eq!(repo.events(&derived).await?.len(), 1, "the derived stream");

    // The same identity replays the outcome, which lists every stream the commit wrote.
    let mut tx = store.transaction();
    match tx.begin_command(&identity, &metadata).await? {
        Begin::Replayed(replayed) => {
            ensure_eq!(replayed.outcome, outcome.outcome, "the replayed outcome");
            ensure_eq!(
                replayed.outcome.event_ids.len(),
                2,
                "both streams are named"
            );
        }
        Begin::Proceed => return Err(Failure("the same identity must replay".into())),
    }

    // A request that names a different stream is a different command.
    let other = CommandIdentity::new("ledger:copy").with_targets([derived.clone()]);
    let mut tx = store.transaction();
    expect_reuse(
        tx.begin_command(&other, &metadata).await,
        "another target under the same command ID",
    )?;

    Ok(())
}

fn metadata_for_plain() -> Metadata {
    metadata()
}

async fn outcome_atomicity<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);
    let plain = Store::<Ledger, _>::new(repo, ()).with_snapshot_interval(0);
    let (a, b) = (ledger_id(), ledger_id());
    let command = command_id();
    let identity = CommandIdentity::new("ledger:pair").with_targets([a.clone(), b.clone()]);

    let mut tx = store.transaction();
    tx.begin_command(&identity, &command_metadata(&command))
        .await?;
    let mut first = tx.read::<Ledger>(&a).await?;
    let mut second = tx.read::<Ledger>(&b).await?;
    tx.append(&mut first, vec![Added { amount: 1 }.into()])?;
    tx.append(&mut second, vec![Added { amount: 1 }.into()])?;

    // Another writer takes the second stream's first sequence.
    plain.execute(&b, Add(9), metadata()).await?;

    let result = tx.commit_with_outcome(None).await;
    ensure!(
        matches!(&result, Err(err) if err.is_conflict()),
        "a stream conflict fails the commit: {result:?}"
    );
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "a failed commit leaves no outcome row"
    );
    ensure!(
        repo.events(&a).await?.is_empty(),
        "and no events on the other stream"
    );

    // The retry decides again against the new state and commits both streams with the outcome.
    let mut tx = store.transaction();
    tx.begin_command(&identity, &command_metadata(&command))
        .await?;
    let mut first = tx.read::<Ledger>(&a).await?;
    let mut second = tx.read::<Ledger>(&b).await?;
    tx.append(&mut first, vec![Added { amount: 1 }.into()])?;
    tx.append(&mut second, vec![Added { amount: 1 }.into()])?;
    let committed = tx.commit_with_outcome(Some(b"done".to_vec())).await?;
    let mut want = [a.clone(), b.clone()];
    want.sort_by(|x, y| x.as_str().as_bytes().cmp(y.as_str().as_bytes()));
    let sequences = [
        (want[0].clone(), if want[0] == a { 1 } else { 2 }),
        (want[1].clone(), if want[1] == a { 1 } else { 2 }),
    ];
    ensure_eq!(
        committed.outcome.event_ids,
        sequences
            .iter()
            .map(|(id, sequence)| EventId::for_event(id, EventSequence(*sequence)))
            .collect::<Vec<_>>(),
        "the outcome lists the events in stream-ID byte order"
    );
    ensure_eq!(committed.response(), Some(b"done".as_slice()), "response");

    Ok(())
}

async fn outcome_budget<R: Repository>(repo: &R) -> CaseResult {
    let accepted = |command| Outcome::accepted(command, "digest");

    // marker check + rebuild-lock check + head update + 96 event puts + outcome put = 100
    // actions.
    let creates = Stream::new(&namespace(), "creates", "ledger");
    let command = command_id();
    let result = creates
        .commit_with_outcome(repo, creates.events(1..=97), accepted(command.clone()))
        .await;
    ensure!(
        matches!(&result, Err(Error::CommitTooLarge { actions: 101, .. })),
        "97 events plus the outcome are one action too many: {result:?}"
    );
    ensure!(
        repo.events(&creates.id).await?.is_empty() && repo.outcome(&command).await?.is_none(),
        "an oversized commit writes nothing"
    );
    creates
        .commit_with_outcome(repo, creates.events(1..=96), accepted(command.clone()))
        .await?;
    ensure_eq!(
        repo.outcome(&command)
            .await?
            .map(|stored| stored.event_ids.len()),
        Some(96),
        "96 events fill the budget with the outcome"
    );

    // Appending costs the same as creating: the head update replaces the continuation check, so
    // marker + lock + head update + 96 puts + outcome fill the budget.
    let grows = Stream::new(&namespace(), "grows", "ledger");
    grows.commit(repo, grows.events(1..=1), None).await?;
    let result = grows
        .commit_with_outcome(repo, grows.events(2..=98), accepted(command_id()))
        .await;
    ensure!(
        matches!(&result, Err(Error::CommitTooLarge { actions: 101, .. })),
        "97 appended events plus the outcome are one too many: {result:?}"
    );
    grows
        .commit_with_outcome(repo, grows.events(2..=97), accepted(command_id()))
        .await?;
    ensure_eq!(
        repo.events(&grows.id).await?.len(),
        97,
        "96 appended events fit"
    );

    // The same arithmetic holds for a transaction.
    let multi = Stream::new(&namespace(), "multi", "ledger");
    let result = repo
        .commit_transaction(
            TransactionCommit::new(vec![write(&multi, 1..=97)])
                .with_outcome(accepted(command_id())),
        )
        .await;
    ensure!(
        matches!(&result, Err(Error::CommitTooLarge { actions: 101, .. })),
        "a transaction counts the outcome item too: {result:?}"
    );
    repo.commit_transaction(
        TransactionCommit::new(vec![write(&multi, 1..=96)]).with_outcome(accepted(command_id())),
    )
    .await?;

    // An outcome-only commit is the two control checks and the outcome put.
    let command = command_id();
    repo.commit_transaction(
        TransactionCommit::<R::Operation>::new(Vec::new()).with_outcome(accepted(command.clone())),
    )
    .await?;
    ensure!(
        repo.outcome(&command)
            .await?
            .is_some_and(|stored| stored.is_noop()),
        "an outcome-only transaction stores an accepted no-op"
    );

    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Rejections that carry events
// ---------------------------------------------------------------------------------------------

fn wallet_store<R: Repository>(repo: R, snapshot_interval: usize) -> Store<Wallet, R> {
    Store::new(repo, ())
        .with_snapshot_interval(snapshot_interval)
        .with_outcomes(outcome_config())
}

/// The stored event types of a stream, in order.
async fn event_types<R: Repository>(repo: &R, id: &EntityId) -> Result<Vec<String>, Failure> {
    Ok(repo
        .events(id)
        .await?
        .iter()
        .map(|event| event.event_type.to_string())
        .collect())
}

fn types(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

async fn rejection_events<R: Repository>(repo: &R) -> CaseResult {
    // A snapshot interval of two puts a snapshot on the rejection's own event.
    let store = wallet_store(repo, 2);
    let id = ledger_id();

    let funded = store
        .execute_with_outcome(&id, WalletCommand::Fund(2), command_metadata(&command_id()))
        .await?;
    ensure!(funded.is_accepted(), "the seed commits: {funded:?}");

    // A spend over the balance is rejected, and the refusal is recorded in the same commit.
    let command = command_id();
    let rejected = store
        .execute_with_outcome(&id, WalletCommand::Spend(5), command_metadata(&command))
        .await?;
    ensure!(
        rejected.is_rejected() && !rejected.replayed,
        "a fresh rejection: {rejected:?}"
    );
    ensure_eq!(
        rejected
            .rejection()
            .map(|rejection| rejection.code.as_str()),
        Some("insufficient-funds"),
        "the rejection code"
    );
    ensure_eq!(
        rejected.outcome.event_ids,
        vec![EventId::for_event(&id, EventSequence(2))],
        "the rejected row lists the event it wrote"
    );
    ensure_eq!(rejected.events.len(), 1, "the committed events");
    ensure_eq!(rejected.typed_events.len(), 1, "the typed events");
    ensure!(
        rejected.response().is_none(),
        "a rejection stores no response"
    );
    ensure_eq!(
        event_types(repo, &id).await?,
        types(&["wallet:funded", "wallet:refused"]),
        "the refusal is on the stream"
    );
    ensure_eq!(
        repo.outcome(&command).await?,
        Some(rejected.outcome.clone()),
        "the stored row is the returned outcome"
    );

    let loaded = store.load(&id).await?;
    ensure_eq!(loaded.aggregate().balance, 2, "the refusal moved no funds");
    ensure_eq!(loaded.aggregate().refusals, 1, "and is part of the state");
    ensure!(
        repo.snapshot(&id).await?.is_some(),
        "the rejection's events count toward the snapshot interval"
    );

    // A replay returns the stored rejection and writes nothing again.
    let again = store
        .execute_with_outcome(&id, WalletCommand::Spend(5), command_metadata(&command))
        .await?;
    ensure!(again.replayed && again.is_rejected(), "a replay: {again:?}");
    ensure_eq!(again.outcome, rejected.outcome, "the replayed rejection");
    ensure!(again.events.is_empty(), "a replay commits nothing");
    ensure_eq!(repo.events(&id).await?.len(), 2, "no duplicate event");

    // The ID is terminal for its digest, and another command under it is a reuse.
    let reuse = store
        .execute_with_outcome(&id, WalletCommand::Spend(6), command_metadata(&command))
        .await;
    expect_reuse(reuse, "another identity under the rejected command's ID")?;
    ensure_eq!(repo.events(&id).await?.len(), 2, "nothing was written");

    // A plain rejection (no events behind it) is unchanged.
    let plain = store
        .execute_with_outcome(
            &id,
            WalletCommand::Spend(0),
            command_metadata(&command_id()),
        )
        .await?;
    ensure!(
        plain.is_rejected() && plain.outcome.event_ids.is_empty() && plain.events.is_empty(),
        "a rejection without events writes none: {plain:?}"
    );
    ensure_eq!(repo.events(&id).await?.len(), 2, "still two events");

    // A spend within the balance is an ordinary accepted command, with no failure event.
    let fine = store
        .execute_with_outcome(
            &id,
            WalletCommand::Spend(1),
            command_metadata(&command_id()),
        )
        .await?;
    ensure!(fine.is_accepted(), "within the balance: {fine:?}");
    ensure_eq!(
        event_types(repo, &id).await?,
        types(&["wallet:funded", "wallet:refused", "wallet:spent"]),
        "the stream"
    );

    Ok(())
}

/// A prepare step that stages a refusal on the wallet and rejects, keeping the staged event or not.
async fn refuse_staged<R: Repository>(
    tx: &mut crate::Transaction<'_, R>,
    id: &EntityId,
    with_events: bool,
) -> Result<Decision, Error> {
    let mut wallet = tx.read::<Wallet>(id).await?;
    tx.append(&mut wallet, vec![Refused { amount: 7 }.into()])?;

    let rejection = Rejection::new("apply-failed", "the base is stale");

    Ok(if with_events {
        Decision::reject_with_events(rejection)
    } else {
        Decision::reject(rejection)
    })
}

async fn rejection_events_transaction<R: Repository>(repo: &R) -> CaseResult {
    let store = outcome_store(repo);
    let id = ledger_id();
    let identity = CommandIdentity::new("wallet:apply").with_targets([id.clone()]);
    let refusal = || Rejection::new("apply-failed", "the base is stale");

    let command = command_id();

    // The staged event, on a stream the command read, commits with the rejection; plain
    // rejection discards it, as before.
    let plain = store
        .execute_transaction(&identity, &command_metadata(&command_id()), |tx| {
            Box::pin(refuse_staged(tx, &id, false))
        })
        .await?;
    ensure!(
        plain.is_rejected() && plain.outcome.event_ids.is_empty() && plain.events.is_empty(),
        "reject discards staged events: {plain:?}"
    );
    ensure!(repo.events(&id).await?.is_empty(), "and writes none");

    let marked = store
        .execute_transaction(&identity, &command_metadata(&command), |tx| {
            Box::pin(refuse_staged(tx, &id, true))
        })
        .await?;
    ensure!(
        marked.is_rejected() && !marked.replayed,
        "a fresh rejection: {marked:?}"
    );
    ensure_eq!(
        marked.rejection().cloned(),
        Some(refusal()),
        "the rejection"
    );
    ensure_eq!(
        marked.outcome.event_ids,
        vec![EventId::for_event(&id, EventSequence(1))],
        "the row lists the event"
    );
    ensure_eq!(marked.events.len(), 1, "the committed events");
    ensure_eq!(
        event_types(repo, &id).await?,
        types(&["wallet:refused"]),
        "the stream"
    );
    ensure_eq!(
        repo.outcome(&command).await?,
        Some(marked.outcome.clone()),
        "the stored row"
    );

    // A replay prepares nothing and writes nothing.
    let calls = AtomicUsize::new(0);
    let replayed = store
        .execute_transaction(&identity, &command_metadata(&command), |_| {
            calls.fetch_add(1, Ordering::SeqCst);

            Box::pin(async { Ok(Decision::accept()) })
        })
        .await?;
    ensure!(replayed.replayed, "a replay: {replayed:?}");
    ensure_eq!(replayed.outcome, marked.outcome, "the replayed rejection");
    ensure_eq!(calls.load(Ordering::SeqCst), 0, "nothing was prepared");
    ensure_eq!(repo.events(&id).await?.len(), 1, "no duplicate event");

    // The Transaction API: with nothing staged it is an outcome-only rejection.
    let mut tx = store.transaction();
    tx.begin_command(&identity, &command_metadata(&command_id()))
        .await?;
    tx.read::<Wallet>(&id).await?;
    let bare = tx.reject_with_events(refusal()).await?;
    ensure!(
        bare.is_rejected() && bare.outcome.event_ids.is_empty(),
        "no staged events: {bare:?}"
    );

    Ok(())
}

async fn rejection_events_rules<R: Repository>(repo: &R) -> CaseResult {
    let rejected = |command: CommandId| {
        Outcome::rejected(command, "digest", Rejection::new("refused", "refused"))
    };

    // A conflicting commit writes neither its events nor its rejected row.
    let stream = Stream::new(&namespace(), "rules", "ledger");
    stream.commit(repo, stream.events(1..=1), None).await?;
    let command = command_id();
    let result = stream
        .commit_with_outcome(repo, stream.events(1..=1), rejected(command.clone()))
        .await;
    expect_conflict(result, "a stale sequence")?;
    ensure!(
        repo.outcome(&command).await?.is_none() && repo.events(&stream.id).await?.len() == 1,
        "a failed commit leaves no row"
    );

    // The winning commit writes both, and the row lists the events.
    let command = command_id();
    stream
        .commit_with_outcome(repo, stream.events(2..=3), rejected(command.clone()))
        .await?;
    let stored = repo
        .outcome(&command)
        .await?
        .ok_or_else(|| Failure("the rejected row".into()))?;
    ensure!(stored.is_rejected(), "the row is a rejection");
    ensure_eq!(
        stored.event_ids,
        vec![
            EventId::for_event(&stream.id, EventSequence(2)),
            EventId::for_event(&stream.id, EventSequence(3)),
        ],
        "the row lists what the commit wrote"
    );

    // A reserved stream is refused before any write.
    let reserved = Stream::new("SYSTEM#", &namespace(), "ledger");
    let command = command_id();
    let result = repo
        .commit_transaction(
            TransactionCommit::new(vec![write(&reserved, 1..=1)])
                .with_outcome(rejected(command.clone())),
        )
        .await;
    ensure!(
        matches!(&result, Err(Error::ReservedEntityId { .. })),
        "a reserved stream is refused: {result:?}"
    );
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "no row for a refused commit"
    );

    // The rejected row counts against the budget like an accepted one.
    let creates = Stream::new(&namespace(), "budget", "ledger");
    let command = command_id();
    let result = creates
        .commit_with_outcome(repo, creates.events(1..=97), rejected(command.clone()))
        .await;
    ensure!(
        matches!(&result, Err(Error::CommitTooLarge { actions: 101, .. })),
        "97 events plus the row are one action too many: {result:?}"
    );
    ensure!(
        repo.events(&creates.id).await?.is_empty() && repo.outcome(&command).await?.is_none(),
        "an oversized commit writes nothing"
    );
    creates
        .commit_with_outcome(repo, creates.events(1..=96), rejected(command_id()))
        .await?;

    // The same bound holds for a command: a transaction that stages too many events with its
    // rejection fails before any write.
    let store = outcome_store(repo);
    let id = ledger_id();
    let identity = CommandIdentity::new("wallet:apply").with_targets([id.clone()]);
    let command = command_id();
    let result = store
        .execute_transaction(&identity, &command_metadata(&command), |tx| {
            let id = id.clone();

            Box::pin(async move {
                let mut wallet = tx.read::<Wallet>(&id).await?;
                tx.append(
                    &mut wallet,
                    (0..98).map(|_| Refused { amount: 1 }.into()).collect(),
                )?;

                Ok(Decision::reject_with_events(Rejection::new(
                    "refused", "no",
                )))
            })
        })
        .await;
    ensure!(
        matches!(&result, Err(Error::CommitTooLarge { .. })),
        "an oversized rejection fails: {result:?}"
    );
    ensure!(
        repo.events(&id).await?.is_empty() && repo.outcome(&command).await?.is_none(),
        "and writes nothing"
    );

    // A reserved stream staged by a command is refused too, and not retried.
    let calls = AtomicUsize::new(0);
    let command = command_id();
    let result = store
        .execute_transaction(&identity, &command_metadata(&command), |tx| {
            calls.fetch_add(1, Ordering::SeqCst);

            Box::pin(async move {
                let mut wallet = tx.read::<Wallet>("SYSTEM#rejection-events").await?;
                tx.append(&mut wallet, vec![Refused { amount: 1 }.into()])?;

                Ok(Decision::reject_with_events(Rejection::new(
                    "refused", "no",
                )))
            })
        })
        .await;
    ensure!(
        matches!(&result, Err(Error::ReservedEntityId { .. })),
        "a reserved stream is refused: {result:?}"
    );
    ensure_eq!(calls.load(Ordering::SeqCst), 1, "and not retried");
    ensure!(
        repo.outcome(&command).await?.is_none(),
        "no outcome was recorded"
    );

    Ok(())
}

async fn rejection_events_retry<R: Repository>(repo: &R) -> CaseResult {
    let id = ledger_id();
    Store::<Wallet, _>::new(repo, ())
        .with_snapshot_interval(0)
        .execute(&id, WalletCommand::Fund(2), metadata())
        .await?;

    let wallet_retry = |meddling| {
        Store::<Wallet, _>::new(meddling, ())
            .with_snapshot_interval(0)
            .with_outcomes(outcome_config().with_retry(RetryPolicy::new(3)))
    };

    // Another writer moves the stream between the load and the first commit, once. Still short of
    // funds, the second attempt rejects again, and its event follows the rival's.
    let meddling = Meddling::new(repo, &id, Trigger::Events, 1).funding(1);
    let store = wallet_retry(&meddling);
    let rejected = store
        .execute_with_outcome(
            &id,
            WalletCommand::Spend(50),
            command_metadata(&command_id()),
        )
        .await?;
    ensure!(rejected.is_rejected(), "still short of funds: {rejected:?}");
    ensure_eq!(
        meddling.delays().len(),
        1,
        "the failed commit re-prepared the command once"
    );
    ensure_eq!(
        event_types(repo, &id).await?,
        types(&["wallet:funded", "wallet:funded", "wallet:refused"]),
        "only the committing attempt's event was written"
    );
    ensure_eq!(
        rejected.outcome.event_ids,
        vec![EventId::for_event(&id, EventSequence(3))],
        "the row lists the committed event"
    );

    // The rival's move can change the decision: now the command is accepted and records no
    // failure at all.
    let meddling = Meddling::new(repo, &id, Trigger::Events, 1).funding(100);
    let store = wallet_retry(&meddling);
    let flipped = store
        .execute_with_outcome(
            &id,
            WalletCommand::Spend(50),
            command_metadata(&command_id()),
        )
        .await?;
    ensure!(
        flipped.is_accepted(),
        "decided on the fresh state: {flipped:?}"
    );
    ensure_eq!(
        event_types(repo, &id).await?,
        types(&[
            "wallet:funded",
            "wallet:funded",
            "wallet:refused",
            "wallet:funded",
            "wallet:spent",
        ]),
        "no failure event from the superseded attempt"
    );

    // Every attempt loses: the stale-fence rejection is recorded with none of the caller's events.
    let before = repo.events(&id).await?.len();
    let meddling = Meddling::new(repo, &id, Trigger::Events, u32::MAX).funding(1);
    let store = wallet_retry(&meddling);
    let command = command_id();
    let lost = store
        .execute_with_outcome(&id, WalletCommand::Spend(5000), command_metadata(&command))
        .await?;
    ensure!(lost.is_rejected(), "the attempts ran out: {lost:?}");
    ensure_eq!(
        lost.rejection().cloned(),
        Some(Rejection::retries_exhausted(3)),
        "the exhausted stale-fence rejection"
    );
    ensure!(
        lost.outcome.event_ids.is_empty() && lost.events.is_empty(),
        "none of the caller's events: {lost:?}"
    );
    let after = event_types(repo, &id).await?;
    ensure_eq!(
        after.len(),
        before + 3,
        "three rival events and nothing else"
    );
    ensure!(
        after[before..].iter().all(|kind| kind == "wallet:funded"),
        "no refusal was written: {after:?}"
    );

    // The exhausted ID replays the stale-fence rejection.
    meddling.disarm();
    let replayed = store
        .execute_with_outcome(&id, WalletCommand::Spend(5000), command_metadata(&command))
        .await?;
    ensure!(replayed.replayed, "a replay: {replayed:?}");
    ensure_eq!(replayed.outcome, lost.outcome, "the replayed rejection");

    Ok(())
}

async fn rejection_events_caller_fence<R: Repository>(repo: &R) -> CaseResult {
    let referral = ledger_id();
    let target = ledger_id();
    seed(repo, &referral, 1).await?;

    let identity = CommandIdentity::new("wallet:apply").with_targets([target.clone()]);
    let calls = AtomicUsize::new(0);

    // The caller decided on the referral at head 1 and rejects with a failure event on the target;
    // another writer moves the referral before the commit.
    let meddling = Meddling::new(repo, &referral, Trigger::Events, 1);
    let store = retry_store(&meddling, 3);
    let command = command_id();

    let outcome = store
        .execute_transaction(&identity, &command_metadata(&command), |tx| {
            calls.fetch_add(1, Ordering::SeqCst);

            let (referral, target) = (referral.clone(), target.clone());

            Box::pin(async move {
                let mut wallet = tx.read::<Wallet>(&target).await?;
                tx.append(&mut wallet, vec![Refused { amount: 1 }.into()])?;
                tx.fence(ReadFence::new(
                    referral,
                    Ledger::TYPE,
                    EventSequence(1),
                    FenceProvenance::Caller,
                ))?;

                Ok(Decision::reject_with_events(Rejection::new(
                    "apply-failed",
                    "the base is stale",
                )))
            })
        })
        .await?;
    ensure!(
        outcome.is_rejected() && !outcome.replayed,
        "a stale caller-stamped fence is a persisted rejection, not an error: {outcome:?}"
    );
    ensure_eq!(
        outcome.rejection().cloned(),
        Some(Rejection::stale_fence()),
        "the stale-fence rejection, not the caller's"
    );
    ensure!(
        outcome.outcome.event_ids.is_empty() && outcome.events.is_empty(),
        "without the caller's events: {outcome:?}"
    );
    ensure_eq!(calls.load(Ordering::SeqCst), 1, "not prepared again");
    ensure!(
        repo.events(&target).await?.is_empty(),
        "nothing was written on the target"
    );
    ensure_eq!(
        repo.outcome(&command).await?,
        Some(outcome.outcome.clone()),
        "the stored row"
    );

    Ok(())
}

#[cfg(feature = "journal")]
mod compaction;
#[cfg(feature = "journal")]
mod journal;
mod rebuild;
mod view_repository;
mod views;

#[cfg(feature = "journal")]
#[cfg_attr(docsrs, doc(cfg(feature = "journal")))]
pub use compaction::compaction_cases;
#[cfg(feature = "journal")]
#[cfg_attr(docsrs, doc(cfg(feature = "journal")))]
pub use journal::journal_cases;
pub use rebuild::{NoViewRows, RebuildSuite, TestClock, projection_rebuild_cases};
pub use view_repository::{committed_view_cases, view_repository_cases};
pub use views::{CommitViewTarget, commit_view_cases};
