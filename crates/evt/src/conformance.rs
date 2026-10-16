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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::aggregate::{Aggregate, Projector};
use crate::error::{BoxError, Error};
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::features::{self, FeatureMarker};
use crate::ids::{EntityId, EntityType, EventId, EventSequence, EventType, EventVersion};
use crate::metadata::Metadata;
use crate::repository::{
    Commit, FenceProvenance, NewSnapshot, ReadFence, Repository, TransactionCommit,
};
use crate::retry::RetryPolicy;
use crate::store::Store;

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

/// Runs the conformance cases against one repository.
pub struct Suite<'a, R, V = NoViews> {
    repository: &'a R,
    snapshot_sequence_conflicts: bool,
    views: Option<V>,
}

impl<'a, R: Repository> Suite<'a, R> {
    /// Creates a suite over a repository. The repository may hold data from earlier runs.
    pub fn new(repository: &'a R) -> Self {
        Self {
            repository,
            snapshot_sequence_conflicts: true,
            views: None,
        }
    }
}

impl<'a, R: Repository, V: ViewTarget<R>> Suite<'a, R, V> {
    /// Whether a commit whose snapshot sequence is not the stored one plus one is rejected
    /// (default `true`). PostgreSQL relies on the event-sequence conflict instead, so it opts out.
    pub fn snapshot_sequence_conflicts(mut self, enforced: bool) -> Self {
        self.snapshot_sequence_conflicts = enforced;
        self
    }

    /// Adds the view storage for the projector case.
    pub fn with_views<W: ViewTarget<R>>(self, views: W) -> Suite<'a, R, W> {
        Suite {
            repository: self.repository,
            snapshot_sequence_conflicts: self.snapshot_sequence_conflicts,
            views: Some(views),
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
        record("compact below a snapshot", compact_below(repo).await);
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

    format!(
        "conf-{nanos}-{}",
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

    Ok(())
}

async fn commit_too_large<R: Repository>(repo: &R) -> CaseResult {
    let stream = Stream::new(&namespace(), "large", "ledger");

    // The feature-marker check, 98 event puts, and the floor check are exactly 100 actions, so a
    // 98-event commit fits on every backend. Only emitted actions count against the budget.
    let fits = Stream::new(&namespace(), "fits", "ledger");
    fits.commit(repo, fits.events(1..=98), None).await?;
    ensure_eq!(
        repo.events(&fits.id).await?.len(),
        98,
        "a commit that fills the budget exactly is accepted"
    );

    // 101 event puts plus the control rows and the floor check exceed the 100-item budget.
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

    // Compaction deletes the events the snapshot covers, which empties the slot a stale fence
    // checks — the companion on the snapshot item must catch the move.
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
                })
                .await?;
        }

        self.inner.commit(commit).await
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

/// Runs the feature-marker cases (v0.9, C9) against a repository that stores the
/// `SYSTEM#FEATURES` marker row. Like [`Suite`], it tolerates a table that holds data from
/// earlier runs: it clears the manifest first, so features a previous run left behind cannot
/// brick the table, and it leaves the manifest empty at the end.
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
        "the manifest starts empty",
        clear_manifest(repo, other).await,
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

/// Clears every declaration, so the run starts and ends with a manifest no writer refuses.
async fn clear_manifest<R: FeatureMarker>(repo: &R, other: &R) -> CaseResult {
    for repository in [repo, other] {
        repository
            .update_manifest(|manifest| {
                manifest.features.clear();
                manifest.history_dependent.clear();
                manifest.retention.clear();
            })
            .await?;
    }

    let manifest = repo.read_manifest().await?;
    ensure!(
        manifest.is_some_and(|manifest| {
            manifest.features.is_empty()
                && manifest.history_dependent.is_empty()
                && manifest.retention.is_empty()
        }),
        "the cleared manifest stores an empty feature set"
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
            manifest.features.insert("journal".to_owned(), 1);
        })
        .await?;
    let manifest = other
        .read_manifest()
        .await?
        .ok_or(Failure("no manifest".into()))?;
    ensure_eq!(
        manifest.features.get("journal"),
        Some(&1),
        "the declaration"
    );

    // Declaring never counts as validation, either on the declaring handle or one whose
    // cached validation predates the declaration. Maintenance snapshots obey the same refusal.
    let before_snapshot = repo.snapshot(&stream.id).await?;
    for writer in [other, repo] {
        let unsupported = |err: &Error| matches!(err, Error::UnsupportedFeature { feature, version: 1 } if feature == "journal");
        expect_err(
            stream.commit(writer, stream.events(6..=6), None).await,
            "committing to a table that declares journal",
            unsupported,
        )?;
        expect_err(
            writer.put_snapshot(&stream.snapshot(2, 5, "{}")).await,
            "writing a snapshot to a table that declares journal",
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
            manifest.features.remove("journal");
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
