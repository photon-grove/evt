//! A backend-neutral conformance suite for [`Repository`] implementations, the Rust counterpart of
//! Go's `conformance` package.
//!
//! It checks the contract documented on [`Repository`] (ordering, isolation, optimistic
//! concurrency, snapshots, compaction, entity heads) and the [`Store`] behavior built on it
//! (snapshot-aware loads, stale-commit conflicts, retries, duplicate commands, and in-band
//! projector operations that commit atomically with events). Every case namespaces its entity IDs,
//! so durable backends can share one table across runs. The repository cases also namespace entity
//! types; the [`Store`] cases use the [`Ledger`] type, so a shared table accumulates `ledger` rows.
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
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::aggregate::{Aggregate, Projector};
use crate::error::{BoxError, Error};
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::ids::{EntityId, EntityType, EventId, EventSequence, EventType, EventVersion};
use crate::metadata::Metadata;
use crate::repository::{Commit, NewSnapshot, Repository};
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
        record("isolate entities", isolate_entities(repo).await);
        record("events after a sequence", events_after(repo).await);
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
        record("entity heads", entity_heads(repo).await);
        record("store loads from a snapshot", store_snapshots(repo).await);
        record(
            "store rejects a stale commit",
            store_stale_commit(repo).await,
        );
        record("store retries a conflict", store_retry(repo).await);
        record(
            "store rejects a duplicate command",
            store_duplicate_command(repo).await,
        );
        record("store lists entity ids", store_entity_ids(repo).await);
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

async fn commit_and_read<R: Repository>(repo: &R) -> CaseResult {
    let stream = Stream::new(&namespace(), "1", "ledger");

    stream.commit(repo, stream.events(1..=3), None).await?;
    stream.commit(repo, stream.events(4..=5), None).await?;

    ensure_eq!(
        repo.events(&stream.id).await?,
        stream.events(1..=5),
        "events() returns every event as written, ascending"
    );

    Ok(())
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

async fn events_after<R: Repository>(repo: &R) -> CaseResult {
    let stream = Stream::new(&namespace(), "1", "ledger");
    stream.commit(repo, stream.events(1..=5), None).await?;

    for (after, want) in [
        (0, stream.events(1..=5)),
        (2, stream.events(3..=5)),
        (5, Vec::new()),
        (9, Vec::new()),
    ] {
        ensure_eq!(
            repo.events_after(&stream.id, EventSequence(after)).await?,
            want,
            format!("events_after({after})")
        );
    }

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
    let stream = Stream::new(&namespace(), "1", "ledger");
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

    let unknown = EntityType::new(format!("{ns}-unknown"));
    ensure!(
        repo.entity_heads(Some(&unknown)).await?.is_empty(),
        "an unknown type has no heads"
    );

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

/// Lets another writer commit the same sequences just before the first commit through it.
struct Interfering<'a, R> {
    inner: &'a R,
    armed: AtomicBool,
}

impl<R: Repository> Repository for Interfering<'_, R> {
    type Operation = R::Operation;

    async fn commit(&self, commit: Commit<R::Operation>) -> Result<(), Error> {
        if self.armed.swap(false, Ordering::SeqCst) {
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
        self.inner.events_after(entity_id, after).await
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

async fn store_retry<R: Repository>(repo: &R) -> CaseResult {
    let interfering = |inner| Interfering {
        inner,
        armed: AtomicBool::new(true),
    };

    let store = Store::<Ledger, _>::new(interfering(repo), ()).with_snapshot_interval(0);
    let id = ledger_id();
    let executed = store.execute_with_retry(&id, Add(5), metadata(), 2).await?;
    ensure_eq!(
        (executed.state.aggregate().total, executed.state.sequence()),
        (105, EventSequence(2)),
        "the retry reloads the competing event and commits after it"
    );

    let store = Store::<Ledger, _>::new(interfering(repo), ()).with_snapshot_interval(0);
    let result = store
        .execute_with_retry(&ledger_id(), Add(5), metadata(), 1)
        .await;
    ensure!(
        result.as_ref().is_err_and(|err| err.is_conflict()),
        "a single attempt returns the conflict: got {result:?}"
    );

    Ok(())
}

async fn store_duplicate_command<R: Repository>(repo: &R) -> CaseResult {
    let store = Store::<Ledger, _>::new(repo, ());
    let id = ledger_id();
    let metadata = metadata().with_command_id(format!("{id}-command"));

    store.execute(&id, Add(1), metadata.clone()).await?;
    let result = store.execute(&id, Add(1), metadata).await;
    ensure!(
        result.as_ref().is_err_and(|err| err.is_duplicate_command()),
        "a repeated command ID is a duplicate: got {result:?}"
    );
    ensure_eq!(
        store.load(&id).await?.sequence(),
        EventSequence(1),
        "the duplicate writes nothing"
    );

    Ok(())
}

async fn store_entity_ids<R: Repository>(repo: &R) -> CaseResult {
    let store = Store::<Ledger, _>::new(repo, ());
    let ids = [ledger_id(), ledger_id()];

    for id in &ids {
        store.execute(id, Add(1), metadata()).await?;
    }

    let listed = store.entity_ids().await?;
    for id in &ids {
        ensure!(listed.contains(id), "entity_ids() lists {id}");
    }

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
