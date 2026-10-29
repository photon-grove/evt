use std::sync::Mutex;

use super::*;
use crate::conformance::{Add, Added, Ledger, LedgerEvent, TestClock};
use crate::error::BoxError;
use crate::mem::MemRepository;
use crate::metadata::Metadata;
use crate::rebuild::acquired_from_settled;
use crate::store::Store;
use crate::views::ViewRepository;
use crate::{Commit, Outcome, RebuildGeneration, RebuildState, SerializedSnapshot, ViewRow};

const LEASE: Duration = Duration::from_secs(60);

/// A current-state projector: one row per stream holding its state, under `pk`.
struct Totals {
    pk: &'static str,
    fail_on: Option<&'static str>,
}

impl Totals {
    fn arc(pk: &'static str) -> Arc<dyn CommitProjector> {
        Arc::new(Self { pk, fail_on: None })
    }
}

impl CommitProjector for Totals {
    fn name(&self) -> &str {
        "totals"
    }

    fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
        commit
            .streams
            .iter()
            .map(|stream| {
                if self.fail_on == Some(stream.entity_id.as_str()) {
                    return Err("cannot project this one".into());
                }

                // A claim-style condition: the rebuild writes it without the condition.
                Ok(ViewOperation::put(
                    self.pk,
                    stream.entity_id.as_str(),
                    stream.entity_id.clone(),
                    stream.entity_type.as_str(),
                    stream.state.clone(),
                )
                .if_absent())
            })
            .collect()
    }
}

/// A history-dependent projector over `ledger`: a row per commit and stream, a latest row per
/// stream, and a marker row that is put after an odd number of entries and deleted after an even
/// one.
struct Feed {
    prefix: &'static str,
    history: Vec<EntityType>,
}

impl Feed {
    fn arc(prefix: &'static str) -> Arc<dyn CommitProjector> {
        Arc::new(Self {
            prefix,
            history: vec![EntityType::new(Ledger::TYPE)],
        })
    }
}

impl CommitProjector for Feed {
    fn name(&self) -> &str {
        "feed"
    }

    fn history(&self) -> &[EntityType] {
        &self.history
    }

    fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
        let mut operations = Vec::new();
        for stream in &commit.streams {
            let state: Ledger = serde_json::from_slice(&stream.state)?;
            let last = stream.events.last().map_or(0, |event| event.sequence.0);
            let id = stream.entity_id.as_str();
            let payload = format!(
                r#"{{"commit":{:?},"events":{},"total":{}}}"#,
                commit.commit_id,
                stream.events.len(),
                state.total
            );

            operations.push(ViewOperation::put(
                format!("{}-feed", self.prefix),
                format!("{id}#{last:04}"),
                stream.entity_id.clone(),
                "ledger",
                payload.clone(),
            ));
            operations.push(ViewOperation::put(
                format!("{}-latest", self.prefix),
                id,
                stream.entity_id.clone(),
                "ledger",
                payload,
            ));

            let marker = (format!("{}-marker", self.prefix), id.to_owned());
            operations.push(if state.entries % 2 == 1 {
                ViewOperation::put(marker.0, marker.1, stream.entity_id.clone(), "ledger", "{}")
            } else {
                ViewOperation::delete(marker.0, marker.1, stream.entity_id.clone())
            });
        }

        Ok(operations)
    }
}

async fn rows(repository: &MemRepository, pk: &str) -> Vec<(String, String, Vec<u8>)> {
    repository
        .list_views_by_pk(pk)
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.sk, row.entity_id, row.payload))
        .collect()
}

async fn add<R: Repository>(store: &Store<Ledger, R>, id: &str, amounts: &[i64]) {
    for amount in amounts {
        store
            .execute(id, Add(*amount), Metadata::default())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn current_state_projectors_rebuild_from_final_states_seeded_from_snapshots() {
    let repository = MemRepository::new();
    let store = Store::<Ledger, _>::new(&repository, ()).with_snapshot_interval(2);
    add(&store, "l-1", &[1, 2, 3]).await;
    add(&store, "l-2", &[10]).await;
    // l-1 keeps only its snapshot (at 2) and event 3.
    repository
        .compact_below(&"l-1".into(), EventSequence(2))
        .await
        .unwrap();

    let report = ProjectionRebuild::new(&repository)
        .with_projector(Totals::arc("totals"))
        .aggregate::<Ledger>()
        .run("rebuild-1", LEASE)
        .await
        .unwrap();

    assert_eq!(
        report,
        RebuildReport {
            entities: 2,
            commits: 0,
            puts: 2,
            deletes: 0,
            batches: 1,
            ..RebuildReport::default()
        }
    );
    assert_eq!(
        rows(&repository, "totals").await,
        vec![
            (
                "l-1".into(),
                "l-1".into(),
                br#"{"total":6,"entries":3}"#.to_vec()
            ),
            (
                "l-2".into(),
                "l-2".into(),
                br#"{"total":10,"entries":1}"#.to_vec()
            ),
        ]
    );
    assert_eq!(
        repository.rebuild_generation().await.unwrap(),
        RebuildGeneration {
            value: 2,
            force_released: false
        },
        "the run took and released the lock"
    );

    // The rows exist, and their `absent` condition is not evaluated: a second run rewrites them.
    let again = ProjectionRebuild::new(&repository)
        .with_projector(Totals::arc("totals"))
        .aggregate::<Ledger>()
        .run("rebuild-2", LEASE)
        .await
        .unwrap();
    assert_eq!(again.puts, 2);
}

#[tokio::test]
async fn history_dependent_projectors_replay_every_commit_and_reproduce_the_live_rows() {
    let repository = MemRepository::new();
    let live = Store::<Ledger, _>::new(&repository, ())
        .with_snapshot_interval(2)
        .with_commit_projector(Feed::arc("live"));
    add(&live, "l-1", &[1, 2]).await;
    add(&live, "l-2", &[5]).await;

    // A multi-stream commit with two events on one stream.
    let mut tx = live.transaction();
    let mut first = tx.read::<Ledger>("l-1").await.unwrap();
    let mut second = tx.read::<Ledger>("l-2").await.unwrap();
    tx.append(
        &mut first,
        vec![Added { amount: 3 }.into(), Added { amount: 4 }.into()],
    )
    .unwrap();
    tx.append(&mut second, vec![LedgerEvent::from(Added { amount: 6 })])
        .unwrap();
    tx.commit(&Metadata::default().with_command_id("transfer-1"))
        .await
        .unwrap();
    add(&live, "l-2", &[7]).await;

    let report = ProjectionRebuild::new(&repository)
        .with_projector(Feed::arc("rebuilt"))
        .aggregate::<Ledger>()
        .run("rebuild-1", LEASE)
        .await
        .unwrap();

    assert_eq!(
        report.commits, 5,
        "four single-stream commits and one transfer"
    );
    assert_eq!(report.entities, 0, "no current-state projector");
    for view in ["feed", "latest", "marker"] {
        assert_eq!(
            rows(&repository, &format!("rebuilt-{view}")).await,
            rows(&repository, &format!("live-{view}")).await,
            "the {view} rows"
        );
    }
    assert_eq!(rows(&repository, "rebuilt-feed").await.len(), 6);
    // l-1 ended with 4 entries, so its marker was deleted; l-2 with 3, so it stays.
    assert_eq!(
        rows(&repository, "rebuilt-marker").await,
        vec![("l-2".into(), "l-2".into(), b"{}".to_vec())]
    );
    assert_eq!(
        report.deletes, 1,
        "the deleted marker is written as a delete"
    );
}

#[tokio::test]
async fn a_history_dependent_replay_of_a_compacted_stream_fails_and_releases_the_lock() {
    let repository = MemRepository::new();
    let store = Store::<Ledger, _>::new(&repository, ()).with_snapshot_interval(2);
    add(&store, "l-1", &[1, 2, 3]).await;
    add(&store, "l-2", &[1]).await;
    repository
        .compact_below(&"l-1".into(), EventSequence(2))
        .await
        .unwrap();

    let error = ProjectionRebuild::new(&repository)
        .with_projector(Totals::arc("totals"))
        .with_projector(Feed::arc("rebuilt"))
        .aggregate::<Ledger>()
        .run("rebuild-1", LEASE)
        .await
        .unwrap_err();

    assert!(
        matches!(
            &error,
            Error::CompactionUncovered { entity_id, through, covered }
                if entity_id.as_str() == "l-1"
                    && *through == EventSequence(2)
                    && *covered == Some(EventSequence(2))
        ),
        "{error:?}"
    );
    assert!(
        rows(&repository, "totals").await.is_empty(),
        "nothing is written, not even the current-state rows"
    );
    assert_eq!(
        repository.rebuild_generation().await.unwrap(),
        RebuildGeneration {
            value: 2,
            force_released: false
        },
        "a run that wrote nothing releases the lock"
    );

    // A stream compacted down to its snapshot has no events left, and fails the same way.
    let only = MemRepository::new();
    let store = Store::<Ledger, _>::new(&only, ()).with_snapshot_interval(2);
    add(&store, "l-3", &[1, 2]).await;
    only.compact_below(&"l-3".into(), EventSequence(2))
        .await
        .unwrap();
    let error = ProjectionRebuild::new(&only)
        .with_projector(Feed::arc("rebuilt"))
        .aggregate::<Ledger>()
        .operations()
        .await
        .unwrap_err();
    assert!(
        matches!(
            &error,
            Error::CompactionUncovered { entity_id, through, .. }
                if entity_id.as_str() == "l-3" && *through == EventSequence(2)
        ),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_history_dependent_replay_refuses_a_stream_whose_tail_expired() {
    let probe = Probe::new(MemRepository::new());
    let store = Store::<Ledger, _>::new(&probe.inner, ()).with_snapshot_interval(3);
    add(&store, "l-1", &[1, 2, 3]).await;
    add(&store, "l-2", &[5]).await;
    // TTL deleted events 2 and 3 of l-1 but not yet event 1 or the snapshot at 3, so its head is
    // still 3 while its remaining history starts at 1.
    probe.expired.lock().unwrap().extend([
        (EntityId::new("l-1"), EventSequence(2)),
        (EntityId::new("l-1"), EventSequence(3)),
    ]);

    let error = ProjectionRebuild::new(&probe)
        .with_projector(Feed::arc("rebuilt"))
        .aggregate::<Ledger>()
        .run("rebuild-1", LEASE)
        .await
        .unwrap_err();

    assert!(
        matches!(
            &error,
            Error::CompactionUncovered { entity_id, through, covered }
                if entity_id.as_str() == "l-1"
                    && *through == EventSequence(3)
                    && *covered == Some(EventSequence(3))
        ),
        "{error:?}"
    );
    assert!(
        probe.writes.lock().unwrap().is_empty(),
        "nothing is written from the state at sequence 1"
    );
    assert!(
        probe.inner.rebuild_generation().await.unwrap().is_settled(),
        "a run that wrote nothing releases the lock it acquired"
    );
}

/// A [`MemRepository`] that records, at every guarded write, its batch size, whether the lock was
/// held, and whether a commit was refused, and can fail the `n`th write (1-based, 0 for none)
/// after applying it, as an ambiguous write does. Events listed in `expired` are left out of the
/// log enumeration, as DynamoDB's TTL deletes them: lazily and in no order, so the stream's head
/// (its snapshot) can outlive them.
struct Probe {
    inner: MemRepository,
    writes: Mutex<Vec<(usize, bool, bool)>>,
    fail_write: std::sync::atomic::AtomicUsize,
    expired: Mutex<Vec<(EntityId, EventSequence)>>,
}

impl Probe {
    fn new(inner: MemRepository) -> Self {
        Self {
            inner,
            writes: Mutex::new(Vec::new()),
            fail_write: Default::default(),
            expired: Mutex::new(Vec::new()),
        }
    }
}

impl Repository for Probe {
    type Operation = <MemRepository as Repository>::Operation;

    fn writes_views(&self) -> bool {
        self.inner.writes_views()
    }

    async fn commit(&self, commit: Commit<Self::Operation>) -> Result<(), Error> {
        self.inner.commit(commit).await
    }

    async fn outcome(&self, command_id: &crate::CommandId) -> Result<Option<Outcome>, Error> {
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

impl EventLog for Probe {
    const BATCHES_BY_ENTITY: bool = <MemRepository as EventLog>::BATCHES_BY_ENTITY;

    fn stream_all_events(
        &self,
        filter: StreamFilter,
    ) -> impl Stream<Item = crate::EventBatch> + Send {
        let expired = self.expired.lock().unwrap().clone();
        let inner: BoxStream<'_, crate::EventBatch> =
            Box::pin(self.inner.stream_all_events(filter));

        unfold((inner, expired), |(mut inner, expired)| async move {
            let batch = poll_fn(|cx| inner.as_mut().poll_next(cx))
                .await?
                .map(|events| {
                    events
                        .into_iter()
                        .filter(|event| {
                            !expired.contains(&(event.entity_id.clone(), event.sequence))
                        })
                        .collect()
                });

            Some((batch, (inner, expired)))
        })
    }
}

impl RebuildFence for Probe {
    async fn rebuild_generation(&self) -> Result<RebuildGeneration, Error> {
        self.inner.rebuild_generation().await
    }

    async fn rebuild_state(&self) -> Result<RebuildState, Error> {
        self.inner.rebuild_state().await
    }

    async fn begin_rebuild(&self, owner: &str, lease: Duration) -> Result<RebuildLease, Error> {
        self.inner.begin_rebuild(owner, lease).await
    }

    async fn renew_rebuild(
        &self,
        lease: &RebuildLease,
        extend: Duration,
    ) -> Result<RebuildLease, Error> {
        self.inner.renew_rebuild(lease, extend).await
    }

    async fn end_rebuild(&self, lease: &RebuildLease) -> Result<RebuildGeneration, Error> {
        self.inner.end_rebuild(lease).await
    }

    async fn force_release_rebuild(&self, owner: &str) -> Result<RebuildGeneration, Error> {
        self.inner.force_release_rebuild(owner).await
    }

    async fn write_rebuild_views(
        &self,
        lease: &RebuildLease,
        views: &[ProjectedView],
    ) -> Result<(), Error> {
        let locked = self.inner.rebuild_state().await?.in_progress();
        let refused = matches!(
            Store::<Ledger, _>::new(&self.inner, ())
                .execute("bystander", Add(1), Metadata::default())
                .await,
            Err(crate::ExecuteError::Store(Error::RebuildInProgress))
        );
        let count = {
            let mut writes = self.writes.lock().unwrap();
            writes.push((views.len(), locked, refused));
            writes.len()
        };

        self.inner.write_rebuild_views(lease, views).await?;
        if self.fail_write.load(std::sync::atomic::Ordering::SeqCst) == count {
            return Err(Error::Indeterminate(
                "injected: the write's result is unknown".into(),
            ));
        }

        Ok(())
    }
}

#[tokio::test]
async fn the_rebuild_holds_the_fence_and_writes_guarded_batches_of_at_most_99() {
    let probe = Probe::new(MemRepository::new());
    {
        let store = Store::<Ledger, _>::new(&probe.inner, ()).with_snapshot_interval(0);
        for index in 0..150 {
            add(&store, &format!("l-{index:03}"), &[1]).await;
        }
    }

    let report = ProjectionRebuild::new(&probe)
        .with_projector(Totals::arc("totals"))
        .aggregate::<Ledger>()
        .run("rebuild-1", LEASE)
        .await
        .unwrap();

    assert_eq!(
        (report.entities, report.puts, report.batches),
        (150, 150, 2)
    );
    assert_eq!(
        *probe.writes.lock().unwrap(),
        vec![(99, true, true), (51, true, true)],
        "each batch is written under the lock, while commits are refused"
    );
    assert_eq!(
        probe.inner.list_views_by_pk("totals").await.unwrap().len(),
        150
    );

    // The lock is released: commits land again.
    Store::<Ledger, _>::new(&probe.inner, ())
        .execute("after", Add(1), Metadata::default())
        .await
        .unwrap();
}

async fn ledgers<R: Repository>(repository: &R, count: usize) {
    let store = Store::<Ledger, _>::new(repository, ()).with_snapshot_interval(0);
    for index in 0..count {
        add(&store, &format!("l-{index:03}"), &[1]).await;
    }
}

#[tokio::test]
async fn a_failed_or_ambiguous_write_keeps_the_lock_until_a_rerun_takes_it_over() {
    for fail_write in [1, 2] {
        let clock = TestClock::at(1_000_000);
        let probe = Probe::new(MemRepository::new().with_clock(clock.reader()));
        probe
            .fail_write
            .store(fail_write, std::sync::atomic::Ordering::SeqCst);
        ledgers(&probe.inner, 120).await;

        let rebuild = ProjectionRebuild::new(&probe)
            .with_projector(Totals::arc("totals"))
            .aggregate::<Ledger>();
        let error = rebuild.run("rebuild-1", LEASE).await.unwrap_err();
        assert!(matches!(error, Error::Indeterminate(_)), "{error:?}");
        assert!(
            probe.inner.rebuild_state().await.unwrap().in_progress(),
            "write {fail_write} may have applied, so the lock is kept"
        );
        let refused = Store::<Ledger, _>::new(&probe.inner, ())
            .execute("l-000", Add(1), Metadata::default())
            .await;
        assert!(
            matches!(
                refused,
                Err(crate::ExecuteError::Store(Error::RebuildInProgress))
            ),
            "commits stay refused"
        );

        // A rerun cannot begin while the lease holds; once it lapses, it takes over and finishes.
        probe
            .fail_write
            .store(0, std::sync::atomic::Ordering::SeqCst);
        let rebuild = ProjectionRebuild::new(&probe)
            .with_projector(Totals::arc("totals"))
            .aggregate::<Ledger>();
        assert!(matches!(
            rebuild.run("rebuild-2", LEASE).await,
            Err(Error::RebuildLockHeld { .. })
        ));
        clock.advance(LEASE.as_secs() + 1);
        let report = rebuild.run("rebuild-2", LEASE).await.unwrap();
        assert_eq!(report.puts, 120);
        assert!(probe.inner.rebuild_generation().await.unwrap().is_settled());
    }
}

#[tokio::test]
async fn a_projector_error_is_found_before_any_write_and_releases_the_lock() {
    let repository = MemRepository::new();
    ledgers(&repository, 120).await;

    let failing: Arc<dyn CommitProjector> = Arc::new(Totals {
        pk: "totals",
        fail_on: Some("l-110"),
    });
    let error = ProjectionRebuild::new(&repository)
        .with_projector(failing)
        .aggregate::<Ledger>()
        .run("rebuild-1", LEASE)
        .await
        .unwrap_err();

    assert!(matches!(error, Error::Projector(_)), "{error:?}");
    assert!(
        rows(&repository, "totals").await.is_empty(),
        "every row is computed before the first is written"
    );
    assert!(repository.rebuild_generation().await.unwrap().is_settled());
}

#[tokio::test]
async fn a_run_that_took_over_a_half_written_rebuild_keeps_the_fence_when_it_fails_before_writing()
{
    let clock = TestClock::at(1_000_000);
    let repository = MemRepository::new().with_clock(clock.reader());
    ledgers(&repository, 3).await;
    let failing = || {
        ProjectionRebuild::new(&repository)
            .with_projector(Arc::new(Totals {
                pk: "totals",
                fail_on: Some("l-001"),
            }))
            .aggregate::<Ledger>()
    };
    let fenced = |what: &'static str| {
        let repository = &repository;
        async move {
            let state = repository.rebuild_state().await.unwrap();
            assert!(
                state.in_progress() && !state.generation.is_settled(),
                "{what}: the fence is kept, {state:?}"
            );
            let refused = Store::<Ledger, _>::new(repository, ())
                .execute("l-000", Add(1), Metadata::default())
                .await;
            assert!(
                matches!(
                    refused,
                    Err(crate::ExecuteError::Store(Error::RebuildInProgress))
                ),
                "{what}: commits stay refused, {refused:?}"
            );
        }
    };

    // An earlier rebuild wrote part of its rows and died: they are half-written.
    let crashed = repository.begin_rebuild("crashed", LEASE).await.unwrap();
    repository
        .write_rebuild_views(
            &crashed,
            &[ProjectedView::new(
                "totals",
                0,
                0,
                ViewOperation::put("totals", "l-000", "l-000", "ledger", "{}"),
            )],
        )
        .await
        .unwrap();
    clock.advance(LEASE.as_secs() + 1);

    // A retry takes over the lapsed lease and fails before its own first write. Releasing would
    // settle the generation over the crashed run's rows.
    let error = failing().run("retry", LEASE).await.unwrap_err();
    assert!(matches!(error, Error::Projector(_)), "{error:?}");
    fenced("after a takeover").await;
    assert_eq!(
        repository
            .rebuild_state()
            .await
            .unwrap()
            .lock
            .unwrap()
            .owner,
        "retry"
    );

    // The same after an operator force-releases it: the run acquires from the force-released
    // generation and keeps it.
    repository.force_release_rebuild("retry").await.unwrap();
    let error = failing().run("after-force", LEASE).await.unwrap_err();
    assert!(matches!(error, Error::Projector(_)), "{error:?}");
    fenced("after a force-release").await;

    // Only a rebuild that completes settles the generation.
    clock.advance(LEASE.as_secs() + 1);
    ProjectionRebuild::new(&repository)
        .with_projector(Totals::arc("totals"))
        .aggregate::<Ledger>()
        .run("recovery", LEASE)
        .await
        .unwrap();
    let generation = repository.rebuild_generation().await.unwrap();
    assert!(
        generation.is_settled() && !generation.force_released,
        "{generation:?}"
    );
    assert_eq!(
        rows(&repository, "totals").await[0].2,
        br#"{"total":1,"entries":1}"#.to_vec(),
        "the crashed run's row is rebuilt"
    );
}

#[test]
fn only_a_lease_one_past_a_settled_generation_was_acquired_from_it() {
    let lease = |generation| RebuildLease {
        owner: "w".into(),
        generation,
        lease_expires_at: 0,
    };
    let generation = |value, force_released| RebuildGeneration {
        value,
        force_released,
    };

    assert!(acquired_from_settled(generation(4, false), &lease(5)));
    assert!(acquired_from_settled(generation(0, false), &lease(1)));
    // A takeover of 3, or an acquire from a force-released 3, produces 5.
    assert!(!acquired_from_settled(generation(3, false), &lease(5)));
    assert!(!acquired_from_settled(generation(3, true), &lease(5)));
    // Another rebuild began and ended between the read and the begin, or began and was taken
    // over: not provably from the generation read, so the fence is kept.
    assert!(!acquired_from_settled(generation(4, false), &lease(7)));
}

/// A current-state projector that writes one row of `size` payload bytes per stream, and, when
/// `shared`, also a row every stream writes at one key.
struct Sized {
    size: usize,
    shared: bool,
}

impl CommitProjector for Sized {
    fn name(&self) -> &str {
        "sized"
    }

    fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
        let mut operations = Vec::new();
        for stream in &commit.streams {
            let id = stream.entity_id.clone();
            operations.push(ViewOperation::put(
                "sized",
                id.as_str(),
                id.clone(),
                "ledger",
                "x".repeat(self.size),
            ));
            if self.shared {
                operations.push(ViewOperation::put("sized", "everyone", id, "ledger", "{}"));
            }
        }

        Ok(operations)
    }
}

#[tokio::test]
async fn batches_also_fit_the_byte_budget() {
    let probe = Probe::new(MemRepository::new());
    ledgers(&probe.inner, 100).await;

    let report = ProjectionRebuild::new(&probe)
        .with_projector(Arc::new(Sized {
            size: 100_000,
            shared: false,
        }))
        .aggregate::<Ledger>()
        .run("rebuild-1", LEASE)
        .await
        .unwrap();

    let sizes: Vec<usize> = probe
        .writes
        .lock()
        .unwrap()
        .iter()
        .map(|(size, ..)| *size)
        .collect();
    assert_eq!(sizes, vec![37, 37, 26], "at most 3.8 MB with the guard");
    assert_eq!((report.puts, report.batches), (100, 3));
    assert_eq!(
        probe.inner.list_views_by_pk("sized").await.unwrap().len(),
        100
    );

    // A row over the per-item limit could never be written: it fails before any write.
    let error = ProjectionRebuild::new(&probe)
        .with_projector(Arc::new(Sized {
            size: 400_000,
            shared: false,
        }))
        .aggregate::<Ledger>()
        .run("rebuild-2", LEASE)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::CommitTooLarge { .. }), "{error:?}");
    assert_eq!(probe.writes.lock().unwrap().len(), 3, "no further write");
    assert!(probe.inner.rebuild_generation().await.unwrap().is_settled());
}

#[tokio::test]
async fn a_row_two_streams_would_write_is_refused_before_any_write() {
    let repository = MemRepository::new();
    ledgers(&repository, 3).await;

    let rebuild = ProjectionRebuild::new(&repository)
        .with_projector(Arc::new(Sized {
            size: 2,
            shared: true,
        }))
        .aggregate::<Ledger>();
    let error = rebuild.run("rebuild-1", LEASE).await.unwrap_err();

    assert!(
        matches!(&error, Error::SharedViewKey { sk, .. } if sk == "everyone"),
        "{error:?}"
    );
    assert!(rows(&repository, "sized").await.is_empty());
    assert!(repository.rebuild_generation().await.unwrap().is_settled());
    assert!(
        matches!(rebuild.operations().await, Err(Error::SharedViewKey { .. })),
        "the dry run refuses it too"
    );
}

#[tokio::test]
async fn a_dry_run_returns_the_operations_without_locking_or_writing() {
    let repository = MemRepository::new();
    let store = Store::<Ledger, _>::new(&repository, ()).with_snapshot_interval(0);
    add(&store, "l-1", &[2]).await;

    let operations = ProjectionRebuild::new(&repository)
        .with_projector(Totals::arc("totals"))
        .aggregate::<Ledger>()
        .operations()
        .await
        .unwrap();

    assert_eq!(
        operations,
        vec![ProjectedView::new(
            "totals",
            0,
            0,
            ViewOperation::put(
                "totals",
                "l-1",
                "l-1",
                "ledger",
                r#"{"total":2,"entries":1}"#
            )
        )],
        "the operation without its condition"
    );
    assert_eq!(repository.rebuild_generation().await.unwrap().value, 0);
    assert!(rows(&repository, "totals").await.is_empty());
}

/// A projector that reads the engine's metadata, as the journal's does.
struct Engine;

impl CommitProjector for Engine {
    fn name(&self) -> &str {
        "engine"
    }

    fn engine_metadata(&self, _: crate::projection::EngineOnly) -> bool {
        true
    }

    fn project(&self, _: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn a_configuration_the_rebuild_cannot_run_is_refused_before_the_lock() {
    let repository = MemRepository::new();
    let cases: Vec<(&str, ProjectionRebuild<'_, MemRepository>)> = vec![
        (
            "no projector",
            ProjectionRebuild::new(&repository).aggregate::<Ledger>(),
        ),
        (
            "no aggregate",
            ProjectionRebuild::new(&repository).with_projector(Totals::arc("totals")),
        ),
        (
            "a type registered twice",
            ProjectionRebuild::new(&repository)
                .with_projector(Totals::arc("totals"))
                .aggregate::<Ledger>()
                .aggregate_as::<Ledger>("ledger"),
        ),
        (
            "a history type not registered",
            ProjectionRebuild::new(&repository)
                .with_projector(Feed::arc("rebuilt"))
                .aggregate_as::<Ledger>("other"),
        ),
        (
            "an engine-metadata projector",
            ProjectionRebuild::new(&repository)
                .with_projector(Arc::new(Engine))
                .aggregate::<Ledger>(),
        ),
    ];

    for (name, rebuild) in cases {
        let error = rebuild.run("rebuild-1", LEASE).await.unwrap_err();
        assert!(
            matches!(error, Error::InvalidConfig(_)),
            "{name}: {error:?}"
        );
        assert_eq!(
            repository.rebuild_generation().await.unwrap().value,
            0,
            "{name}: no lock was taken"
        );
    }
}

#[tokio::test]
async fn aggregate_as_folds_streams_stored_under_another_type() {
    let repository = MemRepository::new();
    let store = Store::<Ledger, _>::new(&repository, ()).with_snapshot_interval(0);
    add(&store, "l-1", &[4]).await;
    let mut renamed = repository.events(&"l-1".into()).await.unwrap();
    for event in &mut renamed {
        event.entity_id = "old-1".into();
        event.id = crate::EventId::for_event(&event.entity_id, event.sequence);
        event.entity_type = "legacy-ledger".into();
    }
    repository
        .commit(Commit::new("old-1", renamed))
        .await
        .unwrap();

    let operations = ProjectionRebuild::new(&repository)
        .with_projector(Totals::arc("totals"))
        .aggregate_as::<Ledger>("legacy-ledger")
        .operations()
        .await
        .unwrap();

    assert_eq!(
        operations
            .iter()
            .map(|item| (item.op.sk.as_str(), item.op.entity_type.as_str()))
            .collect::<Vec<_>>(),
        vec![("old-1", "legacy-ledger")],
        "only the registered type, under its stored type"
    );
}

#[test]
fn a_rebuild_future_is_send() {
    fn send<T: Send>(_: &T) {}

    let repository = MemRepository::new();
    let rebuild = ProjectionRebuild::new(&repository)
        .with_projector(Totals::arc("totals"))
        .aggregate::<Ledger>();
    send(&rebuild.run("rebuild-1", LEASE));
    send(&rebuild.operations());
}

/// A current-state projector that takes longer than half a second over its first stream.
struct Slow(std::sync::atomic::AtomicBool);

impl CommitProjector for Slow {
    fn name(&self) -> &str {
        "slow"
    }

    fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
        if !self.0.swap(true, std::sync::atomic::Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(600));
        }

        Totals::arc("totals").project(commit)
    }
}

#[tokio::test]
async fn a_run_under_a_callers_lease_renews_it_in_place() {
    let clock = TestClock::at(1_000_000);
    let repository = MemRepository::new().with_clock(clock.reader());
    let store = Store::<Ledger, _>::new(&repository, ()).with_snapshot_interval(0);
    add(&store, "l-1", &[1]).await;
    add(&store, "l-2", &[2]).await;

    let mut lease = repository.begin_rebuild("caller", LEASE).await.unwrap();
    let taken = lease.clone();
    let rebuild = ProjectionRebuild::new(&repository)
        .with_projector(Arc::new(Slow(Default::default())))
        .aggregate::<Ledger>();

    assert!(
        matches!(
            rebuild
                .run_with_lease(&mut lease, Duration::from_millis(999))
                .await,
            Err(RebuildFailure { ref error, attempted: 0, .. })
                if matches!(**error, Error::InvalidArgument(_))
        ),
        "a renewal under a second is refused before anything is read"
    );

    // The first stream takes over half of a one-second renewal, so the run renews after it.
    let report = rebuild
        .run_with_lease(&mut lease, Duration::from_secs(1))
        .await
        .unwrap();

    assert_eq!((report.entities, report.batches), (2, 1));
    assert_eq!(
        lease.generation, taken.generation,
        "the same lease, renewed"
    );
    assert_eq!(lease.lease_expires_at, clock.seconds() + 1);
    assert!(
        repository.rebuild_state().await.unwrap().in_progress(),
        "the caller's lock is still held"
    );
    repository.end_rebuild(&lease).await.unwrap();
}

/// Ledgers whose live commits ran [`Totals`], so the table holds the rows a rebuild plans.
async fn live_totals(repository: &MemRepository, count: usize) {
    let store =
        Store::<Ledger, _>::new(repository, ()).with_commit_projector(Totals::arc("totals"));
    for index in 0..count {
        add(&store, &format!("l-{index:03}"), &[1]).await;
    }
}

fn diffing(probe: &Probe) -> ProjectionRebuild<'_, Probe> {
    ProjectionRebuild::new(probe)
        .with_projector(Totals::arc("totals"))
        .aggregate::<Ledger>()
        .diff(&probe.inner, ["ledger"])
}

#[tokio::test]
async fn a_diff_rebuild_of_intact_views_writes_nothing() {
    let probe = Probe::new(MemRepository::new());
    live_totals(&probe.inner, 120).await;
    let before = rows(&probe.inner, "totals").await;

    let report = diffing(&probe).run("rebuild-1", LEASE).await.unwrap();

    assert!(report.is_clean(), "{report:?}");
    assert_eq!(
        (report.existing, report.unchanged, report.batches),
        (120, 120, 0)
    );
    assert!(probe.writes.lock().unwrap().is_empty(), "no guarded write");
    assert_eq!(rows(&probe.inner, "totals").await, before);
    assert!(probe.inner.rebuild_generation().await.unwrap().is_settled());
}

#[tokio::test]
async fn a_diff_rebuild_repairs_owned_rows_deletes_stale_ones_and_leaves_the_rest() {
    let probe = Probe::new(MemRepository::new());
    live_totals(&probe.inner, 3).await;
    let before = rows(&probe.inner, "totals").await;

    // A changed row, a missing row, a stale owned row, and a row of a type nobody here owns.
    let views = &probe.inner;
    views
        .put_view(&ViewRow::new("totals", "l-000", "l-000", "ledger", "{}"))
        .await
        .unwrap();
    views.delete_view("totals", "l-001").await.unwrap();
    views
        .put_view(&ViewRow::new("totals", "gone", "gone", "ledger", "{}"))
        .await
        .unwrap();
    views
        .put_view(&ViewRow::new("totals", "foreign", "x", "other", "{}"))
        .await
        .unwrap();

    let plan = diffing(&probe).plan().await.unwrap();
    let kinds: Vec<_> = plan
        .operations
        .iter()
        .map(|item| (item.op.kind, item.op.sk.as_str()))
        .collect();
    assert_eq!(
        kinds,
        [
            (ViewOperationKind::Put, "l-000"),
            (ViewOperationKind::Put, "l-001"),
            (ViewOperationKind::Delete, "gone"),
        ],
        "the dry run shows the difference"
    );
    assert_eq!(
        (
            plan.report.puts,
            plan.report.deletes,
            plan.report.unchanged,
            plan.batches
        ),
        (2, 1, 1, 1)
    );
    assert!(
        probe.writes.lock().unwrap().is_empty(),
        "a dry run writes nothing"
    );

    let progress = Mutex::new(Vec::new());
    let report = diffing(&probe)
        .on_progress(|step| {
            progress.lock().unwrap().push(match step {
                RebuildProgress::Planned { report, batches } => (report.puts, batches),
                RebuildProgress::Wrote { batch, of } => (batch, of),
            });
        })
        .run("rebuild-1", LEASE)
        .await
        .unwrap();

    assert_eq!(
        (
            report.puts,
            report.deletes,
            report.unchanged,
            report.existing,
            report.batches
        ),
        (2, 1, 1, 3, 1)
    );
    assert_eq!(
        report.changes.get("ledger"),
        Some(&RowChanges {
            puts: 2,
            deletes: 1
        })
    );
    assert_eq!(
        *progress.lock().unwrap(),
        [(2, 1), (1, 1)],
        "planned two puts in one batch, then wrote it"
    );

    let mut after = rows(&probe.inner, "totals").await;
    let foreign = after.remove(after.iter().position(|row| row.0 == "foreign").unwrap());
    assert_eq!(
        after, before,
        "owned rows are back to what the commits wrote"
    );
    assert_eq!(foreign.1, "x", "a row of another type is untouched");
}

#[tokio::test]
async fn a_diff_refuses_a_planned_row_of_a_type_it_does_not_own() {
    let repository = MemRepository::new();
    live_totals(&repository, 2).await;
    let before = repository.rebuild_generation().await.unwrap();

    let error = ProjectionRebuild::new(&repository)
        .with_projector(Totals::arc("totals"))
        .aggregate::<Ledger>()
        .diff(&repository, ["other"])
        .run("rebuild-1", LEASE)
        .await
        .unwrap_err();

    assert!(matches!(error, Error::InvalidConfig(_)), "{error:?}");
    let after = repository.rebuild_generation().await.unwrap();
    assert_eq!(
        after.value,
        before.value + 2,
        "found before any write: released"
    );

    // Owning nothing is refused before the lock.
    let error = ProjectionRebuild::new(&repository)
        .with_projector(Totals::arc("totals"))
        .aggregate::<Ledger>()
        .diff(&repository, Vec::<String>::new())
        .run("rebuild-2", LEASE)
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidConfig(_)), "{error:?}");
    assert_eq!(repository.rebuild_generation().await.unwrap(), after);
}

/// A view repository whose type listing shows a fixed, out-of-date picture of the table, as an
/// eventually consistent index can.
struct Lagging {
    inner: MemRepository,
    listing: Vec<ViewRow>,
}

impl ViewRepository for Lagging {
    async fn get_view_at(&self, pk: &str, sk: &str) -> Result<Option<ViewRow>, Error> {
        self.inner.get_view_at(pk, sk).await
    }

    async fn put_view(&self, view: &ViewRow) -> Result<(), Error> {
        self.inner.put_view(view).await
    }

    async fn delete_view(&self, pk: &str, sk: &str) -> Result<(), Error> {
        self.inner.delete_view(pk, sk).await
    }

    async fn list_views_by_pk_paged(
        &self,
        pk: &str,
        limit: Option<usize>,
        cursor: Option<&str>,
    ) -> Result<crate::ViewPage, Error> {
        self.inner.list_views_by_pk_paged(pk, limit, cursor).await
    }

    async fn list_views_by_entity_type_paged(
        &self,
        _: &str,
        _: Option<usize>,
        _: Option<&str>,
    ) -> Result<crate::ViewPage, Error> {
        Ok(crate::ViewPage::new(self.listing.clone(), None))
    }
}

#[tokio::test]
async fn a_lagging_listing_is_confirmed_by_key_before_a_row_is_skipped_or_deleted() {
    let repository = MemRepository::new();
    live_totals(&repository, 2).await;
    let live = repository.list_views_by_pk("totals").await.unwrap();

    // The listing still shows l-000 as the commits wrote it, but the table has changed since; it
    // still shows a stale row that is already gone; and it does not show l-001 yet.
    repository
        .put_view(&ViewRow::new("totals", "l-000", "l-000", "ledger", "{}"))
        .await
        .unwrap();
    let lagging = Lagging {
        inner: repository.clone(),
        listing: vec![
            live[0].clone(),
            ViewRow::new("totals", "gone", "gone", "ledger", "{}"),
        ],
    };

    let report = ProjectionRebuild::new(&repository)
        .with_projector(Totals::arc("totals"))
        .aggregate::<Ledger>()
        .diff(&lagging, ["ledger"])
        .run("rebuild-1", LEASE)
        .await
        .unwrap();

    assert_eq!(
        (report.puts, report.deletes, report.unchanged),
        (2, 0, 0),
        "l-000 is rewritten, l-001 is written because the listing missed it, and nothing is deleted"
    );
    assert_eq!(repository.list_views_by_pk("totals").await.unwrap(), live);
}
