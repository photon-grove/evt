//! [`JournalRepository`]: a repository that journals the events it commits.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_core::Stream;

use super::config::{Journal, allocate};
use super::projector::{JournalProjector, PROJECTOR_NAME, PositionTaken};
use super::read::{self, JournalCompactionStore, JournalPage, JournalStore};
use super::row::{JournalRow, counter_payload};
use super::scope::{COUNTER_ENTITY_TYPE, COUNTER_OWNER, COUNTER_SORT_KEY, Scope, set_allocations};
use crate::enumerate::{EventBatch, EventLog, StreamFilter};
use crate::error::{Error, RowTooLarge};
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::features::{FeatureMarker, JOURNAL_FEATURE, JOURNAL_VERSION};
use crate::ids::{CommandId, EntityId, EntityType, EventSequence};
use crate::outcome::Outcome;
use crate::projection::{
    CommitProjector, CommitStream, CommitView, ProjectedView, ViewOperation, project_commit,
};
use crate::repository::{Commit, Repository, TransactionCommit};
use crate::retry::Budget;

/// A [`Repository`] that journals the events it commits (ADR 0005, X1).
///
/// It wraps a repository that writes view operations (the in-memory repository, or DynamoDB with a
/// views table) and, for every commit that writes an event of a journaled entity type:
///
/// 1. **allocates**: reads the counter of every scope the commit's events map to, and gives each
///    event the next position in each of its scopes, in the commit's canonical order (streams by
///    entity-ID bytes, events by sequence);
/// 2. **persists the allocations in the events' metadata** (`evt.journal`), so the journal rows
///    and counters can always be derived again from the stored events alone;
/// 3. **guards the row size** of every row before anything is written
///    ([`Error::JournalRowTooLarge`]);
/// 4. adds the **journal rows** (from the [`JournalProjector`], so the owner and key rules of
///    commit-wide projectors apply) and the **scope counters** to the commit's view operations, so
///    they commit atomically with the events;
/// 5. declares the `journal` feature and the journaled entity types history-dependent in the
///    feature marker before the first such commit, so a writer that cannot maintain the journal
///    refuses the table and compaction is refused for the events the journal is rebuilt from.
///
/// When another writer takes a position first, the commit fails on a row that must be absent. The
/// repository then re-reads the counters and allocates again, with backoff, up to the journal's
/// [`RetryPolicy`](crate::RetryPolicy); when attempts run out it returns [`Error::Conflict`] for
/// the caller's own retry. Nothing else about a commit's failures changes.
///
/// Everything else (reads, snapshots, compaction, outcomes) is the wrapped repository's. Events the
/// store hands back from [`Store::execute`](crate::Store::execute) are the ones the store
/// serialized; read the stream (or a journal row) to see the stored allocation.
///
/// Every writer that commits to a journal table must go through a `JournalRepository` with an
/// equivalent configuration: an event committed any other way is silently missing from the
/// journal. The feature marker stops builds without the `journal` cargo feature; it cannot tell a
/// journaling writer from one that bypasses the journal.
#[derive(Clone, Debug)]
pub struct JournalRepository<R> {
    inner: R,
    journal: Journal,
    declared: Arc<AtomicBool>,
}

impl<R> JournalRepository<R> {
    /// Journals the commits of `inner`.
    pub fn new(inner: R, journal: Journal) -> Self {
        Self {
            inner,
            journal,
            declared: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The wrapped repository.
    pub fn inner(&self) -> &R {
        &self.inner
    }

    /// The journal configuration.
    pub fn journal(&self) -> &Journal {
        &self.journal
    }

    /// The journal's commit-wide projector, for replays
    /// ([`group_commits`](crate::group_commits) and [`replay_views`](crate::replay_views)).
    pub fn projector(&self) -> Arc<dyn CommitProjector> {
        Arc::new(JournalProjector::new(self.journal.clone()))
    }
}

impl<R: JournalStore> JournalRepository<R> {
    /// The scope's high watermark. See [`high_watermark`](super::high_watermark).
    pub async fn high_watermark(&self, scope: &Scope) -> Result<u64, Error> {
        read::high_watermark(&self.inner, scope).await
    }
}

impl<R: JournalCompactionStore> JournalRepository<R> {
    /// A page of the scope's rows after `after`. See [`page`](super::page).
    pub async fn page(
        &self,
        scope: &Scope,
        after: u64,
        limit: usize,
    ) -> Result<JournalPage, Error> {
        read::page(&self.inner, scope, after, limit).await
    }
}

impl<R: JournalStore> JournalStore for JournalRepository<R> {
    fn read_counter(&self, scope: &Scope) -> impl Future<Output = Result<u64, Error>> + Send {
        self.inner.read_counter(scope)
    }

    fn read_counters(
        &self,
        scopes: &[Scope],
    ) -> impl Future<Output = Result<Vec<u64>, Error>> + Send {
        self.inner.read_counters(scopes)
    }

    fn read_last_position(&self, scope: &Scope) -> impl Future<Output = Result<u64, Error>> + Send {
        self.inner.read_last_position(scope)
    }

    fn read_rows(
        &self,
        scope: &Scope,
        after: u64,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<JournalRow>, Error>> + Send {
        self.inner.read_rows(scope, after, limit)
    }
}

impl<R: JournalCompactionStore> JournalCompactionStore for JournalRepository<R> {
    fn read_compaction_row(
        &self,
        scope: &Scope,
        sort_key: &str,
    ) -> impl Future<Output = Result<Option<crate::projection::ViewRow>, Error>> + Send {
        self.inner.read_compaction_row(scope, sort_key)
    }
}

/// A commit the journal can stage: the streams' event lists in canonical order, and a place to add
/// view operations.
trait Staged: Send {
    /// A copy to stage one attempt on. Written out field by field, so a field the commit types
    /// gain later fails to compile here instead of being dropped from a retried attempt.
    fn duplicate(&self) -> Self;

    /// The entity ID to name in an error.
    fn label(&self) -> EntityId;

    /// The event lists of the streams that write events, ascending by entity-ID bytes.
    fn event_streams(&mut self) -> Vec<&mut Vec<SerializedEvent>>;

    /// The commit-level view operations, which the journal appends to.
    fn views(&mut self) -> &mut Vec<ProjectedView>;

    /// The checks the wrapped repository makes before it writes: event shape, reserved entity IDs,
    /// duplicate streams, view-operation keys. Run on the staged attempt, before the marker is
    /// touched.
    fn validate(&self) -> Result<(), Error>;
}

fn duplicate_commit<Op: Clone>(commit: &Commit<Op>) -> Commit<Op> {
    Commit {
        entity_id: commit.entity_id.clone(),
        events: commit.events.clone(),
        snapshot: commit.snapshot.clone(),
        operations: commit.operations.clone(),
        views: commit.views.clone(),
        outcome: commit.outcome.clone(),
    }
}

impl<Op: Clone + Send> Staged for Commit<Op> {
    fn duplicate(&self) -> Self {
        duplicate_commit(self)
    }

    fn label(&self) -> EntityId {
        self.entity_id.clone()
    }

    fn event_streams(&mut self) -> Vec<&mut Vec<SerializedEvent>> {
        if self.events.is_empty() {
            Vec::new()
        } else {
            vec![&mut self.events]
        }
    }

    fn views(&mut self) -> &mut Vec<ProjectedView> {
        &mut self.views
    }

    fn validate(&self) -> Result<(), Error> {
        Commit::validate(self)?;

        crate::features::check_not_reserved(&self.entity_id)
    }
}

impl<Op: Clone + Send> Staged for TransactionCommit<Op> {
    fn duplicate(&self) -> Self {
        TransactionCommit {
            streams: self.streams.iter().map(duplicate_commit).collect(),
            fences: self.fences.clone(),
            operations: self.operations.clone(),
            views: self.views.clone(),
            outcome: self.outcome.clone(),
        }
    }

    fn label(&self) -> EntityId {
        self.streams
            .first()
            .map(|stream| stream.entity_id.clone())
            .unwrap_or_default()
    }

    fn event_streams(&mut self) -> Vec<&mut Vec<SerializedEvent>> {
        let mut streams: Vec<(&EntityId, &mut Vec<SerializedEvent>)> = self
            .streams
            .iter_mut()
            .filter(|stream| !stream.events.is_empty())
            .map(|stream| (&stream.entity_id, &mut stream.events))
            .collect();
        streams.sort_by(|a, b| a.0.cmp(b.0));

        streams.into_iter().map(|(_, events)| events).collect()
    }

    fn views(&mut self) -> &mut Vec<ProjectedView> {
        &mut self.views
    }

    fn validate(&self) -> Result<(), Error> {
        self.duplicate().normalize()
    }
}

impl<R> JournalRepository<R>
where
    R: Repository + FeatureMarker + JournalCompactionStore,
{
    /// Declares the `journal` feature and the journaled entity types history-dependent, once per
    /// repository (clones share the flag). Nothing is written if the marker already declares them.
    /// Returns whether this call wrote the `journal` declaration, so a commit that then fails
    /// because the table is not migrated can take it back.
    async fn ensure_declared(&self) -> Result<bool, Error> {
        if self.declared.load(Ordering::Acquire) {
            return Ok(false);
        }

        let manifest = self.inner.read_manifest().await?;
        let mut wrote = false;
        match manifest
            .as_ref()
            .and_then(|manifest| manifest.features.get(JOURNAL_FEATURE))
        {
            Some(&JOURNAL_VERSION) => {}
            Some(&version) => {
                return Err(Error::UnsupportedFeature {
                    feature: JOURNAL_FEATURE.to_owned(),
                    version,
                });
            }
            None => {
                self.inner
                    .update_manifest(|manifest| manifest.declare_journal())
                    .await?;
                wrote = true;
            }
        }

        self.inner
            .declare_history_dependent(self.journal.entity_types())
            .await?;
        self.declared.store(true, Ordering::Release);

        Ok(wrote)
    }

    /// Takes back a `journal` declaration this repository just wrote, for a commit that then
    /// failed because the table has not been migrated to stream heads: the marker must not be left
    /// declaring an extension on a table nothing journaled into. Best effort; the
    /// history-dependent declaration stays, which only forbids compacting the types.
    async fn retract(&self) {
        self.declared.store(false, Ordering::Release);

        let _ = self
            .inner
            .update_manifest(|manifest| {
                manifest.features.remove(JOURNAL_FEATURE);
            })
            .await;
    }

    /// The scopes each event of the commit maps to, in canonical event order.
    fn plan<C: Staged>(&self, commit: &mut C) -> Result<Vec<Vec<Scope>>, Error> {
        let mut plan = Vec::new();

        for events in commit.event_streams() {
            for event in events.iter() {
                plan.push(self.journal.scopes_of(event)?);
            }
        }

        Ok(plan)
    }

    /// The stored counter of every scope (0 for none), in one batch read, and the position each
    /// scope allocates from. With `repair`, a counter behind the scope's highest row (a lost or
    /// lowered counter) allocates from that row's position instead, and the commit writes the
    /// counter back.
    ///
    /// A compacted scope's rows below its floor are gone, so neither a missing counter nor the
    /// highest row can show positions a compaction deleted. A counter that reads 0 (a new scope,
    /// or a lost counter), and every counter being repaired, allocates from the scope's compaction
    /// floor at least. The commit's counter put is conditioned on the stored counter it read, so
    /// a writer whose read went stale (another commit, then a compaction, deleted the rows that
    /// would have refused it) fails instead of reallocating a position.
    async fn read_counters(&self, scopes: &[Scope], repair: bool) -> Result<Counters, Error> {
        let values = self.inner.read_counters(scopes).await?;
        let mut counters = Counters::default();

        for (scope, stored) in scopes.iter().zip(values) {
            let mut base = stored;

            if repair {
                base = base.max(self.inner.read_last_position(scope).await?);
            }

            if repair || base == 0 {
                base = base.max(super::compact::read_floor(&self.inner, scope).await?);
            }

            counters.stored.insert(scope.clone(), stored);
            counters.base.insert(scope.clone(), base);
        }

        Ok(counters)
    }

    /// Stages one attempt on a fresh copy of the commit: allocates positions from `counters`,
    /// writes the allocations into the events' metadata, guards every row, and appends the rows
    /// and counters to the commit's view operations.
    fn stage<C: Staged>(
        &self,
        commit: &mut C,
        plan: &[Vec<Scope>],
        counters: &Counters,
    ) -> Result<(), Error> {
        let allocated = allocate(&counters.base, plan)?;
        let projector_index = commit
            .views()
            .iter()
            .map(|view| view.projector_index + 1)
            .max()
            .unwrap_or(0);

        let mut view_streams = Vec::new();
        let mut at = 0;
        let mut commit_id = String::new();
        let mut widest: Option<RowTooLarge> = None;

        for events in commit.event_streams() {
            for event in events.iter_mut() {
                // The key is the engine's: whatever a caller put there is replaced.
                set_allocations(event, &allocated.allocations[at]);

                for allocation in &allocated.allocations[at] {
                    match self
                        .journal
                        .check_row(&allocation.scope()?, allocation.position, event)
                    {
                        Err(Error::JournalRowTooLarge(row))
                            if widest.as_ref().is_none_or(|widest| row.size > widest.size) =>
                        {
                            widest = Some(row);
                        }
                        Err(Error::JournalRowTooLarge(_)) | Ok(()) => {}
                        Err(other) => return Err(other),
                    }
                }

                at += 1;
            }

            if commit_id.is_empty() {
                commit_id = events[0].metadata.commit_id.clone();
            }

            view_streams.push(CommitStream::new(
                events[0].entity_id.clone(),
                events[0].entity_type.clone(),
                events.clone(),
                Vec::new(),
            ));
        }

        // Every row is measured before any is refused, so the error names the widest one.
        if let Some(row) = widest {
            return Err(row.into());
        }

        let projector: Arc<dyn CommitProjector> =
            Arc::new(JournalProjector::new(self.journal.clone()));
        let mut views = project_commit(&[projector], &CommitView::new(commit_id, view_streams))?;
        let rows = views.len();

        for (operation, (scope, position)) in (rows..).zip(&allocated.counters) {
            // A compare-and-set on the counter this attempt read: absent, or exactly its stored
            // payload. A writer that lost the race fails here even when the rows it allocated
            // are gone (compacted), and retries like one whose position was taken.
            let put = ViewOperation::put(
                scope.counter_pk(),
                COUNTER_SORT_KEY,
                COUNTER_OWNER,
                COUNTER_ENTITY_TYPE,
                counter_payload(scope, *position),
            );
            let put = match counters.stored.get(scope).copied().unwrap_or(0) {
                0 => put.if_absent(),
                stored => put.if_payload(counter_payload(scope, stored)),
            };

            views.push(ProjectedView::new(
                PROJECTOR_NAME,
                0,
                operation,
                put.on_conflict(PositionTaken {
                    scope: scope.as_str().to_owned(),
                    position: *position,
                }),
            ));
        }

        for view in &mut views {
            view.projector_index = projector_index;
        }
        commit.views().extend(views);

        Ok(())
    }

    /// Commits `base` with its journal rows, retrying while other writers keep taking the
    /// positions it allocated (and on a transient failure, which includes two transactions meeting
    /// on a shared counter item).
    ///
    /// The order matters: counters are read and the commit staged (allocation, size guard, rows)
    /// before the marker is touched, so a commit that cannot be built declares nothing.
    async fn drive<C, F, Fut>(&self, mut base: C, send: F) -> Result<(), Error>
    where
        C: Staged,
        F: Fn(C) -> Fut,
        Fut: Future<Output = Result<(), Error>>,
    {
        let covered = base
            .event_streams()
            .into_iter()
            .flatten()
            .any(|event| self.journal.covers(&event.entity_type));

        // The rows and counters are view operations: a repository that does not write them would
        // commit the events and their allocations and drop the rows. Refused before anything is
        // planned or declared.
        if covered && !self.inner.writes_views() {
            return Err(Error::ViewsUnsupported {
                repository: std::any::type_name::<R>().to_owned(),
            });
        }

        // The counter put is a compare-and-set: a repository that would write it unconditionally
        // is refused, fail-closed, before anything is planned or declared.
        if covered && !self.inner.applies_expected_payload() {
            return Err(Error::ExpectedPayloadUnsupported {
                repository: std::any::type_name::<R>().to_owned(),
            });
        }

        let plan = self.plan(&mut base)?;

        if !covered {
            // Nothing here is journaled, but the key is the engine's: a forged allocation on an
            // uncovered event must not reach the log, where a rebuild would read it.
            for events in base.event_streams() {
                for event in events.iter_mut() {
                    set_allocations(event, &[]);
                }
            }

            return send(base).await;
        }

        let scopes: Vec<Scope> = plan
            .iter()
            .flatten()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut budget = Budget::new(*self.journal.retry());
        let mut repair = false;

        loop {
            let counters = self.read_counters(&scopes, repair).await?;
            let mut attempt = base.duplicate();
            self.stage(&mut attempt, &plan, &counters)?;

            // What the repository will refuse is refused here, before the marker is written.
            attempt.validate()?;

            let declared_now = self.ensure_declared().await?;
            let error = match send(attempt).await {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };

            if declared_now && matches!(error, Error::StreamHeadsNotMigrated) {
                self.retract().await;

                return Err(error);
            }

            let lost = if taken(&error) {
                // Another writer holds a position this attempt allocated. If the counters have
                // not moved since they were read, the counter is behind its rows (lost, lowered
                // by a rebuild, or wiped): the next attempt allocates from the highest row.
                repair |= self.read_counters(&scopes, false).await?.stored == counters.stored;

                true
            } else if matches!(error, Error::Transient(_)) {
                false
            } else {
                // Everything else is the caller's: a conflict on the stream (which outranks the
                // journal's own failures, so it is never the journal's doing), a failed outcome,
                // an unknown result.
                return Err(error);
            };

            let Some(delay) = budget.next() else {
                if !lost {
                    return Err(error);
                }

                return Err(Error::Conflict {
                    entity_id: base.label(),
                    message: format!(
                        "journal positions kept being taken by other writers; {} attempts exhausted",
                        budget.attempts()
                    ),
                });
            };

            self.inner.backoff(delay).await;
        }
    }
}

/// The counters one attempt read: what each scope's counter row stores (0 for none), which the
/// commit's counter put is conditioned on, and the position each scope allocates from.
#[derive(Default)]
struct Counters {
    stored: BTreeMap<Scope, u64>,
    base: BTreeMap<Scope, u64>,
}

/// Reports whether the error is a journal row that found its position taken, or a counter that
/// moved since the attempt read it.
fn taken(error: &Error) -> bool {
    let Error::ProjectorOperation(operation) = error else {
        return false;
    };

    operation.projector == PROJECTOR_NAME
        && operation
            .mapped()
            .is_some_and(|mapped| mapped.downcast_ref::<PositionTaken>().is_some())
}

impl<R> Repository for JournalRepository<R>
where
    R: Repository + FeatureMarker + JournalCompactionStore,
    R::Operation: Clone,
{
    type Operation = R::Operation;

    async fn commit(&self, commit: Commit<Self::Operation>) -> Result<(), Error> {
        self.drive(commit, |commit| self.inner.commit(commit)).await
    }

    async fn commit_transaction(
        &self,
        commit: TransactionCommit<Self::Operation>,
    ) -> Result<(), Error> {
        self.drive(commit, |commit| self.inner.commit_transaction(commit))
            .await
    }

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

    fn backoff(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        self.inner.backoff(duration)
    }

    fn deadline(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        self.inner.deadline(duration)
    }

    fn outcome(
        &self,
        command_id: &CommandId,
    ) -> impl Future<Output = Result<Option<Outcome>, Error>> + Send {
        self.inner.outcome(command_id)
    }

    fn events(
        &self,
        entity_id: &EntityId,
    ) -> impl Future<Output = Result<Vec<SerializedEvent>, Error>> + Send {
        self.inner.events(entity_id)
    }

    fn events_after(
        &self,
        entity_id: &EntityId,
        after: EventSequence,
    ) -> impl Future<Output = Result<Vec<SerializedEvent>, Error>> + Send {
        self.inner.events_after(entity_id, after)
    }

    fn latest(
        &self,
        entity_id: &EntityId,
        n: usize,
    ) -> impl Future<Output = Result<Vec<SerializedEvent>, Error>> + Send {
        self.inner.latest(entity_id, n)
    }

    fn snapshot(
        &self,
        entity_id: &EntityId,
    ) -> impl Future<Output = Result<Option<SerializedSnapshot>, Error>> + Send {
        self.inner.snapshot(entity_id)
    }

    fn put_snapshot(
        &self,
        snapshot: &SerializedSnapshot,
    ) -> impl Future<Output = Result<bool, Error>> + Send {
        self.inner.put_snapshot(snapshot)
    }

    fn compact_below(
        &self,
        entity_id: &EntityId,
        through: EventSequence,
    ) -> impl Future<Output = Result<usize, Error>> + Send {
        self.inner.compact_below(entity_id, through)
    }

    fn entity_heads(
        &self,
        entity_type: Option<&EntityType>,
    ) -> impl Future<Output = Result<std::collections::BTreeMap<EntityId, EventSequence>, Error>> + Send
    {
        self.inner.entity_heads(entity_type)
    }
}

impl<R: EventLog> EventLog for JournalRepository<R> {
    const BATCHES_BY_ENTITY: bool = R::BATCHES_BY_ENTITY;

    fn stream_all_events(&self, filter: StreamFilter) -> impl Stream<Item = EventBatch> + Send {
        self.inner.stream_all_events(filter)
    }
}

impl<R: crate::rebuild::RebuildFence> crate::rebuild::RebuildFence for JournalRepository<R> {
    fn rebuild_generation(
        &self,
    ) -> impl Future<Output = Result<crate::rebuild::RebuildGeneration, Error>> + Send {
        self.inner.rebuild_generation()
    }

    fn rebuild_state(
        &self,
    ) -> impl Future<Output = Result<crate::rebuild::RebuildState, Error>> + Send {
        self.inner.rebuild_state()
    }

    fn begin_rebuild(
        &self,
        owner: &str,
        lease: Duration,
    ) -> impl Future<Output = Result<crate::rebuild::RebuildLease, Error>> + Send {
        self.inner.begin_rebuild(owner, lease)
    }

    fn renew_rebuild(
        &self,
        lease: &crate::rebuild::RebuildLease,
        extend: Duration,
    ) -> impl Future<Output = Result<crate::rebuild::RebuildLease, Error>> + Send {
        self.inner.renew_rebuild(lease, extend)
    }

    fn end_rebuild(
        &self,
        lease: &crate::rebuild::RebuildLease,
    ) -> impl Future<Output = Result<crate::rebuild::RebuildGeneration, Error>> + Send {
        self.inner.end_rebuild(lease)
    }

    fn force_release_rebuild(
        &self,
        owner: &str,
    ) -> impl Future<Output = Result<crate::rebuild::RebuildGeneration, Error>> + Send {
        self.inner.force_release_rebuild(owner)
    }

    fn write_rebuild_views(
        &self,
        lease: &crate::rebuild::RebuildLease,
        views: &[ProjectedView],
    ) -> impl Future<Output = Result<(), Error>> + Send {
        self.inner.write_rebuild_views(lease, views)
    }
}
