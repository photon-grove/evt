//! The full projection rebuild (P4): commit-wide projectors replayed over the event log, their
//! rows written in guarded batches under the rebuild fence, and compacted streams seeded from
//! their snapshots (P11).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::future::poll_fn;
use std::marker::PhantomData;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_core::Stream;

use super::{
    Held, RebuildFailure, RebuildFence, RebuildLease, Renewing, check_renew, lock_check_size,
    rebuild_batches,
};
use crate::aggregate::Aggregate;
use crate::enumerate::{EventLog, StreamFilter, unfold};
use crate::error::Error;
use crate::event::SerializedEvent;
use crate::ids::{EntityId, EntityType, EventSequence};
use crate::json;
use crate::projection::{
    CommitProjector, CommitStream, CommitView, ProjectedView, ViewOperation, ViewOperationKind,
    ViewReplay, group_commits, project_with, view_operation_size,
};
use crate::repository::{MAX_COMMIT_BYTES, MAX_COMMIT_ITEM_BYTES, Repository};
use crate::store::Loaded;
use crate::views::ViewRepository;

mod diff;

/// A full rebuild of commit-wide projectors (v0.9, C7) from the event log, under the rebuild fence
/// (C10): the Rust counterpart of Go's `RebuildProjections`, for commit-wide projectors.
///
/// Register the projectors and the aggregate types in scope, then [`run`](ProjectionRebuild::run)
/// it. The run takes the rebuild lock, so every commit to the table fails with
/// [`Error::RebuildInProgress`] and readers under the epoch check are fenced while it reads the log
/// and writes the rows. It computes every row first, checking them across the whole run, then
/// writes them through [`write_rebuild_views`](RebuildFence::write_rebuild_views) in batches of at
/// most [`MAX_REBUILD_VIEW_BATCH`](crate::MAX_REBUILD_VIEW_BATCH) operations that fit the commit
/// byte budget, each asserting the lease, and releases the lock at the end, which settles the
/// generation.
///
/// ```
/// # #[cfg(feature = "conformance")]
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use photon_evt::conformance::{Add, Ledger};
/// use photon_evt::{
///     BoxError, CommitProjector, CommitView, MemRepository, Metadata, ProjectionRebuild,
///     Store, ViewOperation, ViewRepository,
/// };
///
/// /// A current-state projector: each ledger's total, from its state alone.
/// struct Totals;
///
/// impl CommitProjector for Totals {
///     fn name(&self) -> &str {
///         "totals"
///     }
///
///     fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
///         Ok(commit
///             .streams
///             .iter()
///             .map(|stream| {
///                 let id = stream.entity_id.clone();
///                 ViewOperation::put("totals", id.as_str(), id.clone(), "ledger", stream.state.clone())
///             })
///             .collect())
///     }
/// }
///
/// let repository = MemRepository::new();
/// let store = Store::<Ledger, _>::new(&repository, ());
/// store.execute("ledger-1", Add(5), Metadata::default()).await.unwrap();
///
/// // The rows are derived state: rebuild them from the log.
/// let report = ProjectionRebuild::new(&repository)
///     .with_projector(Arc::new(Totals))
///     .aggregate::<Ledger>()
///     .run("rebuild-1", Duration::from_secs(300))
///     .await
///     .unwrap();
/// assert_eq!((report.entities, report.puts), (1, 1));
///
/// let row = repository.get_view_at("totals", "ledger-1").await.unwrap().unwrap();
/// assert_eq!(row.payload, br#"{"total":5,"entries":1}"#);
/// # });
/// ```
///
/// # What each kind of projector is rebuilt from
///
/// - A **current-state** projector (no [`history`](CommitProjector::history)) is rebuilt from each
///   stream's final state, the state a load at the head returns: for every entity of every
///   registered type, it runs once over a view of that one stream with its state, no events, and
///   an empty commit ID. Final states come from
///   [`stream_entities_from_snapshots`](EventLog::stream_entities_from_snapshots), so a stream that
///   was compacted is seeded from its snapshot and rebuilds correctly. Live, the projector saw every
///   stream its commit wrote, so this reproduces exactly the rows each owner's own state determines.
///   **Only rebuild a projector whose rows depend on their owner's state alone.** A row that reads
///   another stream's state (an account row carrying its team's name from a team stream written in
///   the same commit) is recomputed from the one stream and written over the live row; one owned
///   by another stream fails the run with [`Error::InvalidViewOperation`] before anything is
///   written. Such rows need the state entry point the storage spec describes, which is not
///   implemented yet.
/// - A **history-dependent** projector is rebuilt by replaying every commit: the events of the
///   types the history-dependent projectors name are regrouped by commit ID ([`group_commits`]),
///   each stream's state after each commit is folded, and the projector runs over each commit. The
///   operations are applied in that order and the rows they leave are written, puts and deletes,
///   refusing a row two owners wrote ([`Error::SharedViewKey`]). Live, the projector also saw the
///   commit's streams of other types, by ID and type only; the replay leaves those out, so a row
///   must not depend on them. The replay needs the whole history, from sequence 1 to the stream's
///   head: a stream of one of those types whose early events were compacted (or expired), or whose
///   tail expired while an earlier event and a snapshot remain (DynamoDB retention deletes lazily
///   and in no order), fails the rebuild with [`Error::CompactionUncovered`] before anything is
///   written, because the snapshot cannot stand in for the commits it covers.
///
/// Only streams of the [registered](ProjectionRebuild::aggregate) types are read (and, with the
/// journal, every event of the journal's entity types): register every type whose streams own the
/// projectors' rows, and every type a history-dependent projector names. Conditions are not
/// evaluated: a put is written over the owner's row and a delete removes it, so the rebuild can run
/// over existing rows, and it is idempotent. By default it rewrites every row it plans and does not
/// delete rows that no projector produces any more; in [diff mode](ProjectionRebuild::diff) it
/// writes only the rows that changed and deletes the owned rows the plan no longer produces.
///
/// The run holds every row it will write in memory, and the history replay every event of its
/// types, twice. With the journal it also holds every event of the journal's types, and each
/// journal row, which carries its whole event. Read the log strongly consistently (DynamoDB's
/// default): an eventually consistent read can miss a commit that landed just before the lock was
/// taken, and the rebuilt row would
/// then overwrite the newer one that commit wrote.
///
/// # Failure
///
/// Every row is computed and checked before the first write, so a failure while reading or
/// projecting (an entity that does not load, a projector error, a compacted stream, a row two
/// owners would write, an operation too large to write) leaves the derived rows as they were, and
/// [`run`](ProjectionRebuild::run) releases the lock if it acquired it from a settled generation.
/// Once a write has been attempted, a failure (a lost lease, a backend error, an ambiguous write
/// that may have applied) keeps the lock, because the rows may be half-written: commits stay
/// refused and readers fenced until a retry takes over the lapsed lease (or an operator
/// force-releases it, which leaves commits refused until a rebuild ends). Run the rebuild again.
///
/// A run that took over an expired lease, or acquired the lock after a force-release, starts from
/// rows an earlier rebuild may have half-written, so it keeps the lock on any failure, even one
/// before its own first write: only a rebuild that completes settles that generation.
pub struct ProjectionRebuild<'r, R> {
    repository: &'r R,
    projectors: Vec<Arc<dyn CommitProjector>>,
    scopes: Vec<Arc<dyn Scope<R> + 'r>>,
    duplicate: Option<EntityType>,
    #[cfg(feature = "journal")]
    journal: Option<crate::journal::Journal>,
    diff: Option<Diff<'r>>,
    progress: Option<Arc<Observer<'r>>>,
}

/// A progress observer ([`ProjectionRebuild::on_progress`]).
type Observer<'a> = dyn Fn(RebuildProgress<'_>) + Send + Sync + 'a;

/// A diff-mode rebuild's reader and the row types it owns.
struct Diff<'r> {
    rows: &'r dyn diff::Rows,
    owned: BTreeSet<String>,
}

impl<R> fmt::Debug for ProjectionRebuild<'_, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectionRebuild")
            .field(
                "projectors",
                &self
                    .projectors
                    .iter()
                    .map(|projector| projector.name().to_owned())
                    .collect::<Vec<_>>(),
            )
            .field(
                "aggregates",
                &self
                    .scopes
                    .iter()
                    .map(|scope| scope.entity_type().clone())
                    .collect::<Vec<_>>(),
            )
            .field("journal", &self.has_journal())
            .field("owned", &self.diff.as_ref().map(|diff| &diff.owned))
            .finish_non_exhaustive()
    }
}

impl<R> ProjectionRebuild<'_, R> {
    /// Whether the run rebuilds the journal too.
    fn has_journal(&self) -> bool {
        #[cfg(feature = "journal")]
        return self.journal.is_some();

        #[cfg(not(feature = "journal"))]
        false
    }

    /// The row types of the journal's rows and counters when the run rebuilds it, none otherwise.
    fn journal_row_types(&self) -> &'static [&'static str] {
        #[cfg(feature = "journal")]
        if self.has_journal() {
            return &[
                crate::journal::ROW_ENTITY_TYPE,
                crate::journal::COUNTER_ENTITY_TYPE,
            ];
        }

        &[]
    }

    /// The entity types the journal reads, none without one.
    fn journal_types(&self) -> &[EntityType] {
        #[cfg(feature = "journal")]
        if let Some(journal) = &self.journal {
            return journal.entity_types();
        }

        &[]
    }
}

/// What a [`ProjectionRebuild`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RebuildReport {
    /// Entities whose final state the current-state projectors ran over.
    pub entities: usize,
    /// Commits the history-dependent projectors replayed.
    pub commits: usize,
    /// Rows written (in [diff mode](ProjectionRebuild::diff), only missing or changed rows).
    pub puts: usize,
    /// Rows deleted (in diff mode, including owned rows the plan no longer produces).
    pub deletes: usize,
    /// Diff mode: planned rows the table already held as planned, which were not rewritten.
    pub unchanged: usize,
    /// Diff mode: the owned rows the run listed before comparing.
    pub existing: usize,
    /// Diff mode: owned rows the plan does not produce that were kept, not deleted, because their
    /// owner is a stream of a type the run did not read (a type that is not registered, which a
    /// rebuild that registers it would reproduce or delete), or they have no owner.
    pub kept: usize,
    /// Diff mode: the puts and deletes by row type (the row's `entityType`).
    pub changes: BTreeMap<String, RowChanges>,
    /// Guarded batches written ([`RebuildFence::write_rebuild_views`] calls).
    pub batches: usize,
    /// Events read from the log: every event of the types the history-dependent projectors and
    /// the journal read, each counted once.
    pub events: usize,
    /// Journal rows the events imply (`with_journal`, with the `journal` cargo feature).
    pub journal_rows: usize,
    /// Journal scope counters the events imply.
    pub journal_counters: usize,
}

impl RebuildReport {
    /// Whether the run changed no row: nothing to put or delete.
    pub fn is_clean(&self) -> bool {
        self.puts == 0 && self.deletes == 0
    }
}

/// The puts and deletes of one row type, in a [diff-mode](ProjectionRebuild::diff) report.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RowChanges {
    /// Rows written: missing, or different from the plan.
    pub puts: usize,
    /// Rows deleted.
    pub deletes: usize,
}

/// What a [`plan`](ProjectionRebuild::plan) found: the operations a run would write, in order,
/// the report it would return, and the guarded batches they split into.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct RebuildPlan {
    /// The counts a run would report; `batches` is 0, because nothing was written.
    pub report: RebuildReport,
    /// The operations, in the order a run writes them.
    pub operations: Vec<ProjectedView>,
    /// The guarded batches the operations split into.
    pub batches: usize,
}

/// Where a run is, for an operator watching it ([`on_progress`](ProjectionRebuild::on_progress)).
#[derive(Debug)]
#[non_exhaustive]
pub enum RebuildProgress<'a> {
    /// Every row is computed (and, in diff mode, compared); nothing is written yet.
    Planned {
        /// The counts so far; `batches` is 0.
        report: &'a RebuildReport,
        /// The guarded batches the run will write.
        batches: usize,
    },
    /// A guarded batch was written.
    Wrote {
        /// The batches written so far.
        batch: usize,
        /// The batches to write.
        of: usize,
    },
}

impl<'r, R> ProjectionRebuild<'r, R>
where
    R: Repository + EventLog + RebuildFence,
{
    /// A rebuild over `repository`, with no projectors or aggregate types yet.
    pub fn new(repository: &'r R) -> Self {
        Self {
            repository,
            projectors: Vec::new(),
            scopes: Vec::new(),
            duplicate: None,
            #[cfg(feature = "journal")]
            journal: None,
            diff: None,
            progress: None,
        }
    }

    /// Adds a commit-wide projector. Projectors run in the order they are added, as on a store.
    /// One that reads the engine's own metadata ([`CommitProjector::engine_metadata`], the
    /// journal's projector) is refused when the rebuild runs; rebuild the journal with
    /// `with_journal` (the `journal` cargo feature).
    #[must_use]
    pub fn with_projector(mut self, projector: Arc<dyn CommitProjector>) -> Self {
        self.projectors.push(projector);
        self
    }

    /// Rebuilds the sync journal (ADR 0005) in the same run, under the same lease: its rows and
    /// counters are derived from every event of `journal`'s entity types
    /// ([`journal::replay`](crate::journal::replay)), read under the lock, and written in the same
    /// guarded batches as the views, so readers see one rebuild (one generation) for both. A
    /// projector that plans a row in the journal's partitions fails the run with
    /// [`Error::ReservedViewKey`] before anything is written. With a journal the rebuild may have
    /// no projectors: it then rebuilds the journal alone, and needs no aggregate types.
    ///
    /// Pass the journal configuration the table's [`JournalRepository`](crate::journal::JournalRepository)
    /// commits with; only its entity types are read. Like the journal's own rebuild, it restores
    /// rows a journal compaction pruned, and does not touch the compaction partition.
    #[cfg(feature = "journal")]
    #[must_use]
    pub fn with_journal(mut self, journal: crate::journal::Journal) -> Self {
        self.journal = Some(journal);
        self
    }

    /// Diff mode: writes only what changed, and deletes owned rows the log no longer implies.
    ///
    /// After computing every row, the run reads the rows it owns through `views`: every row whose
    /// type (the `entityType` a put stores, [`ViewOperation::entity_type`]) is one of `owned`, and
    /// the journal's row and counter types when it [rebuilds the journal](Self::with_journal).
    /// Then it writes a put only for a planned row that is missing or differs (owner, type,
    /// payload, or a TTL), a planned delete only for a row that exists, and a delete for every
    /// owned row the plan does not produce (a row a projector change orphaned, a bad manual
    /// write). Rows that already match are not rewritten, so a rebuild of intact views writes
    /// nothing, and a table with a change stream emits no record for them. The report counts the
    /// rows left unchanged and the changes by row type, and [`plan`](Self::plan) (the dry run)
    /// shows the same difference.
    ///
    /// `views` reads the table the repository writes views to (the in-memory repository itself,
    /// or DynamoDB's [`view_repository`](crate::dynamodb::DynamoRepository::view_repository)).
    /// Ownership is by row type and must be exact. Every put the projectors plan must be of an
    /// owned type, or the run fails with [`Error::InvalidConfig`] before writing; and nothing else
    /// may write rows of an owned type, because the run deletes the ones it does not plan. The
    /// type listing may lag behind the table (DynamoDB serves it from an eventually consistent
    /// index), so a row is skipped as unchanged, or deleted as stale, only after a strongly
    /// consistent read of its key finds it so; a planned row the listing misses is simply written.
    /// An owned row that a commit wrote just before the lock, and that the listing does not show
    /// yet, is not seen as stale until a later run (and a row the listing shows late is rewritten,
    /// not skipped).
    ///
    /// A stale row is deleted only when its owner is a stream the run read (or the journal's
    /// counter owner), or no stream of that ID exists in the log; a row owned by a stream of a
    /// type the run did not read, or with no owner, is kept and counted
    /// ([`RebuildReport::kept`]), so a type left out of the registration does not have its rows
    /// deleted. Register every type and every projector
    /// that writes an owned type, and look at [`plan`](Self::plan) first: a projector left out
    /// would have its rows deleted. A planned delete at a key holding a row of a type the diff
    /// does not own fails with [`Error::InvalidConfig`]; a planned put writes its key whatever is
    /// there, as a rebuild without the diff does. A stale row no guarded write could delete fails
    /// the run (and the dry run) before any write. Owning no type without a journal, a journal type
    /// without the journal, or the journal compaction's rows is refused before the lock.
    #[must_use]
    pub fn diff<V: ViewRepository>(
        mut self,
        views: &'r V,
        owned: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.diff = Some(Diff {
            rows: views,
            owned: owned.into_iter().map(Into::into).collect(),
        });
        self
    }

    /// Calls `observe` as a run goes: once every row is computed (and compared, in diff mode),
    /// before the first write, and after each guarded batch. A dry run reports nothing.
    #[must_use]
    pub fn on_progress(mut self, observe: impl Fn(RebuildProgress<'_>) + Send + Sync + 'r) -> Self {
        self.progress = Some(Arc::new(observe));
        self
    }

    /// Brings the streams of `A`'s type, [`Aggregate::TYPE`], into the rebuild, folded by `A`.
    #[must_use]
    pub fn aggregate<A: Aggregate>(self) -> Self {
        self.aggregate_as::<A>(A::TYPE)
    }

    /// Brings the streams stored under `entity_type` into the rebuild, folded by `A`: for stored
    /// events of another type that fold into `A`, for example after a rename. Registering a type
    /// twice is refused when the rebuild runs.
    #[must_use]
    pub fn aggregate_as<A: Aggregate>(mut self, entity_type: impl Into<EntityType>) -> Self {
        let entity_type = entity_type.into();
        if self
            .scopes
            .iter()
            .any(|scope| *scope.entity_type() == entity_type)
        {
            self.duplicate.get_or_insert(entity_type.clone());
        }

        self.scopes.push(Arc::new(Typed::<A> {
            entity_type,
            aggregate: PhantomData,
        }));
        self
    }

    /// Takes the rebuild lock as `owner` for `lease`, rebuilds, and releases the lock, which
    /// settles the generation. The lease is renewed for another `lease` whenever half of it has
    /// passed: checked after each entity, after each type's events are read, every 1024 replayed
    /// commits, and before each batch. A lock another rebuild holds is
    /// [`Error::RebuildLockHeld`]. A begin whose result is unknown is begun again once it can no
    /// longer apply, and is otherwise [`Error::RebuildBeginIndeterminate`]: the run may hold the
    /// lock under the error's lease, which a caller whose owner token is its own settles with
    /// [`RebuildState::holds_begun`](crate::RebuildState::holds_begun) and ends. A configuration error is [`Error::InvalidConfig`], before the
    /// lock is taken, except a diff's ownership errors, which are found while planning (before any
    /// write, so the lock is released).
    ///
    /// Every row is computed before the first is written, so a failure while reading the log or
    /// projecting (a compacted stream a history-dependent projector reads, an entity that does not
    /// load, a projector error, a row two owners would write) leaves the derived rows as they were:
    /// the run releases the lock and returns the error, provided it acquired the lock from a settled
    /// generation. A run that took over an expired lease or acquired after a force-release keeps the
    /// lock on any failure, because an earlier rebuild may have left the rows half-written; so does
    /// a failure once writing has begun (see [`ProjectionRebuild`]). To tell the two apart it reads
    /// the generation just before the begin.
    pub async fn run(&self, owner: &str, lease: Duration) -> Result<RebuildReport, Error> {
        self.check()?;

        let mut held = Held::take(self.repository, owner, lease).await?;
        let taken = held.taken;
        let result = self.fenced(&mut held.lease, lease, Some(taken)).await;

        held.finish(self.repository, result).await
    }

    /// Rebuilds under a lease the caller took ([`begin_rebuild`](RebuildFence::begin_rebuild)) and
    /// will end, so one lease can cover this rebuild and other work. The lease is renewed for
    /// `renew` (at least one second) when the run starts, and again whenever half of `renew` has
    /// passed; `lease` is updated in place, so the caller ends the renewed lease.
    ///
    /// A failure says how many guarded batches were attempted ([`RebuildFailure::attempted`]).
    /// Keep the lock unless no write was attempted under the lease (this run's or your own)
    /// **and** your begin acquired the lock from a settled generation
    /// ([`RebuildLease::acquired_from`]): a lease that took over an expired one, or was acquired
    /// after a force-release, covers rows an earlier rebuild may have left half-written, and only
    /// a completed rebuild may settle it.
    pub async fn run_with_lease(
        &self,
        lease: &mut RebuildLease,
        renew: Duration,
    ) -> Result<RebuildReport, RebuildFailure> {
        let unattempted = |error| RebuildFailure {
            error: Box::new(error),
            attempted: 0,
            batches: 0,
        };
        self.check().map_err(unattempted)?;
        check_renew(renew).map_err(unattempted)?;

        self.fenced(lease, renew, None).await
    }

    /// Rebuilds under `lease`. `renewed` is when the lease was last taken or renewed; `None` renews
    /// it first.
    async fn fenced(
        &self,
        lease: &mut RebuildLease,
        renew: Duration,
        renewed: Option<Instant>,
    ) -> Result<RebuildReport, RebuildFailure> {
        let mut output = Output::new(Some(Renewing::new(self.repository, lease, renew, renewed)));
        output.progress = self.progress.as_deref();

        let result = match output.keep_alive().await {
            Ok(()) => match self.rebuild(&mut output).await {
                Ok(()) => output.write().await,
                Err(error) => Err(error),
            },
            Err(error) => Err(error),
        };

        match result {
            Ok(()) => Ok(output.report),
            Err(error) => Err(RebuildFailure {
                error: Box::new(error),
                attempted: output.attempted(),
                batches: output.batches,
            }),
        }
    }

    /// A dry run: the operations a [`run`](ProjectionRebuild::run) would write, in order, without
    /// taking the lock or writing anything, with the same checks (in diff mode, the difference).
    /// Live commits may land while it reads.
    pub async fn operations(&self) -> Result<Vec<ProjectedView>, Error> {
        Ok(self.plan().await?.operations)
    }

    /// A dry run with its counts: what [`operations`](Self::operations) returns, the report a run
    /// would return, and the batches it would write.
    pub async fn plan(&self) -> Result<RebuildPlan, Error> {
        self.check()?;

        let mut output = Output::new(None);
        self.rebuild(&mut output).await?;

        Ok(RebuildPlan {
            batches: rebuild_batches(&output.planned).count(),
            report: output.report,
            operations: output.planned,
        })
    }

    /// Refuses a configuration the rebuild cannot run, before it reads or locks anything.
    fn check(&self) -> Result<(), Error> {
        let invalid = |message: String| Err(Error::InvalidConfig(message));

        if self.projectors.is_empty() && !self.has_journal() {
            return invalid(
                "a projection rebuild needs at least one commit-wide projector or a journal".into(),
            );
        }
        if let Some(projector) = self.projectors.iter().find(|p| p.reads_engine_metadata()) {
            return invalid(format!(
                "projector {:?} reads engine metadata (the journal's); rebuild the journal with \
                 `with_journal`",
                projector.name()
            ));
        }
        if self.scopes.is_empty() && !self.projectors.is_empty() {
            return invalid("a projection rebuild needs at least one aggregate type".into());
        }
        if let Some(diff) = &self.diff {
            if diff.owned.is_empty() && !self.has_journal() {
                return invalid("a diff-mode rebuild owns at least one row type".into());
            }
            #[cfg(feature = "journal")]
            for entity_type in &diff.owned {
                let journal = [
                    crate::journal::ROW_ENTITY_TYPE,
                    crate::journal::COUNTER_ENTITY_TYPE,
                ];
                if *entity_type == crate::journal::COMPACTION_ENTITY_TYPE
                    || (journal.contains(&entity_type.as_str()) && !self.has_journal())
                {
                    return invalid(format!(
                        "a diff cannot own the journal's {entity_type:?} rows on their own: rebuild \
                         the journal with `with_journal`, which owns its rows and counters"
                    ));
                }
            }
        }
        if let Some(entity_type) = &self.duplicate {
            return invalid(format!(
                "entity type {:?} is registered twice",
                entity_type.as_str()
            ));
        }
        for projector in &self.projectors {
            for entity_type in projector.history() {
                if self.scope(entity_type).is_none() {
                    return invalid(format!(
                        "history-dependent projector {:?} reads entity type {:?}, which is not \
                         registered: register its aggregate so the replay can fold its state",
                        projector.name(),
                        entity_type.as_str()
                    ));
                }
            }
        }

        Ok(())
    }

    fn scope(&self, entity_type: &EntityType) -> Option<&Arc<dyn Scope<R> + 'r>> {
        self.scopes
            .iter()
            .find(|scope| scope.entity_type() == entity_type)
    }

    async fn rebuild(&self, output: &mut Output<'_, R>) -> Result<(), Error> {
        let current: Vec<_> = self
            .projectors
            .iter()
            .enumerate()
            .filter(|(_, projector)| projector.history().is_empty())
            .collect();
        let history: Vec<_> = self
            .projectors
            .iter()
            .enumerate()
            .filter(|(_, projector)| !projector.history().is_empty())
            .collect();

        let history_types: BTreeSet<EntityType> = history
            .iter()
            .flat_map(|(_, projector)| projector.history().iter().cloned())
            .collect();

        // Every event the history replay and the journal read, each type read once. The history
        // replay's types are checked to run from sequence 1 to the head, so a compacted stream
        // fails the rebuild with nothing written.
        let mut events: BTreeMap<EntityType, Vec<SerializedEvent>> = BTreeMap::new();
        for entity_type in &history_types {
            events.insert(entity_type.clone(), self.whole_history(entity_type).await?);
            output.keep_alive().await?;
        }
        for entity_type in self.journal_types() {
            if !events.contains_key(entity_type) {
                events.insert(entity_type.clone(), self.all_events(entity_type).await?);
                output.keep_alive().await?;
            }
        }
        output.report.events = events.values().map(Vec::len).sum();

        // The streams the run read: a diff deletes a stale owned row only when its owner is one.
        let mut streams: BTreeSet<String> = BTreeSet::new();
        if self.diff.is_some() {
            streams.extend(
                events
                    .values()
                    .flatten()
                    .map(|event| event.entity_id.to_string()),
            );
            #[cfg(feature = "journal")]
            if self.has_journal() {
                streams.insert(crate::journal::COUNTER_OWNER.to_owned());
            }
        }

        #[cfg(feature = "journal")]
        let journal = match &self.journal {
            Some(journal) => Some(crate::journal::replay(
                journal
                    .entity_types()
                    .iter()
                    .filter_map(|entity_type| events.get(entity_type))
                    .flatten(),
            )?),
            None => None,
        };

        let replayed = if history.is_empty() {
            None
        } else {
            let replayed: Vec<SerializedEvent> = history_types
                .iter()
                .filter_map(|entity_type| events.remove(entity_type))
                .flatten()
                .collect();

            Some(self.replay_history(&history, replayed, output).await?)
        };
        drop(events);

        if !current.is_empty() {
            for scope in &self.scopes {
                let mut states = scope.final_states(self.repository);
                while let Some(state) = poll_fn(|cx| states.as_mut().poll_next(cx)).await {
                    let (entity_id, state) = state?;
                    if self.diff.is_some() {
                        streams.insert(entity_id.to_string());
                    }
                    let view = CommitView::new(
                        "",
                        vec![CommitStream::new(
                            entity_id,
                            scope.entity_type().clone(),
                            Vec::new(),
                            state,
                        )],
                    );

                    for item in project_with(current.iter().copied(), &view)? {
                        output.plan(unconditioned(item))?;
                    }
                    output.report.entities += 1;
                    output.keep_alive().await?;
                }
            }
        }

        if let Some((replay, commits)) = replayed {
            output.report.commits += commits;
            for item in replay.into_last() {
                output.plan(unconditioned(item))?;
            }
        }

        #[cfg(feature = "journal")]
        if let Some(journal) = journal {
            output.report.journal_rows = journal.rows.len();
            output.report.journal_counters = journal.counters.len();
            for item in crate::journal::operations(&journal) {
                output.plan(item)?;
            }
        }

        if let Some(diff) = &self.diff {
            let unread = output
                .diff(diff.rows, &self.owned_types(diff), &streams)
                .await?;

            // An owned row whose owner the run did not read is deleted only when no such stream
            // exists: one of a type left out of the registration is kept, as is a row with no
            // owner (no guarded write could delete it).
            let (mut orphans, mut kept) = (Vec::new(), 0);
            let mut exists: BTreeMap<String, bool> = BTreeMap::new();
            for row in unread {
                let found = match exists.get(&row.entity_id) {
                    Some(found) => *found,
                    None if row.entity_id.is_empty() => true,
                    None => {
                        let found = self.stream_exists(&row.entity_id).await?;
                        exists.insert(row.entity_id.clone(), found);
                        output.keep_alive().await?;

                        found
                    }
                };
                if found {
                    kept += 1;
                } else {
                    orphans.push(row);
                }
            }
            output.finish_diff(orphans, kept)?;
        }

        Ok(())
    }

    /// Whether the log holds a stream `id`: any event, or a snapshot (a stream compacted to its
    /// head keeps its snapshot).
    async fn stream_exists(&self, id: &str) -> Result<bool, Error> {
        let id = EntityId::new(id);
        if !self.repository.latest(&id, 1).await?.is_empty() {
            return Ok(true);
        }

        Ok(self.repository.snapshot(&id).await?.is_some())
    }

    /// The row types a diff owns: the declared ones, and the journal's rows and counters when the
    /// run rebuilds it.
    fn owned_types(&self, diff: &Diff<'_>) -> BTreeSet<String> {
        let mut owned = diff.owned.clone();
        owned.extend(self.journal_row_types().iter().map(|t| (*t).to_owned()));

        owned
    }

    /// Replays every commit of `events`, the history-dependent projectors' types, and returns the
    /// rows the replay leaves, with the number of commits replayed.
    async fn replay_history(
        &self,
        projectors: &[(usize, &Arc<dyn CommitProjector>)],
        events: Vec<SerializedEvent>,
        output: &mut Output<'_, R>,
    ) -> Result<(ViewReplay, usize), Error> {
        let commits = group_commits(events)?;
        let count = commits.len();
        let mut folds: HashMap<EntityId, Box<dyn Fold>> = HashMap::new();
        let mut replay = ViewReplay::default();

        for (index, mut commit) in commits.into_iter().enumerate() {
            if index % 1024 == 1023 {
                output.keep_alive().await?;
            }

            for stream in &mut commit.streams {
                let fold = match folds.get_mut(&stream.entity_id) {
                    Some(fold) => fold,
                    None => {
                        let scope = self
                            .scope(&stream.entity_type)
                            .expect("every replayed type is registered");
                        folds
                            .entry(stream.entity_id.clone())
                            .or_insert(scope.folder(stream.entity_id.clone()))
                    }
                };

                for event in &stream.events {
                    fold.replay(event.clone())?;
                }
                stream.state = fold.state()?;
            }

            for item in project_with(projectors.iter().copied(), &commit)? {
                replay.apply(item)?;
            }
        }

        Ok((replay, count))
    }

    /// Every stored event of one type, in no particular order, for the journal: its rows are a
    /// function of the events' stored allocations, whatever was compacted.
    async fn all_events(&self, entity_type: &EntityType) -> Result<Vec<SerializedEvent>, Error> {
        let mut events = Vec::new();
        let mut batches = pin!(
            self.repository
                .stream_all_events(StreamFilter::of_type(entity_type.clone()))
        );
        while let Some(batch) = poll_fn(|cx| batches.as_mut().poll_next(cx)).await {
            events.extend(batch?);
        }

        Ok(events)
    }

    /// Every event of every stream of one type, failing with [`Error::CompactionUncovered`] when a
    /// stream's history does not run from sequence 1 to its enumerated head: an early part was
    /// compacted, or (on DynamoDB with retention, whose TTL deletes lazily and in no order) a tail
    /// expired while an earlier event and a snapshot remain. `through` is the last missing
    /// sequence before the first event, or the head for a missing tail. A gap in between fails
    /// the fold with [`Error::SequenceGap`], and an event above the head is
    /// [`Error::StreamHeadDrift`].
    async fn whole_history(&self, entity_type: &EntityType) -> Result<Vec<SerializedEvent>, Error> {
        let heads = self.repository.entity_heads(Some(entity_type)).await?;

        let mut streams: BTreeMap<EntityId, Vec<SerializedEvent>> = BTreeMap::new();
        let mut batches = pin!(
            self.repository
                .stream_all_events(StreamFilter::of_type(entity_type.clone()))
        );
        while let Some(batch) = poll_fn(|cx| batches.as_mut().poll_next(cx)).await {
            for event in batch? {
                streams
                    .entry(event.entity_id.clone())
                    .or_default()
                    .push(event);
            }
        }

        for (entity_id, head) in heads {
            let stream = streams.entry(entity_id.clone()).or_default();
            stream.sort_by_key(|event| event.sequence);

            // The replay must run from sequence 1 to the head. Missing early events were compacted
            // (or expired) and a missing tail expired; either way the snapshot cannot stand in for
            // the commits that are gone, and replaying what is left would build an earlier state.
            let first = stream.first().map(|event| event.sequence);
            let last = stream.last().map(|event| event.sequence);
            let through = match (first, last) {
                (Some(first), _) if first != EventSequence(1) => EventSequence(first.0 - 1),
                (_, Some(last)) if last > head => {
                    return Err(crate::heads::drift(&entity_id, head, last));
                }
                (_, Some(last)) if last == head => continue,
                _ => head,
            };

            let covered = self
                .repository
                .snapshot(&entity_id)
                .await?
                .map(|snapshot| snapshot.event_sequence);

            return Err(Error::CompactionUncovered {
                entity_id,
                through,
                covered,
            });
        }

        Ok(streams.into_values().flatten().collect())
    }
}

/// The operation a rebuild writes: the same row without its condition or mapped error, because a
/// replay applies what the live commits wrote rather than evaluating them again.
fn unconditioned(item: ProjectedView) -> ProjectedView {
    let op = item.op;
    let op = match op.kind {
        ViewOperationKind::Put => {
            ViewOperation::put(op.pk, op.sk, op.owner, op.entity_type, op.payload)
        }
        ViewOperationKind::Delete => ViewOperation::delete(op.pk, op.sk, op.owner),
    };

    ProjectedView { op, ..item }
}

/// A rebuild's planned operations, checked across the whole run, then written in batches under
/// the lease, or only collected for a dry run.
struct Output<'a, R> {
    /// The lease the operations are written under; `None` for a dry run.
    fence: Option<Renewing<'a, R>>,
    planned: Vec<ProjectedView>,
    /// Each planned key: its owner and the provenance of the operation that planned it.
    keys: HashMap<(String, String), (EntityId, String, usize)>,
    /// The batches the plan splits into, once it is complete.
    batches: usize,
    report: RebuildReport,
    progress: Option<&'a Observer<'a>>,
}

impl<'a, R: RebuildFence> Output<'a, R> {
    fn new(fence: Option<Renewing<'a, R>>) -> Self {
        Self {
            fence,
            planned: Vec::new(),
            keys: HashMap::new(),
            batches: 0,
            report: RebuildReport::default(),
            progress: None,
        }
    }

    /// The guarded batches attempted.
    fn attempted(&self) -> usize {
        self.fence.as_ref().map_or(0, |fence| fence.attempted)
    }

    /// Plans one operation, refusing what no batch could write: a key the run already planned
    /// ([`Error::SharedViewKey`] for another owner, [`Error::DuplicateViewKey`] for the same one)
    /// or an operation over the per-item byte limit ([`Error::CommitTooLarge`]).
    fn plan(&mut self, item: ProjectedView) -> Result<(), Error> {
        let size = view_operation_size(&item.op);
        if size > MAX_COMMIT_ITEM_BYTES || size + lock_check_size() > MAX_COMMIT_BYTES {
            return Err(Error::CommitTooLarge {
                actions: 2,
                bytes: size + lock_check_size(),
                largest_item: size,
            });
        }

        let key = (item.op.pk.clone(), item.op.sk.clone());
        if let Some((owner, projector, operation)) = self.keys.get(&key) {
            return Err(if *owner == item.op.owner {
                Error::DuplicateViewKey {
                    pk: key.0,
                    sk: key.1,
                    first: (projector.clone(), *operation),
                    second: (item.projector, item.operation),
                }
            } else {
                Error::SharedViewKey {
                    pk: key.0,
                    sk: key.1,
                    first: owner.to_string(),
                    second: item.op.owner.to_string(),
                }
            });
        }
        self.keys.insert(
            key,
            (
                item.op.owner.clone(),
                item.projector.clone(),
                item.operation,
            ),
        );

        match item.op.kind {
            ViewOperationKind::Put => self.report.puts += 1,
            ViewOperationKind::Delete => self.report.deletes += 1,
        }
        self.planned.push(item);

        Ok(())
    }

    /// Renews the lease when half of it has passed (see [`Renewing::keep_alive`]); a dry run has
    /// none.
    async fn keep_alive(&mut self) -> Result<(), Error> {
        match &mut self.fence {
            Some(fence) => fence.keep_alive().await,
            None => Ok(()),
        }
    }

    /// Writes the planned operations in guarded batches of at most [`MAX_REBUILD_VIEW_BATCH`](crate::MAX_REBUILD_VIEW_BATCH)
    /// operations that fit the commit byte budget with the lease guard.
    async fn write(&mut self) -> Result<(), Error> {
        let planned = std::mem::take(&mut self.planned);
        let batches: Vec<&[ProjectedView]> = rebuild_batches(&planned).collect();
        self.batches = batches.len();

        let Some(fence) = &mut self.fence else {
            return Ok(());
        };
        if let Some(observe) = self.progress {
            observe(RebuildProgress::Planned {
                report: &self.report,
                batches: self.batches,
            });
        }
        for batch in batches {
            fence.write(batch).await?;
            self.report.batches += 1;
            if let Some(observe) = self.progress {
                observe(RebuildProgress::Wrote {
                    batch: self.report.batches,
                    of: self.batches,
                });
            }
        }

        Ok(())
    }
}

type BoxStream<'a, T> = Pin<Box<dyn Stream<Item = T> + Send + 'a>>;

/// One registered aggregate type, with its type erased.
trait Scope<R>: Send + Sync {
    /// The stored entity type.
    fn entity_type(&self) -> &EntityType;

    /// Every entity of the type with its final state's JSON, seeded from its snapshot.
    fn final_states<'a>(
        &'a self,
        repository: &'a R,
    ) -> BoxStream<'a, Result<(EntityId, Vec<u8>), Error>>;

    /// A fold of one stream's events from sequence 1.
    fn folder(&self, entity_id: EntityId) -> Box<dyn Fold>;
}

struct Typed<A> {
    entity_type: EntityType,
    aggregate: PhantomData<fn() -> A>,
}

impl<A, R> Scope<R> for Typed<A>
where
    A: Aggregate,
    R: Repository + EventLog,
{
    fn entity_type(&self) -> &EntityType {
        &self.entity_type
    }

    fn final_states<'a>(
        &'a self,
        repository: &'a R,
    ) -> BoxStream<'a, Result<(EntityId, Vec<u8>), Error>> {
        let loaded: BoxStream<'a, Result<Loaded<A>, Error>> = Box::pin(
            repository.stream_entities_from_snapshots::<A>(StreamFilter::of_type(
                self.entity_type.clone(),
            )),
        );

        Box::pin(unfold(loaded, |mut loaded| async move {
            let next = poll_fn(|cx| loaded.as_mut().poll_next(cx)).await?;
            let state = next.and_then(|loaded| {
                let state = json::to_vec(&loaded.aggregate)?;

                Ok((loaded.entity_id, state))
            });

            Some((state, loaded))
        }))
    }

    fn folder(&self, entity_id: EntityId) -> Box<dyn Fold> {
        Box::new(Loaded::<A>::new(entity_id))
    }
}

/// A stream's state folded event by event, with its aggregate type erased.
trait Fold: Send {
    fn replay(&mut self, event: SerializedEvent) -> Result<(), Error>;

    /// The state's JSON, the bytes a snapshot and a live commit view carry.
    fn state(&self) -> Result<Vec<u8>, Error>;
}

impl<A: Aggregate> Fold for Loaded<A> {
    fn replay(&mut self, event: SerializedEvent) -> Result<(), Error> {
        Loaded::replay(self, event)
    }

    fn state(&self) -> Result<Vec<u8>, Error> {
        Ok(json::to_vec(&self.aggregate)?)
    }
}

#[cfg(test)]
mod tests;
