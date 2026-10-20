//! Commit-wide projectors (ADR 0004, C7; storage spec, Views).
//!
//! A [`CommitProjector`] is a pure function over everything one commit wrote. It returns
//! [`ViewOperation`]s, puts and deletes of rows at any `pk`/`sk`, with optional conditions and
//! mapped errors, which the repository commits in the same transaction as the events. The contract
//! that keeps views derived state: a projector's output may depend only on its [`CommitView`], so
//! grouping stored events by commit ID ([`group_commits`]) and running the projector over each
//! group reproduces the operations the live commit produced. A view that cannot be rebuilt that way
//! (one that records a clock reading, a random value, or a verdict decided outside the event log)
//! is a latent data-loss bug.
//!
//! Register projectors with [`Store::with_commit_projector`](crate::Store::with_commit_projector)
//! or [`Transaction::with_commit_projector`](crate::Transaction::with_commit_projector). The older
//! per-aggregate [`Projector`](crate::Projector) keeps working beside them.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::error::Error as StdError;
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::error::{BoxError, Error};
use crate::event::SerializedEvent;
use crate::ids::{EntityId, EntityType, EventSequence};
use crate::repository::Repository;

/// The longest view partition key, in UTF-8 bytes (DynamoDB's own limit), so a projector fails
/// identically on every backend instead of committing on PostgreSQL and failing on DynamoDB.
pub const MAX_VIEW_PK_BYTES: usize = 2048;

/// The longest view sort key, in UTF-8 bytes (DynamoDB's own limit).
pub const MAX_VIEW_SK_BYTES: usize = 1024;

/// The reason of a view operation whose condition did not hold, on every backend: DynamoDB's
/// cancellation reason code, and the value PostgreSQL and the in-memory repository report for the
/// same failure.
pub const REASON_CONDITION_FAILED: &str = "ConditionalCheckFailed";

/// The kind of a [`ViewOperation`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ViewOperationKind {
    /// Writes a row, replacing any row at the same key unless the operation is
    /// [`ViewCondition::Absent`].
    Put,
    /// Removes a row; deleting an absent row succeeds unless the operation is
    /// [`ViewCondition::Present`].
    Delete,
}

impl fmt::Display for ViewOperationKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Put => "put",
            Self::Delete => "delete",
        })
    }
}

/// A condition a [`ViewOperation`] carries. A failed condition fails the commit with
/// [`Error::ProjectorOperation`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ViewCondition {
    /// No condition (the default).
    #[default]
    None,
    /// No row may exist at the key. Valid only on a put (a put that claims a name).
    Absent,
    /// A row must exist at the key. Valid only on a delete (a delete that releases a claim).
    Present,
}

/// One backend-neutral view write, evaluated and written by the repository in the commit's
/// transaction. Build one with [`put`](ViewOperation::put) or [`delete`](ViewOperation::delete).
///
/// The [mapped error](ViewOperation::on_conflict) is in-memory commit data and is never stored.
/// Equality compares it by its message.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ViewOperation {
    /// Put or delete.
    pub kind: ViewOperationKind,
    /// The row's partition key, chosen by the projector. Required.
    pub pk: String,
    /// The row's sort key, chosen by the projector. Required.
    pub sk: String,
    /// The stream the row belongs to: required, and stored as the row's `entityID` attribute. It
    /// must be a stream the commit wrote. Every write to a row is conditioned on its owner: an
    /// operation by any other stream fails with [`Error::ProjectorOperation`], so a stream never
    /// takes over, overwrites, or deletes another stream's row.
    ///
    /// **A row's owner must never change.** To give a key to another stream, do not delete the row
    /// and put it again under the new owner: keep the row and rewrite it, as its owner, with a
    /// tombstone payload. A replay orders the writes to a row by its owner's sequence, the only
    /// commit order the log keeps, and a row two owners wrote has no such order.
    pub owner: EntityId,
    /// Informational and always written by a put; readers never interpret it. Empty for a delete.
    pub entity_type: String,
    /// The view state JSON of a put. It must be valid UTF-8, because DynamoDB stores it as a
    /// string. Empty for a delete.
    pub payload: Vec<u8>,
    /// The operation's condition.
    pub condition: ViewCondition,
    /// The projector's own error for the case where the condition fails. The commit's
    /// [`ProjectorOperationError`] reports it as its [`source`](StdError::source).
    pub conflict: Option<Arc<dyn StdError + Send + Sync + 'static>>,
}

impl ViewOperation {
    /// An operation that writes a row at `pk` and `sk`.
    pub fn put(
        pk: impl Into<String>,
        sk: impl Into<String>,
        owner: impl Into<EntityId>,
        entity_type: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            kind: ViewOperationKind::Put,
            pk: pk.into(),
            sk: sk.into(),
            owner: owner.into(),
            entity_type: entity_type.into(),
            payload: payload.into(),
            condition: ViewCondition::None,
            conflict: None,
        }
    }

    /// An operation that deletes the row at `pk` and `sk`, which only its `owner` may delete.
    pub fn delete(
        pk: impl Into<String>,
        sk: impl Into<String>,
        owner: impl Into<EntityId>,
    ) -> Self {
        Self {
            kind: ViewOperationKind::Delete,
            pk: pk.into(),
            sk: sk.into(),
            owner: owner.into(),
            entity_type: String::new(),
            payload: Vec::new(),
            condition: ViewCondition::None,
            conflict: None,
        }
    }

    /// Requires that no row exists at the key. Valid on a put: any other kind is refused with
    /// [`Error::InvalidViewOperation`] before any write.
    #[must_use]
    pub fn if_absent(mut self) -> Self {
        self.condition = ViewCondition::Absent;
        self
    }

    /// Requires that a row exists at the key. Valid on a delete: any other kind is refused with
    /// [`Error::InvalidViewOperation`] before any write.
    #[must_use]
    pub fn if_present(mut self) -> Self {
        self.condition = ViewCondition::Present;
        self
    }

    /// Maps a failed condition to the projector's own error: the commit's
    /// [`ProjectorOperationError`] reports `error` as its [`source`](StdError::source), so the
    /// application matches its own error value.
    #[must_use]
    pub fn on_conflict(mut self, error: impl StdError + Send + Sync + 'static) -> Self {
        self.conflict = Some(Arc::new(error));
        self
    }
}

impl PartialEq for ViewOperation {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
            && self.pk == other.pk
            && self.sk == other.sk
            && self.owner == other.owner
            && self.entity_type == other.entity_type
            && self.payload == other.payload
            && self.condition == other.condition
            && self.conflict.as_ref().map(|error| error.to_string())
                == other.conflict.as_ref().map(|error| error.to_string())
    }
}

impl Eq for ViewOperation {}

/// The size of a view operation in the commit budget. A put counts its five attributes (name and
/// value bytes each; `entityID` is the owner), whatever its condition. A delete counts its two key
/// attributes, its condition expression, and the owner value it binds (`:owner`). It equals the
/// size of the DynamoDB action the repository builds, and the in-memory repository budgets
/// operations by it.
pub fn view_operation_size(operation: &ViewOperation) -> usize {
    use crate::wire::dynamo::{OWNER_PLACEHOLDER, delete_condition};

    let keys = "pk".len() + operation.pk.len() + "sk".len() + operation.sk.len();

    match operation.kind {
        ViewOperationKind::Delete => {
            keys + delete_condition(operation.condition).len()
                + OWNER_PLACEHOLDER.len()
                + operation.owner.as_str().len()
        }
        ViewOperationKind::Put => {
            keys + "entityID".len()
                + operation.owner.as_str().len()
                + "entityType".len()
                + operation.entity_type.len()
                + "payload".len()
                + operation.payload.len()
        }
    }
}

/// A view row as a views-table item holds it: the five attributes of a commit-wide view row, plus
/// the optional `ttl` the [`ViewRepository`](crate::ViewRepository) can write (Go's
/// `evt.SerializedView`). Commit-wide view operations never carry a `ttl`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ViewRow {
    /// The partition key.
    pub pk: String,
    /// The sort key.
    pub sk: String,
    /// The row's owner: the stream that wrote it.
    pub entity_id: String,
    /// The informational entity type.
    pub entity_type: String,
    /// The view state JSON.
    pub payload: Vec<u8>,
    /// The expiry in epoch seconds, or `0` for none. Only
    /// [`ViewRepository::put_view`](crate::ViewRepository::put_view) writes one; DynamoDB deletes
    /// the item some time after it passes, so a read can still return an expired row.
    pub ttl: i64,
}

impl ViewRow {
    /// A view row.
    pub fn new(
        pk: impl Into<String>,
        sk: impl Into<String>,
        entity_id: impl Into<String>,
        entity_type: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            pk: pk.into(),
            sk: sk.into(),
            entity_id: entity_id.into(),
            entity_type: entity_type.into(),
            payload: payload.into(),
            ttl: 0,
        }
    }

    /// The row with an expiry in epoch seconds (`0` for none).
    #[must_use]
    pub fn with_ttl(mut self, ttl: i64) -> Self {
        self.ttl = ttl;
        self
    }

    /// The row a put operation writes.
    pub(crate) fn of(operation: &ViewOperation) -> Self {
        Self {
            pk: operation.pk.clone(),
            sk: operation.sk.clone(),
            entity_id: operation.owner.to_string(),
            entity_type: operation.entity_type.clone(),
            payload: operation.payload.clone(),
            ttl: 0,
        }
    }
}

/// One stream a commit wrote, as a commit-wide projector sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommitStream {
    /// The stream.
    pub entity_id: EntityId,
    /// The stream's aggregate type.
    pub entity_type: EntityType,
    /// This commit's events for the stream, ascending by sequence, as stored (their metadata
    /// carries the commit ID).
    pub events: Vec<SerializedEvent>,
    /// The stream's state after the commit: the JSON the aggregate's snapshot would carry
    /// ([`json::to_vec`](crate::json::to_vec) of the aggregate). Empty when the caller has none to
    /// give, as [`group_commits`] leaves it.
    pub state: Vec<u8>,
}

impl CommitStream {
    /// A stream of a commit view. The events' metadata is given its effective `causationId` (the
    /// command ID when none was set), the value a stored event carries, so a projector reads the
    /// same metadata when the commit is live and when a replay reads the stored events.
    pub fn new(
        entity_id: impl Into<EntityId>,
        entity_type: impl Into<EntityType>,
        mut events: Vec<SerializedEvent>,
        state: impl Into<Vec<u8>>,
    ) -> Self {
        for event in &mut events {
            if event.metadata.causation_id.is_none() {
                event.metadata.causation_id = event.metadata.command_id.clone();
            }
        }

        Self {
            entity_id: entity_id.into(),
            entity_type: entity_type.into(),
            events,
            state: state.into(),
        }
    }
}

/// Everything one commit wrote, the input of a [`CommitProjector`]. Streams ascend by entity-ID
/// bytes, the order of the transaction's groups; each holds the commit's events only.
///
/// It carries no outcome context: a rebuild regroups stored events by commit ID and has no outcome
/// row, so a row that depended on the outcome's status could not be rebuilt.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommitView {
    /// The one commit ID every event of the commit carries. Empty for a pre-v0.9 event, which
    /// [`group_commits`] replays as a single-event commit.
    pub commit_id: String,
    /// The written streams, ascending by entity-ID bytes.
    pub streams: Vec<CommitStream>,
}

impl CommitView {
    /// A commit view. The streams must ascend by entity-ID bytes.
    pub fn new(commit_id: impl Into<String>, streams: Vec<CommitStream>) -> Self {
        Self {
            commit_id: commit_id.into(),
            streams,
        }
    }

    /// The number of events across all streams.
    pub fn event_count(&self) -> usize {
        self.streams.iter().map(|stream| stream.events.len()).sum()
    }

    /// The view a projector is given. A current-state projector (it names no `history`) gets an
    /// empty commit ID and streams with no events, and the state of every stream: its rows are a
    /// function of the final states alone, which is what lets a rebuild reproduce them from state.
    /// A history-dependent projector gets the commit ID and, for the streams whose entity type it
    /// names, their events and state; for every other stream it gets neither, so its rows cannot
    /// depend on a stream whose history it did not declare.
    fn visible_to(&self, history: &[EntityType]) -> CommitView {
        let mut view = self.clone();

        if history.is_empty() {
            view.commit_id = String::new();
            for stream in &mut view.streams {
                stream.events = Vec::new();
            }

            return view;
        }

        for stream in &mut view.streams {
            if !history.contains(&stream.entity_type) {
                stream.events = Vec::new();
                stream.state = Vec::new();
            }
        }

        view
    }
}

/// A pure function over every event of a commit, emitting view operations that commit atomically
/// with it. It runs once per commit that writes at least one event, after every stream is staged,
/// and again on every fence-aware re-preparation, so it must be repeatable.
///
/// `project` must be deterministic: the operations may depend only on the commit view, never on a
/// clock, a random source, the environment, a store, or state remembered between calls, and must
/// not be ordered by a hash map's iteration. The operations it returns are appended to the commit
/// in the projector's declaration order, each projector's in emission order.
///
/// A projector is one of two kinds, and [`history`](CommitProjector::history) says which. A
/// current-state projector computes its rows from the state each stream has after the commit; it is
/// given an empty commit ID and no events, and a rebuild never replays history for it. A
/// history-dependent projector also reads the commit ID and the events of the entity types it
/// names; a rebuild replays every commit for it, so the commits it reads must never be compacted
/// away, and its rows must be ordered by something a replay has.
///
/// Every operation of every projector names an [owner](ViewOperation::owner): a stream the commit
/// wrote (and, for a history-dependent projector, one of the types it names). **A row's owner must
/// never change**: never delete a row to hand its key to another stream; keep the row and rewrite
/// it with a tombstone payload.
pub trait CommitProjector: Send + Sync {
    /// Identifies the projector in errors. It should be unique within a store.
    fn name(&self) -> &str;

    /// The entity types whose events the projector reads. Empty (the default) for a current-state
    /// projector, which is then given views with an empty commit ID and streams that carry no
    /// events. A history-dependent projector is given the commit ID and the events and state of the
    /// streams of these types only; every other stream arrives with neither. The store declares
    /// these types in the feature manifest's `historyDependent` set before the first commit that
    /// runs the projector, so `compact_below` refuses them in every process, and each of the
    /// projector's operations must be owned by a stream of one of these types.
    fn history(&self) -> &[EntityType] {
        &[]
    }

    /// Returns the commit's view operations. An error fails the command before any write, as an
    /// internal failure ([`Error::Projector`]).
    fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, BoxError>;
}

/// A view operation with its provenance: which projector emitted it, at which position of the
/// store's projectors, and as which of that projector's operations. The provenance is what a failed
/// operation's [`ProjectorOperationError`] reports.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProjectedView {
    /// The projector's [name](CommitProjector::name).
    pub projector: String,
    /// The projector's position in declaration order.
    pub projector_index: usize,
    /// The operation's index among those the projector emitted.
    pub operation: usize,
    /// The operation.
    pub op: ViewOperation,
}

impl ProjectedView {
    /// An operation with its provenance.
    pub fn new(
        projector: impl Into<String>,
        projector_index: usize,
        operation: usize,
        op: ViewOperation,
    ) -> Self {
        Self {
            projector: projector.into(),
            projector_index,
            operation,
            op,
        }
    }
}

impl ProjectedView {
    /// The view without its row data (entity type and payload), keeping what identifies it in an
    /// error: provenance, kind, keys, owner, condition, and mapped error. Repositories
    /// keep this beside the built actions, so a failed commit can be mapped without holding a
    /// second copy of large payloads.
    #[cfg_attr(not(feature = "dynamodb"), allow(dead_code))]
    pub(crate) fn provenance(&self) -> Self {
        Self {
            projector: self.projector.clone(),
            projector_index: self.projector_index,
            operation: self.operation,
            op: ViewOperation {
                kind: self.op.kind,
                pk: self.op.pk.clone(),
                sk: self.op.sk.clone(),
                owner: self.op.owner.clone(),
                entity_type: String::new(),
                payload: Vec::new(),
                condition: self.op.condition,
                conflict: self.op.conflict.clone(),
            },
        }
    }
}

/// A view operation whose condition failed or whose statement the backend rejected. It names the
/// projector and the operation. It is not a conflict and not transient: re-preparing the command
/// cannot fix it, so it is returned at once and records no outcome. Nothing from the commit was
/// written.
///
/// The operation's [mapped error](ProjectorOperationError::mapped)
/// ([`ViewOperation::on_conflict`]) is kept apart from the backend's own
/// [cause](ProjectorOperationError::cause). [`source`](StdError::source) is the mapped error when
/// set, else the cause.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ProjectorOperationError {
    /// The projector's name.
    pub projector: String,
    /// The projector's position in declaration order.
    pub projector_index: usize,
    /// The operation's index among those the projector emitted.
    pub operation: usize,
    /// The kind of the failed operation.
    pub kind: ViewOperationKind,
    /// The partition key the operation targeted.
    pub pk: String,
    /// The sort key the operation targeted.
    pub sk: String,
    /// The backend's reason: the DynamoDB cancellation reason code (`ConditionalCheckFailed`,
    /// `ValidationError`, `ItemCollectionSizeLimitExceeded`), `ConditionalCheckFailed` for a
    /// failed PostgreSQL or in-memory condition, or the SQLSTATE PostgreSQL raised.
    pub reason: String,
    /// The operation's mapped error: set only for a failed condition
    /// ([`REASON_CONDITION_FAILED`]) whose operation carries one.
    mapped: Option<Arc<dyn StdError + Send + Sync + 'static>>,
    /// The backend's own error, when there is one: the database error PostgreSQL raised, say.
    /// `None` for a DynamoDB or in-memory condition failure.
    cause: Option<Arc<dyn StdError + Send + Sync + 'static>>,
}

impl ProjectorOperationError {
    /// The error for a failed view. A failed condition ([`REASON_CONDITION_FAILED`]) carries the
    /// operation's mapped error; `cause` is the backend's own error, which may be `None`.
    pub fn for_view(
        view: &ProjectedView,
        reason: impl Into<String>,
        cause: Option<BoxError>,
    ) -> Self {
        let reason = reason.into();
        let mapped = (reason == REASON_CONDITION_FAILED)
            .then(|| view.op.conflict.clone())
            .flatten();

        Self {
            projector: view.projector.clone(),
            projector_index: view.projector_index,
            operation: view.operation,
            kind: view.op.kind,
            pk: view.op.pk.clone(),
            sk: view.op.sk.clone(),
            reason,
            mapped,
            cause: cause.map(Arc::<dyn StdError + Send + Sync>::from),
        }
    }

    /// The failed condition of a view: [`reason`](Self::reason) [`REASON_CONDITION_FAILED`], no
    /// backend cause.
    pub fn condition_failed(view: &ProjectedView) -> Self {
        Self::for_view(view, REASON_CONDITION_FAILED, None)
    }

    /// The operation's mapped error ([`ViewOperation::on_conflict`]), set only for a failed
    /// condition whose operation carries one.
    pub fn mapped(&self) -> Option<&(dyn StdError + Send + Sync + 'static)> {
        self.mapped.as_deref()
    }

    /// The backend's own error, when there is one.
    pub fn cause(&self) -> Option<&(dyn StdError + Send + Sync + 'static)> {
        self.cause.as_deref()
    }
}

impl fmt::Display for ProjectorOperationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "projector {:?} (#{}) operation {}: {} of {:?}/{:?} failed: {}",
            self.projector,
            self.projector_index,
            self.operation,
            self.kind,
            self.pk,
            self.sk,
            self.reason
        )
    }
}

impl StdError for ProjectorOperationError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.mapped()
            .or_else(|| self.cause())
            .map(|error| error as &(dyn StdError + 'static))
    }
}

/// A commit projector's own failure: names the projector, keeps its error as the source.
#[derive(Debug)]
struct CommitProjectorFailed {
    projector: String,
    source: BoxError,
}

impl fmt::Display for CommitProjectorFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "commit projector {:?}: {}", self.projector, self.source)
    }
}

impl StdError for CommitProjectorFailed {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(self.source.as_ref())
    }
}

/// The entity types a store (or transaction) has already durably declared history-dependent, so it
/// declares each once. Clones of a store share it.
pub(crate) type HistoryLedger = Arc<Mutex<HashSet<EntityType>>>;

/// Readies a repository for the commit-wide projectors before a commit that runs them: it must
/// write view operations ([`Repository::writes_views`]), and the entity types the projectors read
/// events of are durably added to the feature manifest's `historyDependent` set (once each), so
/// `compact_below` refuses them in every process. It does nothing when there are no projectors.
pub(crate) async fn ready_projectors<R: Repository>(
    repository: &R,
    projectors: &[Arc<dyn CommitProjector>],
    declared: &HistoryLedger,
) -> Result<(), Error> {
    if projectors.is_empty() {
        return Ok(());
    }

    if !repository.writes_views() {
        return Err(Error::ViewsUnsupported {
            repository: std::any::type_name::<R>().to_owned(),
        });
    }

    let pending: Vec<EntityType> = {
        let done = declared
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut pending: Vec<EntityType> = Vec::new();
        for entity_type in projectors.iter().flat_map(|projector| projector.history()) {
            if !done.contains(entity_type) && !pending.contains(entity_type) {
                pending.push(entity_type.clone());
            }
        }

        pending
    };

    if pending.is_empty() {
        return Ok(());
    }

    repository.declare_history_dependent(&pending).await?;

    declared
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .extend(pending);

    Ok(())
}

/// Runs the projectors over a commit view, in declaration order, and returns their operations with
/// provenance in the order the commit writes them. A view with no events runs no projector.
///
/// What a projector sees depends on its kind (see [`CommitProjector`]): a current-state projector
/// (empty [`history`](CommitProjector::history)) gets an empty commit ID and no events; a
/// history-dependent one gets the commit ID and the events and state of the entity types it names,
/// and nothing of any other stream. Every operation of every projector must then be owned by a
/// stream the commit wrote (checked against the whole view), and a history-dependent projector's
/// by a stream of a type it names; otherwise the result is [`Error::InvalidViewOperation`]. A
/// projector's error is an [`Error::Projector`] naming it (an internal failure: nothing is
/// written, nothing is retried); the combined operations are then validated with [`check_views`].
pub fn project_commit(
    projectors: &[Arc<dyn CommitProjector>],
    view: &CommitView,
) -> Result<Vec<ProjectedView>, Error> {
    if projectors.is_empty() || view.event_count() == 0 {
        return Ok(Vec::new());
    }

    let mut projected = Vec::new();
    for (index, projector) in projectors.iter().enumerate() {
        let history = projector.history();
        let operations = projector
            .project(&view.visible_to(history))
            .map_err(|source| {
                Error::Projector(Box::new(CommitProjectorFailed {
                    projector: projector.name().to_owned(),
                    source,
                }))
            })?;

        for (operation, op) in operations.into_iter().enumerate() {
            let item = ProjectedView {
                projector: projector.name().to_owned(),
                projector_index: index,
                operation,
                op,
            };

            check_owner(&item, view, history)?;
            projected.push(item);
        }
    }

    check_views(&projected)?;

    Ok(projected)
}

/// Validates a commit's view operations before any write: each, in order, must be valid (non-empty
/// keys, text that is UTF-8 without U+0000, a condition its kind allows), then within the key
/// limits, then not repeat an earlier operation's key. The first failure is returned as
/// [`Error::InvalidViewOperation`], [`Error::ViewKeyTooLong`], or [`Error::DuplicateViewKey`].
/// Repositories call it on every commit path, and the engine calls it when it projects.
pub fn check_views(views: &[ProjectedView]) -> Result<(), Error> {
    let mut seen: HashMap<(&str, &str), (&str, usize)> = HashMap::with_capacity(views.len());

    for view in views {
        let op = &view.op;

        if let Some(reason) = invalid_reason(op) {
            return Err(Error::InvalidViewOperation(format!(
                "projector {:?} (#{}) operation {}: {reason}",
                view.projector, view.projector_index, view.operation
            )));
        }

        for (field, length, max) in [
            ("pk", op.pk.len(), MAX_VIEW_PK_BYTES),
            ("sk", op.sk.len(), MAX_VIEW_SK_BYTES),
        ] {
            if length > max {
                return Err(Error::ViewKeyTooLong {
                    field,
                    length,
                    max,
                    projector: view.projector.clone(),
                    operation: view.operation,
                });
            }
        }

        if let Some((projector, operation)) =
            seen.insert((&op.pk, &op.sk), (&view.projector, view.operation))
        {
            return Err(Error::DuplicateViewKey {
                pk: op.pk.clone(),
                sk: op.sk.clone(),
                first: (projector.to_owned(), operation),
                second: (view.projector.clone(), view.operation),
            });
        }
    }

    Ok(())
}

/// Why an operation is invalid, or `None`.
fn invalid_reason(op: &ViewOperation) -> Option<String> {
    match (op.kind, op.condition) {
        (ViewOperationKind::Put, ViewCondition::Present) => {
            return Some("a put cannot carry the \"present\" condition".into());
        }
        (ViewOperationKind::Delete, ViewCondition::Absent) => {
            return Some("a delete cannot carry the \"absent\" condition".into());
        }
        _ => {}
    }

    if op.pk.is_empty() {
        return Some("pk is empty".into());
    }
    if op.sk.is_empty() {
        return Some("sk is empty".into());
    }
    if op.owner.is_empty() {
        return Some("owner is empty".into());
    }

    let text = |name: &'static str, value: &str| {
        value
            .contains('\0')
            .then(|| format!("{name} contains U+0000"))
    };
    if let Some(reason) = text("pk", &op.pk)
        .or_else(|| text("sk", &op.sk))
        .or_else(|| text("owner", op.owner.as_str()))
    {
        return Some(reason);
    }

    if op.kind == ViewOperationKind::Put {
        return text("entityType", &op.entity_type).or_else(|| {
            match std::str::from_utf8(&op.payload) {
                Err(_) => Some("payload is not valid UTF-8".into()),
                Ok(payload) => text("payload", payload),
            }
        });
    }

    None
}

/// Regroups stored events into the commits that wrote them, the way a rebuild replays a
/// history-dependent projector (storage spec, "Rebuilding a history-dependent projector"): events
/// sharing a commit ID form one [`CommitView`], its streams ascending by entity-ID bytes and each
/// stream's events by sequence. An event with an empty commit ID (written before v0.9) is a
/// single-event commit. `state` is left empty: the caller folds each stream's state after the
/// commit and fills it in before running a projector that reads it.
///
/// The views come back in an order every stream agrees with: a commit that wrote a stream's
/// sequence 3 precedes the commit that wrote its sequence 4, for every stream. That is the only
/// commit order the log keeps (commit IDs and timestamps are not causal), and it is the order of
/// every write to a row owned by one stream ([`ViewOperation::owner`]), so replaying in it
/// reproduces the rows of a history-dependent projector. Commits no stream orders relative to each
/// other come in a fixed arbitrary order, by their first event's (entity-ID bytes, sequence); a
/// projector whose rows depend on that order shares a row between owners, which [`replay_views`]
/// refuses. It returns [`Error::CommitOrder`] when no such order exists.
pub fn group_commits(
    events: impl IntoIterator<Item = SerializedEvent>,
) -> Result<Vec<CommitView>, Error> {
    let mut sorted: Vec<SerializedEvent> = events.into_iter().collect();
    sorted.sort_by(compare_events);

    let mut groups: Vec<Vec<SerializedEvent>> = Vec::new();
    let mut by_commit: BTreeMap<String, usize> = BTreeMap::new();
    let mut members: Vec<usize> = Vec::with_capacity(sorted.len());

    for event in &sorted {
        if event.metadata.commit_id.is_empty() {
            members.push(groups.len());
            groups.push(vec![event.clone()]);

            continue;
        }

        let index = *by_commit
            .entry(event.metadata.commit_id.clone())
            .or_insert_with(|| {
                groups.push(Vec::new());

                groups.len() - 1
            });
        members.push(index);
        groups[index].push(event.clone());
    }

    // Each stream orders the commits that wrote it: an edge from one commit to the next.
    let mut successors: Vec<Vec<usize>> = vec![Vec::new(); groups.len()];
    let mut waiting: Vec<usize> = vec![0; groups.len()];

    for i in 1..sorted.len() {
        let (previous, current) = (members[i - 1], members[i]);
        if sorted[i - 1].entity_id != sorted[i].entity_id || previous == current {
            continue;
        }

        successors[previous].push(current);
        waiting[current] += 1;
    }

    let key = |group: usize| {
        let first = &groups[group][0];

        (first.entity_id.as_str().as_bytes().to_vec(), first.sequence)
    };

    let mut ready: BinaryHeap<Reverse<(CommitKey, usize)>> = BinaryHeap::new();
    for (group, count) in waiting.iter().enumerate() {
        if *count == 0 {
            ready.push(Reverse((key(group), group)));
        }
    }

    let mut order = Vec::with_capacity(groups.len());
    while let Some(Reverse((_, group))) = ready.pop() {
        order.push(group);

        for &successor in &successors[group] {
            waiting[successor] -= 1;
            if waiting[successor] == 0 {
                ready.push(Reverse((key(successor), successor)));
            }
        }
    }

    if order.len() < groups.len() {
        let mut commits: Vec<String> = (0..groups.len())
            .filter(|&group| waiting[group] > 0)
            .map(|group| groups[group][0].metadata.commit_id.clone())
            .collect();
        commits.sort();

        return Err(Error::CommitOrder { commits });
    }

    let mut taken: Vec<Option<Vec<SerializedEvent>>> = groups.into_iter().map(Some).collect();

    Ok(order
        .into_iter()
        .filter_map(|group| taken[group].take())
        .map(commit_view_of)
        .collect())
}

/// A commit's place among commits no stream relates: its first event's (entity-ID bytes, sequence).
type CommitKey = (Vec<u8>, EventSequence);

fn compare_events(a: &SerializedEvent, b: &SerializedEvent) -> Ordering {
    a.entity_id
        .as_str()
        .as_bytes()
        .cmp(b.entity_id.as_str().as_bytes())
        .then_with(|| a.sequence.cmp(&b.sequence))
}

/// A rebuild of history-dependent projectors in memory: runs the projectors over the commits, in
/// the given order ([`group_commits`]' order, with each stream's state after its commit in
/// [`CommitStream::state`]), applies their operations to an empty set of rows, and returns the rows
/// ordered by `pk` then `sk` bytes. Conditions are not evaluated: a replay applies what the live
/// commits wrote. It returns [`Error::SharedViewKey`] when two operations of different owner
/// streams touch one row, because the final row would then depend on an order the log does not
/// keep.
pub fn replay_views(
    projectors: &[Arc<dyn CommitProjector>],
    commits: &[CommitView],
) -> Result<Vec<ViewRow>, Error> {
    let mut rows: BTreeMap<(Vec<u8>, Vec<u8>), ViewRow> = BTreeMap::new();
    let mut owners: HashMap<(String, String), EntityId> = HashMap::new();

    for commit in commits {
        for item in project_commit(projectors, commit)? {
            let op = item.op;
            let target = (op.pk.clone(), op.sk.clone());

            match owners.get(&target) {
                Some(first) if *first != op.owner => {
                    return Err(Error::SharedViewKey {
                        pk: target.0,
                        sk: target.1,
                        first: first.to_string(),
                        second: op.owner.to_string(),
                    });
                }
                _ => {
                    owners.insert(target.clone(), op.owner.clone());
                }
            }

            let key = (op.pk.as_bytes().to_vec(), op.sk.as_bytes().to_vec());
            match op.kind {
                ViewOperationKind::Delete => {
                    rows.remove(&key);
                }
                ViewOperationKind::Put => {
                    rows.insert(
                        key,
                        ViewRow::new(
                            op.pk,
                            op.sk,
                            op.owner.to_string(),
                            op.entity_type,
                            op.payload,
                        ),
                    );
                }
            }
        }
    }

    Ok(rows.into_values().collect())
}

/// Requires an operation to be owned by a stream the commit wrote, and, for a history-dependent
/// projector, by a stream of a type it names. The owner is looked up in the whole commit view, not
/// in the view the projector was given.
fn check_owner(
    view: &ProjectedView,
    commit: &CommitView,
    history: &[EntityType],
) -> Result<(), Error> {
    let invalid = |reason: String| {
        Err(Error::InvalidViewOperation(format!(
            "projector {:?} (#{}) operation {}: {reason}",
            view.projector, view.projector_index, view.operation
        )))
    };

    let owner = &view.op.owner;
    let Some(stream) = commit
        .streams
        .iter()
        .find(|stream| &stream.entity_id == owner)
    else {
        return invalid(format!(
            "the owner stream {owner:?} was not written by this commit"
        ));
    };

    if !history.is_empty() && !history.contains(&stream.entity_type) {
        return invalid(format!(
            "the owner stream {owner:?} is of type {:?}, which the projector does not declare in \
             its history",
            stream.entity_type.as_str()
        ));
    }

    Ok(())
}

/// The view of one commit's events, already in (entity-ID bytes, sequence) order.
fn commit_view_of(events: Vec<SerializedEvent>) -> CommitView {
    let commit_id = events
        .first()
        .map(|event| event.metadata.commit_id.clone())
        .unwrap_or_default();

    let mut streams: Vec<CommitStream> = Vec::new();
    for event in events {
        match streams.last_mut() {
            Some(stream) if stream.entity_id == event.entity_id => stream.events.push(event),
            _ => streams.push(CommitStream {
                entity_id: event.entity_id.clone(),
                entity_type: event.entity_type.clone(),
                events: vec![event],
                state: Vec::new(),
            }),
        }
    }

    CommitView { commit_id, streams }
}

#[cfg(test)]
mod tests {
    use std::fmt;
    use std::sync::Mutex;

    use super::*;
    use crate::ids::EventSequence;
    use crate::metadata::Metadata;

    #[derive(Debug)]
    struct Taken;

    impl fmt::Display for Taken {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("name taken")
        }
    }

    impl StdError for Taken {}

    fn event(entity: &str, sequence: i64, commit: &str) -> SerializedEvent {
        SerializedEvent {
            id: format!("{entity}:{sequence}").into(),
            sequence: EventSequence(sequence),
            event_type: "t:happened".into(),
            version: crate::EventVersion(1),
            entity_id: entity.into(),
            entity_type: "t".into(),
            payload: b"{}".to_vec(),
            metadata: Metadata {
                commit_id: commit.to_owned(),
                ..Metadata::default()
            },
        }
    }

    #[test]
    fn a_live_commit_view_carries_the_effective_causation_a_stored_event_has() {
        use crate::CommandId;

        let mut live = event("a", 1, "c");
        live.metadata.command_id = Some(CommandId::new("cmd-1"));
        assert!(live.metadata.causation_id.is_none());

        let stream = CommitStream::new("a", "t", vec![live], Vec::new());

        assert_eq!(
            stream.events[0].metadata.causation_id,
            Some(CommandId::new("cmd-1")),
            "a stored event carries the command ID as its causation"
        );
    }

    struct Fixed {
        name: &'static str,
        operations: Vec<ViewOperation>,
    }

    impl CommitProjector for Fixed {
        fn name(&self) -> &str {
            self.name
        }

        fn project(&self, _: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
            Ok(self.operations.clone())
        }
    }

    struct Failing;

    impl CommitProjector for Failing {
        fn name(&self) -> &str {
            "failing"
        }

        fn project(&self, _: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
            Err(Box::new(Taken))
        }
    }

    fn view_of(events: Vec<SerializedEvent>) -> CommitView {
        group_commits(events).unwrap().remove(0)
    }

    fn projected(projector: &str, operation: usize, op: ViewOperation) -> ProjectedView {
        ProjectedView::new(projector, 0, operation, op)
    }

    #[test]
    fn builders_set_the_kind_condition_and_mapped_error() {
        let put = ViewOperation::put("p", "s", "id", "type", "{}")
            .if_absent()
            .on_conflict(Taken);
        assert_eq!(put.kind, ViewOperationKind::Put);
        assert_eq!(put.condition, ViewCondition::Absent);
        assert_eq!(put.conflict.as_ref().unwrap().to_string(), "name taken");
        assert_eq!(put.payload, b"{}");

        let delete = ViewOperation::delete("p", "s", "a").if_present();
        assert_eq!(delete.kind, ViewOperationKind::Delete);
        assert_eq!(delete.condition, ViewCondition::Present);
        assert!(delete.conflict.is_none());

        // Clone keeps the mapped error, and equality compares it by message.
        assert_eq!(put.clone(), put);
        assert_ne!(
            put.clone(),
            ViewOperation::put("p", "s", "id", "type", "{}").if_absent()
        );
    }

    #[test]
    fn project_commit_tags_provenance_in_declaration_then_emission_order() {
        let projectors: Vec<Arc<dyn CommitProjector>> = vec![
            Arc::new(Fixed {
                name: "first",
                operations: vec![
                    ViewOperation::put("a", "1", "a", "", "{}"),
                    ViewOperation::delete("a", "2", "a"),
                ],
            }),
            Arc::new(Fixed {
                name: "second",
                operations: vec![ViewOperation::put("b", "1", "a", "", "{}")],
            }),
        ];

        let views = project_commit(&projectors, &view_of(vec![event("a", 1, "c1")])).unwrap();

        let provenance: Vec<_> = views
            .iter()
            .map(|view| {
                (
                    view.projector.as_str(),
                    view.projector_index,
                    view.operation,
                )
            })
            .collect();
        assert_eq!(
            provenance,
            [("first", 0, 0), ("first", 0, 1), ("second", 1, 0)]
        );
    }

    #[test]
    fn project_commit_runs_nothing_for_a_view_without_events() {
        let projectors: Vec<Arc<dyn CommitProjector>> = vec![Arc::new(Failing)];

        assert!(
            project_commit(&projectors, &CommitView::new("c", Vec::new()))
                .unwrap()
                .is_empty()
        );

        let empty_stream = CommitView::new(
            "c",
            vec![CommitStream::new("a", "t", Vec::new(), Vec::new())],
        );
        assert!(
            project_commit(&projectors, &empty_stream)
                .unwrap()
                .is_empty()
        );
        assert!(
            project_commit(&[], &view_of(vec![event("a", 1, "c")]))
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn a_projector_error_names_the_projector_and_is_not_retryable() {
        let projectors: Vec<Arc<dyn CommitProjector>> = vec![Arc::new(Failing)];

        let error = project_commit(&projectors, &view_of(vec![event("a", 1, "c")])).unwrap_err();

        assert!(matches!(error, Error::Projector(_)), "{error:?}");
        assert!(error.to_string().contains("\"failing\""), "{error}");
        assert!(error.to_string().contains("name taken"), "{error}");
        assert!(!error.is_retryable());
        assert!(!error.is_conflict());
    }

    #[test]
    fn project_commit_checks_the_combined_operations() {
        let projectors: Vec<Arc<dyn CommitProjector>> = vec![
            Arc::new(Fixed {
                name: "one",
                operations: vec![ViewOperation::put("p", "s", "a", "", "{}")],
            }),
            Arc::new(Fixed {
                name: "two",
                operations: vec![ViewOperation::delete("p", "s", "a")],
            }),
        ];

        let error = project_commit(&projectors, &view_of(vec![event("a", 1, "c")])).unwrap_err();

        match error {
            Error::DuplicateViewKey {
                pk,
                sk,
                first,
                second,
            } => {
                assert_eq!((pk.as_str(), sk.as_str()), ("p", "s"));
                assert_eq!(first, ("one".to_owned(), 0));
                assert_eq!(second, ("two".to_owned(), 0));
            }
            other => panic!("expected a duplicate key, got {other:?}"),
        }
    }

    #[test]
    fn check_views_refuses_invalid_operations_first() {
        let invalid = |op: ViewOperation| match check_views(&[projected("p", 3, op)]) {
            Err(Error::InvalidViewOperation(message)) => message,
            other => panic!("expected an invalid operation, got {other:?}"),
        };

        assert!(invalid(ViewOperation::put("", "s", "a", "", "{}")).contains("pk is empty"));
        assert!(invalid(ViewOperation::put("p", "", "a", "", "{}")).contains("sk is empty"));
        assert!(invalid(ViewOperation::delete("", "s", "a")).contains("pk is empty"));
        assert!(
            invalid(ViewOperation::put("a\0b", "s", "a", "", "{}")).contains("pk contains U+0000")
        );
        assert!(
            invalid(ViewOperation::put("p", "a\0", "a", "", "{}")).contains("sk contains U+0000")
        );
        assert!(invalid(ViewOperation::put("p", "s", "", "", "{}")).contains("owner is empty"));
        assert!(invalid(ViewOperation::delete("p", "s", "")).contains("owner is empty"));
        assert!(
            invalid(ViewOperation::put("p", "s", "i\0", "", "{}"))
                .contains("owner contains U+0000")
        );
        assert!(
            invalid(ViewOperation::delete("p", "s", "i\0")).contains("owner contains U+0000"),
            "a delete's owner is text-checked too"
        );
        assert!(invalid(ViewOperation::put("p", "s", "a", "t\0", "{}")).contains("entityType"));
        assert!(
            invalid(ViewOperation::put("p", "s", "a", "", "{\0}")).contains("payload contains")
        );
        assert!(
            invalid(ViewOperation::put("p", "s", "a", "", vec![0xff, 0xfe]))
                .contains("payload is not valid UTF-8")
        );
        assert!(
            invalid(ViewOperation::put("p", "s", "a", "", "{}").if_present()).contains("present")
        );
        assert!(invalid(ViewOperation::delete("p", "s", "a").if_absent()).contains("absent"));

        // The message names the projector and the operation.
        let message = invalid(ViewOperation::put("", "s", "a", "", "{}"));
        assert!(
            message.contains("\"p\"") && message.contains("operation 3"),
            "{message}"
        );

        // A delete carries no entity type or payload, so only its keys and owner are checked.
        assert!(
            check_views(&[projected(
                "p",
                0,
                ViewOperation::delete("p", "s", "a").if_present()
            )])
            .is_ok()
        );
    }

    #[test]
    fn check_views_bounds_keys_in_utf8_bytes() {
        let key_error = |op: ViewOperation| match check_views(&[projected("p", 1, op)]) {
            Err(Error::ViewKeyTooLong {
                field,
                length,
                max,
                projector,
                operation,
            }) => (field, length, max, projector, operation),
            other => panic!("expected a key length error, got {other:?}"),
        };

        // Exactly at the limits passes.
        let at_limit = ViewOperation::put("p".repeat(2048), "s".repeat(1024), "a", "", "{}");
        assert!(check_views(&[projected("p", 0, at_limit)]).is_ok());

        assert_eq!(
            key_error(ViewOperation::put("p".repeat(2049), "s", "a", "", "{}")),
            ("pk", 2049, 2048, "p".to_owned(), 1)
        );
        assert_eq!(
            key_error(ViewOperation::delete("p", "s".repeat(1025), "a")),
            ("sk", 1025, 1024, "p".to_owned(), 1)
        );

        // Bytes, not characters: 1025 two-byte characters are 2050 bytes.
        let wide = "é".repeat(1025);
        assert_eq!(key_error(ViewOperation::delete(wide, "s", "a")).1, 2050);
        let fits = "é".repeat(1024);
        assert!(check_views(&[projected("p", 0, ViewOperation::delete(fits, "s", "a"))]).is_ok());
    }

    #[test]
    fn check_views_reports_the_first_failure_in_operation_order() {
        // Operation 0 is a duplicate-free key too long; operation 1 is invalid. The first
        // operation's failure wins even though a later one fails an earlier check.
        let views = [
            projected(
                "p",
                0,
                ViewOperation::put("p".repeat(2049), "s", "a", "", "{}"),
            ),
            projected("p", 1, ViewOperation::put("", "s", "a", "", "{}")),
        ];
        assert!(matches!(
            check_views(&views),
            Err(Error::ViewKeyTooLong { operation: 0, .. })
        ));

        // A duplicate is reported on the second operation, after its own validity checks.
        let views = [
            projected("p", 0, ViewOperation::put("p", "s", "a", "", "{}")),
            projected("p", 1, ViewOperation::put("p", "s", "a", "", "{}")),
        ];
        match check_views(&views) {
            Err(Error::DuplicateViewKey { first, second, .. }) => {
                assert_eq!(first, ("p".to_owned(), 0));
                assert_eq!(second, ("p".to_owned(), 1));
            }
            other => panic!("expected a duplicate key, got {other:?}"),
        }

        // Keys that differ in either part are distinct rows.
        let views = [
            projected("p", 0, ViewOperation::put("p", "s", "a", "", "{}")),
            projected("p", 1, ViewOperation::put("p", "t", "a", "", "{}")),
            projected("p", 2, ViewOperation::put("q", "s", "a", "", "{}")),
        ];
        assert!(check_views(&views).is_ok());
    }

    #[test]
    fn view_operation_sizes_count_names_values_and_the_owner_condition() {
        // "pk"+"p"+"sk"+"s"+"entityID"+"id"+"entityType"+"ty"+"payload"+"{}". The entityID is the
        // owner.
        let put = ViewOperation::put("p", "s", "id", "ty", "{}");
        assert_eq!(
            view_operation_size(&put),
            2 + 1 + 2 + 1 + 8 + 2 + 10 + 2 + 7 + 2
        );
        // A put's condition is not budgeted, like every put's.
        assert_eq!(
            view_operation_size(&put.clone().if_absent()),
            view_operation_size(&put)
        );

        // A delete counts its key, its condition expression, and the owner value it binds.
        let delete = ViewOperation::delete("p", "s", "id");
        let owner_value = ":owner".len() + "id".len();
        assert_eq!(
            view_operation_size(&delete),
            2 + 1 + 2 + 1 + "attribute_not_exists(pk) OR entityID = :owner".len() + owner_value
        );
        assert_eq!(
            view_operation_size(&delete.if_present()),
            2 + 1 + 2 + 1 + "attribute_exists(pk) AND entityID = :owner".len() + owner_value
        );
    }

    #[test]
    fn group_commits_orders_streams_and_events_and_splits_pre_v09_events() {
        let events = vec![
            event("b", 2, "c2"),
            event("a", 2, "c1"),
            event("b", 1, "c1"),
            event("a", 1, "c1"),
            event("z", 1, ""),
            event("a", 3, ""),
        ];

        let views = group_commits(events).unwrap();

        // Ordered by each group's first event in (stream bytes, sequence) order.
        let shape: Vec<_> = views
            .iter()
            .map(|view| {
                (
                    view.commit_id.as_str(),
                    view.streams
                        .iter()
                        .map(|stream| {
                            (
                                stream.entity_id.as_str(),
                                stream
                                    .events
                                    .iter()
                                    .map(|event| event.sequence.0)
                                    .collect::<Vec<_>>(),
                            )
                        })
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        assert_eq!(
            shape,
            [
                ("c1", vec![("a", vec![1, 2]), ("b", vec![1])]),
                ("", vec![("a", vec![3])]),
                ("c2", vec![("b", vec![2])]),
                ("", vec![("z", vec![1])]),
            ]
        );

        // The state is left for the caller to fold.
        assert!(
            views
                .iter()
                .flat_map(|view| &view.streams)
                .all(|stream| stream.state.is_empty())
        );
        assert!(group_commits(Vec::new()).unwrap().is_empty());
    }

    #[test]
    fn group_commits_orders_commits_by_each_streams_sequences_not_by_entity_id() {
        // Commit x wrote z:1. Commit y wrote z:2 and a:1. Commit w wrote a:2. Sorted by first
        // event alone, y (a:1) would replay before x (z:1), though z orders x first.
        let views = group_commits(vec![
            event("z", 1, "x"),
            event("z", 2, "y"),
            event("a", 1, "y"),
            event("a", 2, "w"),
        ])
        .unwrap();

        let order: Vec<_> = views.iter().map(|view| view.commit_id.as_str()).collect();
        assert_eq!(
            order,
            ["x", "y", "w"],
            "z orders x before y, and a orders y before w"
        );

        // Commits no stream relates come in a fixed order, by their first event.
        let views = group_commits(vec![event("m", 1, "late"), event("b", 1, "early")]).unwrap();
        assert_eq!(views[0].commit_id, "early");
        assert_eq!(views[1].commit_id, "late");
    }

    #[test]
    fn group_commits_refuses_events_no_order_fits() {
        // c1 wrote a:1 and b:2; c2 wrote a:2 and b:1: a puts c1 first, b puts c2 first.
        let error = group_commits(vec![
            event("a", 1, "c1"),
            event("a", 2, "c2"),
            event("b", 1, "c2"),
            event("b", 2, "c1"),
        ])
        .unwrap_err();

        assert!(
            matches!(&error, Error::CommitOrder { commits } if commits == &["c1", "c2"]),
            "{error:?}"
        );
    }

    fn reading(
        history: &[&str],
        script: impl Fn(&CommitView) -> Vec<ViewOperation> + Send + Sync + 'static,
    ) -> Arc<dyn CommitProjector> {
        type Script = Box<dyn Fn(&CommitView) -> Vec<ViewOperation> + Send + Sync>;

        struct Reading(Vec<EntityType>, Script);

        impl CommitProjector for Reading {
            fn name(&self) -> &str {
                "reading"
            }

            fn history(&self) -> &[EntityType] {
                &self.0
            }

            fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
                Ok((self.1)(commit))
            }
        }

        Arc::new(Reading(
            history
                .iter()
                .map(|entity_type| EntityType::new(*entity_type))
                .collect(),
            Box::new(script),
        ))
    }

    fn row_for(commit_id: &str, owner: &str) -> ViewOperation {
        ViewOperation::put("shared", "row", owner, "t", format!("\"{commit_id}\""))
    }

    #[test]
    fn a_current_state_projector_sees_no_events_and_a_history_one_only_its_types() {
        let mut view = view_of(vec![event("a", 1, "c"), event("b", 1, "c")]);
        view.streams[0].state = b"{\"a\":1}".to_vec();
        view.streams[1].state = b"{\"b\":1}".to_vec();
        view.streams[1].entity_type = EntityType::new("other");

        type Seen = (&'static str, String, Vec<(usize, usize)>);
        let seen: Arc<Mutex<Vec<Seen>>> = Arc::new(Mutex::new(Vec::new()));
        let record = |name: &'static str, history: &'static [&'static str]| {
            let seen = seen.clone();

            reading(history, move |commit| {
                seen.lock().unwrap().push((
                    name,
                    commit.commit_id.clone(),
                    commit
                        .streams
                        .iter()
                        .map(|stream| (stream.events.len(), stream.state.len()))
                        .collect(),
                ));

                Vec::new()
            })
        };

        project_commit(&[record("current", &[]), record("reads", &["t"])], &view).unwrap();

        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen[0],
            ("current", String::new(), vec![(0, 7), (0, 7)]),
            "no commit ID, no events, every state"
        );
        assert_eq!(
            seen[1],
            ("reads", "c".to_owned(), vec![(1, 7), (0, 0)]),
            "the commit ID, its type's events and state, and nothing of the other type"
        );
        assert_eq!(view.commit_id, "c", "the caller's view is unchanged");
        assert_eq!(view.streams[1].events.len(), 1);
        assert_eq!(view.streams[1].state.len(), 7);
    }

    #[test]
    fn every_operation_names_an_owner_the_commit_wrote() {
        let mut view = view_of(vec![event("a", 1, "c"), event("b", 1, "c")]);
        view.streams[1].entity_type = EntityType::new("other");

        let run = |history: &[&str], op: ViewOperation| {
            project_commit(&[reading(history, move |_| vec![op.clone()])], &view)
        };
        let put = |owner: &str| ViewOperation::put("pk", "sk", owner, "", "{}");

        for history in [&[][..], &["t"][..]] {
            assert!(
                run(history, put("a")).is_ok(),
                "{history:?}: a written stream"
            );
            assert!(
                matches!(run(history, put("elsewhere")), Err(Error::InvalidViewOperation(m)) if m.contains("not written")),
                "{history:?}: a stream the commit did not write"
            );
            assert!(
                matches!(run(history, put("")), Err(Error::InvalidViewOperation(_))),
                "{history:?}: no owner"
            );
            assert!(
                matches!(
                    run(history, ViewOperation::delete("pk", "sk", "elsewhere")),
                    Err(Error::InvalidViewOperation(_))
                ),
                "{history:?}: a delete is owned too"
            );
        }

        // A current-state projector may own a row by a stream of any type; a history-dependent one
        // only by a stream of a type it declares. The owner is looked up in the whole commit, so a
        // stream whose events the projector cannot see is still a stream the commit wrote.
        assert!(run(&[], put("b")).is_ok());
        assert!(matches!(
            run(&["t"], put("b")),
            Err(Error::InvalidViewOperation(m)) if m.contains("history")
        ));
    }

    #[test]
    fn the_owner_error_names_the_projector_and_the_operation() {
        let view = view_of(vec![event("a", 1, "c")]);
        let projector = reading(&[], |_| {
            vec![
                ViewOperation::put("pk", "one", "a", "", "{}"),
                ViewOperation::put("pk", "two", "elsewhere", "", "{}"),
            ]
        });

        match project_commit(&[projector], &view) {
            Err(Error::InvalidViewOperation(message)) => {
                assert!(
                    message.contains("\"reading\"") && message.contains("operation 1"),
                    "{message}"
                );
            }
            other => panic!("expected an invalid operation, got {other:?}"),
        }
    }

    #[test]
    fn replay_reproduces_the_live_row_when_one_stream_owns_it() {
        // The row belongs to z. Commit x wrote z:1; commit y wrote z:2 and a:1. Live, y wrote
        // last. The old order (by first event) replayed y before x and kept x's value.
        let groups = group_commits(vec![
            event("z", 1, "x"),
            event("z", 2, "y"),
            event("a", 1, "y"),
        ])
        .unwrap();
        let projector = reading(&["t"], |commit| vec![row_for(&commit.commit_id, "z")]);

        let rows = replay_views(&[projector], &groups).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].payload, b"\"y\"",
            "the later commit of the owner stream wins"
        );
        assert_eq!(rows[0].entity_id, "z", "the stored entityID is the owner");
    }

    #[test]
    fn replay_refuses_a_row_two_streams_write() {
        // Commit x (stream z) puts the shared row; the later commit y (stream a) overwrites it. The
        // log keeps no order between z and a, and entity-ID order would replay y first.
        let groups = group_commits(vec![event("z", 1, "x"), event("a", 1, "y")]).unwrap();
        let projector = reading(&["t"], |commit| {
            vec![row_for(
                &commit.commit_id,
                commit.streams[0].entity_id.as_str(),
            )]
        });

        let error = replay_views(&[projector], &groups).unwrap_err();

        assert!(
            matches!(&error, Error::SharedViewKey { pk, sk, .. } if pk == "shared" && sk == "row"),
            "{error:?}"
        );
    }

    #[test]
    fn replay_refuses_a_row_two_streams_write_even_for_a_delete() {
        let groups = group_commits(vec![event("z", 1, "x"), event("a", 1, "y")]).unwrap();
        let projector = reading(&[], |commit| {
            let owner = commit.streams[0].entity_id.as_str();

            if owner == "z" {
                vec![ViewOperation::put("pk", "sk", owner, "", "{}")]
            } else {
                vec![ViewOperation::delete("pk", "sk", owner)]
            }
        });

        assert!(matches!(
            replay_views(&[projector], &groups),
            Err(Error::SharedViewKey { .. })
        ));
    }

    #[test]
    fn replay_applies_deletes_and_sorts_the_rows() {
        let groups = group_commits(vec![event("a", 1, "c1"), event("a", 2, "c2")]).unwrap();
        let projector = reading(&["t"], |commit| {
            if commit.commit_id == "c2" {
                vec![ViewOperation::delete("pk", "gone", "a")]
            } else {
                vec![
                    ViewOperation::put("pk", "kept", "a", "", "{}"),
                    ViewOperation::put("pk", "gone", "a", "", "{}"),
                ]
            }
        });

        let rows = replay_views(&[projector], &groups).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].sk, "kept");
    }

    #[test]
    fn group_commits_orders_stream_ids_by_bytes() {
        // "B" (0x42) sorts before "a" (0x61) bytewise, unlike a locale-aware comparison.
        let views = group_commits(vec![event("a", 1, "c"), event("B", 1, "c")]).unwrap();

        let ids: Vec<_> = views[0]
            .streams
            .iter()
            .map(|stream| stream.entity_id.as_str())
            .collect();
        assert_eq!(ids, ["B", "a"]);
    }

    #[test]
    fn the_projector_operation_error_reports_the_mapped_error_as_its_source() {
        let view = projected(
            "claims",
            2,
            ViewOperation::put("p", "s", "a", "", "{}")
                .if_absent()
                .on_conflict(Taken),
        );

        let error = ProjectorOperationError::condition_failed(&view);

        assert_eq!(error.projector, "claims");
        assert_eq!(error.operation, 2);
        assert_eq!(error.reason, REASON_CONDITION_FAILED);
        assert_eq!(error.source().unwrap().to_string(), "name taken");
        assert_eq!(error.mapped().unwrap().to_string(), "name taken");
        assert!(error.cause().is_none());

        let wrapped = Error::ProjectorOperation(Box::new(error));
        assert!(!wrapped.is_retryable());
        assert!(!wrapped.is_conflict());
        // The wrapper forwards both the message and the source chain.
        assert_eq!(wrapped.source().unwrap().to_string(), "name taken");
        assert!(wrapped.to_string().contains("claims"), "{wrapped}");

        // Without a mapped error the backend's cause stands in; with neither, there is no source.
        let plain = projected("claims", 0, ViewOperation::delete("p", "s", "a"));
        assert!(
            ProjectorOperationError::condition_failed(&plain)
                .source()
                .is_none()
        );
        let caused = ProjectorOperationError::for_view(&plain, "23505", Some(Box::new(Taken)));
        assert_eq!(caused.reason, "23505");
        assert_eq!(caused.cause().unwrap().to_string(), "name taken");
        assert!(caused.mapped().is_none());
        assert_eq!(caused.source().unwrap().to_string(), "name taken");

        // The mapped error belongs to a failed condition only: another reason keeps the cause.
        let mapped = projected(
            "claims",
            0,
            ViewOperation::put("p", "s", "a", "", "{}").on_conflict(Taken),
        );
        let validation = ProjectorOperationError::for_view(&mapped, "ValidationError", None);
        assert!(validation.mapped().is_none() && validation.source().is_none());
    }
}
