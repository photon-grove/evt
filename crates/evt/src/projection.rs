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

use std::collections::{BTreeMap, HashMap};
use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use crate::error::{BoxError, Error};
use crate::event::SerializedEvent;
use crate::ids::{EntityId, EntityType};

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
    /// Informational and always written by a put; readers never interpret it. A row that belongs
    /// to no single entity repeats the commit's primary stream or any stable label. Empty for a
    /// delete.
    pub entity_id: String,
    /// Informational, like [`entity_id`](ViewOperation::entity_id).
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
        entity_id: impl Into<String>,
        entity_type: impl Into<String>,
        payload: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            kind: ViewOperationKind::Put,
            pk: pk.into(),
            sk: sk.into(),
            entity_id: entity_id.into(),
            entity_type: entity_type.into(),
            payload: payload.into(),
            condition: ViewCondition::None,
            conflict: None,
        }
    }

    /// An operation that deletes the row at `pk` and `sk`.
    pub fn delete(pk: impl Into<String>, sk: impl Into<String>) -> Self {
        Self {
            kind: ViewOperationKind::Delete,
            pk: pk.into(),
            sk: sk.into(),
            entity_id: String::new(),
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
            && self.entity_id == other.entity_id
            && self.entity_type == other.entity_type
            && self.payload == other.payload
            && self.condition == other.condition
            && self.conflict.as_ref().map(|error| error.to_string())
                == other.conflict.as_ref().map(|error| error.to_string())
    }
}

impl Eq for ViewOperation {}

/// The size of a view operation in the commit budget: a put counts its five attributes (name and
/// value bytes each), and a delete its two key attributes plus the condition expression when it
/// carries one. It equals the size of the DynamoDB action the repository builds, and the in-memory
/// repository budgets operations by it.
pub fn view_operation_size(operation: &ViewOperation) -> usize {
    let keys = "pk".len() + operation.pk.len() + "sk".len() + operation.sk.len();

    match operation.kind {
        ViewOperationKind::Delete => {
            if operation.condition == ViewCondition::Present {
                keys + crate::wire::dynamo::VIEW_PRESENT_CONDITION.len()
            } else {
                keys
            }
        }
        ViewOperationKind::Put => {
            keys + "entityID".len()
                + operation.entity_id.len()
                + "entityType".len()
                + operation.entity_type.len()
                + "payload".len()
                + operation.payload.len()
        }
    }
}

/// A view row as the in-memory repository stores it: the five attributes of a views-table item.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ViewRow {
    /// The partition key.
    pub pk: String,
    /// The sort key.
    pub sk: String,
    /// The informational entity ID.
    pub entity_id: String,
    /// The informational entity type.
    pub entity_type: String,
    /// The view state JSON.
    pub payload: Vec<u8>,
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
        }
    }

    /// The row a put operation writes.
    pub(crate) fn of(operation: &ViewOperation) -> Self {
        Self {
            pk: operation.pk.clone(),
            sk: operation.sk.clone(),
            entity_id: operation.entity_id.clone(),
            entity_type: operation.entity_type.clone(),
            payload: operation.payload.clone(),
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
}

/// A pure function over every event of a commit, emitting view operations that commit atomically
/// with it. It runs once per commit that writes at least one event, after every stream is staged,
/// and again on every fence-aware re-preparation, so it must be repeatable.
///
/// `project` must be deterministic: the operations may depend only on the commit view, never on a
/// clock, a random source, the environment, a store, or state remembered between calls, and must
/// not be ordered by a hash map's iteration. The operations it returns are appended to the commit
/// in the projector's declaration order, each projector's in emission order.
pub trait CommitProjector: Send + Sync {
    /// Identifies the projector in errors. It should be unique within a store.
    fn name(&self) -> &str;

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
    /// The view without its row data (informational attributes and payload), keeping what
    /// identifies it in an error: provenance, kind, keys, condition, and mapped error. Repositories
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
                entity_id: String::new(),
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

/// Runs the projectors over a commit view, in declaration order, and returns their operations with
/// provenance in the order the commit writes them. A view with no events runs no projector. A
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
        let operations = projector.project(view).map_err(|source| {
            Error::Projector(Box::new(CommitProjectorFailed {
                projector: projector.name().to_owned(),
                source,
            }))
        })?;

        for (operation, op) in operations.into_iter().enumerate() {
            projected.push(ProjectedView {
                projector: projector.name().to_owned(),
                projector_index: index,
                operation,
                op,
            });
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

    let text = |name: &'static str, value: &str| {
        value
            .contains('\0')
            .then(|| format!("{name} contains U+0000"))
    };
    if let Some(reason) = text("pk", &op.pk).or_else(|| text("sk", &op.sk)) {
        return Some(reason);
    }

    if op.kind == ViewOperationKind::Put {
        return text("entityID", &op.entity_id)
            .or_else(|| text("entityType", &op.entity_type))
            .or_else(|| match std::str::from_utf8(&op.payload) {
                Err(_) => Some("payload is not valid UTF-8".into()),
                Ok(payload) => text("payload", payload),
            });
    }

    None
}

/// Regroups stored events into the commits that wrote them, the way a rebuild replays a
/// history-dependent projector (storage spec, "Rebuilding a history-dependent projector"): events
/// sharing a commit ID form one [`CommitView`], its streams ascending by entity-ID bytes and each
/// stream's events by sequence. An event with an empty commit ID (written before v0.9) is a
/// single-event commit.
///
/// The returned views ascend by their first event in (entity-ID bytes, sequence) order; the order
/// carries no meaning, because a projector's output depends only on its own commit. `state` is left
/// empty: the caller folds each stream's state after the commit and fills it in before running a
/// projector that reads it.
pub fn group_commits(events: impl IntoIterator<Item = SerializedEvent>) -> Vec<CommitView> {
    let mut sorted: Vec<SerializedEvent> = events.into_iter().collect();
    sorted.sort_by(|a, b| {
        a.entity_id
            .as_str()
            .as_bytes()
            .cmp(b.entity_id.as_str().as_bytes())
            .then_with(|| a.sequence.cmp(&b.sequence))
    });

    let mut groups: Vec<Vec<SerializedEvent>> = Vec::new();
    let mut by_commit: BTreeMap<String, usize> = BTreeMap::new();

    for event in sorted {
        if event.metadata.commit_id.is_empty() {
            groups.push(vec![event]);

            continue;
        }

        let index = *by_commit
            .entry(event.metadata.commit_id.clone())
            .or_insert_with(|| {
                groups.push(Vec::new());

                groups.len() - 1
            });
        groups[index].push(event);
    }

    groups.into_iter().map(commit_view_of).collect()
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
        group_commits(events).remove(0)
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

        let delete = ViewOperation::delete("p", "s").if_present();
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
                    ViewOperation::put("a", "1", "", "", "{}"),
                    ViewOperation::delete("a", "2"),
                ],
            }),
            Arc::new(Fixed {
                name: "second",
                operations: vec![ViewOperation::put("b", "1", "", "", "{}")],
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
                operations: vec![ViewOperation::put("p", "s", "", "", "{}")],
            }),
            Arc::new(Fixed {
                name: "two",
                operations: vec![ViewOperation::delete("p", "s")],
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

        assert!(invalid(ViewOperation::put("", "s", "", "", "{}")).contains("pk is empty"));
        assert!(invalid(ViewOperation::put("p", "", "", "", "{}")).contains("sk is empty"));
        assert!(invalid(ViewOperation::delete("", "s")).contains("pk is empty"));
        assert!(
            invalid(ViewOperation::put("a\0b", "s", "", "", "{}")).contains("pk contains U+0000")
        );
        assert!(
            invalid(ViewOperation::put("p", "a\0", "", "", "{}")).contains("sk contains U+0000")
        );
        assert!(invalid(ViewOperation::put("p", "s", "i\0", "", "{}")).contains("entityID"));
        assert!(invalid(ViewOperation::put("p", "s", "", "t\0", "{}")).contains("entityType"));
        assert!(invalid(ViewOperation::put("p", "s", "", "", "{\0}")).contains("payload contains"));
        assert!(
            invalid(ViewOperation::put("p", "s", "", "", vec![0xff, 0xfe]))
                .contains("payload is not valid UTF-8")
        );
        assert!(
            invalid(ViewOperation::put("p", "s", "", "", "{}").if_present()).contains("present")
        );
        assert!(invalid(ViewOperation::delete("p", "s").if_absent()).contains("absent"));

        // The message names the projector and the operation.
        let message = invalid(ViewOperation::put("", "s", "", "", "{}"));
        assert!(
            message.contains("\"p\"") && message.contains("operation 3"),
            "{message}"
        );

        // A delete carries no entity or payload, so only its keys are text-checked.
        assert!(
            check_views(&[projected(
                "p",
                0,
                ViewOperation::delete("p", "s").if_present()
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
        let at_limit = ViewOperation::put("p".repeat(2048), "s".repeat(1024), "", "", "{}");
        assert!(check_views(&[projected("p", 0, at_limit)]).is_ok());

        assert_eq!(
            key_error(ViewOperation::put("p".repeat(2049), "s", "", "", "{}")),
            ("pk", 2049, 2048, "p".to_owned(), 1)
        );
        assert_eq!(
            key_error(ViewOperation::delete("p", "s".repeat(1025))),
            ("sk", 1025, 1024, "p".to_owned(), 1)
        );

        // Bytes, not characters: 1025 two-byte characters are 2050 bytes.
        let wide = "é".repeat(1025);
        assert_eq!(key_error(ViewOperation::delete(wide, "s")).1, 2050);
        let fits = "é".repeat(1024);
        assert!(check_views(&[projected("p", 0, ViewOperation::delete(fits, "s"))]).is_ok());
    }

    #[test]
    fn check_views_reports_the_first_failure_in_operation_order() {
        // Operation 0 is a duplicate-free key too long; operation 1 is invalid. The first
        // operation's failure wins even though a later one fails an earlier check.
        let views = [
            projected(
                "p",
                0,
                ViewOperation::put("p".repeat(2049), "s", "", "", "{}"),
            ),
            projected("p", 1, ViewOperation::put("", "s", "", "", "{}")),
        ];
        assert!(matches!(
            check_views(&views),
            Err(Error::ViewKeyTooLong { operation: 0, .. })
        ));

        // A duplicate is reported on the second operation, after its own validity checks.
        let views = [
            projected("p", 0, ViewOperation::put("p", "s", "", "", "{}")),
            projected("p", 1, ViewOperation::put("p", "s", "", "", "{}")),
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
            projected("p", 0, ViewOperation::put("p", "s", "", "", "{}")),
            projected("p", 1, ViewOperation::put("p", "t", "", "", "{}")),
            projected("p", 2, ViewOperation::put("q", "s", "", "", "{}")),
        ];
        assert!(check_views(&views).is_ok());
    }

    #[test]
    fn view_operation_sizes_count_names_values_and_the_present_condition() {
        // "pk"+"p"+"sk"+"s"+"entityID"+"id"+"entityType"+"ty"+"payload"+"{}"
        let put = ViewOperation::put("p", "s", "id", "ty", "{}");
        assert_eq!(
            view_operation_size(&put),
            2 + 1 + 2 + 1 + 8 + 2 + 10 + 2 + 7 + 2
        );
        // An absent condition is not budgeted, like every put's.
        assert_eq!(
            view_operation_size(&put.clone().if_absent()),
            view_operation_size(&put)
        );

        let delete = ViewOperation::delete("p", "s");
        assert_eq!(view_operation_size(&delete), 2 + 1 + 2 + 1);
        assert_eq!(
            view_operation_size(&delete.if_present()),
            2 + 1 + 2 + 1 + "attribute_exists(pk)".len()
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

        let views = group_commits(events);

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
        assert!(group_commits(Vec::new()).is_empty());
    }

    #[test]
    fn group_commits_orders_stream_ids_by_bytes() {
        // "B" (0x42) sorts before "a" (0x61) bytewise, unlike a locale-aware comparison.
        let views = group_commits(vec![event("a", 1, "c"), event("B", 1, "c")]);

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
            ViewOperation::put("p", "s", "", "", "{}")
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
        let plain = projected("claims", 0, ViewOperation::delete("p", "s"));
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
            ViewOperation::put("p", "s", "", "", "{}").on_conflict(Taken),
        );
        let validation = ProjectorOperationError::for_view(&mapped, "ValidationError", None);
        assert!(validation.mapped().is_none() && validation.source().is_none());
    }
}
