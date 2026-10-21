//! Error types.

use std::fmt;

use crate::ids::{CommandId, EntityId, EntityType, EventSequence, EventType, EventVersion};

/// A boxed, thread-safe error for backend and user-callback failures.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Errors raised by stores and repositories.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Optimistic concurrency failed: another writer committed the same event sequence, or the
    /// snapshot moved, since the entity was loaded. Reload and retry the command.
    #[error("conflict on entity {entity_id}: {message}")]
    Conflict {
        /// The entity whose stream conflicted.
        entity_id: EntityId,
        /// Backend detail.
        message: String,
    },

    /// A loaded stream skips a sequence: another writer snapshotted and compacted it between the
    /// snapshot read and the event read, or events expired. Reloading resolves it, so it is a
    /// retryable conflict.
    #[error(
        "conflict on entity {entity_id}: the stream skips from sequence {after} to {found}: it \
         was compacted during the load, or events expired"
    )]
    SequenceGap {
        /// The entity whose stream skips.
        entity_id: EntityId,
        /// The last sequence applied before the gap.
        after: EventSequence,
        /// The next stored sequence, which is not `after + 1`.
        found: EventSequence,
    },

    /// The command ID was already recorded by an event replayed since the latest snapshot.
    /// Treat it as an idempotent success.
    #[error("duplicate command: {0}")]
    DuplicateCommand(CommandId),

    /// A stream's head item disagrees with its events or snapshot (ADR 0007, invariants H2 to
    /// H4): a writer appended without maintaining the head, events were deleted, a snapshot is
    /// ahead of the head, or a stream has data and no head item. It is raised by the failure-path
    /// probe, by a commit whose head update passed while an event put failed, and by an
    /// out-of-band snapshot write that finds the snapshot ahead of the head.
    ///
    /// It is **not retryable** and is never exhausted into a `stale-fence` rejection: it writes no
    /// outcome row, and running the command again would fail the same way. Repair is offline
    /// (`evt-doctor migrate-heads --repair`).
    #[error(
        "stream head drift on entity {entity_id}: stored head {}, derived head {derived}",
        stored.map_or_else(|| "absent".to_owned(), |head| head.to_string())
    )]
    StreamHeadDrift {
        /// The stream whose head item disagrees with its events and snapshot.
        entity_id: EntityId,
        /// The head item's `head`, or `None` when the stream has no head item.
        stored: Option<EventSequence>,
        /// The head derived from the stream's events and snapshot: the larger of the highest
        /// event sequence and the snapshot's event sequence.
        derived: EventSequence,
    },

    /// The table's feature manifest does not declare `stream-heads` at format version 1, so this
    /// writer refuses to write (ADR 0007). The table was written by a build without stream heads
    /// and has not been migrated (`evt-doctor migrate-heads`). Every write path that validates the
    /// feature marker refuses with it: commits (outcome-only and rejection commits included),
    /// [`Repository::put_snapshot`](crate::Repository::put_snapshot), and
    /// [`Repository::compact_below`](crate::Repository::compact_below). Reads do not need the
    /// feature. Nothing was written.
    #[error(
        "the table is not migrated to stream heads: the feature manifest does not declare \
         stream-heads at stored-format version 1"
    )]
    StreamHeadsNotMigrated,

    /// Compaction was refused because no durable snapshot covers the requested range.
    #[error(
        "compaction refused: no durable snapshot covers the requested range (entity {entity_id}, \
         through {through}, snapshot covers {})",
        covered.map_or_else(|| "nothing".to_owned(), |sequence| sequence.to_string())
    )]
    CompactionUncovered {
        /// The entity.
        entity_id: EntityId,
        /// The requested upper bound.
        through: EventSequence,
        /// The event sequence the current snapshot covers, if any.
        covered: Option<EventSequence>,
    },

    /// Replay failed closed while upcasting or decoding a stored event.
    #[error("replay failed while {phase} {event_type} v{version}: {source}")]
    Replay {
        /// The stored event type.
        event_type: EventType,
        /// The stored (or partially upcast) version.
        version: EventVersion,
        /// The failing phase.
        phase: ReplayPhase,
        /// The underlying error.
        source: BoxError,
    },

    /// The stored snapshot payload could not be decoded into the aggregate.
    #[error("decoding snapshot for entity {entity_id}: {source}")]
    Snapshot {
        /// The entity.
        entity_id: EntityId,
        /// The decode error.
        source: serde_json::Error,
    },

    /// A stored record is malformed (for example, a missing key attribute or invalid metadata).
    #[error("invalid stored record: {0}")]
    InvalidRecord(String),

    /// Shared DynamoDB-compatible transaction budget exceeded.
    #[error(
        "commit exceeds transaction budget: {actions} actions, {bytes} bytes, largest item {largest_item}"
    )]
    CommitTooLarge {
        /// Number of actions.
        actions: usize,
        /// Total encoded item bytes.
        bytes: usize,
        /// Largest encoded item.
        largest_item: usize,
    },

    /// A read-only input moved: another writer advanced the fenced stream past the head the
    /// transaction observed. Engine-filled fences are retryable; caller-stamped fences are a
    /// stale-fence rejection the caller must handle.
    #[error("read fence failed for {entity_id} at head {head} ({provenance:?})")]
    FenceConflict {
        /// Fenced stream.
        entity_id: EntityId,
        /// The head the transaction fenced the stream at.
        head: EventSequence,
        /// Origin of the fenced head.
        provenance: crate::repository::FenceProvenance,
    },

    /// A transaction's calls arrived in an order that would detach the work from its command:
    /// [`begin_command`](crate::Transaction::begin_command) must be the first operation on a
    /// transaction (before any read, append, or fence), and once it returns
    /// [`Begin::Replayed`](crate::Begin::Replayed) the transaction is finished, so no further
    /// begin, read, append, fence, or commit is accepted. Nothing was written.
    #[error("transaction call order: {reason}")]
    CommandOrder {
        /// What the caller did wrong.
        reason: &'static str,
    },

    /// A transaction carried projector operations but wrote no event. Views are derived state, so
    /// operations without a backing event in the same commit could never be rebuilt by replay.
    /// Nothing was written.
    #[error(
        "commit carries {operations} projector operations but no events; views must be projections of committed events"
    )]
    ProjectorWithoutEvents {
        /// The number of projector operations the commit carried.
        operations: usize,
    },

    /// An append named a different projector set than the one already fixed for the stream by its
    /// first projector-bearing append. The stream's operations are derived with one set, so the
    /// append was refused and nothing changed.
    #[error("stream {entity_id} already has a different projector set in this transaction")]
    ProjectorSetConflict {
        /// The stream whose projector set conflicts.
        entity_id: EntityId,
    },

    /// A transaction's append failed after it had already changed a handle's aggregate, leaving
    /// the handle ahead of the staged events. A half-applied handle can never reach a commit, so
    /// every later append and the commit are refused. Discard the transaction and build a new one.
    #[error("the transaction is poisoned by an earlier failed append; discard it")]
    TransactionPoisoned,

    /// TTL retention cannot protect a read fence.
    #[error("read fences refused for retention-enabled entity type {entity_type}")]
    RetentionConflict {
        /// Durable retention-enabled type.
        entity_type: EntityType,
    },

    /// A commit's rebuild-lock check failed (v0.9, C10): the rebuild generation is unsettled,
    /// because a rebuild holds the `SYSTEM#REBUILD` lock (including one whose lease expired) or a
    /// force-release left the rebuild unfinished. Nothing was written.
    ///
    /// It is **transient**, never a conflict: the commit can succeed once a rebuild ends and
    /// settles the generation. A force-release alone does not: it leaves the generation unsettled,
    /// and commits are refused with this error until a later rebuild ends. Fence-aware retry runs
    /// the command again after a backoff and returns this error when the attempts run out; it
    /// never becomes a `stale-fence` rejection (even when the commit carried caller-stamped
    /// fences) and records no outcome. A failed feature-marker check outranks it; it outranks
    /// every other failure of the commit.
    #[error("a projection rebuild is in progress")]
    RebuildInProgress,

    /// [`begin_rebuild`](crate::RebuildFence::begin_rebuild) found another rebuild holding an
    /// unexpired lease. Not retried by the engine. Nothing was written.
    #[error("the rebuild lock is held by {owner:?} until {lease_expires_at} (Unix seconds)")]
    RebuildLockHeld {
        /// The holder's owner token.
        owner: String,
        /// When the holder's lease lapses, in whole Unix seconds.
        lease_expires_at: i64,
    },

    /// The caller no longer holds the rebuild lock it named: the lock row is gone or names another
    /// owner or, for a lease (a renew, an end, a guarded view write), another fencing token, as
    /// after a takeover by a new incarnation under the same owner token; or, for a guarded view
    /// write, the lease lapsed. The worker must stop. Nothing was written.
    #[error("rebuild owner {owner:?} does not hold the lock")]
    RebuildLeaseLost {
        /// The owner token the caller named.
        owner: String,
    },

    /// [`begin_rebuild`](crate::RebuildFence::begin_rebuild) lost its race against other lifecycle
    /// operations [`MAX_REBUILD_BEGIN_ATTEMPTS`](crate::MAX_REBUILD_BEGIN_ATTEMPTS) times. It is
    /// **transient**.
    #[error("the rebuild lock kept changing; gave up after {attempts} attempts")]
    RebuildConflict {
        /// The attempts made.
        attempts: usize,
    },

    /// The rebuild lock and generation disagree in a way the protocol cannot produce (an odd
    /// generation with no lock and no force-release flag, or a lock with an even generation),
    /// because every transition writes both in one transaction. It is reported, never repaired.
    #[error("the rebuild lock and generation disagree: {0}")]
    RebuildStateInvalid(String),

    /// A reader found an odd rebuild generation: a rebuild is in progress, or a force-release left
    /// derived state untrustworthy (`force_released`). It is **transient**: read again once a
    /// rebuild has ended. See [`RebuildEpoch`](crate::RebuildEpoch).
    #[error(
        "derived state is unavailable: rebuild generation {generation} is odd{}",
        if *force_released { " (left by a force-release)" } else { "" }
    )]
    RebuildUnavailable {
        /// The odd generation.
        generation: i64,
        /// The generation was left by a force-release.
        force_released: bool,
    },

    /// A reader's rebuild generation changed between its before and after reads: a rebuild
    /// started, finished, or was taken over while the read ran. It is **transient**.
    #[error("the rebuild generation changed from {observed} to {current} during the read")]
    RebuildEpochChanged {
        /// The epoch the read was made under.
        observed: i64,
        /// The settled generation read afterwards.
        current: i64,
    },

    /// Encoding a payload, metadata, or snapshot failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// The aggregate's handler failed with an error that is not a domain rejection
    /// ([`ToRejection`](crate::ToRejection) returned `None`): an internal failure. Nothing was
    /// committed and no outcome was recorded, so a retry after the cause is fixed starts clean.
    #[error("command handler failed: {0}")]
    Handler(#[source] BoxError),

    /// An in-band projector failed. Nothing was committed. A commit-wide projector's error names
    /// the projector. A legacy projector operation that the backend rejected also maps here.
    #[error("projector failed: {0}")]
    Projector(#[source] BoxError),

    /// A commit-wide view operation's condition failed, or the backend rejected its statement
    /// (v0.9, C7). The error names the projector and the operation, and its
    /// [`source`](std::error::Error::source) is the operation's mapped error when it carried one
    /// ([`ViewOperation::on_conflict`](crate::ViewOperation::on_conflict)). It is **not** a
    /// conflict and **not** transient: re-preparing the command cannot fix it, so it is never
    /// retried and records no outcome. Nothing was written.
    #[error(transparent)]
    ProjectorOperation(Box<crate::projection::ProjectorOperationError>),

    /// A view operation cannot be stored the same way on every backend: an empty `pk` or `sk`,
    /// text that is not valid UTF-8 or contains U+0000, or a condition its kind does not allow.
    /// Nothing was written.
    #[error("invalid view operation: {0}")]
    InvalidViewOperation(String),

    /// A view row's key is over DynamoDB's limit, 2048 UTF-8 bytes for a `pk` and 1024 for an
    /// `sk`, so it fails identically on every backend. Nothing was written.
    #[error(
        "view key too long: projector {projector:?} operation {operation}: {field} is {length} \
         bytes, the limit is {max}"
    )]
    ViewKeyTooLong {
        /// `pk` or `sk`.
        field: &'static str,
        /// The key's length in UTF-8 bytes.
        length: usize,
        /// The limit.
        max: usize,
        /// The projector that emitted the operation.
        projector: String,
        /// The operation's index among that projector's operations.
        operation: usize,
    },

    /// Two view operations of one commit target the same `(pk, sk)` row, which DynamoDB would fail
    /// with an unclassified validation error. Nothing was written.
    #[error(
        "duplicate view key {pk:?}/{sk:?}: projector {:?} operation {} repeats projector {:?} \
         operation {}",
        .second.0, .second.1, .first.0, .first.1
    )]
    DuplicateViewKey {
        /// The partition key.
        pk: String,
        /// The sort key.
        sk: String,
        /// The earlier operation: its projector's name and operation index.
        first: (String, usize),
        /// The later operation that repeats the key.
        second: (String, usize),
    },

    /// Stored events whose commits cannot be put in an order that respects every stream's own
    /// sequence: one commit wrote a stream before another, which wrote a second stream before the
    /// first. No live system writes that.
    #[error("commits {commits:?} cannot be ordered consistently with their streams' sequences")]
    CommitOrder {
        /// The commit IDs involved, ascending.
        commits: Vec<String>,
    },

    /// A row was written on behalf of two different streams. Their commits have no order the log
    /// keeps, so a replay cannot tell which write came last and refuses rather than guess.
    #[error(
        "view row {pk:?}/{sk:?} is written for streams {first:?} and {second:?}, whose commits \
         have no durable order"
    )]
    SharedViewKey {
        /// The row's partition key.
        pk: String,
        /// The row's sort key.
        sk: String,
        /// The owner streams, in the order the replay met them.
        first: String,
        /// The second owner stream.
        second: String,
    },

    /// A commit-wide projector was registered over a repository that has not declared it writes
    /// view operations ([`Repository::writes_views`](crate::Repository::writes_views)); it would
    /// commit the events and silently drop the rows. Nothing was written.
    #[error(
        "repository {repository} does not write view operations; refusing a commit-wide projector"
    )]
    ViewsUnsupported {
        /// The refusing repository's Rust type.
        repository: String,
    },

    /// The journal was asked to commit through a repository that does not apply a view put's
    /// expected-payload condition
    /// ([`applies_expected_payload`](crate::Repository::applies_expected_payload)): its counter
    /// compare-and-set would be written unconditionally, which can duplicate positions once rows
    /// are deleted. Refused before anything is planned, declared, or written.
    #[error(
        "repository {repository} does not apply the expected-payload view condition; refusing to journal"
    )]
    ExpectedPayloadUnsupported {
        /// The refusing repository's Rust type.
        repository: String,
    },

    /// A history-dependent projector was registered over a repository that cannot declare history
    /// dependence durably; compaction could then delete the commits its rebuild replays. Nothing
    /// was written.
    #[error(
        "repository {repository} cannot declare history-dependent entity types; refusing a \
         history-dependent projector"
    )]
    HistoryDeclarationUnsupported {
        /// The refusing repository's Rust type.
        repository: String,
    },

    /// A commit carries view operations, but the repository has no place to write them (no views
    /// table, no view writer). Dropping them would commit the events without the rows meant to be
    /// atomic with them, so the repository fails closed. Nothing was written.
    #[error("the {backend} repository has no views table or view writer configured")]
    ViewsNotConfigured {
        /// The refusing backend: `dynamodb` or `postgres`.
        backend: &'static str,
    },

    /// A journal row would exceed the configured row-size cap (ADR 0005, X1). Returned before any
    /// write, by the journal's pre-check and again by the commit-time guard; nothing was written.
    /// The application turns it into its own rejection with [`RowTooLarge::to_rejection`].
    #[error(transparent)]
    JournalRowTooLarge(#[from] RowTooLarge),

    /// A journal configuration, scope, or stored allocation is invalid (ADR 0005, X1): an empty or
    /// oversized scope, a malformed or inconsistent allocation in event metadata, or a position
    /// claimed twice. Nothing was written.
    #[error("invalid journal: {0}")]
    InvalidJournal(String),

    /// A journal page asked for rows below the scope's compaction floor that are no longer there
    /// (ADR 0005, X2): the `410 Gone` handoff. It carries the snapshot that replaces them; install
    /// it and page on from its checkpoint. Not retryable as it is: the same read is gone again.
    #[cfg(feature = "journal")]
    #[error(
        "journal scope {} is compacted through {}: rows after {} are gone; install snapshot {}",
        .0.scope, .0.checkpoint, .0.after, .0.digest
    )]
    JournalGone(Box<crate::journal::JournalGone>),

    /// A snapshot chunk is missing or does not match the manifest it was read for: a compaction
    /// swapped the manifest in between (ADR 0005, X2). It is **transient**: read the manifest
    /// again and start over.
    #[error("the snapshot of journal scope {scope:?} through {checkpoint} has moved")]
    JournalSnapshotMoved {
        /// The scope.
        scope: String,
        /// The checkpoint of the manifest the chunk was read for.
        checkpoint: u64,
    },

    /// [`begin_compaction`](crate::journal::begin_compaction) found another compaction of the
    /// scope holding an unexpired lease (ADR 0005, X2). Nothing was written.
    #[error(
        "the compaction lease of journal scope {scope:?} is held by {owner:?} until {lease_expires_at} (Unix seconds)"
    )]
    CompactionLeaseHeld {
        /// The scope.
        scope: String,
        /// The holder's owner token.
        owner: String,
        /// When the holder's lease lapses, in whole Unix seconds.
        lease_expires_at: i64,
    },

    /// The caller no longer holds the compaction lease it named (ADR 0005, X2): it was taken over,
    /// released, or force-released, or it lapsed. The worker must stop; nothing more was written,
    /// and the scope is consistent.
    #[error("compaction owner {owner:?} does not hold the lease of journal scope {scope:?}")]
    CompactionLeaseLost {
        /// The scope.
        scope: String,
        /// The owner token the caller named.
        owner: String,
    },

    /// The backend failed in a way that may succeed on retry, and running the operation again is
    /// safe: throttling, a deadlock, a serialization failure, or a timed-out read. A commit is only
    /// transient when the backend rejected it. A commit whose outcome is unknown, such as one that
    /// timed out, is [`Error::Backend`].
    #[error("transient backend failure: {0}")]
    Transient(#[source] BoxError),

    /// A commit was built wrong, for example with an event for another entity. Nothing was
    /// written.
    #[error("invalid commit: {0}")]
    InvalidCommit(String),

    /// The repository was configured with an invalid value, such as an unsafe table name.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// A read or write was called with an invalid argument, such as
    /// [`Repository::latest`](crate::Repository::latest) with `n = 0`. Returned before any
    /// request.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// A write path was given an entity ID in a reserved namespace (`SYSTEM#`,
    /// `command-outcome#`), which only the engine may write. Nothing was written.
    #[error("entity ID {entity_id} is in a reserved namespace (SYSTEM#, command-outcome#)")]
    ReservedEntityId {
        /// The reserved entity ID.
        entity_id: EntityId,
    },

    /// The table's feature manifest declares a `(name, formatVersion)` pair this writer does not
    /// model, so it refuses to write. Nothing was written.
    #[error(
        "the feature manifest declares {feature} at stored-format version {version}, which this \
         writer does not support"
    )]
    UnsupportedFeature {
        /// The declared feature name.
        feature: String,
        /// The declared stored-format version.
        version: i64,
    },

    /// Compaction was refused because the entity type appears in the feature manifest's durable
    /// `historyDependent` set: its commit-wide projectors need the full commit stream. Nothing
    /// was deleted.
    #[error(
        "compaction refused: entity type {entity_type} has history-dependent projectors \
         (entity {entity_id})"
    )]
    HistoryDependentProjection {
        /// The entity's type.
        entity_type: EntityType,
        /// The entity.
        entity_id: EntityId,
    },

    /// A command ID is longer than 512 UTF-8 bytes, the bound for a store with command outcomes.
    /// Returned before any write.
    #[error(
        "command ID is {length} bytes; the limit is {} bytes",
        crate::outcome::MAX_COMMAND_ID_BYTES
    )]
    CommandIdTooLong {
        /// The ID's length in UTF-8 bytes.
        length: usize,
    },

    /// A stored response or an encoded rejection is over 65536 bytes. Returned before any write.
    #[error(
        "outcome {what} is {size} bytes; the limit is {} bytes",
        crate::outcome::MAX_OUTCOME_BYTES
    )]
    OutcomeTooLarge {
        /// The oversized value: `response` or `rejection`.
        what: &'static str,
        /// The value's size in bytes.
        size: usize,
    },

    /// The command ID already carries an outcome recorded under a different identity digest. It
    /// is returned as an error and never stored: the existing row belongs to the first command
    /// that claimed the ID.
    #[error("command ID {command_id} was already used by a different command")]
    IdempotencyKeyReuse {
        /// The reused command ID.
        command_id: CommandId,
        /// The stored outcome the ID belongs to.
        prior_outcome: Box<crate::outcome::Outcome>,
    },

    /// A commit's outcome put lost its `attribute_not_exists(pk)` condition: another writer
    /// already recorded an outcome for the command ID. It is never a generic conflict, and
    /// re-preparing the command cannot resolve it. A [`Store`](crate::Store) resolves it by
    /// reading the row; a repository returns it.
    #[error("an outcome is already recorded for command {command_id}")]
    OutcomeConflict {
        /// The command ID.
        command_id: CommandId,
    },

    /// A commit's result is unknown (a timeout, a lost connection, a cancelled call, a server
    /// fault) and polling the outcome row for the store's recovery window did not settle it. An
    /// absent row is never proof that the commit did not apply: the transaction may still land, so
    /// the error never says it did not. It is retryable: running the command again with the same
    /// ID is safe, because every attempt reads the outcome row first and every write is conditional,
    /// and it finds the outcome if the first commit lands.
    #[error("command {command_id} may or may not have been applied: {source}")]
    AmbiguousWrite {
        /// The command ID.
        command_id: CommandId,
        /// The indeterminate failure.
        source: BoxError,
    },

    /// A commit that carries a command outcome failed in a way that leaves it unknown whether the
    /// transaction applied: a timeout, a lost connection or response, an internal server error, a
    /// connection dropped during `COMMIT`. Backends return it only for such genuinely indeterminate
    /// failures of outcome-carrying commits; a [`Store`](crate::Store) settles it by re-reading the
    /// outcome row and reports [`Error::AmbiguousWrite`]. A definitive failure (validation, a
    /// missing table or relation, authorization, a constraint error) stays [`Error::Backend`] and
    /// is never recovered.
    #[error("commit result unknown: {0}")]
    Indeterminate(#[source] BoxError),

    /// The storage backend failed. This is a definitive failure, or a failure of a commit that
    /// carries no outcome (whose result may be unknown but cannot be recovered).
    #[error(transparent)]
    Backend(BoxError),
}

impl Error {
    /// Wraps any backend error.
    pub fn backend(err: impl Into<BoxError>) -> Self {
        Self::Backend(err.into())
    }

    /// Reports whether the error is an optimistic-concurrency conflict. A fence conflict is one:
    /// another writer moved the fenced stream, like a competing sequence put.
    pub fn is_conflict(&self) -> bool {
        matches!(
            self,
            Self::Conflict { .. } | Self::SequenceGap { .. } | Self::FenceConflict { .. }
        )
    }

    /// Reports whether the error is a duplicate command (an idempotent success).
    pub fn is_duplicate_command(&self) -> bool {
        matches!(self, Self::DuplicateCommand(_))
    }

    /// Reports whether reloading and running the command again may succeed: a conflict, a
    /// transient backend failure, an ambiguous write that provably did not apply, or a transient
    /// rebuild-fence refusal (a commit refused during a rebuild, a lost
    /// [`begin_rebuild`](crate::RebuildFence::begin_rebuild) race, an unavailable or changed
    /// reader epoch).
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Conflict { .. }
                | Self::SequenceGap { .. }
                | Self::Transient(_)
                | Self::AmbiguousWrite { .. }
                | Self::RebuildInProgress
                | Self::RebuildConflict { .. }
                | Self::RebuildUnavailable { .. }
                | Self::RebuildEpochChanged { .. }
                | Self::JournalSnapshotMoved { .. }
                | Self::FenceConflict {
                    provenance: crate::repository::FenceProvenance::Engine,
                    ..
                }
        )
    }
}

/// A journal row over the configured size cap: which row, how large, and the cap. The journal's
/// pre-check and its commit-time guard both raise it ([`Error::JournalRowTooLarge`]) before any
/// write.
///
/// The size is the byte length of the row's stored payload, the widest row of the commit being
/// the one reported.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct RowTooLarge {
    /// The scope whose row is too large.
    pub scope: String,
    /// The position the row was (or would be) allocated. For a pre-check, which allocates
    /// nothing, the widest position the format allows ([`u64::MAX`]).
    pub position: u64,
    /// The event the row carries.
    pub event_id: String,
    /// The row's size in bytes.
    pub size: usize,
    /// The configured cap in bytes.
    pub limit: usize,
}

impl RowTooLarge {
    /// The domain rejection an application records for the refused command: code
    /// `journal-row-too-large`, with the scope, event, size, and cap in the details. A pre-check
    /// raised in a command handler can map to it from the handler's
    /// [`ToRejection`](crate::ToRejection), so the outcome records why the command was refused.
    pub fn to_rejection(&self) -> crate::outcome::Rejection {
        crate::outcome::Rejection::new("journal-row-too-large", self.to_string()).with_details(
            serde_json::json!({
                "scope": self.scope,
                "eventId": self.event_id,
                "size": self.size,
                "limit": self.limit,
            }),
        )
    }
}

impl fmt::Display for RowTooLarge {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the journal row for event {} in scope {:?} is {} bytes, over the {} byte cap",
            self.event_id, self.scope, self.size, self.limit
        )
    }
}

impl std::error::Error for RowTooLarge {}

/// The replay phase that failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReplayPhase {
    /// Running upcasters.
    Upcast,
    /// Decoding the payload into the typed event.
    Deserialize,
}

impl fmt::Display for ReplayPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Upcast => "upcasting",
            Self::Deserialize => "deserializing",
        })
    }
}

/// The outcome of [`Store::execute`](crate::Store::execute) when it does not succeed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ExecuteError<E> {
    /// The aggregate rejected the command. Nothing was committed.
    #[error("command rejected: {0}")]
    Rejected(#[source] E),

    /// Loading or committing failed.
    #[error(transparent)]
    Store(#[from] Error),
}

impl<E> ExecuteError<E> {
    /// Returns the store error, if this is one.
    pub fn as_store_error(&self) -> Option<&Error> {
        match self {
            Self::Store(err) => Some(err),
            Self::Rejected(_) => None,
        }
    }

    /// Reports whether the error is an optimistic-concurrency conflict.
    pub fn is_conflict(&self) -> bool {
        self.as_store_error().is_some_and(Error::is_conflict)
    }

    /// Reports whether the error is a duplicate command (an idempotent success).
    pub fn is_duplicate_command(&self) -> bool {
        self.as_store_error()
            .is_some_and(Error::is_duplicate_command)
    }

    /// Reports whether running the command again may succeed. Rejections are never retryable.
    pub fn is_retryable(&self) -> bool {
        self.as_store_error().is_some_and(Error::is_retryable)
    }
}
