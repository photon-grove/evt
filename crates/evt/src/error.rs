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

    /// The command ID was already recorded by an event replayed since the latest snapshot.
    /// Treat it as an idempotent success.
    #[error("duplicate command: {0}")]
    DuplicateCommand(CommandId),

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

    /// A commit would exceed the backend's transaction size limit.
    #[error("too many operations: {count}, the backend supports at most {limit} per transaction")]
    TooManyOperations {
        /// Operations in the commit.
        count: usize,
        /// The backend limit.
        limit: usize,
    },

    /// Shared DynamoDB-compatible transaction budget exceeded.
    #[error("commit exceeds transaction budget: {actions} actions, {bytes} bytes, largest item {largest_item}")]
    CommitTooLarge {
        /// Number of actions.
        actions: usize,
        /// Total encoded item bytes.
        bytes: usize,
        /// Largest encoded item.
        largest_item: usize,
    },

    /// A read-only input moved. The index is the pinned transaction action index.
    #[error("read fence failed for {entity_id} at action {index} ({provenance:?})")]
    FenceConflict {
        /// Fenced stream.
        entity_id: EntityId,
        /// Failing action's zero-based index.
        index: usize,
        /// Origin of the fenced head.
        provenance: crate::repository::FenceProvenance,
    },

    /// TTL retention cannot protect a read fence.
    #[error("read fences refused for retention-enabled entity type {entity_type}")]
    RetentionConflict {
        /// Durable retention-enabled type.
        entity_type: EntityType,
    },

    /// A durable rebuild lock blocks writes, including when its lease expired.
    #[error("a projection rebuild is in progress")]
    RebuildInProgress,

    /// Encoding a payload, metadata, or snapshot failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// An in-band projector failed. Nothing was committed.
    #[error("projector failed: {0}")]
    Projector(#[source] BoxError),

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

    /// The storage backend failed.
    #[error(transparent)]
    Backend(BoxError),
}

impl Error {
    /// Wraps any backend error.
    pub fn backend(err: impl Into<BoxError>) -> Self {
        Self::Backend(err.into())
    }

    /// Reports whether the error is an optimistic-concurrency conflict.
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict { .. })
    }

    /// Reports whether the error is a duplicate command (an idempotent success).
    pub fn is_duplicate_command(&self) -> bool {
        matches!(self, Self::DuplicateCommand(_))
    }

    /// Reports whether reloading and running the command again may succeed: a conflict or a
    /// transient backend failure.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Conflict { .. } | Self::Transient(_) | Self::FenceConflict { provenance: crate::repository::FenceProvenance::Engine, .. })
    }
}

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
