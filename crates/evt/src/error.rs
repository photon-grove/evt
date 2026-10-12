//! Error types.

use std::fmt;

use crate::ids::{CommandId, EntityId, EventSequence, EventType, EventVersion};

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

    /// Encoding a payload, metadata, or snapshot failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// An in-band projector failed. Nothing was committed.
    #[error("projector failed: {0}")]
    Projector(#[source] BoxError),

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
}

/// The replay phase that failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
}
