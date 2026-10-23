//! Event sourcing for Rust, wire-compatible with the Go [`evt`](https://github.com/photon-grove/evt)
//! framework.
//!
//! Aggregates handle commands and fold events. A [`Store`] loads them from a [`Repository`],
//! commits new events, and writes snapshots. Events written by this crate and by Go's `evt` are
//! byte-for-byte interchangeable in DynamoDB, PostgreSQL, and stream envelopes, so services in
//! either language can share one event log. The `compat/` directory in the repository holds the
//! shared fixtures and the cross-language integration suite that enforce this.
//!
//! - Define events with [`domain_events!`] and state with [`Aggregate`].
//! - Use [`MemRepository`] in tests, [`dynamodb::DynamoRepository`] (feature `dynamodb`), or
//!   [`postgres::PostgresRepository`] (feature `postgres`).
//! - Test command flows given/when/then with [`testing::Scenario`] (feature `testing`).
//! - Migrate old payloads with [`Upcaster`]s such as [`JsonUpcaster`].
//! - Record every command as a durable, replayable [`Outcome`] with [`Store::with_outcomes`]:
//!   idempotent retries, persisted domain [`Rejection`]s, and accepted no-ops.
//! - Walk the whole log lazily with [`EventLog`]: [`stream_all_events`](EventLog::stream_all_events)
//!   and [`stream_entities`](EventLog::stream_entities) return [`futures_core::Stream`]s.
//! - Read and write view rows outside the commit path with [`ViewRepository`]
//!   ([`MemRepository`], [`dynamodb::DynamoViewRepository`]), and layer typed JSON values on it with
//!   [`ViewCodec`].
//! - Deliver events in per-scope order with the sync journal ([`journal`], feature `journal`):
//!   scope allocations in event metadata, counters and rows written in the commit, and
//!   `high_watermark` / `page` reads that detect gaps.
//! - Bound log growth with [`Repository::compact_below`], which only deletes events a durable
//!   snapshot covers.
//! - Fence projection rebuilds durably with [`RebuildFence`]: while a rebuild holds the lock every
//!   commit is refused with [`Error::RebuildInProgress`], and readers check a [`RebuildEpoch`].
//!
//! The storage contract is documented in the
//! [storage format spec](https://github.com/photon-grove/evt/blob/main/docs/reference/storage-format.md).
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

mod aggregate;
mod commit_id;
#[cfg(any(test, feature = "conformance"))]
#[cfg_attr(docsrs, doc(cfg(feature = "conformance")))]
pub mod conformance;
mod enumerate;
mod error;
mod event;
mod features;
mod heads;
mod ids;
#[cfg(feature = "journal")]
#[cfg_attr(docsrs, doc(cfg(feature = "journal")))]
pub mod journal;
pub mod json;
mod mem;
mod metadata;
mod numbers;
mod outcome;
mod projection;
mod rebuild;
mod repository;
mod retry;
mod sha256;
mod store;
#[cfg(any(test, feature = "testing"))]
#[cfg_attr(docsrs, doc(cfg(feature = "testing")))]
pub mod testing;
mod transaction;
mod upcast;
mod viewcodec;
mod views;
pub mod wire;

#[cfg(feature = "dynamodb")]
#[cfg_attr(docsrs, doc(cfg(feature = "dynamodb")))]
pub mod dynamodb;

#[cfg(feature = "postgres")]
#[cfg_attr(docsrs, doc(cfg(feature = "postgres")))]
pub mod postgres;

pub use aggregate::{Aggregate, Projector};
pub use commit_id::CommitIdGenerator;
pub use enumerate::{EventBatch, EventLog, StreamFilter};
pub use error::{BoxError, Error, ExecuteError, ReplayPhase, RowTooLarge};
pub use event::{DomainEvent, PayloadError, SerializedEvent, SerializedSnapshot};
pub use features::{
    COMMAND_OUTCOME_PREFIX, FEATURES_ID, FeatureManifest, FeatureMarker,
    JOURNAL_COMPACTION_FEATURE, JOURNAL_COMPACTION_VERSION, JOURNAL_FEATURE, JOURNAL_VERSION,
    STREAM_HEADS_FEATURE, STREAM_HEADS_VERSION, SUPPORTED_FEATURES, SYSTEM_PREFIX, ValidatedMarker,
};
pub use ids::{
    CommandId, EntityId, EntityType, EventId, EventSequence, EventType, EventVersion, PrincipalId,
};
pub use mem::MemRepository;
pub use metadata::{Metadata, Origin, format_rfc3339, rfc3339_now};
pub use numbers::canonical_numbers;
pub use outcome::{
    CommandIdentity, CommandOutcome, DigestInput, Digester, MAX_COMMAND_ID_BYTES,
    MAX_OUTCOME_BYTES, Outcome, OutcomeCommand, OutcomeConfig, OutcomeStatus, Rejection,
    RejectionCode, ToRejection, canonical_digest_bytes, canonical_json, check_command_id,
    check_outcome, decode_event_ids, default_digest, encode_event_ids,
};
pub use projection::{
    CommitProjector, CommitStream, CommitView, MAX_VIEW_PK_BYTES, MAX_VIEW_SK_BYTES, ProjectedView,
    ProjectorOperationError, REASON_CONDITION_FAILED, ViewCondition, ViewOperation,
    ViewOperationKind, ViewRow, check_views, group_commits, project_commit, replay_views,
    view_operation_size,
};
pub use rebuild::{
    MAX_REBUILD_BEGIN_ATTEMPTS, MAX_REBUILD_OWNER_BYTES, MAX_REBUILD_VIEW_BATCH,
    REBUILD_GENERATION_ID, REBUILD_LOCK_ID, REBUILD_SORT_KEY, RebuildEpoch, RebuildFence,
    RebuildGeneration, RebuildLease, RebuildLock, RebuildState, observe_rebuild_epoch,
    read_at_rebuild_epoch, verify_rebuild_epoch,
};
pub use repository::{
    Commit, FenceProvenance, NewSnapshot, ReadFence, Repository, TransactionCommit,
    snapshot_events_due,
};
pub use retry::{DEFAULT_BASE_DELAY_MS, DEFAULT_MAX_ATTEMPTS, DEFAULT_MAX_DELAY_MS, RetryPolicy};
pub use store::{
    DEFAULT_SNAPSHOT_INTERVAL, Decision, Executed, Loaded, PrepareFuture, Store, serialize_events,
};
pub use transaction::{Begin, Read, Transaction};
pub use upcast::{JsonUpcaster, Upcaster};
pub use viewcodec::{SingleView, ViewCodec, ViewCodecError, ViewDecodeError, decode_json_view};
pub use views::{DEFAULT_VIEW_SK, ViewPage, ViewRepository};

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;

#[doc(hidden)]
pub mod __private {
    pub use serde_json;
}
