//! AWS Lambda helpers for [`photon_evt`] deployments on DynamoDB: the consumer side of an event
//! log (ADR 0005, X3).
//!
//! - **Event decode** ([`decode_event`]): the committed events in an event-log table's stream
//!   (`NEW_IMAGE`), with the engine's own items (stream heads, snapshots, the feature marker, the
//!   rebuild fence, command outcomes) skipped.
//! - **Filter patterns** ([`EventFilter`]; `JournalFilter`, feature `journal`): event source
//!   mapping filter patterns by event type, entity type, or journal scope, and the same test for
//!   the handler.
//! - **Journal wake-ups** (`decode_wakeup`, feature `journal`): a views-table stream record
//!   (`KEYS_ONLY` is enough) that tells a journal consumer which scope to read.
//! - **Partial batch failures** ([`process_events`], `process_wakeups`, [`process_records`]):
//!   in-order processing that stops at the first failure and returns the `batchItemFailures`
//!   response Lambda resumes from.
//!
//! The crate does not depend on a Lambda runtime or the AWS SDK: [`StreamEvent`] deserializes
//! from the payload Lambda delivers, and [`BatchResponse`] serializes to the response it expects.
//! With the `lambda_runtime` crate, a projector or fan-out function is:
//!
//! ```ignore
//! use lambda_runtime::{LambdaEvent, service_fn};
//! use photon_evt_lambda::{BatchResponse, EventFilter, StreamEvent, StreamedEvent, process_events};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), lambda_runtime::Error> {
//!     let filter = EventFilter::new().entity_type("account");
//!
//!     lambda_runtime::run(service_fn(|request: LambdaEvent<StreamEvent>| {
//!         let filter = filter.clone();
//!         async move {
//!             let outcome = process_events(&request.payload, &filter, async |streamed: StreamedEvent| {
//!                 // Idempotent: keyed on streamed.idempotency_key().
//!                 publish(streamed).await
//!             })
//!             .await;
//!
//!             if let Some(failure) = &outcome.failure {
//!                 eprintln!("stopped at {}: {}", failure.sequence_number, failure.error);
//!             }
//!
//!             Ok::<BatchResponse, lambda_runtime::Error>(outcome.response())
//!         }
//!     }))
//!     .await
//! }
//! ```
//!
//! The event source mapping needs `FunctionResponseTypes: ["ReportBatchItemFailures"]`, bounded
//! retries, and an on-failure destination; see [`batch`] for the semantics. The crate README has the
//! deployment notes, including why journal wake-ups read a filtered stream of the views table.
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]

pub mod batch;
mod error;
mod events;
mod filter;
#[cfg(feature = "journal")]
#[cfg_attr(docsrs, doc(cfg(feature = "journal")))]
mod journal;
mod record;

pub use batch::{
    BatchItemFailure, BatchOutcome, BatchResponse, RecordFailure, process_events, process_records,
};
pub use error::{DecodeError, FilterError};
pub use events::{StreamedEvent, decode_event, is_event_key};
pub use filter::{EventFilter, MAX_PATTERN_BYTES};
#[cfg(feature = "journal")]
pub use journal::{JournalFilter, JournalWakeup, coalesce_wakeups, decode_wakeup, process_wakeups};
pub use record::{
    EventName, StreamChange, StreamEvent, StreamRecord, StreamViewType, TTL_PRINCIPAL, UserIdentity,
};

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
struct ReadmeDoctests;
