//! The sync journal (ADR 0005, X1): per-scope ordered, replayable delivery rows written in the
//! commit that wrote the events.
//!
//! A **scope** is a named sequence, such as one per world and one per place in it. Each event of a
//! journaled entity type maps to zero or more scopes ([`ScopeResolver`]) and takes the next
//! **position** in each. A consumer reads a scope with [`high_watermark`] and [`page`]: the rows
//! after the position it has seen, contiguous, with any gap reported instead of skipped.
//!
//! ```text
//! event ──resolve──▶ scopes ──allocate──▶ positions ──▶ event metadata ("evt.journal")
//!                                                  └──▶ journal rows   journal#<scope> / <position>
//!                                                  └──▶ scope counters journal-counter#<scope>
//! ```
//!
//! The pieces:
//!
//! - **Allocations live in event metadata** ([`allocations`]), so every row and counter is a
//!   function of stored events alone. A rebuild ([`replay`], [`rebuild`]) needs no commit order and
//!   no commit grouping; feed it the events in any order.
//! - **Rows and counters are view operations in the commit.** [`JournalRepository`] wraps a
//!   repository that writes views and adds them to each commit that writes a journaled event, so
//!   they are atomic with the events. The rows come from a commit-wide projector
//!   ([`JournalProjector`]): each is owned by the event's stream and claims its position with an
//!   `absent` condition. The counters are shared by every stream of a scope, so they carry the
//!   engine's constant owner [`COUNTER_OWNER`] instead.
//! - **A row-size cap** ([`Journal::with_max_row_bytes`]) is enforced before any write, and
//!   [`Journal::pre_check`] lets a command handler turn an oversized row into its own rejection
//!   ([`RowTooLarge::to_rejection`](crate::RowTooLarge::to_rejection)).
//! - **Contention is retried**: a writer that lost a position re-reads the counters and allocates
//!   again ([`Journal::with_retry`]).
//! - **The feature marker declares the journal** (`journal`, format version 1) and the journaled
//!   entity types history-dependent, so builds without this module refuse the table and the events
//!   the rows are rebuilt from are never compacted.
//!
//! The stored shapes are in the storage format spec, "Journal".
//!
//! ```
//! # #[cfg(feature = "conformance")]
//! # {
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! use photon_evt::conformance::{Add, Ledger};
//! use photon_evt::journal::{Journal, JournalRepository, Scope};
//! use photon_evt::{MemRepository, Metadata, SerializedEvent, Store};
//!
//! // Each ledger event goes to the journal scope of its ledger.
//! let journal = Journal::new(["ledger"], |event: &SerializedEvent| {
//!     vec![Scope::new(format!("ledger:{}", event.entity_id)).unwrap()]
//! })
//! .unwrap();
//! let repository = JournalRepository::new(MemRepository::new(), journal);
//! let store = Store::<Ledger, _>::new(&repository, ());
//!
//! store.execute("a", Add(5), Metadata::new("local")).await.unwrap();
//! store.execute("a", Add(7), Metadata::new("local")).await.unwrap();
//!
//! let scope = Scope::new("ledger:a").unwrap();
//! assert_eq!(repository.high_watermark(&scope).await.unwrap(), 2);
//!
//! let page = repository.page(&scope, 0, 100).await.unwrap();
//! assert_eq!(page.rows.len(), 2);
//! assert!(page.is_caught_up());
//! # });
//! # }
//! ```

mod compact;
mod compaction;
mod config;
mod projector;
mod read;
mod replay;
mod repository;
mod row;
mod scope;
#[cfg(test)]
mod tests;

pub use compact::{
    COMPACTION_OWNER, CompactionReport, begin_compaction, compact, compact_with_lease,
    end_compaction, force_release_compaction, read_audit, read_chunk, read_floor, read_lease,
    read_manifest, renew_compaction,
};
#[cfg(any(test, feature = "conformance"))]
pub(crate) use compaction::ChunkBuilder;
pub use compaction::{
    AUDIT_SORT_KEY_PREFIX, CHUNK_SORT_KEY_PREFIX, COMPACTION_ENTITY_TYPE,
    COMPACTION_PARTITION_PREFIX, CompactionAudit, CompactionChunk, CompactionLease,
    CompactionManifest, CompactionStore, CompactionWrite, CompactionWritten, Expect, JournalGone,
    LEASE_SORT_KEY, LeaseRow, MANIFEST_SORT_KEY, MAX_CHUNK_BYTES, MAX_CHUNK_ROWS,
    MAX_COMPACTION_WRITES, audit_sk, check_compaction_row, chunk_sk, compaction_pk,
    compaction_view, released_payload,
};
pub(crate) use compaction::{check_compaction_writes, meets};
pub use config::{
    DEFAULT_MAX_ROW_BYTES, DEFAULT_RETRY, IntoScopes, Journal, MAX_ROW_BYTES, ScopeResolver,
};
pub use projector::{JournalProjector, PROJECTOR_NAME, PositionTaken};
pub use read::{
    Gap, JournalCompactionStore, JournalPage, JournalStore, MAX_PAGE_ROWS, high_watermark,
    high_watermark_at_epoch, page, page_at_epoch,
};
pub use replay::{
    JournalState, REBUILD_OWNER, rebuild, rebuild_with_lease, rebuild_with_lock, replay,
};
pub use repository::JournalRepository;
pub use row::{
    JournalRow, counter_payload, counter_view, encode_row, read_counter, row_size, row_view,
};
pub use scope::{
    ALLOCATION_KEY, Allocation, COUNTER_ENTITY_TYPE, COUNTER_OWNER, COUNTER_PARTITION_PREFIX,
    COUNTER_SORT_KEY, MAX_SCOPE_BYTES, POSITION_WIDTH, ROW_ENTITY_TYPE, ROW_PARTITION_PREFIX,
    Scope, allocations, encode_allocations, parse_row_sk, row_sk, set_allocations,
};
