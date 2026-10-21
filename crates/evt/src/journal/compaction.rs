//! Journal compaction's stored shapes (ADR 0005, X2): the compaction partition of a scope, its
//! lease, manifest, chunks, and audit rows, and the backend primitive that writes them.
//!
//! Every row lives in the views table under `journal-compaction#<scope>`, owned by the engine
//! ([`COUNTER_OWNER`]), with the entity type [`COMPACTION_ENTITY_TYPE`]:
//!
//! | Sort key                    | Row      | Payload                                                     |
//! | --------------------------- | -------- | ----------------------------------------------------------- |
//! | `LEASE`                     | lease    | `{"scope","token","owner","leaseExpiresAt"}` (held), or `{"scope","token"}` (released) |
//! | `MANIFEST`                  | manifest | `{"scope","checkpoint","chunks":[<chunk digest>…]}`          |
//! | `chunk#<20-digit index>`    | chunk    | `{"scope","index","from","through","rows":[<row payload>…]}` |
//! | `audit#<20-digit checkpoint>` | audit  | `{"scope","checkpoint","digest","chunks","deleted","owner","token","completedAt"}` |
//!
//! A digest is the lowercase hex SHA-256 of a row's stored payload: a chunk's digest is listed in
//! the manifest, and the manifest's digest identifies the snapshot.

use std::future::Future;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use super::row::{JournalRow, decode_row_payload, encode_row};
use super::scope::{COUNTER_OWNER, POSITION_WIDTH, Scope};
use crate::error::Error;
use crate::features::FeatureMarker;
use crate::json;
use crate::projection::ViewRow;
use crate::rebuild::{RebuildEpoch, RebuildFence};
use crate::sha256::sha256_hex;

use super::read::JournalCompactionStore;

/// The partition-key prefix of a scope's compaction rows (`journal-compaction#<scope>`).
pub const COMPACTION_PARTITION_PREFIX: &str = "journal-compaction#";

/// The informational entity type of every compaction row.
pub const COMPACTION_ENTITY_TYPE: &str = "journal-compaction";

/// The sort key of a scope's compaction lease.
pub const LEASE_SORT_KEY: &str = "LEASE";

/// The sort key of a scope's manifest.
pub const MANIFEST_SORT_KEY: &str = "MANIFEST";

/// The sort-key prefix of a chunk (`chunk#<20-digit index>`).
pub const CHUNK_SORT_KEY_PREFIX: &str = "chunk#";

/// The sort-key prefix of an audit row (`audit#<20-digit checkpoint>`).
pub const AUDIT_SORT_KEY_PREFIX: &str = "audit#";

/// The most rows a chunk holds. Part of the format: chunk boundaries are canonical, so every
/// implementation closes a chunk at the same row.
pub const MAX_CHUNK_ROWS: usize = 500;

/// The longest chunk or manifest payload, in bytes. Part of the format, like [`MAX_CHUNK_ROWS`].
/// It holds one row at the journal's largest row cap ([`MAX_ROW_BYTES`](super::MAX_ROW_BYTES))
/// with the chunk's header, and a whole row stays inside DynamoDB's single-item budget.
pub const MAX_CHUNK_BYTES: usize = 375_000;

/// The most writes one [`CompactionStore::write_compaction`] call carries: on DynamoDB the 100th
/// action of the transaction is the rebuild-epoch check.
pub const MAX_COMPACTION_WRITES: usize = 99;

/// The partition key of a scope's compaction rows.
pub fn compaction_pk(scope: &Scope) -> String {
    format!("{COMPACTION_PARTITION_PREFIX}{scope}")
}

/// The sort key of chunk `index`.
pub fn chunk_sk(index: u64) -> String {
    format!(
        "{CHUNK_SORT_KEY_PREFIX}{index:0width$}",
        width = POSITION_WIDTH
    )
}

/// The sort key of the audit row of `checkpoint`.
pub fn audit_sk(checkpoint: u64) -> String {
    format!(
        "{AUDIT_SORT_KEY_PREFIX}{checkpoint:0width$}",
        width = POSITION_WIDTH
    )
}

/// The engine-owned view row of a compaction payload.
pub fn compaction_view(scope: &Scope, sk: impl Into<String>, payload: Vec<u8>) -> ViewRow {
    ViewRow::new(
        compaction_pk(scope),
        sk,
        COUNTER_OWNER,
        COMPACTION_ENTITY_TYPE,
        payload,
    )
}

/// Checks that a row read from the scope's compaction partition is the engine's: at the key asked
/// for, owned by [`COUNTER_OWNER`] (`SYSTEM#JOURNAL`, which no stream can be, so no projector can
/// write it), with the entity type [`COMPACTION_ENTITY_TYPE`]. Anything else is
/// [`Error::ForeignJournalRow`]: compaction and readers refuse to act on it.
pub fn check_compaction_row(scope: &Scope, sort_key: &str, row: ViewRow) -> Result<ViewRow, Error> {
    let pk = compaction_pk(scope);

    if row.pk != pk
        || row.sk != sort_key
        || row.entity_id != COUNTER_OWNER
        || row.entity_type != COMPACTION_ENTITY_TYPE
    {
        return Err(Error::ForeignJournalRow {
            pk: row.pk,
            sk: row.sk,
            owner: row.entity_id,
            entity_type: row.entity_type,
        });
    }

    Ok(row)
}

fn corrupt(scope: &Scope, what: &str, why: impl std::fmt::Display) -> Error {
    Error::InvalidRecord(format!("journal compaction {what} of {scope}: {why}"))
}

/// A scope's compaction lease: who holds it, the fencing token of the hold, and when it lapses.
///
/// The token rises by one on every acquire and takeover and is kept when the lease is released,
/// so a lease names one hold. Every write a compaction makes checks that the lease row still
/// holds this lease's exact payload, so a worker whose lease was taken over or force-released
/// writes nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionLease {
    /// The scope.
    pub scope: Scope,
    /// The holder's owner token.
    pub owner: String,
    /// The fencing token of this hold.
    pub token: u64,
    /// When the lease lapses, in whole Unix seconds. A lapsed lease can be taken over.
    pub lease_expires_at: i64,
    /// The lease's length in whole seconds, which a run renews it by when less than half of it
    /// is left. Not stored: 0 for a lease read back from its row, which is never renewed by a run.
    pub lease_seconds: i64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeasePayload {
    scope: String,
    token: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    owner: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lease_expires_at: Option<i64>,
}

/// A lease row as stored: held by an owner, or released (the token kept).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseRow {
    /// A compaction holds the scope.
    Held(CompactionLease),
    /// No compaction holds the scope; the last hold had this token.
    Released {
        /// The scope.
        scope: Scope,
        /// The token of the last hold.
        token: u64,
    },
}

impl CompactionLease {
    /// The stored payload of this held lease.
    pub fn payload(&self) -> Vec<u8> {
        json::to_vec(&LeasePayload {
            scope: self.scope.as_str().to_owned(),
            token: self.token,
            owner: Some(self.owner.clone()),
            lease_expires_at: Some(self.lease_expires_at),
        })
        .expect("a lease serializes")
    }

    /// The stored payload of this lease, released.
    pub fn released_payload(&self) -> Vec<u8> {
        released_payload(&self.scope, self.token)
    }
}

/// The stored payload of a released lease.
pub fn released_payload(scope: &Scope, token: u64) -> Vec<u8> {
    json::to_vec(&LeasePayload {
        scope: scope.as_str().to_owned(),
        token,
        owner: None,
        lease_expires_at: None,
    })
    .expect("a lease serializes")
}

impl LeaseRow {
    /// Decodes a stored lease row of `scope`.
    pub fn decode(scope: &Scope, payload: &[u8]) -> Result<Self, Error> {
        let stored: LeasePayload =
            json::from_slice(payload).map_err(|err| corrupt(scope, "lease", err))?;

        if stored.scope != scope.as_str() {
            return Err(corrupt(
                scope,
                "lease",
                format!("it names {:?}", stored.scope),
            ));
        }

        match (stored.owner, stored.lease_expires_at) {
            (Some(owner), Some(lease_expires_at)) => Ok(Self::Held(CompactionLease {
                scope: scope.clone(),
                owner,
                token: stored.token,
                lease_expires_at,
                lease_seconds: 0,
            })),
            (None, None) => Ok(Self::Released {
                scope: scope.clone(),
                token: stored.token,
            }),
            _ => Err(corrupt(
                scope,
                "lease",
                "an owner and an expiry come together",
            )),
        }
    }

    /// The token of the last hold.
    pub fn token(&self) -> u64 {
        match self {
            Self::Held(lease) => lease.token,
            Self::Released { token, .. } => *token,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ManifestPayload {
    scope: String,
    checkpoint: u64,
    chunks: Vec<String>,
}

/// A scope's manifest: the compacted prefix of its journal, `1..=checkpoint`, as the ordered
/// digests of its chunks. The checkpoint is the scope's **floor**: a reader whose cursor is below
/// it may find its rows deleted, and is handed the snapshot instead ([`JournalGone`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionManifest {
    /// The scope.
    pub scope: Scope,
    /// The last position the chunks hold.
    pub checkpoint: u64,
    /// The digest of each chunk, by index.
    pub chunks: Vec<String>,
}

impl CompactionManifest {
    /// The stored payload.
    pub fn payload(&self) -> Vec<u8> {
        json::to_vec(&ManifestPayload {
            scope: self.scope.as_str().to_owned(),
            checkpoint: self.checkpoint,
            chunks: self.chunks.clone(),
        })
        .expect("a manifest serializes")
    }

    /// The manifest's digest, which identifies the snapshot: the SHA-256 of its payload.
    pub fn digest(&self) -> String {
        sha256_hex(&self.payload())
    }

    /// Decodes a stored manifest of `scope`, checking it is well formed: a checkpoint of at least
    /// 1, one chunk at least, and 64-digit lowercase hex digests.
    pub fn decode(scope: &Scope, payload: &[u8]) -> Result<Self, Error> {
        let stored: ManifestPayload =
            json::from_slice(payload).map_err(|err| corrupt(scope, "manifest", err))?;

        if stored.scope != scope.as_str() {
            return Err(corrupt(
                scope,
                "manifest",
                format!("it names {:?}", stored.scope),
            ));
        }

        if stored.checkpoint == 0 || stored.chunks.is_empty() {
            return Err(corrupt(scope, "manifest", "it covers nothing"));
        }

        if !stored.chunks.iter().all(|digest| is_digest(digest)) {
            return Err(corrupt(
                scope,
                "manifest",
                "a chunk digest is not SHA-256 hex",
            ));
        }

        Ok(Self {
            scope: scope.clone(),
            checkpoint: stored.checkpoint,
            chunks: stored.chunks,
        })
    }

    /// What a reader below the floor is handed: the snapshot that replaces the rows it asked for.
    pub fn gone(&self, after: u64) -> JournalGone {
        JournalGone {
            scope: self.scope.clone(),
            after,
            checkpoint: self.checkpoint,
            chunk_count: self.chunks.len(),
            digest: self.digest(),
        }
    }
}

fn is_digest(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// A page read below a scope's floor: the rows the reader asked for may have been deleted by
/// compaction. This is the `410 Gone` handoff: the reader installs the snapshot (the manifest and
/// its chunks, [`read_manifest`](super::read_manifest) and [`read_chunk`](super::read_chunk)),
/// sets its cursor to [`checkpoint`](Self::checkpoint), and pages on from there, which never
/// meets a gap the compaction made. It is not retryable as it is: the same read is gone again.
///
/// It names the snapshot without disclosing any of the deleted rows. An HTTP layer maps it to
/// [`HTTP_STATUS`](Self::HTTP_STATUS) with these fields as the body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalGone {
    /// The scope.
    pub scope: Scope,
    /// The position the reader asked to read after.
    pub after: u64,
    /// The snapshot's checkpoint, the scope's floor: resume after it.
    pub checkpoint: u64,
    /// The number of chunks the snapshot has.
    pub chunk_count: usize,
    /// The manifest digest, which identifies the snapshot.
    pub digest: String,
}

impl JournalGone {
    /// The HTTP status a gone page maps to.
    pub const HTTP_STATUS: u16 = 410;

    /// The cursor to resume paging from once the snapshot is installed.
    pub fn resume_after(&self) -> u64 {
        self.checkpoint
    }
}

#[derive(Deserialize)]
struct ChunkIn<'a> {
    scope: String,
    index: u64,
    from: u64,
    through: u64,
    #[serde(borrow)]
    rows: Vec<&'a RawValue>,
}

/// One chunk of a snapshot: the journal rows `from..=through`, contiguous, as they were stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionChunk {
    /// The scope.
    pub scope: Scope,
    /// The chunk's index in the manifest.
    pub index: u64,
    /// The first position it holds.
    pub from: u64,
    /// The last position it holds.
    pub through: u64,
    /// The rows, ascending.
    pub rows: Vec<JournalRow>,
}

impl CompactionChunk {
    /// Decodes a stored chunk of `scope`, checking that its rows are the contiguous positions
    /// `from..=through` of the scope.
    pub fn decode(scope: &Scope, payload: &[u8]) -> Result<Self, Error> {
        let stored = decode_chunk(scope, payload)?;
        let rows = stored
            .rows
            .iter()
            .map(|raw| decode_row_payload(scope, raw.get().as_bytes()))
            .collect::<Result<Vec<_>, _>>()?;

        let contiguous = rows
            .iter()
            .zip(stored.from..)
            .all(|(row, position)| row.position == position);
        if !contiguous || stored.through + 1 != stored.from + rows.len() as u64 {
            return Err(corrupt(
                scope,
                "chunk",
                format!("its rows are not {}..={}", stored.from, stored.through),
            ));
        }

        Ok(Self {
            scope: scope.clone(),
            index: stored.index,
            from: stored.from,
            through: stored.through,
            rows,
        })
    }
}

fn decode_chunk<'a>(scope: &Scope, payload: &'a [u8]) -> Result<ChunkIn<'a>, Error> {
    let stored: ChunkIn<'a> =
        serde_json::from_slice(payload).map_err(|err| corrupt(scope, "chunk", err))?;

    if stored.scope != scope.as_str() || stored.rows.is_empty() || stored.from == 0 {
        return Err(corrupt(scope, "chunk", "it is not a chunk of this scope"));
    }

    Ok(stored)
}

/// A chunk being built: the canonical rule closes a chunk before the row that would make it hold
/// more than [`MAX_CHUNK_ROWS`] rows or a payload over [`MAX_CHUNK_BYTES`] bytes.
#[derive(Clone, Debug)]
pub(crate) struct ChunkBuilder {
    scope: Scope,
    index: u64,
    from: u64,
    rows: Vec<Vec<u8>>,
    row_bytes: usize,
}

/// A built chunk: its index, range, payload, and digest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BuiltChunk {
    pub(crate) index: u64,
    pub(crate) from: u64,
    pub(crate) through: u64,
    pub(crate) payload: Vec<u8>,
    pub(crate) digest: String,
}

impl ChunkBuilder {
    /// An empty chunk `index` starting at `from`.
    pub(crate) fn new(scope: &Scope, index: u64, from: u64) -> Self {
        Self {
            scope: scope.clone(),
            index,
            from,
            rows: Vec::new(),
            row_bytes: 0,
        }
    }

    /// Resumes a stored chunk's rows, by their stored bytes, so its rebuilt payload is the one it
    /// was stored with.
    pub(crate) fn resume(scope: &Scope, payload: &[u8]) -> Result<Self, Error> {
        let stored = decode_chunk(scope, payload)?;
        let mut builder = Self::new(scope, stored.index, stored.from);

        for raw in &stored.rows {
            builder.rows.push(raw.get().as_bytes().to_vec());
            builder.row_bytes += raw.get().len();
        }

        Ok(builder)
    }

    fn through(&self) -> u64 {
        self.from + self.rows.len() as u64 - 1
    }

    fn header(&self, through: u64) -> Vec<u8> {
        let scope = json::to_vec(self.scope.as_str()).expect("a scope serializes");
        let mut header = Vec::with_capacity(scope.len() + 96);
        header.extend_from_slice(b"{\"scope\":");
        header.extend_from_slice(&scope);
        header.extend_from_slice(
            format!(
                ",\"index\":{},\"from\":{},\"through\":{through},\"rows\":[",
                self.index, self.from
            )
            .as_bytes(),
        );

        header
    }

    /// The payload size with `extra` more bytes of rows and `count` more rows.
    fn size_with(&self, extra: usize, count: usize) -> usize {
        let rows = self.rows.len() + count;
        let through = self.from + rows as u64 - 1;

        self.header(through).len() + self.row_bytes + extra + rows.saturating_sub(1) + 2
    }

    /// Adds the row at the next position. When the row does not fit, the chunk so far is closed
    /// and returned, and the row starts the next chunk.
    pub(crate) fn push(&mut self, row: &JournalRow) -> Result<Option<BuiltChunk>, Error> {
        let bytes = encode_row(&row.scope, row.position, &row.event)?;
        let expected = self.from + self.rows.len() as u64;

        if row.position != expected {
            return Err(Error::InvalidJournal(format!(
                "compaction of {} expected position {expected}, found {}",
                self.scope, row.position
            )));
        }

        let mut closed = None;
        if !self.rows.is_empty()
            && (self.rows.len() == MAX_CHUNK_ROWS
                || self.size_with(bytes.len(), 1) > MAX_CHUNK_BYTES)
        {
            let next = Self::new(&self.scope, self.index + 1, row.position);
            closed = Some(std::mem::replace(self, next).build());
        }

        self.row_bytes += bytes.len();
        self.rows.push(bytes);

        if self.size_with(0, 0) > MAX_CHUNK_BYTES {
            return Err(Error::InvalidJournal(format!(
                "the row at position {} of {} does not fit a chunk",
                row.position, self.scope
            )));
        }

        Ok(closed)
    }

    /// Closes the chunk.
    pub(crate) fn build(self) -> BuiltChunk {
        let through = self.through();
        let mut payload = self.header(through);
        for (at, row) in self.rows.iter().enumerate() {
            if at > 0 {
                payload.push(b',');
            }
            payload.extend_from_slice(row);
        }
        payload.extend_from_slice(b"]}");

        BuiltChunk {
            index: self.index,
            from: self.from,
            through,
            digest: sha256_hex(&payload),
            payload,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AuditPayload {
    scope: String,
    checkpoint: u64,
    digest: String,
    chunks: usize,
    deleted: u64,
    owner: String,
    token: u64,
    completed_at: i64,
}

/// The audit row a completed compaction writes for its checkpoint: what it covered, who ran it,
/// and when. A run that completes the same checkpoint again overwrites it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionAudit {
    /// The scope.
    pub scope: Scope,
    /// The checkpoint compacted through.
    pub checkpoint: u64,
    /// The manifest digest.
    pub digest: String,
    /// The number of chunks.
    pub chunks: usize,
    /// The journal rows this run deleted.
    pub deleted: u64,
    /// The lease owner that ran it.
    pub owner: String,
    /// The lease's fencing token.
    pub token: u64,
    /// When it completed, in whole Unix seconds.
    pub completed_at: i64,
}

impl CompactionAudit {
    /// The stored payload.
    pub fn payload(&self) -> Vec<u8> {
        json::to_vec(&AuditPayload {
            scope: self.scope.as_str().to_owned(),
            checkpoint: self.checkpoint,
            digest: self.digest.clone(),
            chunks: self.chunks,
            deleted: self.deleted,
            owner: self.owner.clone(),
            token: self.token,
            completed_at: self.completed_at,
        })
        .expect("an audit serializes")
    }

    /// Decodes a stored audit row of `scope`.
    pub fn decode(scope: &Scope, payload: &[u8]) -> Result<Self, Error> {
        let stored: AuditPayload =
            json::from_slice(payload).map_err(|err| corrupt(scope, "audit", err))?;

        if stored.scope != scope.as_str() {
            return Err(corrupt(
                scope,
                "audit",
                format!("it names {:?}", stored.scope),
            ));
        }

        Ok(Self {
            scope: scope.clone(),
            checkpoint: stored.checkpoint,
            digest: stored.digest,
            chunks: stored.chunks,
            deleted: stored.deleted,
            owner: stored.owner,
            token: stored.token,
            completed_at: stored.completed_at,
        })
    }
}

/// What a compaction write expects of a row before it applies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expect {
    /// Nothing.
    Any,
    /// No row at the key.
    Absent,
    /// An engine-owned row (`SYSTEM#JOURNAL`) whose payload is exactly these bytes.
    Payload(Vec<u8>),
}

/// One write of a [`CompactionStore::write_compaction`] call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompactionWrite {
    /// Writes the row, if the row at its key meets `expect`.
    Put {
        /// The row.
        row: ViewRow,
        /// The condition.
        expect: Expect,
    },
    /// Deletes the row at the key if it is absent or owned by `owner` (a view delete's owner
    /// condition, which an absent row passes).
    Delete {
        /// The partition key.
        pk: String,
        /// The sort key.
        sk: String,
        /// The row's owner.
        owner: String,
    },
    /// Writes nothing, and requires the row at the key to meet `expect`.
    Check {
        /// The partition key.
        pk: String,
        /// The sort key.
        sk: String,
        /// The condition.
        expect: Expect,
    },
}

impl CompactionWrite {
    /// The write's key.
    pub fn key(&self) -> (&str, &str) {
        match self {
            Self::Put { row, .. } => (&row.pk, &row.sk),
            Self::Delete { pk, sk, .. } | Self::Check { pk, sk, .. } => (pk, sk),
        }
    }
}

/// What a [`CompactionStore::write_compaction`] call did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactionWritten {
    /// Every write applied.
    Written,
    /// Nothing was written: the rebuild generation is no longer the epoch the call named.
    EpochChanged,
    /// Nothing was written: the write at this index did not meet its condition.
    Refused(usize),
}

/// A journal store that can compact: it writes compaction rows and deletes journal rows in one
/// atomic step fenced on the rebuild generation.
///
/// [`MemRepository`](crate::MemRepository) and the DynamoDB repository with a views table
/// implement it. Application code uses [`compact`](super::compact) and the lease functions rather
/// than this primitive.
pub trait CompactionStore: JournalCompactionStore + RebuildFence + FeatureMarker {
    /// Applies `writes` atomically to the views table: all of them, or none when one of their
    /// conditions fails ([`CompactionWritten::Refused`]) or, with an `epoch`, when the rebuild
    /// generation is not that epoch's ([`CompactionWritten::EpochChanged`]). Between 1 and
    /// [`MAX_COMPACTION_WRITES`] writes, each to a distinct key, or [`Error::InvalidArgument`]
    /// before any request. A repository without a views table refuses with
    /// [`Error::ViewsNotConfigured`].
    fn write_compaction(
        &self,
        epoch: Option<RebuildEpoch>,
        writes: &[CompactionWrite],
    ) -> impl Future<Output = Result<CompactionWritten, Error>> + Send;

    /// The repository's clock, in whole Unix seconds: leases lapse by it (`with_clock` on each
    /// repository, for tests).
    fn compaction_now(&self) -> i64;
}

/// Checks a compaction write batch before any request: 1 to [`MAX_COMPACTION_WRITES`] writes,
/// each to a distinct key.
pub(crate) fn check_compaction_writes(writes: &[CompactionWrite]) -> Result<(), Error> {
    if writes.is_empty() || writes.len() > MAX_COMPACTION_WRITES {
        return Err(Error::InvalidArgument(format!(
            "a compaction write holds 1 to {MAX_COMPACTION_WRITES} writes; got {}",
            writes.len()
        )));
    }

    let mut keys = std::collections::BTreeSet::new();
    for write in writes {
        if !keys.insert(write.key()) {
            return Err(Error::InvalidArgument(format!(
                "a compaction write names {:?} twice",
                write.key()
            )));
        }
    }

    Ok(())
}

/// Reports whether a stored row meets an expectation: `row` is the row at the key, if any.
pub(crate) fn meets(expect: &Expect, row: Option<&ViewRow>) -> bool {
    match expect {
        Expect::Any => true,
        Expect::Absent => row.is_none(),
        Expect::Payload(payload) => {
            row.is_some_and(|row| row.entity_id == COUNTER_OWNER && row.payload == *payload)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::SerializedEvent;
    use crate::journal::scope::{Allocation, set_allocations};

    fn scope() -> Scope {
        Scope::new("world:7").unwrap()
    }

    fn row(position: u64, padding: usize) -> JournalRow {
        let mut event = SerializedEvent {
            id: format!("acct-1:{position}").into(),
            sequence: crate::EventSequence(position as i64),
            event_type: "account:deposited".into(),
            version: crate::EventVersion(1),
            entity_id: "acct-1".into(),
            entity_type: "account".into(),
            payload: format!("{{\"note\":\"{}\"}}", "x".repeat(padding)).into_bytes(),
            ..Default::default()
        };
        set_allocations(&mut event, &[Allocation::new(&scope(), position)]);

        JournalRow {
            scope: scope(),
            position,
            owner: "acct-1".into(),
            event,
        }
    }

    #[test]
    fn keys_and_payloads_have_the_documented_shape() {
        assert_eq!(compaction_pk(&scope()), "journal-compaction#world:7");
        assert_eq!(chunk_sk(3), "chunk#00000000000000000003");
        assert_eq!(audit_sk(12), "audit#00000000000000000012");

        let lease = CompactionLease {
            scope: scope(),
            owner: "compactor-1".into(),
            token: 2,
            lease_expires_at: 1_700_000_060,
            lease_seconds: 0,
        };
        assert_eq!(
            lease.payload(),
            br#"{"scope":"world:7","token":2,"owner":"compactor-1","leaseExpiresAt":1700000060}"#
        );
        assert_eq!(
            lease.released_payload(),
            br#"{"scope":"world:7","token":2}"#
        );
        assert_eq!(
            LeaseRow::decode(&scope(), &lease.payload()).unwrap(),
            LeaseRow::Held(lease.clone())
        );
        assert_eq!(
            LeaseRow::decode(&scope(), &lease.released_payload()).unwrap(),
            LeaseRow::Released {
                scope: scope(),
                token: 2
            }
        );
        assert!(
            LeaseRow::decode(&scope(), br#"{"scope":"world:7","token":2,"owner":"a"}"#).is_err()
        );
        assert!(LeaseRow::decode(&Scope::new("other").unwrap(), &lease.payload()).is_err());
    }

    #[test]
    fn a_manifest_round_trips_and_its_digest_is_its_payload_hash() {
        let manifest = CompactionManifest {
            scope: scope(),
            checkpoint: 4,
            chunks: vec!["a".repeat(64)],
        };
        let payload = manifest.payload();

        assert_eq!(
            payload,
            format!(
                r#"{{"scope":"world:7","checkpoint":4,"chunks":["{}"]}}"#,
                "a".repeat(64)
            )
            .into_bytes()
        );
        assert_eq!(
            CompactionManifest::decode(&scope(), &payload).unwrap(),
            manifest
        );
        assert_eq!(manifest.digest(), sha256_hex(&payload));

        let gone = manifest.gone(2);
        assert_eq!((gone.checkpoint, gone.chunk_count, gone.after), (4, 1, 2));
        assert_eq!(gone.resume_after(), 4);
        assert_eq!(JournalGone::HTTP_STATUS, 410);

        for bad in [
            r#"{"scope":"world:7","checkpoint":0,"chunks":["aa"]}"#.to_owned(),
            r#"{"scope":"world:7","checkpoint":4,"chunks":[]}"#.to_owned(),
            r#"{"scope":"world:7","checkpoint":4,"chunks":["AB"]}"#.to_owned(),
            format!(
                r#"{{"scope":"x","checkpoint":4,"chunks":["{}"]}}"#,
                "a".repeat(64)
            ),
        ] {
            assert!(
                CompactionManifest::decode(&scope(), bad.as_bytes()).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn a_chunk_embeds_the_stored_rows_and_decodes_back() {
        let mut builder = ChunkBuilder::new(&scope(), 0, 1);
        for position in 1..=3 {
            assert!(builder.push(&row(position, 1)).unwrap().is_none());
        }
        let chunk = builder.build();

        let first = encode_row(&scope(), 1, &row(1, 1).event).unwrap();
        assert!(chunk.payload.starts_with(
            br#"{"scope":"world:7","index":0,"from":1,"through":3,"rows":[{"scope":"world:7","position":1,"#
        ));
        assert!(
            chunk
                .payload
                .windows(first.len())
                .any(|window| window == first.as_slice())
        );
        assert_eq!((chunk.from, chunk.through), (1, 3));
        assert_eq!(chunk.digest, sha256_hex(&chunk.payload));

        let decoded = CompactionChunk::decode(&scope(), &chunk.payload).unwrap();
        assert_eq!(decoded.rows, vec![row(1, 1), row(2, 1), row(3, 1)]);

        // Resuming a stored chunk and adding nothing rebuilds the same bytes.
        let resumed = ChunkBuilder::resume(&scope(), &chunk.payload).unwrap();
        assert_eq!(resumed.build(), chunk);
    }

    #[test]
    fn chunks_close_at_the_row_cap_and_the_byte_cap() {
        let mut builder = ChunkBuilder::new(&scope(), 0, 1);
        let mut closed = Vec::new();
        for position in 1..=(MAX_CHUNK_ROWS as u64 + 1) {
            closed.extend(builder.push(&row(position, 1)).unwrap());
        }
        assert_eq!(closed.len(), 1);
        assert_eq!(
            (closed[0].from, closed[0].through),
            (1, MAX_CHUNK_ROWS as u64)
        );
        assert_eq!(
            (builder.index, builder.from),
            (1, MAX_CHUNK_ROWS as u64 + 1)
        );

        // Rows of about 133,000 bytes (the event payload is base64): two fit, the third closes
        // the chunk.
        let mut builder = ChunkBuilder::new(&scope(), 0, 1);
        assert!(builder.push(&row(1, 100_000)).unwrap().is_none());
        assert!(builder.push(&row(2, 100_000)).unwrap().is_none());
        let closed = builder.push(&row(3, 100_000)).unwrap().unwrap();
        assert_eq!((closed.from, closed.through), (1, 2));
        assert!(closed.payload.len() <= MAX_CHUNK_BYTES);

        // A row near the journal's largest cap fits a chunk alone.
        let widest = row(1, 277_000);
        let size = super::super::row_size(&scope(), 1, &widest.event).unwrap();
        assert!(
            (369_000..=super::super::MAX_ROW_BYTES).contains(&size),
            "{size}"
        );
        let mut builder = ChunkBuilder::new(&scope(), 0, 1);
        builder.push(&widest).unwrap();
        assert!(builder.build().payload.len() <= MAX_CHUNK_BYTES);
    }

    #[test]
    fn a_chunk_must_be_contiguous() {
        let mut builder = ChunkBuilder::new(&scope(), 0, 1);
        builder.push(&row(1, 1)).unwrap();
        assert!(matches!(
            builder.push(&row(3, 1)),
            Err(Error::InvalidJournal(_))
        ));

        let chunk = {
            let mut builder = ChunkBuilder::new(&scope(), 0, 1);
            builder.push(&row(1, 1)).unwrap();
            builder.build()
        };
        let lying = String::from_utf8(chunk.payload)
            .unwrap()
            .replace("\"through\":1", "\"through\":2");
        assert!(CompactionChunk::decode(&scope(), lying.as_bytes()).is_err());
    }

    #[test]
    fn writes_are_checked_and_expectations_match() {
        let check = |sk: &str| CompactionWrite::Check {
            pk: "p".into(),
            sk: sk.into(),
            expect: Expect::Any,
        };

        assert!(check_compaction_writes(&[]).is_err());
        assert!(check_compaction_writes(&[check("a"), check("a")]).is_err());
        assert!(check_compaction_writes(&vec![check("a"); 1]).is_ok());
        let many: Vec<_> = (0..=MAX_COMPACTION_WRITES)
            .map(|i| check(&i.to_string()))
            .collect();
        assert!(check_compaction_writes(&many).is_err());

        let row = ViewRow::new("p", "s", COUNTER_OWNER, "t", b"x".to_vec());
        assert!(meets(&Expect::Any, None));
        assert!(meets(&Expect::Absent, None) && !meets(&Expect::Absent, Some(&row)));
        assert!(meets(&Expect::Payload(b"x".to_vec()), Some(&row)));
        assert!(!meets(&Expect::Payload(b"y".to_vec()), Some(&row)));
        assert!(!meets(&Expect::Payload(b"x".to_vec()), None));

        // The same bytes under another owner are not the engine's row.
        let foreign = ViewRow::new("p", "s", "acct-1", "t", b"x".to_vec());
        assert!(!meets(&Expect::Payload(b"x".to_vec()), Some(&foreign)));

        // A compaction row is trusted only at its key, engine-owned, with its entity type.
        let scope = Scope::new("world:7").unwrap();
        let engine = compaction_view(&scope, MANIFEST_SORT_KEY, b"{}".to_vec());
        assert!(check_compaction_row(&scope, MANIFEST_SORT_KEY, engine.clone()).is_ok());
        for (name, bad) in [
            (
                "owner",
                ViewRow {
                    entity_id: "acct-1".into(),
                    ..engine.clone()
                },
            ),
            (
                "type",
                ViewRow {
                    entity_type: "ledger".into(),
                    ..engine.clone()
                },
            ),
            (
                "key",
                ViewRow {
                    sk: LEASE_SORT_KEY.into(),
                    ..engine.clone()
                },
            ),
        ] {
            assert!(
                matches!(
                    check_compaction_row(&scope, MANIFEST_SORT_KEY, bad),
                    Err(Error::ForeignJournalRow { .. })
                ),
                "{name}"
            );
        }
    }
}
