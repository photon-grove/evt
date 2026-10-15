//! The PostgreSQL table layout of the event log, independent of any driver.
//!
//! Events live in one table keyed by `(entity_id, sequence)`, and snapshots live in a second table
//! keyed by `entity_id`. `payload` is the raw JSON bytes (`bytea`). `metadata` is `jsonb`, so
//! PostgreSQL normalizes its whitespace and key order and readers must compare it as JSON.

use crate::error::Error;
use crate::event::{SerializedEvent, SerializedSnapshot};
use crate::ids::{EntityId, EntityType, EventId, EventSequence, EventType, EventVersion};
use crate::json;
use crate::metadata::Metadata;

/// The default event table.
pub const DEFAULT_EVENTS_TABLE: &str = "evt_events";

/// The default snapshot table.
pub const DEFAULT_SNAPSHOTS_TABLE: &str = "evt_snapshots";

/// The reserved infix engine relations derive from the events table name, so several event logs
/// in one database never share engine rows. Configured table names may not contain it
/// (case-insensitively), so one log's events or snapshots table can never equal another log's
/// derived relation.
pub const RESERVED_INFIX: &str = "__evt_";

/// The longest events table name allowed. The longest relation derived from it,
/// `{events}__evt_rebuild_generation_pkey`, adds 29 bytes, and PostgreSQL truncates identifiers
/// to 63 bytes (v0.9).
pub const MAX_EVENTS_TABLE_NAME_LEN: usize = 63 - "__evt_rebuild_generation_pkey".len();

/// The feature-marker table for an events table (`evt_events` yields
/// `evt_events__evt_features`).
pub fn features_table(events_table: &str) -> String {
    format!("{events_table}{RESERVED_INFIX}features")
}

/// Returns the idempotent DDL for the event and snapshot tables. It is the same DDL Go's
/// `postgres.Repository.EnsureSchema` runs, so either implementation can create the schema.
pub fn schema_ddl(events_table: &str, snapshots_table: &str) -> String {
    format!(
        r#"
CREATE TABLE IF NOT EXISTS {events_table} (
    entity_id   text   NOT NULL,
    sequence    bigint NOT NULL,
    event_id    text   NOT NULL,
    event_type  text   NOT NULL,
    version     bigint NOT NULL,
    entity_type text   NOT NULL,
    payload     bytea  NOT NULL,
    metadata    jsonb  NOT NULL DEFAULT '{{}}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_id, sequence)
);
CREATE INDEX IF NOT EXISTS {events_table}_entity_type_idx ON {events_table} (entity_type);

CREATE TABLE IF NOT EXISTS {snapshots_table} (
    entity_id      text   PRIMARY KEY,
    entity_type    text   NOT NULL,
    sequence       bigint NOT NULL,
    event_sequence bigint NOT NULL,
    payload        bytea  NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {features} (
    id                integer PRIMARY KEY,
    version           bigint NOT NULL,
    features          text   NOT NULL,
    history_dependent text   NOT NULL DEFAULT '[]',
    retention         text   NOT NULL DEFAULT '[]'
);
"#,
        features = features_table(events_table)
    )
}

/// PostgreSQL's reserved keywords (`pg_get_keywords()` categories `R` and `T`), which cannot be
/// used as unquoted table names.
const RESERVED_KEYWORDS: &[&str] = &[
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "authorization",
    "binary",
    "both",
    "case",
    "cast",
    "check",
    "collate",
    "collation",
    "column",
    "concurrently",
    "constraint",
    "create",
    "cross",
    "current_catalog",
    "current_date",
    "current_role",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_user",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "false",
    "fetch",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "grant",
    "group",
    "having",
    "ilike",
    "in",
    "initially",
    "inner",
    "intersect",
    "into",
    "is",
    "isnull",
    "join",
    "lateral",
    "leading",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "natural",
    "not",
    "notnull",
    "null",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "outer",
    "overlaps",
    "placing",
    "primary",
    "references",
    "returning",
    "right",
    "select",
    "session_user",
    "similar",
    "some",
    "symmetric",
    "system_user",
    "table",
    "tablesample",
    "then",
    "to",
    "trailing",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "variadic",
    "verbose",
    "when",
    "where",
    "window",
    "with",
];

/// The longest snapshot table name allowed. PostgreSQL truncates identifiers to 63 bytes, and the
/// longest name derived from a snapshot table, `{snapshots_table}_pkey`, adds 6.
pub const MAX_SNAPSHOTS_TABLE_NAME_LEN: usize = 63 - "_pkey".len();

/// Takes the per-entity transaction lock (`$1` is the entity ID) that serializes commits and
/// snapshot writes for one entity, so a snapshot cannot raise the floor while a commit below it is
/// in flight. Go and Rust run this exact statement, so their writers exclude each other too.
pub const ENTITY_LOCK: &str = "SELECT pg_advisory_xact_lock(1702261760, hashtext($1))";

/// The shared advisory lock a commit's feature-marker check takes (`$1` is the lock key from
/// [`features_lock_key`]). It is held to `COMMIT`, so a declaration's exclusive lock waits for
/// every commit holding the shared lock and cannot slip between a commit's version check and its
/// commit. Commits do not serialize against each other.
pub const FEATURES_LOCK_SHARED: &str =
    "SELECT pg_advisory_xact_lock_shared(1702261761, hashtext($1))";

/// The exclusive advisory lock a manifest declaration takes (`$1` is the lock key from
/// [`features_lock_key`]) around its short read-modify-write.
pub const FEATURES_LOCK_EXCLUSIVE: &str = "SELECT pg_advisory_xact_lock(1702261761, hashtext($1))";

/// The advisory-lock key of an events table's feature marker, derived from the configured events
/// table name so two event logs in one database never block each other's engine rows.
pub fn features_lock_key(events_table: &str) -> String {
    format!("{events_table}:features")
}

/// Reports whether a table name is safe to use unquoted: it matches `^[A-Za-z_][A-Za-z0-9_]*$`,
/// is at most `max_len` bytes, and is not a reserved keyword.
pub fn is_valid_identifier(name: &str, max_len: usize) -> bool {
    let mut chars = name.chars();

    name.len() <= max_len
        && chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !RESERVED_KEYWORDS
            .iter()
            .any(|keyword| keyword.eq_ignore_ascii_case(name))
}

/// Checks a pair of table names: each must be a valid identifier within its length cap and must
/// not contain the reserved infix, and no two relations the schema creates (the tables, the
/// entity-type index, the feature-marker table, and each table's `{table}_pkey` index) may share
/// a name. Unquoted identifiers fold to lower case, so names are compared that way.
/// `CREATE TABLE IF NOT EXISTS` would otherwise skip a table whose name an index already took.
pub fn check_table_names(events_table: &str, snapshots_table: &str) -> Result<(), String> {
    for (name, max_len) in [
        (events_table, MAX_EVENTS_TABLE_NAME_LEN),
        (snapshots_table, MAX_SNAPSHOTS_TABLE_NAME_LEN),
    ] {
        if !is_valid_identifier(name, max_len) {
            return Err(format!("invalid table name {name:?}"));
        }

        if name.to_ascii_lowercase().contains(RESERVED_INFIX) {
            return Err(format!(
                "table name {name:?} must not contain the reserved infix {RESERVED_INFIX:?}"
            ));
        }
    }

    let events = events_table.to_ascii_lowercase();
    let snapshots = snapshots_table.to_ascii_lowercase();
    let features = features_table(&events);
    let relations = [
        events.clone(),
        format!("{events}_pkey"),
        format!("{events}{RESERVED_INFIX}entity_type_idx"),
        features.clone(),
        format!("{features}_pkey"),
        snapshots.clone(),
        format!("{snapshots}_pkey"),
    ];

    for (index, relation) in relations.iter().enumerate() {
        if relations[..index].contains(relation) {
            return Err(format!(
                "tables {events_table:?} and {snapshots_table:?} would both use the name {relation:?}"
            ));
        }
    }

    Ok(())
}

/// One row of the event table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventRow {
    /// `entity_id`.
    pub entity_id: String,
    /// `sequence`.
    pub sequence: i64,
    /// `event_id`.
    pub event_id: String,
    /// `event_type`.
    pub event_type: String,
    /// `version`.
    pub version: i64,
    /// `entity_type`.
    pub entity_type: String,
    /// `payload` (bytea).
    pub payload: Vec<u8>,
    /// `metadata` (jsonb), as JSON text.
    pub metadata: String,
}

impl EventRow {
    /// Encodes an event as a row.
    pub fn from_event(event: &SerializedEvent) -> Result<Self, Error> {
        Ok(Self {
            entity_id: event.entity_id.to_string(),
            sequence: event.sequence.0,
            event_id: event.id.to_string(),
            event_type: event.event_type.to_string(),
            version: event.version.0,
            entity_type: event.entity_type.to_string(),
            payload: event.payload.clone(),
            metadata: json::to_string(&event.metadata)?,
        })
    }

    /// Decodes a row into an event. Empty or `null` metadata decodes to the default, as in Go.
    pub fn into_event(self) -> Result<SerializedEvent, Error> {
        let metadata = if self.metadata.trim().is_empty() {
            Metadata::default()
        } else {
            serde_json::from_str::<Option<Metadata>>(&self.metadata)
                .map_err(|err| {
                    Error::InvalidRecord(format!("event {} metadata: {err}", self.event_id))
                })?
                .unwrap_or_default()
        };

        Ok(SerializedEvent {
            id: EventId::new(self.event_id),
            sequence: EventSequence(self.sequence),
            event_type: EventType::new(self.event_type),
            version: EventVersion(self.version),
            entity_id: EntityId::new(self.entity_id),
            entity_type: EntityType::new(self.entity_type),
            payload: self.payload,
            metadata,
        })
    }
}

/// One row of the snapshot table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotRow {
    /// `entity_id`.
    pub entity_id: String,
    /// `entity_type`.
    pub entity_type: String,
    /// `sequence`.
    pub sequence: i64,
    /// `event_sequence`.
    pub event_sequence: i64,
    /// `payload` (bytea).
    pub payload: Vec<u8>,
}

impl SnapshotRow {
    /// Encodes a snapshot as a row.
    pub fn from_snapshot(snapshot: &SerializedSnapshot) -> Self {
        Self {
            entity_id: snapshot.entity_id.to_string(),
            entity_type: snapshot.entity_type.to_string(),
            sequence: snapshot.sequence.0,
            event_sequence: snapshot.event_sequence.0,
            payload: snapshot.payload.clone(),
        }
    }

    /// Decodes a row into a snapshot.
    pub fn into_snapshot(self) -> SerializedSnapshot {
        SerializedSnapshot {
            entity_type: EntityType::new(self.entity_type),
            entity_id: EntityId::new(self.entity_id),
            sequence: EventSequence(self.sequence),
            event_sequence: EventSequence(self.event_sequence),
            payload: self.payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_or_empty_metadata_decodes_to_the_default() {
        for metadata in ["null", ""] {
            let row = EventRow {
                entity_id: "acct-1".into(),
                sequence: 1,
                event_id: "acct-1:1".into(),
                event_type: "account:opened".into(),
                version: 1,
                entity_type: "account".into(),
                payload: b"{}".to_vec(),
                metadata: metadata.into(),
            };

            assert_eq!(row.into_event().unwrap().metadata, Metadata::default());
        }
    }
}
