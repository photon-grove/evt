//! The PostgreSQL table layout of the event log, independent of any driver.
//!
//! Events live in one table keyed by `(entity_id, sequence)`; snapshots live in a second table
//! keyed by `entity_id`. `payload` is the raw JSON bytes (`bytea`); `metadata` is `jsonb`, so
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
"#
    )
}

/// Reports whether a table name is a safe SQL identifier (`^[A-Za-z_][A-Za-z0-9_]*$`).
pub fn is_valid_identifier(name: &str) -> bool {
    let mut chars = name.chars();

    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
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

    /// Decodes a row into an event. Empty metadata decodes to the default.
    pub fn into_event(self) -> Result<SerializedEvent, Error> {
        let metadata: Metadata = if self.metadata.trim().is_empty() {
            Metadata::default()
        } else {
            serde_json::from_str(&self.metadata).map_err(|err| {
                Error::InvalidRecord(format!("event {} metadata: {err}", self.event_id))
            })?
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
