//! Serialized events and snapshots (the storage format) and the [`DomainEvent`] trait that maps
//! typed events onto it.

use serde::{Deserialize, Serialize};

use crate::ids::{EntityId, EntityType, EventId, EventSequence, EventType, EventVersion};
use crate::json::{self, base64_bytes, null_default};
use crate::metadata::Metadata;

/// An event in its stored form. This is the unit every repository reads and writes, and its JSON
/// encoding is the `detail` of a stream envelope.
///
/// The JSON shape matches Go's `evt.SerializedEvent`: `payload` is base64 (Go `[]byte`), and the
/// entity ID key is `entityId`. Use [`SerializedEvent::to_json`] to get Go-identical bytes.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedEvent {
    /// `{entityId}:{sequence}`.
    #[serde(default)]
    pub id: EventId,

    /// The 1-based position of the event in its entity's stream.
    #[serde(default)]
    pub sequence: EventSequence,

    /// The event kind, such as `account:opened`.
    #[serde(rename = "type", default)]
    pub event_type: EventType,

    /// The payload schema version.
    #[serde(default)]
    pub version: EventVersion,

    /// The entity (stream) the event belongs to.
    #[serde(rename = "entityId", default)]
    pub entity_id: EntityId,

    /// The aggregate type of the entity.
    #[serde(rename = "entityType", default)]
    pub entity_type: EntityType,

    /// The event payload: the JSON encoding of the domain event.
    #[serde(with = "base64_bytes", default)]
    pub payload: Vec<u8>,

    /// Command, trace, and origin metadata.
    #[serde(default, deserialize_with = "null_default")]
    pub metadata: Metadata,
}

impl SerializedEvent {
    /// Encodes the event as JSON bytes identical to Go's `json.Marshal(evt.SerializedEvent)`.
    pub fn to_json(&self) -> serde_json::Result<Vec<u8>> {
        json::to_vec(self)
    }

    /// Decodes an event from its JSON encoding.
    pub fn from_json(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }
}

/// A snapshot of aggregate state. Repositories keep at most one per entity: the latest.
///
/// The JSON shape matches Go's `evt.SerializedSnapshot`. Note the key `entityID` (capital `ID`),
/// which differs from [`SerializedEvent`]'s `entityId`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedSnapshot {
    /// The aggregate type.
    #[serde(rename = "entityType", default)]
    pub entity_type: EntityType,

    /// The entity the snapshot belongs to.
    #[serde(rename = "entityID", default)]
    pub entity_id: EntityId,

    /// The snapshot's own sequence: 1 for the first snapshot, then incremented by one per snapshot.
    #[serde(default)]
    pub sequence: EventSequence,

    /// The last event sequence folded into the snapshot.
    #[serde(rename = "eventSequence", default)]
    pub event_sequence: EventSequence,

    /// The JSON encoding of the aggregate state.
    #[serde(with = "base64_bytes", default)]
    pub payload: Vec<u8>,
}

impl SerializedSnapshot {
    /// Encodes the snapshot as JSON bytes identical to Go's `json.Marshal(evt.SerializedSnapshot)`.
    pub fn to_json(&self) -> serde_json::Result<Vec<u8>> {
        json::to_vec(self)
    }

    /// Decodes a snapshot from its JSON encoding.
    pub fn from_json(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }
}

/// Why a stored payload could not be turned into a typed event.
#[derive(Debug, thiserror::Error)]
pub enum PayloadError {
    /// No variant is registered for the event type.
    #[error("unknown event type: {0}")]
    UnknownType(String),

    /// The payload did not match the variant's shape.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// A typed domain event: usually an enum with one variant per event kind.
///
/// The stored payload is the JSON of the variant's inner value, not a tagged enum, so the same
/// stored event can be read by the Go framework. Implement it with [`domain_events!`] rather than
/// by hand.
///
/// [`domain_events!`]: crate::domain_events
pub trait DomainEvent: Sized + Send + Sync + 'static {
    /// The stored event type, such as `account:opened`.
    fn event_type(&self) -> &'static str;

    /// The current payload schema version for this event.
    fn event_version(&self) -> EventVersion;

    /// Encodes the payload. Use [`crate::json::to_vec`] so bytes match Go's encoder.
    fn to_payload(&self) -> serde_json::Result<Vec<u8>>;

    /// Decodes a payload of the given type. Upcasters have already run, so `version` is current.
    fn from_payload(
        event_type: &str,
        version: EventVersion,
        payload: &[u8],
    ) -> Result<Self, PayloadError>;
}

/// Declares a domain event enum and implements [`DomainEvent`] for it.
///
/// Each variant wraps a payload struct and names its stored event type and schema version. The
/// macro also implements `From<Payload>` for each variant.
///
/// ```
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// pub struct Opened {
///     pub owner: String,
/// }
///
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// #[serde(rename_all = "camelCase")]
/// pub struct Deposited {
///     pub amount_cents: i64,
/// }
///
/// photon_evt::domain_events! {
///     #[derive(Clone, Debug, PartialEq)]
///     pub enum AccountEvent {
///         Opened(Opened) => ("account:opened", 1),
///         Deposited(Deposited) => ("account:deposited", 1),
///     }
/// }
///
/// use photon_evt::DomainEvent;
///
/// let event = AccountEvent::from(Deposited { amount_cents: 250 });
/// assert_eq!(event.event_type(), "account:deposited");
/// assert_eq!(event.to_payload().unwrap(), br#"{"amountCents":250}"#);
/// ```
#[macro_export]
macro_rules! domain_events {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident($payload:ty) => ($event_type:literal, $version:literal)
            ),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        $vis enum $name {
            $(
                $(#[$variant_meta])*
                $variant($payload),
            )+
        }

        impl $crate::DomainEvent for $name {
            fn event_type(&self) -> &'static str {
                match self {
                    $(Self::$variant(_) => $event_type,)+
                }
            }

            fn event_version(&self) -> $crate::EventVersion {
                match self {
                    $(Self::$variant(_) => $crate::EventVersion($version),)+
                }
            }

            fn to_payload(&self) -> $crate::__private::serde_json::Result<::std::vec::Vec<u8>> {
                match self {
                    $(Self::$variant(payload) => $crate::json::to_vec(payload),)+
                }
            }

            fn from_payload(
                event_type: &str,
                _version: $crate::EventVersion,
                payload: &[u8],
            ) -> ::std::result::Result<Self, $crate::PayloadError> {
                match event_type {
                    $($event_type => Ok(Self::$variant($crate::__private::serde_json::from_slice(payload)?)),)+
                    other => Err($crate::PayloadError::UnknownType(other.to_owned())),
                }
            }
        }

        $(
            impl ::std::convert::From<$payload> for $name {
                fn from(payload: $payload) -> Self {
                    Self::$variant(payload)
                }
            }
        )+
    };
}
