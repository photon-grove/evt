//! Identifier and sequence newtypes shared by every event, snapshot, and repository.
//!
//! The string identifiers serialize as plain JSON strings and the numeric types as plain JSON
//! numbers, matching the Go `evt` types they mirror. A JSON `null` decodes to the empty or zero
//! value, the same way Go's `encoding/json` leaves a field untouched on `null`.

use std::borrow::Borrow;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

macro_rules! string_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Creates an identifier from any string-like value.
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns the identifier as a string slice.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Reports whether the identifier is empty.
            pub fn is_empty(&self) -> bool {
                self.0.is_empty()
            }

            /// Consumes the identifier and returns the inner string.
            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                Ok(Self(Option::<String>::deserialize(deserializer)?.unwrap_or_default()))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_owned())
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&String> for $name {
            fn from(value: &String) -> Self {
                Self(value.clone())
            }
        }

        impl From<&$name> for $name {
            fn from(value: &$name) -> Self {
                value.clone()
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl Borrow<str> for $name {
            fn borrow(&self) -> &str {
                &self.0
            }
        }

        impl PartialEq<str> for $name {
            fn eq(&self, other: &str) -> bool {
                self.0 == other
            }
        }

        impl PartialEq<&str> for $name {
            fn eq(&self, other: &&str) -> bool {
                self.0 == *other
            }
        }
    };
}

string_id!(
    /// Identifies one aggregate instance (one event stream). It is the DynamoDB partition key and
    /// the PostgreSQL `entity_id` column.
    EntityId
);

string_id!(
    /// Identifies an aggregate type, such as `account`.
    EntityType
);

string_id!(
    /// Identifies an event kind, such as `account:opened`.
    EventType
);

string_id!(
    /// A stable event identifier composed of the entity ID and sequence (`{entityId}:{sequence}`).
    EventId
);

string_id!(
    /// Identifies one command invocation. Stored in event metadata and used to reject duplicate
    /// commands.
    CommandId
);

impl EventId {
    /// Builds the stable event ID for an entity and sequence: `{entityId}:{sequence}`.
    pub fn for_event(entity_id: &EntityId, sequence: EventSequence) -> Self {
        Self(format!("{entity_id}:{sequence}"))
    }
}

macro_rules! number_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(
            Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize,
        )]
        #[serde(transparent)]
        pub struct $name(pub i64);

        impl $name {
            /// Returns the inner value.
            pub const fn get(self) -> i64 {
                self.0
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                Ok(Self(Option::<i64>::deserialize(deserializer)?.unwrap_or_default()))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }

        impl From<i64> for $name {
            fn from(value: i64) -> Self {
                Self(value)
            }
        }

        impl From<$name> for i64 {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

number_id!(
    /// The 1-based position of an event within its entity's stream. Snapshot rows use 0.
    EventSequence
);

number_id!(
    /// The schema version of an event payload. Upcasters migrate older versions on read.
    EventVersion
);

impl EventSequence {
    /// The sequence of an entity with no events.
    pub const ZERO: Self = Self(0);

    /// Returns the next sequence, saturating at `i64::MAX`. Use
    /// [`checked_next`](Self::checked_next) where reaching the maximum must be an error.
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }

    /// Returns the next sequence, or `None` at `i64::MAX`.
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(next) => Some(Self(next)),
            None => None,
        }
    }
}
