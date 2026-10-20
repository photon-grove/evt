//! Scopes, allocations, and the journal's key layout.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::event::SerializedEvent;
use crate::json;
use crate::metadata::Metadata;

/// The event-metadata extension key that carries an event's allocations. Keys beginning `evt.` are
/// reserved for the engine's extensions: a journaled commit overwrites (or removes) this key on
/// every event it writes, so an application cannot forge an allocation.
pub const ALLOCATION_KEY: &str = crate::projection::ENGINE_ALLOCATION_KEY;

/// The longest scope, in UTF-8 bytes. A row's partition key is the scope behind
/// [`ROW_PARTITION_PREFIX`], which must stay within the view partition-key limit.
pub const MAX_SCOPE_BYTES: usize = 512;

/// The partition-key prefix of a scope's journal rows (`journal#<scope>`).
pub const ROW_PARTITION_PREFIX: &str = "journal#";

/// The partition-key prefix of a scope's counter row (`journal-counter#<scope>`).
pub const COUNTER_PARTITION_PREFIX: &str = "journal-counter#";

/// The sort key of a scope's counter row.
pub const COUNTER_SORT_KEY: &str = "COUNTER";

/// The owner of every counter row: an engine-managed `SYSTEM#` identity that no stream can take
/// (entity IDs in that namespace are refused). A counter is shared by every stream that writes its
/// scope, so no stream can own it; its one constant owner never changes.
pub const COUNTER_OWNER: &str = "SYSTEM#JOURNAL";

/// The informational entity type of a journal row.
pub const ROW_ENTITY_TYPE: &str = "journal";

/// The informational entity type of a counter row.
pub const COUNTER_ENTITY_TYPE: &str = "journal-counter";

/// The width of a row's sort key: a position in zero-padded decimal, so bytewise order is numeric
/// order for every `u64`.
pub const POSITION_WIDTH: usize = 20;

/// A journal scope: a named, independently ordered sequence of rows, such as `world:7` or
/// `world:7/place:3`. Scopes are opaque to the engine.
///
/// A scope is 1 to [`MAX_SCOPE_BYTES`] bytes of UTF-8 without U+0000.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Scope(String);

impl Scope {
    /// Checks and wraps a scope.
    pub fn new(scope: impl Into<String>) -> Result<Self, Error> {
        let scope = scope.into();

        if scope.is_empty() {
            return Err(Error::InvalidJournal("a scope cannot be empty".into()));
        }

        if scope.len() > MAX_SCOPE_BYTES {
            return Err(Error::InvalidJournal(format!(
                "a scope is at most {MAX_SCOPE_BYTES} bytes; {:?}... is {}",
                scope.chars().take(32).collect::<String>(),
                scope.len()
            )));
        }

        if scope.contains('\0') {
            return Err(Error::InvalidJournal(
                "a scope cannot contain U+0000".into(),
            ));
        }

        Ok(Self(scope))
    }

    /// The scope text.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The partition key of the scope's journal rows.
    pub fn row_pk(&self) -> String {
        format!("{ROW_PARTITION_PREFIX}{}", self.0)
    }

    /// The partition key of the scope's counter row.
    pub fn counter_pk(&self) -> String {
        format!("{COUNTER_PARTITION_PREFIX}{}", self.0)
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<&str> for Scope {
    type Error = Error;

    fn try_from(scope: &str) -> Result<Self, Error> {
        Self::new(scope)
    }
}

/// The sort key of the row at `position`: 20 digits, zero-padded.
pub fn row_sk(position: u64) -> String {
    format!("{position:0width$}", width = POSITION_WIDTH)
}

/// Parses a row sort key back to its position. `None` unless it is exactly [`POSITION_WIDTH`]
/// digits.
pub fn parse_row_sk(sk: &str) -> Option<u64> {
    if sk.len() != POSITION_WIDTH || !sk.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }

    sk.parse().ok()
}

/// One position an event holds in one scope. An event's allocations are stored in its metadata
/// under [`ALLOCATION_KEY`], ascending by scope, which is what makes a rebuild independent of the
/// order the commits happened in: the row and the counter value it implies are read from the event.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Allocation {
    /// The scope.
    pub scope: String,
    /// The event's position in the scope, from 1.
    pub position: u64,
}

impl Allocation {
    /// An allocation of `position` in `scope`.
    pub fn new(scope: &Scope, position: u64) -> Self {
        Self {
            scope: scope.as_str().to_owned(),
            position,
        }
    }

    /// The allocation's scope, checked.
    pub fn scope(&self) -> Result<Scope, Error> {
        Scope::new(self.scope.clone())
    }
}

/// Encodes allocations as the metadata extension value: a JSON array of `{scope, position}`
/// objects, ascending by scope.
pub fn encode_allocations(allocations: &[Allocation]) -> String {
    json::to_string(&allocations).expect("allocations serialize")
}

/// Stores an event's allocations in its metadata, replacing whatever the key held; an empty list
/// removes the key.
pub fn set_allocations(event: &mut SerializedEvent, allocations: &[Allocation]) {
    if allocations.is_empty() {
        event.metadata.extensions.remove(ALLOCATION_KEY);

        return;
    }

    event
        .metadata
        .extensions
        .insert(ALLOCATION_KEY.to_owned(), encode_allocations(allocations));
}

/// Reads the allocations out of an event's metadata: the stored scopes and positions, or none for
/// an event that was not journaled. The list must be canonical (ascending by scope, no scope
/// twice, positions from 1), otherwise [`Error::InvalidJournal`]: a rebuild must never guess.
pub fn allocations(metadata: &Metadata) -> Result<Vec<Allocation>, Error> {
    let Some(text) = metadata.extensions.get(ALLOCATION_KEY) else {
        return Ok(Vec::new());
    };

    let allocations: Vec<Allocation> = json::from_slice(text.as_bytes())
        .map_err(|err| Error::InvalidJournal(format!("the {ALLOCATION_KEY} metadata: {err}")))?;

    for allocation in &allocations {
        allocation.scope()?;

        if allocation.position == 0 {
            return Err(Error::InvalidJournal(format!(
                "scope {:?} has position 0; positions start at 1",
                allocation.scope
            )));
        }
    }

    if let Some(pair) = allocations
        .windows(2)
        .find(|pair| pair[0].scope.as_bytes() >= pair[1].scope.as_bytes())
    {
        return Err(Error::InvalidJournal(format!(
            "allocations must ascend by scope with no repeat; {:?} is followed by {:?}",
            pair[0].scope, pair[1].scope
        )));
    }

    Ok(allocations)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes_are_checked() {
        assert!(Scope::new("world:7").is_ok());
        assert!(Scope::new("é/\u{10000}").is_ok());
        assert!(Scope::new("a".repeat(MAX_SCOPE_BYTES)).is_ok());

        for bad in [
            String::new(),
            "a".repeat(MAX_SCOPE_BYTES + 1),
            "nul\0".into(),
        ] {
            assert!(
                matches!(Scope::new(bad.clone()), Err(Error::InvalidJournal(_))),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn a_scope_at_the_cap_fits_the_view_partition_key() {
        let scope = Scope::new("é".repeat(MAX_SCOPE_BYTES / 2)).unwrap();

        assert!(scope.counter_pk().len() <= crate::projection::MAX_VIEW_PK_BYTES);
        assert!(scope.row_pk().len() <= crate::projection::MAX_VIEW_PK_BYTES);
    }

    #[test]
    fn sort_keys_order_numerically_as_bytes() {
        let positions = [1, 2, 9, 10, 99, 100, u64::MAX - 1, u64::MAX];
        let keys: Vec<String> = positions.iter().map(|p| row_sk(*p)).collect();

        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]), "{keys:?}");
        assert_eq!(row_sk(5), "00000000000000000005");
        assert_eq!(row_sk(u64::MAX), "18446744073709551615");

        for position in positions {
            assert_eq!(parse_row_sk(&row_sk(position)), Some(position));
        }

        for bad in ["", "5", "0000000000000000000x", "000000000000000000050"] {
            assert_eq!(parse_row_sk(bad), None, "{bad:?}");
        }
    }

    fn metadata_with(value: &str) -> Metadata {
        let mut metadata = Metadata::default();
        metadata
            .extensions
            .insert(ALLOCATION_KEY.to_owned(), value.to_owned());

        metadata
    }

    #[test]
    fn allocations_round_trip_through_metadata_in_canonical_form() {
        let world = Scope::new("world:1").unwrap();
        let place = Scope::new("world:1/place:9").unwrap();
        let list = vec![Allocation::new(&world, 17), Allocation::new(&place, 3)];

        assert_eq!(
            encode_allocations(&list),
            r#"[{"scope":"world:1","position":17},{"scope":"world:1/place:9","position":3}]"#
        );

        let mut event = SerializedEvent::default();
        set_allocations(&mut event, &list);
        assert_eq!(allocations(&event.metadata).unwrap(), list);

        set_allocations(&mut event, &[]);
        assert!(!event.metadata.extensions.contains_key(ALLOCATION_KEY));
        assert_eq!(allocations(&event.metadata).unwrap(), Vec::new());
    }

    #[test]
    fn a_malformed_or_inconsistent_allocation_is_refused() {
        for (name, value) in [
            ("not json", "{"),
            ("not an array", r#"{"scope":"a","position":1}"#),
            ("position zero", r#"[{"scope":"a","position":0}]"#),
            ("negative", r#"[{"scope":"a","position":-1}]"#),
            ("empty scope", r#"[{"scope":"","position":1}]"#),
            (
                "unsorted",
                r#"[{"scope":"b","position":1},{"scope":"a","position":1}]"#,
            ),
            (
                "repeated",
                r#"[{"scope":"a","position":1},{"scope":"a","position":2}]"#,
            ),
        ] {
            assert!(
                matches!(
                    allocations(&metadata_with(value)),
                    Err(Error::InvalidJournal(_))
                ),
                "{name}"
            );
        }
    }
}
