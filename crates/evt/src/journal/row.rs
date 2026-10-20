//! Journal rows and counter rows: their stored payloads and the view rows that hold them.

use serde::{Deserialize, Serialize};

use super::scope::{
    COUNTER_ENTITY_TYPE, COUNTER_OWNER, COUNTER_SORT_KEY, ROW_ENTITY_TYPE, Scope, parse_row_sk,
    row_sk,
};
use crate::error::Error;
use crate::event::SerializedEvent;
use crate::ids::EntityId;
use crate::json;
use crate::projection::ViewRow;

#[derive(Serialize)]
struct RowOut<'a> {
    scope: &'a str,
    position: u64,
    event: &'a SerializedEvent,
}

#[derive(Deserialize)]
struct RowIn {
    scope: String,
    position: u64,
    event: SerializedEvent,
}

#[derive(Serialize, Deserialize)]
struct CounterPayload {
    scope: String,
    position: u64,
}

/// One row of a scope's journal: an event, at the position it holds in the scope.
///
/// The stored payload is the JSON `{"scope":…,"position":…,"event":<the stored event>}`, the
/// event encoded exactly as a stream envelope's `detail`. Its length in bytes is the row's size,
/// the number the row-size guard measures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JournalRow {
    /// The scope.
    pub scope: Scope,
    /// The position in the scope, from 1.
    pub position: u64,
    /// The stream that wrote the event, which owns the row.
    pub owner: EntityId,
    /// The event as it was stored, allocations included.
    pub event: SerializedEvent,
}

/// Encodes a journal row's payload.
pub fn encode_row(scope: &Scope, position: u64, event: &SerializedEvent) -> Result<Vec<u8>, Error> {
    Ok(json::to_vec(&RowOut {
        scope: scope.as_str(),
        position,
        event,
    })?)
}

/// The size in bytes of the row an event would hold at `position` in `scope`: the length of its
/// stored payload.
pub fn row_size(scope: &Scope, position: u64, event: &SerializedEvent) -> Result<usize, Error> {
    Ok(encode_row(scope, position, event)?.len())
}

/// The view row that holds an event's journal row: `journal#<scope>` at the 20-digit position,
/// owned by the event's stream.
pub fn row_view(scope: &Scope, position: u64, event: &SerializedEvent) -> Result<ViewRow, Error> {
    Ok(ViewRow::new(
        scope.row_pk(),
        row_sk(position),
        event.entity_id.as_str(),
        ROW_ENTITY_TYPE,
        encode_row(scope, position, event)?,
    ))
}

/// The payload of a scope's counter row at `position`.
pub fn counter_payload(scope: &Scope, position: u64) -> Vec<u8> {
    json::to_vec(&CounterPayload {
        scope: scope.as_str().to_owned(),
        position,
    })
    .expect("a counter serializes")
}

/// The view row that holds a scope's counter: `journal-counter#<scope>` at `COUNTER`, owned by the
/// engine ([`COUNTER_OWNER`]), holding the highest position allocated.
pub fn counter_view(scope: &Scope, position: u64) -> ViewRow {
    ViewRow::new(
        scope.counter_pk(),
        COUNTER_SORT_KEY,
        COUNTER_OWNER,
        COUNTER_ENTITY_TYPE,
        counter_payload(scope, position),
    )
}

/// Reads a counter row's position, checking the row is the counter of `scope`.
pub fn read_counter(scope: &Scope, row: &ViewRow) -> Result<u64, Error> {
    let corrupt = |why: String| Error::InvalidRecord(format!("journal counter of {scope}: {why}"));

    if row.pk != scope.counter_pk() || row.sk != COUNTER_SORT_KEY {
        return Err(corrupt(format!("found the row {:?}/{:?}", row.pk, row.sk)));
    }

    let payload: CounterPayload = json::from_slice(&row.payload)
        .map_err(|err| corrupt(format!("the payload does not decode: {err}")))?;

    if payload.scope != scope.as_str() {
        return Err(corrupt(format!("it names the scope {:?}", payload.scope)));
    }

    Ok(payload.position)
}

impl JournalRow {
    /// Decodes a stored journal row, checking that its key, owner, and payload agree. A mismatch
    /// is [`Error::InvalidRecord`]: the row is not one this journal wrote.
    pub fn from_view(scope: &Scope, row: &ViewRow) -> Result<Self, Error> {
        let corrupt = |why: String| {
            Error::InvalidRecord(format!(
                "journal row {:?}/{:?} of {scope}: {why}",
                row.pk, row.sk
            ))
        };

        if row.pk != scope.row_pk() {
            return Err(corrupt("it is in another partition".into()));
        }

        let position = parse_row_sk(&row.sk)
            .ok_or_else(|| corrupt("the sort key is not a position".into()))?;
        let payload: RowIn = json::from_slice(&row.payload)
            .map_err(|err| corrupt(format!("the payload does not decode: {err}")))?;

        if payload.scope != scope.as_str() || payload.position != position {
            return Err(corrupt(format!(
                "the payload says {:?} at {}",
                payload.scope, payload.position
            )));
        }

        if payload.event.entity_id.as_str() != row.entity_id {
            return Err(corrupt(format!(
                "the owner {:?} is not the event's stream {}",
                row.entity_id, payload.event.entity_id
            )));
        }

        Ok(Self {
            scope: scope.clone(),
            position,
            owner: EntityId::new(row.entity_id.clone()),
            event: payload.event,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::scope::{Allocation, set_allocations};

    fn event() -> SerializedEvent {
        let mut event = SerializedEvent {
            id: "ledger-1:2".into(),
            sequence: crate::EventSequence(2),
            event_type: "ledger:added".into(),
            version: crate::EventVersion(1),
            entity_id: "ledger-1".into(),
            entity_type: "ledger".into(),
            payload: br#"{"amount":5}"#.to_vec(),
            ..Default::default()
        };
        let scope = Scope::new("world:1").unwrap();
        set_allocations(&mut event, &[Allocation::new(&scope, 4)]);

        event
    }

    #[test]
    fn a_row_round_trips_and_its_size_is_its_payload_length() {
        let scope = Scope::new("world:1").unwrap();
        let event = event();
        let view = row_view(&scope, 4, &event).unwrap();

        assert_eq!(view.pk, "journal#world:1");
        assert_eq!(view.sk, "00000000000000000004");
        assert_eq!(view.entity_id, "ledger-1");
        assert_eq!(view.entity_type, "journal");
        assert!(
            view.payload
                .starts_with(br#"{"scope":"world:1","position":4,"event":{"id":"ledger-1:2""#)
        );
        assert_eq!(row_size(&scope, 4, &event).unwrap(), view.payload.len());

        let row = JournalRow::from_view(&scope, &view).unwrap();
        assert_eq!(row.position, 4);
        assert_eq!(row.owner, EntityId::new("ledger-1"));
        assert_eq!(row.event, event);
    }

    #[test]
    fn a_row_that_disagrees_with_its_key_is_refused() {
        let scope = Scope::new("world:1").unwrap();
        let good = row_view(&scope, 4, &event()).unwrap();

        let mut other_position = good.clone();
        other_position.sk = row_sk(5);
        let mut other_owner = good.clone();
        other_owner.entity_id = "ledger-2".into();
        let mut other_scope = good.clone();
        other_scope.pk = Scope::new("world:2").unwrap().row_pk();
        let mut garbage = good.clone();
        garbage.payload = b"{".to_vec();
        let mut bad_sk = good.clone();
        bad_sk.sk = "4".into();

        for (name, row) in [
            ("position", other_position),
            ("owner", other_owner),
            ("scope", other_scope),
            ("payload", garbage),
            ("sort key", bad_sk),
        ] {
            assert!(
                matches!(
                    JournalRow::from_view(&scope, &row),
                    Err(Error::InvalidRecord(_))
                ),
                "{name}"
            );
        }
    }

    #[test]
    fn a_counter_round_trips_and_is_engine_owned() {
        let scope = Scope::new("world:1").unwrap();
        let view = counter_view(&scope, 12);

        assert_eq!(view.pk, "journal-counter#world:1");
        assert_eq!(view.sk, "COUNTER");
        assert_eq!(view.entity_id, "SYSTEM#JOURNAL");
        assert_eq!(view.payload, br#"{"scope":"world:1","position":12}"#);
        assert_eq!(read_counter(&scope, &view).unwrap(), 12);

        let other = Scope::new("world:2").unwrap();
        assert!(read_counter(&other, &view).is_err());
    }
}
