//! `NEW_IMAGE` decode: the committed events in an event-log table's stream.

use photon_evt::wire::dynamo::{AttributeValue, Item, event_from_item};
use photon_evt::{COMMAND_OUTCOME_PREFIX, SYSTEM_PREFIX, SerializedEvent};

use crate::error::DecodeError;
use crate::record::{EventName, StreamRecord};

/// A committed event, as one stream record delivered it.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct StreamedEvent {
    /// The event as it was stored. Upcasting is the consumer's: apply its upcasters before it
    /// decodes the payload.
    pub event: SerializedEvent,
    /// The record's sequence number in its shard.
    pub sequence_number: String,
    /// When the change was made, in Unix seconds (approximate).
    pub approximate_creation_date_time: Option<f64>,
}

impl StreamedEvent {
    /// The key a consumer deduplicates on: the event ID, `{entityId}:{sequence}`. A redelivered
    /// record carries the same event, so the key is stable across retries, and no two events share
    /// it.
    pub fn idempotency_key(&self) -> &str {
        self.event.id.as_str()
    }
}

/// Whether an item key is an event item of the event log: a string `pk` outside the reserved
/// namespaces (`SYSTEM#`, `command-outcome#`) and a number `sk` of at least 1.
///
/// Every other item of the table is engine metadata: the stream head (`sk = -1`), the inline
/// snapshot (`sk = 0`), the feature marker and the rebuild fence (`SYSTEM#…`), and command outcomes
/// (`command-outcome#…`, at `sk = 1`).
pub fn is_event_key(keys: &Item) -> bool {
    let pk = match keys.get("pk") {
        Some(AttributeValue::S(pk)) => pk,
        _ => return false,
    };

    if pk.starts_with(SYSTEM_PREFIX) || pk.starts_with(COMMAND_OUTCOME_PREFIX) {
        return false;
    }

    matches!(keys.get("sk"), Some(AttributeValue::N(sk)) if sk.parse::<i64>().is_ok_and(|sk| sk >= 1))
}

/// Decodes the event a record inserted.
///
/// `Ok(None)` for every record that is not the insert of an event item: a `MODIFY` or `REMOVE`
/// (events are append-only, so neither is a new event; a compaction or a time-to-live expiry
/// removes them), and the engine's metadata items ([`is_event_key`]). An event insert without a
/// new image, or whose item does not decode, is an error.
pub fn decode_event(record: &StreamRecord) -> Result<Option<StreamedEvent>, DecodeError> {
    if record.event_name != EventName::Insert {
        return Ok(None);
    }

    let keys = if record.change.keys.is_empty() {
        record.change.new_image.as_ref()
    } else {
        Some(&record.change.keys)
    };

    if !keys.is_some_and(is_event_key) {
        return Ok(None);
    }

    let Some(image) = &record.change.new_image else {
        return Err(DecodeError::MissingNewImage {
            sequence_number: record.sequence_number().to_owned(),
            view_type: record
                .change
                .stream_view_type
                .map_or("unset", |view_type| view_type.as_str())
                .to_owned(),
        });
    };

    let event = event_from_item(image).map_err(|source| DecodeError::Record {
        sequence_number: record.sequence_number().to_owned(),
        source: Box::new(source),
    })?;

    Ok(Some(StreamedEvent {
        event,
        sequence_number: record.sequence_number().to_owned(),
        approximate_creation_date_time: record.change.approximate_creation_date_time,
    }))
}

#[cfg(test)]
mod tests {
    use photon_evt::wire::dynamo::event_item;
    use photon_evt::{EventSequence, EventVersion, Metadata};

    use super::*;
    use crate::record::StreamViewType;

    fn event() -> SerializedEvent {
        SerializedEvent {
            id: "acct-1:3".into(),
            sequence: EventSequence(3),
            event_type: "account:deposited".into(),
            version: EventVersion(1),
            entity_id: "acct-1".into(),
            entity_type: "account".into(),
            payload: br#"{"amount":5}"#.to_vec(),
            metadata: Metadata::default().with_command_id("cmd-1"),
        }
    }

    fn keys(pk: &str, sk: AttributeValue) -> Item {
        Item::from([
            ("pk".into(), AttributeValue::S(pk.into())),
            ("sk".into(), sk),
        ])
    }

    fn n(value: i64) -> AttributeValue {
        AttributeValue::N(value.to_string())
    }

    #[test]
    fn an_inserted_event_decodes_with_its_idempotency_key() {
        let item = event_item(&event(), None).unwrap();
        let record =
            StreamRecord::new(EventName::Insert, keys("acct-1", n(3)), "7").with_new_image(item);

        let decoded = decode_event(&record).unwrap().unwrap();

        assert_eq!(decoded.event, event());
        assert_eq!(decoded.sequence_number, "7");
        assert_eq!(decoded.idempotency_key(), "acct-1:3");
    }

    #[test]
    fn engine_items_and_non_inserts_are_not_events() {
        let image = event_item(&event(), None).unwrap();

        for (name, record) in [
            (
                "modify",
                StreamRecord::new(EventName::Modify, keys("acct-1", n(3)), "1"),
            ),
            (
                "remove",
                StreamRecord::new(EventName::Remove, keys("acct-1", n(3)), "1"),
            ),
            (
                "head",
                StreamRecord::new(EventName::Insert, keys("acct-1", n(-1)), "1"),
            ),
            (
                "snapshot",
                StreamRecord::new(EventName::Insert, keys("acct-1", n(0)), "1"),
            ),
            (
                "marker",
                StreamRecord::new(EventName::Insert, keys("SYSTEM#FEATURES", n(0)), "1"),
            ),
            (
                "outcome",
                StreamRecord::new(EventName::Insert, keys("command-outcome#cmd-1", n(1)), "1"),
            ),
            (
                "view row",
                StreamRecord::new(
                    EventName::Insert,
                    keys("account#acct-1", AttributeValue::S("VIEW".into())),
                    "1",
                ),
            ),
        ] {
            let record = record.with_new_image(image.clone());
            assert_eq!(decode_event(&record).unwrap(), None, "{name}");
        }
    }

    #[test]
    fn an_event_without_a_new_image_or_with_a_corrupt_item_is_an_error() {
        let mut keys_only = StreamRecord::new(EventName::Insert, keys("acct-1", n(3)), "9");
        keys_only.change.stream_view_type = Some(StreamViewType::KeysOnly);

        let err = decode_event(&keys_only).unwrap_err();
        assert!(
            matches!(err, DecodeError::MissingNewImage { ref view_type, .. } if view_type == "KEYS_ONLY")
        );
        assert_eq!(err.sequence_number(), "9");

        let mut item = event_item(&event(), None).unwrap();
        item.remove("metadata");
        let corrupt =
            StreamRecord::new(EventName::Insert, keys("acct-1", n(3)), "10").with_new_image(item);

        assert!(matches!(
            decode_event(&corrupt),
            Err(DecodeError::Record { .. })
        ));
    }
}
