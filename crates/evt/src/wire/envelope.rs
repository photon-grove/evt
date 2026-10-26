//! The stream message envelope published for each committed event.
//!
//! The envelope is an EventBridge-style `CloudWatchEvent` around each event, as the removed Go
//! SNS publisher wrote it. Consumers (Go's `projectors` package, or Rust code using [`decode`])
//! unwrap the serialized event from `detail`.
//!
//! ```json
//! {"version":"","id":"<event id>","detail-type":"<event type>","source":"<source>",
//!  "account":"","time":"0001-01-01T00:00:00Z","region":"","resources":null,
//!  "detail":<serialized event JSON>}
//! ```

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::error::Error;
use crate::event::SerializedEvent;
use crate::json;

/// Go's zero `time.Time`, which the publisher leaves in the `time` field.
const ZERO_TIME: &str = "0001-01-01T00:00:00Z";

#[derive(Serialize)]
struct EnvelopeOut<'a> {
    version: &'a str,
    id: &'a str,
    #[serde(rename = "detail-type")]
    detail_type: &'a str,
    source: &'a str,
    account: &'a str,
    time: &'a str,
    region: &'a str,
    resources: Option<&'a [String]>,
    detail: &'a RawValue,
}

#[derive(Deserialize)]
struct EnvelopeIn {
    #[serde(default)]
    detail: Option<SerializedEvent>,
}

/// Encodes the envelope for an event, byte-identical to the message body the removed Go SNS publisher wrote.
pub fn encode(event: &SerializedEvent, source: &str) -> Result<Vec<u8>, Error> {
    let detail = String::from_utf8(event.to_json()?).expect("serde_json writes UTF-8");
    let detail = RawValue::from_string(detail)?;

    let envelope = EnvelopeOut {
        version: "",
        id: event.id.as_str(),
        detail_type: event.event_type.as_str(),
        source,
        account: "",
        time: ZERO_TIME,
        region: "",
        resources: None,
        detail: &detail,
    };

    Ok(json::to_vec(&envelope)?)
}

/// Decodes the serialized event from an envelope body (an SNS message or a raw-delivery SQS body).
/// Like Go's `projectors.StreamRecordFromEnvelope`, it rejects events without an entity or
/// event ID.
pub fn decode(body: &[u8]) -> Result<SerializedEvent, Error> {
    let envelope: EnvelopeIn = serde_json::from_slice(body)?;
    let event = envelope.detail.unwrap_or_default();

    if event.entity_id.is_empty() {
        return Err(Error::InvalidRecord(
            "event envelope has no entity id".into(),
        ));
    }
    if event.id.is_empty() {
        return Err(Error::InvalidRecord(
            "event envelope has no event id".into(),
        ));
    }

    Ok(event)
}

/// The SNS message attributes Go's publisher sets: `entityType`, `eventType`, and, when present,
/// `commandId`, `traceparent`, and `correlationId` (the command ID, else the traceparent).
pub fn message_attributes(event: &SerializedEvent) -> BTreeMap<String, String> {
    let mut attributes = BTreeMap::new();
    attributes.insert("entityType".to_owned(), event.entity_type.to_string());
    attributes.insert("eventType".to_owned(), event.event_type.to_string());

    if let Some(command_id) = event
        .metadata
        .command_id
        .as_ref()
        .filter(|id| !id.is_empty())
    {
        attributes.insert("commandId".to_owned(), command_id.to_string());
        attributes.insert("correlationId".to_owned(), command_id.to_string());
    }

    let traceparent = event
        .metadata
        .trace
        .as_ref()
        .and_then(|trace| trace.get("traceparent"))
        .filter(|value| !value.is_empty());

    if let Some(traceparent) = traceparent {
        attributes.insert("traceparent".to_owned(), traceparent.clone());
        attributes
            .entry("correlationId".to_owned())
            .or_insert_with(|| traceparent.clone());
    }

    attributes
}
