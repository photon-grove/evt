//! Upcasters migrate stored events from older payload versions before they are decoded.

use serde_json::Value;

use crate::error::BoxError;
use crate::event::SerializedEvent;
use crate::ids::{EventType, EventVersion};

/// Migrates a stored event from an older version to a newer one before replay.
///
/// Upcasters run in declaration order, once each, the same way the Go framework runs them: each
/// upcaster sees the output of the ones before it, so a v1 → v2 upcaster followed by a v2 → v3
/// upcaster migrates a v1 event all the way to v3.
pub trait Upcaster: Send + Sync {
    /// Reports whether this upcaster applies to the event type and version.
    fn can_upcast(&self, event_type: &EventType, version: EventVersion) -> bool;

    /// Rewrites the event. It must return the new version in `event.version`.
    fn upcast(&self, event: SerializedEvent) -> Result<SerializedEvent, BoxError>;
}

/// An upcaster that rewrites the JSON payload of one event type from one version to the next.
///
/// It is `const`-constructible, so it can live in a `static`:
///
/// ```
/// use photon_evt::JsonUpcaster;
/// use serde_json::Value;
///
/// fn rename_name_to_owner(payload: &mut Value) -> Result<(), photon_evt::BoxError> {
///     let object = payload.as_object_mut().ok_or("payload is not an object")?;
///     if let Some(name) = object.remove("name") {
///         object.insert("owner".into(), name);
///     }
///     Ok(())
/// }
///
/// static RENAMED_V1: JsonUpcaster = JsonUpcaster::new("account:renamed", 1, 2, rename_name_to_owner);
/// ```
#[derive(Clone, Copy, Debug)]
pub struct JsonUpcaster {
    event_type: &'static str,
    from_version: i64,
    to_version: i64,
    migrate: fn(&mut Value) -> Result<(), BoxError>,
}

impl JsonUpcaster {
    /// Creates an upcaster for `event_type` that migrates `from_version` payloads to `to_version`.
    pub const fn new(
        event_type: &'static str,
        from_version: i64,
        to_version: i64,
        migrate: fn(&mut Value) -> Result<(), BoxError>,
    ) -> Self {
        Self {
            event_type,
            from_version,
            to_version,
            migrate,
        }
    }
}

impl Upcaster for JsonUpcaster {
    fn can_upcast(&self, event_type: &EventType, version: EventVersion) -> bool {
        event_type.as_str() == self.event_type && version.0 == self.from_version
    }

    fn upcast(&self, mut event: SerializedEvent) -> Result<SerializedEvent, BoxError> {
        let mut payload: Value = serde_json::from_slice(&event.payload)?;
        (self.migrate)(&mut payload)?;

        event.payload = crate::json::to_vec(&payload)?;
        event.version = EventVersion(self.to_version);

        Ok(event)
    }
}

/// An upcaster failure, with the event type and version it was given.
pub(crate) struct UpcastFailure {
    pub(crate) event_type: EventType,
    pub(crate) version: EventVersion,
    pub(crate) source: BoxError,
}

/// Runs each applicable upcaster over the event, in order.
pub(crate) fn upcast_event(
    upcasters: &[&dyn Upcaster],
    mut event: SerializedEvent,
) -> Result<SerializedEvent, Box<UpcastFailure>> {
    for upcaster in upcasters {
        if upcaster.can_upcast(&event.event_type, event.version) {
            let (event_type, version) = (event.event_type.clone(), event.version);
            event = upcaster.upcast(event).map_err(|source| {
                Box::new(UpcastFailure {
                    event_type,
                    version,
                    source,
                })
            })?;
        }
    }

    Ok(event)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn add_field(payload: &mut Value) -> Result<(), BoxError> {
        payload["added"] = Value::Bool(true);
        Ok(())
    }

    fn bump(payload: &mut Value) -> Result<(), BoxError> {
        payload["bumped"] = Value::Bool(true);
        Ok(())
    }

    static V1_TO_V2: JsonUpcaster = JsonUpcaster::new("thing:changed", 1, 2, add_field);
    static V2_TO_V3: JsonUpcaster = JsonUpcaster::new("thing:changed", 2, 3, bump);

    #[test]
    fn chains_upcasters_in_order() {
        let event = SerializedEvent {
            event_type: "thing:changed".into(),
            version: EventVersion(1),
            payload: br#"{"value":1}"#.to_vec(),
            ..SerializedEvent::default()
        };

        let upcasted = upcast_event(&[&V1_TO_V2, &V2_TO_V3], event).ok().unwrap();

        assert_eq!(upcasted.version, EventVersion(3));
        assert_eq!(
            upcasted.payload,
            br#"{"added":true,"bumped":true,"value":1}"#
        );
    }
}
