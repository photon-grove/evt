//! Event metadata: the envelope fields carried alongside every committed event.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::ids::CommandId;
use crate::json::null_default;

/// Metadata stored with each event. It mirrors the Go `evt.Metadata` struct field for field, and
/// every field is always written (absent values are written as `null` or `""`).
///
/// Readers are lenient the way Go is: missing fields and `null` decode to their empty values and
/// unknown fields are ignored.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metadata {
    /// The command that produced the event. Used to reject duplicate commands on replay.
    #[serde(rename = "commandId", default)]
    pub command_id: Option<CommandId>,

    /// W3C trace-context carrier (for example `traceparent`). Keys serialize in sorted order.
    #[serde(default)]
    pub trace: Option<BTreeMap<String, String>>,

    /// The system and entry point that initiated the command.
    #[serde(default)]
    pub origin: Option<Origin>,

    /// An optional caller-provided address.
    #[serde(default)]
    pub address: Option<String>,

    /// The region the command ran in.
    #[serde(default, deserialize_with = "null_default")]
    pub region: String,

    /// When the command ran, as RFC 3339 UTC with second precision (`2026-01-02T03:04:05Z`).
    #[serde(default, deserialize_with = "null_default")]
    pub timestamp: String,
}

/// The system (`source`) and entry point (`endpoint`) that initiated a command.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Origin {
    /// The initiating system.
    #[serde(default, deserialize_with = "null_default")]
    pub source: String,

    /// The entry point the request came in through.
    #[serde(default, deserialize_with = "null_default")]
    pub endpoint: String,
}

impl Origin {
    /// Creates an origin from a source and endpoint.
    pub fn new(source: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            endpoint: endpoint.into(),
        }
    }
}

impl Metadata {
    /// Creates metadata stamped with the current UTC time, like Go's `evt.NewMetadata`.
    pub fn new(region: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            timestamp: rfc3339_now(),
            ..Self::default()
        }
    }

    /// Sets the command ID used for duplicate-command detection.
    pub fn with_command_id(mut self, command_id: impl Into<CommandId>) -> Self {
        self.command_id = Some(command_id.into());
        self
    }

    /// Sets the origin.
    pub fn with_origin(mut self, source: impl Into<String>, endpoint: impl Into<String>) -> Self {
        self.origin = Some(Origin::new(source, endpoint));
        self
    }

    /// Sets the address. An empty address clears it, matching Go's `evt.WithAddress`.
    pub fn with_address(mut self, address: impl Into<String>) -> Self {
        let address = address.into();
        self.address = (!address.is_empty()).then_some(address);
        self
    }

    /// Sets the trace-context carrier.
    pub fn with_trace<I, K, V>(mut self, carrier: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.trace = Some(
            carrier
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        );
        self
    }

    /// Overrides the timestamp. Useful for deterministic tests and imports.
    pub fn with_timestamp(mut self, timestamp: impl Into<String>) -> Self {
        self.timestamp = timestamp.into();
        self
    }
}

/// Formats the current time as RFC 3339 UTC with second precision, matching Go's
/// `time.Now().UTC().Format(time.RFC3339)`.
pub fn rfc3339_now() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs() as i64)
        .unwrap_or_default();

    format_rfc3339(seconds)
}

/// Formats Unix seconds as RFC 3339 UTC with second precision.
pub fn format_rfc3339(unix_seconds: i64) -> String {
    let days = unix_seconds.div_euclid(86_400);
    let seconds_of_day = unix_seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);

    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        seconds_of_day / 3600,
        seconds_of_day % 3600 / 60,
        seconds_of_day % 60,
    )
}

/// Converts days since 1970-01-01 to a proleptic Gregorian (year, month, day).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let day_of_era = z.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_index + 2) / 5 + 1) as u32;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    } as u32;
    let year = year_of_era + era * 400 + i64::from(month <= 2);

    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_rfc3339_seconds() {
        assert_eq!(format_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_rfc3339(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(format_rfc3339(1_791_763_200), "2026-10-12T00:00:00Z");
        assert_eq!(format_rfc3339(1_791_766_861), "2026-10-12T01:01:01Z");
    }

    #[test]
    fn empty_address_clears_it() {
        let metadata = Metadata::default()
            .with_address("10.0.0.1")
            .with_address("");

        assert_eq!(metadata.address, None);
    }
}
