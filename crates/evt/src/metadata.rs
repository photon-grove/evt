//! Event metadata: the envelope fields carried alongside every committed event.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize, Serializer};

use crate::ids::{CommandId, PrincipalId};
use crate::json::null_default;

/// Metadata stored with each event. It mirrors the Go `evt.Metadata` struct field for field, and
/// every field is always written (absent values are written as `null`, `""`, or `{}`).
///
/// The v0.9 keys (ADR 0004, C6) are appended after the existing ones, so stored events written
/// before v0.9 still decode: missing fields and `null` decode to their empty values and unknown
/// fields are ignored.
///
/// Encoding applies the storage format's defaults: `causationId` is written as the explicitly set
/// causation, otherwise the `commandId`, otherwise `null`, and `extensions` is always an object
/// (`{}` when unset or empty). Decoding stays faithful to the stored bytes and does not apply the
/// defaults. Equality compares effective causation IDs, so metadata equals its encoded round trip.
#[derive(Clone, Debug, Default, Eq, Deserialize)]
#[non_exhaustive]
pub struct Metadata {
    /// The command that produced the event. Used to reject duplicate commands on replay.
    #[serde(rename = "commandId", default)]
    pub command_id: Option<CommandId>,

    /// W3C trace-context carrier (for example `traceparent`). Keys serialize in sorted order.
    #[serde(default, deserialize_with = "trace_carrier")]
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

    /// Who issued the command (the caller's identity), for audit and per-principal checks.
    #[serde(rename = "principalId", default)]
    pub principal_id: Option<PrincipalId>,

    /// The command that caused this command. Encoding writes the `commandId` when unset.
    #[serde(rename = "causationId", default)]
    pub causation_id: Option<CommandId>,

    /// String-keyed application-specific metadata. Keys serialize in sorted order; an unset or
    /// empty map serializes as `{}`.
    #[serde(default, deserialize_with = "extensions")]
    pub extensions: BTreeMap<String, String>,
}

/// The system (`source`) and entry point (`endpoint`) that initiated a command.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
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
    #[must_use]
    pub fn with_command_id(mut self, command_id: impl Into<CommandId>) -> Self {
        self.command_id = Some(command_id.into());
        self
    }

    /// Sets the origin.
    #[must_use]
    pub fn with_origin(mut self, source: impl Into<String>, endpoint: impl Into<String>) -> Self {
        self.origin = Some(Origin::new(source, endpoint));
        self
    }

    /// Sets the address. An empty address clears it, matching Go's `evt.WithAddress`.
    #[must_use]
    pub fn with_address(mut self, address: impl Into<String>) -> Self {
        let address = address.into();
        self.address = (!address.is_empty()).then_some(address);
        self
    }

    /// Sets the trace-context carrier.
    #[must_use]
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
    #[must_use]
    pub fn with_timestamp(mut self, timestamp: impl Into<String>) -> Self {
        self.timestamp = timestamp.into();
        self
    }

    /// Sets the caller's identity. An empty principal clears it, matching Go's
    /// `evt.WithPrincipalID`.
    #[must_use]
    pub fn with_principal_id(mut self, principal: impl Into<PrincipalId>) -> Self {
        let principal = principal.into();
        self.principal_id = (!principal.is_empty()).then_some(principal);
        self
    }

    /// Sets the command that caused this command. An empty causation clears it, matching Go's
    /// `evt.WithCausationID`.
    #[must_use]
    pub fn with_causation_id(mut self, causation: impl Into<CommandId>) -> Self {
        let causation = causation.into();
        self.causation_id = (!causation.is_empty()).then_some(causation);
        self
    }

    fn effective_causation_id(&self) -> Option<&CommandId> {
        self.causation_id.as_ref().or(self.command_id.as_ref())
    }

    /// Adds one application-specific extension.
    #[must_use]
    pub fn with_extension(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extensions.insert(key.into(), value.into());
        self
    }
}

impl PartialEq for Metadata {
    fn eq(&self, other: &Self) -> bool {
        self.command_id == other.command_id
            && self.trace == other.trace
            && self.origin == other.origin
            && self.address == other.address
            && self.region == other.region
            && self.timestamp == other.timestamp
            && self.principal_id == other.principal_id
            && self.effective_causation_id() == other.effective_causation_id()
            && self.extensions == other.extensions
    }
}

/// The encoding shape of [`Metadata`]: the fields in the storage format's key order, with the
/// effective `causationId` already applied.
#[derive(Serialize)]
struct MetadataWire<'a> {
    #[serde(rename = "commandId")]
    command_id: &'a Option<CommandId>,
    #[serde(rename = "trace")]
    trace: &'a Option<BTreeMap<String, String>>,
    #[serde(rename = "origin")]
    origin: &'a Option<Origin>,
    #[serde(rename = "address")]
    address: &'a Option<String>,
    #[serde(rename = "region")]
    region: &'a str,
    #[serde(rename = "timestamp")]
    timestamp: &'a str,
    #[serde(rename = "principalId")]
    principal_id: &'a Option<PrincipalId>,
    #[serde(rename = "causationId")]
    causation_id: Option<&'a CommandId>,
    #[serde(rename = "extensions")]
    extensions: &'a BTreeMap<String, String>,
}

impl Serialize for Metadata {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // causationId is written with its effective value: the explicitly set causation, otherwise
        // the commandId, otherwise null — matching Go's Metadata.MarshalJSON.
        MetadataWire {
            command_id: &self.command_id,
            trace: &self.trace,
            origin: &self.origin,
            address: &self.address,
            region: &self.region,
            timestamp: &self.timestamp,
            principal_id: &self.principal_id,
            causation_id: self.effective_causation_id(),
            extensions: &self.extensions,
        }
        .serialize(serializer)
    }
}

/// Decodes the trace carrier, reading `null` values as `""` the way Go's `encoding/json` does for a
/// `map[string]string`.
fn trace_carrier<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<BTreeMap<String, String>>, D::Error> {
    let carrier = Option::<BTreeMap<String, Option<String>>>::deserialize(deserializer)?;

    Ok(carrier.map(|carrier| {
        carrier
            .into_iter()
            .map(|(key, value)| (key, value.unwrap_or_default()))
            .collect()
    }))
}

/// Decodes the extensions map the way Go decodes a `map[string]string`: a `null` map is empty, and
/// `null` values decode to `""`.
fn extensions<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, String>, D::Error> {
    let map = Option::<BTreeMap<String, Option<String>>>::deserialize(deserializer)?;

    Ok(map
        .unwrap_or_default()
        .into_iter()
        .map(|(key, value)| (key, value.unwrap_or_default()))
        .collect())
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
        let cases = [
            (0, "1970-01-01T00:00:00Z"),
            (946_684_799, "1999-12-31T23:59:59Z"),
            (951_782_400, "2000-02-29T00:00:00Z"),
            (4_107_542_400, "2100-03-01T00:00:00Z"),
        ];

        for (seconds, formatted) in cases {
            assert_eq!(format_rfc3339(seconds), formatted);
        }
    }

    #[test]
    fn empty_address_clears_it() {
        let metadata = Metadata::default()
            .with_address("10.0.0.1")
            .with_address("");

        assert_eq!(metadata.address, None);
    }

    #[test]
    fn causation_defaults_to_the_command_id_at_encode() {
        let defaulted = to_json_string(&Metadata::default().with_command_id("cmd-0001"));
        assert!(
            defaulted.contains(r#""causationId":"cmd-0001""#),
            "{defaulted}"
        );

        let explicit = to_json_string(
            &Metadata::default()
                .with_command_id("cmd-0002")
                .with_causation_id("cmd-0001"),
        );
        assert!(
            explicit.contains(r#""causationId":"cmd-0001""#),
            "{explicit}"
        );
        assert!(
            !explicit.contains(r#""causationId":"cmd-0002""#),
            "{explicit}"
        );

        let neither = to_json_string(&Metadata::default());
        assert!(neither.contains(r#""causationId":null"#), "{neither}");
    }

    #[test]
    fn equality_uses_effective_causation_and_survives_round_trip() {
        let cases = [
            Metadata::default(),
            Metadata::default().with_command_id("c-1"),
            Metadata::default()
                .with_command_id("c-1")
                .with_principal_id("user:42")
                .with_causation_id("c-0")
                .with_extension("source", "cli")
                .with_extension("empty", ""),
            Metadata::default().with_causation_id("c-0"),
        ];

        for original in cases {
            let decoded: Metadata = serde_json::from_str(&to_json_string(&original)).unwrap();
            assert_eq!(original, decoded);
            assert_eq!(decoded, original);
        }

        let defaulted = Metadata::default().with_command_id("c-1");
        let explicit = defaulted.clone().with_causation_id("c-1");
        assert_eq!(defaulted, explicit);
        assert_eq!(explicit, defaulted);
        assert_ne!(defaulted, explicit.with_causation_id("c-0"));
        assert_ne!(
            Metadata::default(),
            Metadata::default().with_causation_id("c-0")
        );

        let legacy: Metadata = serde_json::from_str(r#"{"extensions":null}"#).unwrap();
        assert_eq!(legacy, Metadata::default());
    }

    #[test]
    fn extensions_always_encode_as_an_object_with_sorted_keys() {
        let unset = to_json_string(&Metadata::default());
        assert!(unset.contains(r#""extensions":{}"#), "{unset}");

        let sorted = to_json_string(
            &Metadata::default()
                .with_extension("tenant", "example")
                .with_extension("attempt", "2")
                .with_extension("requestSource", "cli"),
        );
        assert!(
            sorted.contains(
                r#""extensions":{"attempt":"2","requestSource":"cli","tenant":"example"}"#
            ),
            "{sorted}"
        );
    }

    #[test]
    fn v09_keys_decode_leniently_from_legacy_metadata() {
        let metadata: Metadata =
            serde_json::from_str(r#"{"commandId":"c-1","region":"r"}"#).unwrap();

        assert_eq!(metadata.principal_id, None);
        assert_eq!(metadata.causation_id, None);
        assert!(metadata.extensions.is_empty());

        // Encoding applies the defaults even to a decoded legacy event.
        let encoded = to_json_string(&metadata);
        assert!(encoded.contains(r#""principalId":null"#), "{encoded}");
        assert!(encoded.contains(r#""causationId":"c-1""#), "{encoded}");
        assert!(encoded.contains(r#""extensions":{}"#), "{encoded}");
    }

    #[test]
    fn extensions_decode_nulls_like_go() {
        let metadata: Metadata =
            serde_json::from_str(r#"{"extensions":{"a":null,"b":"x"}}"#).unwrap();
        assert_eq!(
            metadata.extensions,
            BTreeMap::from([
                ("a".to_owned(), "".to_owned()),
                ("b".to_owned(), "x".to_owned())
            ])
        );

        let metadata: Metadata = serde_json::from_str(r#"{"extensions":null}"#).unwrap();
        assert!(metadata.extensions.is_empty());
    }

    /// Encodes metadata with the Go-identical JSON encoder, for assertions on stored bytes.
    fn to_json_string(metadata: &Metadata) -> String {
        crate::json::to_string(metadata).unwrap()
    }
}
