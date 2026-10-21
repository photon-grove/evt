//! Event source mapping filter patterns, and the same test applied in the handler.
//!
//! A Lambda event source mapping can drop records before they reach the function: its filter
//! criteria hold JSON patterns over each record, and only records that match a pattern invoke the
//! function. [`EventFilter`] writes such a pattern for an event-log table's stream
//! ([`EventFilter::pattern`]), and [`EventFilter::matches`] applies the same test to a record in
//! the handler, so a function stays correct when its mapping has no filter, or an older one.
//!
//! Patterns use only the rules Lambda supports for DynamoDB records: string equality, `prefix`,
//! and `exists`, over `eventName` and the `S` values of `Keys` and `NewImage`. Numbers in DynamoDB
//! JSON are strings, so numeric rules never match them, and none are used.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use photon_evt::SerializedEvent;
use photon_evt::wire::dynamo::{AttributeValue, Item};
use serde::Serialize;

use crate::error::FilterError;
use crate::record::{EventName, StreamRecord};

/// The longest filter pattern Lambda accepts, in bytes.
pub const MAX_PATTERN_BYTES: usize = 4096;

#[derive(Serialize)]
#[serde(untagged)]
enum Matcher<'a> {
    Equals(&'a str),
    Prefix { prefix: &'a str },
    Exists { exists: bool },
}

#[derive(Serialize)]
struct StringField<'a> {
    #[serde(rename = "S")]
    s: Vec<Matcher<'a>>,
}

#[derive(Serialize)]
struct Change<'a> {
    #[serde(rename = "Keys", skip_serializing_if = "BTreeMap::is_empty")]
    keys: BTreeMap<&'static str, StringField<'a>>,
    #[serde(rename = "NewImage", skip_serializing_if = "BTreeMap::is_empty")]
    new_image: BTreeMap<&'static str, StringField<'a>>,
}

#[derive(Serialize)]
struct Pattern<'a> {
    #[serde(rename = "eventName")]
    event_name: &'a [&'static str],
    dynamodb: Change<'a>,
}

/// One string attribute's rule: any of some values or prefixes, or, with neither, present.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct StringRule {
    pub(crate) values: BTreeSet<String>,
    pub(crate) prefixes: BTreeSet<String>,
}

impl StringRule {
    pub(crate) fn is_any(&self) -> bool {
        self.values.is_empty() && self.prefixes.is_empty()
    }

    fn matchers(&self) -> Vec<Matcher<'_>> {
        if self.is_any() {
            return vec![Matcher::Exists { exists: true }];
        }

        self.values
            .iter()
            .map(|value| Matcher::Equals(value))
            .chain(
                self.prefixes
                    .iter()
                    .map(|prefix| Matcher::Prefix { prefix }),
            )
            .collect()
    }

    fn field(&self) -> StringField<'_> {
        StringField { s: self.matchers() }
    }

    pub(crate) fn matches(&self, value: &str) -> bool {
        self.is_any()
            || self.values.contains(value)
            || self
                .prefixes
                .iter()
                .any(|prefix| value.starts_with(prefix.as_str()))
    }

    pub(crate) fn matches_attribute(&self, item: &Item, name: &str) -> bool {
        matches!(item.get(name), Some(AttributeValue::S(value)) if self.matches(value))
    }

    pub(crate) fn check(&self, what: &'static str) -> Result<(), FilterError> {
        if self
            .values
            .iter()
            .chain(&self.prefixes)
            .any(String::is_empty)
        {
            return Err(FilterError::Empty(what));
        }

        Ok(())
    }
}

fn field_map<'a>(
    fields: Vec<(&'static str, &'a StringRule)>,
) -> BTreeMap<&'static str, StringField<'a>> {
    fields
        .into_iter()
        .map(|(name, rule)| (name, rule.field()))
        .collect()
}

pub(crate) fn encode_pattern(
    event_names: &[&'static str],
    keys: Vec<(&'static str, &StringRule)>,
    new_image: Vec<(&'static str, &StringRule)>,
) -> Result<String, FilterError> {
    let pattern = Pattern {
        event_name: event_names,
        dynamodb: Change {
            keys: field_map(keys),
            new_image: field_map(new_image),
        },
    };
    let encoded = serde_json::to_string(&pattern).expect("a filter pattern serializes");

    if encoded.len() > MAX_PATTERN_BYTES {
        return Err(FilterError::TooLong {
            len: encoded.len(),
            max: MAX_PATTERN_BYTES,
        });
    }

    Ok(encoded)
}

/// Selects the committed events of an event-log table's stream, by event type and entity type.
///
/// A record matches when it is an `INSERT` whose new image has a string `metadata` (only event
/// items have one: heads, snapshots, the feature marker, the rebuild fence, and outcomes do not),
/// a `type` that is one of the event types or starts with one of the prefixes, and an `entityType`
/// that is one of the entity types. An empty set of event types and prefixes, or of entity types,
/// matches any. The table's stream must write new images (`NEW_IMAGE` or `NEW_AND_OLD_IMAGES`): on
/// a `KEYS_ONLY` or `OLD_IMAGE` stream the pattern matches nothing, so the mapping drops every
/// record without an error. ([`decode_event`](crate::decode_event) without a mapping filter
/// reports [`MissingNewImage`](crate::DecodeError::MissingNewImage) instead.)
///
/// ```
/// use photon_evt_lambda::EventFilter;
///
/// let filter = EventFilter::new()
///     .entity_type("account")
///     .event_type("account:opened")
///     .event_type_prefix("account:closed");
///
/// assert_eq!(
///     filter.pattern().unwrap(),
///     r#"{"eventName":["INSERT"],"dynamodb":{"NewImage":{"entityType":{"S":["account"]},"metadata":{"S":[{"exists":true}]},"type":{"S":["account:opened",{"prefix":"account:closed"}]}}}}"#,
/// );
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EventFilter {
    event_type: StringRule,
    entity_type: StringRule,
}

impl EventFilter {
    /// A filter that matches every committed event.
    pub fn new() -> Self {
        Self::default()
    }

    /// Also matches events of this type.
    pub fn event_type(mut self, event_type: impl Into<String>) -> Self {
        self.event_type.values.insert(event_type.into());
        self
    }

    /// Also matches events of these types.
    pub fn event_types<I, S>(self, event_types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        event_types.into_iter().fold(self, Self::event_type)
    }

    /// Also matches events whose type starts with `prefix`, such as `account:`.
    pub fn event_type_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.event_type.prefixes.insert(prefix.into());
        self
    }

    /// Also matches events of streams of this entity type.
    pub fn entity_type(mut self, entity_type: impl Into<String>) -> Self {
        self.entity_type.values.insert(entity_type.into());
        self
    }

    /// Also matches events of streams of these entity types.
    pub fn entity_types<I, S>(self, entity_types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        entity_types.into_iter().fold(self, Self::entity_type)
    }

    /// The event source mapping filter pattern: one entry of the mapping's `FilterCriteria`
    /// (`Filters[].Pattern`). The text is deterministic, so it can be compared with the deployed
    /// one.
    pub fn pattern(&self) -> Result<String, FilterError> {
        self.event_type.check("event type")?;
        self.entity_type.check("entity type")?;

        let any = StringRule::default();
        let mut image = vec![("metadata", &any), ("type", &self.event_type)];

        if !self.entity_type.is_any() {
            image.push(("entityType", &self.entity_type));
        }

        encode_pattern(&["INSERT"], Vec::new(), image)
    }

    /// Whether the record matches, by the rule the pattern states.
    pub fn matches(&self, record: &StreamRecord) -> bool {
        let Some(image) = &record.change.new_image else {
            return false;
        };

        record.event_name == EventName::Insert
            && StringRule::default().matches_attribute(image, "metadata")
            && self.event_type.matches_attribute(image, "type")
            && (self.entity_type.is_any()
                || self.entity_type.matches_attribute(image, "entityType"))
    }

    /// Whether a decoded event matches the filter's event and entity types.
    pub fn matches_event(&self, event: &SerializedEvent) -> bool {
        self.event_type.matches(event.event_type.as_str())
            && self.entity_type.matches(event.entity_type.as_str())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use photon_evt::wire::dynamo::event_item;
    use photon_evt::{EventSequence, EventVersion, Metadata};
    use serde_json::Value;

    use super::*;

    /// Evaluates a Lambda filter pattern over a record's JSON, for the rules this crate emits:
    /// a list of values (equality, `prefix`, `exists: true`) at a leaf, nested objects otherwise.
    pub(crate) fn lambda_matches(pattern: &str, record: &StreamRecord) -> bool {
        fn walk(pattern: &Value, value: Option<&Value>) -> bool {
            match pattern {
                Value::Object(fields) => fields
                    .iter()
                    .all(|(name, rule)| walk(rule, value.and_then(|value| value.get(name)))),
                Value::Array(rules) => rules.iter().any(|rule| match (rule, value) {
                    (Value::String(want), Some(Value::String(got))) => want == got,
                    (Value::Object(rule), Some(Value::String(got)))
                        if rule.contains_key("prefix") =>
                    {
                        got.starts_with(rule["prefix"].as_str().unwrap())
                    }
                    (Value::Object(rule), got) if rule.contains_key("exists") => {
                        rule["exists"].as_bool().unwrap() == got.is_some()
                    }
                    _ => false,
                }),
                other => panic!("unexpected pattern node {other}"),
            }
        }

        let pattern: Value = serde_json::from_str(pattern).unwrap();
        let record = serde_json::to_value(record).unwrap();

        walk(&pattern, Some(&record))
    }

    pub(crate) fn event(entity_type: &str, event_type: &str) -> SerializedEvent {
        SerializedEvent {
            id: "e-1:1".into(),
            sequence: EventSequence(1),
            event_type: event_type.into(),
            version: EventVersion(1),
            entity_id: "e-1".into(),
            entity_type: entity_type.into(),
            payload: b"{}".to_vec(),
            metadata: Metadata::default(),
        }
    }

    fn insert(image: Item) -> StreamRecord {
        let keys = image
            .iter()
            .filter(|(name, _)| *name == "pk" || *name == "sk")
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();

        StreamRecord::new(EventName::Insert, keys, "1").with_new_image(image)
    }

    #[test]
    fn the_handler_test_agrees_with_the_pattern() {
        let filters = [
            EventFilter::new(),
            EventFilter::new().entity_type("account"),
            EventFilter::new().event_types(["account:opened", "order:placed"]),
            EventFilter::new()
                .entity_types(["account", "order"])
                .event_type_prefix("account:")
                .event_type("order:placed"),
        ];

        let mut head = Item::new();
        head.insert("pk".into(), AttributeValue::S("e-1".into()));
        head.insert("sk".into(), AttributeValue::N("-1".into()));
        head.insert("head".into(), AttributeValue::N("1".into()));

        let event_records = [
            ("account", "account:opened"),
            ("account", "account:closed"),
            ("order", "order:placed"),
            ("order", "order:shipped"),
            ("ledger", "ledger:added"),
        ]
        .map(|(entity_type, event_type)| {
            insert(event_item(&event(entity_type, event_type), None).unwrap())
        });
        let mut modify = event_records[0].clone();
        modify.event_name = EventName::Modify;
        let mut keys_only = event_records[0].clone();
        keys_only.change.new_image = None;

        let records: Vec<StreamRecord> = event_records
            .iter()
            .cloned()
            .chain([insert(head), modify, keys_only])
            .collect();

        for filter in &filters {
            let pattern = filter.pattern().unwrap();

            for record in &records {
                assert_eq!(
                    filter.matches(record),
                    lambda_matches(&pattern, record),
                    "{pattern} on {record:?}"
                );
            }
        }

        let all = EventFilter::new();
        assert_eq!(
            records.iter().filter(|record| all.matches(record)).count(),
            5,
            "every event and nothing else"
        );

        let accounts = &filters[3];
        assert!(accounts.matches_event(&event("account", "account:closed")));
        assert!(accounts.matches_event(&event("order", "order:placed")));
        assert!(!accounts.matches_event(&event("order", "order:shipped")));
        assert!(!accounts.matches_event(&event("ledger", "account:opened")));
    }

    #[test]
    fn empty_values_and_oversized_patterns_are_refused() {
        assert_eq!(
            EventFilter::new().event_type_prefix("").pattern(),
            Err(FilterError::Empty("event type"))
        );
        assert_eq!(
            EventFilter::new().entity_type("").pattern(),
            Err(FilterError::Empty("entity type"))
        );

        let many = EventFilter::new().event_types((0..400).map(|n| format!("account:event-{n}")));
        assert!(matches!(
            many.pattern(),
            Err(FilterError::TooLong { max: 4096, .. })
        ));
    }

    #[test]
    fn the_every_event_pattern_is_stable() {
        assert_eq!(
            EventFilter::new().pattern().unwrap(),
            r#"{"eventName":["INSERT"],"dynamodb":{"NewImage":{"metadata":{"S":[{"exists":true}]},"type":{"S":[{"exists":true}]}}}}"#
        );
    }
}
