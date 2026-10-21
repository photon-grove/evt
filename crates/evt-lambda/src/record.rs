//! The DynamoDB Streams event a Lambda function receives from its event source mapping.
//!
//! The shapes follow the JSON Lambda delivers. Item images are DynamoDB JSON, decoded into
//! [`photon_evt::wire::dynamo::Item`], the same type the event-log and view codecs read, so no AWS
//! SDK is needed. Fields this crate does not interpret are kept when Lambda sends them and default
//! when it does not.

use photon_evt::wire::dynamo::{AttributeValue, Item};
use serde::{Deserialize, Serialize};

/// The principal DynamoDB names on a record of a deletion its time-to-live process made.
pub const TTL_PRINCIPAL: &str = "dynamodb.amazonaws.com";

/// One invocation's batch: the records of one shard, in stream order.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct StreamEvent {
    /// The records, oldest first.
    #[serde(rename = "Records", default)]
    pub records: Vec<StreamRecord>,
}

/// The kind of change a record reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventName {
    /// A new item.
    #[serde(rename = "INSERT")]
    Insert,
    /// An existing item changed.
    #[serde(rename = "MODIFY")]
    Modify,
    /// An item was deleted.
    #[serde(rename = "REMOVE")]
    Remove,
    /// A name this crate does not know.
    #[serde(rename = "UNKNOWN", other)]
    Unknown,
}

/// What a stream writes into each record, configured per table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StreamViewType {
    /// Only the key attributes.
    #[serde(rename = "KEYS_ONLY")]
    KeysOnly,
    /// The item as it is after the change.
    #[serde(rename = "NEW_IMAGE")]
    NewImage,
    /// The item as it was before the change.
    #[serde(rename = "OLD_IMAGE")]
    OldImage,
    /// Both images.
    #[serde(rename = "NEW_AND_OLD_IMAGES")]
    NewAndOldImages,
    /// A view type this crate does not know.
    #[serde(rename = "UNKNOWN", other)]
    Unknown,
}

impl StreamViewType {
    /// The name DynamoDB uses.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::KeysOnly => "KEYS_ONLY",
            Self::NewImage => "NEW_IMAGE",
            Self::OldImage => "OLD_IMAGE",
            Self::NewAndOldImages => "NEW_AND_OLD_IMAGES",
            Self::Unknown => "UNKNOWN",
        }
    }
}

/// The change itself: the item's keys, its images, and its place in the shard.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct StreamChange {
    /// When the change was made, in Unix seconds (approximate).
    #[serde(
        rename = "ApproximateCreationDateTime",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub approximate_creation_date_time: Option<f64>,
    /// The item's key attributes. Present on every record, whatever the view type.
    #[serde(rename = "Keys", default)]
    pub keys: Item,
    /// The item after the change (`NEW_IMAGE`, `NEW_AND_OLD_IMAGES`; absent on a `REMOVE`).
    #[serde(rename = "NewImage", default, skip_serializing_if = "Option::is_none")]
    pub new_image: Option<Item>,
    /// The item before the change (`OLD_IMAGE`, `NEW_AND_OLD_IMAGES`; absent on an `INSERT`).
    #[serde(rename = "OldImage", default, skip_serializing_if = "Option::is_none")]
    pub old_image: Option<Item>,
    /// The record's sequence number in its shard: the identifier a partial batch failure reports.
    #[serde(rename = "SequenceNumber", default)]
    pub sequence_number: String,
    /// The record's size in bytes.
    #[serde(rename = "SizeBytes", default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    /// The table's stream view type when the record was written.
    #[serde(
        rename = "StreamViewType",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub stream_view_type: Option<StreamViewType>,
}

/// Who made a change. DynamoDB sets it only on deletions its time-to-live process made.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct UserIdentity {
    /// `Service` for a time-to-live deletion.
    #[serde(rename = "type", default)]
    pub kind: String,
    /// [`TTL_PRINCIPAL`] for a time-to-live deletion.
    #[serde(rename = "principalId", default)]
    pub principal_id: String,
}

/// One record of a batch. Build one with [`StreamRecord::new`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct StreamRecord {
    /// A unique identifier of the record.
    #[serde(rename = "eventID", default)]
    pub event_id: String,
    /// The kind of change.
    #[serde(rename = "eventName")]
    pub event_name: EventName,
    /// The record format version.
    #[serde(rename = "eventVersion", default)]
    pub event_version: String,
    /// `aws:dynamodb`.
    #[serde(rename = "eventSource", default)]
    pub event_source: String,
    /// The table's region.
    #[serde(rename = "awsRegion", default)]
    pub aws_region: String,
    /// The change.
    #[serde(rename = "dynamodb", default)]
    pub change: StreamChange,
    /// The ARN of the stream the record came from; it names the table.
    #[serde(rename = "eventSourceARN", default)]
    pub event_source_arn: String,
    /// Set on deletions made by the table's time-to-live process.
    #[serde(
        rename = "userIdentity",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub user_identity: Option<UserIdentity>,
}

impl StreamRecord {
    /// A record of `event_name` for an item with these keys and no images, at `sequence_number`.
    pub fn new(event_name: EventName, keys: Item, sequence_number: impl Into<String>) -> Self {
        Self {
            event_id: String::new(),
            event_name,
            event_version: "1.1".into(),
            event_source: "aws:dynamodb".into(),
            aws_region: String::new(),
            change: StreamChange {
                keys,
                sequence_number: sequence_number.into(),
                ..StreamChange::default()
            },
            event_source_arn: String::new(),
            user_identity: None,
        }
    }

    /// Adds the item after the change, as a `NEW_IMAGE` stream writes it.
    pub fn with_new_image(mut self, image: Item) -> Self {
        self.change.new_image = Some(image);
        self.change.stream_view_type = Some(StreamViewType::NewImage);
        self
    }

    /// The record's sequence number in its shard.
    pub fn sequence_number(&self) -> &str {
        &self.change.sequence_number
    }

    /// The item's key attribute `name`, from the keys or, if the keys lack it, the new image.
    pub fn key(&self, name: &str) -> Option<&AttributeValue> {
        self.change.keys.get(name).or_else(|| {
            self.change
                .new_image
                .as_ref()
                .and_then(|image| image.get(name))
        })
    }

    /// The partition key `pk`, if it is a string.
    pub fn pk(&self) -> Option<&str> {
        match self.key("pk") {
            Some(AttributeValue::S(pk)) => Some(pk),
            _ => None,
        }
    }

    /// Whether DynamoDB's time-to-live process made this change.
    pub fn is_ttl_deletion(&self) -> bool {
        self.event_name == EventName::Remove
            && self
                .user_identity
                .as_ref()
                .is_some_and(|identity| identity.principal_id == TTL_PRINCIPAL)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_lambda_payload_decodes_and_unknown_names_do_not_fail_the_batch() {
        let event: StreamEvent = serde_json::from_str(
            r#"{"Records":[
              {"eventID":"1","eventName":"INSERT","eventVersion":"1.1","eventSource":"aws:dynamodb",
               "awsRegion":"us-west-2",
               "dynamodb":{"ApproximateCreationDateTime":1760918400,"Keys":{"pk":{"S":"acct-1"},"sk":{"N":"1"}},
                 "NewImage":{"pk":{"S":"acct-1"},"sk":{"N":"1"},"flags":{"SS":["a"]},"blob":{"B":"AAE="},
                   "nested":{"M":{"x":{"L":[{"NULL":true},{"BOOL":false}]}}}},
                 "SequenceNumber":"100","SizeBytes":42,"StreamViewType":"NEW_IMAGE"},
               "eventSourceARN":"arn:aws:dynamodb:us-west-2:000000000000:table/events/stream/x"},
              {"eventName":"SOMETHING_NEW","dynamodb":{"Keys":{},"SequenceNumber":"101","StreamViewType":"LATER"}},
              {"eventName":"REMOVE","dynamodb":{"Keys":{"pk":{"S":"v"}},"SequenceNumber":"102"},
               "userIdentity":{"type":"Service","principalId":"dynamodb.amazonaws.com"}}
            ]}"#,
        )
        .unwrap();

        let [insert, unknown, ttl] = &event.records[..] else {
            panic!("three records");
        };
        assert_eq!(insert.event_name, EventName::Insert);
        assert_eq!(insert.sequence_number(), "100");
        assert_eq!(insert.pk(), Some("acct-1"));
        assert_eq!(
            insert.change.stream_view_type,
            Some(StreamViewType::NewImage)
        );
        assert_eq!(
            insert.change.new_image.as_ref().unwrap()["blob"],
            AttributeValue::B(vec![0, 1])
        );
        assert!(!insert.is_ttl_deletion());

        assert_eq!(unknown.event_name, EventName::Unknown);
        assert_eq!(
            unknown.change.stream_view_type,
            Some(StreamViewType::Unknown)
        );

        assert!(ttl.is_ttl_deletion());
    }
}
