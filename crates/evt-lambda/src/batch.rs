//! Partial batch failures: process a batch in order, stop at the first failure, and tell Lambda
//! where to resume.
//!
//! With `FunctionResponseTypes: ["ReportBatchItemFailures"]` on the event source mapping, a
//! function returns `{"batchItemFailures":[{"itemIdentifier":"<SequenceNumber>"}]}`. For a stream,
//! Lambda checkpoints **before the lowest reported sequence number**: the records ahead of it are
//! done, and it delivers that record and every later one again. So the helpers here hand records
//! to the handler in stream order, stop at the first record that fails (a decode error or a handler
//! error), and report exactly that record. The records after it are not attempted: they come back
//! in the retry, behind the one that failed, so per-item order holds.
//!
//! Every record from the failed one on may be delivered more than once, and so may the failed
//! record's own side effects if it failed halfway: **handlers must be idempotent**. Key them on
//! the event ID ([`StreamedEvent::idempotency_key`](crate::StreamedEvent::idempotency_key)), or,
//! for a projector, on the last sequence it applied per stream. Journal consumers are idempotent
//! by construction: they read from their own cursor.
//!
//! Without `ReportBatchItemFailures`, a returned response is ignored, and only an error fails (and
//! retries) the whole batch. An empty `itemIdentifier` also fails the whole batch, which is what a
//! record with no sequence number gets. Bound the retries on the mapping (`MaximumRetryAttempts`,
//! `MaximumRecordAgeInSeconds`) and give it an on-failure destination, or one poison record stops
//! its shard until the record leaves the stream.

use photon_evt::BoxError;
use serde::{Deserialize, Serialize};

use crate::events::{StreamedEvent, decode_event};
use crate::filter::EventFilter;
use crate::record::{StreamEvent, StreamRecord};

/// The response a function returns for a partial batch failure.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchResponse {
    /// The failed records; empty when the whole batch succeeded.
    #[serde(rename = "batchItemFailures", default)]
    pub batch_item_failures: Vec<BatchItemFailure>,
}

/// One failed record.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchItemFailure {
    /// The record's sequence number.
    #[serde(rename = "itemIdentifier")]
    pub item_identifier: String,
}

impl BatchResponse {
    /// The whole batch succeeded.
    pub fn success() -> Self {
        Self::default()
    }

    /// Resume from the record with this sequence number.
    pub fn failed_from(sequence_number: impl Into<String>) -> Self {
        Self {
            batch_item_failures: vec![BatchItemFailure {
                item_identifier: sequence_number.into(),
            }],
        }
    }

    /// Whether no record failed.
    pub fn is_success(&self) -> bool {
        self.batch_item_failures.is_empty()
    }
}

/// The record a batch stopped at, and why.
#[derive(Debug)]
#[non_exhaustive]
pub struct RecordFailure {
    /// Its index in the batch.
    pub index: usize,
    /// Its sequence number, which the response reports.
    pub sequence_number: String,
    /// The decode or handler error.
    pub error: BoxError,
}

/// How a batch went: what to return to Lambda, and what to log.
#[derive(Debug, Default)]
#[non_exhaustive]
#[must_use = "return `outcome.response()` from the function, or the failure is lost"]
pub struct BatchOutcome {
    /// How many records of the batch, from the start, are done: every record when it succeeded,
    /// otherwise the index of the record that failed.
    pub completed: usize,
    /// How many times the handler ran and succeeded.
    pub handled: usize,
    /// The record the batch stopped at.
    pub failure: Option<RecordFailure>,
}

impl BatchOutcome {
    pub(crate) fn failed(&mut self, index: usize, sequence_number: &str, error: BoxError) {
        self.completed = index;
        self.failure = Some(RecordFailure {
            index,
            sequence_number: sequence_number.to_owned(),
            error,
        });
    }

    /// The response to return from the function.
    pub fn response(&self) -> BatchResponse {
        match &self.failure {
            None => BatchResponse::success(),
            Some(failure) => BatchResponse::failed_from(failure.sequence_number.clone()),
        }
    }

    /// Whether every record succeeded.
    pub fn is_success(&self) -> bool {
        self.failure.is_none()
    }
}

/// Hands every record to `handler`, in order, stopping at the first error.
pub async fn process_records<F, E>(event: &StreamEvent, mut handler: F) -> BatchOutcome
where
    F: AsyncFnMut(&StreamRecord) -> Result<(), E>,
    E: Into<BoxError>,
{
    let mut outcome = BatchOutcome::default();

    for (index, record) in event.records.iter().enumerate() {
        if let Err(err) = handler(record).await {
            outcome.failed(index, record.sequence_number(), err.into());

            return outcome;
        }

        outcome.handled += 1;
    }

    outcome.completed = event.records.len();

    outcome
}

/// Hands every committed event of an event-log table's batch that `filter` matches to `handler`,
/// in order, stopping at the first record that does not decode or whose handler fails.
///
/// Records that are not committed events (see [`decode_event`]) and events the filter does not
/// match are acknowledged without calling the handler. The filter is applied again here so the
/// function stays correct whatever its mapping's filter criteria say; pass [`EventFilter::new`]
/// for every event.
pub async fn process_events<F, E>(
    event: &StreamEvent,
    filter: &EventFilter,
    mut handler: F,
) -> BatchOutcome
where
    F: AsyncFnMut(StreamedEvent) -> Result<(), E>,
    E: Into<BoxError>,
{
    let mut outcome = BatchOutcome::default();

    for (index, record) in event.records.iter().enumerate() {
        let streamed = match decode_event(record) {
            Ok(Some(streamed)) if filter.matches_event(&streamed.event) => streamed,
            Ok(_) => continue,
            Err(err) => {
                outcome.failed(index, record.sequence_number(), Box::new(err));

                return outcome;
            }
        };

        if let Err(err) = handler(streamed).await {
            outcome.failed(index, record.sequence_number(), err.into());

            return outcome;
        }

        outcome.handled += 1;
    }

    outcome.completed = event.records.len();

    outcome
}

#[cfg(test)]
mod tests {
    use photon_evt::wire::dynamo::{AttributeValue, Item, event_item};

    use super::*;
    use crate::filter::tests::event;
    use crate::record::EventName;

    fn event_record(entity_type: &str, event_type: &str, sequence: &str) -> StreamRecord {
        let item = event_item(&event(entity_type, event_type), None).unwrap();
        let keys = Item::from([
            ("pk".into(), item["pk"].clone()),
            ("sk".into(), item["sk"].clone()),
        ]);

        StreamRecord::new(EventName::Insert, keys, sequence).with_new_image(item)
    }

    fn batch(records: Vec<StreamRecord>) -> StreamEvent {
        StreamEvent { records }
    }

    #[tokio::test]
    async fn a_batch_that_succeeds_reports_no_failure() {
        let mut head = event_record("account", "account:opened", "2");
        head.change
            .keys
            .insert("sk".into(), AttributeValue::N("-1".into()));
        let event = batch(vec![
            event_record("account", "account:opened", "1"),
            head,
            event_record("order", "order:placed", "3"),
            event_record("account", "account:closed", "4"),
        ]);

        let mut seen = Vec::new();
        let outcome = process_events(
            &event,
            &EventFilter::new().entity_type("account"),
            async |streamed: StreamedEvent| {
                seen.push(streamed.sequence_number);
                Ok::<_, BoxError>(())
            },
        )
        .await;

        assert!(outcome.is_success());
        assert_eq!(outcome.completed, 4);
        assert_eq!(outcome.handled, 2);
        assert_eq!(seen, ["1", "4"]);
        assert_eq!(
            serde_json::to_string(&outcome.response()).unwrap(),
            r#"{"batchItemFailures":[]}"#
        );
    }

    #[tokio::test]
    async fn the_first_failure_is_reported_and_later_records_are_not_attempted() {
        let event = batch(vec![
            event_record("account", "account:opened", "11"),
            event_record("account", "account:failing", "12"),
            event_record("account", "account:closed", "13"),
        ]);

        let mut seen = Vec::new();
        let outcome = process_events(
            &event,
            &EventFilter::new(),
            async |streamed: StreamedEvent| {
                seen.push(streamed.sequence_number.clone());

                if streamed.event.event_type.as_str() == "account:failing" {
                    return Err("downstream unavailable");
                }

                Ok(())
            },
        )
        .await;

        assert_eq!(seen, ["11", "12"]);
        assert_eq!(outcome.completed, 1);
        assert_eq!(outcome.handled, 1);

        let failure = outcome.failure.as_ref().unwrap();
        assert_eq!(failure.index, 1);
        assert_eq!(failure.error.to_string(), "downstream unavailable");
        assert_eq!(
            serde_json::to_string(&outcome.response()).unwrap(),
            r#"{"batchItemFailures":[{"itemIdentifier":"12"}]}"#
        );
    }

    #[tokio::test]
    async fn a_record_that_does_not_decode_fails_the_batch_there() {
        let mut corrupt = event_record("account", "account:opened", "22");
        corrupt
            .change
            .new_image
            .as_mut()
            .unwrap()
            .remove("metadata");
        let event = batch(vec![
            event_record("account", "account:opened", "21"),
            corrupt,
            event_record("account", "account:opened", "23"),
        ]);

        let mut handled = 0;
        let outcome = process_events(&event, &EventFilter::new(), async |_: StreamedEvent| {
            handled += 1;
            Ok::<_, BoxError>(())
        })
        .await;

        assert_eq!(handled, 1);
        assert_eq!(outcome.response(), BatchResponse::failed_from("22"));
    }

    #[tokio::test]
    async fn process_records_hands_over_every_record() {
        let event = batch(vec![
            event_record("account", "account:opened", "1"),
            StreamRecord::new(EventName::Remove, Item::new(), "2"),
        ]);

        let outcome = process_records(&event, async |record: &StreamRecord| {
            if record.event_name == EventName::Remove {
                return Err("no removals");
            }

            Ok(())
        })
        .await;

        assert_eq!(outcome.completed, 1);
        assert_eq!(outcome.response(), BatchResponse::failed_from("2"));

        let empty = process_records(&StreamEvent::default(), async |_: &StreamRecord| {
            Ok::<_, BoxError>(())
        })
        .await;
        assert!(empty.is_success());
    }
}
