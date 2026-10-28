//! A DynamoDB Streams handler for an event-log table, without a Lambda runtime.
//!
//! The batch below is the JSON Lambda delivers for an event-log table's stream (`NEW_IMAGE`). The
//! handler deserializes it into a `StreamEvent`, and `process_events` decodes each record into the
//! committed event, skips the engine's own items (here, a stream head) and the events the
//! `EventFilter` does not match (here, an account), and hands the rest to a projector in stream
//! order. The projector keeps an order board and is idempotent: it remembers the last sequence it
//! applied per order and ignores anything at or below it.
//!
//! The example then plays three deliveries of the batch:
//!
//! 1. The projector fails on one record (a simulated outage). Processing stops there, and the
//!    response names that record's `SequenceNumber`, so Lambda resumes from it.
//! 2. Lambda delivers that record and everything after it again, and they succeed.
//! 3. The whole batch arrives again (delivery is at least once), and nothing is applied twice.
//!
//! In a function, return `outcome.response()` from the `lambda_runtime` handler (see the crate
//! documentation) and set `FunctionResponseTypes: ["ReportBatchItemFailures"]` on the mapping.
//!
//! Run it with `cargo run -p photon-evt-lambda --example stream_handler`.

use std::collections::BTreeMap;

use photon_evt::{BoxError, EventSequence};
use photon_evt_lambda::{BatchOutcome, EventFilter, StreamEvent, StreamedEvent, process_events};

/// One batch from an event-log table's stream, as Lambda delivers it. Each event item carries its
/// stream (`pk`), sequence (`sk`), type, version, payload JSON and metadata JSON.
const BATCH: &str = r#"{
  "Records": [
    {
      "eventID": "1",
      "eventName": "INSERT",
      "eventSource": "aws:dynamodb",
      "awsRegion": "us-west-2",
      "dynamodb": {
        "Keys": { "pk": { "S": "order-1" }, "sk": { "N": "1" } },
        "NewImage": {
          "pk": { "S": "order-1" },
          "sk": { "N": "1" },
          "entityType": { "S": "order" },
          "type": { "S": "order.placed" },
          "version": { "N": "1" },
          "payload": { "S": "{\"total\":1200}" },
          "metadata": { "S": "{\"commandId\":\"place-order-1\",\"region\":\"us-west-2\"}" }
        },
        "SequenceNumber": "100",
        "StreamViewType": "NEW_IMAGE"
      }
    },
    {
      "eventID": "2",
      "eventName": "INSERT",
      "eventSource": "aws:dynamodb",
      "awsRegion": "us-west-2",
      "dynamodb": {
        "Keys": { "pk": { "S": "order-1" }, "sk": { "N": "-1" } },
        "NewImage": {
          "pk": { "S": "order-1" },
          "sk": { "N": "-1" },
          "head": { "N": "1" }
        },
        "SequenceNumber": "101",
        "StreamViewType": "NEW_IMAGE"
      }
    },
    {
      "eventID": "3",
      "eventName": "INSERT",
      "eventSource": "aws:dynamodb",
      "awsRegion": "us-west-2",
      "dynamodb": {
        "Keys": { "pk": { "S": "acct-1" }, "sk": { "N": "1" } },
        "NewImage": {
          "pk": { "S": "acct-1" },
          "sk": { "N": "1" },
          "entityType": { "S": "account" },
          "type": { "S": "account.opened" },
          "version": { "N": "1" },
          "payload": { "S": "{\"balance\":100}" },
          "metadata": { "S": "{\"commandId\":\"open-acct-1\",\"region\":\"us-west-2\"}" }
        },
        "SequenceNumber": "102",
        "StreamViewType": "NEW_IMAGE"
      }
    },
    {
      "eventID": "4",
      "eventName": "INSERT",
      "eventSource": "aws:dynamodb",
      "awsRegion": "us-west-2",
      "dynamodb": {
        "Keys": { "pk": { "S": "order-2" }, "sk": { "N": "1" } },
        "NewImage": {
          "pk": { "S": "order-2" },
          "sk": { "N": "1" },
          "entityType": { "S": "order" },
          "type": { "S": "order.placed" },
          "version": { "N": "1" },
          "payload": { "S": "{\"total\":800}" },
          "metadata": { "S": "{\"commandId\":\"place-order-2\",\"region\":\"us-west-2\"}" }
        },
        "SequenceNumber": "103",
        "StreamViewType": "NEW_IMAGE"
      }
    },
    {
      "eventID": "5",
      "eventName": "INSERT",
      "eventSource": "aws:dynamodb",
      "awsRegion": "us-west-2",
      "dynamodb": {
        "Keys": { "pk": { "S": "order-1" }, "sk": { "N": "2" } },
        "NewImage": {
          "pk": { "S": "order-1" },
          "sk": { "N": "2" },
          "entityType": { "S": "order" },
          "type": { "S": "order.shipped" },
          "version": { "N": "1" },
          "payload": { "S": "{}" },
          "metadata": { "S": "{\"commandId\":\"ship-order-1\",\"region\":\"us-west-2\"}" }
        },
        "SequenceNumber": "104",
        "StreamViewType": "NEW_IMAGE"
      }
    }
  ]
}"#;

/// A read model of order statuses, fed from the stream.
#[derive(Debug, Default)]
pub struct OrderBoard {
    /// The last sequence applied per order: the projector's idempotency checkpoint. Records arrive
    /// in order per stream, so anything at or below it was applied by an earlier delivery.
    applied: BTreeMap<String, EventSequence>,
    /// Each order's status.
    pub status: BTreeMap<String, &'static str>,
    /// How many events were applied, counting no redelivery twice.
    pub applied_events: usize,
}

impl OrderBoard {
    /// Applies a streamed event unless an earlier delivery already applied it.
    pub fn apply(&mut self, streamed: &StreamedEvent) -> Result<(), BoxError> {
        let event = &streamed.event;
        let order = event.entity_id.to_string();

        if self
            .applied
            .get(&order)
            .is_some_and(|last| event.sequence <= *last)
        {
            return Ok(());
        }

        let status = match event.event_type.as_str() {
            "order.placed" => "placed",
            "order.shipped" => "shipped",
            other => return Err(format!("unknown order event {other}").into()),
        };

        self.status.insert(order.clone(), status);
        self.applied.insert(order, event.sequence);
        self.applied_events += 1;

        Ok(())
    }
}

/// Runs one delivery of `batch` through the board. `outage` names an order whose events fail, as a
/// downstream failure would.
pub async fn deliver(
    batch: &StreamEvent,
    board: &mut OrderBoard,
    outage: Option<&str>,
) -> BatchOutcome {
    let filter = EventFilter::new().entity_type("order");

    process_events(batch, &filter, async |streamed: StreamedEvent| {
        if outage == Some(streamed.event.entity_id.as_str()) {
            return Err(
                format!("downstream unavailable for {}", streamed.idempotency_key()).into(),
            );
        }

        board.apply(&streamed)
    })
    .await
}

/// Plays the three deliveries and returns what `main` prints.
pub async fn run() -> Result<Vec<String>, BoxError> {
    let batch: StreamEvent = serde_json::from_str(BATCH)?;
    let mut board = OrderBoard::default();
    let mut lines = vec![format!(
        "filter: {}",
        EventFilter::new().entity_type("order").pattern()?
    )];

    // 1. order-2's projection fails: the batch stops there and reports its record.
    let first = deliver(&batch, &mut board, Some("order-2")).await;
    lines.push(describe("delivery 1", &first, &board)?);

    // 2. Lambda delivers the failed record and every record after it again.
    let failed = first.failure.as_ref().ok_or("expected a failure")?;
    let retry = StreamEvent {
        records: batch.records[failed.index..].to_vec(),
    };
    let second = deliver(&retry, &mut board, None).await;
    lines.push(describe("delivery 2", &second, &board)?);

    // 3. The whole batch arrives again; the checkpoints keep it from applying twice.
    let third = deliver(&batch, &mut board, None).await;
    lines.push(describe("delivery 3", &third, &board)?);

    let statuses: Vec<String> = board
        .status
        .iter()
        .map(|(order, status)| format!("{order}={status}"))
        .collect();
    lines.push(format!("orders: {}", statuses.join(" ")));

    Ok(lines)
}

fn describe(label: &str, outcome: &BatchOutcome, board: &OrderBoard) -> Result<String, BoxError> {
    Ok(format!(
        "{label}: completed {} records, applied {} events in total, response {}",
        outcome.completed,
        board.applied_events,
        serde_json::to_string(&outcome.response())?,
    ))
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    for line in run().await? {
        println!("{line}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use photon_evt_lambda::BatchResponse;

    use super::*;

    #[tokio::test]
    async fn it_prints_each_delivery() {
        assert_eq!(
            run().await.unwrap(),
            [
                r#"filter: {"eventName":["INSERT"],"dynamodb":{"NewImage":{"entityType":{"S":["order"]},"metadata":{"S":[{"exists":true}]},"type":{"S":[{"exists":true}]}}}}"#,
                r#"delivery 1: completed 3 records, applied 1 events in total, response {"batchItemFailures":[{"itemIdentifier":"103"}]}"#,
                r#"delivery 2: completed 2 records, applied 3 events in total, response {"batchItemFailures":[]}"#,
                r#"delivery 3: completed 5 records, applied 3 events in total, response {"batchItemFailures":[]}"#,
                "orders: order-1=shipped order-2=placed",
            ]
        );
    }

    #[tokio::test]
    async fn the_fixture_decodes_to_three_order_events() {
        let batch: StreamEvent = serde_json::from_str(BATCH).unwrap();
        let mut keys = Vec::new();

        let outcome = process_events(
            &batch,
            &EventFilter::new().entity_type("order"),
            async |streamed: StreamedEvent| {
                keys.push(streamed.idempotency_key().to_owned());
                Ok::<_, BoxError>(())
            },
        )
        .await;

        assert!(outcome.is_success());
        assert_eq!(outcome.completed, 5);
        assert_eq!(keys, ["order-1:1", "order-2:1", "order-1:2"]);
    }

    #[tokio::test]
    async fn a_failure_reports_the_record_to_resume_from() {
        let batch: StreamEvent = serde_json::from_str(BATCH).unwrap();
        let mut board = OrderBoard::default();

        let outcome = deliver(&batch, &mut board, Some("order-1")).await;

        assert_eq!(outcome.response(), BatchResponse::failed_from("100"));
        assert_eq!(outcome.completed, 0);
        assert!(board.status.is_empty());
    }
}
