//! [`EventLog`] for DynamoDB: a paginated Scan, one batch per page.

use std::collections::HashMap;

use futures_core::Stream;

use super::{DynamoRepository, from_sdk_item, s, sdk, sdk_error};
use crate::enumerate::{EventBatch, EventLog, StreamFilter, unfold};
use crate::features::{COMMAND_OUTCOME_PREFIX, SYSTEM_PREFIX};
use crate::wire::dynamo as wire;

impl EventLog for DynamoRepository {
    /// Each batch is one Scan page of events, in the table's order and not grouped by entity.
    /// Pages the filter empties out are skipped, and so are every row below `sk = 1` (inline
    /// snapshots at `sk = 0` and stream head items at `sk = -1`) and the reserved namespaces. Reads are strongly consistent when the repository is configured for
    /// it.
    fn stream_all_events(&self, filter: StreamFilter) -> impl Stream<Item = EventBatch> + Send {
        unfold(
            Scan {
                repository: self,
                filter,
                start: None,
                finished: false,
            },
            next_page,
        )
    }
}

struct Scan<'a> {
    repository: &'a DynamoRepository,
    filter: StreamFilter,
    start: Option<HashMap<String, sdk::AttributeValue>>,
    finished: bool,
}

async fn next_page(mut scan: Scan<'_>) -> Option<(EventBatch, Scan<'_>)> {
    loop {
        if scan.finished {
            return None;
        }

        match read_page(&mut scan).await {
            Ok(events) if events.is_empty() => {}
            Ok(events) => return Some((Ok(events), scan)),
            Err(err) => {
                scan.finished = true;

                return Some((Err(err), scan));
            }
        }
    }
}

/// Reads one Scan page and advances the scan's start key. A filtered scan can return an empty
/// page with a continuation key, so the caller keeps reading until a page has events.
async fn read_page(scan: &mut Scan<'_>) -> Result<Vec<crate::SerializedEvent>, crate::Error> {
    let repository = scan.repository;

    // Reserved partitions are engine-managed rows, never ordinary aggregates (v0.9, C8).
    let mut filter = "NOT begins_with(pk, :system) AND NOT begins_with(pk, :outcome)".to_owned();
    if scan.filter.entity_type.is_some() {
        filter.push_str(" AND entityType = :et");
    }

    let mut request = repository
        .client
        .scan()
        .table_name(&repository.table)
        .consistent_read(repository.consistent_read)
        .filter_expression(&filter)
        .expression_attribute_values(":system", s(SYSTEM_PREFIX))
        .expression_attribute_values(":outcome", s(COMMAND_OUTCOME_PREFIX))
        .set_limit(repository.scan_page_size)
        .set_exclusive_start_key(scan.start.take());

    if let Some(entity_type) = &scan.filter.entity_type {
        request = request.expression_attribute_values(":et", s(entity_type.as_str()));
    }

    let page = request.send().await.map_err(sdk_error)?;

    match page.last_evaluated_key.as_ref() {
        Some(key) if !key.is_empty() => scan.start = Some(key.clone()),
        _ => scan.finished = true,
    }

    let mut events = Vec::new();
    for item in page.items() {
        let item = from_sdk_item(item);
        // Only events are decoded: the snapshot (`sk = 0`) and a stream's head item (`sk = -1`,
        // which an unfiltered scan returns because it has no `entityType`) are not events. A row
        // with no readable sort key is left to the decoder, which refuses it.
        if wire::sort_key(&item).is_some_and(|sk| sk < 1) {
            continue;
        }

        events.push(wire::event_from_item(&item)?);
    }

    Ok(events)
}
