//! [`EventLog`] for the in-memory repository.

use std::ops::Bound;

use futures_core::Stream;

use super::{BTreeMap, EntityId, MemRepository, Stream as EntityStream, features};
use crate::enumerate::{EventBatch, EventLog, StreamFilter, unfold};

impl EventLog for MemRepository {
    const BATCHES_BY_ENTITY: bool = true;

    fn stream_all_events(&self, filter: StreamFilter) -> impl Stream<Item = EventBatch> + Send {
        unfold(
            Cursor {
                streams: self.streams.clone(),
                after: None,
                filter,
            },
            next_batch,
        )
    }
}

/// Where the enumeration stands. Each step locks the log briefly and reads the next stream after
/// the last one yielded, so no lock is held between polls and a dropped stream leaves nothing
/// behind. Entities committed behind the cursor while a stream is open are not seen.
struct Cursor {
    streams: std::sync::Arc<std::sync::Mutex<BTreeMap<EntityId, EntityStream>>>,
    after: Option<EntityId>,
    filter: StreamFilter,
}

async fn next_batch(mut cursor: Cursor) -> Option<(EventBatch, Cursor)> {
    let found = {
        let streams = cursor
            .streams
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let lower = cursor
            .after
            .as_ref()
            .map_or(Bound::Unbounded, Bound::Excluded);

        streams
            .range::<EntityId, _>((lower, Bound::Unbounded))
            .filter(|(id, _)| !features::is_reserved(id))
            .filter(|(_, stream)| {
                stream
                    .entity_type()
                    .is_some_and(|entity_type| cursor.filter.matches(entity_type))
            })
            .find(|(_, stream)| !stream.events.is_empty())
            .map(|(id, stream)| (id.clone(), stream.events.values().cloned().collect()))
    };

    let (id, events) = found?;
    cursor.after = Some(id);

    Some((Ok(events), cursor))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn conforms_to_the_enumeration_cases() {
        crate::conformance::enumeration_cases(&MemRepository::new()).await;
    }
}
