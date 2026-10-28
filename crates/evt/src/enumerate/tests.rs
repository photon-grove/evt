use std::collections::VecDeque;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::conformance::Ledger;
use crate::ids::{EventId, EventSequence, EventType, EventVersion};
use crate::mem::MemRepository;
use crate::repository::{Commit, Repository};

async fn collect<S: Stream>(stream: S) -> Vec<S::Item> {
    let mut stream = pin!(stream);
    let mut items = Vec::new();
    while let Some(item) = poll_fn(|cx| stream.as_mut().poll_next(cx)).await {
        items.push(item);
    }

    items
}

fn event(entity: &str, entity_type: &str, sequence: i64, amount: i64) -> SerializedEvent {
    SerializedEvent {
        id: EventId::for_event(&EntityId::new(entity), EventSequence(sequence)),
        sequence: EventSequence(sequence),
        event_type: EventType::new("ledger:added"),
        version: EventVersion(1),
        entity_id: EntityId::new(entity),
        entity_type: EntityType::new(entity_type),
        payload: format!(r#"{{"amount":{amount}}}"#).into_bytes(),
        ..Default::default()
    }
}

async fn append(repo: &MemRepository, entity: &str, entity_type: &str, amounts: &[i64]) {
    let events = amounts
        .iter()
        .enumerate()
        .map(|(index, amount)| event(entity, entity_type, index as i64 + 1, *amount))
        .collect();

    repo.commit(Commit::new(entity, events)).await.unwrap();
}

fn ids(batches: &[EventBatch]) -> Vec<&str> {
    batches
        .iter()
        .map(|batch| batch.as_ref().unwrap()[0].entity_id.as_str())
        .collect()
}

#[tokio::test]
async fn memory_batches_one_entity_at_a_time_in_id_then_sequence_order() {
    let repo = MemRepository::new();
    append(&repo, "b", "ledger", &[1, 2, 3]).await;
    append(&repo, "a", "ledger", &[4]).await;
    append(&repo, "c", "other", &[5, 6]).await;

    let batches = collect(repo.stream_all_events(StreamFilter::all())).await;

    assert_eq!(ids(&batches), ["a", "b", "c"]);
    let sequences: Vec<Vec<i64>> = batches
        .iter()
        .map(|batch| {
            batch
                .as_ref()
                .unwrap()
                .iter()
                .map(|e| e.sequence.0)
                .collect()
        })
        .collect();
    assert_eq!(sequences, [vec![1], vec![1, 2, 3], vec![1, 2]]);
}

#[tokio::test]
async fn the_entity_type_filter_narrows_and_an_empty_type_is_not_a_wildcard() {
    let repo = MemRepository::new();
    append(&repo, "a", "ledger", &[1]).await;
    append(&repo, "b", "other", &[1]).await;
    append(&repo, "c", "", &[1]).await;

    let ledgers = collect(repo.stream_all_events(StreamFilter::of_type("ledger"))).await;
    assert_eq!(ids(&ledgers), ["a"]);

    let untyped = collect(repo.stream_all_events(StreamFilter::of_type(""))).await;
    assert_eq!(ids(&untyped), ["c"]);

    let none = collect(repo.stream_all_events(StreamFilter::of_type("missing"))).await;
    assert!(none.is_empty());
}

#[tokio::test]
async fn memory_skips_streams_with_only_a_snapshot() {
    let repo = MemRepository::new();
    append(&repo, "a", "ledger", &[1, 2]).await;
    repo.put_snapshot(&crate::SerializedSnapshot {
        entity_type: "ledger".into(),
        entity_id: "a".into(),
        sequence: EventSequence(1),
        event_sequence: EventSequence(2),
        payload: b"{}".to_vec(),
    })
    .await
    .unwrap();
    repo.compact_below(&"a".into(), EventSequence(2))
        .await
        .unwrap();

    assert!(
        collect(repo.stream_all_events(StreamFilter::all()))
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn entities_fold_by_type_and_a_bad_entity_does_not_end_the_stream() {
    let repo = MemRepository::new();
    append(&repo, "a", "ledger", &[1, 2]).await;
    append(&repo, "b", "other", &[100]).await;
    append(&repo, "c", "ledger", &[5]).await;
    repo.commit(Commit::new(
        "d",
        vec![SerializedEvent {
            event_type: EventType::new("ledger:unknown"),
            ..event("d", "ledger", 1, 0)
        }],
    ))
    .await
    .unwrap();

    let entities = collect(repo.stream_entities::<Ledger>(StreamFilter::all())).await;

    assert_eq!(entities.len(), 3);
    let a = entities[0].as_ref().unwrap();
    assert_eq!((a.entity_id().as_str(), a.aggregate().total), ("a", 3));
    assert_eq!(entities[1].as_ref().unwrap().aggregate().total, 5);
    assert!(matches!(entities[2], Err(Error::Replay { .. })));
}

#[tokio::test]
async fn a_compacted_entity_reports_its_gap_and_the_rest_still_stream() {
    let repo = MemRepository::new();
    append(&repo, "a", "ledger", &[1, 2, 3]).await;
    append(&repo, "b", "ledger", &[4]).await;
    repo.put_snapshot(&crate::SerializedSnapshot {
        entity_type: "ledger".into(),
        entity_id: "a".into(),
        sequence: EventSequence(1),
        event_sequence: EventSequence(2),
        payload: b"{}".to_vec(),
    })
    .await
    .unwrap();
    repo.compact_below(&"a".into(), EventSequence(2))
        .await
        .unwrap();

    let entities = collect(repo.stream_entities::<Ledger>(StreamFilter::all())).await;

    assert!(matches!(&entities[0], Err(Error::SequenceGap { .. })));
    assert_eq!(entities[1].as_ref().unwrap().aggregate().total, 4);
}

fn snapshot(entity: &str, event_sequence: i64, payload: &str) -> crate::SerializedSnapshot {
    crate::SerializedSnapshot {
        entity_type: "ledger".into(),
        entity_id: entity.into(),
        sequence: EventSequence(1),
        event_sequence: EventSequence(event_sequence),
        payload: payload.as_bytes().to_vec(),
    }
}

#[tokio::test]
async fn snapshot_seeded_entities_load_compacted_streams_from_their_snapshots() {
    let repo = MemRepository::new();
    // Compacted part-way: the snapshot covers 1..=2, event 3 remains.
    append(&repo, "b", "ledger", &[1, 2, 3]).await;
    repo.put_snapshot(&snapshot("b", 2, r#"{"total":3,"entries":2}"#))
        .await
        .unwrap();
    repo.compact_below(&"b".into(), EventSequence(2))
        .await
        .unwrap();
    // Compacted down to the snapshot: no events left at all.
    append(&repo, "a", "ledger", &[4, 5]).await;
    repo.put_snapshot(&snapshot("a", 2, r#"{"total":9,"entries":2}"#))
        .await
        .unwrap();
    repo.compact_below(&"a".into(), EventSequence(2))
        .await
        .unwrap();
    // No snapshot: every event.
    append(&repo, "c", "ledger", &[7]).await;
    append(&repo, "d", "other", &[100]).await;

    let entities =
        collect(repo.stream_entities_from_snapshots::<Ledger>(StreamFilter::all())).await;

    let loaded: Vec<_> = entities
        .into_iter()
        .map(|entity| {
            let entity = entity.unwrap();
            (
                entity.entity_id().to_string(),
                entity.aggregate().total,
                entity.sequence().0,
                entity.snapshot_sequence().0,
            )
        })
        .collect();
    assert_eq!(
        loaded,
        vec![
            ("a".to_owned(), 9, 2, 1),
            ("b".to_owned(), 6, 3, 1),
            ("c".to_owned(), 7, 1, 0),
        ],
        "ascending by ID, the stored type only, each from its snapshot plus the events after it"
    );

    let other =
        collect(repo.stream_entities_from_snapshots::<Ledger>(StreamFilter::of_type("other")))
            .await;
    assert_eq!(other.len(), 1);
    assert_eq!(other[0].as_ref().unwrap().aggregate().total, 100);
}

#[tokio::test]
async fn a_snapshot_seeded_entity_that_fails_to_load_does_not_end_the_stream() {
    let repo = MemRepository::new();
    append(&repo, "a", "ledger", &[1]).await;
    repo.put_snapshot(&snapshot("a", 1, "not json"))
        .await
        .unwrap();
    append(&repo, "b", "ledger", &[2]).await;

    let entities =
        collect(repo.stream_entities_from_snapshots::<Ledger>(StreamFilter::all())).await;

    assert!(matches!(&entities[0], Err(Error::Snapshot { .. })));
    assert_eq!(entities[1].as_ref().unwrap().aggregate().total, 2);
}

/// A log that serves canned pages, like a DynamoDB Scan: no entity grouping, and every pull counted.
struct Paged {
    pages: Vec<Option<Vec<SerializedEvent>>>,
    pulled: Arc<AtomicUsize>,
}

impl Paged {
    fn new(pages: Vec<Option<Vec<SerializedEvent>>>) -> Self {
        Self {
            pages,
            pulled: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl EventLog for Paged {
    fn stream_all_events(&self, _: StreamFilter) -> impl Stream<Item = EventBatch> + Send {
        let pages: VecDeque<_> = self.pages.clone().into();

        unfold(
            (pages, self.pulled.clone()),
            |(mut pages, pulled)| async move {
                let page = pages.pop_front()?;
                pulled.fetch_add(1, Ordering::SeqCst);

                Some((
                    page.ok_or_else(|| Error::backend("scan failed")),
                    (pages, pulled),
                ))
            },
        )
    }
}

fn scan_pages() -> Vec<Option<Vec<SerializedEvent>>> {
    vec![
        Some(vec![
            event("b", "ledger", 2, 20),
            event("a", "ledger", 1, 1),
        ]),
        Some(vec![
            event("a", "ledger", 2, 2),
            event("b", "ledger", 1, 10),
        ]),
        Some(vec![event("c", "ledger", 1, 7)]),
    ]
}

#[tokio::test]
async fn scanned_events_group_by_entity_in_first_seen_order_and_fold_by_sequence() {
    let log = Paged::new(scan_pages());

    let entities = collect(log.stream_entities::<Ledger>(StreamFilter::all())).await;

    let summary: Vec<_> = entities
        .iter()
        .map(|entity| {
            let entity = entity.as_ref().unwrap();
            (
                entity.entity_id().to_string(),
                entity.aggregate().total,
                entity.sequence().0,
            )
        })
        .collect();
    assert_eq!(
        summary,
        [("b".into(), 30, 2), ("a".into(), 3, 2), ("c".into(), 7, 1)]
    );
}

#[tokio::test]
async fn a_failed_scan_yields_the_error_and_no_partial_entities() {
    let mut pages = scan_pages();
    pages.insert(2, None);
    let log = Paged::new(pages);

    let entities = collect(log.stream_entities::<Ledger>(StreamFilter::all())).await;

    assert_eq!(entities.len(), 1);
    assert!(matches!(entities[0], Err(Error::Backend(_))));
    assert_eq!(
        log.pulled.load(Ordering::SeqCst),
        3,
        "the stream stops at the error"
    );
}

#[tokio::test]
async fn dropping_a_stream_stops_pulling_pages() {
    let log = Paged::new(scan_pages());

    {
        let mut stream = pin!(log.stream_all_events(StreamFilter::all()));
        let first = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
        assert!(first.is_some());
        assert_eq!(
            log.pulled.load(Ordering::SeqCst),
            1,
            "pages are pulled on demand"
        );
    }

    assert_eq!(
        log.pulled.load(Ordering::SeqCst),
        1,
        "a dropped stream pulls nothing more"
    );
}

#[tokio::test]
async fn an_entity_stream_pulls_only_as_far_as_it_is_polled_when_batches_are_whole() {
    struct Whole(Paged);

    impl EventLog for Whole {
        const BATCHES_BY_ENTITY: bool = true;

        fn stream_all_events(&self, filter: StreamFilter) -> impl Stream<Item = EventBatch> + Send {
            self.0.stream_all_events(filter)
        }
    }

    let log = Whole(Paged::new(vec![
        Some(vec![event("a", "ledger", 1, 1)]),
        Some(vec![event("b", "ledger", 1, 2)]),
        Some(vec![event("c", "ledger", 1, 3)]),
    ]));

    let mut stream = pin!(log.stream_entities::<Ledger>(StreamFilter::all()));
    let first = poll_fn(|cx| stream.as_mut().poll_next(cx))
        .await
        .unwrap()
        .unwrap();

    assert_eq!(first.aggregate().total, 1);
    assert_eq!(log.0.pulled.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn the_trait_is_usable_through_references_and_arcs() {
    async fn count(log: impl EventLog) -> usize {
        collect(log.stream_all_events(StreamFilter::all()))
            .await
            .len()
    }

    let repo = Arc::new(MemRepository::new());
    append(&repo, "a", "ledger", &[1]).await;

    assert_eq!(count(repo.clone()).await, 1);
    assert_eq!(count(&*repo).await, 1);
}
