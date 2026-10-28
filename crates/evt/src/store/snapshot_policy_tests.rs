//! Unit tests for the snapshot policy (P5, P6) and the load telemetry (P9). The conformance suite
//! covers the policy on every backend; these pin what it cannot see: the `tracing` fields, a
//! failed catch-up write, and an aggregate that disables its own snapshots.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Level, Subscriber};

use crate::conformance::{Add, InvalidAmount, Ledger, LedgerEvent};
use crate::{
    Aggregate, Commit, EntityId, EntityType, Error, EventSequence, MemRepository, Metadata,
    Repository, SerializedEvent, SerializedSnapshot, Store, TransactionCommit,
};

type Fields = BTreeMap<String, String>;

/// The spans and events a test's code emitted, with their fields.
#[derive(Default)]
struct Captured {
    spans: Vec<(&'static str, Fields)>,
    events: Vec<(Level, Option<usize>, Fields)>,
    current: Vec<usize>,
}

/// A minimal `tracing` subscriber that records every span's and event's fields.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Captured>>);

struct FieldVisitor<'a>(&'a mut Fields);

impl Visit for FieldVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
}

impl Subscriber for Capture {
    fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        let mut fields = Fields::new();
        attributes.record(&mut FieldVisitor(&mut fields));

        let mut captured = self.0.lock().unwrap();
        captured.spans.push((attributes.metadata().name(), fields));

        Id::from_u64(captured.spans.len() as u64)
    }

    fn record(&self, span: &Id, values: &Record<'_>) {
        let mut captured = self.0.lock().unwrap();
        let index = span.into_u64() as usize - 1;
        values.record(&mut FieldVisitor(&mut captured.spans[index].1));
    }

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields::new();
        event.record(&mut FieldVisitor(&mut fields));

        let mut captured = self.0.lock().unwrap();
        let parent = captured.current.last().copied();
        captured
            .events
            .push((*event.metadata().level(), parent, fields));
    }

    fn enter(&self, span: &Id) {
        self.0
            .lock()
            .unwrap()
            .current
            .push(span.into_u64() as usize - 1);
    }

    fn exit(&self, _: &Id) {
        self.0.lock().unwrap().current.pop();
    }
}

impl Capture {
    /// The fields of every `load` span, in creation order.
    fn loads(&self) -> Vec<Fields> {
        self.0
            .lock()
            .unwrap()
            .spans
            .iter()
            .filter(|(name, _)| *name == "load")
            .map(|(_, fields)| fields.clone())
            .collect()
    }
}

fn fields(pairs: &[(&str, &str)]) -> Fields {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
        .collect()
}

#[tokio::test]
async fn every_load_records_events_replayed_and_snapshot_seq() {
    let repository = MemRepository::new();
    let store = Store::<Ledger, _>::new(&repository, ()).with_snapshot_interval(2);
    for amount in 1..=3 {
        store
            .execute("acct-1", Add(amount), Metadata::default())
            .await
            .unwrap();
    }
    Store::<Ledger, _>::new(&repository, ())
        .with_snapshot_interval(0)
        .execute("acct-2", Add(1), Metadata::default())
        .await
        .unwrap();

    let capture = Capture::default();
    let guard = tracing::subscriber::set_default(capture.clone());
    store.load("acct-1").await.unwrap();
    store.load("acct-2").await.unwrap();
    let mut tx = store.transaction();
    tx.read::<Ledger>("acct-1").await.unwrap();
    drop(guard);

    assert_eq!(
        capture.loads(),
        vec![
            // From the snapshot at event 2, replaying the one event after it.
            fields(&[
                ("entity_type", "ledger"),
                ("entity_id", "acct-1"),
                ("sequence", "3"),
                ("snapshot_seq", "1"),
                ("events_replayed", "1"),
            ]),
            // No snapshot: every event replays.
            fields(&[
                ("entity_type", "ledger"),
                ("entity_id", "acct-2"),
                ("sequence", "1"),
                ("snapshot_seq", "0"),
                ("events_replayed", "1"),
            ]),
            // A transaction read is a load too.
            fields(&[
                ("entity_type", "ledger"),
                ("entity_id", "acct-1"),
                ("sequence", "3"),
                ("snapshot_seq", "1"),
                ("events_replayed", "1"),
            ]),
        ]
    );
}

/// A repository whose standalone snapshot writes always fail.
struct RefusingSnapshots<R>(R);

impl<R: Repository> Repository for RefusingSnapshots<R> {
    type Operation = R::Operation;

    async fn commit(&self, commit: Commit<R::Operation>) -> Result<(), Error> {
        self.0.commit(commit).await
    }

    async fn commit_transaction(
        &self,
        commit: TransactionCommit<R::Operation>,
    ) -> Result<(), Error> {
        self.0.commit_transaction(commit).await
    }

    async fn events(&self, entity_id: &EntityId) -> Result<Vec<SerializedEvent>, Error> {
        self.0.events(entity_id).await
    }

    async fn events_after(
        &self,
        entity_id: &EntityId,
        after: EventSequence,
    ) -> Result<Vec<SerializedEvent>, Error> {
        self.0.events_after(entity_id, after).await
    }

    async fn latest(&self, entity_id: &EntityId, n: usize) -> Result<Vec<SerializedEvent>, Error> {
        self.0.latest(entity_id, n).await
    }

    async fn snapshot(&self, entity_id: &EntityId) -> Result<Option<SerializedSnapshot>, Error> {
        self.0.snapshot(entity_id).await
    }

    async fn put_snapshot(&self, _: &SerializedSnapshot) -> Result<bool, Error> {
        Err(Error::InvalidConfig("snapshot writes are refused".into()))
    }

    async fn compact_below(
        &self,
        entity_id: &EntityId,
        through: EventSequence,
    ) -> Result<usize, Error> {
        self.0.compact_below(entity_id, through).await
    }

    async fn entity_heads(
        &self,
        entity_type: Option<&EntityType>,
    ) -> Result<BTreeMap<EntityId, EventSequence>, Error> {
        self.0.entity_heads(entity_type).await
    }
}

#[tokio::test]
async fn a_failed_catch_up_is_logged_and_the_load_still_succeeds() {
    let inner = MemRepository::new();
    Store::<Ledger, _>::new(&inner, ())
        .with_snapshot_interval(0)
        .execute("acct-1", Add(5), Metadata::default())
        .await
        .unwrap();
    let store = Store::<Ledger, _>::new(RefusingSnapshots(&inner), ())
        .with_snapshot_interval(0)
        .with_catch_up_snapshot_threshold(1);

    let capture = Capture::default();
    let guard = tracing::subscriber::set_default(capture.clone());
    let loaded = store.load("acct-1").await.unwrap();
    drop(guard);

    assert_eq!(loaded.aggregate().total, 5);
    assert_eq!(
        loaded.snapshot_sequence(),
        EventSequence::ZERO,
        "no snapshot was written, so the loaded state does not claim one"
    );
    assert!(inner.snapshot(&"acct-1".into()).await.unwrap().is_none());

    let captured = capture.0.lock().unwrap();
    let load = captured
        .spans
        .iter()
        .position(|(name, _)| *name == "load")
        .expect("a load span");
    let warnings: Vec<_> = captured
        .events
        .iter()
        .filter(|(level, _, _)| *level == Level::WARN)
        .collect();
    assert_eq!(warnings.len(), 1, "one warning: {warnings:?}");
    let (_, parent, fields) = warnings[0];
    assert_eq!(*parent, Some(load), "the warning is inside the load span");
    assert_eq!(fields.get("phase").map(String::as_str), Some("write"));
    assert_eq!(
        fields.get("message").map(String::as_str),
        Some("snapshot catch-up failed")
    );
}

#[tokio::test]
async fn catch_up_is_off_by_default() {
    let repository = MemRepository::new();
    let store = Store::<Ledger, _>::new(&repository, ()).with_snapshot_interval(0);
    assert_eq!(store.catch_up_snapshot_threshold(), 0);
    for amount in 1..=10 {
        store
            .execute("acct-1", Add(amount), Metadata::default())
            .await
            .unwrap();
    }

    store.load("acct-1").await.unwrap();

    assert!(
        repository
            .snapshot(&"acct-1".into())
            .await
            .unwrap()
            .is_none()
    );
}

/// A [`Ledger`] that never snapshots, whatever the store says.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
struct NeverSnapshots(Ledger);

impl Aggregate for NeverSnapshots {
    const TYPE: &'static str = Ledger::TYPE;
    const SNAPSHOT_INTERVAL: Option<usize> = Some(0);

    type Command = Add;
    type Event = LedgerEvent;
    type Error = InvalidAmount;
    type Services = ();

    async fn handle(&self, command: Add, services: &()) -> Result<Vec<LedgerEvent>, InvalidAmount> {
        self.0.handle(command, services).await
    }

    fn apply(&mut self, event: LedgerEvent) {
        self.0.apply(event);
    }
}

#[tokio::test]
async fn an_aggregate_interval_of_zero_disables_its_snapshots() {
    let repository = MemRepository::new();
    let store = Store::<NeverSnapshots, _>::new(&repository, ()).with_snapshot_interval(1);
    assert_eq!(store.snapshot_interval(), 0);

    for amount in 1..=3 {
        store
            .execute("acct-1", Add(amount), Metadata::default())
            .await
            .unwrap();
    }

    // Through the store's own transactions too, whose default interval is the store's 1.
    let mut tx = store.transaction();
    let mut read = tx.read::<NeverSnapshots>("acct-1").await.unwrap();
    store
        .append_to(
            &mut tx,
            &mut read,
            vec![crate::conformance::Added { amount: 4 }.into()],
        )
        .unwrap();
    tx.append(
        &mut read,
        vec![crate::conformance::Added { amount: 5 }.into()],
    )
    .unwrap();
    tx.commit(&Metadata::default()).await.unwrap();

    assert!(
        repository
            .snapshot(&"acct-1".into())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(store.load("acct-1").await.unwrap().aggregate().0.total, 15);
}
