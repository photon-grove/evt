//! The snapshot policy cases (P5, P6): a per-aggregate snapshot interval and catch-up snapshots
//! written by loads. They run in [`Suite::run`] on every backend.

use super::*;
use crate::transaction::Transaction;

/// A [`Ledger`] stored under the same type that declares its own snapshot interval of 3, so the
/// store's and transaction's intervals do not apply to it.
#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
struct EveryThird(Ledger);

impl Aggregate for EveryThird {
    const TYPE: &'static str = Ledger::TYPE;
    const SNAPSHOT_INTERVAL: Option<usize> = Some(3);

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

async fn stored_snapshot<R: Repository>(
    repo: &R,
    id: &EntityId,
) -> Result<Option<(EventSequence, EventSequence)>, Failure> {
    Ok(repo
        .snapshot(id)
        .await?
        .map(|snapshot| (snapshot.sequence, snapshot.event_sequence)))
}

/// An aggregate's own interval wins over the store's and the transaction's.
pub(super) async fn aggregate_snapshot_interval<R: Repository>(repo: &R) -> CaseResult {
    // The store says never; the type says every third event.
    let store = Store::<EveryThird, _>::new(repo, ()).with_snapshot_interval(0);
    ensure_eq!(store.snapshot_interval(), 3, "the effective interval");
    let id = ledger_id();

    for amount in 1..=2 {
        store.execute(&id, Add(amount), metadata()).await?;
    }
    ensure_eq!(
        stored_snapshot(repo, &id).await?,
        None,
        "no snapshot before the type's interval"
    );

    let executed = store.execute(&id, Add(3), metadata()).await?;
    ensure_eq!(
        stored_snapshot(repo, &id).await?,
        Some((EventSequence(1), EventSequence(3))),
        "the type's interval snapshots at event 3"
    );
    ensure_eq!(
        executed.state.snapshot_sequence(),
        EventSequence(1),
        "the executed state carries the snapshot"
    );

    // A transaction whose own interval is 0 still snapshots the type at its interval.
    let mut tx = Transaction::new(repo).with_snapshot_interval(0);
    let mut read = tx.read::<EveryThird>(&id).await?;
    tx.append(
        &mut read,
        (4..=6).map(|amount| Added { amount }.into()).collect(),
    )?;
    tx.commit(&metadata()).await?;
    ensure_eq!(
        stored_snapshot(repo, &id).await?,
        Some((EventSequence(2), EventSequence(6))),
        "a transaction append snapshots the type at its interval"
    );

    // A type without its own interval still follows the store's.
    let plain = Store::<Ledger, _>::new(repo, ()).with_snapshot_interval(0);
    ensure_eq!(plain.snapshot_interval(), 0, "the store's interval");
    let other = ledger_id();
    for amount in 1..=3 {
        plain.execute(&other, Add(amount), metadata()).await?;
    }
    ensure_eq!(
        stored_snapshot(repo, &other).await?,
        None,
        "the store's interval 0 never snapshots"
    );

    Ok(())
}

/// A load that replays at least the threshold writes a snapshot of the state it folded, and the
/// loaded state stays in step with it, so a commit from it is not a snapshot conflict.
pub(super) async fn catch_up_snapshots<R: Repository>(repo: &R) -> CaseResult {
    let writer = Store::<Ledger, _>::new(repo, ()).with_snapshot_interval(0);
    let reader = Store::<Ledger, _>::new(repo, ())
        .with_snapshot_interval(0)
        .with_catch_up_snapshot_threshold(3);
    ensure_eq!(reader.catch_up_snapshot_threshold(), 3, "the threshold");
    let id = ledger_id();

    for amount in 1..=2 {
        writer.execute(&id, Add(amount), metadata()).await?;
    }
    let loaded = reader.load(&id).await?;
    ensure_eq!(
        (
            stored_snapshot(repo, &id).await?,
            loaded.snapshot_sequence()
        ),
        (None, EventSequence::ZERO),
        "a load below the threshold writes nothing"
    );

    writer.execute(&id, Add(3), metadata()).await?;
    let loaded = reader.load(&id).await?;
    ensure_eq!(
        stored_snapshot(repo, &id).await?,
        Some((EventSequence(1), EventSequence(3))),
        "a load that replays the threshold catches up from the start"
    );
    ensure_eq!(
        (loaded.sequence(), loaded.snapshot_sequence()),
        (EventSequence(3), EventSequence(1)),
        "the loaded state carries the catch-up snapshot"
    );
    let snapshot = repo
        .snapshot(&id)
        .await?
        .ok_or(Failure("no snapshot".into()))?;
    ensure_eq!(
        serde_json::from_slice::<Ledger>(&snapshot.payload)?,
        Ledger {
            total: 6,
            entries: 3
        },
        "the catch-up snapshot holds the folded state"
    );

    reader.load(&id).await?;
    ensure_eq!(
        stored_snapshot(repo, &id).await?,
        Some((EventSequence(1), EventSequence(3))),
        "a load from the fresh snapshot replays nothing and writes nothing"
    );

    // Three more events past the snapshot: a transaction read of the store catches up too.
    for amount in 4..=6 {
        writer.execute(&id, Add(amount), metadata()).await?;
    }
    let mut tx = reader.transaction();
    let read = tx.read::<Ledger>(&id).await?;
    ensure_eq!(
        stored_snapshot(repo, &id).await?,
        Some((EventSequence(2), EventSequence(6))),
        "a transaction read catches up after the snapshot"
    );
    drop((read, tx));

    // A commit from a caught-up load writes the snapshot after the catch-up one, so the snapshot
    // sequence check (stored plus one) passes.
    writer.execute(&id, Add(7), metadata()).await?;
    writer.execute(&id, Add(8), metadata()).await?;
    writer.execute(&id, Add(9), metadata()).await?;
    let loaded = reader.load(&id).await?;
    ensure_eq!(
        loaded.snapshot_sequence(),
        EventSequence(3),
        "the third catch-up snapshot"
    );
    let every_event = Store::<Ledger, _>::new(repo, ()).with_snapshot_interval(1);
    every_event
        .commit(loaded, vec![Added { amount: 10 }.into()], &metadata())
        .await?;
    ensure_eq!(
        stored_snapshot(repo, &id).await?,
        Some((EventSequence(4), EventSequence(10))),
        "a commit from the caught-up load snapshots next without a conflict"
    );

    Ok(())
}
