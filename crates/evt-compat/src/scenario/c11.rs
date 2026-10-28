//! The ops of the v0.9 core scenarios (ADR 0004, C11): `readLatest` (C8), `enumerate` (C8),
//! `declare` (C9), `executeTransaction` (C5), and `groupCommits` (C6/C7).

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use photon_evt::{
    COMMAND_OUTCOME_PREFIX, CommandId, CommandIdentity, Commit, Decision, DomainEvent, EntityId,
    EntityType, Error, EventLog, EventSequence, EventVersion, FeatureManifest, FeatureMarker,
    FenceProvenance, Metadata, Outcome as StoredOutcome, ReadFence, Repository, RetryPolicy,
    SYSTEM_PREFIX, SerializedEvent, SerializedSnapshot, Store, StreamFilter, Transaction,
    TransactionCommit, group_commits,
};

use super::{
    DEFAULT_TIMESTAMP, ORIGIN_SOURCE, Outcome, REGION, Runner, Step, StepError, TransactionWrite,
    classify, raw_event, scope_id, transaction_id,
};
use crate::account::{Account, AccountEvent, LedgerProjector};

/// A boxed future of a marker access.
pub type MarkerFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;

/// What a `declare` step needs of a backend: its feature marker (C9).
pub trait MarkerAccess: Send + Sync {
    /// The stored manifest's version, 0 when the store has no marker row.
    fn version(&self) -> MarkerFuture<'_, i64>;

    /// Applies `change` to the stored manifest in one declaration transaction.
    fn declare<'a>(
        &'a self,
        change: Box<dyn Fn(&mut FeatureManifest) + Send + Sync + 'a>,
    ) -> MarkerFuture<'a, ()>;
}

impl<M: FeatureMarker> MarkerAccess for M {
    fn version(&self) -> MarkerFuture<'_, i64> {
        Box::pin(async move {
            Ok(self
                .read_manifest()
                .await?
                .map_or(0, |manifest| manifest.version))
        })
    }

    fn declare<'a>(
        &'a self,
        change: Box<dyn Fn(&mut FeatureManifest) + Send + Sync + 'a>,
    ) -> MarkerFuture<'a, ()> {
        Box::pin(self.update_manifest(move |manifest| change(manifest)))
    }
}

fn is_reserved(entity_id: &EntityId) -> bool {
    entity_id.as_str().starts_with(SYSTEM_PREFIX)
        || entity_id.as_str().starts_with(COMMAND_OUTCOME_PREFIX)
}

impl<R: Repository + EventLog> Runner<R> {
    /// Reads the newest `n` events of the step's stream, newest first.
    pub(super) async fn read_latest(
        &self,
        entity_id: &EntityId,
        step: &Step,
    ) -> Result<Outcome, StepError> {
        let id = transaction_id(entity_id, step.suffix.as_deref().unwrap_or(""));
        let events = self.store.repository().latest(&id, step.n).await?;

        Ok(Outcome {
            events: events.len(),
            sequences: events.iter().map(|event| event.sequence.0).collect(),
            ..Outcome::of("ok")
        })
    }

    /// Walks the event log and the entity heads, as a rebuild does, and counts the events of the
    /// scenario's streams. Engine rows are not streams: one of the scenario's that either walk
    /// yields is an error.
    pub(super) async fn enumerate(&self, entity_id: &EntityId) -> Result<Outcome, StepError> {
        let repository = self.store.repository();
        let base = entity_id.as_str();
        let mut count = 0;

        let stream = repository.stream_all_events(StreamFilter::all());
        let mut stream = std::pin::pin!(stream);
        while let Some(batch) =
            std::future::poll_fn(|cx| futures_core::Stream::poll_next(stream.as_mut(), cx)).await
        {
            for event in batch? {
                if !event.entity_id.as_str().contains(base) {
                    continue;
                }
                if is_reserved(&event.entity_id) {
                    return Err(StepError::Unexpected(format!(
                        "enumeration yielded the engine row {}",
                        event.entity_id
                    )));
                }
                count += 1;
            }
        }

        for id in repository.entity_heads(None).await?.keys() {
            if id.as_str().contains(base) && is_reserved(id) {
                return Err(StepError::Unexpected(format!(
                    "entity heads yielded the engine row {id}"
                )));
            }
        }

        Ok(Outcome {
            events: count,
            ..Outcome::of("ok")
        })
    }

    /// Applies one declaration to the store's feature manifest and reports the version bump.
    pub(super) async fn declare(
        &self,
        entity_id: &EntityId,
        step: &Step,
    ) -> Result<Outcome, StepError> {
        let marker = self
            .marker
            .as_ref()
            .ok_or(StepError::Missing("a feature marker"))?;

        let scoped = |types: &[String]| -> Vec<String> {
            types.iter().map(|t| scope_id(t, entity_id)).collect()
        };
        let (retention, history) = (scoped(&step.retention), scoped(&step.history_dependent));
        let features = step.features.clone();
        let retract = step.retract;

        let before = marker.version().await?;
        marker
            .declare(Box::new(move |manifest: &mut FeatureManifest| {
                if retract {
                    for name in features.keys() {
                        manifest.features.remove(name);
                    }
                    for entity_type in &retention {
                        manifest.retention.remove(entity_type);
                    }
                    for entity_type in &history {
                        manifest.history_dependent.remove(entity_type);
                    }
                } else {
                    manifest.features.extend(features.clone());
                    manifest.retention.extend(retention.iter().cloned());
                    manifest.history_dependent.extend(history.iter().cloned());
                }
            }))
            .await?;
        let after = marker.version().await?;

        Ok(Outcome {
            delta: after - before,
            ..Outcome::of("ok")
        })
    }

    /// Runs a multi-stream command under fence-aware retry (C5), as Go's `executeTransaction`
    /// does: a store with outcomes and `attempts` immediate attempts, a decision step that reads
    /// `reads` and every stream it writes, adds the step's fences as caller-stamped ones, and
    /// appends `writes` on the first attempt and `retryWrites` (when set) on the ones after, while
    /// `interfere` moves a stream before the first commits.
    pub(super) async fn execute_transaction(
        &self,
        entity_id: &EntityId,
        step: &Step,
    ) -> Result<Outcome, StepError> {
        let outcomes = self
            .outcomes
            .as_ref()
            .ok_or(StepError::Missing("an outcomes scenario"))?;
        let identity = step
            .identity
            .as_ref()
            .ok_or(StepError::Missing("identity"))?;

        let mut command =
            CommandIdentity::new(&identity.command_type).with_targets([entity_id.clone()]);
        if let Some(payload) = &identity.payload {
            command = command.with_payload(payload)?;
        }

        let interfering = Interfering {
            inner: self.store.repository(),
            base: entity_id.clone(),
            step,
            moved: AtomicUsize::new(0),
            lock: Mutex::new(()),
        };
        let config = outcomes
            .config
            .clone()
            .with_retry(RetryPolicy::immediate(step.attempts.max(1)));
        let mut store = Store::<Account, _>::new(&interfering, ())
            .with_snapshot_interval(self.snapshot_interval)
            .with_outcomes(config);
        if self.projector {
            store = store.with_commit_projector(std::sync::Arc::new(LedgerProjector));
        }

        let decision = StepCommand {
            base: entity_id,
            step,
            prepares: AtomicUsize::new(0),
        };
        let result = store
            .execute_transaction(&command, &self.step_metadata(step), |tx| {
                Box::pin(decision.prepare(tx))
            })
            .await;

        let mut outcome = classify(result)?;
        outcome.prepares = decision.prepares.load(Ordering::SeqCst);

        Ok(outcome)
    }

    /// Reads every stream the scenario writes and regroups their events into the commits that
    /// wrote them, as a rebuild of a history-dependent projector does.
    pub(super) async fn group_commits(
        &self,
        entity_id: &EntityId,
        step: &Step,
    ) -> Result<Outcome, StepError> {
        let repository = self.store.repository();
        let mut events = Vec::new();
        for suffix in std::iter::once("").chain(step.reads.iter().map(String::as_str)) {
            events.extend(
                repository
                    .events(&transaction_id(entity_id, suffix))
                    .await?,
            );
        }
        let count = events.len();

        let groups = group_commits(events)?
            .iter()
            .map(|commit| {
                let id = if commit.commit_id.is_empty() {
                    "-"
                } else {
                    commit.commit_id.as_str()
                };
                let members: Vec<String> = commit
                    .streams
                    .iter()
                    .flat_map(|stream| {
                        let suffix = stream
                            .entity_id
                            .as_str()
                            .strip_prefix(entity_id.as_str())
                            .unwrap_or(stream.entity_id.as_str());
                        let suffix = suffix.strip_prefix('-').unwrap_or(suffix).to_owned();

                        stream
                            .events
                            .iter()
                            .map(move |event| format!("{suffix}:{}", event.sequence.0))
                    })
                    .collect();

                format!("{id} {}", members.join(" "))
            })
            .collect();

        Ok(Outcome {
            events: count,
            groups,
            ..Outcome::of("ok")
        })
    }
}

/// An `executeTransaction` step's decision: read, fence, append. It counts its runs.
struct StepCommand<'a> {
    base: &'a EntityId,
    step: &'a Step,
    prepares: AtomicUsize,
}

impl StepCommand<'_> {
    async fn prepare<R: Repository>(&self, tx: &mut Transaction<'_, R>) -> Result<Decision, Error> {
        let attempt = self.prepares.fetch_add(1, Ordering::SeqCst) + 1;
        let writes: &[TransactionWrite] = if attempt > 1 && !self.step.retry_writes.is_empty() {
            &self.step.retry_writes
        } else {
            &self.step.writes
        };

        let mut read = BTreeMap::new();
        for suffix in &self.step.reads {
            let id = transaction_id(self.base, suffix);
            if let Entry::Vacant(slot) = read.entry(id.clone()) {
                slot.insert(tx.read::<Account>(id).await?);
            }
        }

        for fence in &self.step.fences {
            tx.fence(ReadFence::new(
                transaction_id(self.base, &fence.suffix),
                fence.entity_type(self.base),
                EventSequence(fence.head),
                FenceProvenance::Caller,
            ))?;
        }

        for write in writes {
            let id = transaction_id(self.base, &write.suffix);
            let handle = match read.entry(id.clone()) {
                Entry::Occupied(slot) => slot.into_mut(),
                Entry::Vacant(slot) => slot.insert(tx.read::<Account>(id).await?),
            };

            let event = AccountEvent::from_payload(
                &write.event.event_type,
                EventVersion(write.event.version),
                &write.event.payload(),
            )
            .map_err(|err| Error::InvalidRecord(err.to_string()))?;
            tx.append(handle, vec![event])?;
        }

        Ok(Decision::accept())
    }
}

/// Another writer: before each of an `executeTransaction` step's first `interfere.times` commits
/// that carry an outcome, it appends the interference event to its stream at the stream's next
/// sequence, under a commit ID of its own (as Go's harness does).
struct Interfering<'a, R: Repository> {
    inner: &'a R,
    base: EntityId,
    step: &'a Step,
    moved: AtomicUsize,
    lock: Mutex<()>,
}

impl<R: Repository> Interfering<'_, R> {
    async fn interfere(&self) -> Result<(), Error> {
        let Some(interference) = &self.step.interfere else {
            return Ok(());
        };
        let moved = {
            let _guard = self
                .lock
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let moved = self.moved.load(Ordering::SeqCst);
            if moved >= interference.times {
                return Ok(());
            }
            self.moved.store(moved + 1, Ordering::SeqCst);
            moved + 1
        };

        let id = transaction_id(&self.base, &interference.suffix);
        let current = Store::<Account, _>::new(self.inner, ())
            .load(&id)
            .await?
            .sequence();

        let mut metadata = Metadata::default()
            .with_origin(ORIGIN_SOURCE, &self.step.by)
            .with_address(String::new())
            .with_timestamp(DEFAULT_TIMESTAMP);
        metadata.region = REGION.to_owned();
        metadata.commit_id = format!(
            "compat-{}-{}-interfere-{moved}",
            self.step.by,
            self.step.command_id.as_deref().unwrap_or_default()
        );

        let event = raw_event(&id, current.next(), &interference.event, &metadata);

        self.inner.commit(Commit::new(id, vec![event])).await
    }
}

impl<R: Repository> Repository for Interfering<'_, R> {
    type Operation = R::Operation;

    fn writes_views(&self) -> bool {
        self.inner.writes_views()
    }

    fn applies_expected_payload(&self) -> bool {
        self.inner.applies_expected_payload()
    }

    fn declare_history_dependent(
        &self,
        types: &[EntityType],
    ) -> impl Future<Output = Result<(), Error>> + Send {
        self.inner.declare_history_dependent(types)
    }

    async fn commit(&self, commit: Commit<R::Operation>) -> Result<(), Error> {
        if commit.outcome.is_some() {
            self.interfere().await?;
        }

        self.inner.commit(commit).await
    }

    async fn commit_transaction(
        &self,
        commit: TransactionCommit<R::Operation>,
    ) -> Result<(), Error> {
        if commit.outcome.is_some() {
            self.interfere().await?;
        }

        self.inner.commit_transaction(commit).await
    }

    fn backoff(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        self.inner.backoff(duration)
    }

    fn deadline(&self, duration: Duration) -> impl Future<Output = ()> + Send {
        self.inner.deadline(duration)
    }

    async fn outcome(&self, command_id: &CommandId) -> Result<Option<StoredOutcome>, Error> {
        self.inner.outcome(command_id).await
    }

    async fn events(&self, entity_id: &EntityId) -> Result<Vec<SerializedEvent>, Error> {
        self.inner.events(entity_id).await
    }

    async fn events_after(
        &self,
        entity_id: &EntityId,
        after: EventSequence,
    ) -> Result<Vec<SerializedEvent>, Error> {
        self.inner.events_after(entity_id, after).await
    }

    async fn latest(&self, entity_id: &EntityId, n: usize) -> Result<Vec<SerializedEvent>, Error> {
        self.inner.latest(entity_id, n).await
    }

    async fn snapshot(&self, entity_id: &EntityId) -> Result<Option<SerializedSnapshot>, Error> {
        self.inner.snapshot(entity_id).await
    }

    async fn put_snapshot(&self, snapshot: &SerializedSnapshot) -> Result<bool, Error> {
        self.inner.put_snapshot(snapshot).await
    }

    async fn compact_below(
        &self,
        entity_id: &EntityId,
        through: EventSequence,
    ) -> Result<usize, Error> {
        self.inner.compact_below(entity_id, through).await
    }

    async fn entity_heads(
        &self,
        entity_type: Option<&EntityType>,
    ) -> Result<BTreeMap<EntityId, EventSequence>, Error> {
        self.inner.entity_heads(entity_type).await
    }
}
