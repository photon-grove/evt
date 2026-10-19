//! Shared scenarios (`compat/scenarios.json`), their golden expectations
//! (`compat/fixtures/scenarios.json`), and a runner that executes steps against any repository.

use std::collections::BTreeMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use photon_evt::conformance::{FaultInjectingRepository, HeadFixtures};
use photon_evt::wire::{dynamo, envelope};
use photon_evt::{
    Begin, CommandIdentity, CommandOutcome, Commit, DigestInput, EntityId, EntityType, Error,
    EventId, EventSequence, EventType, EventVersion, ExecuteError, FenceProvenance, Metadata,
    NewSnapshot, OutcomeConfig, ReadFence, Repository, RetryPolicy, SerializedEvent,
    SerializedSnapshot, Store, TransactionCommit, default_digest, json,
};
use serde::{Deserialize, Serialize};

use crate::account::{Account, AccountCommand, AccountEvent, balance_response};

/// The writer name of this implementation.
pub const WRITER: &str = "rust";
/// The metadata origin source every step uses.
pub const ORIGIN_SOURCE: &str = "evt-compat";
/// The metadata region every step uses.
pub const REGION: &str = "us-west-2";
/// The metadata timestamp of steps that do not set one.
pub const DEFAULT_TIMESTAMP: &str = "2026-10-12T00:00:00Z";
/// The run name the golden fixtures were generated under.
pub const FIXTURE_RUN: &str = "fixture";

/// Returns the repository's `compat/` directory.
pub fn compat_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../compat")
}

#[derive(Clone, Debug, Deserialize)]
pub struct Scenarios {
    pub scenarios: Vec<Scenario>,
}

impl Scenarios {
    /// Reads `compat/scenarios.json`.
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let data = std::fs::read(compat_dir().join("scenarios.json"))?;

        Ok(serde_json::from_slice(&data)?)
    }

    /// The largest number of steps in any scenario.
    pub fn max_steps(&self) -> usize {
        self.scenarios
            .iter()
            .map(|s| s.steps.len())
            .max()
            .unwrap_or_default()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Scenario {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub snapshot_interval: usize,
    /// Runs the scenario on stores with command outcomes enabled (C3): every step carries a
    /// command ID, and execute steps record and replay outcomes.
    #[serde(default)]
    pub outcomes: bool,
    pub steps: Vec<Step>,
}

impl Scenario {
    /// The stream ID of this scenario within a run.
    pub fn entity_id(&self, run: &str) -> EntityId {
        EntityId::new(format!("compat-{run}-{}", self.name))
    }

    /// Every non-empty stream suffix the scenario writes, fences, appends to, or compacts, in
    /// ascending order: each is a stream beside the primary one that the fixtures and both
    /// languages' verification observe.
    pub fn sibling_suffixes(&self) -> Vec<String> {
        let mut suffixes = std::collections::BTreeSet::new();

        for step in &self.steps {
            suffixes.extend(step.suffix.clone());
            suffixes.extend(step.streams.iter().map(|stream| stream.suffix.clone()));
            suffixes.extend(step.fences.iter().map(|fence| fence.suffix.clone()));
        }
        suffixes.remove("");

        suffixes.into_iter().collect()
    }

    /// The stream IDs of [`Scenario::sibling_suffixes`] within a run.
    pub fn sibling_ids(&self, run: &str) -> Vec<EntityId> {
        let entity_id = self.entity_id(run);

        self.sibling_suffixes()
            .iter()
            .map(|suffix| transaction_id(&entity_id, suffix))
            .collect()
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Step {
    pub by: String,
    pub op: String,
    #[serde(default)]
    pub command_id: Option<String>,
    #[serde(default)]
    pub principal_id: Option<String>,
    #[serde(default)]
    pub causation_id: Option<String>,
    #[serde(default)]
    pub extensions: BTreeMap<String, String>,
    #[serde(default)]
    pub command: Option<AccountCommand>,
    #[serde(default)]
    pub event: Option<RawEvent>,
    #[serde(default)]
    pub through: i64,
    #[serde(default)]
    pub sequence: i64,
    #[serde(default)]
    pub trace: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub timestamp: Option<String>,
    pub expect: Outcome,
    #[serde(default)]
    pub streams: Vec<TransactionStream>,
    #[serde(default)]
    pub fences: Vec<TransactionFence>,
    #[serde(default)]
    pub suffix: Option<String>,
    /// An explicit commit ID for steps without a command, pinning what the engine would
    /// otherwise generate unpredictably so fixtures stay byte-for-byte reproducible.
    #[serde(default)]
    pub commit_id: Option<String>,
    /// Makes the step's commit fail indeterminately: `applyThenFail` (the commit applies, the
    /// caller sees an error) or `failWithoutApplying` (execute steps of outcomes scenarios).
    #[serde(default)]
    pub inject_commit_fault: Option<String>,
    /// The command identity of an `outcomeOnly` step.
    #[serde(default)]
    pub identity: Option<Identity>,
    /// The response an `outcomeOnly` step stores.
    #[serde(default)]
    pub response: Option<String>,
    /// The snapshot a `putSnapshot` step writes outside a commit.
    #[serde(default)]
    pub snapshot: Option<TransactionSnapshot>,
    /// The event sequence a `putSnapshot` step's snapshot covers. Present even when 0.
    #[serde(default)]
    pub event_sequence: Option<i64>,
}

/// The command identity of an `outcomeOnly` step: its type string and payload object.
#[derive(Clone, Debug, Deserialize)]
pub struct Identity {
    #[serde(rename = "type")]
    pub command_type: String,
    #[serde(default)]
    pub payload: Option<serde_json::Value>,
}

impl Step {
    /// The step's metadata. Every field is determined by the step, so both languages write the
    /// same bytes for it.
    pub fn metadata(&self) -> Metadata {
        let mut metadata = Metadata::default()
            .with_origin(ORIGIN_SOURCE, &self.by)
            .with_address(self.address.clone().unwrap_or_default())
            .with_timestamp(
                self.timestamp
                    .clone()
                    .filter(|timestamp| !timestamp.is_empty())
                    .unwrap_or_else(|| DEFAULT_TIMESTAMP.to_owned()),
            );

        metadata.command_id = self
            .command_id
            .clone()
            .filter(|id| !id.is_empty())
            .map(Into::into);
        // Mirror Go's StepMetadata: the commit ID follows the command ID, or an explicit one
        // pinned for steps without a command (a raw commit writes it as-is; the store derives it
        // from the command ID either way).
        metadata.commit_id = match &self.command_id {
            Some(command_id) if !command_id.is_empty() => {
                format!("compat-{}-{command_id}", self.by)
            }
            _ => self.commit_id.clone().unwrap_or_default(),
        };
        metadata.principal_id = self
            .principal_id
            .clone()
            .filter(|id| !id.is_empty())
            .map(Into::into);
        metadata.causation_id = self
            .causation_id
            .clone()
            .filter(|id| !id.is_empty())
            .map(Into::into);
        metadata.extensions = self.extensions.clone();
        metadata.trace = self.trace.clone();
        metadata.region = REGION.to_owned();

        metadata
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionStream {
    #[serde(default)]
    pub suffix: String,
    pub sequence: i64,
    pub events: Vec<RawEvent>,
    #[serde(default)]
    pub snapshot: Option<TransactionSnapshot>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionSnapshot {
    pub sequence: i64,
    pub payload: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionFence {
    #[serde(default)]
    pub suffix: String,
    pub head: i64,
    #[serde(default)]
    pub caller_stamped: bool,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RawEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub version: i64,
    pub payload: String,
}

/// The observable result of a step.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Outcome {
    pub result: String,
    #[serde(default)]
    pub events: usize,
    #[serde(default)]
    pub deleted: usize,
    /// The rejection code of a rejected outcome.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub code: String,
    /// The outcome was read back from a stored row rather than produced by the step.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub replayed: bool,
}

impl Outcome {
    fn of(result: &str) -> Self {
        Self {
            result: result.to_owned(),
            ..Self::default()
        }
    }

    /// Classifies the result of a command on an outcomes store.
    fn of_command<E>(command: &CommandOutcome<E>) -> Self {
        let outcome = &command.outcome;

        match &outcome.rejection {
            Some(rejection) => Self {
                code: rejection.code.to_string(),
                events: outcome.event_ids.len(),
                replayed: command.replayed,
                ..Self::of("rejected")
            },
            None => Self {
                events: outcome.event_ids.len(),
                replayed: command.replayed,
                ..Self::of("ok")
            },
        }
    }
}

/// Golden expectations for every scenario, generated by the Go implementation.
#[derive(Clone, Debug, Deserialize)]
pub struct ScenarioFixtures {
    pub run: String,
    pub source: String,
    pub scenarios: Vec<Expectation>,
}

impl ScenarioFixtures {
    /// Reads `compat/fixtures/scenarios.json`.
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let data = std::fs::read(compat_dir().join("fixtures/scenarios.json"))?;

        Ok(serde_json::from_slice(&data)?)
    }

    /// Returns the expectation for a scenario, rewritten for a run.
    pub fn for_run(&self, name: &str, run: &str) -> Option<Expectation> {
        let expectation = self.scenarios.iter().find(|e| e.name == name)?;
        let encoded = serde_json::to_string(expectation).ok()?;
        let rewritten =
            encoded.replace(&format!("compat-{FIXTURE_RUN}-"), &format!("compat-{run}-"));

        serde_json::from_str(&rewritten).ok()
    }
}

/// The stored and loaded state of a stream.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Expectation {
    pub name: String,
    pub entity_id: String,
    pub sequence: i64,
    pub snapshot_sequence: i64,
    pub state: String,
    pub events: Vec<String>,
    pub snapshot: Option<String>,
    /// The stream's head (its `sk = -1` item, or its PostgreSQL heads row), absent when it has none.
    #[serde(default)]
    pub head: Option<i64>,
    #[serde(default, deserialize_with = "null_to_empty")]
    pub dynamo_items: Vec<serde_json::Value>,
    #[serde(default, deserialize_with = "null_to_empty")]
    pub sns_messages: Vec<String>,
    #[serde(default, deserialize_with = "null_to_empty")]
    pub siblings: Vec<SiblingExpectation>,
    #[serde(default, deserialize_with = "null_to_empty")]
    pub transactions: Vec<TransactionExpectation>,
    /// The command-outcome rows an outcomes scenario leaves behind, ordered by command ID.
    #[serde(default, deserialize_with = "null_to_empty")]
    pub outcomes: Vec<OutcomeExpectation>,
}

/// One stored command-outcome row, as its DynamoDB item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutcomeExpectation {
    pub command_id: String,
    pub dynamo_item: serde_json::Value,
}

/// The ordered `TransactWriteItems` actions one step sent, as the Go implementation recorded
/// them: `actions` is the one request of a transaction step, `calls` every request of a step of an
/// outcomes scenario, in order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TransactionExpectation {
    pub step: usize,
    #[serde(default, deserialize_with = "null_to_empty")]
    pub actions: Vec<serde_json::Value>,
    #[serde(default, deserialize_with = "null_to_empty")]
    pub calls: Vec<Vec<serde_json::Value>>,
}

/// The stored state of one of a scenario's sibling streams (transactional siblings, fenced
/// streams).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SiblingExpectation {
    pub entity_id: String,
    pub events: Vec<String>,
    pub snapshot: Option<String>,
    /// The stream's head, absent when it has none.
    #[serde(default)]
    pub head: Option<i64>,
    #[serde(default, deserialize_with = "null_to_empty")]
    pub dynamo_items: Vec<serde_json::Value>,
}

/// Decodes a Go-emitted sequence the way Go means it: a missing key or an explicit `null` (a nil
/// slice) is empty, so fixtures round-trip without Go having to distinguish nil from empty.
fn null_to_empty<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}

/// Unexpected failures while running a step.
#[derive(Debug, thiserror::Error)]
pub enum StepError {
    #[error("step has no {0}")]
    Missing(&'static str),
    #[error("unknown op {0}")]
    UnknownOp(String),
    #[error(transparent)]
    Store(#[from] Error),
    #[error("unexpected outcome: {0}")]
    Unexpected(String),
}

/// The settings of a scenario that runs on a store with command outcomes.
struct OutcomeRun {
    config: OutcomeConfig,
    run: String,
    scenario: String,
}

/// Executes steps against one repository through the Rust store.
pub struct Runner<R: Repository> {
    store: Store<Account, R>,
    snapshot_interval: usize,
    outcomes: Option<OutcomeRun>,
    heads: Option<Arc<dyn HeadAccess>>,
}

/// A boxed future of a head access.
pub type HeadFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, Error>> + Send + 'a>>;

/// Raw access to a backend's stream heads (ADR 0007): the `appendRawBypassingHead` step writes an
/// event without its head update, and `observe` reads the stored head back.
pub trait HeadAccess: Send + Sync {
    /// Stores the event without touching its stream's head.
    fn append_bypassing_head<'a>(&'a self, event: &'a SerializedEvent) -> HeadFuture<'a, ()>;

    /// The stream's stored head, `None` when it has no head item or row.
    fn stored_head<'a>(&'a self, entity_id: &'a EntityId) -> HeadFuture<'a, Option<EventSequence>>;
}

impl HeadAccess for photon_evt::MemRepository {
    fn append_bypassing_head<'a>(&'a self, event: &'a SerializedEvent) -> HeadFuture<'a, ()> {
        Box::pin(HeadFixtures::<photon_evt::MemRepository>::append_bypassing_head(self, event))
    }

    fn stored_head<'a>(&'a self, entity_id: &'a EntityId) -> HeadFuture<'a, Option<EventSequence>> {
        Box::pin(HeadFixtures::<photon_evt::MemRepository>::stored_head(
            self, entity_id,
        ))
    }
}

impl<R: Repository> Runner<R> {
    pub fn new(repository: R, snapshot_interval: usize) -> Self {
        Self {
            store: Store::new(repository, ()).with_snapshot_interval(snapshot_interval),
            snapshot_interval,
            outcomes: None,
            heads: None,
        }
    }

    /// Gives the runner raw access to the backend's stream heads, which the
    /// `appendRawBypassingHead` step and the `head` observations need.
    #[must_use]
    pub fn with_head_access(mut self, heads: impl HeadAccess + 'static) -> Self {
        self.heads = Some(Arc::new(heads));

        self
    }

    /// A runner for a scenario in a run: a store with command outcomes for an outcomes scenario,
    /// a plain one otherwise. The outcomes store's identity digest replaces the run name in each
    /// target stream ID with the fixture run's, so a command digests the same in every run and
    /// the golden fixtures' digests hold against live backends. Command IDs are scoped to the run
    /// and the scenario, because outcome rows are global to the table.
    pub fn for_scenario(repository: R, scenario: &Scenario, run: &str) -> Self {
        let mut runner = Self::new(repository, scenario.snapshot_interval);

        if scenario.outcomes {
            let (from, to) = (format!("compat-{run}-"), format!("compat-{FIXTURE_RUN}-"));
            // A short recovery window keeps the dropped-commit step quick; it still ends in the retryable
            // ambiguous result because the row never appears. One attempt per step: a scenario step
            // pins what a single attempt returns, so the engine must not run the command again on
            // its own (fence-aware retry has its own conformance cases).
            let config = OutcomeConfig::new()
                .with_recovery_wait(std::time::Duration::from_millis(100))
                .with_retry(RetryPolicy::immediate(1))
                .with_digester(move |input: &DigestInput| {
                    let mut input = input.clone();
                    for target in &mut input.targets {
                        *target = EntityId::new(target.as_str().replacen(&from, &to, 1));
                    }

                    default_digest(&input)
                });

            runner.store = runner.store.with_outcomes(config.clone());
            runner.outcomes = Some(OutcomeRun {
                config,
                run: run.to_owned(),
                scenario: scenario.name.clone(),
            });
        }

        runner
    }

    pub fn store(&self) -> &Store<Account, R> {
        &self.store
    }

    /// The step's metadata, with the command ID scoped on an outcomes runner.
    fn step_metadata(&self, step: &Step) -> Metadata {
        let mut metadata = step.metadata();

        if let (Some(outcomes), Some(command_id)) = (&self.outcomes, &step.command_id)
            && !command_id.is_empty()
        {
            metadata.command_id =
                Some(scoped_command_id(&outcomes.run, &outcomes.scenario, command_id).into());
        }

        metadata
    }

    /// Runs one step and classifies its outcome.
    pub async fn execute(&self, entity_id: &EntityId, step: &Step) -> Result<Outcome, StepError> {
        match step.op.as_str() {
            "execute" => self.execute_command(entity_id, step).await,
            "appendRaw" => {
                let raw = step.event.as_ref().ok_or(StepError::Missing("event"))?;
                let id = transaction_id(entity_id, step.suffix.as_deref().unwrap_or(""));
                let sequence = self.store.load(&id).await?.sequence().next();
                self.commit_raw(&id, sequence, raw, step).await
            }
            "conflictProbe" => {
                let probe = RawEvent {
                    event_type: "account:deposited".to_owned(),
                    version: 1,
                    payload: r#"{"amountCents":1,"memo":null}"#.to_owned(),
                };
                self.commit_raw(entity_id, EventSequence(step.sequence), &probe, step)
                    .await
            }
            "appendRawBypassingHead" => self.append_bypassing_head(entity_id, step).await,
            "putSnapshot" => self.put_snapshot(entity_id, step).await,
            "transaction" => self.transaction(entity_id, step).await,
            "outcomeOnly" => self.outcome_only(step).await,
            "resolveOutcome" => self.resolve_outcome(entity_id, step).await,
            "compact" => {
                let id = transaction_id(entity_id, step.suffix.as_deref().unwrap_or(""));
                match self
                    .store
                    .repository()
                    .compact_below(&id, EventSequence(step.through))
                    .await
                {
                    Ok(deleted) => Ok(Outcome {
                        deleted,
                        ..Outcome::of("compacted")
                    }),
                    Err(Error::CompactionUncovered { .. }) => Ok(Outcome::of("uncovered")),
                    Err(err) => Err(err.into()),
                }
            }
            other => Err(StepError::UnknownOp(other.to_owned())),
        }
    }

    /// Writes the stream's next event straight to the backend, as a writer that does not
    /// maintain the stream's head item would: the head is left one behind the log.
    async fn append_bypassing_head(
        &self,
        entity_id: &EntityId,
        step: &Step,
    ) -> Result<Outcome, StepError> {
        let raw = step.event.as_ref().ok_or(StepError::Missing("event"))?;
        let heads = self
            .heads
            .as_ref()
            .ok_or(StepError::Missing("a way to bypass the head item"))?;
        let id = transaction_id(entity_id, step.suffix.as_deref().unwrap_or(""));
        let sequence = self.store.load(&id).await?.sequence().next();
        let event = raw_event(&id, sequence, raw, &step.metadata());

        heads.append_bypassing_head(&event).await?;

        Ok(Outcome {
            events: 1,
            ..Outcome::of("ok")
        })
    }

    /// Writes a snapshot outside a commit. A snapshot ahead of the stream's head is drift; one
    /// that regresses is a silent no-op, and both a write and a no-op are `ok`.
    async fn put_snapshot(&self, entity_id: &EntityId, step: &Step) -> Result<Outcome, StepError> {
        let snapshot = step
            .snapshot
            .as_ref()
            .ok_or(StepError::Missing("snapshot"))?;
        let event_sequence = step
            .event_sequence
            .ok_or(StepError::Missing("eventSequence"))?;

        let result = self
            .store
            .repository()
            .put_snapshot(&SerializedSnapshot {
                entity_type: EntityType::new("account"),
                entity_id: transaction_id(entity_id, step.suffix.as_deref().unwrap_or("")),
                sequence: EventSequence(snapshot.sequence),
                event_sequence: EventSequence(event_sequence),
                payload: snapshot.payload.as_bytes().to_vec(),
            })
            .await;

        match result {
            Ok(_) => Ok(Outcome::of("ok")),
            Err(Error::StreamHeadDrift { .. }) => Ok(Outcome::of("drift")),
            Err(error) => Err(error.into()),
        }
    }

    /// Runs one transaction step: appends to several streams and fences read-only streams in
    /// one atomic commit.
    async fn transaction(&self, entity_id: &EntityId, step: &Step) -> Result<Outcome, StepError> {
        let metadata = step.metadata();

        let mut streams = Vec::new();
        for stream in &step.streams {
            let id = transaction_id(entity_id, &stream.suffix);
            let mut events = Vec::new();
            for (index, raw) in stream.events.iter().enumerate() {
                let sequence = EventSequence(stream.sequence + index as i64);
                let event = raw_event(&id, sequence, raw, &metadata);
                events.push(event);
            }
            let mut commit = Commit::new(id, events);
            if let Some(snapshot) = &stream.snapshot {
                commit = commit.with_snapshot(NewSnapshot::new(
                    "account",
                    EventSequence(snapshot.sequence),
                    snapshot.payload.as_bytes(),
                ));
            }
            streams.push(commit);
        }

        let mut commit = TransactionCommit::new(streams);
        commit.fences = step
            .fences
            .iter()
            .map(|fence| ReadFence {
                entity_id: transaction_id(entity_id, &fence.suffix),
                entity_type: EntityType::new("account"),
                head: EventSequence(fence.head),
                provenance: if fence.caller_stamped {
                    FenceProvenance::Caller
                } else {
                    FenceProvenance::Engine
                },
            })
            .collect();

        if let Err(error) = commit.normalize() {
            return Err(error.into());
        }

        // Every stream the transaction touches: a failed transaction must leave all of them
        // unchanged, not just one watched sibling.
        let touched: Vec<EntityId> = step
            .streams
            .iter()
            .map(|stream| transaction_id(entity_id, &stream.suffix))
            .chain(
                step.fences
                    .iter()
                    .map(|fence| transaction_id(entity_id, &fence.suffix)),
            )
            .collect();

        let before = self.observe_stored(&touched).await?;
        match self.store.repository().commit_transaction(commit).await {
            Ok(()) => Ok(Outcome {
                events: step.streams.iter().map(|stream| stream.events.len()).sum(),
                ..Outcome::of("ok")
            }),
            Err(error) => {
                let after = self.observe_stored(&touched).await?;
                if before != after {
                    return Err(StepError::Unexpected(format!(
                        "failed transaction left partial events: {error}"
                    )));
                }
                if matches!(error, Error::StreamHeadDrift { .. }) {
                    return Ok(Outcome::of("drift"));
                }
                if error.is_conflict() {
                    return Ok(Outcome::of("conflict"));
                }
                Err(error.into())
            }
        }
    }

    /// Reads the stored events and snapshot of each stream, so a failed transaction can be
    /// checked to have left every touched stream unchanged.
    async fn observe_stored(&self, entity_ids: &[EntityId]) -> Result<String, StepError> {
        let repository = self.store.repository();
        let mut states = Vec::new();

        for entity_id in entity_ids {
            let events = repository.events(entity_id).await?;
            let snapshot = repository.snapshot(entity_id).await?;
            states.push((events, snapshot));
        }

        serde_json::to_string(&states).map_err(|error| StepError::Unexpected(error.to_string()))
    }

    async fn execute_command(
        &self,
        entity_id: &EntityId,
        step: &Step,
    ) -> Result<Outcome, StepError> {
        if let Some(outcomes) = &self.outcomes {
            return self.execute_with_outcome(outcomes, entity_id, step).await;
        }

        let command = step.command.clone().ok_or(StepError::Missing("command"))?;
        let before = self.store.load(entity_id).await?.sequence();

        match self
            .store
            .execute(entity_id, command, step.metadata())
            .await
        {
            Ok(executed) => Ok(Outcome {
                events: (executed.state.sequence().0 - before.0) as usize,
                ..Outcome::of("ok")
            }),
            Err(ExecuteError::Rejected(_)) => Ok(Outcome::of("rejected")),
            Err(err) if err.is_duplicate_command() => Ok(Outcome::of("duplicate")),
            Err(err) if err.is_conflict() => Ok(Outcome::of("conflict")),
            Err(ExecuteError::Store(Error::StreamHeadDrift { .. })) => Ok(Outcome::of("drift")),
            Err(ExecuteError::Store(err)) => Err(err.into()),
            Err(other) => Err(StepError::Unexpected(other.to_string())),
        }
    }

    /// Executes a command on the outcomes store. An injected commit fault runs it over a
    /// decorated repository that applies or drops the commit and reports an indeterminate error.
    async fn execute_with_outcome(
        &self,
        outcomes: &OutcomeRun,
        entity_id: &EntityId,
        step: &Step,
    ) -> Result<Outcome, StepError> {
        let metadata = self.step_metadata(step);

        let result = match step.inject_commit_fault.as_deref() {
            None => execute_on(&self.store, entity_id, step, metadata).await?,
            Some(fault_kind) => {
                let fault = FaultInjectingRepository::new(self.store.repository());
                match fault_kind {
                    "applyThenFail" => fault.arm_after_apply(),
                    "failWithoutApplying" => fault.arm_before_apply(),
                    other => {
                        return Err(StepError::Unexpected(format!(
                            "unknown injectCommitFault {other:?}"
                        )));
                    }
                }

                let store = Store::<Account, _>::new(&fault, ())
                    .with_snapshot_interval(self.snapshot_interval)
                    .with_outcomes(outcomes.config.clone());

                execute_on(&store, entity_id, step, metadata).await?
            }
        };

        classify(result)
    }

    /// Records a command that writes no events: an accepted outcome carrying the step's response,
    /// from an outcome-only transaction that names no streams.
    async fn outcome_only(&self, step: &Step) -> Result<Outcome, StepError> {
        let identity = step
            .identity
            .as_ref()
            .ok_or(StepError::Missing("identity"))?;
        let mut command = CommandIdentity::new(&identity.command_type);
        if let Some(payload) = &identity.payload {
            command = command.with_payload(payload)?;
        }

        let metadata = self.step_metadata(step);
        let mut tx = self.store.transaction();

        let result = match tx.begin_command(&command, &metadata).await {
            Ok(Begin::Replayed(replayed)) => Ok(*replayed),
            Ok(Begin::Proceed) => {
                let response = step
                    .response
                    .as_ref()
                    .filter(|response| !response.is_empty())
                    .map(|response| response.clone().into_bytes());

                tx.commit_with_outcome(response).await
            }
            Err(error) => Err(error),
        };

        classify(result)
    }

    /// Re-reads the outcome row for the step's command, as ambiguous-write recovery does,
    /// without running or writing anything.
    async fn resolve_outcome(
        &self,
        entity_id: &EntityId,
        step: &Step,
    ) -> Result<Outcome, StepError> {
        let command = step.command.clone().ok_or(StepError::Missing("command"))?;

        match self
            .store
            .resolve_outcome(entity_id, &command, &self.step_metadata(step))
            .await
        {
            Ok(None) => Ok(Outcome::of("absent")),
            Ok(Some(resolved)) => Ok(Outcome::of_command(&resolved)),
            Err(error) => classify::<AccountEvent>(Err(error)),
        }
    }

    /// Reads the outcome row of every command ID a scenario uses in `run`, ordered by ID, as the
    /// DynamoDB items this implementation encodes for them. A command that left no row (a
    /// rejected ID, a dropped commit) is simply absent.
    pub async fn observe_outcomes(
        &self,
        scenario: &Scenario,
        run: &str,
    ) -> Result<Vec<OutcomeExpectation>, Error> {
        let ids: std::collections::BTreeSet<String> = scenario
            .steps
            .iter()
            .filter_map(|step| step.command_id.as_ref().filter(|id| !id.is_empty()))
            .map(|id| scoped_command_id(run, &scenario.name, id))
            .collect();

        let mut outcomes = Vec::new();
        for id in ids {
            // An ID over the limit can never have a row (the step fails with CommandIdTooLong
            // before any write), and reading one is refused.
            if id.len() > photon_evt::MAX_COMMAND_ID_BYTES {
                continue;
            }

            if let Some(outcome) = self.store.repository().outcome(&id.as_str().into()).await? {
                outcomes.push(OutcomeExpectation {
                    command_id: id,
                    dynamo_item: serde_json::to_value(dynamo::outcome_item(&outcome)?)?,
                });
            }
        }

        Ok(outcomes)
    }

    async fn commit_raw(
        &self,
        entity_id: &EntityId,
        sequence: EventSequence,
        raw: &RawEvent,
        step: &Step,
    ) -> Result<Outcome, StepError> {
        let event = SerializedEvent {
            id: EventId::for_event(entity_id, sequence),
            sequence,
            event_type: EventType::new(&raw.event_type),
            version: EventVersion(raw.version),
            entity_id: entity_id.clone(),
            entity_type: EntityType::new("account"),
            payload: raw.payload.as_bytes().to_vec(),
            metadata: step.metadata(),
        };

        let commit = Commit::new(entity_id, vec![event]);

        match self.store.repository().commit(commit).await {
            Ok(()) => Ok(Outcome {
                events: 1,
                ..Outcome::of("ok")
            }),
            Err(err) if err.is_conflict() => Ok(Outcome::of("conflict")),
            Err(Error::StreamHeadDrift { .. }) => Ok(Outcome::of("drift")),
            Err(err) => Err(err.into()),
        }
    }

    /// The stream's stored head, read through the backend's head fixtures. A runner without them
    /// observes no heads.
    async fn stored_head(&self, entity_id: &EntityId) -> Result<Option<EventSequence>, Error> {
        match &self.heads {
            Some(heads) => heads.stored_head(entity_id).await,
            None => Ok(None),
        }
    }

    /// Reads a stream back: stored events and snapshot, loaded state, and the DynamoDB items and
    /// stream envelopes this implementation encodes for them.
    pub async fn observe(&self, name: &str, entity_id: &EntityId) -> Result<Expectation, Error> {
        let repository = self.store.repository();
        let events = repository.events(entity_id).await?;
        let snapshot = repository.snapshot(entity_id).await?;

        // A stream with a sequence gap — a failed transaction burned a sequence, or compaction
        // landed mid-load — has no coherent aggregate view. Mirroring Go's Observe, its stored
        // rows are still compared while the loaded state is recorded as absent.
        let loaded = match self.store.load(entity_id).await {
            Ok(loaded) => Some(loaded),
            Err(Error::SequenceGap { .. }) => None,
            Err(err) => return Err(err),
        };
        let (state, sequence, snapshot_sequence) = match &loaded {
            Some(loaded) => (
                json::to_string(loaded.aggregate())?,
                loaded.sequence().0,
                loaded.snapshot_sequence().0,
            ),
            None => (String::new(), 0, 0),
        };

        let head = self.stored_head(entity_id).await?;
        let mut dynamo_items = Vec::new();
        if let Some(head) = head {
            dynamo_items.push(serde_json::to_value(dynamo::head_item(
                entity_id, head, None,
            ))?);
        }
        if let Some(snapshot) = &snapshot {
            dynamo_items.push(serde_json::to_value(dynamo::snapshot_item(
                snapshot, None,
            )?)?);
        }

        let mut encoded_events = Vec::new();
        let mut sns_messages = Vec::new();
        for event in &events {
            dynamo_items.push(serde_json::to_value(dynamo::event_item(event, None)?)?);
            encoded_events.push(utf8(event.to_json()?));
            sns_messages.push(utf8(envelope::encode(event, ORIGIN_SOURCE)?));
        }

        Ok(Expectation {
            name: name.to_owned(),
            entity_id: entity_id.to_string(),
            sequence,
            snapshot_sequence,
            state,
            events: encoded_events,
            snapshot: snapshot.map(|s| s.to_json()).transpose()?.map(utf8),
            head: head.map(|head| head.0),
            dynamo_items,
            sns_messages,
            siblings: Vec::new(),
            transactions: Vec::new(),
            outcomes: Vec::new(),
        })
    }

    /// Reads the stored events, snapshot, and DynamoDB items of each sibling stream, in the order
    /// given. A stream a scenario fences but never writes reads back as empty.
    pub async fn observe_siblings(
        &self,
        entity_ids: &[EntityId],
    ) -> Result<Vec<SiblingExpectation>, Error> {
        let repository = self.store.repository();
        let mut siblings = Vec::with_capacity(entity_ids.len());

        for entity_id in entity_ids {
            let events = repository.events(entity_id).await?;
            let snapshot = repository.snapshot(entity_id).await?;

            let head = self.stored_head(entity_id).await?;
            let mut dynamo_items = Vec::new();
            if let Some(head) = head {
                dynamo_items.push(serde_json::to_value(dynamo::head_item(
                    entity_id, head, None,
                ))?);
            }
            if let Some(snapshot) = &snapshot {
                dynamo_items.push(serde_json::to_value(dynamo::snapshot_item(
                    snapshot, None,
                )?)?);
            }
            for event in &events {
                dynamo_items.push(serde_json::to_value(dynamo::event_item(event, None)?)?);
            }

            siblings.push(SiblingExpectation {
                entity_id: entity_id.to_string(),
                events: events
                    .iter()
                    .map(|event| event.to_json().map(utf8))
                    .collect::<Result<Vec<_>, _>>()?,
                snapshot: snapshot
                    .as_ref()
                    .map(|snapshot| snapshot.to_json().map(utf8))
                    .transpose()?,
                head: head.map(|head| head.0),
                dynamo_items,
            });
        }

        Ok(siblings)
    }
}

/// The command ID of a step in an outcomes scenario: outcome rows are global to the table, so a
/// bare `c1` would collide across scenarios and across runs.
pub fn scoped_command_id(run: &str, scenario: &str, command_id: &str) -> String {
    format!("compat-{run}-{scenario}-{command_id}")
}

async fn execute_on<S: Repository>(
    store: &Store<Account, S>,
    entity_id: &EntityId,
    step: &Step,
    metadata: Metadata,
) -> Result<Result<CommandOutcome<AccountEvent>, Error>, StepError> {
    let command = step.command.clone().ok_or(StepError::Missing("command"))?;
    let balance = command.responds_with_balance();

    Ok(store
        .execute_with_response(entity_id, command, metadata, move |account, _| {
            Ok(balance.then(|| balance_response(account)))
        })
        .await)
}

/// Maps the result of a command on an outcomes store to a step outcome.
fn classify<E>(result: Result<CommandOutcome<E>, Error>) -> Result<Outcome, StepError> {
    match result {
        Ok(command) => Ok(Outcome::of_command(&command)),
        Err(Error::IdempotencyKeyReuse { .. }) => Ok(Outcome::of("reuse")),
        Err(Error::CommandIdTooLong { .. }) => Ok(Outcome::of("idTooLong")),
        Err(Error::AmbiguousWrite { .. }) => Ok(Outcome::of("ambiguous")),
        Err(Error::DuplicateCommand(_)) => Ok(Outcome::of("duplicate")),
        Err(error) if error.is_conflict() => Ok(Outcome::of("conflict")),
        Err(Error::StreamHeadDrift { .. }) => Ok(Outcome::of("drift")),
        Err(error) => Err(error.into()),
    }
}

fn transaction_id(base: &EntityId, suffix: &str) -> EntityId {
    if suffix.is_empty() {
        base.clone()
    } else {
        EntityId::new(format!("{base}-{suffix}"))
    }
}

fn raw_event(
    entity_id: &EntityId,
    sequence: EventSequence,
    raw: &RawEvent,
    metadata: &Metadata,
) -> SerializedEvent {
    SerializedEvent {
        id: EventId::for_event(entity_id, sequence),
        sequence,
        event_type: EventType::new(&raw.event_type),
        version: EventVersion(raw.version),
        entity_id: entity_id.clone(),
        entity_type: EntityType::new("account"),
        payload: raw.payload.as_bytes().to_vec(),
        metadata: metadata.clone(),
    }
}

fn utf8(bytes: Vec<u8>) -> String {
    String::from_utf8(bytes).expect("JSON encoders write UTF-8")
}
