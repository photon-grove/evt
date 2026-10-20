//! The [`Store`]: load aggregates, handle commands, and commit events with snapshots.

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::aggregate::{Aggregate, Projector};
use crate::commit_id::{self, CommitIdGenerator};
use crate::error::{BoxError, Error, ExecuteError, ReplayPhase};
use crate::event::{DomainEvent, SerializedEvent};
use crate::ids::{CommandId, EntityId, EntityType, EventId, EventSequence};
use crate::json;
use crate::metadata::Metadata;
use crate::outcome::{
    CommandIdentity, CommandOutcome, DigestInput, Outcome, OutcomeCommand, OutcomeConfig,
    Rejection, Settled, ToRejection, pre_check, principal, require_command_id, settle,
};
use crate::projection::{CommitProjector, CommitStream, CommitView, HistoryLedger, project_commit};
use crate::repository::{Commit, NewSnapshot, Repository, snapshot_events_due};
use crate::retry::{Budget, Failed, Retry, RetryPolicy};
use crate::transaction::{Begin, PendingCommand, Transaction, freeze_command, persist_rejection};
use crate::upcast::upcast_event;

/// How many times [`Store::load`] reads the stream when it finds a sequence gap.
const LOAD_ATTEMPTS: usize = 3;

/// The default number of events between snapshots, matching the Go backends' defaults.
pub const DEFAULT_SNAPSHOT_INTERVAL: usize = 5;

/// Loads aggregates of type `A` from a repository, handles commands, and commits the resulting
/// events, writing a snapshot every [`snapshot interval`](Store::with_snapshot_interval) events.
///
/// A store is `Send + Sync` when its repository and services are, so it can be shared across
/// tasks behind an `Arc`. It is `Clone` when the repository and services are `Clone`; projectors
/// are shared between clones.
pub struct Store<A: Aggregate, R: Repository> {
    pub(crate) repository: R,
    services: A::Services,
    pub(crate) snapshot_interval: usize,
    pub(crate) projectors: Vec<Arc<dyn Projector<A, R::Operation>>>,
    pub(crate) commit_projectors: Vec<Arc<dyn CommitProjector>>,
    pub(crate) history_declared: HistoryLedger,
    pub(crate) commit_ids: CommitIdGenerator,
    pub(crate) outcomes: Option<OutcomeConfig>,
}

impl<A, R> Clone for Store<A, R>
where
    A: Aggregate,
    A::Services: Clone,
    R: Repository + Clone,
{
    fn clone(&self) -> Self {
        Self {
            repository: self.repository.clone(),
            services: self.services.clone(),
            snapshot_interval: self.snapshot_interval,
            projectors: self.projectors.clone(),
            commit_projectors: self.commit_projectors.clone(),
            history_declared: self.history_declared.clone(),
            commit_ids: self.commit_ids.clone(),
            outcomes: self.outcomes.clone(),
        }
    }
}

impl<A: Aggregate, R: Repository + fmt::Debug> fmt::Debug for Store<A, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Store")
            .field("aggregate", &A::TYPE)
            .field("repository", &self.repository)
            .field("snapshot_interval", &self.snapshot_interval)
            .field("projectors", &self.projectors.len())
            .field("commit_projectors", &self.commit_projectors.len())
            .finish_non_exhaustive()
    }
}

impl<A: Aggregate, R: Repository> Store<A, R> {
    /// Creates a store with the default snapshot interval.
    pub fn new(repository: R, services: A::Services) -> Self {
        Self {
            repository,
            services,
            snapshot_interval: DEFAULT_SNAPSHOT_INTERVAL,
            projectors: Vec::new(),
            commit_projectors: Vec::new(),
            history_declared: HistoryLedger::default(),
            commit_ids: commit_id::default_generator(),
            outcomes: None,
        }
    }

    /// Records every command as a durable outcome (C3, C4): a global `command-outcome#<id>` row,
    /// written in the same transaction as the events. The store then requires a command ID on
    /// every command, replays the stored outcome of a repeated command, and persists domain
    /// rejections. Use [`execute_with_outcome`](Store::execute_with_outcome) and
    /// [`Store::transaction`] with [`Transaction::begin_command`](crate::Transaction::begin_command);
    /// the plain [`execute`](Store::execute) and [`commit`](Store::commit) refuse to run on such a
    /// store with [`Error::InvalidConfig`] instead of silently skipping the outcome.
    ///
    /// The repository must support outcomes ([`Repository::outcome`]); the in-memory, DynamoDB, and
    /// PostgreSQL repositories do.
    #[must_use]
    pub fn with_outcomes(mut self, config: OutcomeConfig) -> Self {
        self.outcomes = Some(config);
        self
    }

    /// The outcome configuration, when command outcomes are enabled.
    pub fn outcomes(&self) -> Option<&OutcomeConfig> {
        self.outcomes.as_ref()
    }

    /// Replaces the generator that stamps a commit ID on commits whose metadata carries neither
    /// a command ID nor a commit ID. The default produces ULID-format IDs from the system clock;
    /// inject one for deterministic IDs in tests or to follow an existing ID scheme.
    #[must_use]
    pub fn with_commit_id_generator(
        mut self,
        generator: impl Fn() -> String + Send + Sync + 'static,
    ) -> Self {
        self.commit_ids = Arc::new(generator);
        self
    }

    /// Sets the number of events between snapshots. 0 disables snapshots.
    #[must_use]
    pub fn with_snapshot_interval(mut self, interval: usize) -> Self {
        self.snapshot_interval = interval;
        self
    }

    /// Adds an in-band projector whose operations commit atomically with each command's events.
    #[must_use]
    pub fn with_projector(mut self, projector: impl Projector<A, R::Operation> + 'static) -> Self {
        self.projectors.push(Arc::new(projector));
        self
    }

    /// Adds a commit-wide projector (v0.9, C7): a pure function over every event of a commit,
    /// whose [view operations](crate::ViewOperation) commit atomically with it, on any backend
    /// configured to write views. Projectors run in the order they are added, each once per
    /// commit that writes an event, in every path that builds one ([`execute`](Store::execute),
    /// [`commit`](Store::commit), the outcome paths, and [`Store::transaction`], which inherits
    /// them). A re-prepared command projects again from scratch. See [`CommitProjector`].
    #[must_use]
    pub fn with_commit_projector(mut self, projector: Arc<dyn CommitProjector>) -> Self {
        self.commit_projectors.push(projector);
        self
    }

    /// The underlying repository.
    pub fn repository(&self) -> &R {
        &self.repository
    }

    /// The services passed to command handlers.
    pub fn services(&self) -> &A::Services {
        &self.services
    }

    /// The configured snapshot interval.
    pub fn snapshot_interval(&self) -> usize {
        self.snapshot_interval
    }

    /// Loads an aggregate: from its snapshot plus later events when a snapshot exists, otherwise
    /// from every event. An entity with no events loads as `A::default()` at sequence 0.
    ///
    /// Event sequences must be contiguous. A gap means the stream was compacted between reading
    /// the snapshot and the events, so the load starts over, up to three times. A gap that
    /// persists, for example from expired events, fails with [`Error::SequenceGap`].
    ///
    /// Like any read, a load can be one compaction behind: if another writer snapshots and compacts
    /// during the load, the event read can stop short without a gap (it is empty, or a paginated
    /// query returns only its first page), and the load returns an older state. That state was real
    /// when it was read. A commit from it starts at or below the new
    /// snapshot and fails with [`Error::Conflict`], which [`execute_with_retry`] retries.
    ///
    /// [`execute_with_retry`]: Store::execute_with_retry
    pub async fn load(&self, entity_id: impl Into<EntityId>) -> Result<Loaded<A>, Error> {
        load_aggregate(&self.repository, entity_id.into()).await
    }

    /// Loads the aggregate, handles the command, and commits the resulting events.
    ///
    /// When `metadata` carries a command ID already recorded by an event replayed since the
    /// latest snapshot, it returns [`Error::DuplicateCommand`] without calling the handler.
    ///
    /// # Cancellation
    ///
    /// Not cancellation safe. If the future is dropped while the commit is in flight, the events
    /// may or may not have been stored. The same holds for a commit that fails with
    /// [`Error::Backend`] after a timeout. Reload the entity before deciding what to do. A command
    /// ID in `metadata` catches the repeat only if the lost commit did not write a snapshot and
    /// none has been written since, because IDs folded into a snapshot are no longer checked.
    pub async fn execute(
        &self,
        entity_id: impl Into<EntityId>,
        command: A::Command,
        metadata: Metadata,
    ) -> Result<Executed<A>, ExecuteError<A::Error>> {
        self.refuse_outcomes("execute")?;

        let loaded = self.load(entity_id).await?;

        if let Some(command_id) = &metadata.command_id
            && loaded.has_seen(command_id)
        {
            return Err(Error::DuplicateCommand(command_id.clone()).into());
        }

        let events = loaded
            .aggregate
            .handle(command, &self.services)
            .await
            .map_err(ExecuteError::Rejected)?;

        let (state, events) = self.commit(loaded, events, &metadata).await?;

        Ok(Executed { state, events })
    }

    /// Like [`execute`](Store::execute), but reloads and re-handles the command after a
    /// [retryable](Error::is_retryable) error (a conflict or a transient backend failure), waiting
    /// [`RetryPolicy::delay`] between attempts with `sleep`, such as `tokio::time::sleep`.
    ///
    /// This is blind whole-command retry for a store without outcomes: when attempts run out the
    /// last error is returned. On a store with [outcomes enabled](Store::with_outcomes) use
    /// [`execute_with_outcome`](Store::execute_with_outcome) or
    /// [`execute_transaction`](Store::execute_transaction), which retry under the store's own
    /// policy (fence-aware retry, C5) and record a stale-fence rejection when attempts run out.
    pub async fn execute_with_retry<S, F>(
        &self,
        entity_id: impl Into<EntityId>,
        command: A::Command,
        metadata: Metadata,
        policy: &RetryPolicy,
        sleep: S,
    ) -> Result<Executed<A>, ExecuteError<A::Error>>
    where
        A::Command: Clone,
        S: Fn(Duration) -> F + Send,
        F: Future<Output = ()> + Send,
    {
        let entity_id = entity_id.into();
        let max_attempts = policy.max_attempts.max(1);
        let mut retry = 0;

        loop {
            match self
                .execute(entity_id.clone(), command.clone(), metadata.clone())
                .await
            {
                Err(err) if err.is_retryable() && retry + 1 < max_attempts => {
                    sleep(policy.delay(retry)).await;
                    retry += 1;
                }
                result => return result,
            }
        }
    }

    /// Applies events to a loaded aggregate and commits them, snapshotting when the commit
    /// reaches the snapshot interval. Consumes `loaded` so a failed commit cannot leave stale
    /// state in use. On success it returns the updated state and the stored events.
    ///
    /// Refuses with [`Error::InvalidConfig`] on a store with
    /// [outcomes enabled](Store::with_outcomes); use
    /// [`execute_with_outcome`](Store::execute_with_outcome).
    pub async fn commit(
        &self,
        loaded: Loaded<A>,
        events: Vec<A::Event>,
        metadata: &Metadata,
    ) -> Result<(Loaded<A>, Vec<SerializedEvent>), Error> {
        self.refuse_outcomes("commit")?;

        if let Some(command_id) = &metadata.command_id
            && loaded.has_seen(command_id)
        {
            return Err(Error::DuplicateCommand(command_id.clone()));
        }

        if events.is_empty() {
            return Ok((loaded, Vec::new()));
        }

        self.ready_projectors().await?;

        let (loaded, serialized, commit) = self.stage(loaded, events, metadata)?;

        self.repository.commit(commit).await?;

        Ok((loaded, serialized))
    }

    /// Readies the repository for the commit-wide projectors before a commit that runs them: it
    /// must write view operations, and the entity types the projectors read events of are durably
    /// declared history-dependent (once each).
    async fn ready_projectors(&self) -> Result<(), Error> {
        crate::projection::ready_projectors(
            &self.repository,
            &self.commit_projectors,
            &self.history_declared,
        )
        .await
    }

    fn refuse_outcomes(&self, method: &str) -> Result<(), Error> {
        if self.outcomes.is_some() {
            return Err(Error::InvalidConfig(format!(
                "this store records command outcomes: {method} would skip the outcome row; use \
                 execute_with_outcome (or a Transaction with begin_command)"
            )));
        }

        Ok(())
    }

    /// Applies the decided events to the loaded aggregate and builds the repository commit that
    /// persists them (events, snapshot, projector operations), without writing it. The returned
    /// state is the one after the commit applies; it is only used if the commit succeeds.
    #[allow(clippy::type_complexity)]
    fn stage(
        &self,
        mut loaded: Loaded<A>,
        events: Vec<A::Event>,
        metadata: &Metadata,
    ) -> Result<(Loaded<A>, Vec<SerializedEvent>, Commit<R::Operation>), Error> {
        // The engine stamps the commit ID once per commit; every event of this commit carries it.
        let stamped = stamp_commit_id(metadata, &*self.commit_ids);
        let serialized =
            serialize_events::<A>(&loaded.entity_id, loaded.sequence, &events, &stamped)?;
        let snapshot_due =
            snapshot_events_due(loaded.sequence, events.len(), self.snapshot_interval) > 0;
        let projected_events = (!self.projectors.is_empty()).then(|| events.clone());

        for event in events {
            loaded.aggregate.apply(event);
        }

        let mut operations = Vec::new();
        if let Some(projected_events) = &projected_events {
            for projector in &self.projectors {
                let ops = projector
                    .project(&loaded.entity_id, &loaded.aggregate, projected_events)
                    .map_err(Error::Projector)?;
                operations.extend(ops);
            }
        }

        // The aggregate's JSON after the commit is the snapshot's payload and the state the
        // commit-wide projectors see, so it is encoded once and only when one of them needs it.
        let state = (snapshot_due || !self.commit_projectors.is_empty())
            .then(|| json::to_vec(&loaded.aggregate))
            .transpose()?;

        let views = if self.commit_projectors.is_empty() {
            Vec::new()
        } else {
            project_commit(
                &self.commit_projectors,
                &CommitView::new(
                    stamped.commit_id.clone(),
                    vec![CommitStream::new(
                        loaded.entity_id.clone(),
                        EntityType::new(A::TYPE),
                        serialized.clone(),
                        state.clone().unwrap_or_default(),
                    )],
                ),
            )?
        };

        let snapshot = if snapshot_due {
            Some(NewSnapshot {
                entity_type: EntityType::new(A::TYPE),
                sequence: loaded.snapshot_sequence.next(),
                payload: state.unwrap_or_default(),
            })
        } else {
            None
        };

        let snapshot_sequence = snapshot.as_ref().map(|snapshot| snapshot.sequence);

        let commit = Commit {
            entity_id: loaded.entity_id.clone(),
            events: serialized.clone(),
            snapshot,
            operations,
            views,
            outcome: None,
        };

        loaded.sequence = serialized
            .last()
            .map_or(loaded.sequence, |event| event.sequence);
        if let Some(sequence) = snapshot_sequence {
            loaded.snapshot_sequence = sequence;
        }
        if let Some(command_id) = metadata
            .command_id
            .as_ref()
            .filter(|id| !id.as_str().is_empty())
        {
            loaded.seen_command_ids.insert(command_id.clone());
        }

        Ok((loaded, serialized, commit))
    }

    /// Loads the aggregate, handles the command, and records the result as a durable outcome on a
    /// store with [outcomes enabled](Store::with_outcomes) (C3, C4), under fence-aware retry (C5).
    ///
    /// Every attempt starts by reading the command's outcome row, strongly consistent:
    ///
    /// - present with the same identity digest: the stored outcome is returned
    ///   ([`CommandOutcome::replayed`]), the handler does not run, and nothing is written;
    /// - present with another digest: [`Error::IdempotencyKeyReuse`];
    /// - absent: the aggregate is loaded and the command handled. Events (if any) and the outcome
    ///   commit in one transaction. An error returned by the handler is a domain rejection: it is
    ///   mapped with [`ToRejection`] and, when that yields a rejection, persisted as a rejected
    ///   outcome (a `None` is an internal failure: [`Error::Handler`], nothing recorded), which a retry of the
    ///   command replays verbatim. It writes no events unless the aggregate's
    ///   [`rejection_events`](Aggregate::rejection_events) returns some: those commit with the
    ///   rejected row in the same transaction, and the outcome lists them. A handler that returns
    ///   no events is an accepted no-op.
    ///
    /// When the commit conflicts with another writer the command is re-prepared: the aggregate is
    /// loaded again and the handler runs again against the fresh state, after a backoff on the
    /// repository's timer ([`Repository::backoff`]), up to the store's [`RetryPolicy`]
    /// ([`OutcomeConfig::with_retry`]). A single-aggregate command fences nothing, so every
    /// conflict can be re-prepared. When the last attempt also conflicts, the engine records a
    /// stale-fence rejection ([`Rejection::retries_exhausted`](crate::Rejection::retries_exhausted))
    /// as the command's outcome and returns it, so a replay of the command ID gets the same
    /// answer; the remedy is a new command ID. The handler may therefore run several times for one
    /// command, but at most one attempt commits. A transient backend failure is retried the same
    /// way and returned as an error when attempts run out. The command is cloned for every
    /// attempt, which is why it must be [`Clone`].
    ///
    /// Returns a [`CommandOutcome`], not an aggregate, so a replayed outcome has no loaded aggregate
    /// to keep in sync; call [`load`](Store::load) for the current state.
    ///
    /// Internal failures (encoding, storage, replay, projector errors) are errors and record no
    /// outcome. A lost outcome-put race resolves through the winner's row; an indeterminate commit
    /// failure is recovered by re-reading the row (see [`Error::AmbiguousWrite`]).
    ///
    /// The identity digest covers the command's type and payload ([`OutcomeCommand`]), the entity
    /// ID, and the metadata's principal. The command ID is required ([`Error::InvalidArgument`])
    /// and at most 512 bytes ([`Error::CommandIdTooLong`]), both checked before any read.
    ///
    /// # Cancellation
    ///
    /// Not cancellation safe, like [`execute`](Store::execute). If the future is dropped while the
    /// commit is in flight, the outcome row tells whether it applied: run the command again with
    /// the same command ID.
    pub async fn execute_with_outcome(
        &self,
        entity_id: impl Into<EntityId>,
        command: A::Command,
        metadata: Metadata,
    ) -> Result<CommandOutcome<A::Event>, Error>
    where
        A::Command: OutcomeCommand + Clone,
        A::Error: ToRejection,
    {
        // Boxed, like each attempt: callers that chain many commands in one task keep a small
        // future (the conformance cases do, in debug builds, on a default test-thread stack).
        Box::pin(self.execute_with_response(entity_id, command, metadata, |_, _| Ok(None))).await
    }

    /// Like [`execute_with_outcome`](Store::execute_with_outcome), and stores a response with an
    /// accepted outcome so a retried command returns the original answer. `respond` runs after the
    /// handler succeeds, with the aggregate as it is after the decided events (the current state
    /// for a no-op) and those events. The response is at most 65536 bytes
    /// ([`Error::OutcomeTooLarge`]); `None` or empty stores none. When the command is re-prepared
    /// after a conflict, `respond` runs again with the fresh state.
    pub async fn execute_with_response<F>(
        &self,
        entity_id: impl Into<EntityId>,
        command: A::Command,
        metadata: Metadata,
        mut respond: F,
    ) -> Result<CommandOutcome<A::Event>, Error>
    where
        A::Command: OutcomeCommand + Clone,
        A::Error: ToRejection,
        F: FnMut(&A, &[A::Event]) -> Result<Option<Vec<u8>>, BoxError>,
    {
        let config = self.outcomes.as_ref().ok_or_else(|| {
            Error::InvalidConfig("command outcomes are not enabled; call with_outcomes".into())
        })?;

        let entity_id = entity_id.into();
        crate::outcome::check_targets([&entity_id])?;
        let command_id = require_command_id(&metadata)?;
        let digest = config.digest(&DigestInput {
            command_type: command.command_type(),
            targets: vec![entity_id.clone()],
            payload: command
                .canonical_payload()
                .map_err(|err| Error::InvalidArgument(format!("the command payload: {err}")))?
                .unwrap_or_default(),
            principal: principal(&metadata),
        })?;

        let mut budget = Budget::new(config.retry());

        loop {
            // Boxed: an attempt's future holds the load, the handler, and the commit, and keeping
            // it on the heap keeps this loop's own future small for callers that chain many
            // commands in one task (the conformance cases do, in debug builds).
            let result = Box::pin(self.attempt_with_response(
                entity_id.clone(),
                command.clone(),
                &metadata,
                &command_id,
                &digest,
                &mut respond,
            ))
            .await;

            let failed = match result {
                Ok(outcome) => return Ok(outcome),
                Err(failed) => failed,
            };

            let kind = failed.retry();
            if kind == Retry::Never {
                return Err(failed.error);
            }

            let Some(delay) = budget.next() else {
                if kind == Retry::Transient {
                    return Err(failed.error);
                }

                return persist_rejection(
                    &self.repository,
                    &command_id,
                    &digest,
                    config.recovery_wait(),
                    Rejection::retries_exhausted(budget.attempts()),
                )
                .await;
            };

            self.repository.backoff(delay).await;
        }
    }

    /// One attempt of [`execute_with_response`](Store::execute_with_response): the outcome
    /// pre-check, the load, the handler, and the commit, all from scratch.
    async fn attempt_with_response<F>(
        &self,
        entity_id: EntityId,
        command: A::Command,
        metadata: &Metadata,
        command_id: &CommandId,
        digest: &str,
        respond: &mut F,
    ) -> Result<CommandOutcome<A::Event>, Failed>
    where
        A::Error: ToRejection,
        F: FnMut(&A, &[A::Event]) -> Result<Option<Vec<u8>>, BoxError>,
    {
        if let Some(stored) = pre_check(&self.repository, command_id, digest)
            .await
            .map_err(Failed::deciding)?
        {
            return Ok(CommandOutcome::replay(stored));
        }

        let loaded = self.load(entity_id).await.map_err(Failed::deciding)?;

        // Events written before outcomes existed carry command IDs but have no outcome row; the
        // window catches those duplicates (and only those: this store's own commits write rows).
        if loaded.has_seen(command_id) {
            return Err(Failed::internal(Error::DuplicateCommand(
                command_id.clone(),
            )));
        }

        let entity_id = loaded.entity_id.clone();
        let digest = digest.to_owned();
        let command_id = command_id.clone();

        let events = match loaded.aggregate.handle(command, &self.services).await {
            Ok(events) => events,
            Err(error) => {
                // `None` is an internal failure: returned, and nothing is recorded.
                let Some(rejection) = error.to_rejection() else {
                    return Err(Failed::internal(Error::Handler(Box::new(error))));
                };

                // Canonicalized through its stored encoding, so a first execution and a replay
                // return the same rejection.
                let rejection = rejection.canonical().map_err(Failed::internal)?;
                let outcome = Outcome::rejected(command_id, digest, rejection);

                // A rejection may carry events: they commit with the rejected row like an
                // accepted command's, decided on the state this attempt loaded.
                let failure_events = loaded.aggregate.rejection_events(&error);
                if failure_events.is_empty() {
                    let commit = Commit::new(entity_id, Vec::new()).with_outcome(outcome);

                    return self
                        .record(commit, Vec::new(), Vec::new())
                        .await
                        .map_err(Failed::committing);
                }

                let decided = failure_events.clone();
                self.ready_projectors().await.map_err(Failed::internal)?;
                let (_, serialized, mut commit) = self
                    .stage(loaded, failure_events, metadata)
                    .map_err(Failed::internal)?;
                commit.outcome = Some(outcome);

                return self
                    .record(commit, serialized, decided)
                    .await
                    .map_err(Failed::committing);
            }
        };

        let decided = events.clone();

        if events.is_empty() {
            let response = respond(&loaded.aggregate, &[])
                .map_err(response_error)
                .map_err(Failed::internal)?;
            let outcome = with_response(Outcome::accepted(command_id, digest), response);
            let commit = Commit::new(entity_id, Vec::new()).with_outcome(outcome);

            return self
                .record(commit, Vec::new(), decided)
                .await
                .map_err(Failed::committing);
        }

        self.ready_projectors().await.map_err(Failed::internal)?;
        let (loaded, serialized, mut commit) = self
            .stage(loaded, events, metadata)
            .map_err(Failed::internal)?;
        let response = respond(&loaded.aggregate, &decided)
            .map_err(response_error)
            .map_err(Failed::internal)?;
        commit.outcome = Some(with_response(
            Outcome::accepted(command_id, digest),
            response,
        ));

        self.record(commit, serialized, decided)
            .await
            .map_err(Failed::committing)
    }

    /// Reads the outcome row of a command without running or writing anything, as ambiguous-write
    /// recovery does. `command` supplies the identity (its type and payload) and `entity_id` the
    /// target stream, exactly as [`execute_with_outcome`](Store::execute_with_outcome) digests
    /// them; `metadata` supplies the command ID and principal.
    ///
    /// Returns `None` when no row exists, the stored outcome ([`CommandOutcome::replayed`]) for
    /// the same identity digest, and [`Error::IdempotencyKeyReuse`] for another. A missing row is
    /// not proof the command did not apply: a transaction can land after any single read.
    pub async fn resolve_outcome(
        &self,
        entity_id: impl Into<EntityId>,
        command: &A::Command,
        metadata: &Metadata,
    ) -> Result<Option<CommandOutcome<A::Event>>, Error>
    where
        A::Command: OutcomeCommand,
    {
        let config = self.outcomes.as_ref().ok_or_else(|| {
            Error::InvalidConfig("command outcomes are not enabled; call with_outcomes".into())
        })?;

        let entity_id = entity_id.into();
        crate::outcome::check_targets([&entity_id])?;
        let command_id = require_command_id(metadata)?;
        let digest = config.digest(&DigestInput {
            command_type: command.command_type(),
            targets: vec![entity_id],
            payload: command
                .canonical_payload()
                .map_err(|err| Error::InvalidArgument(format!("the command payload: {err}")))?
                .unwrap_or_default(),
            principal: principal(metadata),
        })?;

        Ok(pre_check(&self.repository, &command_id, &digest)
            .await?
            .map(CommandOutcome::replay))
    }

    /// Writes a commit that carries an outcome and settles the result.
    async fn record(
        &self,
        commit: Commit<R::Operation>,
        serialized: Vec<SerializedEvent>,
        typed_events: Vec<A::Event>,
    ) -> Result<CommandOutcome<A::Event>, Error> {
        let stored = commit
            .stored_outcome()
            .expect("record is only called with an outcome");
        let (command_id, digest) = (stored.command_id.clone(), stored.digest.clone());

        let result = self.repository.commit(commit).await;

        let window = self
            .outcomes
            .as_ref()
            .map_or(Duration::ZERO, OutcomeConfig::recovery_wait);

        match settle(&self.repository, &command_id, &digest, window, result).await? {
            Settled::Committed => Ok(CommandOutcome::fresh(stored, serialized, typed_events)),
            Settled::Replayed(outcome) => Ok(CommandOutcome::replay(outcome)),
        }
    }

    /// Runs a multi-stream command on a store with [outcomes enabled](Store::with_outcomes) under
    /// fence-aware retry (C5). Each attempt starts a [`Transaction`], begins the command
    /// ([`Transaction::begin_command`]: it validates the command ID, computes the identity digest
    /// from `identity` and the metadata's principal, and reads the outcome row), runs `prepare`
    /// against the transaction, and commits it with the [`Decision`] it returns. A stored outcome
    /// with the same digest is returned without running `prepare`.
    ///
    /// `prepare` is the command's decision step, and does what a handler does: read the aggregates
    /// it needs ([`Transaction::read`]), decide, append events to any of them, and add
    /// caller-stamped fences ([`Transaction::fence`]). Return [`Decision::accept`] to commit,
    /// [`Decision::reject`] with a domain [`Rejection`] to record a rejected outcome (fenced on
    /// everything it read, so it is only recorded on state that still holds; staged events are
    /// discarded), [`Decision::reject_with_events`] to commit the staged events with the
    /// rejection in the same atomic commit, or an `Err` for an
    /// internal failure, which is returned with no outcome written. Appending nothing and
    /// accepting is an accepted no-op.
    ///
    /// When the commit conflicts with another writer, the command is re-prepared: `prepare` runs
    /// again, from scratch, against freshly loaded state, after a backoff on the repository's
    /// timer, up to the store's [`RetryPolicy`] ([`OutcomeConfig::with_retry`]), provided the
    /// failed item is one the engine can re-derive: a head update, an event put, a snapshot
    /// condition, or an engine-filled fence. Stream head drift is never retried. A failed caller-stamped fence is not retried:
    /// the engine records the stale-fence rejection at once ([`Transaction::commit_with_outcome`]).
    /// When the last attempt also conflicts, it records a stale-fence rejection
    /// ([`Rejection::retries_exhausted`](crate::Rejection::retries_exhausted)) as the outcome
    /// instead of returning the conflict, so a replay of the command ID gets the same answer; the
    /// remedy is a new command ID. A transient backend failure is retried the same way and
    /// returned as an error when attempts run out.
    ///
    /// `prepare` must therefore be safe to repeat: it decides from the transaction's reads, not
    /// from state captured by an earlier call, and its external side effects must be idempotent.
    /// Only the attempt that commits writes anything.
    ///
    /// The identity is the caller's intent, fixed before anything is read. Its targets are the
    /// streams the request names, not the streams `prepare` ends up writing, so a command whose
    /// write set depends on state, such as a rename that releases whichever handle the entity
    /// currently holds, keeps one identity across attempts, client retries, and recovery.
    ///
    /// # Cancellation
    ///
    /// Not cancellation safe, like [`execute_with_outcome`](Store::execute_with_outcome): run the
    /// command again with the same command ID, and the outcome row tells whether it applied.
    pub async fn execute_transaction<'s, F>(
        &'s self,
        identity: &CommandIdentity,
        metadata: &Metadata,
        mut prepare: F,
    ) -> Result<CommandOutcome, Error>
    where
        F: for<'t> FnMut(&'t mut Transaction<'s, R>) -> PrepareFuture<'t>,
    {
        let config = self.outcomes.as_ref().ok_or_else(|| {
            Error::InvalidConfig("command outcomes are not enabled; call with_outcomes".into())
        })?;

        // The command is validated and its identity digest computed once, before the first
        // attempt; every attempt's transaction installs these frozen values.
        let pending = freeze_command(config, identity, metadata)?;
        let mut budget = Budget::new(config.retry());

        loop {
            // Boxed for the same reason as the single-aggregate attempt.
            let result = Box::pin(self.attempt_transaction(&pending, &mut prepare)).await;

            let failed = match result {
                Ok(outcome) => return Ok(outcome),
                Err(failed) => failed,
            };

            let kind = failed.retry();
            if kind == Retry::Never {
                return Err(failed.error);
            }

            let Some(delay) = budget.next() else {
                if kind != Retry::Conflict {
                    return Err(failed.error);
                }

                return persist_rejection(
                    &self.repository,
                    pending.command_id(),
                    pending.digest(),
                    config.recovery_wait(),
                    Rejection::retries_exhausted(budget.attempts()),
                )
                .await;
            };

            self.repository.backoff(delay).await;
        }
    }

    /// One attempt of [`execute_transaction`](Store::execute_transaction) with the command frozen
    /// before the first attempt.
    async fn attempt_transaction<'s, F>(
        &'s self,
        pending: &PendingCommand,
        prepare: &mut F,
    ) -> Result<CommandOutcome, Failed>
    where
        F: for<'t> FnMut(&'t mut Transaction<'s, R>) -> PrepareFuture<'t>,
    {
        let mut transaction = self.transaction();

        match transaction
            .begin_frozen(pending)
            .await
            .map_err(Failed::deciding)?
        {
            Begin::Replayed(outcome) => return Ok(*outcome),
            Begin::Proceed => {}
        }

        let decision = match prepare(&mut transaction).await {
            Ok(decision) => decision,
            // An error the step passed on from a failed `Transaction::read` is a load failure,
            // retried but never recorded against the command. Any other error is the step's own
            // failure, even when it is a `Conflict` or `Transient`: it is returned at once.
            Err(error) if transaction.is_read_failure(&error) => {
                return Err(Failed::deciding(error));
            }
            Err(error) => return Err(Failed::internal(error)),
        };

        match decision {
            Decision::Accept(response) => transaction
                .commit_with_outcome(response)
                .await
                .map_err(Failed::committing),
            Decision::Reject(rejection) => transaction
                .reject(*rejection)
                .await
                .map_err(Failed::committing),
            Decision::RejectWithEvents(rejection) => transaction
                .reject_with_events(*rejection)
                .await
                .map_err(Failed::committing),
        }
    }

    /// Lists the IDs of every stored entity of this aggregate type, for rebuilds. Load each with
    /// [`load`](Store::load), which is snapshot-aware and therefore correct after compaction.
    pub async fn entity_ids(&self) -> Result<Vec<EntityId>, Error> {
        let heads = self
            .repository
            .entity_heads(Some(&EntityType::new(A::TYPE)))
            .await?;

        Ok(heads.into_keys().collect())
    }
}

/// Loads any aggregate type from a repository, retrying when a sequence gap shows a compaction
/// raced the read. [`Store::load`] and [`Transaction::read`](crate::Transaction::read) share it.
pub(crate) async fn load_aggregate<A: Aggregate, R: Repository>(
    repository: &R,
    entity_id: EntityId,
) -> Result<Loaded<A>, Error> {
    let mut attempt = 1;

    // A sequence gap means another writer snapshotted and compacted the stream between the
    // snapshot read and the event read. Loading again starts from the newer snapshot.
    loop {
        match load_once(repository, entity_id.clone()).await {
            Err(err) if err.is_conflict() && attempt < LOAD_ATTEMPTS => attempt += 1,
            result => return result,
        }
    }
}

async fn load_once<A: Aggregate, R: Repository>(
    repository: &R,
    entity_id: EntityId,
) -> Result<Loaded<A>, Error> {
    let snapshot = repository.snapshot(&entity_id).await?;
    let has_snapshot = snapshot.is_some();

    let mut loaded = match snapshot {
        Some(snapshot) => {
            let aggregate =
                serde_json::from_slice(&snapshot.payload).map_err(|source| Error::Snapshot {
                    entity_id: entity_id.clone(),
                    source,
                })?;

            Loaded {
                entity_id,
                aggregate,
                sequence: snapshot.event_sequence,
                snapshot_sequence: snapshot.sequence,
                seen_command_ids: HashSet::new(),
            }
        }
        None => Loaded::new(entity_id),
    };

    let events = if has_snapshot {
        repository
            .events_after(&loaded.entity_id, loaded.sequence)
            .await?
    } else {
        repository.events(&loaded.entity_id).await?
    };

    for event in events {
        loaded.replay(event)?;
    }

    Ok(loaded)
}

/// An aggregate loaded from the event log, with the stream position it was loaded at.
#[derive(Debug)]
pub struct Loaded<A> {
    pub(crate) entity_id: EntityId,
    pub(crate) aggregate: A,
    pub(crate) sequence: EventSequence,
    pub(crate) snapshot_sequence: EventSequence,
    pub(crate) seen_command_ids: HashSet<CommandId>,
}

impl<A: Aggregate> Loaded<A> {
    pub(crate) fn new(entity_id: EntityId) -> Self {
        Self {
            entity_id,
            aggregate: A::default(),
            sequence: EventSequence::ZERO,
            snapshot_sequence: EventSequence::ZERO,
            seen_command_ids: HashSet::new(),
        }
    }

    /// Upcasts, decodes, and applies one stored event.
    pub(crate) fn replay(&mut self, stored: SerializedEvent) -> Result<(), Error> {
        if stored.sequence != self.sequence.next() {
            return Err(Error::SequenceGap {
                entity_id: self.entity_id.clone(),
                after: self.sequence,
                found: stored.sequence,
            });
        }

        let stored = upcast_event(A::upcasters(), stored).map_err(|failure| Error::Replay {
            event_type: failure.event_type,
            version: failure.version,
            phase: ReplayPhase::Upcast,
            source: failure.source,
        })?;

        let event =
            A::Event::from_payload(stored.event_type.as_str(), stored.version, &stored.payload)
                .map_err(|source| Error::Replay {
                    event_type: stored.event_type.clone(),
                    version: stored.version,
                    phase: ReplayPhase::Deserialize,
                    source: source.into(),
                })?;

        self.sequence = stored.sequence;
        if let Some(command_id) = stored
            .metadata
            .command_id
            .filter(|id| !id.as_str().is_empty())
        {
            self.seen_command_ids.insert(command_id);
        }

        self.aggregate.apply(event);

        Ok(())
    }

    /// The entity ID.
    pub fn entity_id(&self) -> &EntityId {
        &self.entity_id
    }

    /// The aggregate state.
    pub fn aggregate(&self) -> &A {
        &self.aggregate
    }

    /// Consumes the loaded state and returns the aggregate.
    pub fn into_aggregate(self) -> A {
        self.aggregate
    }

    /// The sequence of the last event folded into the state (0 for a new entity).
    pub fn sequence(&self) -> EventSequence {
        self.sequence
    }

    /// The sequence of the latest snapshot (0 when the entity has none).
    pub fn snapshot_sequence(&self) -> EventSequence {
        self.snapshot_sequence
    }

    /// Reports whether a command ID was recorded by an event replayed since the latest snapshot.
    pub fn has_seen(&self, command_id: &CommandId) -> bool {
        // An empty command ID is absent, never a duplicate, as when stamping the commit ID.
        !command_id.as_str().is_empty() && self.seen_command_ids.contains(command_id)
    }
}

/// The result of a successful [`Store::execute`].
#[derive(Debug)]
#[non_exhaustive]
pub struct Executed<A> {
    /// The aggregate state after the new events.
    pub state: Loaded<A>,
    /// The events as stored.
    pub events: Vec<SerializedEvent>,
}

fn response_error(err: BoxError) -> Error {
    Error::InvalidArgument(format!("the response callback failed: {err}"))
}

fn with_response(outcome: Outcome, response: Option<Vec<u8>>) -> Outcome {
    match response {
        Some(response) => outcome.with_response(response),
        None => outcome,
    }
}

/// Serializes typed events into stored events starting after `sequence`.
pub fn serialize_events<A: Aggregate>(
    entity_id: &EntityId,
    sequence: EventSequence,
    events: &[A::Event],
    metadata: &Metadata,
) -> Result<Vec<SerializedEvent>, Error> {
    let mut sequence = sequence;

    events
        .iter()
        .map(|event| {
            sequence = sequence.checked_next().ok_or_else(|| {
                Error::InvalidCommit(format!(
                    "entity {entity_id} is at the maximum event sequence"
                ))
            })?;

            Ok(SerializedEvent {
                id: EventId::for_event(entity_id, sequence),
                sequence,
                event_type: event.event_type().into(),
                version: event.event_version(),
                entity_id: entity_id.clone(),
                entity_type: EntityType::new(A::TYPE),
                payload: event.to_payload()?,
                metadata: metadata.clone(),
            })
        })
        .collect()
}

/// Stamps the engine's commit identity: the command ID when present and non-empty, a
/// caller-supplied commit ID otherwise, else a generated ULID-format ID. An empty command ID is
/// absent, as in Go's `CommitMetadata`, so both languages derive the same commit ID.
pub(crate) fn stamp_commit_id(
    metadata: &Metadata,
    generate: &(dyn Fn() -> String + Send + Sync),
) -> Metadata {
    let mut metadata = metadata.clone();
    if let Some(command_id) = metadata
        .command_id
        .as_ref()
        .filter(|command_id| !command_id.as_str().is_empty())
    {
        metadata.commit_id = command_id.to_string();

        return metadata;
    }

    if metadata.commit_id.is_empty() {
        metadata.commit_id = generate();
    }

    metadata
}

/// The decision a [`Store::execute_transaction`] `prepare` function reaches: commit the staged
/// events and record an accepted outcome, or record a domain rejection, with or without the
/// staged events.
#[derive(Debug)]
#[non_exhaustive]
pub enum Decision {
    /// Commit what the transaction staged and record an accepted outcome, storing the response
    /// (at most 65536 bytes; `None` or empty stores none) for a retried command to return.
    Accept(Option<Vec<u8>>),
    /// Record the command as rejected, writing no events. The rejection is fenced on everything
    /// the transaction read, so it is only recorded on state that still holds.
    Reject(Box<Rejection>),
    /// Record the command as rejected and commit the events the transaction staged in the same
    /// atomic commit (see [`Transaction::reject_with_events`]).
    RejectWithEvents(Box<Rejection>),
}

impl Decision {
    /// Accepts the command without a response.
    pub fn accept() -> Self {
        Self::Accept(None)
    }

    /// Accepts the command and stores `response` with the outcome.
    pub fn accept_with(response: impl Into<Vec<u8>>) -> Self {
        Self::Accept(Some(response.into()))
    }

    /// Rejects the command with a domain rejection.
    pub fn reject(rejection: Rejection) -> Self {
        Self::Reject(Box::new(rejection))
    }

    /// Rejects the command and commits the events staged with [`Transaction::append`] in the same
    /// atomic commit, so the aggregate leaves the state the command refused and the history shows
    /// why: for example, a proposal whose apply fails records an `apply-failed` event beside the
    /// rejection. [`Decision::reject`] discards staged events instead.
    pub fn reject_with_events(rejection: Rejection) -> Self {
        Self::RejectWithEvents(Box::new(rejection))
    }
}

/// The future a [`Store::execute_transaction`] `prepare` function returns. Box it:
/// `|tx| Box::pin(async move { ... })`.
pub type PrepareFuture<'t> = Pin<Box<dyn Future<Output = Result<Decision, Error>> + Send + 't>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::{Added, Ledger, LedgerEvent};

    /// An aggregate whose handler decides on derived state under the reader epoch check, reading
    /// the generation through its services.
    #[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
    struct Gauge;

    #[derive(Clone)]
    struct Decide;

    impl OutcomeCommand for Decide {
        fn command_type(&self) -> String {
            "gauge:decide".into()
        }

        fn canonical_payload(&self) -> Result<Option<Vec<u8>>, BoxError> {
            Ok(None)
        }
    }

    #[derive(Debug, thiserror::Error)]
    #[error(transparent)]
    struct Unready(Error);

    impl ToRejection for Unready {
        fn to_rejection(&self) -> Option<Rejection> {
            None
        }
    }

    impl Aggregate for Gauge {
        const TYPE: &'static str = "gauge";

        type Command = Decide;
        type Event = LedgerEvent;
        type Error = Unready;
        /// The fence to read the generation from, and a count of the handler's runs.
        type Services = (crate::MemRepository, Arc<std::sync::atomic::AtomicUsize>);

        async fn handle(
            &self,
            _: Decide,
            (fence, runs): &Self::Services,
        ) -> Result<Vec<LedgerEvent>, Unready> {
            runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            crate::read_at_rebuild_epoch(fence, |_| async {
                Ok::<_, Error>(vec![Added { amount: 1 }.into()])
            })
            .await
            .map_err(Unready)
        }

        fn apply(&mut self, _: LedgerEvent) {}
    }

    #[tokio::test]
    async fn a_handler_that_read_derived_state_during_a_rebuild_fails_at_once_with_no_outcome() {
        use crate::RebuildFence;

        let repository = crate::MemRepository::new();
        let runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let store = Store::<Gauge, _>::new(repository.clone(), (repository.clone(), runs.clone()))
            .with_outcomes(OutcomeConfig::new().with_retry(RetryPolicy::immediate(5)));
        let metadata = || Metadata::default().with_command_id("cmd-1");

        let lease = repository
            .begin_rebuild("rebuild-1", Duration::from_secs(60))
            .await
            .unwrap();
        let error = store
            .execute_with_outcome("g-1", Decide, metadata())
            .await
            .unwrap_err();

        // The handler's own error: returned at once, never retried, and nothing recorded.
        let Error::Handler(source) = &error else {
            panic!("{error:?}");
        };
        assert!(
            matches!(
                source.downcast_ref::<Unready>(),
                Some(Unready(Error::RebuildUnavailable { generation: 1, .. }))
            ),
            "{source}"
        );
        assert_eq!(
            runs.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the handler ran once"
        );
        assert!(repository.outcome(&"cmd-1".into()).await.unwrap().is_none());
        assert!(repository.events(&"g-1".into()).await.unwrap().is_empty());

        // Once the rebuild is over the same command ID runs.
        repository.end_rebuild(&lease).await.unwrap();
        let outcome = store
            .execute_with_outcome("g-1", Decide, metadata())
            .await
            .unwrap();
        assert!(outcome.is_accepted());
    }

    #[test]
    fn serializing_past_the_maximum_sequence_fails() {
        let events = [Added { amount: 1 }.into()];
        let id = EntityId::new("ledger-1");

        let result =
            serialize_events::<Ledger>(&id, EventSequence(i64::MAX), &events, &Metadata::default());

        assert!(matches!(result, Err(Error::InvalidCommit(_))), "{result:?}");
    }

    #[test]
    fn an_empty_command_id_is_absent_when_stamping_the_commit_id() {
        let generated = || "GENERATED".to_owned();

        let empty = Metadata::default().with_command_id("");
        assert_eq!(stamp_commit_id(&empty, &generated).commit_id, "GENERATED");

        let mut pinned = Metadata::default().with_command_id("");
        pinned.commit_id = "caller-commit".to_owned();
        assert_eq!(
            stamp_commit_id(&pinned, &generated).commit_id,
            "caller-commit"
        );

        // A non-empty command ID still wins over a caller-supplied commit ID.
        let mut derived = Metadata::default().with_command_id("cmd-1");
        derived.commit_id = "caller-commit".to_owned();
        assert_eq!(stamp_commit_id(&derived, &generated).commit_id, "cmd-1");
    }

    #[tokio::test]
    async fn an_empty_command_id_is_never_a_duplicate() {
        let store = crate::Store::<Ledger, _>::new(crate::MemRepository::new(), ());
        let empty = Metadata::default().with_command_id("");

        // The same empty command ID twice: absent both times, so neither is a duplicate.
        store
            .execute("ledger-1", crate::conformance::Add(1), empty.clone())
            .await
            .unwrap();
        store
            .execute("ledger-1", crate::conformance::Add(1), empty.clone())
            .await
            .unwrap();

        let loaded = store.load("ledger-1").await.unwrap();
        assert_eq!(loaded.aggregate().entries, 2);
        assert!(!loaded.has_seen(&CommandId::new("")));

        // A real command ID is still deduplicated.
        let real = Metadata::default().with_command_id("cmd-1");
        store
            .execute("ledger-1", crate::conformance::Add(1), real.clone())
            .await
            .unwrap();
        let duplicate = store
            .execute("ledger-1", crate::conformance::Add(1), real)
            .await;
        assert!(matches!(
            duplicate.unwrap_err().as_store_error(),
            Some(Error::DuplicateCommand(_))
        ));
    }

    mod outcomes {
        use serde::{Deserialize, Serialize};

        use super::*;
        use crate::conformance::FaultInjectingRepository;
        use crate::outcome::{Rejection, ToRejection};
        use crate::{Aggregate, MemRepository, OutcomeCommand, OutcomeConfig, RetryPolicy};

        /// A counter whose command is the amount to add: zero is a no-op, a negative amount is
        /// refused.
        #[derive(Debug, Default, Serialize, Deserialize)]
        struct Counter {
            total: i64,
        }

        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        struct Bumped {
            amount: i64,
        }

        crate::domain_events! {
            #[derive(Clone, Debug, PartialEq)]
            enum CounterEvent {
                Bumped(Bumped) => ("counter:bumped", 1),
            }
        }

        #[derive(Debug, thiserror::Error)]
        #[error("negative amount {0}")]
        struct Negative(i64);

        impl ToRejection for Negative {
            fn to_rejection(&self) -> Option<Rejection> {
                Some(
                    Rejection::new("negative", self.to_string())
                        .with_details(serde_json::json!({ "amount": self.0 })),
                )
            }
        }

        #[derive(Clone, Copy)]
        struct Bump(i64);

        impl OutcomeCommand for Bump {
            fn command_type(&self) -> String {
                "counter:bump".into()
            }

            fn canonical_payload(&self) -> Result<Option<Vec<u8>>, BoxError> {
                Ok(Some(crate::canonical_json(&self.0)?))
            }
        }

        impl Aggregate for Counter {
            const TYPE: &'static str = "counter";

            type Command = Bump;
            type Event = CounterEvent;
            type Error = Negative;
            type Services = ();

            async fn handle(
                &self,
                Bump(amount): Bump,
                _: &(),
            ) -> Result<Vec<CounterEvent>, Negative> {
                match amount {
                    0 => Ok(Vec::new()),
                    amount if amount < 0 => Err(Negative(amount)),
                    amount => Ok(vec![Bumped { amount }.into()]),
                }
            }

            fn apply(&mut self, event: CounterEvent) {
                match event {
                    CounterEvent::Bumped(bumped) => self.total += bumped.amount,
                }
            }
        }

        fn store() -> Store<Counter, MemRepository> {
            Store::new(MemRepository::new(), ())
                .with_snapshot_interval(0)
                .with_outcomes(OutcomeConfig::new())
        }

        fn command(id: &str) -> Metadata {
            Metadata::default().with_command_id(id)
        }

        /// A repository whose event reads skip sequence 1, as a load that raced a compaction (or
        /// one over expired events) sees the stream.
        struct Gapped<R>(R);

        impl<R: crate::Repository> crate::Repository for Gapped<R> {
            type Operation = R::Operation;

            fn writes_views(&self) -> bool {
                self.0.writes_views()
            }

            async fn commit(&self, commit: crate::Commit<R::Operation>) -> Result<(), Error> {
                self.0.commit(commit).await
            }

            async fn outcome(
                &self,
                command_id: &CommandId,
            ) -> Result<Option<crate::Outcome>, Error> {
                self.0.outcome(command_id).await
            }

            async fn events(&self, entity_id: &EntityId) -> Result<Vec<SerializedEvent>, Error> {
                let mut events = self.0.events(entity_id).await?;
                events.push(SerializedEvent {
                    id: EventId::for_event(entity_id, EventSequence(2)),
                    entity_id: entity_id.clone(),
                    sequence: EventSequence(2),
                    ..events.first().cloned().expect("a seeded event")
                });
                events.remove(0);

                Ok(events)
            }

            async fn events_after(
                &self,
                entity_id: &EntityId,
                after: EventSequence,
            ) -> Result<Vec<SerializedEvent>, Error> {
                self.0.events_after(entity_id, after).await
            }

            async fn latest(
                &self,
                entity_id: &EntityId,
                n: usize,
            ) -> Result<Vec<SerializedEvent>, Error> {
                self.0.latest(entity_id, n).await
            }

            async fn snapshot(
                &self,
                entity_id: &EntityId,
            ) -> Result<Option<crate::SerializedSnapshot>, Error> {
                self.0.snapshot(entity_id).await
            }

            async fn put_snapshot(
                &self,
                snapshot: &crate::SerializedSnapshot,
            ) -> Result<bool, Error> {
                self.0.put_snapshot(snapshot).await
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
                entity_type: Option<&crate::EntityType>,
            ) -> Result<std::collections::BTreeMap<EntityId, EventSequence>, Error> {
                self.0.entity_heads(entity_type).await
            }
        }

        #[tokio::test]
        async fn a_load_gap_is_retried_but_never_recorded_as_a_stale_fence() {
            let inner = MemRepository::new();
            Store::<Counter, _>::new(&inner, ())
                .execute("c-1", Bump(1), Metadata::default())
                .await
                .unwrap();

            let gapped = Gapped(&inner);
            let store = Store::<Counter, _>::new(&gapped, ())
                .with_snapshot_interval(0)
                .with_outcomes(OutcomeConfig::new().with_retry(RetryPolicy::immediate(3)));

            // The gap persists through every attempt: it is returned, not turned into a rejection,
            // because it says the read was inconsistent, not that the decision went stale.
            let error = store
                .execute_with_outcome("c-1", Bump(1), command("gap"))
                .await
                .unwrap_err();
            assert!(matches!(error, Error::SequenceGap { .. }), "{error:?}");
            assert!(
                inner
                    .outcome(&CommandId::new("gap"))
                    .await
                    .unwrap()
                    .is_none(),
                "no outcome is recorded for an inconsistent read"
            );
        }

        #[tokio::test]
        async fn an_error_a_prepare_step_makes_up_is_never_retried() {
            use std::sync::atomic::{AtomicUsize, Ordering};

            use crate::{CommandIdentity, Decision};

            let store = Store::<Counter, _>::new(MemRepository::new(), ())
                .with_snapshot_interval(0)
                .with_outcomes(OutcomeConfig::new().with_retry(RetryPolicy::immediate(5)));
            let identity = CommandIdentity::new("counter:check").with_targets(["c-1"]);

            // Whatever variant the step returns, an error it produced itself is its own failure:
            // the same `Conflict` or `Transient` a failed read would give is not retried, and no
            // rejection is recorded for it.
            type Made = fn() -> Error;
            let made: [(&str, Made); 2] = [
                ("conflict", || Error::Conflict {
                    entity_id: EntityId::new("c-1"),
                    message: "made up by the step".into(),
                }),
                ("transient", || {
                    Error::Transient("made up by the step".into())
                }),
            ];

            for (name, make) in made {
                let calls = AtomicUsize::new(0);

                let result = store
                    .execute_transaction(&identity, &command(name), |_| {
                        calls.fetch_add(1, Ordering::SeqCst);

                        Box::pin(async move { Err::<Decision, _>(make()) })
                    })
                    .await;

                assert!(
                    matches!(&result, Err(Error::Conflict { .. } | Error::Transient(_))),
                    "{name}: {result:?}"
                );
                assert_eq!(calls.load(Ordering::SeqCst), 1, "{name}: no re-prepare");
                assert!(
                    store
                        .repository()
                        .outcome(&CommandId::new(name))
                        .await
                        .unwrap()
                        .is_none(),
                    "{name}: nothing recorded"
                );
            }
        }

        #[tokio::test]
        async fn a_failed_read_a_prepare_step_passes_on_is_retried_but_never_recorded() {
            use std::sync::atomic::{AtomicUsize, Ordering};

            use crate::{CommandIdentity, Decision};

            let inner = MemRepository::new();
            Store::<Counter, _>::new(&inner, ())
                .execute("c-1", Bump(1), Metadata::default())
                .await
                .unwrap();

            let gapped = Gapped(&inner);
            let store = Store::<Counter, _>::new(&gapped, ())
                .with_snapshot_interval(0)
                .with_outcomes(OutcomeConfig::new().with_retry(RetryPolicy::immediate(3)));
            let identity = CommandIdentity::new("counter:check").with_targets(["c-1"]);
            let calls = AtomicUsize::new(0);

            // The step reads a stream whose load keeps finding a sequence gap and passes the error
            // on with `?`: it is retried until the attempts run out, then returned, never recorded.
            let result = store
                .execute_transaction(&identity, &command("read"), |tx| {
                    calls.fetch_add(1, Ordering::SeqCst);

                    Box::pin(async move {
                        tx.read::<Counter>("c-1").await?;

                        Ok(Decision::accept())
                    })
                })
                .await;

            assert!(
                matches!(&result, Err(Error::SequenceGap { .. })),
                "{result:?}"
            );
            assert_eq!(
                calls.load(Ordering::SeqCst),
                3,
                "retried like a load failure"
            );
            assert!(
                inner
                    .outcome(&CommandId::new("read"))
                    .await
                    .unwrap()
                    .is_none(),
                "a failed read says nothing about the decision going stale"
            );
        }

        #[tokio::test]
        async fn a_bad_custom_digest_is_a_programmer_error_before_anything_is_read() {
            let store: Store<Counter, MemRepository> = Store::new(MemRepository::new(), ())
                .with_outcomes(OutcomeConfig::new().with_digester(|_| Ok(String::new())));

            let executed = store
                .execute_with_outcome("c-1", Bump(1), command("bad"))
                .await;
            assert!(
                matches!(executed, Err(Error::InvalidArgument(_))),
                "{executed:?}"
            );

            let resolved = store
                .resolve_outcome("c-1", &Bump(1), &command("bad"))
                .await;
            assert!(
                matches!(resolved, Err(Error::InvalidArgument(_))),
                "{resolved:?}"
            );

            let failing: Store<Counter, MemRepository> = Store::new(MemRepository::new(), ())
                .with_outcomes(OutcomeConfig::new().with_digester(|_| Err("no digest".into())));
            assert!(matches!(
                failing
                    .execute_with_outcome("c-1", Bump(1), command("bad"))
                    .await,
                Err(Error::InvalidArgument(_))
            ));
            assert!(store.load("c-1").await.unwrap().sequence().0 == 0);
        }

        #[tokio::test]
        async fn a_zero_event_command_is_an_accepted_no_op_with_its_response() {
            let store = store();

            let first = store
                .execute_with_response("c-1", Bump(0), command("noop"), |counter, events| {
                    assert!(events.is_empty());

                    Ok(Some(format!("total={}", counter.total).into_bytes()))
                })
                .await
                .unwrap();
            assert!(first.is_accepted() && first.is_noop() && !first.replayed);
            assert_eq!(first.response(), Some(&b"total=0"[..]));
            assert!(first.events.is_empty() && first.typed_events.is_empty());

            // The retry answers from the stored row; the handler and the callback do not run.
            let again = store
                .execute_with_response("c-1", Bump(0), command("noop"), |_, _| {
                    panic!("the response callback must not run on a replay")
                })
                .await
                .unwrap();
            assert!(again.replayed);
            assert_eq!(again.outcome, first.outcome);
            assert_eq!(
                store
                    .repository()
                    .events(&"c-1".into())
                    .await
                    .unwrap()
                    .len(),
                0
            );
        }

        #[tokio::test]
        async fn the_response_sees_the_state_after_the_decided_events() {
            let store = store();

            let done = store
                .execute_with_response("c-1", Bump(5), command("bump"), |counter, events| {
                    assert_eq!(events, [CounterEvent::from(Bumped { amount: 5 })]);

                    Ok(Some(counter.total.to_string().into_bytes()))
                })
                .await
                .unwrap();

            assert_eq!(done.response(), Some(&b"5"[..]));
            assert_eq!(
                done.typed_events,
                vec![CounterEvent::from(Bumped { amount: 5 })]
            );
            assert_eq!(done.events.len(), 1);
            assert_eq!(
                done.events[0].metadata.commit_id, "bump",
                "the command ID is the commit ID"
            );
        }

        #[tokio::test]
        async fn a_domain_error_is_a_durable_rejection_and_an_internal_error_is_not() {
            let store = store();

            let rejected = store
                .execute_with_outcome("c-1", Bump(-3), command("refused"))
                .await
                .unwrap();
            assert!(rejected.is_rejected());
            let rejection = rejected.rejection().unwrap().clone();
            assert_eq!(rejection.code, "negative");
            assert_eq!(rejection.details, Some(serde_json::json!({ "amount": -3 })));

            let again = store
                .execute_with_outcome("c-1", Bump(-3), command("refused"))
                .await
                .unwrap();
            assert!(again.replayed);
            assert_eq!(again.rejection(), Some(&rejection));

            // A failing response callback is an error before any write: no outcome is recorded.
            let error = store
                .execute_with_response("c-2", Bump(1), command("broken"), |_, _| Err("boom".into()))
                .await
                .unwrap_err();
            assert!(matches!(error, Error::InvalidArgument(_)), "{error:?}");
            assert!(
                store
                    .repository()
                    .outcome(&"broken".into())
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                store
                    .repository()
                    .events(&"c-2".into())
                    .await
                    .unwrap()
                    .is_empty()
            );
        }

        #[tokio::test]
        async fn the_command_id_is_checked_before_anything_is_read() {
            let store = store();

            let missing = store
                .execute_with_outcome("c-1", Bump(1), Metadata::default())
                .await;
            assert!(
                matches!(missing, Err(Error::InvalidArgument(_))),
                "{missing:?}"
            );

            let long = store
                .execute_with_outcome("c-1", Bump(1), command(&"x".repeat(513)))
                .await;
            assert!(
                matches!(long, Err(Error::CommandIdTooLong { length: 513 })),
                "{long:?}"
            );

            assert!(
                store
                    .repository()
                    .events(&"c-1".into())
                    .await
                    .unwrap()
                    .is_empty()
            );
        }

        #[tokio::test]
        async fn a_store_without_outcomes_refuses_the_outcome_methods_and_vice_versa() {
            let plain = Store::<Counter, _>::new(MemRepository::new(), ());
            let result = plain
                .execute_with_outcome("c-1", Bump(1), command("c"))
                .await;
            assert!(matches!(result, Err(Error::InvalidConfig(_))), "{result:?}");

            let with = store();
            let executed = with.execute("c-1", Bump(1), command("c")).await;
            assert!(matches!(
                executed.unwrap_err().as_store_error(),
                Some(Error::InvalidConfig(_))
            ));
            let loaded = with.load("c-1").await.unwrap();
            let committed = with.commit(loaded, Vec::new(), &Metadata::default()).await;
            assert!(matches!(committed, Err(Error::InvalidConfig(_))));
        }

        #[tokio::test]
        async fn resolving_reads_the_row_without_running_anything() {
            let store = store();
            let metadata = command("resolve");

            assert!(
                store
                    .resolve_outcome("c-1", &Bump(2), &metadata)
                    .await
                    .unwrap()
                    .is_none()
            );

            store
                .execute_with_outcome("c-1", Bump(2), metadata.clone())
                .await
                .unwrap();

            let resolved = store
                .resolve_outcome("c-1", &Bump(2), &metadata)
                .await
                .unwrap()
                .unwrap();
            assert!(resolved.replayed && resolved.is_accepted());

            let reuse = store.resolve_outcome("c-1", &Bump(3), &metadata).await;
            assert!(
                matches!(reuse, Err(Error::IdempotencyKeyReuse { .. })),
                "{reuse:?}"
            );
        }

        #[tokio::test]
        async fn a_custom_digester_decides_identity() {
            // Only the command type is identity: another payload is the same command.
            let store = Store::<Counter, _>::new(MemRepository::new(), ())
                .with_snapshot_interval(0)
                .with_outcomes(
                    OutcomeConfig::new().with_digester(|input| Ok(input.command_type.clone())),
                );

            store
                .execute_with_outcome("c-1", Bump(1), command("c"))
                .await
                .unwrap();
            let repeat = store
                .execute_with_outcome("c-1", Bump(9), command("c"))
                .await
                .unwrap();

            assert!(repeat.replayed);
            assert_eq!(repeat.outcome.digest, "counter:bump");
        }

        #[tokio::test]
        async fn a_commit_that_never_lands_is_a_retryable_ambiguous_write_never_not_applied() {
            let fault = FaultInjectingRepository::new(MemRepository::new());

            // One attempt shows what a single attempt returns.
            let once = Store::<Counter, _>::new(&fault, ())
                .with_snapshot_interval(0)
                .with_outcomes(OutcomeConfig::new().with_retry(RetryPolicy::immediate(1)));

            fault.arm_before_apply();
            let error = once
                .execute_with_outcome("c-1", Bump(1), command("c"))
                .await
                .unwrap_err();
            assert!(
                matches!(error, Error::AmbiguousWrite { .. }) && error.is_retryable(),
                "an absent row never proves the commit did not apply: {error:?}"
            );
            assert!(
                fault
                    .inner()
                    .events(&"c-1".into())
                    .await
                    .unwrap()
                    .is_empty(),
                "a transient failure is returned as an error, never recorded as a rejection"
            );

            // With attempts to spare, the engine runs the command again on its own.
            let store = Store::<Counter, _>::new(&fault, ())
                .with_snapshot_interval(0)
                .with_outcomes(OutcomeConfig::new().with_retry(RetryPolicy::immediate(3)));

            fault.arm_before_apply();
            let done = store
                .execute_with_outcome("c-1", Bump(1), command("c"))
                .await
                .unwrap();
            assert!(
                done.is_accepted() && !done.replayed,
                "the second attempt commits"
            );
            assert_eq!(fault.inner().events(&"c-1".into()).await.unwrap().len(), 1);
        }

        #[tokio::test]
        async fn an_applied_ambiguous_write_returns_the_stored_outcome_without_rerunning() {
            let fault = FaultInjectingRepository::new(MemRepository::new());
            let store = Store::<Counter, _>::new(&fault, ())
                .with_snapshot_interval(0)
                .with_outcomes(OutcomeConfig::new());

            fault.arm_after_apply();
            let done = store
                .execute_with_outcome("c-1", Bump(4), command("c"))
                .await
                .unwrap();
            assert!(done.replayed && done.is_accepted());
            assert_eq!(fault.inner().events(&"c-1".into()).await.unwrap().len(), 1);

            // When the recovery read fails too, the original failure is reported, still retryable.
            // One attempt shows it; with attempts to spare the engine would run the command again
            // and find the row.
            let once = Store::<Counter, _>::new(&fault, ())
                .with_snapshot_interval(0)
                .with_outcomes(OutcomeConfig::new().with_retry(RetryPolicy::immediate(1)));
            fault.arm_after_apply();
            fault.fail_outcome_read_after(1);
            let error = once
                .execute_with_outcome("c-2", Bump(4), command("d"))
                .await
                .unwrap_err();
            assert!(
                matches!(&error, Error::AmbiguousWrite { .. }) && error.is_retryable(),
                "{error:?}"
            );
        }
    }
}
