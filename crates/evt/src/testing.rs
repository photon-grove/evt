//! Given/when/then test utilities over [`MemRepository`], the Rust counterpart of Go's `test`
//! package.
//!
//! A [`Scenario`] seeds one stream with prior events, runs a command through a real [`Store`], and
//! asserts on the emitted events, the rejection, or the resulting state. A [`Fixture`] is the
//! lower-level piece for flows that span streams: seed several aggregates, drive a
//! [`Transaction`], then assert on what each stream holds.
//!
//! Enable the `testing` feature in `dev-dependencies`:
//!
//! ```toml
//! [dev-dependencies]
//! photon-evt = { version = "0.1", features = ["testing"] }
//! ```
//!
//! ```
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! use photon_evt::conformance::{Add, Added, InvalidAmount, Ledger};
//! use photon_evt::testing::Scenario;
//!
//! // Given a ledger with one addition, when 5 is added, then one `Added` event is emitted.
//! Scenario::<Ledger>::given([Added { amount: 2 }.into()])
//!     .when(Add(5))
//!     .then_events([Added { amount: 5 }.into()])
//!     .await;
//!
//! // A rejected command emits nothing.
//! Scenario::<Ledger>::given([])
//!     .when(Add(0))
//!     .then_rejected(|error| matches!(error, InvalidAmount))
//!     .await;
//! # });
//! ```
//!
//! Assertion failures panic with a message naming the expected and actual values, so a failing
//! scenario fails the surrounding `#[test]` or `#[tokio::test]`.

use std::fmt::Debug;

use crate::aggregate::Aggregate;
use crate::error::{Error, ExecuteError};
use crate::event::{DomainEvent, SerializedEvent};
use crate::ids::{EntityId, EventSequence};
use crate::mem::MemRepository;
use crate::metadata::Metadata;
use crate::repository::{Commit, Repository};
use crate::store::{Executed, Loaded, Store, load_aggregate, serialize_events};
use crate::transaction::Transaction;
use crate::upcast::upcast_event;

/// A [`MemRepository`] with helpers to seed and inspect streams of any aggregate type.
///
/// Clones share storage, like clones of the repository. Use [`Fixture::transaction`] to run a
/// multi-stream [`Transaction`] over the same data a [`Scenario`] sees.
///
/// ```
/// # tokio::runtime::Runtime::new().unwrap().block_on(async {
/// use photon_evt::conformance::{Add, Added, Claim, ClaimEvent, Ledger, Taken};
/// use photon_evt::Metadata;
/// use photon_evt::testing::Fixture;
///
/// let fixture = Fixture::new();
/// fixture.given::<Ledger>("ledger-1", [Added { amount: 1 }.into()]).await;
///
/// // One transaction over two aggregate types.
/// let mut tx = fixture.transaction();
/// let mut ledger = tx.read::<Ledger>("ledger-1").await.unwrap();
/// let mut claim = tx.read::<Claim>("claim-1").await.unwrap();
/// tx.append(&mut ledger, vec![Added { amount: 2 }.into()]).unwrap();
/// tx.append(&mut claim, vec![Taken { holder: "ada".into() }.into()]).unwrap();
/// tx.commit(&Metadata::default()).await.unwrap();
///
/// assert_eq!(fixture.events::<Ledger>("ledger-1").await.len(), 2);
/// assert_eq!(
///     fixture.events::<Claim>("claim-1").await,
///     [ClaimEvent::from(Taken { holder: "ada".into() })]
/// );
/// # });
/// ```
#[derive(Clone, Debug, Default)]
pub struct Fixture {
    repository: MemRepository,
}

impl Fixture {
    /// Creates a fixture over an empty repository.
    pub fn new() -> Self {
        Self::default()
    }

    /// The underlying repository, to hand to stores and projectors under test.
    pub fn repository(&self) -> &MemRepository {
        &self.repository
    }

    /// A store of `A` over the fixture's repository.
    pub fn store<A: Aggregate>(&self, services: A::Services) -> Store<A, MemRepository> {
        Store::new(self.repository.clone(), services)
    }

    /// Begins a multi-stream transaction over the fixture's repository.
    pub fn transaction(&self) -> Transaction<'_, MemRepository> {
        Transaction::new(&self.repository)
    }

    /// Appends events to a stream as if an earlier command had committed them. Called on a stream
    /// that already has events, it continues the sequence, which also stands in for a concurrent
    /// writer moving a stream between a transaction's read and its commit.
    ///
    /// # Panics
    ///
    /// Panics if the events cannot be serialized or stored.
    pub async fn given<A: Aggregate>(
        &self,
        entity_id: impl Into<EntityId>,
        events: impl IntoIterator<Item = A::Event>,
    ) {
        let entity_id = entity_id.into();
        let events: Vec<A::Event> = events.into_iter().collect();
        if events.is_empty() {
            return;
        }

        let head = self
            .repository
            .events(&entity_id)
            .await
            .unwrap_or_else(|error| panic!("reading {entity_id}: {error}"))
            .last()
            .map_or(EventSequence::ZERO, |event| event.sequence);

        let metadata = Metadata {
            commit_id: format!("given-{entity_id}-{}", head.next()),
            ..Metadata::default()
        };

        let serialized = serialize_events::<A>(&entity_id, head, &events, &metadata)
            .unwrap_or_else(|error| panic!("serializing events for {entity_id}: {error}"));

        self.repository
            .commit(Commit::new(entity_id.clone(), serialized))
            .await
            .unwrap_or_else(|error| panic!("seeding {entity_id}: {error}"));
    }

    /// Every stored event of a stream, upcast and decoded, oldest first.
    ///
    /// # Panics
    ///
    /// Panics if the stream cannot be read or an event does not decode as `A::Event`.
    pub async fn events<A: Aggregate>(&self, entity_id: impl Into<EntityId>) -> Vec<A::Event> {
        let entity_id = entity_id.into();
        let stored = self
            .repository
            .events(&entity_id)
            .await
            .unwrap_or_else(|error| panic!("reading {entity_id}: {error}"));

        decode_events::<A>(stored)
    }

    /// The aggregate loaded from a stream, snapshot-aware like [`Store::load`].
    ///
    /// # Panics
    ///
    /// Panics if the load fails.
    pub async fn load<A: Aggregate>(&self, entity_id: impl Into<EntityId>) -> Loaded<A> {
        let entity_id = entity_id.into();

        load_aggregate::<A, _>(&self.repository, entity_id.clone())
            .await
            .unwrap_or_else(|error| panic!("loading {entity_id}: {error}"))
    }

    /// Asserts that a stream holds exactly these events.
    ///
    /// # Panics
    ///
    /// Panics when the stored events differ.
    pub async fn assert_events<A>(
        &self,
        entity_id: impl Into<EntityId>,
        expected: impl IntoIterator<Item = A::Event>,
    ) where
        A: Aggregate,
        A::Event: PartialEq + Debug,
    {
        let entity_id = entity_id.into();
        let expected: Vec<A::Event> = expected.into_iter().collect();
        let actual = self.events::<A>(entity_id.clone()).await;

        assert_eq!(actual, expected, "events of stream {entity_id}");
    }
}

/// Upcasts and decodes stored events as `A::Event`.
fn decode_events<A: Aggregate>(stored: Vec<SerializedEvent>) -> Vec<A::Event> {
    stored
        .into_iter()
        .map(|event| {
            let event = upcast_event(A::upcasters(), event)
                .unwrap_or_else(|failure| panic!("upcasting a stored event: {}", failure.source));

            A::Event::from_payload(event.event_type.as_str(), event.version, &event.payload)
                .unwrap_or_else(|error| {
                    panic!("decoding {} as {}: {error}", event.event_type, A::TYPE)
                })
        })
        .collect()
}

/// A given/when/then test of one command against one stream.
///
/// `given` seeds prior events, [`when`](Scenario::when) names the command, and a `then_*` method
/// runs it through a [`Store`] over a [`MemRepository`] and asserts the outcome. Use
/// [`run`](When::run) for an [`Outcome`] to inspect further.
///
/// The stream defaults to the entity ID `entity-1`; change it with [`entity`](Scenario::entity).
/// Aggregates with non-default [`Services`](Aggregate::Services) use
/// [`with_services`](Scenario::with_services).
pub struct Scenario<A: Aggregate> {
    fixture: Fixture,
    entity_id: EntityId,
    services: A::Services,
    given: Vec<A::Event>,
    metadata: Metadata,
}

impl<A: Aggregate> Scenario<A>
where
    A::Services: Default,
{
    /// Starts a scenario whose stream already holds `events`, with default services.
    pub fn given(events: impl IntoIterator<Item = A::Event>) -> Self {
        Self::with_services(A::Services::default(), events)
    }
}

impl<A: Aggregate> Scenario<A> {
    /// Starts a scenario with explicit services (clocks, clients) and prior events.
    pub fn with_services(
        services: A::Services,
        events: impl IntoIterator<Item = A::Event>,
    ) -> Self {
        Self {
            fixture: Fixture::new(),
            entity_id: EntityId::new("entity-1"),
            services,
            given: events.into_iter().collect(),
            metadata: Metadata::default(),
        }
    }

    /// Sets the stream's entity ID.
    #[must_use]
    pub fn entity(mut self, entity_id: impl Into<EntityId>) -> Self {
        self.entity_id = entity_id.into();
        self
    }

    /// Sets the metadata the command executes with, such as a command ID to test duplicates.
    #[must_use]
    pub fn metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Runs against an existing fixture, so other streams it holds are visible to the test and
    /// several scenarios can build on each other.
    #[must_use]
    pub fn on(mut self, fixture: &Fixture) -> Self {
        self.fixture = fixture.clone();
        self
    }

    /// Names the command to execute.
    pub fn when(self, command: A::Command) -> When<A> {
        When {
            scenario: self,
            command,
        }
    }
}

/// A [`Scenario`] with its command chosen, ready to run.
pub struct When<A: Aggregate> {
    scenario: Scenario<A>,
    command: A::Command,
}

impl<A: Aggregate> When<A> {
    /// Seeds the given events, executes the command, and returns the [`Outcome`].
    ///
    /// # Panics
    ///
    /// Panics if seeding the given events fails.
    pub async fn run(self) -> Outcome<A> {
        let Scenario {
            fixture,
            entity_id,
            services,
            given,
            metadata,
        } = self.scenario;

        fixture.given::<A>(entity_id.clone(), given).await;

        let store = fixture.store::<A>(services);
        let result = store
            .execute(entity_id.clone(), self.command, metadata)
            .await;

        Outcome {
            fixture,
            entity_id,
            result,
        }
    }

    /// Asserts that the command succeeds and emits exactly these events.
    ///
    /// # Panics
    ///
    /// Panics if the command fails or emits different events.
    pub async fn then_events(self, expected: impl IntoIterator<Item = A::Event>)
    where
        A::Event: PartialEq + Debug,
    {
        self.run().await.assert_events(expected);
    }

    /// Asserts that the command succeeds and emits nothing.
    ///
    /// # Panics
    ///
    /// Panics if the command fails or emits events.
    pub async fn then_no_events(self)
    where
        A::Event: PartialEq + Debug,
    {
        self.run().await.assert_events([]);
    }

    /// Asserts that the aggregate rejects the command with an error `matches` accepts.
    ///
    /// # Panics
    ///
    /// Panics if the command succeeds, fails in the store, or `matches` returns false.
    pub async fn then_rejected(self, matches: impl FnOnce(&A::Error) -> bool) {
        self.run().await.assert_rejected(matches);
    }

    /// Asserts that the store refuses the command (a conflict or duplicate command, for example)
    /// with an [`Error`] `matches` accepts.
    ///
    /// # Panics
    ///
    /// Panics if the command succeeds, is rejected by the aggregate, or `matches` returns false.
    pub async fn then_error(self, matches: impl FnOnce(&Error) -> bool) {
        self.run().await.assert_error(matches);
    }

    /// Asserts on the aggregate state after the command succeeds.
    ///
    /// # Panics
    ///
    /// Panics if the command fails or `check` panics.
    pub async fn then_state(self, check: impl FnOnce(&A)) {
        let outcome = self.run().await;
        outcome.assert_succeeded();
        check(outcome.state().aggregate());
    }
}

/// The result of running a [`Scenario`].
pub struct Outcome<A: Aggregate> {
    fixture: Fixture,
    entity_id: EntityId,
    result: Result<Executed<A>, ExecuteError<A::Error>>,
}

impl<A: Aggregate> Outcome<A> {
    /// The raw result of [`Store::execute`].
    pub fn result(&self) -> &Result<Executed<A>, ExecuteError<A::Error>> {
        &self.result
    }

    /// The fixture the scenario ran on, to inspect other streams.
    pub fn fixture(&self) -> &Fixture {
        &self.fixture
    }

    /// The events as stored, if the command succeeded.
    pub fn stored_events(&self) -> Option<&[SerializedEvent]> {
        self.result
            .as_ref()
            .ok()
            .map(|executed| &executed.events[..])
    }

    /// The aggregate state after the command.
    ///
    /// # Panics
    ///
    /// Panics if the command failed.
    pub fn state(&self) -> &Loaded<A> {
        &self.assert_succeeded().state
    }

    /// Asserts that the command succeeded and returns what it did.
    ///
    /// # Panics
    ///
    /// Panics with the failure if it did not.
    pub fn assert_succeeded(&self) -> &Executed<A> {
        match &self.result {
            Ok(executed) => executed,
            Err(error) => panic!("expected {} command to succeed, got: {error:?}", A::TYPE),
        }
    }

    /// Asserts that the command emitted exactly these events.
    ///
    /// # Panics
    ///
    /// Panics if the command failed or emitted different events.
    pub fn assert_events(&self, expected: impl IntoIterator<Item = A::Event>)
    where
        A::Event: PartialEq + Debug,
    {
        let executed = self.assert_succeeded();
        let actual = decode_events::<A>(executed.events.clone());
        let expected: Vec<A::Event> = expected.into_iter().collect();

        assert_eq!(
            actual,
            expected,
            "events emitted for {} {}",
            A::TYPE,
            self.entity_id
        );
    }

    /// Asserts that the aggregate rejected the command with an error `matches` accepts.
    ///
    /// # Panics
    ///
    /// Panics on success, on a store error, or when `matches` returns false.
    pub fn assert_rejected(&self, matches: impl FnOnce(&A::Error) -> bool) {
        match &self.result {
            Err(ExecuteError::Rejected(error)) => {
                assert!(matches(error), "unexpected rejection: {error:?}");
            }
            Err(other) => panic!("expected a rejection, got a store failure: {other:?}"),
            Ok(executed) => panic!("expected a rejection, got events: {:?}", executed.events),
        }
    }

    /// Asserts that the store failed with an [`Error`] `matches` accepts.
    ///
    /// # Panics
    ///
    /// Panics on success, on a rejection, or when `matches` returns false.
    pub fn assert_error(&self, matches: impl FnOnce(&Error) -> bool) {
        match &self.result {
            Err(ExecuteError::Store(error)) => {
                assert!(matches(error), "unexpected store error: {error:?}");
            }
            Err(other) => panic!("expected a store error, got a rejection: {other:?}"),
            Ok(executed) => panic!("expected a store error, got events: {:?}", executed.events),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conformance::{
        Add, Added, AlreadyHeld, Claim, ClaimEvent, InvalidAmount, Ledger, LedgerEvent, Take, Taken,
    };

    fn added(amount: i64) -> LedgerEvent {
        Added { amount }.into()
    }

    #[tokio::test]
    async fn emits_events_after_the_given_history() {
        Scenario::<Ledger>::given([added(2), added(3)])
            .when(Add(5))
            .then_events([added(5)])
            .await;
    }

    #[tokio::test]
    async fn state_reflects_given_and_new_events() {
        Scenario::<Ledger>::given([added(2)])
            .when(Add(5))
            .then_state(|ledger| {
                assert_eq!(ledger.total, 7);
                assert_eq!(ledger.entries, 2);
            })
            .await;
    }

    #[tokio::test]
    async fn rejections_emit_nothing() {
        let outcome = Scenario::<Ledger>::given([]).when(Add(-1)).run().await;

        outcome.assert_rejected(|error| matches!(error, InvalidAmount));
        assert!(outcome.stored_events().is_none());
        assert!(
            outcome
                .fixture()
                .events::<Ledger>("entity-1")
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_repeated_command_id_is_a_store_error() {
        let fixture = Fixture::new();
        let metadata = Metadata::default().with_command_id("cmd-1");

        Scenario::<Ledger>::given([])
            .on(&fixture)
            .metadata(metadata.clone())
            .when(Add(1))
            .then_events([added(1)])
            .await;

        Scenario::<Ledger>::given([])
            .on(&fixture)
            .metadata(metadata)
            .when(Add(1))
            .then_error(Error::is_duplicate_command)
            .await;
    }

    #[tokio::test]
    #[should_panic(expected = "events emitted for ledger entity-1")]
    async fn mismatched_events_fail_the_test() {
        Scenario::<Ledger>::given([])
            .when(Add(1))
            .then_events([added(2)])
            .await;
    }

    #[tokio::test]
    #[should_panic(expected = "expected a rejection")]
    async fn expecting_a_rejection_that_does_not_happen_fails() {
        Scenario::<Ledger>::given([])
            .when(Add(1))
            .then_rejected(|_| true)
            .await;
    }

    #[tokio::test]
    async fn the_entity_id_is_configurable() {
        let outcome = Scenario::<Ledger>::given([added(1)])
            .entity("ledger-9")
            .when(Add(1))
            .run()
            .await;

        outcome.assert_succeeded();
        assert_eq!(
            outcome.fixture().events::<Ledger>("ledger-9").await.len(),
            2
        );
    }

    #[tokio::test]
    async fn fixtures_span_aggregate_types_in_one_transaction() {
        let fixture = Fixture::new();
        fixture.given::<Ledger>("ledger-1", [added(1)]).await;

        let mut tx = fixture.transaction();
        let mut ledger = tx.read::<Ledger>("ledger-1").await.unwrap();
        let mut claim = tx.read::<Claim>("claim-1").await.unwrap();
        tx.append(&mut ledger, vec![added(2)]).unwrap();
        tx.append(
            &mut claim,
            vec![
                Taken {
                    holder: "ada".into(),
                }
                .into(),
            ],
        )
        .unwrap();
        tx.commit(&Metadata::default()).await.unwrap();

        fixture
            .assert_events::<Ledger>("ledger-1", [added(1), added(2)])
            .await;
        fixture
            .assert_events::<Claim>(
                "claim-1",
                [ClaimEvent::from(Taken {
                    holder: "ada".into(),
                })],
            )
            .await;
        assert_eq!(
            fixture.load::<Ledger>("ledger-1").await.aggregate().total,
            3
        );
    }

    #[tokio::test]
    async fn a_concurrent_writer_fails_a_fenced_read() {
        let fixture = Fixture::new();
        fixture.given::<Claim>("claim-1", []).await;
        fixture.given::<Ledger>("ledger-1", [added(1)]).await;

        let mut tx = fixture.transaction();
        let mut claim = tx.read::<Claim>("claim-1").await.unwrap();
        tx.read::<Ledger>("ledger-1").await.unwrap();
        tx.append(
            &mut claim,
            vec![
                Taken {
                    holder: "ada".into(),
                }
                .into(),
            ],
        )
        .unwrap();

        // The ledger was read but not written, so it is fenced. Move it before the commit.
        fixture.given::<Ledger>("ledger-1", [added(1)]).await;

        let error = tx.commit(&Metadata::default()).await.unwrap_err();
        assert!(error.is_conflict(), "{error:?}");
        assert!(fixture.events::<Claim>("claim-1").await.is_empty());
    }

    #[tokio::test]
    async fn claims_reject_a_second_holder() {
        Scenario::<Claim>::given([Taken {
            holder: "ada".into(),
        }
        .into()])
        .when(Take("grace".into()))
        .then_rejected(|error| matches!(error, AlreadyHeld))
        .await;
    }
}
