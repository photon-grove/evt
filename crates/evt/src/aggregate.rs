//! The [`Aggregate`] trait and in-band [`Projector`]s.

use std::future::Future;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::BoxError;
use crate::event::DomainEvent;
use crate::ids::EntityId;
use crate::upcast::Upcaster;

/// An event-sourced aggregate: state rebuilt by folding its events.
///
/// Commands go through [`handle`](Aggregate::handle), which decides and returns events without
/// changing state. [`apply`](Aggregate::apply) folds each event into state and never fails:
/// validation belongs in `handle`, and an unknown stored event fails replay before `apply` runs.
///
/// The aggregate's serde encoding is its snapshot format. Annotate the struct with
/// `#[serde(default)]` so snapshots written before a field existed still load.
///
/// ```
/// use photon_evt::{Aggregate, domain_events};
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
/// #[serde(rename_all = "camelCase")]
/// pub struct Deposited {
///     pub amount_cents: i64,
/// }
///
/// domain_events! {
///     #[derive(Clone, Debug, PartialEq)]
///     pub enum CounterEvent {
///         Deposited(Deposited) => ("counter:deposited", 1),
///     }
/// }
///
/// #[derive(Debug, thiserror::Error)]
/// #[error("amount must be positive")]
/// pub struct InvalidAmount;
///
/// #[derive(Debug, Default, Serialize, Deserialize)]
/// #[serde(default, rename_all = "camelCase")]
/// pub struct Counter {
///     pub balance_cents: i64,
/// }
///
/// impl Aggregate for Counter {
///     const TYPE: &'static str = "counter";
///     type Command = i64;
///     type Event = CounterEvent;
///     type Error = InvalidAmount;
///     type Services = ();
///
///     async fn handle(&self, amount: i64, _: &()) -> Result<Vec<CounterEvent>, InvalidAmount> {
///         if amount <= 0 {
///             return Err(InvalidAmount);
///         }
///         Ok(vec![Deposited { amount_cents: amount }.into()])
///     }
///
///     fn apply(&mut self, event: CounterEvent) {
///         match event {
///             CounterEvent::Deposited(e) => self.balance_cents += e.amount_cents,
///         }
///     }
/// }
/// ```
pub trait Aggregate: Default + Serialize + DeserializeOwned + Send + Sync + 'static {
    /// The stored entity type, such as `account`.
    const TYPE: &'static str;

    /// This aggregate type's own snapshot interval: the number of events between snapshots, with
    /// `Some(0)` disabling snapshots for the type. `None` (the default) uses the interval of the
    /// [`Store`](crate::Store) or [`Transaction`](crate::Transaction) that commits it.
    ///
    /// When set, it overrides that interval wherever this type is committed, including streams of
    /// it appended in a transaction of another store, as Go's per-type
    /// `snapshots.Store.WithSnapshotSize` overrides the store's default size.
    const SNAPSHOT_INTERVAL: Option<usize> = None;

    /// The commands this aggregate handles. Usually an enum.
    type Command: Send;

    /// The events this aggregate emits. Usually declared with [`domain_events!`](crate::domain_events).
    type Event: DomainEvent + Clone;

    /// The domain error returned when a command is rejected.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Dependencies available to command handlers (clients, clocks, configuration). Use `()` when
    /// there are none.
    type Services: Send + Sync;

    /// Decides which events a command produces. It must not change state.
    fn handle(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> impl Future<Output = Result<Vec<Self::Event>, Self::Error>> + Send;

    /// Folds one event into state.
    fn apply(&mut self, event: Self::Event);

    /// Events to commit with a rejection, in the same atomic commit as the rejected outcome row
    /// (see [`Store::execute_with_outcome`](crate::Store::execute_with_outcome)).
    ///
    /// It runs on a store with outcomes enabled when [`handle`](Aggregate::handle) returned an
    /// `error` that maps to a rejection ([`ToRejection`](crate::ToRejection)), with the state the
    /// command was decided on. The default is no events: a plain rejection, which writes none. A
    /// domain that must record what happened beside the refusal, such as a proposal whose apply
    /// fails recording an `apply-failed` event, returns it here, decided from the state and the
    /// error (carry what the event needs in the error).
    ///
    /// The events are committed on this aggregate's stream under the rules of an accepted commit:
    /// they are applied to the state, snapshotted, and projected, and the outcome row lists them.
    /// A conflict re-prepares the whole command, so this runs again on the fresh state; when
    /// retries run out the engine records the stale-fence rejection without them. A replay returns
    /// the stored rejection and never commits them again. It must not change state or fail.
    fn rejection_events(&self, error: &Self::Error) -> Vec<Self::Event> {
        let _ = error;

        Vec::new()
    }

    /// Upcasters for stored events of this aggregate, run in order before decoding.
    fn upcasters() -> &'static [&'static dyn Upcaster] {
        &[]
    }
}

/// Produces backend operations that commit atomically with an aggregate's new events: for
/// example, a DynamoDB view row written in the same transaction.
///
/// `Op` is the repository's [`Operation`](crate::Repository::Operation) type, so a projector built
/// for one backend cannot be registered on a store for another.
pub trait Projector<A: Aggregate, Op>: Send + Sync {
    /// Returns the operations for the committed events. `aggregate` already reflects them.
    fn project(
        &self,
        entity_id: &EntityId,
        aggregate: &A,
        events: &[A::Event],
    ) -> Result<Vec<Op>, BoxError>;
}
