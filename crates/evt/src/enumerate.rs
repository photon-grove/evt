//! Event-log enumeration: walk the whole log, or fold it into aggregates, lazily.

use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;

use crate::aggregate::Aggregate;
use crate::error::Error;
use crate::event::SerializedEvent;
use crate::ids::{EntityId, EntityType};
use crate::store::Loaded;

/// Narrows an enumeration. The default matches every entity.
///
/// Go's `StreamFilter` treats an empty entity type as "no filter". Here that is `None`, and
/// `Some("")` matches only entities stored with an empty type, as [`Repository::entity_heads`]
/// does.
///
/// [`Repository::entity_heads`]: crate::Repository::entity_heads
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamFilter {
    /// When set, only entities of this type match.
    pub entity_type: Option<EntityType>,
}

impl StreamFilter {
    /// A filter that matches every entity.
    pub fn all() -> Self {
        Self::default()
    }

    /// A filter that matches entities of one type.
    pub fn of_type(entity_type: impl Into<EntityType>) -> Self {
        Self {
            entity_type: Some(entity_type.into()),
        }
    }

    /// Reports whether an entity of the given type passes the filter.
    pub fn matches(&self, entity_type: &EntityType) -> bool {
        self.entity_type
            .as_ref()
            .is_none_or(|wanted| wanted == entity_type)
    }
}

/// A batch of events from [`EventLog::stream_all_events`], or the error that ended the stream.
pub type EventBatch = Result<Vec<SerializedEvent>, Error>;

/// A repository whose whole log can be enumerated.
///
/// Implemented by [`MemRepository`](crate::MemRepository),
/// [`DynamoRepository`](crate::dynamodb::DynamoRepository), and
/// [`PostgresRepository`](crate::postgres::PostgresRepository). It is a separate trait from
/// [`Repository`](crate::Repository) so existing implementations keep compiling.
///
/// [`EventLog::stream_all_events`] and [`EventLog::stream_entities`] are the Rust counterparts of
/// Go's `StreamAllEvents` and `StreamEntities`. They return [`futures_core::Stream`]s, so a rebuild
/// or a diagnostic tool can consume the log one batch at a time, apply backpressure by not polling,
/// and cancel by dropping the stream.
///
/// # Ordering
///
/// The guarantees match Go's, and the in-memory repository is stricter than Go's map iteration:
///
/// | Backend    | Batches of `stream_all_events`                                                  | Order                                             |
/// |------------|----------------------------------------------------------------------------------|---------------------------------------------------|
/// | PostgreSQL | One batch per entity, events ascending by sequence                               | Entities ascending by `entity_id` (collation)     |
/// | In memory  | One batch per entity, events ascending by sequence                               | Entities ascending by ID (bytewise)               |
/// | DynamoDB   | One batch per Scan page: any mix of entities, in no guaranteed order             | None. An entity's events can span pages           |
///
/// [`stream_entities`](EventLog::stream_entities) yields entities in the order the first of their
/// events was seen. Each entity folds its events in ascending sequence order, whatever order the
/// backend delivered them in.
///
/// # Memory
///
/// `stream_all_events` holds one page (DynamoDB), one entity's events (PostgreSQL, in memory), at
/// most. `stream_entities` has the same bound on PostgreSQL and in memory. On DynamoDB a Scan does
/// not deliver an entity's events together, so it buffers every matched event until the scan
/// completes, as Go does; use it for rebuilds and diagnostics, not request paths.
///
/// # Errors
///
/// A backend failure ends the stream: the stream yields the error as its last item. Nothing is
/// yielded by `stream_entities` after a failed DynamoDB scan, because the grouped events would be
/// incomplete. A failure to fold one entity (an unknown event type, a failed upcast, a sequence
/// gap) is yielded as an item and the stream goes on with the next entity.
///
/// # Reserved namespaces
///
/// Entities in the reserved [`SYSTEM#`](crate::SYSTEM_PREFIX) and
/// [`command-outcome#`](crate::COMMAND_OUTCOME_PREFIX) namespaces are never yielded; explicit reads
/// by ID still return them. Snapshots are not events and are never yielded.
///
/// # Cancellation
///
/// Dropping a stream cancels it. No lock is held between polls, and a PostgreSQL stream returns
/// its pooled connection when dropped.
pub trait EventLog: Send + Sync {
    /// Whether [`stream_all_events`](EventLog::stream_all_events) yields each entity's events
    /// whole, in one batch and in ascending sequence order. When it does,
    /// [`stream_entities`](EventLog::stream_entities) folds a batch at a time. Otherwise it groups
    /// every event by entity first.
    const BATCHES_BY_ENTITY: bool = false;

    /// Streams the event log in batches (see the trait documentation for what a batch
    /// holds on each backend). Batches are never empty. Reserved namespaces and snapshots are
    /// skipped.
    fn stream_all_events(&self, filter: StreamFilter) -> impl Stream<Item = EventBatch> + Send;

    /// Streams every entity folded from its events.
    ///
    /// The filter narrows to the aggregate's type, [`Aggregate::TYPE`], unless it names a type.
    /// Name one when stored events of another type should fold into `A`, for example after
    /// a rename.
    ///
    /// Entities fold from the event log alone, as in Go's `StreamEntities`: it does not read
    /// snapshots, so an entity whose early events were compacted fails with [`Error::SequenceGap`]
    /// instead of loading, and [`Store::load`](crate::Store::load) is the way to read it.
    fn stream_entities<A: Aggregate>(
        &self,
        filter: StreamFilter,
    ) -> impl Stream<Item = Result<Loaded<A>, Error>> + Send {
        let filter = StreamFilter {
            entity_type: filter.entity_type.or_else(|| Some(A::TYPE.into())),
        };
        let events: Batches<'_> = Box::pin(self.stream_all_events(filter));

        unfold(
            Entities {
                events,
                grouping: !Self::BATCHES_BY_ENTITY,
                group: Group::default(),
                ready: Vec::new().into_iter(),
                finished: false,
            },
            next_entity::<A>,
        )
    }
}

impl<L: EventLog + ?Sized> EventLog for &L {
    const BATCHES_BY_ENTITY: bool = L::BATCHES_BY_ENTITY;

    fn stream_all_events(&self, filter: StreamFilter) -> impl Stream<Item = EventBatch> + Send {
        (**self).stream_all_events(filter)
    }
}

impl<L: EventLog + ?Sized> EventLog for std::sync::Arc<L> {
    const BATCHES_BY_ENTITY: bool = L::BATCHES_BY_ENTITY;

    fn stream_all_events(&self, filter: StreamFilter) -> impl Stream<Item = EventBatch> + Send {
        (**self).stream_all_events(filter)
    }
}

type Batches<'a> = Pin<Box<dyn Stream<Item = EventBatch> + Send + 'a>>;

type Ready = std::vec::IntoIter<(EntityId, Vec<SerializedEvent>)>;

struct Entities<'a> {
    events: Batches<'a>,
    /// Whether events of one entity can arrive in several batches.
    grouping: bool,
    group: Group,
    ready: Ready,
    finished: bool,
}

/// Events grouped by entity, remembering which entity was seen first.
#[derive(Default)]
struct Group {
    order: Vec<EntityId>,
    events: HashMap<EntityId, Vec<SerializedEvent>>,
}

impl Group {
    fn add(&mut self, batch: Vec<SerializedEvent>) {
        for event in batch {
            if !self.events.contains_key(&event.entity_id) {
                self.order.push(event.entity_id.clone());
            }

            self.events
                .entry(event.entity_id.clone())
                .or_default()
                .push(event);
        }
    }

    fn take(&mut self) -> Ready {
        let mut events = std::mem::take(&mut self.events);

        std::mem::take(&mut self.order)
            .into_iter()
            .filter_map(|id| events.remove(&id).map(|events| (id, events)))
            .collect::<Vec<_>>()
            .into_iter()
    }
}

async fn next_batch(events: &mut Batches<'_>) -> Option<EventBatch> {
    poll_fn(|cx| events.as_mut().poll_next(cx)).await
}

async fn next_entity<A: Aggregate>(
    mut state: Entities<'_>,
) -> Option<(Result<Loaded<A>, Error>, Entities<'_>)> {
    loop {
        if let Some((id, events)) = state.ready.next() {
            return Some((fold::<A>(id, events), state));
        }

        if state.finished {
            return None;
        }

        match next_batch(&mut state.events).await {
            None => {
                state.finished = true;
                state.ready = state.group.take();
            }
            Some(Err(err)) => {
                // Grouped events of a failed scan are incomplete: surface the error alone.
                state.finished = true;
                state.group = Group::default();

                return Some((Err(err), state));
            }
            Some(Ok(batch)) => {
                state.group.add(batch);

                if !state.grouping {
                    state.ready = state.group.take();
                }
            }
        }
    }
}

fn fold<A: Aggregate>(id: EntityId, mut events: Vec<SerializedEvent>) -> Result<Loaded<A>, Error> {
    events.sort_by_key(|event| event.sequence);

    let mut loaded = Loaded::new(id);
    for event in events {
        loaded.replay(event)?;
    }

    Ok(loaded)
}

/// A stream built from a state and an async step, like `futures::stream::unfold`. Each step takes
/// the state and returns the next item with the state for the following step, or `None` to end.
/// Dropping the stream drops the step in flight and the state.
pub(crate) struct Unfold<S, F, Fut> {
    state: Option<S>,
    step: F,
    pending: Option<Pin<Box<Fut>>>,
}

pub(crate) fn unfold<S, T, F, Fut>(state: S, step: F) -> Unfold<S, F, Fut>
where
    F: FnMut(S) -> Fut,
    Fut: Future<Output = Option<(T, S)>>,
{
    Unfold {
        state: Some(state),
        step,
        pending: None,
    }
}

// The step future is boxed and the state is only moved, never pinned in place.
impl<S, F, Fut> Unpin for Unfold<S, F, Fut> {}

impl<S, T, F, Fut> Stream for Unfold<S, F, Fut>
where
    F: FnMut(S) -> Fut,
    Fut: Future<Output = Option<(T, S)>>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let this = self.get_mut();

        if this.pending.is_none() {
            let Some(state) = this.state.take() else {
                return Poll::Ready(None);
            };

            this.pending = Some(Box::pin((this.step)(state)));
        }

        let pending = this.pending.as_mut().expect("a step is in flight");
        let output = std::task::ready!(pending.as_mut().poll(cx));
        this.pending = None;

        match output {
            Some((item, state)) => {
                this.state = Some(state);

                Poll::Ready(Some(item))
            }
            None => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod tests;
