//! The journal's configuration: which entity types it covers, how an event maps to scopes, the
//! row-size cap, and the contention retry policy. Allocation is a pure function of the counters it
//! is given.

use std::collections::BTreeMap;
use std::sync::Arc;

use super::row::row_size;
use super::scope::{Allocation, Scope, set_allocations};
use crate::aggregate::Aggregate;
use crate::error::{BoxError, Error, RowTooLarge};
use crate::event::SerializedEvent;
use crate::ids::{EntityId, EntityType, EventSequence};
use crate::metadata::Metadata;
use crate::retry::RetryPolicy;
use crate::store::{serialize_events, stamp_commit_id};

/// The default row-size cap in bytes: a page of 100 rows stays under 3 MB, well inside a
/// Lambda response. (A page may hold up to [`MAX_PAGE_ROWS`](super::MAX_PAGE_ROWS) rows; size the
/// page for your response limit, not the cap alone.) Set another with [`Journal::with_max_row_bytes`].
pub const DEFAULT_MAX_ROW_BYTES: usize = 30_000;

/// The largest row-size cap [`Journal::with_max_row_bytes`] accepts: the row's payload plus its keys
/// and attribute names must stay inside the commit budget's single-item limit (380,000 bytes, a
/// 5% margin under DynamoDB's 400 KB item limit; see the storage format, "Writes").
pub const MAX_ROW_BYTES: usize = 370_000;

/// The default contention policy: five attempts, starting at 25 ms (see [`RetryPolicy`]).
pub const DEFAULT_RETRY: RetryPolicy = RetryPolicy::new(crate::retry::DEFAULT_MAX_ATTEMPTS);

/// Maps an event to the journal scopes it is delivered through, such as a world scope and a place
/// scope for an event that happened in a place.
///
/// The mapping must be a **pure function of the event** (its type, payload, and metadata other
/// than the journal's own allocation): no clock, no randomness, no lookup. It is called only for
/// events of the journaled entity types, when a commit stages them. Its result is then written
/// into the event's metadata, and everything after that, including a rebuild, reads the
/// allocation back from the stored event and never calls the resolver again. Changing the
/// resolver therefore changes where *future* events go and never rewrites history.
///
/// Return no scope to leave an event out of the journal. Duplicates are collapsed.
pub trait ScopeResolver: Send + Sync {
    /// The scopes of one event.
    fn scopes(&self, event: &SerializedEvent) -> Result<Vec<Scope>, BoxError>;
}

/// What a scope-resolver closure may return: the scopes, or a `Result` of them whose error becomes
/// the failure of the commit (nothing is written).
pub trait IntoScopes {
    /// The scopes, or the resolver's error.
    fn into_scopes(self) -> Result<Vec<Scope>, BoxError>;
}

impl IntoScopes for Vec<Scope> {
    fn into_scopes(self) -> Result<Vec<Scope>, BoxError> {
        Ok(self)
    }
}

impl<E: Into<BoxError>> IntoScopes for Result<Vec<Scope>, E> {
    fn into_scopes(self) -> Result<Vec<Scope>, BoxError> {
        self.map_err(Into::into)
    }
}

/// A closure over the event is a resolver. It returns the scopes, or a `Result` of them so it can
/// use `?` (for example on [`Scope::new`]); an error is [`Error::InvalidJournal`] at commit time,
/// before anything is written.
impl<F, T> ScopeResolver for F
where
    F: Fn(&SerializedEvent) -> T + Send + Sync,
    T: IntoScopes,
{
    fn scopes(&self, event: &SerializedEvent) -> Result<Vec<Scope>, BoxError> {
        self(event).into_scopes()
    }
}

/// The journal's configuration, shared by the [`JournalRepository`](super::JournalRepository)
/// that writes it and by the [`JournalProjector`](super::JournalProjector) that derives its rows.
///
/// ```
/// use photon_evt::journal::{Journal, Scope};
///
/// // Every `ledger` event is journaled into one scope per ledger.
/// let journal = Journal::new(["ledger"], |event: &photon_evt::SerializedEvent| {
///     vec![Scope::new(format!("ledger:{}", event.entity_id)).unwrap()]
/// })
/// .unwrap()
/// .with_max_row_bytes(30_000)
/// .unwrap();
///
/// assert_eq!(journal.max_row_bytes(), 30_000);
/// ```
#[derive(Clone)]
pub struct Journal {
    entity_types: Arc<[EntityType]>,
    resolver: Arc<dyn ScopeResolver>,
    max_row_bytes: usize,
    pre_check_headroom: usize,
    retry: RetryPolicy,
}

impl std::fmt::Debug for Journal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Journal")
            .field("entity_types", &self.entity_types)
            .field("max_row_bytes", &self.max_row_bytes)
            .field("retry", &self.retry)
            .finish_non_exhaustive()
    }
}

impl Journal {
    /// A journal over the events of the given entity types, delivered to the scopes `resolver`
    /// names. The entity types are the journal's **history**: its rows are rebuilt from their
    /// events, so the first journaled commit durably declares them history-dependent and
    /// compaction is refused for them in every process ([`Repository::compact_below`]).
    ///
    /// At least one type is required; an empty list is [`Error::InvalidJournal`].
    ///
    /// [`Repository::compact_below`]: crate::Repository::compact_below
    pub fn new<T>(
        entity_types: impl IntoIterator<Item = T>,
        resolver: impl ScopeResolver + 'static,
    ) -> Result<Self, Error>
    where
        T: Into<EntityType>,
    {
        let mut entity_types: Vec<EntityType> = entity_types.into_iter().map(Into::into).collect();
        entity_types.sort();
        entity_types.dedup();

        if entity_types.is_empty() {
            return Err(Error::InvalidJournal(
                "a journal covers at least one entity type".into(),
            ));
        }

        Ok(Self {
            entity_types: entity_types.into(),
            resolver: Arc::new(resolver),
            max_row_bytes: DEFAULT_MAX_ROW_BYTES,
            pre_check_headroom: 0,
            retry: DEFAULT_RETRY,
        })
    }

    /// Sets the row-size cap in bytes (default [`DEFAULT_MAX_ROW_BYTES`]). A row whose stored
    /// payload is longer is refused before any write with [`Error::JournalRowTooLarge`]. A cap of
    /// zero refuses every row. A cap over [`MAX_ROW_BYTES`] is refused here, because a row that
    /// large would pass the guard and then fail the DynamoDB item limit late, as a projector
    /// failure.
    pub fn with_max_row_bytes(mut self, max_row_bytes: usize) -> Result<Self, Error> {
        if max_row_bytes > MAX_ROW_BYTES {
            return Err(Error::InvalidJournal(format!(
                "a row cap is at most {MAX_ROW_BYTES} bytes; got {max_row_bytes}"
            )));
        }

        self.max_row_bytes = max_row_bytes;

        Ok(self)
    }

    /// Makes the [pre-check](Journal::pre_check) measure `bytes` wider than the row it computes,
    /// as a margin for what the handler cannot see of the commit (the principal, extensions, and
    /// command ID the commit's metadata will carry, if the handler's copy differs). The
    /// commit-time guard is not affected. Default 0.
    #[must_use]
    pub fn with_pre_check_headroom(mut self, bytes: usize) -> Self {
        self.pre_check_headroom = bytes;
        self
    }

    /// Sets the contention policy: how many times a journaled commit is attempted when another
    /// writer takes its positions first, and how long it waits between attempts (default
    /// [`DEFAULT_RETRY`]). A deadline in the policy bounds the whole commit.
    #[must_use]
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// The row-size cap in bytes.
    pub fn max_row_bytes(&self) -> usize {
        self.max_row_bytes
    }

    /// The contention policy.
    pub fn retry(&self) -> &RetryPolicy {
        &self.retry
    }

    /// The journaled entity types, sorted.
    pub fn entity_types(&self) -> &[EntityType] {
        &self.entity_types
    }

    /// Reports whether the journal covers events of this entity type.
    pub fn covers(&self, entity_type: &EntityType) -> bool {
        self.entity_types.contains(entity_type)
    }

    /// The scopes an event is delivered through, ascending and without repeats. An event of an
    /// uncovered entity type has none. The resolver is shown the event without a journal
    /// allocation, so the answer cannot depend on one.
    pub fn scopes_of(&self, event: &SerializedEvent) -> Result<Vec<Scope>, Error> {
        if !self.covers(&event.entity_type) {
            return Ok(Vec::new());
        }

        let mut scopes = if event
            .metadata
            .extensions
            .contains_key(super::scope::ALLOCATION_KEY)
        {
            let mut bare = event.clone();
            set_allocations(&mut bare, &[]);

            self.resolver.scopes(&bare)
        } else {
            self.resolver.scopes(event)
        }
        .map_err(|err| {
            Error::InvalidJournal(format!("the scope resolver failed for {}: {err}", event.id))
        })?;

        scopes.sort();
        scopes.dedup();

        Ok(scopes)
    }

    /// Checks one row against the cap.
    pub(crate) fn check_row(
        &self,
        scope: &Scope,
        position: u64,
        event: &SerializedEvent,
    ) -> Result<(), Error> {
        self.check_row_with(scope, position, event, 0)
    }

    fn check_row_with(
        &self,
        scope: &Scope,
        position: u64,
        event: &SerializedEvent,
        headroom: usize,
    ) -> Result<(), Error> {
        let size = row_size(scope, position, event)?.saturating_add(headroom);

        if size > self.max_row_bytes {
            return Err(RowTooLarge {
                scope: scope.as_str().to_owned(),
                position,
                event_id: event.id.to_string(),
                size,
                limit: self.max_row_bytes,
            }
            .into());
        }

        Ok(())
    }

    /// Pre-checks events against the row-size cap, the way the commit will guard them, but without
    /// reading a counter: no position is known yet, so each row is measured at the widest position
    /// the format allows ([`u64::MAX`], 20 digits). For events identical to the ones the commit
    /// will store (same metadata), a pre-check that passes therefore means the commit-time guard
    /// passes. It is **advisory** otherwise: a handler cannot see the commit's metadata (principal,
    /// extensions, command-ID length), so pass the same metadata, or add a margin with
    /// [`with_pre_check_headroom`](Journal::with_pre_check_headroom). A row the guard refuses
    /// anyway fails the commit with the same error, which a store with outcomes records as the
    /// command's `journal-row-too-large` rejection. The pre-check stops at the widest refused row
    /// of the first event that has one and reports it as [`Error::JournalRowTooLarge`].
    ///
    /// `events` are the events exactly as the commit will store them (see
    /// [`pre_check_events`](Journal::pre_check_events) for typed events). Events of uncovered
    /// entity types are skipped.
    ///
    /// This is the hook an application uses to turn an oversized row into its own rejection: call
    /// it in the command handler and map [`RowTooLarge::to_rejection`] into the command's
    /// [`ToRejection`](crate::ToRejection), so the refusal is recorded as the command's outcome
    /// instead of surfacing as an internal commit failure.
    pub fn pre_check(&self, events: &[SerializedEvent]) -> Result<(), Error> {
        for event in events {
            let scopes = self.scopes_of(event)?;
            if scopes.is_empty() {
                continue;
            }

            let mut widest: Option<RowTooLarge> = None;
            let mut measured = event.clone();
            let allocations: Vec<Allocation> = scopes
                .iter()
                .map(|scope| Allocation::new(scope, u64::MAX))
                .collect();
            set_allocations(&mut measured, &allocations);

            for scope in &scopes {
                if let Err(Error::JournalRowTooLarge(row)) =
                    self.check_row_with(scope, u64::MAX, &measured, self.pre_check_headroom)
                    && widest.as_ref().is_none_or(|widest| row.size > widest.size)
                {
                    widest = Some(row);
                }
            }

            if let Some(row) = widest {
                return Err(row.into());
            }
        }

        Ok(())
    }

    /// [`pre_check`](Journal::pre_check) for typed events a handler has just decided: serializes
    /// them as stream `entity_id` would store them after `sequence` events, with `metadata`
    /// stamped the way the commit stamps it (the command ID, else the caller's commit ID, else a
    /// generated 26-character ULID), so the measure matches the commit's. A store with a custom
    /// commit-ID generator whose IDs are not 26 characters measures a little off; the commit-time
    /// guard is the authority.
    pub fn pre_check_events<A: Aggregate>(
        &self,
        entity_id: &EntityId,
        sequence: EventSequence,
        events: &[A::Event],
        metadata: &Metadata,
    ) -> Result<(), Error> {
        let stamped = stamp_commit_id(metadata, &|| "0".repeat(26));
        let serialized = serialize_events::<A>(entity_id, sequence, events, &stamped)?;

        self.pre_check(&serialized)
    }
}

/// The result of allocating positions for a commit: each event's allocations, in the order the
/// events were given, and the counters after the commit.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Allocated {
    /// The allocations of each event, ascending by scope.
    pub allocations: Vec<Vec<Allocation>>,
    /// The highest position now allocated in each scope the commit touched.
    pub counters: BTreeMap<Scope, u64>,
}

/// Allocates positions: each event, in order, takes the next position of every scope in its plan,
/// continuing from `counters` (a scope with no counter starts at 1). Pure; positions are
/// contiguous in each scope, and a scope's counter after the commit is the last position taken.
pub(crate) fn allocate(
    counters: &BTreeMap<Scope, u64>,
    plan: &[Vec<Scope>],
) -> Result<Allocated, Error> {
    let mut next: BTreeMap<Scope, u64> = BTreeMap::new();
    let mut allocations = Vec::with_capacity(plan.len());

    for scopes in plan {
        let mut event_allocations = Vec::with_capacity(scopes.len());

        for scope in scopes {
            let current = *next
                .get(scope)
                .or_else(|| counters.get(scope))
                .unwrap_or(&0);
            let position = current.checked_add(1).ok_or_else(|| {
                Error::InvalidJournal(format!("scope {scope} is out of positions"))
            })?;

            next.insert(scope.clone(), position);
            event_allocations.push(Allocation::new(scope, position));
        }

        allocations.push(event_allocations);
    }

    Ok(Allocated {
        allocations,
        counters: next,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope(name: &str) -> Scope {
        Scope::new(name).unwrap()
    }

    #[test]
    fn positions_continue_each_scope_from_its_counter() {
        let counters = BTreeMap::from([(scope("world"), 10)]);
        let plan = vec![
            vec![scope("place"), scope("world")],
            vec![scope("world")],
            vec![],
            vec![scope("place"), scope("world")],
        ];

        let allocated = allocate(&counters, &plan).unwrap();
        let positions: Vec<Vec<(&str, u64)>> = allocated
            .allocations
            .iter()
            .map(|event| {
                event
                    .iter()
                    .map(|allocation| (allocation.scope.as_str(), allocation.position))
                    .collect()
            })
            .collect();

        assert_eq!(
            positions,
            vec![
                vec![("place", 1), ("world", 11)],
                vec![("world", 12)],
                vec![],
                vec![("place", 2), ("world", 13)],
            ]
        );
        assert_eq!(
            allocated.counters,
            BTreeMap::from([(scope("place"), 2), (scope("world"), 13)]),
            "the counters are the last position taken, only for the scopes touched"
        );
    }

    #[test]
    fn a_scope_at_the_last_position_is_out_of_positions() {
        let counters = BTreeMap::from([(scope("world"), u64::MAX)]);

        assert!(matches!(
            allocate(&counters, &[vec![scope("world")]]),
            Err(Error::InvalidJournal(_))
        ));
    }

    #[test]
    fn an_empty_journal_is_refused() {
        let none: [&str; 0] = [];

        assert!(matches!(
            Journal::new(none, |_: &SerializedEvent| Vec::new()),
            Err(Error::InvalidJournal(_))
        ));
    }

    #[test]
    fn scopes_are_sorted_deduplicated_and_only_for_covered_types() {
        let journal = Journal::new(["ledger"], |_: &SerializedEvent| {
            vec![scope("b"), scope("a"), scope("b")]
        })
        .unwrap();

        let event = SerializedEvent {
            entity_type: "ledger".into(),
            ..Default::default()
        };
        assert_eq!(
            journal.scopes_of(&event).unwrap(),
            vec![scope("a"), scope("b")]
        );

        let other = SerializedEvent {
            entity_type: "other".into(),
            ..Default::default()
        };
        assert_eq!(journal.scopes_of(&other).unwrap(), Vec::new());
    }

    #[test]
    fn the_resolver_never_sees_a_journal_allocation() {
        let journal = Journal::new(["ledger"], |event: &SerializedEvent| {
            let seen = event
                .metadata
                .extensions
                .contains_key(super::super::scope::ALLOCATION_KEY);

            vec![scope(if seen { "saw-allocation" } else { "clean" })]
        })
        .unwrap();

        let mut event = SerializedEvent {
            entity_type: "ledger".into(),
            ..Default::default()
        };
        set_allocations(&mut event, &[Allocation::new(&scope("anything"), 9)]);

        assert_eq!(journal.scopes_of(&event).unwrap(), vec![scope("clean")]);
    }

    #[test]
    fn a_failing_resolver_is_an_invalid_journal() {
        struct Failing;

        impl ScopeResolver for Failing {
            fn scopes(&self, _: &SerializedEvent) -> Result<Vec<Scope>, BoxError> {
                Err("no scope".into())
            }
        }

        let journal = Journal::new(["ledger"], Failing).unwrap();
        let event = SerializedEvent {
            entity_type: "ledger".into(),
            ..Default::default()
        };

        assert!(matches!(
            journal.scopes_of(&event),
            Err(Error::InvalidJournal(message)) if message.contains("no scope")
        ));
    }

    #[test]
    fn the_pre_check_measures_the_widest_row_at_the_widest_position() {
        let event = SerializedEvent {
            id: "ledger-1:1".into(),
            entity_id: "ledger-1".into(),
            entity_type: "ledger".into(),
            payload: br#"{"amount":1}"#.to_vec(),
            ..Default::default()
        };
        let journal = Journal::new(["ledger"], |_: &SerializedEvent| {
            vec![scope("a"), scope("a-much-longer-scope-name")]
        })
        .unwrap();

        // The row of the longer scope is the widest.
        let widest = {
            let mut measured = event.clone();
            set_allocations(
                &mut measured,
                &[
                    Allocation::new(&scope("a"), u64::MAX),
                    Allocation::new(&scope("a-much-longer-scope-name"), u64::MAX),
                ],
            );

            row_size(&scope("a-much-longer-scope-name"), u64::MAX, &measured).unwrap()
        };

        assert!(
            journal
                .clone()
                .with_max_row_bytes(widest)
                .unwrap()
                .pre_check(std::slice::from_ref(&event))
                .is_ok()
        );

        let Err(Error::JournalRowTooLarge(row)) = journal
            .with_max_row_bytes(widest - 1)
            .unwrap()
            .pre_check(std::slice::from_ref(&event))
        else {
            panic!("one byte under the widest row must be refused");
        };
        assert_eq!(row.scope, "a-much-longer-scope-name");
        assert_eq!((row.size, row.limit), (widest, widest - 1));
        assert_eq!(row.event_id, "ledger-1:1");
    }
}
