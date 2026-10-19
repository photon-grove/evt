//! Failure classification shared by every backend (ADR 0007).
//!
//! A commit fails when a head update, an event put, a snapshot put, or a fence check loses its
//! condition. Where several items failed, every backend reports the same one, by rank (highest
//! first): `StreamHeadDrift`, then `SequenceGap`, then any other conflict, the first in
//! transaction order. The feature-marker check, the rebuild-lock check, and the outcome put
//! outrank all of these and are handled by each backend before it ranks streams.
//!
//! A failed head update or fence check is classified by the *failure-path probe*, which reads the
//! stream instead of asking the backend to return the failed item. The reads are backend-specific
//! (strongly consistent reads on DynamoDB, queries in the commit transaction on PostgreSQL, plain
//! lookups in memory); the decisions are here.

use crate::error::Error;
use crate::ids::{EntityId, EventSequence};
use crate::repository::ReadFence;

/// What the first pass of the probe concluded, from the derived head `D1` (read first) and the
/// stored head `S` (read second; an absent head item is 0).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Probe {
    /// `S < D1`: an event or snapshot sits above the head. A healthy stream cannot show it,
    /// because a commit writes the head and its events together and `D1` was read first.
    Drift,
    /// `S = D1`: the head matches the log.
    Settled,
    /// `S > D1`: derive again as `D2`; see [`Probe::after_rederive`].
    Ahead,
}

impl Probe {
    /// The first pass.
    pub(crate) fn first(stored: EventSequence, derived: EventSequence) -> Self {
        match stored.cmp(&derived) {
            std::cmp::Ordering::Less => Self::Drift,
            std::cmp::Ordering::Equal => Self::Settled,
            std::cmp::Ordering::Greater => Self::Ahead,
        }
    }

    /// The second pass, when the stored head was ahead: `D2 < S` means events are missing below
    /// the head (drift); otherwise a commit landed between the two reads, which is an ordinary
    /// conflict.
    pub(crate) fn after_rederive(stored: EventSequence, rederived: EventSequence) -> Self {
        if rederived < stored {
            Self::Drift
        } else {
            Self::Settled
        }
    }
}

/// Builds a [`Error::StreamHeadDrift`]. A stored head of 0 is an absent head item.
pub(crate) fn drift(entity_id: &EntityId, stored: EventSequence, derived: EventSequence) -> Error {
    Error::StreamHeadDrift {
        entity_id: entity_id.clone(),
        stored: (stored > EventSequence::ZERO).then_some(stored),
        derived,
    }
}

/// The verdict for a failed head update on a stream whose head matches its log (`S = D`): a gap
/// when the commit's first sequence skips past the stored head, otherwise an ordinary conflict.
pub(crate) fn gap_or_conflict(
    entity_id: &EntityId,
    first: EventSequence,
    stored: EventSequence,
) -> Error {
    if stored.0 < first.0 - 1 {
        return Error::SequenceGap {
            entity_id: entity_id.clone(),
            after: stored,
            found: first,
        };
    }

    conflict(entity_id)
}

/// An ordinary optimistic-concurrency conflict on a written stream.
pub(crate) fn conflict(entity_id: &EntityId) -> Error {
    Error::Conflict {
        entity_id: entity_id.clone(),
        message: "conditional check failed: the sequence or snapshot changed since it was read"
            .to_owned(),
    }
}

/// A failed fence check that the probe did not explain as drift.
pub(crate) fn fence_conflict(fence: &ReadFence) -> Error {
    Error::FenceConflict {
        entity_id: fence.entity_id.clone(),
        head: fence.head,
        provenance: fence.provenance,
    }
}

/// Picks the error a commit reports from the verdicts of every failed stream and fence item, in
/// transaction order: the first drift, else the first gap, else the first of the rest.
pub(crate) fn rank(verdicts: impl IntoIterator<Item = Error>) -> Option<Error> {
    // `min_by_key` returns the first of several equal minima, which keeps transaction order.
    verdicts.into_iter().min_by_key(|error| match error {
        Error::StreamHeadDrift { .. } => 0,
        Error::SequenceGap { .. } => 1,
        _ => 2,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(name: &str) -> EntityId {
        EntityId::new(name)
    }

    #[test]
    fn the_probe_compares_the_stored_head_with_the_derived_head() {
        let at = EventSequence;

        assert_eq!(Probe::first(at(2), at(5)), Probe::Drift);
        assert_eq!(Probe::first(at(0), at(1)), Probe::Drift, "no head item");
        assert_eq!(Probe::first(at(5), at(5)), Probe::Settled);
        assert_eq!(Probe::first(at(6), at(5)), Probe::Ahead);

        // Ahead of the first derivation: still ahead of the second means missing events, equal
        // or below means a commit landed between the reads.
        assert_eq!(Probe::after_rederive(at(6), at(5)), Probe::Drift);
        assert_eq!(Probe::after_rederive(at(6), at(6)), Probe::Settled);
        assert_eq!(Probe::after_rederive(at(6), at(7)), Probe::Settled);
    }

    #[test]
    fn a_failed_head_update_is_a_gap_only_when_it_skips_past_the_head() {
        let at = EventSequence;
        let a = id("a");

        // The head is 1 and the commit starts at 3: a gap.
        assert!(matches!(
            gap_or_conflict(&a, at(3), at(1)),
            Error::SequenceGap {
                after: EventSequence(1),
                found: EventSequence(3),
                ..
            }
        ));
        // No stream at all and a first sequence above 1: a gap after 0.
        assert!(matches!(
            gap_or_conflict(&a, at(2), at(0)),
            Error::SequenceGap {
                after: EventSequence(0),
                found: EventSequence(2),
                ..
            }
        ));
        // The commit starts at head + 1 (a lost race on another condition) or at or below the
        // head: an ordinary conflict.
        assert!(matches!(
            gap_or_conflict(&a, at(2), at(1)),
            Error::Conflict { .. }
        ));
        assert!(matches!(
            gap_or_conflict(&a, at(1), at(4)),
            Error::Conflict { .. }
        ));
    }

    #[test]
    fn drift_outranks_a_gap_and_a_gap_outranks_a_conflict() {
        let at = EventSequence;
        let conflict = || conflict(&id("a"));
        let gap = || Error::SequenceGap {
            entity_id: id("b"),
            after: at(1),
            found: at(3),
        };
        let drift = |name: &str| drift(&id(name), at(1), at(2));

        let picked = rank([conflict(), gap(), drift("c")]).unwrap();
        assert!(matches!(picked, Error::StreamHeadDrift { .. }));

        let picked = rank([conflict(), gap(), conflict()]).unwrap();
        assert!(matches!(picked, Error::SequenceGap { .. }));

        // Equal ranks keep transaction order.
        let picked = rank([drift("first"), drift("second")]).unwrap();
        assert!(
            matches!(&picked, Error::StreamHeadDrift { entity_id, .. } if entity_id.as_str() == "first")
        );

        assert!(rank(Vec::new()).is_none());
    }

    #[test]
    fn drift_reports_an_absent_head_as_none() {
        let absent = drift(&id("a"), EventSequence(0), EventSequence(3));
        assert!(matches!(
            absent,
            Error::StreamHeadDrift {
                stored: None,
                derived: EventSequence(3),
                ..
            }
        ));
        assert_eq!(
            absent.to_string(),
            "stream head drift on entity a: stored head absent, derived head 3"
        );

        let low = drift(&id("a"), EventSequence(2), EventSequence(3));
        assert!(matches!(
            low,
            Error::StreamHeadDrift {
                stored: Some(EventSequence(2)),
                ..
            }
        ));
        assert!(!low.is_retryable() && !low.is_conflict());
    }
}
