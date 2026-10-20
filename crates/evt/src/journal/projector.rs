//! The journal's commit-wide projector: journal rows derived from the allocations in event
//! metadata.

use std::fmt;

use super::config::Journal;
use super::row::encode_row;
use super::scope::{ROW_ENTITY_TYPE, allocations, row_sk};
use crate::error::BoxError;
use crate::ids::EntityType;
use crate::projection::{CommitProjector, CommitView, ViewOperation};

/// The error a journal row's operation maps to when its position is already taken: another commit
/// allocated the same position of the scope first. The journal's retry matches it, re-reads the
/// counters, and allocates again.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PositionTaken {
    /// The scope.
    pub scope: String,
    /// The position another commit holds.
    pub position: u64,
}

impl fmt::Display for PositionTaken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "position {} of journal scope {:?} is already taken",
            self.position, self.scope
        )
    }
}

impl std::error::Error for PositionTaken {}

/// Derives a commit's journal rows: for each event, one row per allocation in its metadata, at
/// `journal#<scope>` / the 20-digit position, owned by the event's stream, and written only if
/// absent (the position is claimed exactly once).
///
/// It is a **history-dependent** [`CommitProjector`]: it reads the events of the journal's entity
/// types, so those types are declared in the feature manifest's `historyDependent` set and never
/// compacted. It is a pure function of its commit view, so grouping stored events by commit and
/// running it ([`group_commits`](crate::group_commits), [`replay_views`](crate::replay_views))
/// reproduces the live rows exactly. An event with no allocation has no row.
///
/// The counters are not its output: a counter is shared by every stream that writes its scope, so
/// no stream can own it. [`JournalRepository`](super::JournalRepository) writes them in the same
/// commit, and [`replay`](super::replay) derives them (and the rows) from stored events.
#[derive(Clone, Debug)]
pub struct JournalProjector {
    journal: Journal,
}

impl JournalProjector {
    /// The projector of a journal.
    pub fn new(journal: Journal) -> Self {
        Self { journal }
    }
}

/// The name the journal's projector reports in errors.
pub const PROJECTOR_NAME: &str = "journal";

impl CommitProjector for JournalProjector {
    fn name(&self) -> &str {
        PROJECTOR_NAME
    }

    fn history(&self) -> &[EntityType] {
        self.journal.entity_types()
    }

    fn engine_metadata(&self) -> bool {
        true
    }

    fn project(&self, commit: &CommitView) -> Result<Vec<ViewOperation>, BoxError> {
        let mut operations = Vec::new();

        for stream in &commit.streams {
            for event in &stream.events {
                for allocation in allocations(&event.metadata)? {
                    let scope = allocation.scope()?;

                    operations.push(
                        ViewOperation::put(
                            scope.row_pk(),
                            row_sk(allocation.position),
                            event.entity_id.clone(),
                            ROW_ENTITY_TYPE,
                            encode_row(&scope, allocation.position, event)?,
                        )
                        .if_absent()
                        .on_conflict(PositionTaken {
                            scope: allocation.scope,
                            position: allocation.position,
                        }),
                    );
                }
            }
        }

        Ok(operations)
    }
}
