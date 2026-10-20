//! [`JournalStore`] and [`CompactionStore`] for the in-memory repository, over the view rows
//! commits write.

use std::ops::Bound;

use super::MemRepository;
use crate::error::Error;
use crate::journal::{
    COUNTER_SORT_KEY, CompactionStore, CompactionWrite, CompactionWritten, JournalRow,
    JournalStore, Scope, compaction_pk, parse_row_sk, read_counter, row_sk,
};
use crate::projection::ViewRow;
use crate::rebuild::RebuildEpoch;

impl JournalStore for MemRepository {
    async fn read_counter(&self, scope: &Scope) -> Result<u64, Error> {
        self.view(&scope.counter_pk(), COUNTER_SORT_KEY)
            .map_or(Ok(0), |row| read_counter(scope, &row))
    }

    async fn read_last_position(&self, scope: &Scope) -> Result<u64, Error> {
        let pk = scope.row_pk();
        let rows = self.views_lock();

        rows.range((
            Bound::Included((pk.clone(), String::new())),
            Bound::Unbounded,
        ))
        .take_while(|((row_pk, _), _)| *row_pk == pk)
        .last()
        .map_or(Ok(0), |((_, sk), _)| {
            parse_row_sk(sk).ok_or_else(|| {
                Error::InvalidRecord(format!("journal row {pk:?}/{sk:?} has no position"))
            })
        })
    }

    async fn read_rows(
        &self,
        scope: &Scope,
        after: u64,
        limit: usize,
    ) -> Result<Vec<JournalRow>, Error> {
        let pk = scope.row_pk();
        let rows = self.views_lock();

        rows.range((
            Bound::Excluded((pk.clone(), row_sk(after))),
            Bound::Unbounded,
        ))
        .take_while(|((row_pk, _), _)| *row_pk == pk)
        .take(limit)
        .map(|(_, row)| JournalRow::from_view(scope, row))
        .collect()
    }

    async fn read_compaction_row(
        &self,
        scope: &Scope,
        sort_key: &str,
    ) -> Result<Option<ViewRow>, Error> {
        Ok(self.view(&compaction_pk(scope), sort_key))
    }
}

/// Every write runs under the rebuild fence's mutex and then the views' mutex, so the epoch check,
/// the conditions, and the writes are one step, as the DynamoDB transaction is.
impl CompactionStore for MemRepository {
    async fn write_compaction(
        &self,
        epoch: Option<RebuildEpoch>,
        writes: &[CompactionWrite],
    ) -> Result<CompactionWritten, Error> {
        crate::journal::check_compaction_writes(writes)?;

        let fence = self.rebuild_lock();
        if epoch.is_some_and(|epoch| epoch.generation() != fence.generation.value) {
            return Ok(CompactionWritten::EpochChanged);
        }

        let mut rows = self.views_lock();
        for (at, write) in writes.iter().enumerate() {
            let key = write.key();
            let stored = rows.get(&(key.0.to_owned(), key.1.to_owned()));
            let met = match write {
                CompactionWrite::Put { expect, .. } | CompactionWrite::Check { expect, .. } => {
                    crate::journal::meets(expect, stored)
                }
                CompactionWrite::Delete { owner, .. } => {
                    stored.is_none_or(|row| row.entity_id == *owner)
                }
            };

            if !met {
                return Ok(CompactionWritten::Refused(at));
            }
        }

        for write in writes {
            match write {
                CompactionWrite::Put { row, .. } => {
                    rows.insert((row.pk.clone(), row.sk.clone()), row.clone());
                }
                CompactionWrite::Delete { pk, sk, .. } => {
                    rows.remove(&(pk.clone(), sk.clone()));
                }
                CompactionWrite::Check { .. } => {}
            }
        }

        Ok(CompactionWritten::Written)
    }

    fn compaction_now(&self) -> i64 {
        self.clock.unix_seconds()
    }
}
