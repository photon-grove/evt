//! [`JournalStore`] for the in-memory repository, over the view rows commits write.

use std::ops::Bound;

use super::MemRepository;
use crate::error::Error;
use crate::journal::{
    COUNTER_SORT_KEY, JournalRow, JournalStore, Scope, parse_row_sk, read_counter, row_sk,
};

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
}
