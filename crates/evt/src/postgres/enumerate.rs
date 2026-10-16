//! [`EventLog`] for PostgreSQL: one cursor over the events table, one batch per entity.

use std::future::poll_fn;
use std::pin::Pin;

use deadpool_postgres::Object;
use futures_core::Stream;
use tokio_postgres::RowStream;
use tokio_postgres::types::ToSql;

use super::{
    COMMAND_OUTCOME_PREFIX, EVENT_COLUMNS, Error, EventRow, PostgresRepository, SYSTEM_PREFIX,
    pool_error,
};
use crate::enumerate::{EventBatch, EventLog, StreamFilter, unfold};
use crate::event::SerializedEvent;

impl EventLog for PostgresRepository {
    const BATCHES_BY_ENTITY: bool = true;

    /// One batch per entity, entities ascending by `entity_id` in the database's collation and
    /// events ascending by sequence. The rows stream from one server-side cursor, so memory is
    /// bounded by the largest entity. The stream holds one pooled connection until it ends or is
    /// dropped, and sees the snapshot of a single statement.
    fn stream_all_events(&self, filter: StreamFilter) -> impl Stream<Item = EventBatch> + Send {
        unfold(State::Start(self, filter), next_batch)
    }
}

enum State<'a> {
    Start(&'a PostgresRepository, StreamFilter),
    Reading(Box<Reading>),
    Done,
}

struct Reading {
    /// Keeps the connection checked out while its rows are read.
    _connection: Object,
    rows: Pin<Box<RowStream>>,
    /// The entity being collected, whose batch is yielded when the next entity starts.
    batch: Vec<SerializedEvent>,
}

async fn next_batch(state: State<'_>) -> Option<(EventBatch, State<'_>)> {
    let mut reading = match state {
        State::Start(repository, filter) => match open(repository, &filter).await {
            Ok(reading) => Box::new(reading),
            Err(err) => return Some((Err(err), State::Done)),
        },
        State::Reading(reading) => reading,
        State::Done => return None,
    };

    loop {
        let Some(row) = poll_fn(|cx| reading.rows.as_mut().poll_next(cx)).await else {
            let last = std::mem::take(&mut reading.batch);

            return (!last.is_empty()).then_some((Ok(last), State::Done));
        };

        let event = match row.map_err(Error::backend).and_then(|row| {
            EventRow {
                event_id: row.get(0),
                sequence: row.get(1),
                event_type: row.get(2),
                version: row.get(3),
                entity_id: row.get(4),
                entity_type: row.get(5),
                payload: row.get(6),
                metadata: row.get(7),
            }
            .into_event()
        }) {
            Ok(event) => event,
            Err(err) => return Some((Err(err), State::Done)),
        };

        if reading
            .batch
            .last()
            .is_some_and(|last| last.entity_id != event.entity_id)
        {
            let finished = std::mem::replace(&mut reading.batch, vec![event]);

            return Some((Ok(finished), State::Reading(reading)));
        }

        reading.batch.push(event);
    }
}

async fn open(repository: &PostgresRepository, filter: &StreamFilter) -> Result<Reading, Error> {
    let connection = repository.pool.get().await.map_err(pool_error)?;
    let sql = format!(
        "SELECT {EVENT_COLUMNS} FROM {}
         WHERE entity_id NOT LIKE '{SYSTEM_PREFIX}%'
             AND entity_id NOT LIKE '{COMMAND_OUTCOME_PREFIX}%'
             AND ($1::text IS NULL OR entity_type = $1)
         ORDER BY entity_id ASC, sequence ASC",
        repository.events_table
    );
    let entity_type = filter.entity_type.as_ref().map(|t| t.as_str());
    let params: [&(dyn ToSql + Sync); 1] = [&entity_type];

    let rows = connection
        .query_raw(&sql, params)
        .await
        .map_err(Error::backend)?;

    Ok(Reading {
        _connection: connection,
        rows: Box::pin(rows),
        batch: Vec::new(),
    })
}
