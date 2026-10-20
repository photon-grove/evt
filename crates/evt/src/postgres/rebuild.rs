//! [`RebuildFence`] for PostgreSQL: the `{events}__evt_rebuild_lock` and
//! `{events}__evt_rebuild_generation` rows of the rebuild fence (C10), with the statements the
//! storage spec pins ("PostgreSQL", rebuild fence).
//!
//! Every lifecycle transaction takes the fence's exclusive advisory lock and reads the state inside
//! it, so a conditional statement that matches no row means the rows moved outside the protocol.
//! Leases are stored as `timestamptz` and compared as the whole epoch seconds the DynamoDB item
//! stores, converted in SQL on both sides.

use std::time::Duration;

use deadpool_postgres::GenericClient;
use tokio_postgres::error::SqlState;

use super::{
    PostgresRepository, commit_error, pool_error, run_view_statements, statement_error, view_writes,
};
use crate::error::Error;
use crate::projection::ProjectedView;
use crate::rebuild::{
    self, MAX_REBUILD_BEGIN_ATTEMPTS, Plan, PlanKind, RebuildFence, RebuildGeneration,
    RebuildLease, RebuildLock, RebuildState,
};
use crate::wire::postgres::{
    FEATURES_LOCK_EXCLUSIVE, rebuild_generation_table, rebuild_lock_key, rebuild_lock_table,
};

/// What one `begin` attempt came to.
enum Attempt {
    Begun(RebuildLease),
    /// A conditional statement matched no row: the rows moved, so read again and decide again.
    Moved,
}

impl PostgresRepository {
    fn lock_table(&self) -> String {
        rebuild_lock_table(&self.events_table)
    }

    fn generation_table(&self) -> String {
        rebuild_generation_table(&self.events_table)
    }

    /// Takes the fence's exclusive advisory lock for the rest of the transaction.
    async fn lock_fence(&self, tx: &deadpool_postgres::Transaction<'_>) -> Result<(), Error> {
        tx.execute(
            FEATURES_LOCK_EXCLUSIVE,
            &[&rebuild_lock_key(&self.events_table)],
        )
        .await
        .map_err(statement_error)?;

        Ok(())
    }

    /// Reads the generation row; no row is generation 0.
    async fn read_generation(
        &self,
        client: &impl GenericClient,
        for_update: bool,
    ) -> Result<RebuildGeneration, Error> {
        let sql = format!(
            "SELECT value, force_released FROM {} WHERE id = 0{}",
            self.generation_table(),
            if for_update { " FOR UPDATE" } else { "" }
        );
        let row = client.query_opt(&sql, &[]).await.map_err(statement_error)?;

        Ok(
            row.map_or_else(RebuildGeneration::default, |row| RebuildGeneration {
                value: row.get(0),
                force_released: row.get(1),
            }),
        )
    }

    /// Reads the lock row, `None` when none exists.
    async fn read_lock(
        &self,
        client: &impl GenericClient,
        for_update: bool,
    ) -> Result<Option<RebuildLock>, Error> {
        let sql = format!(
            "SELECT owner, EXTRACT(EPOCH FROM lease_expires_at)::bigint FROM {} WHERE id = 0{}",
            self.lock_table(),
            if for_update { " FOR UPDATE" } else { "" }
        );
        let row = client.query_opt(&sql, &[]).await.map_err(statement_error)?;

        Ok(row.map(|row| RebuildLock {
            owner: row.get(0),
            lease_expires_at: row.get(1),
        }))
    }

    /// One `begin` attempt in one transaction under the exclusive advisory lock.
    async fn begin_once(&self, owner: &str, seconds: i64) -> Result<Attempt, Error> {
        let mut client = self.pool.get().await.map_err(pool_error)?;
        let tx = client.transaction().await.map_err(Error::backend)?;
        self.lock_fence(&tx).await?;

        let generation = self.read_generation(&tx, true).await?;
        let lock = self.read_lock(&tx, true).await?;
        let now = self.clock.unix_seconds();
        let plan = Plan::decide(&RebuildState { generation, lock }, now)?;
        let lease_expires_at = now.saturating_add(seconds);

        let moved = if plan.kind == PlanKind::Takeover {
            !self
                .take_over(&tx, owner, &plan, now, lease_expires_at)
                .await?
        } else {
            !self.acquire(&tx, owner, &plan, lease_expires_at).await?
        };
        if moved {
            return Ok(Attempt::Moved);
        }

        tx.commit().await.map_err(|err| commit_error(err, false))?;

        Ok(Attempt::Begun(RebuildLease {
            owner: owner.to_owned(),
            generation: generation.value + plan.increment,
            lease_expires_at,
        }))
    }

    /// The acquire's two statements; `false` when the rows moved.
    async fn acquire(
        &self,
        tx: &deadpool_postgres::Transaction<'_>,
        owner: &str,
        plan: &Plan,
        lease_expires_at: i64,
    ) -> Result<bool, Error> {
        let insert = format!(
            "INSERT INTO {} (id, owner, lease_expires_at) \
             VALUES (0, $1, to_timestamp($2::bigint::double precision))",
            self.lock_table()
        );
        match tx.execute(&insert, &[&owner, &lease_expires_at]).await {
            Ok(_) => {}
            // A lock row exists after all: the rows moved.
            Err(err) if err.code() == Some(&SqlState::UNIQUE_VIOLATION) => return Ok(false),
            Err(err) => return Err(statement_error(err)),
        }

        let generation = self.generation_table();
        let upsert = format!(
            "INSERT INTO {generation} (id, value, force_released) VALUES (0, $1, false) \
             ON CONFLICT (id) DO UPDATE SET value = {generation}.value + $1 \
             WHERE {generation}.value = $2 \
                OR ({generation}.value = $3 AND {generation}.force_released)"
        );
        let affected = tx
            .execute(&upsert, &[&plan.increment, &plan.seen_even, &plan.seen_odd])
            .await
            .map_err(statement_error)?;

        Ok(affected == 1)
    }

    /// The takeover's two statements; `false` when the rows moved.
    async fn take_over(
        &self,
        tx: &deadpool_postgres::Transaction<'_>,
        owner: &str,
        plan: &Plan,
        now: i64,
        lease_expires_at: i64,
    ) -> Result<bool, Error> {
        let update = format!(
            "UPDATE {} SET owner = $1, lease_expires_at = to_timestamp($2::bigint::double precision) \
             WHERE id = 0 AND owner = $3 \
               AND lease_expires_at < to_timestamp($4::bigint::double precision)",
            self.lock_table()
        );
        let observed = plan.observed_owner.as_deref().unwrap_or_default();
        let affected = tx
            .execute(&update, &[&owner, &lease_expires_at, &observed, &now])
            .await
            .map_err(statement_error)?;
        if affected != 1 {
            return Ok(false);
        }

        let bump = format!(
            "UPDATE {} SET value = value + 2 WHERE id = 0 AND value = $1",
            self.generation_table()
        );
        let affected = tx
            .execute(&bump, &[&plan.seen_odd])
            .await
            .map_err(statement_error)?;

        Ok(affected == 1)
    }

    /// A release (`increment` 1, flag cleared) or a force-release (`increment` 2, flag set) of the
    /// named owner's lock, in one transaction under the exclusive advisory lock.
    async fn release_rebuild(
        &self,
        owner: &str,
        increment: i64,
        force_released: bool,
    ) -> Result<RebuildGeneration, Error> {
        rebuild::check_owner(owner)?;

        let mut client = self.pool.get().await.map_err(pool_error)?;
        let tx = client.transaction().await.map_err(Error::backend)?;
        self.lock_fence(&tx).await?;

        // A settled generation means no rebuild holds the fence, so the caller cannot hold it.
        let generation = self.read_generation(&tx, true).await?;
        if generation.is_settled() {
            return Err(lease_lost(owner));
        }

        let deleted = tx
            .execute(
                &format!(
                    "DELETE FROM {} WHERE id = 0 AND owner = $1",
                    self.lock_table()
                ),
                &[&owner],
            )
            .await
            .map_err(statement_error)?;
        if deleted != 1 {
            return Err(lease_lost(owner));
        }

        let bumped = tx
            .execute(
                &format!(
                    "UPDATE {} SET value = value + {increment}, force_released = {force_released} \
                     WHERE id = 0 AND value = $1",
                    self.generation_table()
                ),
                &[&generation.value],
            )
            .await
            .map_err(statement_error)?;
        if bumped != 1 {
            return Err(lease_lost(owner));
        }

        tx.commit().await.map_err(|err| commit_error(err, false))?;

        Ok(RebuildGeneration {
            value: generation.value + increment,
            force_released,
        })
    }
}

impl RebuildFence for PostgresRepository {
    /// PostgreSQL reads are transactionally consistent, so no consistency flag applies.
    async fn rebuild_generation(&self) -> Result<RebuildGeneration, Error> {
        let client = self.pool.get().await.map_err(pool_error)?;

        self.read_generation(&client, false).await
    }

    async fn rebuild_state(&self) -> Result<RebuildState, Error> {
        let client = self.pool.get().await.map_err(pool_error)?;
        let generation = self.read_generation(&client, false).await?;
        let lock = self.read_lock(&client, false).await?;

        Ok(RebuildState { generation, lock })
    }

    async fn begin_rebuild(&self, owner: &str, lease: Duration) -> Result<RebuildLease, Error> {
        let seconds = rebuild::check_lease(owner, lease)?;

        for _ in 0..MAX_REBUILD_BEGIN_ATTEMPTS {
            if let Attempt::Begun(lease) = self.begin_once(owner, seconds).await? {
                return Ok(lease);
            }
        }

        Err(Error::RebuildConflict {
            attempts: MAX_REBUILD_BEGIN_ATTEMPTS,
        })
    }

    async fn renew_rebuild(&self, owner: &str, lease: Duration) -> Result<i64, Error> {
        let seconds = rebuild::check_lease(owner, lease)?;
        let lease_expires_at = self.clock.unix_seconds().saturating_add(seconds);

        let client = self.pool.get().await.map_err(pool_error)?;
        let renewed = client
            .execute(
                &format!(
                    "UPDATE {} SET lease_expires_at = to_timestamp($2::bigint::double precision) \
                     WHERE id = 0 AND owner = $1",
                    self.lock_table()
                ),
                &[&owner, &lease_expires_at],
            )
            .await
            .map_err(statement_error)?;
        if renewed != 1 {
            return Err(lease_lost(owner));
        }

        Ok(lease_expires_at)
    }

    async fn end_rebuild(&self, owner: &str) -> Result<RebuildGeneration, Error> {
        self.release_rebuild(owner, 1, false).await
    }

    async fn force_release_rebuild(&self, owner: &str) -> Result<RebuildGeneration, Error> {
        self.release_rebuild(owner, 2, true).await
    }

    /// One transaction that takes the lock row `FOR UPDATE` and asserts the owner and an unexpired
    /// lease before the view statements. The row lock is held to `COMMIT`, so a takeover, which
    /// must update or delete that row, waits for the batch: the old worker either asserts its lease
    /// and commits first, or waits and then fails the assertion against the new owner.
    async fn write_rebuild_views(&self, owner: &str, views: &[ProjectedView]) -> Result<(), Error> {
        rebuild::check_owner(owner)?;
        rebuild::check_view_batch(views)?;
        let statements = self.view_statements(views)?;

        let mut client = self.pool.get().await.map_err(pool_error)?;
        let tx = client.transaction().await.map_err(Error::backend)?;

        let lock = self.read_lock(&tx, true).await?;
        let now = self.clock.unix_seconds();
        if !lock.is_some_and(|lock| lock.owner == owner && lock.lease_expires_at > now) {
            return Err(lease_lost(owner));
        }

        run_view_statements(&tx, &view_writes(views, &statements)).await?;

        tx.commit().await.map_err(|err| commit_error(err, false))
    }
}

fn lease_lost(owner: &str) -> Error {
    Error::RebuildLeaseLost {
        owner: owner.to_owned(),
    }
}
