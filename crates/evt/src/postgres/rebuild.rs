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
    pub(super) async fn read_generation(
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
            "SELECT owner, generation, EXTRACT(EPOCH FROM lease_expires_at)::bigint FROM {} \
             WHERE id = 0{}",
            self.lock_table(),
            if for_update { " FOR UPDATE" } else { "" }
        );
        let row = client.query_opt(&sql, &[]).await.map_err(statement_error)?;

        Ok(row.map(|row| RebuildLock {
            owner: row.get(0),
            generation: row.get(1),
            lease_expires_at: row.get(2),
        }))
    }

    /// One `begin` attempt in one transaction under the exclusive advisory lock.
    async fn begin_once(&self, owner: &str, seconds: i64) -> Result<Attempt, Error> {
        let mut client = self.pool.get().await.map_err(pool_error)?;
        let tx = super::read_committed(&mut client).await?;
        self.lock_fence(&tx).await?;

        let generation = self.read_generation(&tx, true).await?;
        let lock = self.read_lock(&tx, true).await?;
        let now = self.clock.unix_seconds();
        let plan = Plan::decide(&RebuildState { generation, lock }, now)?;
        let lease = RebuildLease {
            owner: owner.to_owned(),
            generation: generation.value + plan.increment,
            lease_expires_at: now.saturating_add(seconds),
        };

        let moved = if plan.kind == PlanKind::Takeover {
            !self.take_over(&tx, &lease, &plan, now).await?
        } else {
            !self.acquire(&tx, &lease, &plan).await?
        };
        if moved {
            return Ok(Attempt::Moved);
        }

        if let Err(err) = tx.commit().await {
            // An error the database did not raise (the connection dropped while `COMMIT` was in
            // flight) leaves it unknown whether the lock was taken.
            if err.as_db_error().is_none() {
                return Err(Error::RebuildBeginIndeterminate {
                    lease: Box::new(lease),
                    observed: generation.value,
                    source: Box::new(err),
                });
            }

            return Err(commit_error(err, false));
        }

        Ok(Attempt::Begun(lease))
    }

    /// The acquire's two statements, writing `lease` (its generation is the fencing token); `false`
    /// when the rows moved.
    async fn acquire(
        &self,
        tx: &deadpool_postgres::Transaction<'_>,
        lease: &RebuildLease,
        plan: &Plan,
    ) -> Result<bool, Error> {
        let insert = format!(
            "INSERT INTO {} (id, owner, generation, lease_expires_at) \
             VALUES (0, $1, $3, to_timestamp($2::bigint::double precision))",
            self.lock_table()
        );
        let parameters: [&(dyn tokio_postgres::types::ToSql + Sync); 3] =
            [&lease.owner, &lease.lease_expires_at, &lease.generation];
        match tx.execute(&insert, &parameters).await {
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

    /// The takeover's two statements, writing `lease` (its generation, the observed odd one plus
    /// two, is the fencing token); `false` when the rows moved.
    async fn take_over(
        &self,
        tx: &deadpool_postgres::Transaction<'_>,
        lease: &RebuildLease,
        plan: &Plan,
        now: i64,
    ) -> Result<bool, Error> {
        let update = format!(
            "UPDATE {} SET owner = $1, generation = $5, \
             lease_expires_at = to_timestamp($2::bigint::double precision) \
             WHERE id = 0 AND owner = $3 \
               AND lease_expires_at < to_timestamp($4::bigint::double precision)",
            self.lock_table()
        );
        let observed = plan.observed_owner.as_deref().unwrap_or_default();
        let affected = tx
            .execute(
                &update,
                &[
                    &lease.owner,
                    &lease.lease_expires_at,
                    &observed,
                    &now,
                    &lease.generation,
                ],
            )
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

    /// A release (`token` the lease's generation, `increment` 1, flag cleared) or a force-release
    /// (no token, `increment` 2, flag set) of the owner's lock, in one transaction under the
    /// exclusive advisory lock.
    async fn release_rebuild(
        &self,
        owner: &str,
        token: Option<i64>,
        increment: i64,
        force_released: bool,
    ) -> Result<RebuildGeneration, Error> {
        rebuild::check_owner(owner)?;

        let mut client = self.pool.get().await.map_err(pool_error)?;
        let tx = super::read_committed(&mut client).await?;
        self.lock_fence(&tx).await?;

        // A settled generation means no rebuild holds the fence, so the caller cannot hold it; a
        // lease holds the lock only at its own generation.
        let generation = self.read_generation(&tx, true).await?;
        if generation.is_settled() || token.is_some_and(|token| token != generation.value) {
            return Err(lease_lost(owner));
        }

        let deleted = match token {
            Some(token) => {
                tx.execute(
                    &format!(
                        "DELETE FROM {} WHERE id = 0 AND owner = $1 AND generation = $2",
                        self.lock_table()
                    ),
                    &[&owner, &token],
                )
                .await
            }
            None => {
                tx.execute(
                    &format!(
                        "DELETE FROM {} WHERE id = 0 AND owner = $1",
                        self.lock_table()
                    ),
                    &[&owner],
                )
                .await
            }
        }
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

    async fn renew_rebuild(
        &self,
        lease: &RebuildLease,
        extend: Duration,
    ) -> Result<RebuildLease, Error> {
        let seconds = rebuild::check_lease(&lease.owner, extend)?;
        let lease_expires_at = self.clock.unix_seconds().saturating_add(seconds);

        let client = self.pool.get().await.map_err(pool_error)?;
        let renewed = client
            .execute(
                &format!(
                    "UPDATE {} SET lease_expires_at = to_timestamp($2::bigint::double precision) \
                     WHERE id = 0 AND owner = $1 AND generation = $3",
                    self.lock_table()
                ),
                &[&lease.owner, &lease_expires_at, &lease.generation],
            )
            .await
            .map_err(statement_error)?;
        if renewed != 1 {
            return Err(lease_lost(&lease.owner));
        }

        Ok(RebuildLease {
            lease_expires_at,
            ..lease.clone()
        })
    }

    async fn end_rebuild(&self, lease: &RebuildLease) -> Result<RebuildGeneration, Error> {
        self.release_rebuild(&lease.owner, Some(lease.generation), 1, false)
            .await
    }

    async fn force_release_rebuild(&self, owner: &str) -> Result<RebuildGeneration, Error> {
        self.release_rebuild(owner, None, 2, true).await
    }

    /// One transaction that takes the lock row `FOR UPDATE` and asserts the lease's owner and
    /// fencing token and an unexpired lease before the view statements. The row lock is held to
    /// `COMMIT`, so a takeover, which must update or delete that row, waits for the batch: the old
    /// worker either asserts its lease and commits first, or waits and then fails the assertion
    /// against the new holder.
    async fn write_rebuild_views(
        &self,
        lease: &RebuildLease,
        views: &[ProjectedView],
    ) -> Result<(), Error> {
        let owner = lease.owner.as_str();
        rebuild::check_owner(owner)?;
        rebuild::check_view_batch(views)?;
        if self.views_table_target {
            self.ensure_stream_heads().await?;
            self.ensure_views_declared().await?;
        }
        let statements = self.view_statements(views)?;

        let mut client = self.pool.get().await.map_err(pool_error)?;
        let tx = super::read_committed(&mut client).await?;

        let lock = self.read_lock(&tx, true).await?;
        let now = self.clock.unix_seconds();
        if !lock.is_some_and(|lock| lease.holds(&lock) && lock.lease_expires_at > now) {
            return Err(lease_lost(owner));
        }

        // A store whose views live in the views table is rebuilt only through it, as it is
        // committed to.
        let manifest = self.read_manifest_in(&tx).await?;
        self.refuse_foreign_views(manifest.as_ref())?;

        run_view_statements(&tx, &view_writes(views, &statements)).await?;

        tx.commit().await.map_err(|err| commit_error(err, false))
    }
}

fn lease_lost(owner: &str) -> Error {
    Error::RebuildLeaseLost {
        owner: owner.to_owned(),
    }
}
