package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
)

// The rebuild fence's relations derive from the configured events table name through the reserved
// infix, like the feature marker's (storage spec, PostgreSQL).
const (
	rebuildLockInfix       = "__evt_rebuild_lock"
	rebuildGenerationInfix = "__evt_rebuild_generation"
)

// A lease is stored as a timestamptz but compared as the whole epoch seconds the DynamoDB item
// stores, converted in SQL on both sides, so a lease lapses at the same instant on every backend.
const leaseSeconds = "EXTRACT(EPOCH FROM lease_expires_at)::bigint"

// timestampOf is the SQL that turns the epoch seconds bound to placeholder n into a timestamptz.
func timestampOf(n int) string {
	return fmt.Sprintf("to_timestamp($%d::bigint::double precision)", n)
}

// errRebuildMoved rolls an attempt back: a conditional statement matched no row, so the state moved
// under the transaction and Begin decides again.
var errRebuildMoved = errors.New("postgres: the rebuild fence moved")

func (repo *Repository) rebuildLockTable() string { return repo.eventsTable + rebuildLockInfix }

func (repo *Repository) rebuildGenerationTable() string {
	return repo.eventsTable + rebuildGenerationInfix
}

// WithClock returns a shallow copy of the repository that reads the given clock for rebuild lease
// expiry. Intended for tests; production callers leave it unset to use time.Now.
func (repo *Repository) WithClock(now func() time.Time) *Repository {
	r := *repo
	r.now = now

	return &r
}

func (repo *Repository) clock() time.Time {
	if repo.now != nil {
		return repo.now()
	}

	return time.Now()
}

// rebuildAdvisoryKey is the second advisory-lock key of the rebuild fence: lifecycle transactions
// take it exclusively for their short read-modify-write, and every commit takes it shared until
// COMMIT, so no acquire or release can slip between a commit's assertion and its COMMIT.
func (repo *Repository) rebuildAdvisoryKey() string { return repo.eventsTable + ":rebuild" }

func rebuildInProgress() error {
	return &policy.ClassifiedError{Class: policy.ClassTransient, Err: &evt.RebuildInProgressError{}}
}

// assertNoRebuildLock is the commit-time rebuild-lock check (C10): after taking the shared
// advisory lock it asserts that the lock row is absent, an expired row included. A store whose
// schema predates the fence has no lock relation, and nobody can have taken the lock: the
// assertion passes, and the probe is not repeated once the relation has been seen.
func (repo *Repository) assertNoRebuildLock(ctx context.Context, tx pgx.Tx) error {
	query := fmt.Sprintf("SELECT pg_advisory_xact_lock_shared(%d, hashtext($1))", engineRowLockClass)
	if _, err := tx.Exec(ctx, query, repo.rebuildAdvisoryKey()); err != nil {
		return fmt.Errorf("postgres: lock rebuild check: %w", err)
	}

	if !repo.rebuildReady.Load() {
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, repo.rebuildLockTable()).Scan(&exists); err != nil {
			return fmt.Errorf("postgres: checking the rebuild lock relation: %w", err)
		}

		if !exists {
			return nil
		}

		repo.rebuildReady.Store(true)
	}

	var held bool
	if err := tx.QueryRow(ctx,
		fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s WHERE id = 0)`, repo.rebuildLockTable()),
	).Scan(&held); err != nil {
		return fmt.Errorf("postgres: reading the rebuild lock: %w", err)
	}

	if held {
		return rebuildInProgress()
	}

	return nil
}

// RebuildFence returns the rebuild fence over this repository's relations
// (evt.RebuildFenceProvider). Leases are read from the repository's clock (WithClock).
func (repo *Repository) RebuildFence() evt.RebuildFence {
	return &rebuildFence{repo: repo}
}

var _ evt.RebuildFence = (*rebuildFence)(nil)

// rebuildFence is the PostgreSQL evt.RebuildFence.
type rebuildFence struct{ repo *Repository }

// lockExclusive takes the fence's advisory lock exclusively for the rest of the transaction.
func (f *rebuildFence) lockExclusive(ctx context.Context, tx pgx.Tx) error {
	query := fmt.Sprintf("SELECT pg_advisory_xact_lock(%d, hashtext($1))", engineRowLockClass)
	if _, err := tx.Exec(ctx, query, f.repo.rebuildAdvisoryKey()); err != nil {
		return fmt.Errorf("postgres: lock rebuild fence: %w", err)
	}

	return nil
}

// queryRower is the read half of a connection or a transaction.
type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// generation reads the generation row; a missing row (or relation never created) is generation 0.
func (f *rebuildFence) generation(ctx context.Context, db queryRower, forUpdate bool) (evt.RebuildGeneration, error) {
	query := fmt.Sprintf(`SELECT value, force_released FROM %s WHERE id = 0`, f.repo.rebuildGenerationTable())
	if forUpdate {
		query += " FOR UPDATE"
	}

	var generation evt.RebuildGeneration

	err := db.QueryRow(ctx, query).Scan(&generation.Value, &generation.ForceReleased)
	if errors.Is(err, pgx.ErrNoRows) {
		return evt.RebuildGeneration{}, nil
	}

	if err != nil {
		return evt.RebuildGeneration{}, fmt.Errorf("postgres: reading the rebuild generation: %w", err)
	}

	return generation, nil
}

// lock reads the lock row, nil when none exists.
func (f *rebuildFence) lock(ctx context.Context, db queryRower, forUpdate bool) (*evt.RebuildLock, error) {
	query := fmt.Sprintf(`SELECT owner, generation, %s FROM %s WHERE id = 0`, leaseSeconds, f.repo.rebuildLockTable())
	if forUpdate {
		query += " FOR UPDATE"
	}

	var (
		owner      string
		generation int64
		expiry     int64
	)

	err := db.QueryRow(ctx, query).Scan(&owner, &generation, &expiry)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("postgres: reading the rebuild lock: %w", err)
	}

	return &evt.RebuildLock{Owner: owner, Generation: generation, LeaseExpiresAt: time.Unix(expiry, 0)}, nil
}

// Generation reads the generation row. PostgreSQL reads are transactionally consistent, so no
// consistency flag applies.
func (f *rebuildFence) Generation(ctx context.Context) (evt.RebuildGeneration, error) {
	return f.generation(ctx, f.repo.db, false)
}

// State reads the generation, then the lock row.
func (f *rebuildFence) State(ctx context.Context) (evt.RebuildState, error) {
	generation, err := f.generation(ctx, f.repo.db, false)
	if err != nil {
		return evt.RebuildState{}, err
	}

	lock, err := f.lock(ctx, f.repo.db, false)
	if err != nil {
		return evt.RebuildState{}, err
	}

	return evt.RebuildState{Generation: generation, Lock: lock}, nil
}

// Begin acquires the lock or takes over an expired lease in one transaction under the exclusive
// advisory lock. The state is read inside the transaction, so the conditional statements can fail
// only if a writer outside the protocol changed the rows; Begin then decides again.
func (f *rebuildFence) Begin(ctx context.Context, owner string, lease time.Duration) (evt.RebuildLease, error) {
	if err := evt.CheckRebuildOwner(owner); err != nil {
		return evt.RebuildLease{}, err
	}

	seconds, err := evt.RebuildLeaseSeconds(lease)
	if err != nil {
		return evt.RebuildLease{}, err
	}

	for attempt := 1; attempt <= evt.MaxRebuildBeginAttempts; attempt++ {
		var started evt.RebuildLease

		err = f.repo.inTx(ctx, func(tx pgx.Tx) error {
			var berr error

			started, berr = f.begin(ctx, tx, owner, seconds)

			return berr
		})
		if err == nil {
			return started, nil
		}

		if !errors.Is(err, errRebuildMoved) {
			return evt.RebuildLease{}, err
		}
	}

	return evt.RebuildLease{}, &policy.ClassifiedError{
		Class: policy.ClassTransient,
		Err:   &evt.RebuildConflictError{Attempts: evt.MaxRebuildBeginAttempts},
	}
}

// begin runs one Begin attempt inside tx. A conditional statement that matches no row returns
// errRebuildMoved, which rolls the attempt back.
func (f *rebuildFence) begin(ctx context.Context, tx pgx.Tx, owner string, seconds int64) (evt.RebuildLease, error) {
	if err := f.lockExclusive(ctx, tx); err != nil {
		return evt.RebuildLease{}, err
	}

	generation, err := f.generation(ctx, tx, true)
	if err != nil {
		return evt.RebuildLease{}, err
	}

	lock, err := f.lock(ctx, tx, true)
	if err != nil {
		return evt.RebuildLease{}, err
	}

	now := f.repo.clock()

	plan, err := evt.PlanRebuildBegin(evt.RebuildState{Generation: generation, Lock: lock}, now)
	if err != nil {
		return evt.RebuildLease{}, err
	}

	expiry := now.Unix() + seconds

	if plan.Kind == evt.RebuildTakeover {
		err = f.takeover(ctx, tx, owner, plan, generation.Value+plan.Increment, now.Unix(), expiry)
	} else {
		err = f.acquire(ctx, tx, owner, plan, generation.Value+plan.Increment, expiry)
	}

	if err != nil {
		return evt.RebuildLease{}, err
	}

	return evt.RebuildLease{
		Owner:          owner,
		Generation:     generation.Value + plan.Increment,
		LeaseExpiresAt: time.Unix(expiry, 0),
	}, nil
}

func (f *rebuildFence) acquire(
	ctx context.Context,
	tx pgx.Tx,
	owner string,
	plan evt.RebuildPlan,
	token, expiry int64,
) error {
	insert := fmt.Sprintf(
		`INSERT INTO %s (id, owner, generation, lease_expires_at) VALUES (0, $1, $3, %s)`,
		f.repo.rebuildLockTable(), timestampOf(2),
	)
	if _, err := tx.Exec(ctx, insert, owner, expiry, token); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == uniqueViolation {
			return errRebuildMoved
		}

		return fmt.Errorf("postgres: acquiring the rebuild lock: %w", err)
	}

	generation := f.repo.rebuildGenerationTable()
	upsert := fmt.Sprintf(
		`INSERT INTO %[1]s (id, value, force_released) VALUES (0, $1, false)
		 ON CONFLICT (id) DO UPDATE SET value = %[1]s.value + $1
		 WHERE %[1]s.value = $2 OR (%[1]s.value = $3 AND %[1]s.force_released)`,
		generation,
	)

	tag, err := tx.Exec(ctx, upsert, plan.Increment, plan.SeenEven, plan.SeenOdd)
	if err != nil {
		return fmt.Errorf("postgres: bumping the rebuild generation: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return errRebuildMoved
	}

	return nil
}

func (f *rebuildFence) takeover(
	ctx context.Context,
	tx pgx.Tx,
	owner string,
	plan evt.RebuildPlan,
	token, now, expiry int64,
) error {
	update := fmt.Sprintf(
		`UPDATE %s SET owner = $1, generation = $5, lease_expires_at = %s
		 WHERE id = 0 AND owner = $3 AND lease_expires_at < %s`,
		f.repo.rebuildLockTable(), timestampOf(2), timestampOf(4),
	)

	tag, err := tx.Exec(ctx, update, owner, expiry, plan.ObservedOwner, now, token)
	if err != nil {
		return fmt.Errorf("postgres: taking over the rebuild lock: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return errRebuildMoved
	}

	bump := fmt.Sprintf(`UPDATE %s SET value = value + 2 WHERE id = 0 AND value = $1`, f.repo.rebuildGenerationTable())

	tag, err = tx.Exec(ctx, bump, plan.SeenOdd)
	if err != nil {
		return fmt.Errorf("postgres: bumping the rebuild generation: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return errRebuildMoved
	}

	return nil
}

// Renew extends the lease with one conditional update bound to its owner and fencing generation.
func (f *rebuildFence) Renew(ctx context.Context, lease evt.RebuildLease, extend time.Duration) (evt.RebuildLease, error) {
	if err := evt.CheckRebuildOwner(lease.Owner); err != nil {
		return evt.RebuildLease{}, err
	}

	seconds, err := evt.RebuildLeaseSeconds(extend)
	if err != nil {
		return evt.RebuildLease{}, err
	}

	expiry := f.repo.clock().Unix() + seconds

	tag, err := f.repo.db.Exec(ctx,
		fmt.Sprintf(`UPDATE %s SET lease_expires_at = %s WHERE id = 0 AND owner = $1 AND generation = $3`,
			f.repo.rebuildLockTable(), timestampOf(2)),
		lease.Owner, expiry, lease.Generation,
	)
	if err != nil {
		return evt.RebuildLease{}, fmt.Errorf("postgres: renewing the rebuild lease: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return evt.RebuildLease{}, &evt.RebuildLeaseLostError{Owner: lease.Owner}
	}

	lease.LeaseExpiresAt = time.Unix(expiry, 0)

	return lease, nil
}

// End releases the lock and settles the generation in one transaction, bound to the lease.
func (f *rebuildFence) End(ctx context.Context, lease evt.RebuildLease) (evt.RebuildGeneration, error) {
	return f.release(ctx, lease.Owner, &lease.Generation, 1, false)
}

// ForceRelease deletes the named owner's lock and leaves the generation odd and flagged.
func (f *rebuildFence) ForceRelease(ctx context.Context, owner string) (evt.RebuildGeneration, error) {
	return f.release(ctx, owner, nil, 2, true)
}

// release runs a release's or a force-release's transaction: delete the owner's lock row and bump
// the generation from the odd value it holds. A release is bound to its lease's fencing generation
// (token); a force-release names only the owner.
func (f *rebuildFence) release(
	ctx context.Context,
	owner string,
	token *int64,
	increment int64,
	forced bool,
) (evt.RebuildGeneration, error) {
	if err := evt.CheckRebuildOwner(owner); err != nil {
		return evt.RebuildGeneration{}, err
	}

	var released evt.RebuildGeneration

	err := f.repo.inTx(ctx, func(tx pgx.Tx) error {
		if err := f.lockExclusive(ctx, tx); err != nil {
			return err
		}

		generation, err := f.generation(ctx, tx, true)
		if err != nil {
			return err
		}

		// A settled generation means no rebuild holds the fence, so the caller cannot be the holder.
		if generation.Settled() || (token != nil && generation.Value != *token) {
			return &evt.RebuildLeaseLostError{Owner: owner}
		}

		deletion := fmt.Sprintf(`DELETE FROM %s WHERE id = 0 AND owner = $1`, f.repo.rebuildLockTable())
		args := []any{owner}

		if token != nil {
			deletion += " AND generation = $2"
			args = append(args, *token)
		}

		tag, err := tx.Exec(ctx, deletion, args...)
		if err != nil {
			return fmt.Errorf("postgres: releasing the rebuild lock: %w", err)
		}

		if tag.RowsAffected() == 0 {
			return &evt.RebuildLeaseLostError{Owner: owner}
		}

		tag, err = tx.Exec(ctx,
			fmt.Sprintf(
				`UPDATE %s SET value = value + $1, force_released = $2 WHERE id = 0 AND value = $3`,
				f.repo.rebuildGenerationTable(),
			),
			increment, forced, generation.Value,
		)
		if err != nil {
			return fmt.Errorf("postgres: bumping the rebuild generation: %w", err)
		}

		if tag.RowsAffected() == 0 {
			return &evt.RebuildLeaseLostError{Owner: owner}
		}

		released = evt.RebuildGeneration{Value: generation.Value + increment, ForceReleased: forced}

		return nil
	})
	if err != nil {
		return evt.RebuildGeneration{}, err
	}

	return released, nil
}

// WriteViews writes one batch of view operations in a transaction that takes the lock row FOR
// UPDATE and asserts the owner and an unexpired lease. The row lock is held to COMMIT, so a
// takeover, which must update or delete that row, blocks until the batch commits: the old worker
// either asserts its lease and commits first, or waits and then fails the assertion against the
// new owner. A plain predicate read would hold no lock and leave exactly that window.
func (f *rebuildFence) WriteViews(ctx context.Context, lease evt.RebuildLease, views []evt.ProjectedView) error {
	if err := evt.CheckRebuildOwner(lease.Owner); err != nil {
		return err
	}

	if err := evt.CheckRebuildViewBatch(views); err != nil {
		return err
	}

	built, err := f.repo.buildViews(views)
	if err != nil {
		return err
	}

	return f.repo.inTxMapping(ctx, func(tx pgx.Tx) error {
		lock, lerr := f.lock(ctx, tx, true)
		if lerr != nil {
			return lerr
		}

		if lock == nil || lock.Owner != lease.Owner || lock.Generation != lease.Generation ||
			lock.LeaseExpiresAt.Unix() <= f.repo.clock().Unix() {
			return &evt.RebuildLeaseLostError{Owner: lease.Owner}
		}

		return execViews(ctx, tx, built)
	}, true)
}
