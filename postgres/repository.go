package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/result"
)

// StorageTypePostgres identifies transaction groups and repositories backed by PostgreSQL.
const StorageTypePostgres evt.StorageType = "postgres"

// Default table names. They are configurable through NewRepository options so multiple logical
// event logs can share one database.
const (
	defaultEventsTable    = "evt_events"
	defaultSnapshotsTable = "evt_snapshots"
)

// identifierPattern guards table names interpolated into DDL/DML. Table names are configuration, not
// user input, but validating them keeps the interpolation provably injection-free.
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// DB is the minimal subset of the pgx API the Repository depends on. Both *pgxpool.Pool and pgx.Tx
// satisfy it, so callers can pass a pool (the common case) or an ambient transaction. It is the
// abstraction point for the backend, mirroring dynamo.Client.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Repository is a PostgreSQL-backed evt.Repository. Construct it with NewRepository.
type Repository struct {
	db             DB
	eventsTable    string
	snapshotsTable string
}

// Compile-time checks that the Repository satisfies the core contract and every optional capability
// the framework detects by type assertion.
var (
	_ evt.Repository         = (*Repository)(nil)
	_ evt.Compactor          = (*Repository)(nil)
	_ evt.SnapshotStreamer   = (*Repository)(nil)
	_ evt.EntityHeadStreamer = (*Repository)(nil)
	_ evt.EntityHeadVisitor  = (*Repository)(nil)
)

// Option configures a Repository at construction time.
type Option func(*Repository)

// WithEventsTable overrides the event-log table name (default "evt_events").
func WithEventsTable(name string) Option {
	return func(r *Repository) {
		r.eventsTable = name
	}
}

// WithSnapshotsTable overrides the snapshot table name (default "evt_snapshots").
func WithSnapshotsTable(name string) Option {
	return func(r *Repository) {
		r.snapshotsTable = name
	}
}

// NewRepository constructs a Repository over the given pgx handle (typically a *pgxpool.Pool). It
// panics if a configured table name is not a valid SQL identifier, since that is a programming
// error in wiring rather than a runtime condition.
func NewRepository(db DB, opts ...Option) *Repository {
	repo := &Repository{
		db:             db,
		eventsTable:    defaultEventsTable,
		snapshotsTable: defaultSnapshotsTable,
	}

	for _, opt := range opts {
		opt(repo)
	}

	for _, name := range []string{repo.eventsTable, repo.snapshotsTable} {
		if !identifierPattern.MatchString(name) {
			panic(fmt.Sprintf("postgres: invalid table name %q", name))
		}
	}

	return repo
}

// EnsureSchema creates the event-log and snapshot tables if they do not already exist. It is
// idempotent and safe to call on every startup; the integration suite calls it during setup. The
// Repository owns this DDL because the relational schema must stay in lockstep with the Go types
// that marshal into and out of it.
func (repo *Repository) EnsureSchema(ctx context.Context) error {
	if _, err := repo.db.Exec(ctx, repo.schemaDDL()); err != nil {
		return fmt.Errorf("postgres: ensure schema: %w", err)
	}

	return nil
}

// schemaDDL returns the idempotent DDL for the configured tables.
func (repo *Repository) schemaDDL() string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %[1]s (
    entity_id   text   NOT NULL,
    sequence    bigint NOT NULL,
    event_id    text   NOT NULL,
    event_type  text   NOT NULL,
    version     bigint NOT NULL,
    entity_type text   NOT NULL,
    payload     bytea  NOT NULL,
    metadata    jsonb  NOT NULL DEFAULT '{}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_id, sequence)
);
CREATE INDEX IF NOT EXISTS %[1]s_entity_type_idx ON %[1]s (entity_type);

CREATE TABLE IF NOT EXISTS %[2]s (
    entity_id      text   PRIMARY KEY,
    entity_type    text   NOT NULL,
    sequence       bigint NOT NULL,
    event_sequence bigint NOT NULL,
    payload        bytea  NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);
`, repo.eventsTable, repo.snapshotsTable)
}

// Commit persists the events in a SerializedResult inside a single transaction. A duplicate
// (entity_id, sequence) pair violates the primary key and is returned as an evt.ConflictError,
// giving the optimistic-concurrency guarantee the contract requires.
//
// This backend is an event log only: it does not interpret res.Transaction, since projector view
// writes are backend-specific (the only TransactionGroup implementation today is DynamoDB's) and no
// PostgreSQL view store exists yet. Adding one later would extend this to write the view rows in the
// same transaction as the events, preserving atomicity.
func (repo *Repository) Commit(ctx context.Context, res evt.SerializedResult) error {
	return repo.inTx(ctx, func(tx pgx.Tx) error {
		return repo.insertEvents(ctx, tx, res.Events)
	})
}

// CommitStream commits each SerializedResult from the channel in its own transaction, collecting
// per-result errors without aborting the stream. The batch size is one result, which keeps each
// commit atomic and lets a single bad result fail in isolation.
func (repo *Repository) CommitStream(
	ctx context.Context,
	channel <-chan result.Result[evt.SerializedResult],
) []error {
	var errs []error

	for r := range channel {
		res, err := r.Unwrap()
		if err != nil {
			errs = append(errs, err)
			continue
		}

		if err := repo.Commit(ctx, res); err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}

// CommitWithSnapshot commits the events and upserts the entity's durable snapshot in one
// transaction, so the snapshot is never observable without the events it summarizes.
func (repo *Repository) CommitWithSnapshot(
	ctx context.Context,
	res evt.SerializedResult,
	entityType evt.EntityType,
	entityID evt.EntityID,
	payload []byte,
	currentSnapshot evt.EventSequence,
) error {
	if len(res.Events) == 0 {
		return fmt.Errorf("postgres: commit with snapshot requires at least one event")
	}

	eventSequence := res.Events[len(res.Events)-1].Sequence

	return repo.inTx(ctx, func(tx pgx.Tx) error {
		if err := repo.insertEvents(ctx, tx, res.Events); err != nil {
			return err
		}

		return repo.upsertSnapshot(ctx, tx, evt.SerializedSnapshot{
			EntityType:    entityType,
			EntityID:      entityID,
			Sequence:      currentSnapshot,
			EventSequence: eventSequence,
			Payload:       payload,
		})
	})
}

// GetEvents returns every event for an entity in ascending sequence order.
func (repo *Repository) GetEvents(
	ctx context.Context,
	entityID evt.EntityID,
) ([]evt.SerializedEvent, error) {
	query := fmt.Sprintf(
		`SELECT %s FROM %s WHERE entity_id = $1 ORDER BY sequence ASC`,
		eventColumns, repo.eventsTable,
	)

	rows, err := repo.db.Query(ctx, query, string(entityID))
	if err != nil {
		return nil, fmt.Errorf("postgres: get events: %w", err)
	}

	return scanEvents(rows)
}

// GetLatestEvents returns the events for an entity whose sequence is greater than lastSequence,
// ascending. It is the path used to load the events after a snapshot.
func (repo *Repository) GetLatestEvents(
	ctx context.Context,
	entityID evt.EntityID,
	lastSequence evt.EventSequence,
) ([]evt.SerializedEvent, error) {
	query := fmt.Sprintf(
		`SELECT %s FROM %s WHERE entity_id = $1 AND sequence > $2 ORDER BY sequence ASC`,
		eventColumns, repo.eventsTable,
	)

	rows, err := repo.db.Query(ctx, query, string(entityID), int64(lastSequence))
	if err != nil {
		return nil, fmt.Errorf("postgres: get latest events: %w", err)
	}

	return scanEvents(rows)
}

// GetSnapshot returns the entity's durable snapshot, or nil when none exists.
func (repo *Repository) GetSnapshot(
	ctx context.Context,
	entityID evt.EntityID,
) (*evt.SerializedSnapshot, error) {
	query := fmt.Sprintf(
		`SELECT entity_type, entity_id, sequence, event_sequence, payload FROM %s WHERE entity_id = $1`,
		repo.snapshotsTable,
	)

	var (
		snapshot      evt.SerializedSnapshot
		sequence      int64
		eventSequence int64
	)

	err := repo.db.QueryRow(ctx, query, string(entityID)).Scan(
		&snapshot.EntityType,
		&snapshot.EntityID,
		&sequence,
		&eventSequence,
		&snapshot.Payload,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("postgres: get snapshot: %w", err)
	}

	snapshot.Sequence = evt.EventSequence(sequence)
	snapshot.EventSequence = evt.EventSequence(eventSequence)

	return &snapshot, nil
}

// inTx runs fn inside a transaction, committing on success and rolling back on error or panic.
func (repo *Repository) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := repo.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin: %w", err)
	}

	// Roll back on any early return — an error from fn or a panic unwinding through it. This is a
	// no-op once Commit has succeeded, so the happy path is unaffected.
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(tx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return classifyWriteError(fmt.Errorf("postgres: commit: %w", err))
	}

	return nil
}

// insertEvents inserts a batch of events, translating a unique-constraint violation into an
// evt.ConflictError so optimistic-concurrency failures are detectable by callers.
func (repo *Repository) insertEvents(ctx context.Context, db DB, events []evt.SerializedEvent) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		repo.eventsTable,
	)

	for _, event := range events {
		metadata, err := json.Marshal(event.Metadata)
		if err != nil {
			return fmt.Errorf("postgres: marshal metadata: %w", err)
		}

		_, err = db.Exec(ctx, query,
			string(event.EntityID),
			int64(event.Sequence),
			string(event.ID),
			string(event.Type),
			int64(event.Version),
			string(event.EntityType),
			event.Payload,
			metadata,
		)
		if err != nil {
			return classifyWriteError(fmt.Errorf("postgres: insert event %s: %w", event.ID, err))
		}
	}

	return nil
}

// upsertSnapshot writes (or replaces) an entity's durable snapshot. The conflict guard keeps the
// recorded event_sequence monotonic: a write whose snapshot covers fewer events than the stored one
// is ignored, so a snapshot can never regress the authoritative compaction floor. This mirrors the
// DynamoDB backend's monotonic snapshot semantics. On the normal Store path the same-transaction
// event insert already rejects a stale commit via the (entity_id, sequence) primary key; this guard
// additionally protects any out-of-band snapshot writer.
func (repo *Repository) upsertSnapshot(ctx context.Context, db DB, snapshot evt.SerializedSnapshot) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (entity_id, entity_type, sequence, event_sequence, payload, updated_at)
		 VALUES ($1, $2, $3, $4, $5, now())
		 ON CONFLICT (entity_id) DO UPDATE SET
		     entity_type = EXCLUDED.entity_type,
		     sequence = EXCLUDED.sequence,
		     event_sequence = EXCLUDED.event_sequence,
		     payload = EXCLUDED.payload,
		     updated_at = now()
		 WHERE EXCLUDED.event_sequence >= %s.event_sequence`,
		repo.snapshotsTable, repo.snapshotsTable,
	)

	_, err := db.Exec(ctx, query,
		string(snapshot.EntityID),
		string(snapshot.EntityType),
		int64(snapshot.Sequence),
		int64(snapshot.EventSequence),
		snapshot.Payload,
	)
	if err != nil {
		return fmt.Errorf("postgres: upsert snapshot: %w", err)
	}

	return nil
}
