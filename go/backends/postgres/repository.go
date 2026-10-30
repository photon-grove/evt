package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/internal/eventjson"
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

// reservedInfix derives the engine-row relation names from the configured events table name
// ({events}__evt_features today). A configured table name may not contain it, so one log's events
// or snapshots table can never equal another log's derived relation.
const reservedInfix = "__evt_"

// reservedKeywords are PostgreSQL's reserved keywords (pg_get_keywords() categories R and T), which
// cannot be used as unquoted table names.
var reservedKeywords = map[string]struct{}{}

func init() {
	for _, keyword := range strings.Fields(`
		all analyse analyze and any array as asc asymmetric authorization binary both case cast check
		collate collation column concurrently constraint create cross current_catalog current_date
		current_role current_schema current_time current_timestamp current_user default deferrable desc
		distinct do else end except false fetch for foreign freeze from full grant group having ilike in
		initially inner intersect into is isnull join lateral leading left like limit localtime
		localtimestamp natural not notnull null offset on only or order outer overlaps placing primary
		references returning right select session_user similar some symmetric system_user table
		tablesample then to trailing true union unique user using variadic verbose when where window with`) {
		reservedKeywords[keyword] = struct{}{}
	}
}

// Keep every derived relation within PostgreSQL's 63-byte identifier limit.
const (
	maxEventsTableNameLen    = 63 - len("__evt_rebuild_generation_pkey")
	maxSnapshotsTableNameLen = 63 - len("_pkey")
)

// entityLockQuery takes the per-entity transaction lock ($1 is the entity ID) that serializes
// commits and snapshot writes for one entity, so a snapshot cannot raise the floor while a commit
// below it is in flight. The Rust crate runs this exact statement, so writers in either language
// exclude each other.
const entityLockQuery = "SELECT pg_advisory_xact_lock(1702261760, hashtext($1))"

// validIdentifier reports whether a table name is safe to interpolate unquoted.
func validIdentifier(name string) bool {
	_, reserved := reservedKeywords[strings.ToLower(name)]

	return len(name) <= 63 && identifierPattern.MatchString(name) && !reserved
}

// sharedRelationName returns a name that two relations created by EnsureSchema would share, or ""
// when there is none: the tables, the reserved entity-type index, and each table's primary key.
// CREATE TABLE IF NOT EXISTS would otherwise skip a table whose name an index already took.
// Unquoted identifiers fold to lower case, so names are compared that way.
func sharedRelationName(eventsTable, snapshotsTable string) string {
	events, snapshots := strings.ToLower(eventsTable), strings.ToLower(snapshotsTable)
	relations := make([]string, 0, 15)
	relations = append(relations, events, events+"_pkey", events+"__evt_entity_type_idx", snapshots, snapshots+"_pkey")
	for _, suffix := range []string{"command_outcomes", "features", "rebuild_lock", "rebuild_generation", "stream_heads"} {
		name := events + reservedInfix + suffix
		relations = append(relations, name, name+"_pkey")
	}

	for i, relation := range relations {
		if slices.Contains(relations[:i], relation) {
			return relation
		}
	}

	return ""
}

// DB is the minimal subset of the pgx API the Repository depends on. Both *pgxpool.Pool and pgx.Tx
// satisfy it, so callers can pass a pool (the common case) or an ambient transaction. It is the
// abstraction point for the backend, mirroring dynamo.Client.
//
// An ambient transaction must run at READ COMMITTED, the level every engine transaction needs
// (beginReadCommitted). PostgreSQL cannot change the level of a transaction already under way, so
// the Repository refuses to begin inside one at REPEATABLE READ or SERIALIZABLE.
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
	features       *FeatureMarker   // mandatory feature validation (C9)
	viewWriter     ViewWriter       // writes commit-wide view operations (C7); nil refuses them
	now            func() time.Time // clock for rebuild lease expiry; nil means time.Now

	// rebuildReady records that the rebuild fence's relations exist, so a commit's lock assertion
	// probes the catalog only until the first time it finds them. Shared by copies of the
	// repository, like the database handle it describes.
	rebuildReady *atomic.Bool
}

// Compile-time checks that the Repository satisfies the core contract and every optional capability
// the framework detects by type assertion.
var (
	_ evt.Repository              = (*Repository)(nil)
	_ evt.TransactionalRepository = (*Repository)(nil)
	_ evt.OutcomeRepository       = (*Repository)(nil)
	_ evt.RebuildFenceProvider    = (*Repository)(nil)
	_ evt.Compactor               = (*Repository)(nil)
	_ evt.SnapshotStreamer        = (*Repository)(nil)
	_ evt.EntityHeadStreamer      = (*Repository)(nil)
	_ evt.EntityHeadVisitor       = (*Repository)(nil)
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

// WithFeatureMarker returns a shallow copy of the repository whose commits enforce the store's
// feature manifest (ADR-0004 C9): the first commit validates the {events}__evt_features manifest
// against the supported feature set and caches its version, and every commit then re-checks the
// version inside its own transaction under the shared advisory lock, so a manifest declared by
// another writer (say, a Rust service using an extension this writer does not model) fails the
// check and re-validates; an unsupported pair refuses the writer outright.
//
// The default marker supports no extensions. Passing nil restores that default; it never
// disables enforcement.
// Create markers with repo.FeatureMarker. A marker bound to another events table or client
// panics with *evt.FeatureMarkerTableMismatchError or *evt.FeatureMarkerClientMismatchError.
func (repo *Repository) WithFeatureMarker(marker *FeatureMarker) *Repository {
	r := *repo
	r.features = marker
	if r.features == nil {
		r.features = r.FeatureMarker(nil)
	}
	if err := r.checkFeatureMarkerTable(); err != nil {
		panic(err)
	}

	return &r
}

func (repo *Repository) checkFeatureMarkerTable() error {
	if repo.features.eventsTable != repo.eventsTable {
		return &evt.FeatureMarkerTableMismatchError{
			EventsTable: repo.eventsTable, MarkerEventsTable: repo.features.eventsTable,
		}
	}

	// Interface equality is safe only for comparable client handles (normally pointers).
	// Fail closed for other handles rather than compare their configuration or contents.
	if !comparableHandle(repo.db) || !comparableHandle(repo.features.db) || repo.db != repo.features.db {
		return &evt.FeatureMarkerClientMismatchError{}
	}

	return nil
}

// NewRepository constructs a Repository over the given pgx handle (typically a *pgxpool.Pool). It
// panics if a configured table name is not a valid SQL identifier, since that is a programming
// error in wiring rather than a runtime condition.
func NewRepository(db DB, opts ...Option) *Repository {
	repo := &Repository{
		db:             db,
		eventsTable:    defaultEventsTable,
		snapshotsTable: defaultSnapshotsTable,
		rebuildReady:   new(atomic.Bool),
	}

	for _, opt := range opts {
		opt(repo)
	}

	for _, table := range []struct {
		name   string
		maxLen int
	}{
		{repo.eventsTable, maxEventsTableNameLen},
		{repo.snapshotsTable, maxSnapshotsTableNameLen},
	} {
		if !validIdentifier(table.name) || len(table.name) > table.maxLen {
			panic(fmt.Sprintf("postgres: invalid table name %q", table.name))
		}
	}

	// The reserved infix derives the engine-row table names (the feature marker today, command
	// outcomes and the rebuild fence as their capabilities land). A configured name containing it
	// could collide with another log's derived relation, so it is rejected outright.
	for _, name := range []string{repo.eventsTable, repo.snapshotsTable} {
		if strings.Contains(strings.ToLower(name), reservedInfix) {
			panic(fmt.Sprintf("postgres: table name %q contains the reserved infix %q", name, reservedInfix))
		}
	}

	if shared := sharedRelationName(repo.eventsTable, repo.snapshotsTable); shared != "" {
		panic(fmt.Sprintf("postgres: tables %q and %q would both use the name %q",
			repo.eventsTable, repo.snapshotsTable, shared))
	}

	repo.eventsTable = strings.ToLower(repo.eventsTable)
	repo.snapshotsTable = strings.ToLower(repo.snapshotsTable)
	repo.features = repo.FeatureMarker(nil)

	return repo
}

// EnsureSchema creates the event-log and snapshot tables if they do not already exist. It is
// idempotent and safe to call on every startup; the integration suite calls it during setup. The
// Repository owns this DDL because the relational schema must stay in lockstep with the Go types
// that marshal into and out of it.
func (repo *Repository) EnsureSchema(ctx context.Context) error {
	return repo.inTx(ctx, func(tx pgx.Tx) error {
		// IF NOT EXISTS alone can still race in PostgreSQL's catalogs. Serialize DDL
		// across processes (including Rust) before checking or creating any relation.
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(1702261762, hashtext($1))", repo.eventsTable); err != nil {
			return fmt.Errorf("postgres: lock schema: %w", err)
		}
		if _, err := tx.Exec(ctx, repo.schemaDDL()); err != nil {
			return fmt.Errorf("postgres: ensure schema: %w", err)
		}

		return nil
	})
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
CREATE INDEX IF NOT EXISTS %[1]s__evt_entity_type_idx ON %[1]s (entity_type);
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_index
        WHERE indexrelid = to_regclass('%[1]s_entity_type_idx')
          AND indrelid = '%[1]s'::regclass
    ) THEN
        DROP INDEX %[1]s_entity_type_idx;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS %[2]s (
    entity_id      text   PRIMARY KEY,
    entity_type    text   NOT NULL,
    sequence       bigint NOT NULL,
    event_sequence bigint NOT NULL,
    payload        bytea  NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS %[1]s__evt_command_outcomes (
    command_id text        PRIMARY KEY,
    status     text        NOT NULL CHECK (status IN ('accepted', 'rejected')),
    digest     text        NOT NULL,
    response   bytea,
    events     jsonb       NOT NULL DEFAULT '[]'::jsonb,
    rejection  jsonb,
    code       text,
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((status = 'rejected' AND rejection IS NOT NULL AND code IS NOT NULL)
           OR (status = 'accepted' AND rejection IS NULL AND code IS NULL))
);

CREATE TABLE IF NOT EXISTS %[1]s__evt_features (
    id                integer PRIMARY KEY,
    version           bigint NOT NULL,
    features          text   NOT NULL,
    history_dependent text   NOT NULL DEFAULT '[]',
    retention         text   NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS %[1]s__evt_rebuild_lock (
    id               integer PRIMARY KEY,
    owner            text        NOT NULL,
    generation       bigint      NOT NULL,
    lease_expires_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS %[1]s__evt_rebuild_generation (
    id             integer PRIMARY KEY,
    value          bigint   NOT NULL,
    force_released boolean  NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS %[1]s__evt_stream_heads (
    entity_id   text   PRIMARY KEY,
    entity_type text   NOT NULL,
    head        bigint NOT NULL CHECK (head >= 1)
);
`, repo.eventsTable, repo.snapshotsTable)
}

// Commit persists the events in a SerializedResult inside a single transaction. A duplicate
// (entity_id, sequence) pair violates the primary key and is returned as an evt.ConflictError,
// giving the optimistic-concurrency guarantee the contract requires.
//
// Commit-wide view operations (ADR 0004, C7) execute in the same transaction, after the events and
// before the snapshots, through the repository's ViewWriter (WithViewWriter): evt defines no
// PostgreSQL view schema, so the application's writer turns each operation into its own SQL. A
// commit that carries view operations and no writer is refused with *evt.ViewsNotConfiguredError.
// The operations of a transaction group built for another storage type are refused with
// *evt.ProjectorOperationsUnsupportedError before any write instead of being dropped.
func (repo *Repository) Commit(ctx context.Context, res evt.SerializedResult) error {
	if err := repo.checkFeatureMarkerTable(); err != nil {
		return err
	}

	// Projector operations need a backing event in the same commit (ADR-0012); refuse them even
	// when nothing else would be written.
	if err := evt.CheckProjectorOperations(res); err != nil {
		return err
	}

	// This backend executes no transaction group: fail closed rather than commit the events and
	// silently drop the operations of a group built for another storage type.
	if err := evt.CheckLegacyProjectorOperationsSupported("postgres", res); err != nil {
		return err
	}

	if len(res.Events) == 0 && len(res.Snapshots) == 0 && len(res.Fences) == 0 && res.Outcome == nil {
		return nil
	}

	normalized, err := evt.NormalizeCommit(res)
	if err != nil {
		return err
	}

	// The statements are built before the budget so each view operation counts as one action sized
	// by its SQL text, and before any write so a writer that refuses one fails the whole commit.
	views, err := repo.buildViews(normalized.Views)
	if err != nil {
		return err
	}

	if err = commitBudget(normalized, views); err != nil {
		return err
	}

	// A store with no data declares stream-heads itself; one that holds data without it is refused.
	if err = repo.ensureStreamHeads(ctx); err != nil {
		return err
	}

	err = repo.inTxMapping(ctx, func(tx pgx.Tx) error { return repo.commitInTx(ctx, tx, normalized, views) }, len(views) > 0)
	if err == nil {
		return nil
	}

	// A failed COMMIT with an unknown result of a commit that carries an outcome is an ambiguous
	// write the engine resolves by re-reading the outcome row.
	var indeterminate *indeterminateCommitError
	if errors.As(err, &indeterminate) && normalized.Outcome != nil {
		return &evt.AmbiguousWriteError{CommandID: normalized.Outcome.CommandID, Cause: indeterminate.err}
	}

	return err
}

// commitInTx writes one normalized commit inside an open transaction, in the order the storage spec
// pins (Stream Heads, PostgreSQL): the entity locks, the feature-marker check, a probe for an
// existing outcome row, every head statement and fence read (evaluated without stopping at the
// first failure, then ranked), the event inserts, the snapshot upserts, and last the outcome
// insert.
func (repo *Repository) commitInTx(
	ctx context.Context,
	tx pgx.Tx,
	normalized evt.SerializedResult,
	views []builtView,
) error {
	// Every touched stream — written and fenced — takes its advisory lock in canonical
	// (sorted) order, so concurrent multi-stream commits cannot deadlock.
	if err := repo.lockTransactionEntities(ctx, tx, normalized); err != nil {
		return err
	}

	manifest, err := repo.checkFeaturesManifest(ctx, tx)
	if err != nil {
		return err
	}

	// Fences are refused for entity types durably declared with retention. The refusal consults
	// the manifest in this transaction, not this instance's configuration, and needs no read of
	// the lock, so it precedes the rebuild-lock assertion as it does on DynamoDB.
	if err = evt.CheckFenceRetention(normalized.Fences, manifest); err != nil {
		return err
	}

	// The rebuild-lock assertion (C10): an unsettled generation — any lock row, an expired one
	// included, or a force-release no rebuild has finished since — refuses the commit, right after
	// the marker check and before anything is written.
	if err = repo.assertNoRebuildLock(ctx, tx); err != nil {
		return err
	}

	// An existing outcome row for the command outranks drift, gap, and conflicts, as the outcome
	// put does on DynamoDB. The final insert's unique violation still resolves a real race.
	if err = repo.checkOutcomeAbsent(ctx, tx, normalized.Outcome); err != nil {
		return err
	}

	if err = repo.checkHeads(ctx, tx, normalized); err != nil {
		return err
	}

	if err = repo.insertEvents(ctx, tx, normalized.Events); err != nil {
		return repo.classifyInsertFailure(ctx, tx, normalized.Events, err)
	}

	// The view statements run after the events, so a writer that lost a race fails on its head
	// statement (a conflict) before it reaches a view row, and before the snapshots.
	if err = execViews(ctx, tx, views); err != nil {
		return err
	}

	if err = repo.commitSnapshots(ctx, tx, normalized); err != nil {
		return err
	}

	// The outcome insert runs last, after the snapshot upserts (storage spec, PostgreSQL).
	return repo.insertOutcome(ctx, tx, normalized.Outcome)
}

// lockTransactionEntities takes the advisory lock for every stream a transactional commit
// touches: the written streams' event IDs and the fenced read-only streams.
func (repo *Repository) lockTransactionEntities(ctx context.Context, tx pgx.Tx, result evt.SerializedResult) error {
	ids := make([]string, 0, len(result.Events)+len(result.Fences))
	for _, event := range result.Events {
		if !slices.Contains(ids, string(event.EntityID)) {
			ids = append(ids, string(event.EntityID))
		}
	}
	for _, fence := range result.Fences {
		if !slices.Contains(ids, string(fence.EntityID)) {
			ids = append(ids, string(fence.EntityID))
		}
	}
	slices.Sort(ids)

	for _, id := range ids {
		if _, err := tx.Exec(ctx, entityLockQuery, id); err != nil {
			return classifyWriteError(fmt.Errorf("postgres: lock entity %s: %w", id, err))
		}
	}

	return nil
}

// commitSnapshots upserts each commit snapshot under its stream's lock, checking the same
// conditions the DynamoDB snapshot put expresses: the stored snapshot is this one's predecessor
// and the commit starts above its floor.
func (repo *Repository) commitSnapshots(ctx context.Context, tx pgx.Tx, result evt.SerializedResult) error {
	for _, snapshot := range result.Snapshots {
		// The snapshot sequence chain (storage spec, PostgreSQL): the same read as Rust's
		// check_snapshot_predecessor, under the entity's advisory lock.
		var stored struct{ sequence int64 }
		err := tx.QueryRow(ctx,
			fmt.Sprintf(`SELECT sequence FROM %s WHERE entity_id = $1`, repo.snapshotsTable),
			string(snapshot.EntityID),
		).Scan(&stored.sequence)
		if errors.Is(err, pgx.ErrNoRows) {
			if snapshot.Sequence != 1 {
				return transientConflict(fmt.Sprintf(
					"postgres: snapshot sequence %d for %s has no stored predecessor",
					snapshot.Sequence, snapshot.EntityID))
			}
		} else if err != nil {
			return fmt.Errorf("postgres: read snapshot: %w", err)
		} else if stored.sequence != int64(snapshot.Sequence)-1 {
			return transientConflict(fmt.Sprintf(
				"postgres: snapshot sequence moved: stored %d, commit writes %d",
				stored.sequence, snapshot.Sequence))
		}

		if err := repo.upsertSnapshot(ctx, tx, snapshot); err != nil {
			return err
		}
	}

	return nil
}

// AtomicTransactions declares that Commit writes every stream, snapshot, and fence of a result in
// one database transaction (evt.TransactionalRepository).
func (repo *Repository) AtomicTransactions() {}

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
	if err := repo.checkFeatureMarkerTable(); err != nil {
		return err
	}

	if err := evt.CheckProjectorOperations(res); err != nil {
		return err
	}

	if err := evt.CheckLegacyProjectorOperationsSupported("postgres", res); err != nil {
		return err
	}

	if len(res.Events) == 0 {
		return fmt.Errorf("postgres: commit with snapshot requires at least one event")
	}

	if err := evt.CheckReservedEntityID(entityID); err != nil {
		return err
	}

	if err := evt.CheckReservedEntityIDs(res.Events); err != nil {
		return err
	}

	if err := checkContiguous(res.Events); err != nil {
		return err
	}

	// Route through the multi-stream commit path: the snapshot is written in the same
	// transaction as its events, under the same conditions.
	res.Snapshots = append(res.Snapshots, evt.SerializedSnapshot{
		EntityType:    entityType,
		EntityID:      entityID,
		Sequence:      currentSnapshot,
		EventSequence: res.Events[len(res.Events)-1].Sequence,
		Payload:       payload,
	})

	return repo.Commit(ctx, res)
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

// GetRecentEvents returns at most limit events for an entity, newest-first (descending sequence).
// A limit below 1 is a programmer error returned before any request. PostgreSQL's LIMIT returns
// the same at-most-limit window in one statement.
func (repo *Repository) GetRecentEvents(
	ctx context.Context,
	entityID evt.EntityID,
	limit int,
) ([]evt.SerializedEvent, error) {
	if limit < 1 {
		return nil, &evt.InvalidArgumentError{Argument: "limit", Message: fmt.Sprintf("must be at least 1, got %d", limit)}
	}

	query := fmt.Sprintf(
		`SELECT %s FROM %s WHERE entity_id = $1 ORDER BY sequence DESC LIMIT $2`,
		eventColumns, repo.eventsTable,
	)

	rows, err := repo.db.Query(ctx, query, string(entityID), int64(limit))
	if err != nil {
		return nil, fmt.Errorf("postgres: get recent events: %w", err)
	}

	return scanEvents(rows)
}

// GetSnapshot returns the entity's durable snapshot, or nil when none exists.
func (repo *Repository) GetSnapshot(
	ctx context.Context,
	entityID evt.EntityID,
) (*evt.SerializedSnapshot, error) {
	return repo.getSnapshot(ctx, repo.db, entityID)
}

// getSnapshot reads the entity's durable snapshot through db, so a caller holding a transaction
// reads inside it.
func (repo *Repository) getSnapshot(
	ctx context.Context,
	db DB,
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

	err := db.QueryRow(ctx, query, string(entityID)).Scan(
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

// checkFeatures runs the mandatory commit-time feature-marker check.
// It runs after the per-entity locks, before the inserts, matching the control-checks-first order
// the DynamoDB transaction pins.
func (repo *Repository) checkFeatures(ctx context.Context, tx pgx.Tx) error {
	return repo.features.checkInTx(ctx, tx)
}

// checkFeaturesManifest is checkFeatures, returning the manifest it validated against so callers
// can consult durable declarations this instance did not configure.
func (repo *Repository) checkFeaturesManifest(ctx context.Context, tx pgx.Tx) (*evt.FeatureManifest, error) {
	return repo.features.checkManifestTx(ctx, tx)
}

// beginReadCommitted starts a transaction at READ COMMITTED, whatever the server's or role's
// default_transaction_isolation. The engine's transactions read fence rows (the rebuild lock and
// generation, stream heads, the feature manifest) after waiting for advisory locks, and only READ
// COMMITTED takes each statement's snapshot after the wait: a REPEATABLE READ snapshot, taken at the
// first statement, would miss a rebuild or a commit that landed while the transaction waited.
func beginReadCommitted(ctx context.Context, db DB) (pgx.Tx, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return nil, err
	}

	if _, err := tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"); err != nil {
		_ = tx.Rollback(ctx) //nolint:errcheck // the SET's error is the one to report

		return nil, fmt.Errorf("set READ COMMITTED (an ambient transaction must already run at it): %w", err)
	}

	return tx, nil
}

// inTx runs fn inside a transaction, committing on success and rolling back on error or panic.
func (repo *Repository) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	return repo.inTxMapping(ctx, fn, false)
}

// inTxMapping is inTx for a transaction that may carry view operations: an integrity violation at
// COMMIT of one is the projector's failure (a deferred constraint on its table), not the event log's
// conflict.
func (repo *Repository) inTxMapping(ctx context.Context, fn func(pgx.Tx) error, carriesViews bool) error {
	tx, err := beginReadCommitted(ctx, repo.db)
	if err != nil {
		return fmt.Errorf("postgres: begin: %w", err)
	}

	// Roll back on any early return — an error from fn or a panic unwinding through it. This is a
	// no-op once Commit has succeeded, so the happy path is unaffected.
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // after Commit this is ErrTxClosed; on failure fn's error is the one to report

	if err := fn(tx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		if carriesViews {
			if projectorFailure := viewCommitError(err); projectorFailure != nil {
				return projectorFailure
			}
		}

		failure := classifyWriteError(fmt.Errorf("postgres: commit: %w", err))

		// A database error at COMMIT is a definitive refusal. Anything else — a lost connection, a
		// cancelled context — leaves the caller unable to know whether the transaction applied.
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) {
			return &indeterminateCommitError{err: failure}
		}

		return failure
	}

	return nil
}

// indeterminateCommitError marks a COMMIT whose result the caller cannot know. It is transparent
// to errors.Is and errors.As; Commit converts it to evt.AmbiguousWriteError when the commit
// carries an outcome.
type indeterminateCommitError struct{ err error }

func (e *indeterminateCommitError) Error() string { return e.err.Error() }
func (e *indeterminateCommitError) Unwrap() error { return e.err }

// checkContiguous rejects events whose sequences are not contiguous (each one more than the last)
// for each entity. The head condition reads the first event's sequence and the snapshot records the
// last, so a commit like [6, 5] after compacting through 5 would recreate sequence 5, and loads reject a
// stream with a gap such as [1, 3]. Sequences start at 1: DynamoDB keeps the snapshot at sk = 0 and
// reads events after it.
func checkContiguous(events []evt.SerializedEvent) error {
	last := make(map[evt.EntityID]evt.EventSequence, 1)

	for _, event := range events {
		previous, seen := last[event.EntityID]
		if !seen && event.Sequence < 1 {
			return fmt.Errorf("postgres: event %s has sequence %d; sequences start at 1", event.ID, event.Sequence)
		}
		// Nothing follows the maximum sequence. Checking it first keeps previous+1 from wrapping.
		if seen && (previous == math.MaxInt || event.Sequence != previous+1) {
			return fmt.Errorf("postgres: event %s follows sequence %d; sequences must be contiguous", event.ID, previous)
		}
		last[event.EntityID] = event.Sequence
	}

	return nil
}

// insertEvents inserts a batch of events. A sequence already stored is a *duplicateEventError;
// commitInTx classifies it.
func (repo *Repository) insertEvents(ctx context.Context, db DB, events []evt.SerializedEvent) error {
	query := fmt.Sprintf(
		`INSERT INTO %s (entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (entity_id, sequence) DO NOTHING`,
		repo.eventsTable,
	)

	for _, event := range events {
		metadata, err := eventjson.MarshalMetadata(event.Metadata)
		if err != nil {
			return fmt.Errorf("postgres: marshal metadata: %w", err)
		}

		tag, err := db.Exec(ctx, query,
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
			return fmt.Errorf("postgres: insert event %s: %w", event.ID, err)
		}

		// The head statement of the stream passed, so a row already stored at this sequence sits
		// above the head. A plain INSERT would abort the transaction; DO NOTHING lets the commit
		// describe it.
		if tag.RowsAffected() != 1 {
			return &duplicateEventError{event: event}
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

// comparableHandle reports whether a DB handle can be compared with == without panicking; nil is
// comparable. Non-comparable handles (for example structs holding slices) fail the binding closed.
func comparableHandle(db DB) bool {
	return db == nil || reflect.ValueOf(db).Comparable()
}
