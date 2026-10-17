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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/internal/eventjson"
	"github.com/photon-grove/evt/policy"
	"github.com/photon-grove/evt/result" //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
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
	relations := make([]string, 0, 13)
	relations = append(relations, events, events+"_pkey", events+"__evt_entity_type_idx", snapshots, snapshots+"_pkey")
	for _, suffix := range []string{"command_outcomes", "features", "rebuild_lock", "rebuild_generation"} {
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
	features       *FeatureMarker // mandatory feature validation (C9)
}

// Compile-time checks that the Repository satisfies the core contract and every optional capability
// the framework detects by type assertion.
var (
	_ evt.Repository              = (*Repository)(nil)
	_ evt.TransactionalRepository = (*Repository)(nil)
	_ evt.OutcomeRepository       = (*Repository)(nil)
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
`, repo.eventsTable, repo.snapshotsTable)
}

// Commit persists the events in a SerializedResult inside a single transaction. A duplicate
// (entity_id, sequence) pair violates the primary key and is returned as an evt.ConflictError,
// giving the optimistic-concurrency guarantee the contract requires.
//
// This backend is an event log only: it does not execute res.Transaction, since projector view
// writes are backend-specific (the only TransactionGroup implementation today is DynamoDB's) and no
// PostgreSQL view store exists yet. A result carrying any projector operation is refused with
// *evt.ProjectorOperationsUnsupportedError before any write instead of dropping the operations.
// Adding view support later would extend this to write the view rows in the same transaction as
// the events, preserving atomicity.
func (repo *Repository) Commit(ctx context.Context, res evt.SerializedResult) error {
	if err := repo.checkFeatureMarkerTable(); err != nil {
		return err
	}

	// Projector operations need a backing event in the same commit (ADR-0012); refuse them even
	// when nothing else would be written.
	if err := evt.CheckProjectorOperations(res); err != nil {
		return err
	}

	// This backend executes no projector groups until commit-wide projector support lands (E2.6).
	// Fail closed rather than commit the events and silently drop the view writes.
	if err := evt.CheckProjectorOperationsSupported("postgres", res); err != nil {
		return err
	}

	if len(res.Events) == 0 && len(res.Snapshots) == 0 && len(res.Fences) == 0 && res.Outcome == nil {
		return nil
	}

	normalized, err := evt.NormalizeCommit(res)
	if err != nil {
		return err
	}

	if err = evt.PreflightCommit(normalized); err != nil {
		return err
	}

	err = repo.inTx(ctx, func(tx pgx.Tx) error { return repo.commitInTx(ctx, tx, normalized) })
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

// commitInTx writes one normalized commit inside an open transaction: the entity locks, the
// feature-marker check, the continuity, event, floor, snapshot, and fence checks, and last the
// outcome insert.
func (repo *Repository) commitInTx(ctx context.Context, tx pgx.Tx, normalized evt.SerializedResult) error {
	// Every touched stream — written and fenced — takes its advisory lock in canonical
	// (sorted) order, so concurrent multi-stream commits cannot deadlock.
	if err := repo.lockTransactionEntities(ctx, tx, normalized); err != nil {
		return err
	}

	manifest, err := repo.checkFeaturesManifest(ctx, tx)
	if err != nil {
		return err
	}

	// Fences are refused for entity types durably declared with retention: TTL deletes reopen
	// fenced slots without leaving a successor snapshot. The refusal consults the manifest in
	// this transaction, not this instance's configuration.
	if err = evt.CheckFenceRetention(normalized.Fences, manifest); err != nil {
		return err
	}

	// Under the streams' advisory locks, every written stream must continue exactly from its
	// stored head: a gap is refused before anything is inserted.
	if err = repo.checkContinuations(ctx, tx, normalized.Events); err != nil {
		return err
	}

	if err = repo.insertEvents(ctx, tx, normalized.Events); err != nil {
		return err
	}

	if err = repo.checkFloor(ctx, tx, normalized.Events); err != nil {
		return err
	}

	if err = repo.commitSnapshots(ctx, tx, normalized); err != nil {
		return err
	}

	if err = repo.checkFences(ctx, tx, normalized.Fences); err != nil {
		return err
	}

	// The outcome insert runs last, after the snapshot upserts (storage spec, PostgreSQL).
	return repo.insertOutcome(ctx, tx, normalized.Outcome)
}

// checkContinuations asserts, for each written stream whose first sequence is above 1, that the
// stored head is the sequence just before it. The head is the larger of the stream's highest event
// and its snapshot's event sequence, because compaction can delete every covered event. The
// per-entity advisory lock holds the stream steady through the assertion, so no writer can move
// the head between this check and the insert. A first sequence at or below the head is left to the
// primary key and the floor check, which report it as an ordinary conflict.
func (repo *Repository) checkContinuations(ctx context.Context, tx pgx.Tx, events []evt.SerializedEvent) error {
	seen := make(map[evt.EntityID]bool)

	for _, event := range events {
		if seen[event.EntityID] {
			continue
		}
		seen[event.EntityID] = true

		if event.Sequence <= 1 {
			continue
		}

		var head int64
		err := tx.QueryRow(ctx, fmt.Sprintf(
			`SELECT GREATEST(
			         COALESCE((SELECT MAX(sequence) FROM %s WHERE entity_id = $1), 0),
			         COALESCE((SELECT MAX(event_sequence) FROM %s WHERE entity_id = $1), 0))`,
			repo.eventsTable, repo.snapshotsTable,
		), string(event.EntityID)).Scan(&head)
		if err != nil {
			return fmt.Errorf("postgres: check continuation %s: %w", event.EntityID, err)
		}

		if int64(event.Sequence) > head+1 {
			return &policy.ClassifiedError{
				Class: policy.ClassTransient,
				Err:   &evt.SequenceGapError{EntityID: event.EntityID, After: evt.EventSequence(head), Found: event.Sequence},
			}
		}
	}

	return nil
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
		var stored struct{ sequence, eventSequence int64 }
		err := tx.QueryRow(ctx,
			fmt.Sprintf(`SELECT sequence, event_sequence FROM %s WHERE entity_id = $1`, repo.snapshotsTable),
			string(snapshot.EntityID),
		).Scan(&stored.sequence, &stored.eventSequence)
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

// checkFences asserts, for each fenced read-only stream, that no stored event and no snapshot
// covers past the fenced head — the SQL form of the slot check plus the snapshot companion
// (ADR 0004, C2). The locks above hold the stream steady through the assertions. A failed
// assertion is a typed fence conflict carrying the fence's provenance.
func (repo *Repository) checkFences(ctx context.Context, tx pgx.Tx, fences []evt.ReadFence) error {
	for _, fence := range fences {
		var eventHead, snapshotHead int64
		err := tx.QueryRow(ctx, fmt.Sprintf(
			`SELECT COALESCE((SELECT MAX(sequence) FROM %s WHERE entity_id = $1), 0),
			       COALESCE((SELECT MAX(event_sequence) FROM %s WHERE entity_id = $1), 0)`,
			repo.eventsTable, repo.snapshotsTable,
		), string(fence.EntityID)).Scan(&eventHead, &snapshotHead)
		if err != nil {
			return fmt.Errorf("postgres: check fence %s: %w", fence.EntityID, err)
		}

		if eventHead > int64(fence.Head) || snapshotHead > int64(fence.Head) {
			return policy.FenceConflict(fence)
		}
	}

	return nil
}

// AtomicTransactions declares that Commit writes every stream, snapshot, and fence of a result in
// one database transaction (evt.TransactionalRepository).
func (repo *Repository) AtomicTransactions() {}

// CommitStream commits each SerializedResult from the channel in its own transaction, collecting
// per-result errors without aborting the stream. The batch size is one result, which keeps each
// commit atomic and lets a single bad result fail in isolation.
//
// Deprecated: Cut in ADR-0006 (K1). Commit batched commands through the normal commit path
// instead. It will be removed in the next minor release.
func (repo *Repository) CommitStream(
	ctx context.Context,
	channel <-chan result.Result[evt.SerializedResult],
) []error {
	if err := repo.checkFeatureMarkerTable(); err != nil {
		// Drain the channel so producers never block on a refused stream.
		for range channel { //nolint:revive // Drain without reading so producers never block.
			continue
		}

		return []error{err}
	}

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
	if err := repo.checkFeatureMarkerTable(); err != nil {
		return err
	}

	if err := evt.CheckProjectorOperations(res); err != nil {
		return err
	}

	if err := evt.CheckProjectorOperationsSupported("postgres", res); err != nil {
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

// inTx runs fn inside a transaction, committing on success and rolling back on error or panic.
func (repo *Repository) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := repo.db.Begin(ctx)
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
// for each entity. The floor check reads the first event and the snapshot records the last, so a
// commit like [6, 5] after compacting through 5 would recreate sequence 5, and loads reject a
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

// checkFloor rejects a commit whose first event for an entity is at or below that entity's
// snapshot event_sequence. Compaction deletes the events a snapshot covers, so the primary key
// alone would let a writer that loaded before the snapshot recreate a compacted sequence. It runs
// after the inserts: a concurrent writer can only raise the floor past our first sequence by
// inserting that sequence, which our uncommitted insert already holds.
func (repo *Repository) checkFloor(ctx context.Context, tx pgx.Tx, events []evt.SerializedEvent) error {
	first := make(map[evt.EntityID]evt.EventSequence)
	for _, event := range events {
		if current, ok := first[event.EntityID]; !ok || event.Sequence < current {
			first[event.EntityID] = event.Sequence
		}
	}

	query := fmt.Sprintf(
		`SELECT event_sequence FROM %s WHERE entity_id = $1 AND event_sequence >= $2`,
		repo.snapshotsTable,
	)

	for entityID, sequence := range first {
		var floor int64

		err := tx.QueryRow(ctx, query, string(entityID), int64(sequence)).Scan(&floor)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return fmt.Errorf("postgres: check snapshot floor: %w", err)
		}

		return transientConflict(fmt.Sprintf(
			"postgres: event %d for %s is at or below the snapshot floor %d", sequence, entityID, floor,
		))
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
		metadata, err := eventjson.MarshalMetadata(event.Metadata)
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

// comparableHandle reports whether a DB handle can be compared with == without panicking; nil is
// comparable. Non-comparable handles (for example structs holding slices) fail the binding closed.
func comparableHandle(db DB) bool {
	return db == nil || reflect.ValueOf(db).Comparable()
}
