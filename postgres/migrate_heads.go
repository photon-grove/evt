package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/photon-grove/evt"
)

// The v0.8 to stream-heads migration on PostgreSQL (storage spec, Stream Heads, PostgreSQL head
// table; ADR 0007). The backfill, the verification, and the declaration are one transaction under
// the exclusive feature advisory lock, so a store is either migrated and declared or untouched.
// Head-aware commits wait on that lock. The transaction also takes a SHARE lock on the events and
// snapshots tables, which holds off an older writer that does not know the advisory lock for its
// duration (the operator must still keep it stopped afterwards: nothing can stop a writer that
// starts later).

// maxReportedHeadFindings bounds the findings one report carries; Total counts all of them.
const maxReportedHeadFindings = 1000

// StreamHeadsMigrateOptions are the operator's choices for MigrateStreamHeads.
type StreamHeadsMigrateOptions struct {
	// ConfirmWritersStopped states that the operator stopped and fenced every older writer
	// (migration step 1). It is required unless DryRun is set.
	ConfirmWritersStopped bool

	// DryRun counts the head rows the migration would insert in a read-only transaction. It takes
	// no lock and writes nothing, so it is safe beside live writers (its count is a snapshot).
	DryRun bool
}

// StreamHeadsRepairOptions are the operator's choices for RepairStreamHeads.
type StreamHeadsRepairOptions struct {
	// ConfirmWritersStopped is required unless DryRun is set.
	ConfirmWritersStopped bool

	// AllowLower also lowers a head row above the derived head. Lowering breaks head monotonicity
	// (H1), so every writer must be stopped.
	AllowLower bool

	// DryRun reports what a repair would change in a read-only transaction, taking no lock and
	// writing nothing.
	DryRun bool
}

// StreamHeadsMigrationReport is the result of MigrateStreamHeads.
type StreamHeadsMigrationReport struct {
	DryRun bool

	// Streams is the number of streams with events or a snapshot.
	Streams int64

	// Inserted is the number of head rows inserted (that would be, in a dry run).
	Inserted int64

	// Declared reports that this run declared stream-heads.
	Declared bool

	// Manifest is the manifest after the declaration.
	Manifest evt.FeatureManifest
}

// StreamHeadsVerifyReport is the result of VerifyStreamHeads.
type StreamHeadsVerifyReport struct {
	// Streams is the number of streams with events or a snapshot.
	Streams int64

	// Total is the number of failing streams; Findings lists up to the first 1000, by entity ID.
	Total    int64
	Findings []evt.HeadFinding
}

// OK reports whether every stream passed.
func (r StreamHeadsVerifyReport) OK() bool { return r.Total == 0 }

// StreamHeadsRepairReport is the result of RepairStreamHeads.
type StreamHeadsRepairReport struct {
	DryRun bool

	// Inserted, Raised, and Lowered count head rows created, raised to the derived head, and
	// lowered to it (AllowLower).
	Inserted int64
	Raised   int64
	Lowered  int64

	// Repaired lists, in a dry run, the findings the repair would fix. It is empty otherwise.
	Repaired []evt.HeadFinding

	// Left are the findings the repair did not fix: a higher head without AllowLower, and a head
	// row with no events or snapshot to derive from.
	Left []evt.HeadFinding
}

// derivedHeadsCTE is the common table expression every statement below starts from: one row per
// stream, with the head the log derives (invariant H4: the larger of the highest event sequence
// and the snapshot's event sequence) and the entity type of its highest row. Reserved namespaces
// are excluded.
func (repo *Repository) derivedHeadsCTE() string {
	return fmt.Sprintf(`
WITH sources AS (
    SELECT entity_id, entity_type, sequence AS head FROM %s
    UNION ALL
    SELECT entity_id, entity_type, event_sequence AS head FROM %s
), derived AS (
    SELECT entity_id,
           (array_agg(entity_type ORDER BY head DESC, entity_type))[1] AS entity_type,
           MAX(head) AS head
    FROM sources
    WHERE `+reservedExclusion+`
    GROUP BY entity_id
)`, repo.eventsTable, repo.snapshotsTable)
}

// headsRelation names the relation holding head rows, or an empty one when the table does not
// exist yet (a database that never ran the current EnsureSchema), so a read-only verification of
// an unmigrated store reports every stream as missing instead of failing.
func (repo *Repository) headsRelation(ctx context.Context, db DB) (string, error) {
	var exists bool

	err := db.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, repo.headsTable()).Scan(&exists)
	if err != nil {
		return "", fmt.Errorf("postgres: checking the heads table: %w", err)
	}

	if exists {
		return repo.headsTable(), nil
	}

	return `(SELECT NULL::text AS entity_id, NULL::text AS entity_type, NULL::bigint AS head WHERE false)`, nil
}

// lockMigration serializes the migration against head-aware writers and holds off older ones: the
// exclusive feature advisory lock (the one declarations take, which commits' shared lock waits
// on), then SHARE locks on the events and snapshots tables.
func (repo *Repository) lockMigration(ctx context.Context, tx pgx.Tx) error {
	lock := fmt.Sprintf("SELECT pg_advisory_xact_lock(%d, hashtext($1))", engineRowLockClass)
	if _, err := tx.Exec(ctx, lock, repo.eventsTable+":features"); err != nil {
		return fmt.Errorf("postgres: lock the feature marker: %w", err)
	}

	return repo.lockTables(ctx, tx)
}

// checkMigrationManifest refuses a store that declares a feature the migration does not model
// (journal in particular), with *evt.UnsupportedFeatureError.
func checkMigrationManifest(manifest *evt.FeatureManifest) error {
	if manifest == nil {
		return nil
	}

	return manifest.Validate(evt.SupportedFeatures(nil).WithStreamHeads())
}

// VerifyStreamHeads reports every stream whose head row is missing, lower, or higher than the
// derived head, and every head row with no stream behind it. It is read-only and sees one
// consistent snapshot.
func (repo *Repository) VerifyStreamHeads(ctx context.Context) (StreamHeadsVerifyReport, error) {
	manifest, err := repo.features.Manifest(ctx)
	if err != nil {
		return StreamHeadsVerifyReport{}, err
	}

	if err = checkMigrationManifest(manifest); err != nil {
		return StreamHeadsVerifyReport{}, err
	}

	var report StreamHeadsVerifyReport

	err = repo.inReadTx(ctx, func(tx pgx.Tx) error {
		var verr error

		report, verr = repo.verifyStreamHeads(ctx, tx)

		return verr
	})

	return report, err
}

// inReadTx runs fn in a read-only repeatable-read transaction, so a verification that issues
// several statements sees one snapshot.
func (repo *Repository) inReadTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := repo.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin: %w", err)
	}

	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // read-only; rollback ends it

	if _, err = tx.Exec(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		return fmt.Errorf("postgres: begin read-only snapshot: %w", err)
	}

	return fn(tx)
}

// verifyStreamHeads compares head rows with the log inside tx.
func (repo *Repository) verifyStreamHeads(ctx context.Context, tx pgx.Tx) (StreamHeadsVerifyReport, error) {
	var report StreamHeadsVerifyReport

	heads, err := repo.headsRelation(ctx, tx)
	if err != nil {
		return report, err
	}

	err = tx.QueryRow(ctx, repo.derivedHeadsCTE()+`SELECT count(*) FROM derived`).Scan(&report.Streams)
	if err != nil {
		return report, fmt.Errorf("postgres: counting streams: %w", err)
	}

	rows, err := tx.Query(ctx, repo.derivedHeadsCTE()+fmt.Sprintf(`
SELECT COALESCE(d.entity_id, h.entity_id) AS entity_id, h.head, d.head, count(*) OVER () AS total
FROM (SELECT * FROM derived WHERE head >= 1) d
FULL OUTER JOIN %s h ON h.entity_id = d.entity_id
WHERE d.entity_id IS NULL OR h.entity_id IS NULL OR h.head <> d.head
ORDER BY 1
LIMIT %d`, heads, maxReportedHeadFindings))
	if err != nil {
		return report, fmt.Errorf("postgres: verifying stream heads: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id      string
			stored  *int64
			derived *int64
		)

		if err = rows.Scan(&id, &stored, &derived, &report.Total); err != nil {
			return report, fmt.Errorf("postgres: reading a stream head finding: %w", err)
		}

		report.Findings = append(report.Findings, headFinding(evt.EntityID(id), stored, derived))
	}

	if err = rows.Err(); err != nil {
		return report, fmt.Errorf("postgres: verifying stream heads: %w", err)
	}

	return report, nil
}

// headFinding classifies one mismatch from the stored head row and the derived head (nil: absent).
func headFinding(id evt.EntityID, stored, derived *int64) evt.HeadFinding {
	finding := evt.HeadFinding{EntityID: id}

	if stored != nil {
		finding.Stored = evt.EventSequence(*stored)
	}

	if derived != nil {
		finding.Derived = evt.EventSequence(*derived)
	}

	switch {
	case derived == nil:
		finding.Kind = evt.HeadOrphan
	case stored == nil:
		finding.Kind = evt.HeadMissing
	case *stored < *derived:
		finding.Kind = evt.HeadLower
	default:
		finding.Kind = evt.HeadHigher
	}

	return finding
}

// MigrateStreamHeads migrates the store in one transaction under the exclusive feature lock:
// insert a head row for every stream (the maximum of its event and snapshot sequences, skipping a
// maximum of 0, ON CONFLICT DO NOTHING), verify that no stream's row is missing or unequal to the
// derived head, and declare stream-heads. Any mismatch aborts the whole transaction with
// *evt.HeadsNotVerifiedError, leaving the store untouched; RepairStreamHeads is the remedy. A
// store that already declares stream-heads returns evt.ErrHeadsAlreadyDeclared, and one that
// declares a feature the migration does not model (journal) *evt.UnsupportedFeatureError.
//
// It calls EnsureSchema first (a dry run does not), which creates the empty head table and the
// feature table on a database written by v0.8.
func (repo *Repository) MigrateStreamHeads(
	ctx context.Context,
	opts StreamHeadsMigrateOptions,
) (StreamHeadsMigrationReport, error) {
	report := StreamHeadsMigrationReport{DryRun: opts.DryRun}

	if opts.DryRun {
		err := repo.inReadTx(ctx, func(tx pgx.Tx) error { return repo.countMigrationInTx(ctx, tx, &report) })

		return report, err
	}

	if !opts.ConfirmWritersStopped {
		return report, evt.ErrWritersNotConfirmed
	}

	if err := repo.EnsureSchema(ctx); err != nil {
		return report, err
	}

	manifest, err := repo.features.declare(ctx, declareStreamHeads,
		func(ctx context.Context, tx pgx.Tx, manifest *evt.FeatureManifest) error {
			return repo.migrateInTx(ctx, tx, manifest, &report)
		})
	if err != nil {
		return report, err
	}

	report.Declared = true
	report.Manifest = manifest

	return report, nil
}

// migrateInTx is the body of the migration transaction: everything between taking the locks and
// the declaration. An error aborts the transaction, the declaration included.
func (repo *Repository) migrateInTx(
	ctx context.Context,
	tx pgx.Tx,
	manifest *evt.FeatureManifest,
	report *StreamHeadsMigrationReport,
) error {
	if err := checkMigrationManifest(manifest); err != nil {
		return err
	}

	if manifest != nil && manifest.DeclaresStreamHeads() {
		return evt.ErrHeadsAlreadyDeclared
	}

	if err := repo.lockTables(ctx, tx); err != nil {
		return err
	}

	tag, err := tx.Exec(ctx, repo.derivedHeadsCTE()+fmt.Sprintf(`
INSERT INTO %s (entity_id, entity_type, head)
SELECT entity_id, entity_type, head FROM derived WHERE head >= 1
ON CONFLICT (entity_id) DO NOTHING`, repo.headsTable()))
	if err != nil {
		return fmt.Errorf("postgres: backfilling stream heads: %w", err)
	}

	report.Inserted = tag.RowsAffected()

	verified, err := repo.verifyStreamHeads(ctx, tx)
	if err != nil {
		return err
	}

	report.Streams = verified.Streams

	if !verified.OK() {
		return &evt.HeadsNotVerifiedError{Phase: "transaction", Findings: verified.Findings, Total: int(verified.Total)}
	}

	return nil
}

// countMigrationInTx is the dry run: the checks the migration makes and the number of head rows it
// would insert, from one read-only snapshot.
func (repo *Repository) countMigrationInTx(ctx context.Context, tx pgx.Tx, report *StreamHeadsMigrationReport) error {
	manifest, err := repo.features.readManifestTx(ctx, tx)
	if err != nil {
		return err
	}

	if err = checkMigrationManifest(manifest); err != nil {
		return err
	}

	if manifest != nil && manifest.DeclaresStreamHeads() {
		return evt.ErrHeadsAlreadyDeclared
	}

	heads, err := repo.headsRelation(ctx, tx)
	if err != nil {
		return err
	}

	err = tx.QueryRow(ctx, repo.derivedHeadsCTE()+fmt.Sprintf(`
SELECT (SELECT count(*) FROM derived),
       (SELECT count(*) FROM derived d WHERE d.head >= 1
          AND NOT EXISTS (SELECT 1 FROM %s h WHERE h.entity_id = d.entity_id))`, heads),
	).Scan(&report.Streams, &report.Inserted)
	if err != nil {
		return fmt.Errorf("postgres: counting the head rows to insert: %w", err)
	}

	return nil
}

// lockTables takes the SHARE locks of lockMigration; the advisory lock is already held by the
// declaration the migration runs inside.
func (repo *Repository) lockTables(ctx context.Context, tx pgx.Tx) error {
	tables := fmt.Sprintf("LOCK TABLE %s, %s IN SHARE MODE", repo.eventsTable, repo.snapshotsTable)
	if _, err := tx.Exec(ctx, tables); err != nil {
		return fmt.Errorf("postgres: lock the event tables: %w", err)
	}

	return nil
}

// RepairStreamHeads inserts a missing head row, raises one below the derived head, and, with
// AllowLower, lowers one above it, all in one transaction under the same locks as the migration.
// It does not declare the feature. A head row it cannot explain (a stream with no events or
// snapshot, or a derived head of 0) is left and reported, as is a higher head without AllowLower.
// Run it with every writer stopped.
func (repo *Repository) RepairStreamHeads(
	ctx context.Context,
	opts StreamHeadsRepairOptions,
) (StreamHeadsRepairReport, error) {
	report := StreamHeadsRepairReport{DryRun: opts.DryRun}

	if !opts.DryRun && !opts.ConfirmWritersStopped {
		return report, evt.ErrWritersNotConfirmed
	}

	if opts.DryRun {
		err := repo.inReadTx(ctx, func(tx pgx.Tx) error { return repo.planRepairInTx(ctx, tx, opts, &report) })

		return report, err
	}

	if err := repo.EnsureSchema(ctx); err != nil {
		return report, err
	}

	err := repo.inTx(ctx, func(tx pgx.Tx) error {
		if err := repo.lockMigration(ctx, tx); err != nil {
			return err
		}

		manifest, err := repo.features.readManifestTx(ctx, tx)
		if err != nil {
			return err
		}

		if err = checkMigrationManifest(manifest); err != nil {
			return err
		}

		return repo.repairInTx(ctx, tx, opts, &report)
	})

	return report, err
}

// planRepairInTx is the dry run of a repair: it sorts the findings into those a repair would fix
// and those it would leave, from one read-only snapshot.
func (repo *Repository) planRepairInTx(
	ctx context.Context,
	tx pgx.Tx,
	opts StreamHeadsRepairOptions,
	report *StreamHeadsRepairReport,
) error {
	manifest, err := repo.features.readManifestTx(ctx, tx)
	if err != nil {
		return err
	}

	if err = checkMigrationManifest(manifest); err != nil {
		return err
	}

	verified, err := repo.verifyStreamHeads(ctx, tx)
	if err != nil {
		return err
	}

	for _, finding := range verified.Findings {
		fixable := finding.Kind == evt.HeadMissing || finding.Kind == evt.HeadLower ||
			(finding.Kind == evt.HeadHigher && opts.AllowLower)

		if fixable {
			report.Repaired = append(report.Repaired, finding)
		} else {
			report.Left = append(report.Left, finding)
		}
	}

	return nil
}

func (repo *Repository) repairInTx(
	ctx context.Context,
	tx pgx.Tx,
	opts StreamHeadsRepairOptions,
	report *StreamHeadsRepairReport,
) error {
	heads := repo.headsTable()

	inserted, err := tx.Exec(ctx, repo.derivedHeadsCTE()+fmt.Sprintf(`
INSERT INTO %s (entity_id, entity_type, head)
SELECT entity_id, entity_type, head FROM derived WHERE head >= 1
ON CONFLICT (entity_id) DO NOTHING`, heads))
	if err != nil {
		return fmt.Errorf("postgres: creating missing stream heads: %w", err)
	}

	raised, err := tx.Exec(ctx, repo.derivedHeadsCTE()+fmt.Sprintf(`
UPDATE %s h SET head = d.head FROM derived d
WHERE h.entity_id = d.entity_id AND d.head >= 1 AND h.head < d.head`, heads))
	if err != nil {
		return fmt.Errorf("postgres: raising stream heads: %w", err)
	}

	report.Inserted, report.Raised = inserted.RowsAffected(), raised.RowsAffected()

	if opts.AllowLower {
		lowered, lerr := tx.Exec(ctx, repo.derivedHeadsCTE()+fmt.Sprintf(`
UPDATE %s h SET head = d.head FROM derived d
WHERE h.entity_id = d.entity_id AND d.head >= 1 AND h.head > d.head`, heads))
		if lerr != nil {
			return fmt.Errorf("postgres: lowering stream heads: %w", lerr)
		}

		report.Lowered = lowered.RowsAffected()
	}

	remaining, err := repo.verifyStreamHeads(ctx, tx)
	report.Left = remaining.Findings

	return err
}
