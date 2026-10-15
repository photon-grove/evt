package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
)

// The feature-marker table derives from the configured events table name through the reserved
// infix, so two event logs in one database never share engine rows (storage spec, PostgreSQL).
const featuresInfix = "__evt_features"

// engineRowLockClass is the advisory-lock class key for engine-row locks. It derives from the
// configured events table name, so two logs never block each other's engine rows: declarations
// take the exclusive lock for their read-modify-write, commit-time checks take the shared lock.
const engineRowLockClass = 1702261761

// FeatureMarker reads, declares, and validates the feature manifest a store keeps in the
// {events}__evt_features table (ADR-0004 C9). The manifest is durable, shared state: any writer
// that does not support a declared (feature, formatVersion) pair refuses to write to the log.
//
// A writer validates the feature set once, at first write, and caches the version it validated;
// every commit then re-checks the version inside its own transaction, holding the shared advisory
// lock so a declaration cannot slip between the check and the COMMIT.
type FeatureMarker struct {
	db          DB
	eventsTable string
	supported   evt.SupportedFeatures

	// mu guards the cached validation below; commits and declarations run concurrently.
	mu            sync.Mutex
	validated     bool
	hasMarker     bool  // whether the cached version came from an existing row
	cachedVersion int64 // the validated version; meaningful only when validated
}

// FeatureMarker constructs a marker using this repository's client and events table.
// The supported set declares the (feature, formatVersion) pairs this writer models;
// a nil or empty set supports no extensions.
func (repo *Repository) FeatureMarker(supported evt.SupportedFeatures) *FeatureMarker {
	return newFeatureMarker(repo.db, repo.eventsTable, supported)
}

func newFeatureMarker(db DB, eventsTable string, supported evt.SupportedFeatures) *FeatureMarker {
	return &FeatureMarker{
		db:          db,
		eventsTable: strings.ToLower(eventsTable),
		supported:   supported,
	}
}

// TableName returns the derived features table name for the configured events table.
func (m *FeatureMarker) TableName() string {
	return m.eventsTable + featuresInfix
}

// Manifest reads the feature-marker row and returns nil when the log declares nothing — either
// the row is absent or the table itself does not exist yet (a log upgraded from a release without
// the marker).
func (m *FeatureMarker) Manifest(ctx context.Context) (*evt.FeatureManifest, error) {
	return m.readManifest(ctx, m.db)
}

// Declare applies update to the current manifest in a read-modify-write that bumps the version by
// exactly one. The exclusive advisory lock serializes declarations, so the read-modify-write is
// atomic; creating the row writes version 1.
func (m *FeatureMarker) Declare(
	ctx context.Context,
	update func(*evt.FeatureManifest),
) (evt.FeatureManifest, error) {
	tx, err := m.db.Begin(ctx)
	if err != nil {
		return evt.FeatureManifest{}, fmt.Errorf("features: begin declaration: %w", err)
	}

	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // the transaction is read-only until its single conditional write commits

	// Exclusive lock: no other declaration (and no commit's shared lock) can interleave.
	lock := fmt.Sprintf(`SELECT pg_advisory_xact_lock(%d, hashtext($1))`, engineRowLockClass)
	if _, err = tx.Exec(ctx, lock, m.eventsTable+":features"); err != nil {
		return evt.FeatureManifest{}, fmt.Errorf("features: lock declaration: %w", err)
	}

	manifest, err := m.readManifestTx(ctx, tx)
	if err != nil {
		return evt.FeatureManifest{}, err
	}

	current := evt.FeatureManifest{Version: 1, Features: map[string]evt.FeatureFormat{}}
	observed := int64(0)
	if manifest != nil {
		observed = manifest.Version
		current = *manifest
	}

	update(&current)

	// Each declaration transaction adds exactly one to the version, no matter how many features it
	// adds or bumps.
	current.Version = observed + 1

	featuresText, err := current.FeaturesJSON()
	if err != nil {
		return evt.FeatureManifest{}, err
	}

	historyText := "[]"
	if len(current.HistoryDependent) > 0 {
		if historyText, err = current.HistoryDependentJSON(); err != nil {
			return evt.FeatureManifest{}, err
		}
	}

	retentionText := "[]"
	if len(current.Retention) > 0 {
		if retentionText, err = current.RetentionJSON(); err != nil {
			return evt.FeatureManifest{}, err
		}
	}

	table := m.TableName()

	if manifest == nil {
		_, err = tx.Exec(ctx,
			fmt.Sprintf(
				`INSERT INTO %s (id, version, features, history_dependent, retention)
				 VALUES (0, 1, $1, $2, $3)`,
				table,
			),
			featuresText, historyText, retentionText,
		)
		if err != nil {
			return evt.FeatureManifest{}, classifyWriteError(
				fmt.Errorf("features: creating manifest: %w", err))
		}
	} else {
		_, err = tx.Exec(ctx,
			fmt.Sprintf(
				`UPDATE %s SET version = $1, features = $2, history_dependent = $3, retention = $4
				 WHERE id = 0 AND version = $5`,
				table,
			),
			current.Version, featuresText, historyText, retentionText, observed,
		)
		if err != nil {
			return evt.FeatureManifest{}, classifyWriteError(
				fmt.Errorf("features: updating manifest: %w", err))
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return evt.FeatureManifest{}, classifyWriteError(fmt.Errorf("features: declare commit: %w", err))
	}

	m.invalidate()

	return current, nil
}

// readManifestTx reads the feature-marker row inside the given transaction, returning nil when the
// log declares nothing.
func (m *FeatureMarker) readManifestTx(ctx context.Context, tx pgx.Tx) (*evt.FeatureManifest, error) {
	return m.readManifest(ctx, tx)
}

func (m *FeatureMarker) readManifest(ctx context.Context, db DB) (*evt.FeatureManifest, error) {
	// A failing SELECT would abort an open transaction even if we swallowed 42P01.
	// Probe without an error so pre-marker logs can still commit before EnsureSchema runs.
	var exists bool
	if err := db.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, m.TableName()).Scan(&exists); err != nil {
		return nil, fmt.Errorf("features: checking manifest table: %w", err)
	}
	if !exists {
		return nil, nil
	}

	query := fmt.Sprintf(
		`SELECT version, features, history_dependent, retention FROM %s WHERE id = 0`,
		m.TableName(),
	)

	var (
		version          int64
		features         string
		historyDependent string
		retention        string
	)

	err := db.QueryRow(ctx, query).Scan(&version, &features, &historyDependent, &retention)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}

		return nil, fmt.Errorf("features: reading manifest: %w", err)
	}

	manifest, err := evt.ParseFeatureManifest(features, historyDependent, retention)
	if err != nil {
		return nil, fmt.Errorf("features: decoding manifest: %w", err)
	}

	manifest.Version = version

	return &manifest, nil
}

// checkInTx runs the commit-time feature-marker check inside the writer's transaction, after the
// shared advisory lock: a declaration cannot slip between this check and the COMMIT. The first
// commit validates the feature set against the supported pairs and caches the version; later
// commits compare the stored version against the cache, and any difference (a row that changed, or
// one that appeared after the writer validated an empty table) fails the check.
func (m *FeatureMarker) checkInTx(ctx context.Context, tx pgx.Tx) error {
	_, err := m.checkManifestTx(ctx, tx)

	return err
}

// checkManifestTx runs the commit-time feature-marker check inside the writer's transaction and
// returns the manifest it validated against, so callers can consult durable declarations (the
// retention set, for instance) that this instance did not configure.
func (m *FeatureMarker) checkManifestTx(ctx context.Context, tx pgx.Tx) (*evt.FeatureManifest, error) {
	if err := m.lockShared(ctx, tx); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	manifest, err := m.readManifestTx(ctx, tx)
	if err != nil {
		return nil, err
	}

	if !m.validated {
		if manifest != nil {
			if err := manifest.Validate(m.supported); err != nil {
				return nil, err
			}

			m.cacheVersionLocked(manifest.Version, true)

			return manifest, nil
		}

		m.cacheVersionLocked(0, false)

		return nil, nil
	}

	switch {
	case manifest == nil && m.hasMarker:
		m.invalidateLocked()

		return nil, transientFeatureChanged("the marker row was removed")
	case manifest == nil:
		return nil, nil
	case !m.hasMarker || manifest.Version != m.cachedVersion:
		// The manifest changed under the writer: re-validate now — an unsupported pair refuses the
		// writer outright — and classify the mismatch as transient so the commit is retried against
		// the freshly cached version.
		cachedVersion := m.cachedVersion
		m.invalidateLocked()

		if err := manifest.Validate(m.supported); err != nil {
			return nil, err
		}

		m.cacheVersionLocked(manifest.Version, true)

		return manifest, transientFeatureChanged(fmt.Sprintf(
			"validated version %d, store now declares %d", cachedVersion, manifest.Version))
	}

	return manifest, nil
}

// lockShared fences declarations until the calling transaction commits or rolls back.
func (m *FeatureMarker) lockShared(ctx context.Context, tx pgx.Tx) error {
	query := fmt.Sprintf("SELECT pg_advisory_xact_lock_shared(%d, hashtext($1))", engineRowLockClass)
	if _, err := tx.Exec(ctx, query, m.eventsTable+":features"); err != nil {
		return fmt.Errorf("features: lock check: %w", err)
	}

	return nil
}

// cacheVersionLocked stores the validated version while mu is held.
func (m *FeatureMarker) cacheVersionLocked(version int64, hasMarker bool) {
	m.validated = true
	m.hasMarker = hasMarker
	m.cachedVersion = version
}

// invalidate drops the cached validation so the next commit re-reads the manifest.
func (m *FeatureMarker) invalidate() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.invalidateLocked()
}

func (m *FeatureMarker) invalidateLocked() {
	m.validated = false
	m.hasMarker = false
	m.cachedVersion = 0
}

// transientFeatureChanged returns the transient error a failed marker check classifies as, so
// policy.ExecuteWithRetry retries with a re-validation.
func transientFeatureChanged(detail string) error {
	return &policy.ClassifiedError{
		Class: policy.ClassTransient,
		Err:   evt.FeatureManifestChanged(detail),
	}
}
