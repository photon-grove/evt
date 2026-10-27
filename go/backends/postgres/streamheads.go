package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/policy"
)

// The per-stream head table (ADR 0007; storage spec, Stream Heads, PostgreSQL) holds one row per
// written stream: entity_id, entity_type, and head, the highest event sequence ever committed to
// it. Commits maintain it under the per-entity advisory locks they already take, so "head" means
// one thing on every backend. Unlike DynamoDB it is not needed for capacity, only for semantic
// parity, cheap enity-head queries, and one definition of drift.

// streamHeadsInfix derives the head table from the configured events table through the reserved
// infix, so two event logs in one database never share head rows. It is 18 characters past the
// base, which fits the 63-byte identifier limit with the events table name cap.
const streamHeadsInfix = "__evt_stream_heads"

// headsTable returns the derived head table name for the configured events table.
func (repo *Repository) headsTable() string {
	return repo.eventsTable + streamHeadsInfix
}

// ensureStreamHeads makes sure the store declares stream-heads before a write. A store that
// holds no data is initialized by the writer itself (Initialize); one that holds data without the
// declaration is refused with *evt.StreamHeadsNotMigratedError, because its streams have no head
// rows. The declaring step for a populated store is the migration's
// (FeatureMarker.DeclareStreamHeads).
//
// Unlike DynamoDB, a PostgreSQL writer initializes a fresh store itself by default: the check is
// two EXISTS probes under the feature lock it takes anyway, and it needs no privilege a writer
// lacks.
func (repo *Repository) ensureStreamHeads(ctx context.Context) error {
	marker := repo.features

	marker.mu.Lock()
	ready := marker.headsReady
	marker.mu.Unlock()

	if ready {
		return nil
	}

	// The read runs under the shared feature lock, like every other read of the manifest by a
	// write path, so a declaration cannot slip in beside it.
	var declares bool

	err := repo.inTx(ctx, func(tx pgx.Tx) error {
		if err := marker.lockShared(ctx, tx); err != nil {
			return err
		}

		manifest, err := marker.readManifestTx(ctx, tx)
		if err != nil {
			return err
		}

		if manifest != nil {
			if err = manifest.Validate(marker.supported); err != nil {
				return err
			}
		}

		declares = manifest != nil && manifest.DeclaresStreamHeads()

		return nil
	})
	if err != nil {
		return err
	}

	if !declares {
		if _, err = repo.Initialize(ctx); err != nil {
			return err
		}
	}

	marker.mu.Lock()
	marker.headsReady = true
	marker.mu.Unlock()

	return nil
}

// Initialize initializes the store: it declares stream-heads when the events and snapshots tables
// hold no row, in one transaction under the exclusive feature lock, and reports whether this call
// declared it (evt.StoreInitializer). A store that already declares the feature is left alone; a
// populated one that does not is refused with *evt.StreamHeadsNotMigratedError, and a manifest
// that declares a feature this writer does not model with *evt.UnsupportedFeatureError. Run
// EnsureSchema first.
func (repo *Repository) Initialize(ctx context.Context) (bool, error) {
	_, err := repo.features.declare(ctx, declareStreamHeads, repo.requireEmptyStore)
	if errors.Is(err, errAlreadyInitialized) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

// The PostgreSQL repository initializes stores explicitly.
var _ evt.StoreInitializer = (*Repository)(nil)

// errAlreadyInitialized stops a stream-heads declaration whose store already declares the feature:
// there is nothing to write.
var errAlreadyInitialized = errors.New("postgres: the store already declares stream-heads")

// requireEmptyStore is the guard of a stream-heads initialization: inside the declaration
// transaction, under the exclusive feature lock, the event and snapshot tables must hold no row at
// all (the engine rows live in their own tables, so any row is a stream). A manifest that already
// declares the feature (initialized before, or a racing declaration won) stops the declaration
// with errAlreadyInitialized, and one that declares a feature this writer does not model refuses
// it.
func (repo *Repository) requireEmptyStore(ctx context.Context, tx pgx.Tx, manifest *evt.FeatureManifest) error {
	if manifest != nil {
		if err := manifest.Validate(repo.features.supported); err != nil {
			return err
		}

		if manifest.DeclaresStreamHeads() {
			return errAlreadyInitialized
		}
	}

	var occupied bool

	err := tx.QueryRow(ctx, fmt.Sprintf(
		`SELECT EXISTS (SELECT 1 FROM %s) OR EXISTS (SELECT 1 FROM %s)`,
		repo.eventsTable, repo.snapshotsTable,
	)).Scan(&occupied)
	if err != nil {
		return fmt.Errorf("postgres: checking the store is empty: %w", err)
	}

	if occupied {
		return &evt.StreamHeadsNotMigratedError{}
	}

	return nil
}

// checkOutcomeAbsent fails with *evt.OutcomeConflictError when the command already has an outcome
// row: the row is where the answer lives, and it outranks drift, gap, and conflicts (the same rank
// the outcome put has on DynamoDB).
func (repo *Repository) checkOutcomeAbsent(ctx context.Context, tx pgx.Tx, outcome *evt.Outcome) error {
	if outcome == nil {
		return nil
	}

	var exists bool

	err := tx.QueryRow(ctx,
		fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s WHERE command_id = $1)`, repo.outcomesTable()),
		string(outcome.CommandID),
	).Scan(&exists)
	if err != nil {
		return fmt.Errorf("postgres: check outcome %s: %w", outcome.CommandID, err)
	}

	if exists {
		return &evt.OutcomeConflictError{CommandID: outcome.CommandID}
	}

	return nil
}

// writtenStream is one written stream of a commit.
type writtenStream struct {
	id          evt.EntityID
	entityType  evt.EntityType
	first, last evt.EventSequence
}

// writtenStreams groups a normalized commit's events by stream, in the commit's order (bytewise
// by stream ID).
func writtenStreams(events []evt.SerializedEvent) []writtenStream {
	var streams []writtenStream

	for _, event := range events {
		if n := len(streams); n > 0 && streams[n-1].id == event.EntityID {
			streams[n-1].last = event.Sequence

			continue
		}

		streams = append(streams, writtenStream{
			id: event.EntityID, entityType: event.EntityType, first: event.Sequence, last: event.Sequence,
		})
	}

	return streams
}

// headFailure is a head statement or fence read that did not hold.
type headFailure struct {
	stream *writtenStream
	fence  *evt.ReadFence
}

// checkHeads runs every head statement of a commit and every fence read, in the pinned order —
// each written stream in ascending entity-ID order, then each fenced stream — without stopping at
// the first failure, then ranks what failed. Under the entity advisory locks nothing can land
// between the statements and the probe.
//
//   - create (first sequence 1): INSERT ... ON CONFLICT DO NOTHING must affect one row;
//   - append (first sequence above 1): UPDATE ... WHERE head = first - 1 must affect one row;
//   - fence: the stored head (absent as 0) must equal the fenced head.
//
// A failure is ranked drift, then gap, then conflict, exactly as on DynamoDB.
func (repo *Repository) checkHeads(ctx context.Context, tx pgx.Tx, commit evt.SerializedResult) error {
	var failures []headFailure

	streams := writtenStreams(commit.Events)

	for i := range streams {
		stream := &streams[i]

		var (
			affected int64
			err      error
		)

		if stream.first == 1 {
			tag, execErr := tx.Exec(ctx, fmt.Sprintf(
				`INSERT INTO %s (entity_id, entity_type, head) VALUES ($1, $2, $3)
				 ON CONFLICT (entity_id) DO NOTHING`, repo.headsTable(),
			), string(stream.id), string(stream.entityType), int64(stream.last))
			affected, err = tag.RowsAffected(), execErr
		} else {
			tag, execErr := tx.Exec(ctx, fmt.Sprintf(
				`UPDATE %s SET head = $3 WHERE entity_id = $1 AND head = $2`, repo.headsTable(),
			), string(stream.id), int64(stream.first)-1, int64(stream.last))
			affected, err = tag.RowsAffected(), execErr
		}

		if err != nil {
			return classifyWriteError(fmt.Errorf("postgres: head of %s: %w", stream.id, err))
		}

		if affected != 1 {
			failures = append(failures, headFailure{stream: stream})
		}
	}

	for i := range commit.Fences {
		fence := &commit.Fences[i]

		stored, _, err := repo.readHead(ctx, tx, fence.EntityID)
		if err != nil {
			return err
		}

		if stored != fence.Head {
			failures = append(failures, headFailure{fence: fence})
		}
	}

	if len(failures) == 0 {
		return nil
	}

	return repo.rankHeadFailures(ctx, tx, failures)
}

// rankHeadFailures classifies each failed head statement or fence and returns the highest-ranked
// failure: drift, then a gap, then the first conflict in statement order.
func (repo *Repository) rankHeadFailures(ctx context.Context, tx pgx.Tx, failures []headFailure) error {
	var drift, gap, conflict error

	for _, failure := range failures {
		id := failure.entityID()

		probe, err := repo.probeStream(ctx, tx, id)
		if err != nil {
			return err
		}

		switch {
		case probe.kind == probeDrift:
			if drift == nil {
				drift = probe.driftError(id)
			}

		case failure.stream != nil && probe.kind == probeHealthy && failure.stream.first > 1 &&
			probe.stored < failure.stream.first-1:
			if gap == nil {
				gap = &policy.ClassifiedError{
					Class: policy.ClassTransient,
					Err:   &evt.SequenceGapError{EntityID: id, After: probe.stored, Found: failure.stream.first},
				}
			}

		case conflict == nil && failure.fence != nil:
			conflict = policy.FenceConflict(*failure.fence)

		case conflict == nil:
			conflict = transientConflict(fmt.Sprintf(
				"postgres: stream %s moved: commit continues from %d, head is %d",
				id, failure.stream.first-1, probe.stored,
			))
		}
	}

	switch {
	case drift != nil:
		return drift
	case gap != nil:
		return gap
	}

	return conflict
}

func (f headFailure) entityID() evt.EntityID {
	if f.stream != nil {
		return f.stream.id
	}

	return f.fence.EntityID
}

// probeKind is what the probe's reads say about a stream.
type probeKind int

const (
	// probeHealthy: the head row equals the derived head.
	probeHealthy probeKind = iota

	// probeRace: the head moved between the reads; an ordinary conflict.
	probeRace

	// probeDrift: the head row disagrees with the log.
	probeDrift
)

// streamProbe is the outcome of the failure-path probe for one stream.
type streamProbe struct {
	kind    probeKind
	stored  evt.EventSequence
	present bool
	derived evt.EventSequence
}

func (p streamProbe) driftError(id evt.EntityID) error {
	return &evt.StreamHeadDriftError{EntityID: id, Stored: p.stored, HeadAbsent: !p.present, Derived: p.derived}
}

// probeStream is the failure-path probe the storage spec pins for every backend: derive the head
// D1, read the head row S (absent as 0), then S < D1 is drift, S = D1 is healthy, and S > D1
// derives again as D2, where D2 < S is drift and anything else is a conflict. PostgreSQL's reads are
// transactionally consistent and the entity lock holds the stream steady, so the second derivation
// only repeats the first, but the rule is the same.
func (repo *Repository) probeStream(ctx context.Context, db DB, id evt.EntityID) (streamProbe, error) {
	first, err := repo.deriveHead(ctx, db, id)
	if err != nil {
		return streamProbe{}, err
	}

	stored, present, err := repo.readHead(ctx, db, id)
	if err != nil {
		return streamProbe{}, err
	}

	probe := streamProbe{stored: stored, present: present, derived: first}

	switch {
	case stored < first:
		probe.kind = probeDrift

	case stored == first:
		probe.kind = probeHealthy

	default:
		second, derr := repo.deriveHead(ctx, db, id)
		if derr != nil {
			return streamProbe{}, derr
		}

		probe.derived = second

		if second < stored {
			probe.kind = probeDrift
		} else {
			probe.kind = probeRace
		}
	}

	return probe, nil
}

// deriveHead derives a stream's head from its log: the larger of its highest stored event sequence
// and its snapshot's event sequence (invariant H4).
func (repo *Repository) deriveHead(ctx context.Context, db DB, id evt.EntityID) (evt.EventSequence, error) {
	var head int64

	err := db.QueryRow(ctx, fmt.Sprintf(
		`SELECT GREATEST(
		         COALESCE((SELECT MAX(sequence) FROM %s WHERE entity_id = $1), 0),
		         COALESCE((SELECT MAX(event_sequence) FROM %s WHERE entity_id = $1), 0))`,
		repo.eventsTable, repo.snapshotsTable,
	), string(id)).Scan(&head)
	if err != nil {
		return 0, fmt.Errorf("postgres: derive head of %s: %w", id, err)
	}

	return evt.EventSequence(head), nil
}

// readHead reads a stream's head row, absent as head 0.
func (repo *Repository) readHead(ctx context.Context, db DB, id evt.EntityID) (evt.EventSequence, bool, error) {
	var head int64

	err := db.QueryRow(ctx,
		fmt.Sprintf(`SELECT head FROM %s WHERE entity_id = $1`, repo.headsTable()), string(id),
	).Scan(&head)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, fmt.Errorf("postgres: read head of %s: %w", id, err)
	}

	return evt.EventSequence(head), true, nil
}

// classifyInsertFailure interprets a failed event insert. A row already stored at the sequence,
// reported after the stream's head statement passed, is an event above the head: drift. Under the
// entity lock the verdict is certain, with no race to rule out.
func (repo *Repository) classifyInsertFailure(
	ctx context.Context,
	tx pgx.Tx,
	events []evt.SerializedEvent,
	err error,
) error {
	var duplicate *duplicateEventError
	if !errors.As(err, &duplicate) {
		return classifyWriteError(err)
	}

	id := duplicate.event.EntityID
	stream := writtenStreams(events)

	var first evt.EventSequence

	for _, candidate := range stream {
		if candidate.id == id {
			first = candidate.first
		}
	}

	derived, derr := repo.deriveHead(ctx, tx, id)
	if derr != nil {
		return derr
	}

	// The head before this commit's statement: the sequence before its first, or none for a create.
	return &evt.StreamHeadDriftError{
		EntityID:   id,
		Stored:     first - 1,
		HeadAbsent: first <= 1,
		Derived:    derived,
	}
}

// duplicateEventError reports an event insert that found its sequence already stored.
type duplicateEventError struct{ event evt.SerializedEvent }

func (e *duplicateEventError) Error() string {
	return fmt.Sprintf("postgres: event %s is already stored", e.event.ID)
}

// guardSnapshotHead enforces invariant H3 for an out-of-band snapshot write, under the entity lock:
// the stream's head must be at least the snapshot's event sequence, or absent when that is 0 (a
// snapshot of an empty stream). Otherwise the snapshot is ahead of the stream, or the stream has no
// head row, and the write is refused with *evt.StreamHeadDriftError.
func (repo *Repository) guardSnapshotHead(
	ctx context.Context,
	tx pgx.Tx,
	id evt.EntityID,
	eventSequence evt.EventSequence,
) error {
	stored, present, err := repo.readHead(ctx, tx, id)
	if err != nil {
		return err
	}

	if (present && stored >= eventSequence) || (!present && eventSequence == 0) {
		return nil
	}

	derived, err := repo.deriveHead(ctx, tx, id)
	if err != nil {
		return err
	}

	return &evt.StreamHeadDriftError{
		EntityID:   id,
		Stored:     stored,
		HeadAbsent: !present,
		// The head the log would derive once the snapshot is written.
		Derived: max(derived, eventSequence),
	}
}

// DeclareStreamHeads is the migration's declaring step for the repository's store. See
// FeatureMarker.DeclareStreamHeads.
func (repo *Repository) DeclareStreamHeads(ctx context.Context) (evt.FeatureManifest, error) {
	return repo.features.DeclareStreamHeads(ctx)
}
