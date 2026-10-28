package compat

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/snapshots"
	"github.com/photon-grove/evt/go/test"
)

// The ops of the v0.9 core scenarios (ADR 0004, C11): readLatest (C8), enumerate (C8), declare
// (C9), executeTransaction (C5), and groupCommits (C6/C7).

// TransactionWrite is one event an executeTransaction step's decision step appends, to the stream
// named by Suffix relative to the scenario ID.
type TransactionWrite struct {
	Suffix string   `json:"suffix"`
	Event  RawEvent `json:"event"`
}

// Interference is another writer moving a stream while an executeTransaction step runs: before
// each of the step's first Times commits, Event is appended to the stream named by Suffix.
type Interference struct {
	Suffix string   `json:"suffix"`
	Event  RawEvent `json:"event"`
	Times  int      `json:"times"`
}

// readLatest reads the newest N events of the step's stream, newest first.
func (r Runner) readLatest(ctx context.Context, base evt.EntityID, step Step) (Outcome, error) {
	events, err := r.Repo.GetRecentEvents(ctx, transactionID(base, step.Suffix), step.N)
	if err != nil {
		return Outcome{}, err
	}

	sequences := make([]int64, 0, len(events))
	for _, event := range events {
		sequences = append(sequences, int64(event.Sequence))
	}

	return Outcome{Result: ResultOK, Events: len(events), Sequences: sequences}, nil
}

// enumerate walks the event log and the entity heads, as a rebuild does, and counts the events of
// the scenario's streams. Engine rows (outcome rows, the feature marker, the rebuild fence) are not
// streams: one of the scenario's that either walk yields is an error.
func (r Runner) enumerate(ctx context.Context, base evt.EntityID) (Outcome, error) {
	count := 0

	for batch := range r.Repo.StreamAllEvents(ctx, evt.StreamFilter{}) {
		events, err := batch.Unwrap()
		if err != nil {
			return Outcome{}, err
		}

		for _, event := range events {
			if !strings.Contains(string(event.EntityID), string(base)) {
				continue
			}

			if evt.IsReservedEntityID(event.EntityID) {
				return Outcome{}, fmt.Errorf("enumeration yielded the engine row %q", event.EntityID)
			}

			count++
		}
	}

	if heads, ok := r.Repo.(evt.EntityHeadStreamer); ok {
		all, err := heads.StreamEntityHeads(ctx, "")
		if err != nil {
			return Outcome{}, err
		}

		for id := range all {
			if strings.Contains(string(id), string(base)) && evt.IsReservedEntityID(id) {
				return Outcome{}, fmt.Errorf("entity heads yielded the engine row %q", id)
			}
		}
	}

	return Outcome{Result: ResultOK, Events: count}, nil
}

// declare applies one declaration to the store's feature manifest and reports the version bump.
func (r Runner) declare(ctx context.Context, base evt.EntityID, step Step) (Outcome, error) {
	if r.Marker == nil {
		return Outcome{}, errors.New("the backend has no feature marker driver")
	}

	before, err := r.Marker.Manifest(ctx)
	if err != nil {
		return Outcome{}, err
	}

	scoped := func(types []string) []evt.EntityType {
		out := make([]evt.EntityType, 0, len(types))
		for _, entityType := range types {
			out = append(out, evt.EntityType(scopeID(entityType, base)))
		}

		return out
	}

	retention, history := scoped(step.Retention), scoped(step.HistoryDependent)

	declared, err := r.Marker.Declare(ctx, func(manifest *evt.FeatureManifest) {
		if manifest.Features == nil {
			manifest.Features = map[string]evt.FeatureFormat{}
		}

		if step.Retract {
			for name := range step.Features {
				delete(manifest.Features, name)
			}

			manifest.Retention = slices.DeleteFunc(manifest.Retention, func(t evt.EntityType) bool { return slices.Contains(retention, t) })
			manifest.HistoryDependent = slices.DeleteFunc(manifest.HistoryDependent, func(t evt.EntityType) bool { return slices.Contains(history, t) })

			return
		}

		for name, format := range step.Features {
			manifest.Features[name] = evt.FeatureFormat(format)
		}

		for _, entityType := range retention {
			if !slices.Contains(manifest.Retention, entityType) {
				manifest.Retention = append(manifest.Retention, entityType)
			}
		}

		for _, entityType := range history {
			if !slices.Contains(manifest.HistoryDependent, entityType) {
				manifest.HistoryDependent = append(manifest.HistoryDependent, entityType)
			}
		}
	})
	if err != nil {
		return Outcome{}, err
	}

	var version int64
	if before != nil {
		version = before.Version
	}

	return Outcome{Result: ResultOK, Delta: declared.Version - version}, nil
}

// executeTransaction runs a multi-stream command under fence-aware retry (C5): a store with
// outcomes and a retry policy of Attempts immediate attempts. The decision step reads every stream
// in Reads (engine-filled fences) and every stream it writes, adds the step's Fences as
// caller-stamped fences, and appends Writes on the first attempt and RetryWrites (when set) on the
// ones after, so its write set can change between attempts while the identity, the command's type
// and payload targeting the primary stream, stays the same. Interfere moves a stream before the
// first commits, as another writer would. The outcome counts the decision step's runs.
func (r Runner) executeTransaction(ctx context.Context, base evt.EntityID, step Step) (Outcome, error) {
	if r.Outcomes == nil || step.Identity == nil {
		return Outcome{}, errors.New("executeTransaction needs an outcomes scenario and an identity")
	}

	faulty, err := test.NewFaultRepository(r.Repo)
	if err != nil {
		return Outcome{}, err
	}

	if step.Interfere != nil {
		interference := *step.Interfere
		moved := 0

		faulty.Intercept = func(ctx context.Context, result evt.SerializedResult, commit func() error) error {
			if result.Outcome != nil && moved < interference.Times {
				moved++

				if moveErr := r.interfere(ctx, base, step, interference, moved); moveErr != nil {
					return moveErr
				}
			}

			return commit()
		}
	}

	store := outcomeStore(faulty, r.SnapshotInterval, r.Run, r.Projector).
		WithRetry(snapshots.ImmediateRetryPolicy(max(step.Attempts, 1)))

	identity := evt.CommandIdentity{Type: evt.CommandType(step.Identity.Type), Targets: []evt.EntityID{base}}
	if step.Identity.Payload != nil {
		identity.Payload = step.Identity.Payload
	}

	prepares := 0

	outcome, err := store.ExecuteTransaction(ctx, identity, r.metadata(step), func(ctx context.Context, tx *snapshots.Transaction) ([]byte, error) {
		prepares++

		writes := step.Writes
		if prepares > 1 && len(step.RetryWrites) > 0 {
			writes = step.RetryWrites
		}

		return nil, prepareStep(ctx, tx, base, step, writes)
	})

	classified, err := classifyCommand(outcome, err)
	if err != nil {
		return Outcome{}, err
	}

	classified.Prepares = prepares

	return classified, nil
}

// prepareStep is an executeTransaction step's decision: read, fence, append.
func prepareStep(ctx context.Context, tx *snapshots.Transaction, base evt.EntityID, step Step, writes []TransactionWrite) error {
	read := map[evt.EntityID]*Account{}

	load := func(suffix string) (*Account, error) {
		id := transactionID(base, suffix)
		if account, ok := read[id]; ok {
			return account, nil
		}

		account := NewAccount(id)
		if err := tx.Read(ctx, account, id); err != nil {
			return nil, err
		}

		read[id] = account

		return account, nil
	}

	for _, suffix := range step.Reads {
		if _, err := load(suffix); err != nil {
			return err
		}
	}

	for _, fence := range step.Fences {
		if err := tx.Fence(evt.ReadFence{
			EntityID: transactionID(base, fence.Suffix), EntityType: fence.entityType(base), Head: evt.EventSequence(fence.Head),
		}); err != nil {
			return err
		}
	}

	for _, write := range writes {
		account, err := load(write.Suffix)
		if err != nil {
			return err
		}

		event, err := account.DeserializeEvent(evt.SerializedEvent{
			Type: write.Event.Type, Version: write.Event.Version, Payload: write.Event.payload(),
		})
		if err != nil {
			return err
		}

		if err := tx.Append(transactionID(base, write.Suffix), event); err != nil {
			return err
		}
	}

	return nil
}

// interfere appends the interference's event to its stream at the stream's next sequence, as
// another writer: under a commit ID of its own, number n of the step.
func (r Runner) interfere(ctx context.Context, base evt.EntityID, step Step, interference Interference, n int) error {
	id := transactionID(base, interference.Suffix)

	current, err := r.sequence(ctx, id)
	if err != nil {
		return err
	}

	metadata := StepMetadata(Step{By: step.By, CommitID: fmt.Sprintf("compat-%s-%s-interfere-%d", step.By, step.CommandID, n)})
	sequence := current + 1

	return r.Repo.Commit(ctx, evt.SerializedResult{Events: []evt.SerializedEvent{{
		ID: evt.GetEventID(id, sequence), Sequence: sequence, Type: interference.Event.Type,
		Version: interference.Event.Version, EntityID: id, EntityType: AccountType,
		Payload: interference.Event.payload(), Metadata: metadata,
	}}})
}

// groupCommits reads every stream the scenario writes and regroups their events into the commits
// that wrote them, as a rebuild of a history-dependent projector does.
func (r Runner) groupCommits(ctx context.Context, base evt.EntityID, step Step) (Outcome, error) {
	suffixes := append([]string{""}, step.Reads...)

	var events []evt.SerializedEvent

	for _, suffix := range suffixes {
		stored, err := r.Repo.GetEvents(ctx, transactionID(base, suffix))
		if err != nil {
			return Outcome{}, err
		}

		events = append(events, stored...)
	}

	commits, err := evt.GroupCommits(events)
	if err != nil {
		return Outcome{}, err
	}

	groups := make([]string, 0, len(commits))

	for _, commit := range commits {
		id := commit.CommitID
		if id == "" {
			id = "-"
		}

		var members []string

		for _, stream := range commit.Streams {
			suffix := strings.TrimPrefix(strings.TrimPrefix(string(stream.EntityID), string(base)), "-")

			for _, event := range stream.Events {
				members = append(members, fmt.Sprintf("%s:%d", suffix, event.Sequence))
			}
		}

		groups = append(groups, id+" "+strings.Join(members, " "))
	}

	return Outcome{Result: ResultOK, Events: len(events), Groups: groups}, nil
}
