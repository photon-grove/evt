package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/internal/eventjson"
)

// TransactionStream describes one written stream relative to the scenario ID.
// Empty suffix names the primary stream; other suffixes name sibling streams.
type TransactionStream struct {
	Suffix   string               `json:"suffix"`
	Sequence int64                `json:"sequence"`
	Events   []RawEvent           `json:"events"`
	Snapshot *TransactionSnapshot `json:"snapshot,omitempty"`

	// EntityPrefix prefixes the stream ID, so the stream lies in a reserved namespace (C8).
	EntityPrefix string `json:"entityPrefix,omitempty"`
}

// TransactionSnapshot is one written stream's inline snapshot relative to the scenario ID.
type TransactionSnapshot struct {
	Sequence int64  `json:"sequence"`
	Payload  string `json:"payload"`
}

// TransactionFence is one read-only fenced stream relative to the scenario ID.
type TransactionFence struct {
	Suffix        string `json:"suffix"`
	Head          int64  `json:"head"`
	CallerStamped bool   `json:"callerStamped,omitempty"`

	// EntityType is the fenced stream's entity type, the account type when empty. {id} is the
	// scenario's entity ID, so a type in a durable set is scoped to the run.
	EntityType string `json:"entityType,omitempty"`
}

// entityType is the fence's entity type, with {id} resolved against the scenario's entity ID.
func (f TransactionFence) entityType(base evt.EntityID) evt.EntityType {
	if f.EntityType == "" {
		return AccountType
	}

	return evt.EntityType(scopeID(f.EntityType, base))
}

// scopeID replaces the {id} placeholder with the scenario's entity ID.
func scopeID(text string, base evt.EntityID) string {
	return strings.ReplaceAll(text, "{id}", string(base))
}

// SiblingSuffixes lists, in ascending order, every non-empty stream suffix the scenario writes,
// fences, reads, appends to, or compacts: each is a stream beside the primary one that the fixtures and
// both languages' verification must observe.
func (s Scenario) SiblingSuffixes() []string {
	seen := map[string]bool{}
	for _, step := range s.Steps {
		seen[step.Suffix] = true
		for _, stream := range step.Streams {
			seen[stream.Suffix] = true
		}
		for _, fence := range step.Fences {
			seen[fence.Suffix] = true
		}
		for _, suffix := range step.Reads {
			seen[suffix] = true
		}
		for _, write := range append(slices.Clone(step.Writes), step.RetryWrites...) {
			seen[write.Suffix] = true
		}
		if step.Interfere != nil {
			seen[step.Interfere.Suffix] = true
		}
	}
	delete(seen, "")

	suffixes := make([]string, 0, len(seen))
	for suffix := range seen {
		suffixes = append(suffixes, suffix)
	}
	slices.Sort(suffixes)

	return suffixes
}

func transactionID(base evt.EntityID, suffix string) evt.EntityID {
	if suffix == "" {
		return base
	}
	return evt.EntityID(string(base) + "-" + suffix)
}

func transactionResult(base evt.EntityID, step Step) evt.SerializedResult {
	result := evt.SerializedResult{}
	for _, stream := range step.Streams {
		id := evt.EntityID(stream.EntityPrefix) + transactionID(base, stream.Suffix)
		for i, raw := range stream.Events {
			seq := evt.EventSequence(stream.Sequence + int64(i))
			result.Events = append(result.Events, evt.SerializedEvent{
				ID: evt.GetEventID(id, seq), Sequence: seq, Type: raw.Type, Version: raw.Version,
				EntityID: id, EntityType: AccountType, Payload: raw.payload(), Metadata: StepMetadata(step),
			})
		}
		if stream.Snapshot != nil {
			result.Snapshots = append(result.Snapshots, evt.SerializedSnapshot{
				EntityID: id, EntityType: AccountType, Sequence: evt.EventSequence(stream.Snapshot.Sequence),
				EventSequence: evt.EventSequence(stream.Sequence + int64(len(stream.Events)) - 1), Payload: []byte(stream.Snapshot.Payload),
			})
		}
	}
	result.Views = viewsOf(base, step)
	for _, fence := range step.Fences {
		result.Fences = append(result.Fences, evt.ReadFence{
			EntityID: transactionID(base, fence.Suffix), EntityType: fence.entityType(base),
			Head: evt.EventSequence(fence.Head), CallerStamped: fence.CallerStamped,
		})
	}
	return result
}

// transaction checks every touched stream, not just the primary stream that is
// recorded in scenarios.json. In particular, every failed transaction must leave
// its events and snapshots byte-for-byte unchanged across all streams.
func (r Runner) transaction(ctx context.Context, base evt.EntityID, step Step) (Outcome, error) {
	result := transactionResult(base, step)
	ids := map[evt.EntityID]bool{}
	for _, event := range result.Events {
		// A stream in a reserved namespace is refused before any write and never read.
		if !evt.IsReservedEntityID(event.EntityID) {
			ids[event.EntityID] = true
		}
	}
	for _, fence := range result.Fences {
		ids[fence.EntityID] = true
	}
	observe := func() (map[evt.EntityID]string, error) {
		states := map[evt.EntityID]string{}
		for id := range ids {
			events, err := r.Repo.GetEvents(ctx, id)
			if err != nil {
				return nil, err
			}
			snapshot, err := r.Repo.GetSnapshot(ctx, id)
			if err != nil {
				return nil, err
			}
			data, err := json.Marshal(struct {
				Events   []evt.SerializedEvent
				Snapshot *evt.SerializedSnapshot
			}{events, snapshot})
			if err != nil {
				return nil, err
			}
			states[id] = string(data)
		}
		return states, nil
	}
	before, err := observe()
	if err != nil {
		return Outcome{}, err
	}
	commitErr := r.Repo.Commit(ctx, result)
	after, err := observe()
	if err != nil {
		return Outcome{}, err
	}
	if commitErr != nil {
		if !reflect.DeepEqual(before, after) {
			return Outcome{}, fmt.Errorf("failed transaction left partial events or snapshots: %w", commitErr)
		}
		if outcome, ok := classify(commitErr); ok {
			return outcome, nil
		}
		return Outcome{}, commitErr
	}
	for _, expected := range result.Events {
		events, err := r.Repo.GetEvents(ctx, expected.EntityID)
		if err != nil {
			return Outcome{}, err
		}
		// Compare via the canonical encoder: it applies encode-time defaults (effective
		// causation, extensions {}) to both sides, so decode artifacts like nil-vs-empty maps do
		// not cause false mismatches.
		expectedJSON, merr := eventjson.MarshalEvent(expected)
		if merr != nil {
			return Outcome{}, merr
		}
		found := false
		for _, stored := range events {
			storedJSON, serr := eventjson.MarshalEvent(stored)
			if serr != nil {
				return Outcome{}, serr
			}
			if string(storedJSON) == string(expectedJSON) {
				found = true
			}
		}
		if !found {
			return Outcome{}, fmt.Errorf("transaction omitted event %s", expected.ID)
		}
	}
	for _, expected := range result.Snapshots {
		snapshot, err := r.Repo.GetSnapshot(ctx, expected.EntityID)
		if err != nil {
			return Outcome{}, err
		}
		if snapshot == nil || snapshot.Sequence != expected.Sequence ||
			snapshot.EventSequence != expected.EventSequence ||
			snapshot.EntityType != expected.EntityType ||
			string(snapshot.Payload) != string(expected.Payload) {
			return Outcome{}, fmt.Errorf("transaction omitted snapshot %s", expected.EntityID)
		}
	}
	return Outcome{Result: ResultOK, Events: len(result.Events)}, nil
}
