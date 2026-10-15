package compat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"go.opentelemetry.io/otel/propagation"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
)

// Writer names used in scenario steps and recorded as the metadata origin endpoint.
const (
	WriterGo   = "go"
	WriterRust = "rust"
)

// Shared metadata values. Every step's metadata is fully determined by the step, so the bytes
// each language writes are comparable.
const (
	OriginSource     = "evt-compat"
	Region           = "us-west-2"
	DefaultTimestamp = "2026-10-12T00:00:00Z"
	FixtureRun       = "fixture"
)

// Step results.
const (
	ResultOK        = "ok"
	ResultRejected  = "rejected"
	ResultDuplicate = "duplicate"
	ResultConflict  = "conflict"
	ResultCompacted = "compacted"
	ResultUncovered = "uncovered"
)

// Scenarios is the shared scenario file (compat/scenarios.json).
type Scenarios struct {
	Scenarios []Scenario `json:"scenarios"`
}

// Scenario is one stream driven by a sequence of steps, each assigned to a writer language.
type Scenario struct {
	Name             string `json:"name"`
	Description      string `json:"description"`
	SnapshotInterval int    `json:"snapshotInterval"`
	Steps            []Step `json:"steps"`
}

// Step is one operation against the scenario's stream.
type Step struct {
	By        string            `json:"by"`
	Op        string            `json:"op"`
	CommandID string            `json:"commandId,omitempty"`
	Command   *Command          `json:"command,omitempty"`
	Event     *RawEvent         `json:"event,omitempty"`
	Through   int64             `json:"through,omitempty"`
	Sequence  int64             `json:"sequence,omitempty"`
	Trace     map[string]string `json:"trace,omitempty"`
	Address   string            `json:"address,omitempty"`
	Timestamp string            `json:"timestamp,omitempty"`
	Expect    Outcome           `json:"expect"`
}

// RawEvent is an event appended directly to the log, bypassing the aggregate (legacy versions).
type RawEvent struct {
	Type    evt.EventType    `json:"type"`
	Version evt.EventVersion `json:"version"`
	Payload string           `json:"payload"`
}

// Outcome is the observable result of a step.
type Outcome struct {
	Result  string `json:"result"`
	Events  int    `json:"events,omitempty"`
	Deleted int    `json:"deleted,omitempty"`
}

func (o Outcome) String() string {
	return fmt.Sprintf("%s(events=%d, deleted=%d)", o.Result, o.Events, o.Deleted)
}

// Dir returns the repository's compat/ directory.
func Dir() string {
	_, file, _, _ := runtime.Caller(0)

	return filepath.Join(filepath.Dir(file), "..", "..", "compat")
}

// LoadScenarios reads compat/scenarios.json.
func LoadScenarios() (Scenarios, error) {
	var scenarios Scenarios

	data, err := os.ReadFile(filepath.Join(Dir(), "scenarios.json"))
	if err != nil {
		return scenarios, err
	}

	err = json.Unmarshal(data, &scenarios)

	return scenarios, err
}

// EntityID is the stream ID for a scenario within a run.
func EntityID(run string, scenario Scenario) evt.EntityID {
	return evt.EntityID(fmt.Sprintf("compat-%s-%s", run, scenario.Name))
}

// StepMetadata builds the metadata for a step.
func StepMetadata(step Step) evt.Metadata {
	metadata := evt.Metadata{
		Origin:    &evt.Origin{Source: OriginSource, Endpoint: evt.Endpoint(step.By)},
		Region:    Region,
		Timestamp: DefaultTimestamp,
	}

	if step.CommandID != "" {
		id := evt.CommandID(step.CommandID)
		metadata.CommandID = &id
	}
	if step.Trace != nil {
		carrier := make(propagation.MapCarrier, len(step.Trace))
		for key, value := range step.Trace {
			carrier[key] = value
		}
		metadata.Trace = &carrier
	}
	if step.Address != "" {
		address := step.Address
		metadata.Address = &address
	}
	if step.Timestamp != "" {
		metadata.Timestamp = step.Timestamp
	}

	return metadata
}

// Runner executes steps against one backend.
type Runner struct {
	Store            evt.Store
	Repo             evt.Repository
	SnapshotInterval int
}

// Execute runs one step and classifies its outcome. Unexpected errors are returned as errors.
func (r Runner) Execute(ctx context.Context, entityID evt.EntityID, step Step) (Outcome, error) {
	switch step.Op {
	case "execute":
		return r.execute(ctx, entityID, step)
	case "appendRaw":
		return r.appendRaw(ctx, entityID, step)
	case "conflictProbe":
		return r.conflictProbe(ctx, entityID, step)
	case "compact":
		return r.compact(ctx, entityID, step)
	}

	return Outcome{}, fmt.Errorf("unknown op %q", step.Op)
}

func (r Runner) sequence(ctx context.Context, entityID evt.EntityID) (evt.EventSequence, error) {
	_, eventContext, err := evt.LoadEntityWithFactory(ctx, r.Store, func() evt.Entity { return NewAccount(entityID) }, entityID) //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
	if err != nil {
		return 0, err
	}

	return *eventContext.CurrentSequence, nil
}

func (r Runner) execute(ctx context.Context, entityID evt.EntityID, step Step) (Outcome, error) {
	if step.Command == nil {
		return Outcome{}, errors.New("execute step has no command")
	}

	before, err := r.sequence(ctx, entityID)
	if err != nil {
		return Outcome{}, err
	}

	command := *step.Command
	err = r.Store.Execute(ctx, NewAccount(entityID), entityID, &command, StepMetadata(step))
	if outcome, ok := classify(err); ok {
		return outcome, nil
	}
	if err != nil {
		return Outcome{}, err
	}

	after, err := r.sequence(ctx, entityID)
	if err != nil {
		return Outcome{}, err
	}

	return Outcome{Result: ResultOK, Events: int(after - before)}, nil
}

func (r Runner) appendRaw(ctx context.Context, entityID evt.EntityID, step Step) (Outcome, error) {
	if step.Event == nil {
		return Outcome{}, errors.New("appendRaw step has no event")
	}

	current, err := r.sequence(ctx, entityID)
	if err != nil {
		return Outcome{}, err
	}

	return r.commitRaw(ctx, entityID, current+1, *step.Event, step)
}

func (r Runner) conflictProbe(ctx context.Context, entityID evt.EntityID, step Step) (Outcome, error) {
	probe := RawEvent{Type: DepositedEvent, Version: 1, Payload: `{"amountCents":1,"memo":null}`}

	return r.commitRaw(ctx, entityID, evt.EventSequence(step.Sequence), probe, step)
}

func (r Runner) commitRaw(
	ctx context.Context,
	entityID evt.EntityID,
	sequence evt.EventSequence,
	raw RawEvent,
	step Step,
) (Outcome, error) {
	event := evt.SerializedEvent{
		ID:         evt.GetEventID(entityID, sequence),
		Sequence:   sequence,
		Type:       raw.Type,
		Version:    raw.Version,
		EntityID:   entityID,
		EntityType: AccountType,
		Payload:    []byte(raw.Payload),
		Metadata:   StepMetadata(step),
	}

	err := r.Repo.Commit(ctx, evt.SerializedResult{Events: []evt.SerializedEvent{event}})
	if outcome, ok := classify(err); ok {
		return outcome, nil
	}
	if err != nil {
		return Outcome{}, err
	}

	return Outcome{Result: ResultOK, Events: 1}, nil
}

func (r Runner) compact(ctx context.Context, entityID evt.EntityID, step Step) (Outcome, error) {
	compactor, ok := r.Repo.(evt.Compactor)
	if !ok {
		return Outcome{}, errors.New("repository does not support compaction")
	}

	deleted, err := compactor.CompactBelow(ctx, entityID, evt.EventSequence(step.Through))
	if errors.Is(err, evt.ErrCompactionUncovered) {
		return Outcome{Result: ResultUncovered}, nil
	}
	if err != nil {
		return Outcome{}, err
	}

	return Outcome{Result: ResultCompacted, Deleted: deleted}, nil
}

// classify maps expected domain and concurrency errors to outcomes.
func classify(err error) (Outcome, bool) {
	if err == nil {
		return Outcome{}, false
	}

	var rejected *RejectedError
	switch {
	case errors.As(err, &rejected):
		return Outcome{Result: ResultRejected}, true
	case evt.IsDuplicateCommandErr(err):
		return Outcome{Result: ResultDuplicate}, true
	case evt.IsConflictErr(err), errors.Is(err, dynamo.ErrSnapshotRaceCondition):
		return Outcome{Result: ResultConflict}, true
	}

	return Outcome{}, false
}
