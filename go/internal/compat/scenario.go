package compat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"go.opentelemetry.io/otel/propagation"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/dynamo"
	"github.com/photon-grove/evt/go/snapshots"
	"github.com/photon-grove/evt/go/test"
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

	// ResultDrift is *evt.StreamHeadDriftError (Error::StreamHeadDrift): a stream's head item
	// disagrees with its log (ADR 0007).
	ResultDrift = "drift"

	// Commit-wide view results (ADR 0004, C7). ResultProjector is *evt.ProjectorOperationError: a
	// view operation's condition failed (the outcome names the projector and the operation, and the
	// mapped error when the operation carried one); the others are refused before any write.
	ResultProjector        = "projector"
	ResultProjectorOnly    = "projectorOnly"
	ResultViewKeyTooLong   = "viewKeyTooLong"
	ResultDuplicateViewKey = "duplicateViewKey"

	// Outcome-store results (ADR 0004, C3/C4).
	ResultReuse     = "reuse"     // IdempotencyKeyReuse
	ResultIDTooLong = "idTooLong" // CommandIdTooLong, before any write
	ResultAmbiguous = "ambiguous" // an ambiguous write proven not to have applied
	ResultAbsent    = "absent"    // resolveOutcome found no outcome row

	// Rebuild-fence results (ADR 0004, C10). ResultRebuildInProgress is a commit refused because a
	// rebuild holds the lock; the others are the results of the rebuild op's actions.
	ResultRebuildInProgress = "rebuildInProgress"
	ResultHeld              = "held"        // Begin found an unexpired lock
	ResultLeaseLost         = "leaseLost"   // the caller does not hold the lock
	ResultSettled           = "settled"     // a reader's observe found an even generation
	ResultUnavailable       = "unavailable" // a reader's observe found an odd generation
)

// Injected commit faults (the injectCommitFault step field).
const (
	// FaultApplyThenFail makes the commit apply while the caller observes an indeterminate error.
	FaultApplyThenFail = "applyThenFail"

	// FaultFailWithoutApplying drops the commit and reports an indeterminate error.
	FaultFailWithoutApplying = "failWithoutApplying"
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

	// Outcomes runs the scenario on stores with command outcomes enabled: every step carries a
	// command ID, and execute steps record and replay outcomes.
	Outcomes bool `json:"outcomes,omitempty"`

	// Projector registers the shared LedgerProjector (a commit-wide projector) on the scenario's
	// stores, so every commit that writes events also writes ledger rows.
	Projector bool `json:"projector,omitempty"`

	// Exclusive runs the scenario alone, after the interleaved scenarios, because it holds
	// store-wide state across steps: a rebuild lock refuses every other scenario's commits.
	Exclusive bool `json:"exclusive,omitempty"`

	Steps []Step `json:"steps"`
}

// Step is one operation against the scenario's stream.
type Step struct {
	By          string              `json:"by"`
	Op          string              `json:"op"`
	CommandID   string              `json:"commandId,omitempty"`
	CommitID    string              `json:"commitId,omitempty"`
	PrincipalID string              `json:"principalId,omitempty"`
	CausationID string              `json:"causationId,omitempty"`
	Extensions  map[string]string   `json:"extensions,omitempty"`
	Command     *Command            `json:"command,omitempty"`
	Event       *RawEvent           `json:"event,omitempty"`
	Through     int64               `json:"through,omitempty"`
	Sequence    int64               `json:"sequence,omitempty"`
	Trace       map[string]string   `json:"trace,omitempty"`
	Address     string              `json:"address,omitempty"`
	Timestamp   string              `json:"timestamp,omitempty"`
	Expect      Outcome             `json:"expect"`
	Streams     []TransactionStream `json:"streams,omitempty"`
	Fences      []TransactionFence  `json:"fences,omitempty"`
	Suffix      string              `json:"suffix,omitempty"`

	// Projectors are the commit-wide projectors' view operations of a transaction step, in
	// declaration order.
	Projectors []StepProjector `json:"projectors,omitempty"`

	// InjectCommitFault makes the step's commit fail indeterminately: FaultApplyThenFail or
	// FaultFailWithoutApplying (execute steps of outcomes scenarios).
	InjectCommitFault string `json:"injectCommitFault,omitempty"`

	// Identity and Response drive outcomeOnly steps: a command with no events, recorded as an
	// accepted outcome carrying Response.
	Identity *Identity `json:"identity,omitempty"`
	Response string    `json:"response,omitempty"`

	// Snapshot and EventSequence drive putSnapshot steps: an out-of-band snapshot write of the
	// stream named by Suffix (the primary stream when empty), with snapshot sequence
	// Snapshot.Sequence covering events through EventSequence.
	Snapshot      *TransactionSnapshot `json:"snapshot,omitempty"`
	EventSequence *int64               `json:"eventSequence,omitempty"`

	// Action, Owner, LeaseSeconds, and Seconds drive rebuild steps (C10): the lifecycle action
	// (begin, renew, end, forceRelease, generation, writeViews, wait), the owner token (scoped to
	// the run and scenario), the lease, and the length of a wait.
	Action       string `json:"action,omitempty"`
	Owner        string `json:"owner,omitempty"`
	LeaseSeconds int64  `json:"leaseSeconds,omitempty"`
	Seconds      int64  `json:"seconds,omitempty"`
}

// Identity is the command identity of an outcomeOnly step: its type string and payload object.
type Identity struct {
	Type    string         `json:"type"`
	Payload map[string]any `json:"payload,omitempty"`
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

	// Code is the rejection code of a rejected outcome, and Replayed reports that the outcome was
	// read back from a stored row rather than produced by the step (outcomes scenarios).
	Code     string `json:"code,omitempty"`
	Replayed bool   `json:"replayed,omitempty"`

	// Projector and Operation name the failed view operation of a ResultProjector (the projector's
	// name and the operation's index among those it emitted), and Mapped is the message of the
	// operation's mapped error when it carried one.
	Projector string `json:"projector,omitempty"`
	Operation int    `json:"operation,omitempty"`
	Mapped    string `json:"mapped,omitempty"`

	// Delta, ForceReleased, and Rows are the observations of a rebuild step: the generation after
	// the step minus the generation before it, the force-release flag after it, and (writeViews)
	// the number of view rows in the scenario's rebuild partition after it.
	Delta         int64 `json:"delta,omitempty"`
	ForceReleased bool  `json:"forceReleased,omitempty"`
	Rows          int   `json:"rows,omitempty"`
}

func (o Outcome) String() string {
	return fmt.Sprintf("%s(events=%d, deleted=%d, code=%q, replayed=%t, projector=%q, operation=%d, mapped=%q, delta=%d, forceReleased=%t, rows=%d)",
		o.Result, o.Events, o.Deleted, o.Code, o.Replayed, o.Projector, o.Operation, o.Mapped,
		o.Delta, o.ForceReleased, o.Rows)
}

// Dir returns the repository's compat/ directory.
func Dir() string {
	_, file, _, _ := runtime.Caller(0)

	return filepath.Join(filepath.Dir(file), "..", "..", "..", "compat")
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
	if step.PrincipalID != "" {
		id := evt.PrincipalID(step.PrincipalID)
		metadata.PrincipalID = &id
	}
	if step.CausationID != "" {
		id := evt.CommandID(step.CausationID)
		metadata.CausationID = &id
	}
	// The engine derives the commit ID from the command ID, preserves a caller-stamped one,
	// or generates an unpredictable ULID. Scenarios pin an explicit commit ID for steps
	// without a command so the golden fixtures stay byte-for-byte reproducible.
	switch {
	case step.CommandID != "":
		metadata.CommitID = "compat-" + step.By + "-" + step.CommandID
	case step.CommitID != "":
		metadata.CommitID = step.CommitID
	}
	metadata.Extensions = step.Extensions

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

	// Projector registers the shared LedgerProjector on the stores the runner builds for itself
	// (the outcome stores of fault-injecting steps).
	Projector bool

	// Outcomes is the store with command outcomes enabled; it is set for outcomes scenarios and
	// nil otherwise.
	Outcomes *snapshots.Store

	// Run and Scenario name the run and the scenario of an outcomes runner. The outcome digest is
	// independent of the run, and command IDs are scoped to both, because outcome rows are global
	// to the table: a bare "c1" would collide across scenarios and across runs.
	Run      string
	Scenario string

	// BypassHead appends an event to the backend's log without maintaining the stream's head item:
	// the writer a head-aware build must detect. It backs the appendRawBypassingHead step and is
	// set by the harness for each backend; nil means the backend cannot be bypassed.
	BypassHead func(ctx context.Context, event evt.SerializedEvent) error

	// Rebuild backs the rebuild op (C10); nil refuses the op.
	Rebuild *RebuildDriver
}

// RebuildDriver is what a rebuild step needs of a backend: the fence over the store, a way to wait
// that sleeps against a live backend and advances the injected clock against a fixture one, and a
// count of the view rows a scenario's guarded writes left.
type RebuildDriver struct {
	Fence evt.RebuildFence

	// Wait lets seconds pass for lease expiry.
	Wait func(ctx context.Context, seconds int64) error

	// Rows counts the view rows in a partition of the compat views table.
	Rows func(ctx context.Context, pk string) (int, error)
}

// metadata builds a step's metadata. On an outcomes runner the command ID is scoped to the run and
// the scenario.
func (r Runner) metadata(step Step) evt.Metadata {
	if r.Outcomes != nil && step.CommandID != "" {
		step.CommandID = fmt.Sprintf("compat-%s-%s-%s", r.Run, r.Scenario, step.CommandID)
	}

	return StepMetadata(step)
}

// NewOutcomeRunner returns a runner whose store records command outcomes. Its identity digest
// replaces the run name in the target stream IDs with the fixture run's, so a command digests
// the same in every run and the golden fixtures' digests hold against live backends.
func NewOutcomeRunner(repo evt.Repository, scenario Scenario, run string) Runner {
	interval := scenario.SnapshotInterval
	store := outcomeStore(repo, interval, run, scenario.Projector)

	return Runner{
		Store: store, Repo: repo, SnapshotInterval: interval, Outcomes: store, Run: run, Scenario: scenario.Name,
		Projector: scenario.Projector,
	}
}

// NewRunner returns a runner over a plain store, with the shared ledger projector registered when
// the scenario asks for it.
func NewRunner(repo evt.Repository, scenario Scenario) Runner {
	store := snapshots.NewStore(repo, scenario.SnapshotInterval)
	if scenario.Projector {
		store.WithCommitProjectors(LedgerProjector{})
	}

	return Runner{Store: store, Repo: repo, SnapshotInterval: scenario.SnapshotInterval, Projector: scenario.Projector}
}

func outcomeStore(repo evt.Repository, interval int, run string, projector bool) *snapshots.Store {
	// A short recovery window keeps the dropped-commit step quick; the step still ends in the
	// retryable ambiguous result because the row never appears. One attempt per step: a scenario
	// step pins what a single attempt returns, so the engine must not run the command again on its
	// own (fence-aware retry has its own conformance cases).
	store := snapshots.NewStore(repo, interval).
		WithOutcomes(
			snapshots.WithDigester(runIndependentDigest(run)),
			snapshots.WithRecoveryWait(100*time.Millisecond),
		).
		WithRetry(snapshots.RetryPolicy{MaxAttempts: 1})

	if projector {
		store.WithCommitProjectors(LedgerProjector{})
	}

	return store
}

// runIndependentDigest is the default digest with the run name in each target stream ID replaced
// by the fixture run's.
func runIndependentDigest(run string) evt.Digester {
	from, to := "compat-"+run+"-", "compat-"+FixtureRun+"-"

	return func(input evt.DigestInput) (string, error) {
		streams := make([]evt.EntityID, len(input.Targets))
		for i, stream := range input.Targets {
			streams[i] = evt.EntityID(strings.Replace(string(stream), from, to, 1))
		}

		input.Targets = streams

		return evt.DefaultDigest(input)
	}
}

// Execute runs one step and classifies its outcome. Unexpected errors are returned as errors.
func (r Runner) Execute(ctx context.Context, entityID evt.EntityID, step Step) (Outcome, error) {
	switch step.Op {
	case "transaction":
		return r.transaction(ctx, entityID, step)
	case "execute":
		return r.execute(ctx, entityID, step)
	case "appendRaw":
		return r.appendRaw(ctx, entityID, step)
	case "appendRawBypassingHead":
		return r.appendRawBypassingHead(ctx, entityID, step)
	case "putSnapshot":
		return r.putSnapshot(ctx, entityID, step)
	case "conflictProbe":
		return r.conflictProbe(ctx, entityID, step)
	case "compact":
		return r.compact(ctx, entityID, step)
	case "outcomeOnly":
		return r.outcomeOnly(ctx, entityID, step)
	case "resolveOutcome":
		return r.resolveOutcome(ctx, entityID, step)
	case "rebuild":
		return r.rebuild(ctx, entityID, step)
	}

	return Outcome{}, fmt.Errorf("unknown op %q", step.Op)
}

func (r Runner) sequence(ctx context.Context, entityID evt.EntityID) (evt.EventSequence, error) {
	cc, err := r.Store.LoadAggregate(ctx, NewAccount(entityID), entityID)
	if err != nil {
		return 0, err
	}

	return *cc.CurrentSequence, nil
}

func (r Runner) execute(ctx context.Context, entityID evt.EntityID, step Step) (Outcome, error) {
	if step.Command == nil {
		return Outcome{}, errors.New("execute step has no command")
	}

	if r.Outcomes != nil {
		return r.executeWithOutcome(ctx, entityID, step)
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

	entityID = transactionID(entityID, step.Suffix)
	current, err := r.sequence(ctx, entityID)
	if err != nil {
		return Outcome{}, err
	}

	return r.commitRaw(ctx, entityID, current+1, *step.Event, step)
}

// appendRawBypassingHead writes the next event of a stream straight to the backend, as a writer that
// does not maintain the stream's head item would, leaving the head one behind the log.
func (r Runner) appendRawBypassingHead(ctx context.Context, entityID evt.EntityID, step Step) (Outcome, error) {
	if step.Event == nil {
		return Outcome{}, errors.New("appendRawBypassingHead step has no event")
	}

	if r.BypassHead == nil {
		return Outcome{}, errors.New("the backend has no way to bypass the head item")
	}

	entityID = transactionID(entityID, step.Suffix)

	current, err := r.sequence(ctx, entityID)
	if err != nil {
		return Outcome{}, err
	}

	sequence := current + 1
	event := evt.SerializedEvent{
		ID:         evt.GetEventID(entityID, sequence),
		Sequence:   sequence,
		Type:       step.Event.Type,
		Version:    step.Event.Version,
		EntityID:   entityID,
		EntityType: AccountType,
		Payload:    []byte(step.Event.Payload),
		Metadata:   StepMetadata(step),
	}

	if err := r.BypassHead(ctx, event); err != nil {
		return Outcome{}, err
	}

	return Outcome{Result: ResultOK, Events: 1}, nil
}

// snapshotWriter is the out-of-band snapshot write the DynamoDB and PostgreSQL repositories offer.
type snapshotWriter interface {
	PutSnapshot(
		ctx context.Context,
		entityType evt.EntityType,
		entityID evt.EntityID,
		payload []byte,
		snapshotSequence evt.EventSequence,
		eventSequence evt.EventSequence,
	) error
}

// putSnapshot writes a snapshot outside a commit. A snapshot ahead of the stream's head is
// refused as drift; a regressing one is a silent no-op.
func (r Runner) putSnapshot(ctx context.Context, entityID evt.EntityID, step Step) (Outcome, error) {
	if step.Snapshot == nil || step.EventSequence == nil {
		return Outcome{}, errors.New("putSnapshot step needs a snapshot and an eventSequence")
	}

	writer, ok := r.Repo.(snapshotWriter)
	if !ok {
		return Outcome{}, fmt.Errorf("repository %T cannot write snapshots outside a commit", r.Repo)
	}

	entityID = transactionID(entityID, step.Suffix)

	err := writer.PutSnapshot(ctx, AccountType, entityID, []byte(step.Snapshot.Payload),
		evt.EventSequence(step.Snapshot.Sequence), evt.EventSequence(*step.EventSequence))
	if outcome, handled := classify(err); handled {
		return outcome, nil
	}
	if err != nil {
		return Outcome{}, err
	}

	return Outcome{Result: ResultOK}, nil
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

	entityID = transactionID(entityID, step.Suffix)

	deleted, err := compactor.CompactBelow(ctx, entityID, evt.EventSequence(step.Through))
	if errors.Is(err, evt.ErrCompactionUncovered) {
		return Outcome{Result: ResultUncovered}, nil
	}
	if err != nil {
		return Outcome{}, err
	}

	return Outcome{Result: ResultCompacted, Deleted: deleted}, nil
}

// storeFor returns the outcomes store for a step, over a repository that injects the step's commit
// fault when it asks for one.
func (r Runner) storeFor(step Step) (*snapshots.Store, error) {
	if step.InjectCommitFault == "" {
		return r.Outcomes, nil
	}

	fault, err := test.NewFaultRepository(r.Repo)
	if err != nil {
		return nil, err
	}

	cause := errors.New("injected commit fault")

	switch step.InjectCommitFault {
	case FaultApplyThenFail:
		fault.ApplyThenFail(cause)
	case FaultFailWithoutApplying:
		fault.FailWithoutApplying(cause)
	default:
		return nil, fmt.Errorf("unknown injectCommitFault %q", step.InjectCommitFault)
	}

	return outcomeStore(fault, r.SnapshotInterval, r.Run, r.Projector), nil
}

func (r Runner) executeWithOutcome(ctx context.Context, entityID evt.EntityID, step Step) (Outcome, error) {
	store, err := r.storeFor(step)
	if err != nil {
		return Outcome{}, err
	}

	command := *step.Command

	outcome, err := store.ExecuteWithOutcome(ctx, NewAccount(entityID), entityID, &command, r.metadata(step))

	return classifyCommand(outcome, err)
}

// outcomeOnly records a command that writes no events: the store reads the outcome row, and an
// absent row is completed with an accepted outcome carrying the step's response.
func (r Runner) outcomeOnly(ctx context.Context, _ evt.EntityID, step Step) (Outcome, error) {
	if step.Identity == nil {
		return Outcome{}, errors.New("outcomeOnly step has no identity")
	}

	identity := evt.CommandIdentity{Type: evt.CommandType(step.Identity.Type)}
	if step.Identity.Payload != nil {
		identity.Payload = step.Identity.Payload
	}

	tx, replayed, err := r.Outcomes.BeginCommand(ctx, identity, r.metadata(step))
	if err != nil || replayed != nil {
		return classifyCommand(replayed, err)
	}

	outcome, err := tx.Complete(ctx, []byte(step.Response))

	return classifyCommand(outcome, err)
}

// resolveOutcome re-reads the outcome row for the step's command, as ambiguous-write recovery
// does, without running or writing anything.
func (r Runner) resolveOutcome(ctx context.Context, entityID evt.EntityID, step Step) (Outcome, error) {
	if step.Command == nil {
		return Outcome{}, errors.New("resolveOutcome step has no command")
	}

	command := *step.Command
	identity := evt.CommandIdentity{Type: command.Type(), Targets: []evt.EntityID{entityID}, Payload: &command}

	outcome, err := r.Outcomes.ResolveCommand(ctx, identity, r.metadata(step))
	if err == nil && outcome == nil {
		return Outcome{Result: ResultAbsent}, nil
	}

	return classifyCommand(outcome, err)
}

// classifyCommand maps the result of a command on an outcomes store to a step outcome.
func classifyCommand(outcome *evt.CommandOutcome, err error) (Outcome, error) {
	var tooLong *evt.CommandIDTooLongError

	switch {
	case errors.Is(err, evt.ErrIdempotencyKeyReuse):
		return Outcome{Result: ResultReuse}, nil
	case errors.As(err, &tooLong):
		return Outcome{Result: ResultIDTooLong}, nil
	case errors.Is(err, evt.ErrAmbiguousWrite):
		return Outcome{Result: ResultAmbiguous}, nil
	case err != nil:
		if classified, ok := classify(err); ok {
			return classified, nil
		}

		return Outcome{}, err
	case outcome.Rejected():
		return Outcome{
			Result: ResultRejected, Code: string(outcome.Rejection.Code), Replayed: outcome.Replayed,
			Events: len(outcome.EventIDs),
		}, nil
	}

	return Outcome{Result: ResultOK, Events: len(outcome.EventIDs), Replayed: outcome.Replayed}, nil
}

// classify maps expected domain and concurrency errors to outcomes.
func classify(err error) (Outcome, bool) {
	if err == nil {
		return Outcome{}, false
	}

	var (
		rejected   *RejectedError
		projection *evt.ProjectorOperationError
		keyTooLong *evt.ViewKeyTooLongError
		duplicate  *evt.DuplicateViewKeyError
		onlyViews  *evt.ProjectorOnlyCommitError
	)

	switch {
	case errors.As(err, &projection):
		outcome := Outcome{Result: ResultProjector, Projector: projection.Projector, Operation: projection.Operation}
		if projection.Err != nil {
			outcome.Mapped = projection.Err.Error()
		}

		return outcome, true
	case errors.As(err, &onlyViews):
		return Outcome{Result: ResultProjectorOnly}, true
	case errors.As(err, &keyTooLong):
		return Outcome{Result: ResultViewKeyTooLong}, true
	case errors.As(err, &duplicate):
		return Outcome{Result: ResultDuplicateViewKey}, true
	case evt.IsStreamHeadDriftErr(err):
		return Outcome{Result: ResultDrift}, true
	case evt.IsRebuildInProgressErr(err):
		return Outcome{Result: ResultRebuildInProgress}, true
	case errors.As(err, &rejected):
		return Outcome{Result: ResultRejected}, true
	case evt.IsDuplicateCommandErr(err):
		return Outcome{Result: ResultDuplicate}, true
	case evt.IsConflictErr(err), errors.Is(err, dynamo.ErrSnapshotRaceCondition):
		return Outcome{Result: ResultConflict}, true
	}

	return Outcome{}, false
}
