package evt

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/photon-grove/evt/go/internal/jsondefaults"
)

// Commit-wide projectors (ADR 0004, C7; storage spec, Views).
//
// A CommitProjector is a pure function over everything one commit wrote. It returns view
// operations — puts and deletes of rows at any pk/sk, with optional conditions and mapped errors —
// that the repository commits in the same transaction as the events. The contract that keeps views
// derived state: a projector's output may depend only on its CommitView, so grouping stored events
// by commit ID and running the projector over each group reproduces the operations the live
// commit produced (see GroupCommits). A view that cannot be rebuilt that way — one that records a
// clock reading, a random value, a verdict decided outside the event log — is a latent data-loss
// bug.

// Limits on a view row's keys, from DynamoDB's own limits so a projector fails identically on
// every backend instead of committing on PostgreSQL and failing on DynamoDB.
const (
	// MaxViewPKBytes is the longest partition key, in UTF-8 bytes.
	MaxViewPKBytes = 2048

	// MaxViewSKBytes is the longest sort key, in UTF-8 bytes.
	MaxViewSKBytes = 1024
)

// reservedViewPrefixes are the view partition-key prefixes the engine reserves for the journal's
// rows, counters and compaction rows (ADR 0005). Go has no journal, so no Go projector may write
// there: ProjectCommit refuses an operation whose pk begins with one (*ReservedViewKeyError), so no
// table holds application rows where a journaling writer keeps its own.
var reservedViewPrefixes = []string{"journal#", "journal-counter#", "journal-compaction#"}

// ViewOperationKind is the kind of a view operation.
type ViewOperationKind string

// The view operation kinds.
const (
	// ViewPut writes a row, replacing any row at the same key unless it carries ViewAbsent.
	ViewPut ViewOperationKind = "put"

	// ViewDelete removes a row; deleting an absent row succeeds unless it carries ViewPresent.
	ViewDelete ViewOperationKind = "delete"
)

// ViewCondition is a condition a view operation carries. A failed condition fails the commit with
// a *ProjectorOperationError.
type ViewCondition string

// The view conditions. ViewAbsent is valid only on a put, ViewPresent only on a delete.
const (
	// ViewUnconditional is the default: no condition.
	ViewUnconditional ViewCondition = ""

	// ViewAbsent requires that no row exists at the key (a put that claims a name).
	ViewAbsent ViewCondition = "absent"

	// ViewPresent requires that a row exists at the key (a delete that releases a claim).
	ViewPresent ViewCondition = "present"
)

// The lengths of the DynamoDB condition expressions a view delete carries, and of the name of its
// expression value; the DynamoDB repository's conditions are these strings. A put counts its item
// only, like every put the commit carries, so its condition is not budgeted.
const (
	// viewUnconditionalDeleteConditionSize is "attribute_not_exists(pk) OR entityID = :owner".
	viewUnconditionalDeleteConditionSize = len("attribute_not_exists(pk) OR entityID = :owner")

	// viewPresentDeleteConditionSize is "attribute_exists(pk) AND entityID = :owner".
	viewPresentDeleteConditionSize = len("attribute_exists(pk) AND entityID = :owner")

	// viewOwnerValueNameSize is the length of ":owner".
	viewOwnerValueNameSize = len(":owner")
)

// ViewOperation is one backend-neutral view write, evaluated and written by the repository in the
// commit's transaction. Build one with PutView or DeleteView.
//
// Every operation names the stream that owns its row, and the repository stores the owner in the
// row's entityID attribute and refuses any write by a different owner. **A row's owner must never
// change.** The log keeps one commit order only, each stream's own sequence, so a replay can order
// the writes to a row only while they all come from commits that wrote the same stream; a row
// handed from one stream to another (a delete by the first, then a put by the second) is accepted
// live and cannot be rebuilt. A key that must be reused by another stream is never deleted: it is
// overwritten by its owner with a tombstone, or the row is owned by a stream both claimants write
// (see the commit-wide projector guide).
type ViewOperation struct {
	// Kind is ViewPut or ViewDelete.
	Kind ViewOperationKind `json:"kind"`

	// PK and SK are the row's keys, chosen by the projector. Both are required.
	PK string `json:"pk"`
	SK string `json:"sk"`

	// Owner is the stream the row belongs to; required. A projector's operation must name a stream
	// the commit wrote, and a history-dependent projector one of the entity types it declares.
	Owner EntityID `json:"owner"`

	// EntityType is informational and always written by a put; readers never interpret it.
	EntityType EntityType `json:"entityType,omitempty"`

	// Payload is the view state JSON of a put.
	Payload []byte `json:"payload,omitempty"`

	// Condition is the operation's condition, if any.
	Condition ViewCondition `json:"condition,omitempty"`

	// Conflict is the projector's own error for the case where Condition fails. The commit's
	// *ProjectorOperationError wraps it, so errors.Is matches the projector's value. It is
	// in-memory commit data and is never stored.
	Conflict error `json:"-"`
}

// PutView returns an operation that writes a row at pk and sk on behalf of the owner stream.
func PutView(pk, sk string, owner EntityID, entityType EntityType, payload []byte) ViewOperation {
	return ViewOperation{Kind: ViewPut, PK: pk, SK: sk, Owner: owner, EntityType: entityType, Payload: payload}
}

// DeleteView returns an operation that deletes the row at pk and sk on behalf of the owner stream.
// Deleting a row another stream owns fails the commit; deleting an absent row succeeds.
func DeleteView(pk, sk string, owner EntityID) ViewOperation {
	return ViewOperation{Kind: ViewDelete, PK: pk, SK: sk, Owner: owner}
}

// IfAbsent returns the operation requiring that no row exists at its key. It is valid on a put.
func (op ViewOperation) IfAbsent() ViewOperation {
	op.Condition = ViewAbsent

	return op
}

// IfPresent returns the operation requiring that a row owned by the operation's owner exists at its
// key. It is valid on a delete.
func (op ViewOperation) IfPresent() ViewOperation {
	op.Condition = ViewPresent

	return op
}

// OnConflict returns the operation mapped to err: when its condition fails, the commit's
// *ProjectorOperationError wraps err.
func (op ViewOperation) OnConflict(err error) ViewOperation {
	op.Conflict = err

	return op
}

// ViewOperationSize is the DynamoDB-encoded size of an operation in the commit budget: a put
// counts its five attributes (the owner is its entityID), and a delete its two key attributes plus
// its condition expression and the owner expression value. The DynamoDB and in-memory repositories
// budget operations by it, and it equals the size of the item the DynamoDB repository builds.
func ViewOperationSize(op ViewOperation) int {
	keys := len("pk") + len(op.PK) + len("sk") + len(op.SK)

	if op.Kind == ViewDelete {
		condition := viewUnconditionalDeleteConditionSize
		if op.Condition == ViewPresent {
			condition = viewPresentDeleteConditionSize
		}

		return keys + condition + viewOwnerValueNameSize + len(op.Owner)
	}

	return keys +
		len("entityID") + len(op.Owner) +
		len("entityType") + len(op.EntityType) +
		len("payload") + len(op.Payload)
}

// CommitStream is one stream a commit wrote, as a commit-wide projector sees it.
type CommitStream struct {
	EntityID   EntityID
	EntityType EntityType

	// Events are this commit's events for the stream, ascending by sequence.
	Events []SerializedEvent

	// State is the stream's state after the commit: the JSON its snapshot would carry. It is nil
	// when the caller has no state to give, as GroupCommits leaves it.
	State []byte
}

// CommitView is everything one commit wrote, the input of a CommitProjector. Streams ascend by
// entity-ID bytes, the order of the transaction's groups; events are those of the commit only.
//
// It carries no outcome context: a rebuild regroups stored events by commit ID and has no outcome
// row, so a row that depended on the outcome's status could not be rebuilt. What a projector is
// given of it depends on its kind (CommitProjector).
type CommitView struct {
	// CommitID is the one commit ID every event of the commit carries; empty for a pre-v0.9
	// event, which replays as a single-event commit, and always empty for a current-state
	// projector.
	CommitID string

	// Streams are the written streams, ascending by entity ID bytes.
	Streams []CommitStream
}

// EventCount returns the number of events across all streams.
func (v CommitView) EventCount() int {
	count := 0
	for _, stream := range v.Streams {
		count += len(stream.Events)
	}

	return count
}

// CommitProjector is a pure function over every event of a commit, emitting view operations that
// commit atomically with it. It runs once per commit that writes at least one event, after every
// stream is staged, and again on every fence-aware re-preparation, so it must be repeatable.
//
// ProjectCommit must be deterministic: the operations may depend only on the commit view, never on
// a clock, a random source, the environment, a store, or state remembered between calls, and must
// not be ordered by map iteration. It must not modify the view. The operations it returns are
// appended to the commit in the projector's declaration order, each projector's in emission order.
//
// A projector is one of two kinds, and History says which, and the view it is given is what a
// rebuild of that kind can give it again. A current-state projector computes its rows from the
// state each stream has after the commit, and is given nothing else: no events and no commit ID.
// A history-dependent projector also reads the commit's events and its commit ID, of the entity
// types it declares only; a rebuild replays every commit for it, so the commits it reads must never
// be compacted away, and its rows must be ordered by something a replay has.
//
// Every operation of either kind names the stream that owns its row, and a row's owner must never
// change (see ViewOperation).
type CommitProjector interface {
	// Name identifies the projector in errors. It should be unique within a store.
	Name() string

	// History returns the entity types whose events the projector reads. It returns nil for a
	// current-state projector, which is given every stream's state but no events and an empty
	// commit ID. A history-dependent projector is given the commit ID, and the events and state of
	// the streams of these types only; other streams carry neither. The store declares these types
	// in the feature manifest's historyDependent set before the first commit that runs the
	// projector, so CompactBelow refuses them in every process, and the owner of each of its
	// operations must be a stream of one of these types.
	History() []EntityType

	// ProjectCommit returns the commit's view operations.
	ProjectCommit(ctx context.Context, commit CommitView) ([]ViewOperation, error)
}

// ProjectedView is a view operation with its provenance: which projector emitted it, at which
// position of the store's projectors, and as which of that projector's operations. The provenance
// is what a failed operation's *ProjectorOperationError reports.
type ProjectedView struct {
	// Projector is the projector's name.
	Projector string `json:"projector"`

	// ProjectorIndex is the projector's position in declaration order.
	ProjectorIndex int `json:"projectorIndex"`

	// Operation is the operation's index among those the projector emitted.
	Operation int `json:"operation"`

	Op ViewOperation `json:"op"`
}

// ProjectorOperationRef names one view operation of a commit by projector and index.
type ProjectorOperationRef struct {
	Projector      string
	ProjectorIndex int
	Operation      int
}

func (r ProjectorOperationRef) String() string {
	return fmt.Sprintf("projector %q (#%d) operation %d", r.Projector, r.ProjectorIndex, r.Operation)
}

// ErrProjectorOperation matches a ProjectorOperationError with errors.Is.
var ErrProjectorOperation = errors.New("projector operation failed")

// ProjectorOperationError reports a view operation whose condition failed or whose statement the
// backend rejected. It names the projector and the operation. It is not a conflict and not
// transient: re-preparing the command cannot fix it, so it is returned at once and records no
// outcome. Nothing from the commit was written.
type ProjectorOperationError struct {
	ProjectorOperationRef

	// Kind, PK, and SK identify the row the operation targeted.
	Kind ViewOperationKind
	PK   string
	SK   string

	// Reason is the backend's reason: the DynamoDB cancellation reason code
	// (ConditionalCheckFailed, ValidationError, ItemCollectionSizeLimitExceeded),
	// ConditionalCheckFailed for a failed PostgreSQL or in-memory condition, or the SQLSTATE
	// PostgreSQL raised.
	Reason string

	// Err is the operation's mapped error (ViewOperation.Conflict), set only for a failed condition
	// whose operation carries one. Cause is the backend's own error, when there is one: the
	// database error PostgreSQL raised, say. Both stay matchable with errors.Is and errors.As.
	Err   error
	Cause error
}

// Error describes the failed operation.
func (e *ProjectorOperationError) Error() string {
	message := fmt.Sprintf("%s: %s of %q/%q failed: %s", e.ProjectorOperationRef, e.Kind, e.PK, e.SK, e.Reason)

	switch {
	case e.Err != nil:
		message += ": " + e.Err.Error()
	case e.Cause != nil:
		message += ": " + e.Cause.Error()
	}

	return message
}

// Unwrap returns the operation's mapped error and the backend's cause.
func (e *ProjectorOperationError) Unwrap() []error {
	var wrapped []error

	if e.Err != nil {
		wrapped = append(wrapped, e.Err)
	}

	if e.Cause != nil {
		wrapped = append(wrapped, e.Cause)
	}

	return wrapped
}

// Is makes errors.Is(err, ErrProjectorOperation) match.
func (e *ProjectorOperationError) Is(target error) bool { return target == ErrProjectorOperation }

// ReasonConditionFailed is the reason of a view operation whose condition did not hold, on every
// backend: DynamoDB's cancellation reason code, and the value PostgreSQL and the in-memory
// repository report for the same failure.
const ReasonConditionFailed = "ConditionalCheckFailed"

// NewProjectorOperationError builds the error for a failed projected view. A failed condition
// (ReasonConditionFailed) carries the operation's mapped error; cause is the backend's own error,
// which may be nil.
func NewProjectorOperationError(view ProjectedView, reason string, cause error) *ProjectorOperationError {
	failure := &ProjectorOperationError{
		ProjectorOperationRef: ProjectorOperationRef{
			Projector: view.Projector, ProjectorIndex: view.ProjectorIndex, Operation: view.Operation,
		},
		Kind:   view.Op.Kind,
		PK:     view.Op.PK,
		SK:     view.Op.SK,
		Reason: reason,
		Cause:  cause,
	}

	if reason == ReasonConditionFailed {
		failure.Err = view.Op.Conflict
	}

	return failure
}

// ProjectorError reports a commit-wide projector that returned an error. It is an internal
// failure: nothing was written and no outcome is recorded.
type ProjectorError struct {
	Projector string
	Err       error
}

// Error describes the failed projector.
func (e *ProjectorError) Error() string {
	return fmt.Sprintf("projector %q failed: %v", e.Projector, e.Err)
}

// Unwrap returns the projector's error.
func (e *ProjectorError) Unwrap() error { return e.Err }

// InvalidViewOperationError reports a view operation that cannot be stored the same way on every
// backend: an empty key, text that is not valid UTF-8 or contains U+0000, an unknown kind, or a
// condition the kind does not allow. Nothing was written.
type InvalidViewOperationError struct {
	ProjectorOperationRef
	Reason string
}

// Error describes the invalid operation.
func (e *InvalidViewOperationError) Error() string {
	return fmt.Sprintf("invalid view operation: %s: %s", e.ProjectorOperationRef, e.Reason)
}

// Unwrap keeps the error matchable with errors.Is(err, ErrInvalidArgument).
func (e *InvalidViewOperationError) Unwrap() error { return ErrInvalidArgument }

// ViewKeyTooLongError reports a view key over DynamoDB's limit, MaxViewPKBytes for a pk and
// MaxViewSKBytes for an sk. Nothing was written.
type ViewKeyTooLongError struct {
	ProjectorOperationRef

	// Field is "pk" or "sk".
	Field  string
	Length int
	Max    int
}

// Error describes the oversized key.
func (e *ViewKeyTooLongError) Error() string {
	return fmt.Sprintf("view key too long: %s: %s is %d bytes, the limit is %d", e.ProjectorOperationRef, e.Field, e.Length, e.Max)
}

// Unwrap keeps the error matchable with errors.Is(err, ErrInvalidArgument).
func (e *ViewKeyTooLongError) Unwrap() error { return ErrInvalidArgument }

// DuplicateViewKeyError reports two view operations of one commit that target the same row. Nothing
// was written.
type DuplicateViewKeyError struct {
	PK string
	SK string

	// First targeted the row earlier in the commit; Second repeats it.
	First  ProjectorOperationRef
	Second ProjectorOperationRef
}

// Error describes the duplicated key.
func (e *DuplicateViewKeyError) Error() string {
	return fmt.Sprintf("duplicate view key %q/%q: %s repeats %s", e.PK, e.SK, e.Second, e.First)
}

// Unwrap keeps the error matchable with errors.Is(err, ErrInvalidArgument).
func (e *DuplicateViewKeyError) Unwrap() error { return ErrInvalidArgument }

// ReservedViewKeyError reports a projector's view operation on a partition the engine reserves for
// the journal: a pk beginning "journal#", "journal-counter#" or "journal-compaction#". It is not a conflict and is never retried. Nothing was written.
type ReservedViewKeyError struct {
	ProjectorOperationRef
	PK string
}

// Error describes the refused key.
func (e *ReservedViewKeyError) Error() string {
	return fmt.Sprintf("reserved view key: %s: %q is in a partition the engine reserves", e.ProjectorOperationRef, e.PK)
}

// Unwrap keeps the error matchable with errors.Is(err, ErrInvalidArgument).
func (e *ReservedViewKeyError) Unwrap() error { return ErrInvalidArgument }

// ViewsNotConfiguredError refuses a commit that carries view operations to a repository that has
// no place to write them: no views table, no view writer. Dropping them would commit the events
// without the rows that were meant to be atomic with them, so the repository fails closed before
// any write.
type ViewsNotConfiguredError struct {
	// Backend names the refusing repository.
	Backend string

	// Operations is the number of view operations the commit carried.
	Operations int
}

// Error describes the refusal.
func (e *ViewsNotConfiguredError) Error() string {
	return fmt.Sprintf(
		"%s has no views table or view writer configured; refusing %d view operations instead of dropping them",
		e.Backend, e.Operations,
	)
}

// CheckViews validates a commit's view operations before any write: each, in order, must be valid
// (non-empty keys, text that is UTF-8 without U+0000, a known kind and condition), then within the
// key limits, then not repeat an earlier operation's key. The first failure is returned as
// *InvalidViewOperationError, *ViewKeyTooLongError, or *DuplicateViewKeyError. Repositories call it
// on every commit path, and the engine calls it when it projects.
func CheckViews(views []ProjectedView) error {
	type key struct{ pk, sk string }

	seen := make(map[key]ProjectorOperationRef, len(views))

	for _, view := range views {
		ref := ProjectorOperationRef{Projector: view.Projector, ProjectorIndex: view.ProjectorIndex, Operation: view.Operation}
		op := view.Op

		invalid := func(reason string) error {
			return &InvalidViewOperationError{ProjectorOperationRef: ref, Reason: reason}
		}

		if reason := invalidViewReason(op); reason != "" {
			return invalid(reason)
		}

		if len(op.PK) > MaxViewPKBytes {
			return &ViewKeyTooLongError{ProjectorOperationRef: ref, Field: "pk", Length: len(op.PK), Max: MaxViewPKBytes}
		}

		if len(op.SK) > MaxViewSKBytes {
			return &ViewKeyTooLongError{ProjectorOperationRef: ref, Field: "sk", Length: len(op.SK), Max: MaxViewSKBytes}
		}

		target := key{op.PK, op.SK}
		if first, repeated := seen[target]; repeated {
			return &DuplicateViewKeyError{PK: op.PK, SK: op.SK, First: first, Second: ref}
		}

		seen[target] = ref
	}

	return nil
}

// invalidViewReason explains why an operation is invalid, or returns "".
func invalidViewReason(op ViewOperation) string {
	switch op.Kind {
	case ViewPut:
		if op.Condition != ViewUnconditional && op.Condition != ViewAbsent {
			return fmt.Sprintf("a put cannot carry the %q condition", op.Condition)
		}
	case ViewDelete:
		if op.Condition != ViewUnconditional && op.Condition != ViewPresent {
			return fmt.Sprintf("a delete cannot carry the %q condition", op.Condition)
		}
	default:
		return fmt.Sprintf("unknown operation kind %q", op.Kind)
	}

	if op.PK == "" {
		return "pk is empty"
	}

	if op.SK == "" {
		return "sk is empty"
	}

	if op.Owner == "" {
		return "owner is empty"
	}

	fields := []struct{ name, value string }{{"pk", op.PK}, {"sk", op.SK}, {"owner", string(op.Owner)}}
	if op.Kind == ViewPut {
		fields = append(fields,
			struct{ name, value string }{"entityType", string(op.EntityType)},
			struct{ name, value string }{"payload", string(op.Payload)},
		)
	}

	for _, field := range fields {
		if !utf8.ValidString(field.value) {
			return field.name + " is not valid UTF-8"
		}

		if strings.ContainsRune(field.value, 0) {
			return field.name + " contains U+0000"
		}
	}

	return ""
}

// ProjectCommit runs the projectors over a commit view, in declaration order, and returns their
// operations with provenance in the order the commit writes them. A commit that wrote no event
// runs no projector.
//
// Each projector is given the view its kind may see (CommitView.visibleTo). Every operation must
// name an owner stream the commit wrote, and a history-dependent projector's owner must also be of
// an entity type it declares in History (*InvalidViewOperationError otherwise). No operation may
// target a partition the engine reserves for the journal (*ReservedViewKeyError). A projector's error
// is a *ProjectorError; the combined operations are validated with CheckViews. A nil projector is
// skipped but keeps its position.
func ProjectCommit(ctx context.Context, projectors []CommitProjector, view CommitView) ([]ProjectedView, error) {
	if len(projectors) == 0 || view.EventCount() == 0 {
		return nil, nil
	}

	var projected []ProjectedView

	for index, projector := range projectors {
		if projector == nil {
			continue
		}

		history := projector.History()

		operations, err := projector.ProjectCommit(ctx, view.visibleTo(history))
		if err != nil {
			return nil, &ProjectorError{Projector: projector.Name(), Err: err}
		}

		for operation, op := range operations {
			item := ProjectedView{Projector: projector.Name(), ProjectorIndex: index, Operation: operation, Op: op}

			if err := checkOwner(item, view, history); err != nil {
				return nil, err
			}

			if err := checkUnreserved(item); err != nil {
				return nil, err
			}

			projected = append(projected, item)
		}
	}

	if err := CheckViews(projected); err != nil {
		return nil, err
	}

	return projected, nil
}

// visibleTo returns the view a projector is given, which is what a rebuild can give it again.
//
// A current-state projector (History is empty) is given the state each stream has after the commit
// and nothing else: no events and no commit ID, because a rebuild from state alone has neither and
// a row that depended on them could not be rebuilt.
//
// A history-dependent projector is given the commit ID, and the events and state of the streams
// whose entity type it declares; the other streams appear by ID and type only, so it cannot read
// what its declaration does not cover (a stream of an undeclared type may have been compacted).
func (v CommitView) visibleTo(history []EntityType) CommitView {
	streams := make([]CommitStream, len(v.Streams))

	for i, stream := range v.Streams {
		if len(history) == 0 || !slices.Contains(history, stream.EntityType) {
			stream.Events = nil

			if len(history) > 0 {
				stream.State = nil
			}
		}

		streams[i] = stream
	}

	v.Streams = streams

	if len(history) == 0 {
		v.CommitID = ""
	}

	return v
}

// checkUnreserved refuses an operation on a partition the engine reserves for the journal.
func checkUnreserved(view ProjectedView) error {
	for _, prefix := range reservedViewPrefixes {
		if strings.HasPrefix(view.Op.PK, prefix) {
			ref := ProjectorOperationRef{Projector: view.Projector, ProjectorIndex: view.ProjectorIndex, Operation: view.Operation}

			return &ReservedViewKeyError{ProjectorOperationRef: ref, PK: view.Op.PK}
		}
	}

	return nil
}

// checkOwner requires an operation to name an owner stream the commit wrote and, for a
// history-dependent projector, one of an entity type it declares.
func checkOwner(view ProjectedView, commit CommitView, history []EntityType) error {
	ref := ProjectorOperationRef{Projector: view.Projector, ProjectorIndex: view.ProjectorIndex, Operation: view.Operation}

	invalid := func(reason string) error {
		return &InvalidViewOperationError{ProjectorOperationRef: ref, Reason: reason}
	}

	if view.Op.Owner == "" {
		return invalid("every operation must name an owner stream")
	}

	for _, stream := range commit.Streams {
		if stream.EntityID != view.Op.Owner {
			continue
		}

		if len(history) > 0 && !slices.Contains(history, stream.EntityType) {
			return invalid(fmt.Sprintf(
				"the owner stream %q is of entity type %q, which the projector does not declare in History",
				view.Op.Owner, stream.EntityType,
			))
		}

		return nil
	}

	return invalid(fmt.Sprintf("the owner stream %q was not written by this commit", view.Op.Owner))
}

// NewCommitView builds the view of one commit from its events: streams ascending by entity-ID
// bytes, each stream's events ascending by sequence, and each stream's post-commit state from
// states (nil when the stream has none). The commit ID is the first event's.
func NewCommitView(events []SerializedEvent, states map[EntityID][]byte) CommitView {
	sorted := slices.Clone(events)
	slices.SortStableFunc(sorted, compareEvents)

	view := CommitView{}
	if len(sorted) > 0 {
		view.CommitID = sorted[0].Metadata.CommitID
	}

	for _, event := range sorted {
		// The metadata carries the values a stored event has (the command ID as the effective
		// causation, an empty extensions map), so a projector reads the same metadata when the commit
		// is live and when a replay reads the stored events.
		event.Metadata.CausationID, event.Metadata.Extensions = jsondefaults.Metadata(
			event.Metadata.CommandID, event.Metadata.CausationID, event.Metadata.Extensions,
		)

		last := len(view.Streams) - 1
		if last < 0 || view.Streams[last].EntityID != event.EntityID {
			view.Streams = append(view.Streams, CommitStream{
				EntityID: event.EntityID, EntityType: event.EntityType, State: states[event.EntityID],
			})
			last++
		}

		view.Streams[last].Events = append(view.Streams[last].Events, event)
	}

	return view
}

// CommitOrderError reports stored events whose commits cannot be put in an order that respects
// every stream's own sequence: commit A wrote a stream before commit B, and B wrote another stream
// before A. No live system produces that, so the events are not a log this engine wrote.
type CommitOrderError struct {
	// Commits are the commit IDs involved, ascending.
	Commits []string
}

// Error describes the cycle.
func (e *CommitOrderError) Error() string {
	return fmt.Sprintf("commits %v cannot be ordered consistently with their streams' sequences", e.Commits)
}

// GroupCommits regroups stored events into the commits that wrote them, the way a rebuild replays a
// history-dependent projector (storage spec, Views): events sharing a commit ID form one
// CommitView, its streams ascending by entity-ID bytes and its events by sequence. An event with
// no commit ID (written before v0.9) is a single-event commit. State is left nil; the caller folds
// it.
//
// The views come back in an order every stream agrees with: a commit that wrote a stream's
// sequence 3 precedes the commit that wrote its sequence 4, for every stream. That is the only
// commit order the log keeps — commit IDs and timestamps are not causal — and it is the order of
// every write to a row owned by one stream (ViewOperation.Owner), so replaying in it reproduces
// the rows of a history-dependent projector. Commits no stream orders relative to each other
// (streams that never met) come in a fixed arbitrary order, by their first event's (entity ID
// bytes, sequence); a projector whose rows depend on that order shares a row between owners, which
// ReplayViews refuses. It returns *CommitOrderError when no such order exists.
func GroupCommits(events []SerializedEvent) ([]CommitView, error) {
	sorted := slices.Clone(events)
	slices.SortStableFunc(sorted, compareEvents)

	var (
		groups  [][]SerializedEvent
		byID    = map[string]int{}
		members = make([]int, len(sorted))
	)

	for i, event := range sorted {
		id := event.Metadata.CommitID
		if id == "" {
			members[i] = len(groups)
			groups = append(groups, []SerializedEvent{event})

			continue
		}

		index, ok := byID[id]
		if !ok {
			index = len(groups)
			byID[id] = index
			groups = append(groups, nil)
		}

		members[i] = index
		groups[index] = append(groups[index], event)
	}

	// Each stream orders the commits that wrote it: an edge from one commit to the next.
	successors := make([][]int, len(groups))
	waiting := make([]int, len(groups))

	for i := 1; i < len(sorted); i++ {
		previous, current := members[i-1], members[i]
		if sorted[i-1].EntityID != sorted[i].EntityID || previous == current {
			continue
		}

		successors[previous] = append(successors[previous], current)
		waiting[current]++
	}

	ready := &commitHeap{groups: groups}
	for i := range groups {
		if waiting[i] == 0 {
			heap.Push(ready, i)
		}
	}

	views := make([]CommitView, 0, len(groups))

	for ready.Len() > 0 {
		next, ok := heap.Pop(ready).(int)
		if !ok {
			break
		}

		views = append(views, NewCommitView(groups[next], nil))

		for _, successor := range successors[next] {
			if waiting[successor]--; waiting[successor] == 0 {
				heap.Push(ready, successor)
			}
		}
	}

	if len(views) < len(groups) {
		return nil, unorderedCommits(groups, waiting)
	}

	return views, nil
}

func unorderedCommits(groups [][]SerializedEvent, waiting []int) error {
	var stuck []string

	for i, count := range waiting {
		if count > 0 {
			stuck = append(stuck, groups[i][0].Metadata.CommitID)
		}
	}

	slices.Sort(stuck)

	return &CommitOrderError{Commits: stuck}
}

// commitHeap orders ready commits by their first event's (entity ID bytes, sequence).
type commitHeap struct {
	groups [][]SerializedEvent
	items  []int
}

func (h *commitHeap) Len() int { return len(h.items) }

func (h *commitHeap) Less(i, j int) bool {
	return compareEvents(h.groups[h.items[i]][0], h.groups[h.items[j]][0]) < 0
}

func (h *commitHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *commitHeap) Push(x any) {
	if index, ok := x.(int); ok {
		h.items = append(h.items, index)
	}
}

func (h *commitHeap) Pop() any {
	last := h.items[len(h.items)-1]
	h.items = h.items[:len(h.items)-1]

	return last
}

// SharedViewKeyError reports a row written on behalf of two different streams. Their commits have
// no order the log keeps, so a replay cannot tell which write came last, and it refuses rather than
// guess a final view that may differ from the live one.
type SharedViewKeyError struct {
	PK string
	SK string

	// First and Second are the owner streams, in the order the replay met them.
	First  EntityID
	Second EntityID
}

// Error describes the shared row.
func (e *SharedViewKeyError) Error() string {
	return fmt.Sprintf("view row %q/%q is written for streams %q and %q, whose commits have no durable order",
		e.PK, e.SK, e.First, e.Second)
}

// ReplayViews is a rebuild of history-dependent projectors in memory: it runs the projectors over
// the commits, in the given order (GroupCommits' order, with each stream's state after its commit
// in CommitView.State), applies their operations to an empty set of rows, and returns the rows
// ordered by pk then sk bytes. Conditions are not evaluated: a replay applies what the live
// commits wrote. It returns *SharedViewKeyError when two operations of different owner streams
// touch one row, because the final row would then depend on an order the log does not keep.
func ReplayViews(ctx context.Context, projectors []CommitProjector, commits []CommitView) ([]SerializedView, error) {
	type key struct{ pk, sk string }

	var (
		rows   = map[key]SerializedView{}
		owners = map[key]EntityID{}
	)

	for _, commit := range commits {
		projected, err := ProjectCommit(ctx, projectors, commit)
		if err != nil {
			return nil, err
		}

		for _, item := range projected {
			target := key{item.Op.PK, item.Op.SK}

			owner := item.Op.Owner
			if first, seen := owners[target]; seen && first != owner {
				return nil, &SharedViewKeyError{PK: target.pk, SK: target.sk, First: first, Second: owner}
			}

			owners[target] = owner

			if item.Op.Kind == ViewDelete {
				delete(rows, target)

				continue
			}

			rows[target] = SerializedView{
				PK: item.Op.PK, SK: item.Op.SK, EntityID: item.Op.Owner, EntityType: item.Op.EntityType,
				Payload: slices.Clone(item.Op.Payload),
			}
		}
	}

	replayed := make([]SerializedView, 0, len(rows))
	for _, row := range rows {
		replayed = append(replayed, row)
	}

	slices.SortFunc(replayed, func(a, b SerializedView) int {
		if byPK := strings.Compare(a.PK, b.PK); byPK != 0 {
			return byPK
		}

		return strings.Compare(a.SK, b.SK)
	})

	return replayed, nil
}

// compareEvents orders events by entity-ID bytes, then sequence: the pinned order of a commit's
// transaction.
func compareEvents(a, b SerializedEvent) int {
	if byID := strings.Compare(string(a.EntityID), string(b.EntityID)); byID != 0 {
		return byID
	}

	switch {
	case a.Sequence < b.Sequence:
		return -1
	case a.Sequence > b.Sequence:
		return 1
	default:
		return 0
	}
}
