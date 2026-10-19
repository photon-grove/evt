package evt

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"slices"
	"strconv"
	"time"

	"github.com/photon-grove/evt/internal/jsondefaults"
	"github.com/photon-grove/evt/internal/streamheads"
)

// ReadFence pins a read-only stream to a head: the commit fails when any writer has written the
// stream since the reader saw it at Head (a head of 0 means the reader saw no events). On DynamoDB
// the fence is one ConditionCheck on the stream's head item (ADR 0007). CallerStamped records
// provenance for conflict handling only; it is in-memory commit metadata and is never stored.
type ReadFence struct {
	EntityID      EntityID
	EntityType    EntityType
	Head          EventSequence
	CallerStamped bool
}

// FenceConflictError identifies the read-only stream whose fence failed. Another writer moved
// the stream past the fenced head. A conflict on an engine-filled fence is retryable (the engine
// can reload and re-decide, and backends classify it transient like an event-put conflict); a
// conflict on a caller-stamped fence is a stale-fence rejection the caller must handle, because
// the engine cannot know the caller's decision is still valid.
type FenceConflictError struct {
	Fence ReadFence
}

func (e *FenceConflictError) Error() string {
	return fmt.Sprintf("read fence conflict on %s at head %d", e.Fence.EntityID, e.Fence.Head)
}

// Unwrap keeps a fence failure matchable as optimistic concurrency without losing provenance.
func (e *FenceConflictError) Unwrap() error { return NewConflictError(e.Error()) }

// RetentionConflictError refuses fences whose type is durably declared to expire by TTL.
type RetentionConflictError struct{ EntityType EntityType }

func (e *RetentionConflictError) Error() string {
	return fmt.Sprintf("read fences are unsafe for entity type %q declared with retention", e.EntityType)
}

// CommitTooLargeError reports a failed preflight. No part of an oversized commit is written.
type CommitTooLargeError struct {
	Items       int
	Bytes       int
	LargestItem int
}

func (e *CommitTooLargeError) Error() string {
	return fmt.Sprintf("commit too large: %d items, %d bytes, largest item %d bytes", e.Items, e.Bytes, e.LargestItem)
}

// CommitBudget checks DynamoDB's transaction limits — 100 items, 4 MB per request, 400 KB per
// item — against 95% of each limit, a 5% safety margin for key-encoding and rounding
// differences. The budgets apply on every backend, so an oversized commit fails identically
// everywhere instead of committing on one and raising a validation error on another.
func CommitBudget(itemSizes []int) error {
	total, largest := 0, 0
	for _, size := range itemSizes {
		if size < 0 || size > math.MaxInt-total {
			return &CommitTooLargeError{Items: len(itemSizes), Bytes: math.MaxInt, LargestItem: math.MaxInt}
		}

		total += size
		largest = max(largest, size)
	}

	if len(itemSizes) > 100 || total > 3_800_000 || largest > 380_000 {
		return &CommitTooLargeError{Items: len(itemSizes), Bytes: total, LargestItem: largest}
	}

	return nil
}

// ProjectorOnlyCommitError refuses a commit that carries projector operations but writes no
// event. Views are derived state: every view row must be a projection of a committed event, so
// operations without a backing event in the same commit could never be rebuilt by replay
// (ADR-0012). Nothing is written.
type ProjectorOnlyCommitError struct {
	// Operations is the number of projector operations the commit carried.
	Operations int
}

func (e *ProjectorOnlyCommitError) Error() string {
	return fmt.Sprintf("commit carries %d projector operations but no events; views must be projections of committed events", e.Operations)
}

// ProjectorOperationCount returns the number of projector operations the result carries: those of
// its transaction groups and its commit-wide view operations.
func ProjectorOperationCount(result SerializedResult) int {
	return legacyOperationCount(result) + len(result.Views)
}

// legacyOperationCount counts the operations of the result's transaction groups.
func legacyOperationCount(result SerializedResult) int {
	operations := 0
	for _, group := range result.Transaction {
		if group != nil {
			operations += group.Len()
		}
	}

	return operations
}

// CheckProjectorOperations returns a *ProjectorOnlyCommitError when the result carries projector
// operations but no events. Backends call it before any other work; NormalizeCommit calls it too.
func CheckProjectorOperations(result SerializedResult) error {
	if len(result.Events) > 0 {
		return nil
	}

	operations := ProjectorOperationCount(result)
	if operations == 0 {
		return nil
	}

	return &ProjectorOnlyCommitError{Operations: operations}
}

// ProjectorOperationsUnsupportedError refuses a commit that carries projector operations to a
// backend that cannot execute them in the commit's transaction. Dropping them would commit the
// events without the view writes that were meant to be atomic with them, so the backend fails
// closed before any write.
type ProjectorOperationsUnsupportedError struct {
	// Backend names the refusing backend.
	Backend string
	// Operations is the number of projector operations the commit carried.
	Operations int
}

func (e *ProjectorOperationsUnsupportedError) Error() string {
	return fmt.Sprintf(
		"%s cannot execute projector operations in the commit transaction; refusing %d operations instead of dropping them",
		e.Backend, e.Operations,
	)
}

// CheckProjectorOperationsSupported returns *ProjectorOperationsUnsupportedError when the result
// carries any projector operation, whether an operation of a transaction group or a commit-wide
// view operation. A backend that executes neither calls it before writing, so it commits nothing
// instead of dropping the operations. A backend that executes view operations but no transaction
// group calls CheckLegacyProjectorOperationsSupported instead.
func CheckProjectorOperationsSupported(backend string, result SerializedResult) error {
	if operations := ProjectorOperationCount(result); operations > 0 {
		return &ProjectorOperationsUnsupportedError{Backend: backend, Operations: operations}
	}

	return nil
}

// CheckLegacyProjectorOperationsSupported returns *ProjectorOperationsUnsupportedError when the
// result carries any operation of a transaction group. A backend that executes commit-wide view
// operations (SerializedResult.Views) but no transaction group calls it before writing; the view
// operations are its own to write or refuse (*ViewsNotConfiguredError).
func CheckLegacyProjectorOperationsSupported(backend string, result SerializedResult) error {
	if operations := legacyOperationCount(result); operations > 0 {
		return &ProjectorOperationsUnsupportedError{Backend: backend, Operations: operations}
	}

	return nil
}

// UnsupportedBatchEntryError refuses a SerializedResult sent through the deprecated CommitStream
// batch path that carries Snapshots, Fences, or an Outcome. The batch path writes events (and
// projector operations) only, so it would drop them silently; commit such a result with
// Repository.Commit instead. Nothing from the entry was written.
type UnsupportedBatchEntryError struct {
	Snapshots int
	Fences    int
	Outcomes  int
}

func (e *UnsupportedBatchEntryError) Error() string {
	return fmt.Sprintf(
		"CommitStream entry carries %d snapshots, %d read fences, and %d outcomes, which the batch path cannot honor; use Repository.Commit",
		e.Snapshots, e.Fences, e.Outcomes,
	)
}

// CheckBatchEntry returns *UnsupportedBatchEntryError for a result with Snapshots, Fences, or an
// Outcome. A legacy batch path that does not route each entry through Repository.Commit calls it
// before batching, so it fails closed instead of dropping the new fields.
func CheckBatchEntry(result SerializedResult) error {
	outcomes := 0
	if result.Outcome != nil {
		outcomes = 1
	}

	if len(result.Snapshots) == 0 && len(result.Fences) == 0 && outcomes == 0 {
		return nil
	}

	return &UnsupportedBatchEntryError{Snapshots: len(result.Snapshots), Fences: len(result.Fences), Outcomes: outcomes}
}

// InvalidCommitError reports a commit built wrong — for example an event whose ID is not the
// canonical {entityId}:{sequence} for its stream and sequence. Nothing was written.
type InvalidCommitError struct {
	Reason string
}

func (e *InvalidCommitError) Error() string { return "invalid commit: " + e.Reason }

// checkCommitText validates the identifier strings and metadata of the events, snapshots, and fences
// of a commit that records an outcome.
func checkCommitText(result SerializedResult) error {
	for _, event := range result.Events {
		for _, field := range []struct{ name, value string }{
			{"event.id", string(event.ID)},
			{"event.type", string(event.Type)},
			{"event.entityId", string(event.EntityID)},
			{"event.entityType", string(event.EntityType)},
		} {
			if err := checkText(field.name, field.value); err != nil {
				return err
			}
		}

		if err := CheckMetadata(event.Metadata); err != nil {
			return err
		}
	}

	for _, snapshot := range result.Snapshots {
		if err := checkText("snapshot.entityId", string(snapshot.EntityID)); err != nil {
			return err
		}

		if err := checkText("snapshot.entityType", string(snapshot.EntityType)); err != nil {
			return err
		}
	}

	for _, fence := range result.Fences {
		if err := checkText("fence.entityId", string(fence.EntityID)); err != nil {
			return err
		}

		if err := checkText("fence.entityType", string(fence.EntityType)); err != nil {
			return err
		}
	}

	return nil
}

// NormalizeCommit validates a transactional commit and returns a normalized copy: events sorted
// bytewise by stream ID (the pinned transaction order), snapshots and fences likewise, fences
// for written streams dropped (the event put at head + 1 supersedes them), and stream-local
// event order preserved. Noncontiguous input fails rather than being repaired by sorting.
func NormalizeCommit(result SerializedResult) (SerializedResult, error) {
	if err := CheckProjectorOperations(result); err != nil {
		return result, err
	}

	// View operations are validated before anything else is derived, so an invalid, oversized-key,
	// or duplicated operation fails the commit before any write on every backend.
	if err := CheckViews(result.Views); err != nil {
		return result, err
	}

	if result.Outcome != nil {
		// The outcome is checked before anything else is derived: a bad command ID, an oversized
		// response, or an oversized rejection fails the commit before any write.
		if err := CheckOutcome(result.Outcome); err != nil {
			return result, err
		}

		// Everything else this commit persists as text is held to the same rule as the metadata
		// and the outcome: valid UTF-8 and no U+0000 (PostgreSQL cannot store it), so the commit
		// has the same result on every backend and fails before any write.
		if err := checkCommitText(result); err != nil {
			return result, err
		}
	}

	result.Events = slices.Clone(result.Events)
	result.Views = slices.Clone(result.Views)
	result.Snapshots = slices.Clone(result.Snapshots)
	result.Fences = slices.Clone(result.Fences)
	slices.SortStableFunc(result.Events, func(a, b SerializedEvent) int { return compareID(a.EntityID, b.EntityID) })
	slices.SortFunc(result.Snapshots, func(a, b SerializedSnapshot) int { return compareID(a.EntityID, b.EntityID) })
	slices.SortFunc(result.Fences, func(a, b ReadFence) int { return compareID(a.EntityID, b.EntityID) })

	first, last := map[EntityID]SerializedEvent{}, map[EntityID]SerializedEvent{}
	for _, event := range result.Events {
		if err := CheckReservedEntityID(event.EntityID); err != nil {
			return result, err
		}

		// The event ID is the canonical {entityId}:{sequence}. DynamoDB does not store it and rebuilds
		// it from the key, and the outcome receipt names events by it, so a commit that disagrees
		// would write one event and tell retries about another.
		//
		// An absent ID is tolerated on a commit without an outcome, as it was before v0.9 (the
		// stored key is what counts); a commit that records an outcome needs the exact ID.
		if canonical := GetEventID(event.EntityID, event.Sequence); event.ID != canonical &&
			(event.ID != "" || result.Outcome != nil) {
			return result, &InvalidCommitError{Reason: fmt.Sprintf(
				"event %q should have the ID %q", event.ID, canonical,
			)}
		}

		previous, seen := last[event.EntityID]
		if event.Sequence < 1 || (seen && (previous.Sequence == math.MaxInt || event.Sequence != previous.Sequence+1)) {
			return result, fmt.Errorf("event %s: sequences must be contiguous and start at 1", event.ID)
		}
		if seen && previous.EntityType != event.EntityType {
			return result, fmt.Errorf("stream %s mixes entity types", event.EntityID)
		}
		if !seen {
			first[event.EntityID] = event
		}
		last[event.EntityID] = event
	}

	for i, snapshot := range result.Snapshots {
		event, written := last[snapshot.EntityID]
		if !written || snapshot.EventSequence != event.Sequence || snapshot.EntityType != event.EntityType || snapshot.Sequence < 1 {
			return result, fmt.Errorf("snapshot for %s must cover this commit's last event with a positive sequence", snapshot.EntityID)
		}
		if i > 0 && result.Snapshots[i-1].EntityID == snapshot.EntityID {
			return result, fmt.Errorf("duplicate snapshot for %s", snapshot.EntityID)
		}
	}

	fences := result.Fences[:0]
	for i, fence := range result.Fences {
		if err := CheckReservedEntityID(fence.EntityID); err != nil {
			return result, err
		}
		if fence.Head < 0 || fence.Head == math.MaxInt {
			return result, fmt.Errorf("invalid fence head for %s", fence.EntityID)
		}
		if i > 0 && result.Fences[i-1].EntityID == fence.EntityID {
			return result, fmt.Errorf("duplicate read fence for %s", fence.EntityID)
		}
		if _, written := first[fence.EntityID]; !written {
			fences = append(fences, fence)
		}
	}
	result.Fences = fences

	if result.Outcome != nil {
		// The stored event IDs are exactly the events this commit writes, in the order of the
		// transaction's event puts: stream ID bytewise, then sequence. The caller's copy is
		// never mutated.
		outcome := *result.Outcome
		outcome.EventIDs = make([]EventID, 0, len(result.Events))

		// An empty response is no response: the attribute is absent on every backend.
		if len(outcome.Response) == 0 {
			outcome.Response = nil
		}

		for _, event := range result.Events {
			outcome.EventIDs = append(outcome.EventIDs, event.ID)
		}

		if len(outcome.EventIDs) == 0 {
			outcome.EventIDs = nil
		}

		result.Outcome = &outcome
	}

	return result, nil
}

func compareID(a, b EntityID) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// CheckFenceRetention refuses fences for entity types in the durable manifest's retention set.
// The set is read from the manifest, so the refusal binds writers that configured no retention
// themselves. A nil manifest declares nothing and refuses nothing. Use CheckFenceRetentionWith to
// also refuse the types a repository was locally configured to expire.
func CheckFenceRetention(fences []ReadFence, manifest *FeatureManifest) error {
	return CheckFenceRetentionWith(fences, manifest, nil)
}

// CheckFenceRetentionWith refuses fences whose entity type is in the durable manifest's retention
// set or reports true from locallyRetained, the repository's own retention configuration. The two
// are a union: local configuration protects a writer whose types were never declared in the
// manifest, and the manifest protects against writers configured differently. A nil
// locallyRetained consults the manifest only.
func CheckFenceRetentionWith(fences []ReadFence, manifest *FeatureManifest, locallyRetained func(EntityType) bool) error {
	for _, fence := range fences {
		durable := manifest != nil && slices.Contains(manifest.Retention, fence.EntityType)
		if durable || (locallyRetained != nil && locallyRetained(fence.EntityType)) {
			return &RetentionConflictError{EntityType: fence.EntityType}
		}
	}

	return nil
}

// CommitMetadata returns a copy stamped once for the entire engine commit. The commit ID is the
// command ID when present — overriding any caller-supplied commit ID; otherwise a caller-supplied
// commit ID is preserved; otherwise a cryptographically random ULID is generated. Callers must
// stamp one metadata value and use it for every stream of a commit.
func CommitMetadata(metadata Metadata) Metadata {
	if metadata.CommandID != nil && *metadata.CommandID != "" {
		metadata.CommitID = string(*metadata.CommandID)

		return metadata
	}

	if metadata.CommitID != "" {
		return metadata
	}

	var entropy [16]byte
	_, _ = rand.Read(entropy[:]) // crypto/rand.Read cannot fail on supported Go platforms.
	milliseconds := uint64(time.Now().UnixMilli())
	for i := 5; i >= 0; i-- {
		entropy[i] = byte(milliseconds)
		milliseconds >>= 8
	}
	value := new(big.Int).SetBytes(entropy[:])
	base := big.NewInt(32)
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	encoded := make([]byte, 26)
	for i := 25; i >= 0; i-- {
		var remainder big.Int
		value.QuoRem(value, base, &remainder)
		encoded[i] = alphabet[remainder.Int64()]
	}
	metadata.CommitID = string(encoded)

	return metadata
}

// TransactionItemSizes supplies the shared, backend-neutral DynamoDB-shaped estimate for a
// commit's event puts, snapshot puts, head updates, and fence checks. Backend projector groups
// that carry operations must additionally supply their item sizes through
// SizedTransactionGroup, because an opaque group cannot safely bypass commit preflight.
// PreflightCommit adds those group sizes, so every backend budgets projector operations alike.
func TransactionItemSizes(result SerializedResult) ([]int, error) {
	return TransactionItemSizesWithTTL(result, nil)
}

// TransactionItemSizesWithTTL is TransactionItemSizes with the ttl attribute the items of
// retained entity types carry, so a backend with retention policies estimates the same bytes its
// items encode. A ttlFor that returns 0 sizes items as the retention-free TransactionItemSizes
// does.
//
// The estimate follows the commit's actions (storage spec, Stream Heads): the feature-marker
// check, every event put, every snapshot put, one head update per written stream, one head check
// per fenced stream, and the outcome put. A written stream pays one action beyond its events
// whether it creates or appends, and the head update is budgeted by the rule every update
// follows: its key attributes, expression strings, name placeholders and names, and values.
func TransactionItemSizesWithTTL(result SerializedResult, ttlFor func(EntityType) int64) ([]int, error) {
	ttlSize := func(entityType EntityType) int {
		if ttlFor == nil {
			return 0
		}
		if ttl := ttlFor(entityType); ttl != 0 {
			return len("ttl") + len(strconv.FormatInt(ttl, 10))
		}

		return 0
	}

	keySize := func(id EntityID, sequence EventSequence) int {
		return len("pk") + len("sk") + len(id) + len(strconv.Itoa(int(sequence)))
	}
	// Only actions a commit actually emits are budgeted: the feature-marker check today. The
	// rebuild-lock check (C10) joins this list in the change that starts emitting it.
	sizes := []int{keySize(SystemNamespacePrefix+"FEATURES", 0)}

	type written struct {
		entityType  EntityType
		first, last EventSequence
	}

	streams := map[EntityID]written{}
	for _, event := range result.Events {
		metadata := event.Metadata
		metadata.CausationID, metadata.Extensions = jsondefaults.Metadata(
			metadata.CommandID, metadata.CausationID, metadata.Extensions,
		)
		encoded, err := json.Marshal(metadata)
		if err != nil {
			return nil, err
		}
		sizes = append(sizes, keySize(event.EntityID, event.Sequence)+
			len("type")+len(event.Type)+len("version")+len(strconv.Itoa(int(event.Version)))+
			len("entityType")+len(event.EntityType)+len("payload")+len(event.Payload)+len("metadata")+len(encoded)+
			ttlSize(event.EntityType))

		stream, seen := streams[event.EntityID]
		if !seen {
			stream = written{entityType: event.EntityType, first: event.Sequence}
		}
		stream.last = event.Sequence
		streams[event.EntityID] = stream
	}
	for _, snapshot := range result.Snapshots {
		sizes = append(sizes, keySize(snapshot.EntityID, 0)+
			len("seq")+len(strconv.Itoa(int(snapshot.Sequence)))+
			len("eventSeq")+len(strconv.Itoa(int(snapshot.EventSequence)))+
			len("entityType")+len(snapshot.EntityType)+len("payload")+len(snapshot.Payload)+
			ttlSize(snapshot.EntityType))
	}

	// Every written stream contributes its head update, whether it creates or appends and whether
	// or not it writes a snapshot. There is no continuation check and no floor check.
	ids := make([]EntityID, 0, len(streams))
	for id := range streams {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		stream := streams[id]
		var ttl int64
		if ttlFor != nil {
			ttl = ttlFor(stream.entityType)
		}

		sizes = append(sizes, streamheads.UpdateSize(string(id), int64(stream.first), int64(stream.last), ttl))
	}

	// A fenced read-only stream is one check on its head item.
	for _, fence := range result.Fences {
		sizes = append(sizes, streamheads.FenceSize(string(fence.EntityID), int64(fence.Head)))
	}

	// The outcome put is the last action (storage spec, Writes).
	if result.Outcome != nil {
		size, err := OutcomeItemSize(*result.Outcome, result.Events)
		if err != nil {
			return nil, err
		}

		sizes = append(sizes, size)
	}

	return sizes, nil
}

// SizedTransactionGroup is a TransactionGroup that reports the DynamoDB-encoded size of each of
// its operations, so a backend's commit preflight budgets it exactly like DynamoDB does.
type SizedTransactionGroup interface {
	TransactionGroup

	// OperationSizes returns one encoded size in bytes per operation, Len() entries in all.
	OperationSizes() []int
}

// ProjectorOperationSizes returns one budget entry per projector operation the result carries:
// the group's reported sizes when it is a SizedTransactionGroup, otherwise zero bytes per
// operation, then ViewOperationSize for each commit-wide view operation. The action count is exact
// either way, which is what makes CommitTooLarge fire at the same boundary on every backend.
func ProjectorOperationSizes(result SerializedResult) []int {
	var sizes []int

	for _, group := range result.Transaction {
		if group == nil {
			continue
		}

		count := group.Len()
		reported, sized := group.(SizedTransactionGroup)
		var operationSizes []int
		if sized {
			operationSizes = reported.OperationSizes()
		}

		for i := range count {
			size := 0
			if i < len(operationSizes) {
				size = operationSizes[i]
			}
			sizes = append(sizes, size)
		}
	}

	for _, view := range result.Views {
		sizes = append(sizes, ViewOperationSize(view.Op))
	}

	return sizes
}

// PreflightCommit validates a commit's shared budget — the event puts, snapshot puts, floor
// checks, fence checks, control checks, and projector operations — before any backend write. A
// backend that writes projector operations itself (DynamoDB sizes its own items exactly) does not
// call it; a backend that has none to write refuses them before this point.
func PreflightCommit(result SerializedResult) error {
	sizes, err := TransactionItemSizes(result)
	if err != nil {
		return err
	}

	return CommitBudget(append(sizes, ProjectorOperationSizes(result)...))
}
