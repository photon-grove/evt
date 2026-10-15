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
)

// ReadFence pins a read-only stream to a head. The snapshot companion is mandatory even when
// the reader disables snapshots. CallerStamped records provenance for conflict handling only;
// it is in-memory commit metadata and is never stored.
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

// NormalizeCommit validates a transactional commit and returns a normalized copy: events sorted
// bytewise by stream ID (the pinned transaction order), snapshots and fences likewise, fences
// for written streams dropped (the event put at head + 1 supersedes them), and stream-local
// event order preserved. Noncontiguous input fails rather than being repaired by sorting.
func NormalizeCommit(result SerializedResult) (SerializedResult, error) {
	result.Events = slices.Clone(result.Events)
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
// The set is read from the manifest, never from the calling instance's configuration, so the
// refusal does not depend on every writer being configured alike. A nil manifest declares
// nothing and refuses nothing.
func CheckFenceRetention(fences []ReadFence, manifest *FeatureManifest) error {
	if manifest == nil {
		return nil
	}
	for _, fence := range fences {
		if slices.Contains(manifest.Retention, fence.EntityType) {
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
// commit's event puts, snapshot puts, floor checks, and fence checks. Backend projector groups
// that carry operations must additionally supply their item sizes through
// SizedTransactionGroup, because an opaque group cannot safely bypass commit preflight.
func TransactionItemSizes(result SerializedResult) ([]int, error) {
	return TransactionItemSizesWithTTL(result, nil)
}

// TransactionItemSizesWithTTL is TransactionItemSizes with the ttl attribute the items of
// retained entity types carry, so a backend with retention policies estimates the same bytes its
// items encode. A ttlFor that returns 0 sizes items as the retention-free TransactionItemSizes
// does.
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
	sizes := []int{keySize(SystemNamespacePrefix+"FEATURES", 0), keySize(SystemNamespacePrefix+"REBUILD", 0)}

	written := map[EntityID]bool{}
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
		written[event.EntityID] = true
	}
	for _, snapshot := range result.Snapshots {
		sizes = append(sizes, keySize(snapshot.EntityID, 0)+
			len("seq")+len(strconv.Itoa(int(snapshot.Sequence)))+
			len("eventSeq")+len(strconv.Itoa(int(snapshot.EventSequence)))+
			len("entityType")+len(snapshot.EntityType)+len("payload")+len(snapshot.Payload)+
			ttlSize(snapshot.EntityType))
		delete(written, snapshot.EntityID)
	}
	// A written stream without a snapshot contributes a floor check on its snapshot item.
	ids := make([]EntityID, 0, len(written))
	for id := range written {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		sizes = append(sizes, keySize(id, 0))
	}
	for _, fence := range result.Fences {
		sizes = append(sizes, keySize(fence.EntityID, fence.Head+1), keySize(fence.EntityID, 0))
	}

	return sizes, nil
}

// PreflightCommit validates a commit's shared budget — the event puts, snapshot puts, floor
// checks, fence checks, and control checks — before any backend write. Backends add the sizes of
// the additional items they write (projector operations) before checking the total.
func PreflightCommit(result SerializedResult) error {
	sizes, err := TransactionItemSizes(result)
	if err != nil {
		return err
	}

	return CommitBudget(sizes)
}
