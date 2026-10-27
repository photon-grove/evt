package evt

import (
	"context"
	"errors"
	"fmt"
	"maps"
)

// FeatureStreamHeads is the required marker feature of the per-stream head item (ADR 0007;
// storage spec, Stream Heads). A store that declares it has a head item for every stream, and a
// writer refuses a store that does not (StreamHeadsNotMigratedError).
const FeatureStreamHeads = "stream-heads"

// StreamHeadsFormat is the stored-format version of FeatureStreamHeads this build models.
const StreamHeadsFormat FeatureFormat = 1

// WithStreamHeads returns a copy of the supported set that also models the stream-heads feature
// at the format version this build writes. Every writer of this format models it, so each
// repository adds it to whatever extensions the caller configured.
func (s SupportedFeatures) WithStreamHeads() SupportedFeatures {
	supported := make(SupportedFeatures, len(s)+1)
	maps.Copy(supported, s)
	supported[FeatureStreamHeads] = StreamHeadsFormat

	return supported
}

// DeclaresStreamHeads reports whether the manifest declares the stream-heads feature at the format
// version this build writes.
func (m FeatureManifest) DeclaresStreamHeads() bool {
	format, ok := m.Features[FeatureStreamHeads]

	return ok && format == StreamHeadsFormat
}

// RequireStreamHeads returns *StreamHeadsNotMigratedError unless the manifest (nil when the store
// declares nothing) declares stream-heads at the format version this build writes.
func RequireStreamHeads(manifest *FeatureManifest) error {
	if manifest != nil && manifest.DeclaresStreamHeads() {
		return nil
	}

	return &StreamHeadsNotMigratedError{}
}

// ErrStreamHeadsNotMigrated matches a StreamHeadsNotMigratedError with errors.Is.
var ErrStreamHeadsNotMigrated = errors.New("store has not been migrated to stream heads")

// StreamHeadsNotMigratedError refuses a write to a store whose feature marker does not declare
// stream-heads while the store holds data: the streams have no head items, so a head-aware writer
// would fail every stream. The refusal covers every write path that validates the marker —
// commits (outcome-only and rejection commits included), PutSnapshot, and CompactBelow. Run the
// migration (evt-doctor migrate-heads) with every older writer stopped. A store with no data is
// initialized instead (StoreInitializer), or by a lazily initializing writer's first write.
// Nothing was written.
type StreamHeadsNotMigratedError struct{}

// Error returns the error message string.
func (e *StreamHeadsNotMigratedError) Error() string {
	return "store does not declare the " + FeatureStreamHeads +
		" feature and holds data without per-stream head items; migrate it before writing"
}

// Is makes errors.Is(err, ErrStreamHeadsNotMigrated) match.
func (e *StreamHeadsNotMigratedError) Is(target error) bool {
	return target == ErrStreamHeadsNotMigrated
}

// ErrStoreNotInitialized matches a StoreNotInitializedError with errors.Is.
var ErrStoreNotInitialized = errors.New("store is not initialized")

// StoreNotInitializedError refuses a write to a store whose feature marker does not declare
// stream-heads, by a writer that does not initialize stores itself (the DynamoDB default): telling
// a fresh store from one written before stream heads takes a table scan, which a writer never
// runs. Initialize a fresh store once, at deploy time, with StoreInitializer.Initialize (or
// "evt-doctor init"), and migrate a store written before stream heads with "evt-doctor
// migrate-heads". The refusal covers every write path that validates the marker — commits
// (outcome-only and rejection commits included), PutSnapshot, and CompactBelow. Reads need no
// feature. Nothing was written.
type StoreNotInitializedError struct{}

// Error returns the error message string.
func (e *StoreNotInitializedError) Error() string {
	return "store is not initialized: the feature manifest does not declare " + FeatureStreamHeads +
		"; initialize a fresh store, or migrate one written before stream heads, before writing"
}

// Is makes errors.Is(err, ErrStoreNotInitialized) match.
func (e *StoreNotInitializedError) Is(target error) bool {
	return target == ErrStoreNotInitialized
}

// IsStoreNotInitializedErr returns true when err is (or wraps) a StoreNotInitializedError.
func IsStoreNotInitializedErr(err error) bool {
	var notInitialized *StoreNotInitializedError

	return errors.As(err, &notInitialized)
}

// StoreInitializer is implemented by every repository in this module: it initializes a store
// explicitly (storage spec, Stream Heads, Required feature).
type StoreInitializer interface {
	// Initialize declares stream-heads on a store that holds no data, with the verification a
	// lazily initializing writer runs on its first write, and reports whether this call declared
	// it. It is an operator step, run once at deploy time before any writer starts, and it is
	// idempotent: a store that already declares the feature is left alone after one manifest read,
	// with nothing scanned or written. A store that holds data without declaring the feature is
	// refused with *StreamHeadsNotMigratedError (migrate it instead), and a manifest that declares
	// a feature the writer does not model with *UnsupportedFeatureError. Only stream-heads is
	// declared. On DynamoDB the verification is a strongly consistent Scan, so the caller needs
	// dynamodb:Scan on the event log; writers then never scan.
	Initialize(ctx context.Context) (bool, error)
}

// ErrStreamHeadDrift matches a StreamHeadDriftError with errors.Is.
var ErrStreamHeadDrift = errors.New("stream head drift")

// StreamHeadDriftError reports that a stream's head item disagrees with its log: a writer
// appended without maintaining the head, events were deleted, or a snapshot is ahead of the head
// (invariants H2 to H4 are broken). It is not retryable and never exhausted into a stale-fence
// rejection: re-preparing the command cannot repair it, and recording a rejection for it would be
// false. It writes no outcome row. Repair is offline (evt-doctor migrate-heads --repair).
type StreamHeadDriftError struct {
	EntityID EntityID

	// Stored is the stored head item's value. It is meaningful only when HeadAbsent is false.
	Stored EventSequence

	// HeadAbsent reports that the stream has no head item.
	HeadAbsent bool

	// Derived is the head the log itself derives: the larger of the highest event sequence and the
	// snapshot's event sequence (for a snapshot write, including the snapshot being written).
	Derived EventSequence
}

// Error returns the error message string.
func (e *StreamHeadDriftError) Error() string {
	if e.HeadAbsent {
		return fmt.Sprintf("stream %s has no head item but its log derives head %d", e.EntityID, e.Derived)
	}

	return fmt.Sprintf("stream %s has head item %d but its log derives head %d", e.EntityID, e.Stored, e.Derived)
}

// Is makes errors.Is(err, ErrStreamHeadDrift) match.
func (e *StreamHeadDriftError) Is(target error) bool { return target == ErrStreamHeadDrift }

// IsStreamHeadDriftErr returns true when err is (or wraps) a StreamHeadDriftError.
func IsStreamHeadDriftErr(err error) bool {
	var drift *StreamHeadDriftError

	return errors.As(err, &drift)
}

// IsStreamHeadsNotMigratedErr returns true when err is (or wraps) a StreamHeadsNotMigratedError.
func IsStreamHeadsNotMigratedErr(err error) bool {
	var notMigrated *StreamHeadsNotMigratedError

	return errors.As(err, &notMigrated)
}
