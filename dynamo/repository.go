package dynamo

import (
	"context"
	"log/slog"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"

	"github.com/photon-grove/evt"
)

// Repository wires DynamoDB access for the event store.
type Repository struct {
	EventsTable string

	viewsTable string // table of commit-wide view operations (C7); empty refuses them

	client         Client
	encoder        *attributevalue.Encoder
	decoder        *attributevalue.Decoder
	consistentRead bool // default true for backward compatibility
	scanSegments   int  // parallel Scan segments for table-wide reads; <=1 means a single sequential scan
	retention      Retention
	now            func() time.Time // clock for TTL expiry; nil means time.Now
	logger         *slog.Logger
	features       *FeatureMarker // mandatory feature validation (C9)
	lazyInit       bool           // a write initializes an uninitialized store instead of refusing it
	ttlFloors      *ttlFloors     // largest loaded ttl per stream, for the head item's never-lower ttl
}

// Retention maps entity types to TTL durations for events and inline snapshots. Other rows have no
// ttl attribute; DynamoDB TTL must be enabled on `ttl`.
//
// Use it only for terminal, short-lived streams that are never rebuilt. TTL deletes items
// independently and can leave undetectable prefix gaps. For retained history, use durable snapshots
// and Compactor.CompactBelow.
type Retention map[evt.EntityType]time.Duration

const tagKey string = "json"

// NewRepository constructs a Repository with configured encoders/decoders.
func NewRepository(client Client, eventsTable string) *Repository {
	encoder := attributevalue.NewEncoder(func(opts *attributevalue.EncoderOptions) {
		opts.TagKey = tagKey
	})
	decoder := attributevalue.NewDecoder(func(opts *attributevalue.DecoderOptions) {
		opts.TagKey = tagKey
	})

	return &Repository{EventsTable: eventsTable, client: client, encoder: encoder, decoder: decoder, consistentRead: true,
		features: newFeatureMarker(client, eventsTable, nil, false), ttlFloors: newTTLFloors()}
}

// WithLazyInitialization returns a shallow copy of the repository that, when lazy is true,
// initializes a store that does not declare stream-heads on its first write, as Initialize does,
// instead of refusing it with *evt.StoreNotInitializedError. That costs a strongly consistent Scan
// of the table at first write (two when the store is fresh) and needs dynamodb:Scan on the event
// log, so it is for tests, local emulators, and quickstarts; production writers keep the default
// (false) and the store is initialized by an operator step (Initialize, or "evt-doctor init").
//
// The setting travels with the feature marker: the copy gets its own marker (with the same
// supported features, and an empty validation cache), a marker created afterwards with
// FeatureMarker inherits the setting, and a marker passed to WithFeatureMarker brings the setting
// of the repository that created it.
func (repo *Repository) WithLazyInitialization(lazy bool) *Repository {
	r := *repo
	r.lazyInit = lazy
	r.features = newFeatureMarker(r.client, r.EventsTable, repo.features.supported, lazy)

	return &r
}

// Initialize initializes the store: it declares stream-heads on a table that holds no data and
// reports whether this call declared it (evt.StoreInitializer). It is idempotent, and a table
// that already declares the feature costs one strongly consistent GetItem. Otherwise it scans the
// table (strongly consistent) to verify it holds no data, so run it once, at deploy time, with a
// role that may Scan the event log: writers then need no dynamodb:Scan. A populated table that
// does not declare the feature is refused with *evt.StreamHeadsNotMigratedError (run "evt-doctor
// migrate-heads" instead).
func (repo *Repository) Initialize(ctx context.Context) (bool, error) {
	return repo.features.Initialize(ctx)
}

// The DynamoDB repository initializes stores explicitly.
var _ evt.StoreInitializer = (*Repository)(nil)

// WithLogger returns a shallow copy of the repository with the logger updated.
func (repo *Repository) WithLogger(logger *slog.Logger) *Repository {
	r := *repo
	r.logger = logger
	return &r
}

// WithConsistentRead returns a shallow copy of the repository with the consistent read setting
// updated. When false, reads use eventually consistent reads (half the RCU cost).
// The original repository is not modified, so it is safe to derive both a strong-consistency
// and eventual-consistency variant from the same base repository.
func (repo *Repository) WithConsistentRead(consistent bool) *Repository {
	r := *repo
	r.consistentRead = consistent
	return &r
}

// WithScanSegments returns a shallow copy of the repository configured to run table-wide reads
// (StreamAllEvents and, transitively, StreamEntities/RebuildProjections) as a DynamoDB parallel
// Scan split across n segments. n <= 1 keeps the default single sequential scan.
//
// Parallel scans trade higher read throughput (and consumed capacity) for faster table sweeps.
// Pick n based on table size and provisioned/burst capacity; the original repository is unchanged
// so hot-path readers can keep a non-segmented copy.
func (repo *Repository) WithScanSegments(n int) *Repository {
	r := *repo
	r.scanSegments = n
	return &r
}

// maxScanSegments caps configured parallelism. It is well within DynamoDB's TotalSegments limit
// (1,000,000) and keeps the segment count comfortably inside int32 for the Scan parameters.
const maxScanSegments = 1024

// scanSegmentCount normalizes the configured segment count to a usable parallelism factor in the
// range [1, maxScanSegments].
func (repo *Repository) scanSegmentCount() int {
	if repo == nil || repo.scanSegments <= 1 {
		return 1
	}

	if repo.scanSegments > maxScanSegments {
		return maxScanSegments
	}

	return repo.scanSegments
}

// WithRetention returns a shallow copy of the repository that stamps a DynamoDB `ttl` attribute on
// committed events and inline snapshots whose entity type appears in the policy. Entity types absent
// from the policy are written without a `ttl` and are never auto-expired. The original repository is
// unchanged. See Retention for the safety constraint.
func (repo *Repository) WithRetention(retention Retention) *Repository {
	r := *repo
	r.retention = retention
	return &r
}

// WithClock returns a shallow copy of the repository using the given clock to compute TTL expiry
// timestamps. Intended for tests; production callers leave it unset to use time.Now.
func (repo *Repository) WithClock(now func() time.Time) *Repository {
	r := *repo
	r.now = now
	return &r
}

// WithFeatureMarker returns a shallow copy of the repository whose commits enforce the store's
// feature manifest (ADR-0004 C9): the first commit validates the SYSTEM#FEATURES manifest against
// the supported feature set and caches its version, and every commit then carries the mandatory
// ConditionCheck on the marker, so a manifest declared by another writer (say, a Rust service
// using an extension this writer does not model) fails the check and re-validates; an unsupported
// pair refuses the writer outright.
//
// Without this option the mandatory marker check supports no extensions. A nil marker resets
// that default; it never disables enforcement.
// Create markers with repo.FeatureMarker. A marker bound to another events table or client
// panics with *evt.FeatureMarkerTableMismatchError or *evt.FeatureMarkerClientMismatchError.
func (repo *Repository) WithFeatureMarker(marker *FeatureMarker) *Repository {
	r := *repo
	r.features = marker
	if r.features == nil {
		r.features = r.FeatureMarker(nil)
	}
	if err := r.checkFeatureMarkerTable(); err != nil {
		panic(err)
	}

	return &r
}

func (repo *Repository) checkFeatureMarkerTable() error {
	if repo.features.eventsTable != repo.EventsTable {
		return &evt.FeatureMarkerTableMismatchError{
			EventsTable: repo.EventsTable, MarkerEventsTable: repo.features.eventsTable,
		}
	}

	// Interface equality is safe only for comparable client handles (normally pointers).
	// Fail closed for other handles rather than compare their configuration or contents.
	if !comparableHandle(repo.client) || !comparableHandle(repo.features.client) || repo.client != repo.features.client {
		return &evt.FeatureMarkerClientMismatchError{}
	}

	return nil
}

// ttlFor returns the Unix-epoch expiry for an event or snapshot of the given entity type, or 0 when
// the type has no retention policy. A 0 result is dropped by the `ttl,omitempty` tag, so un-policed
// rows carry no ttl attribute and DynamoDB never expires them.
func (repo *Repository) ttlFor(entityType evt.EntityType) int64 {
	if repo == nil || len(repo.retention) == 0 {
		return 0
	}

	duration, ok := repo.retention[entityType]
	if !ok || duration <= 0 {
		return 0
	}

	return repo.nowOrDefault().Add(duration).Unix()
}

func (repo *Repository) nowOrDefault() time.Time {
	if repo != nil && repo.now != nil {
		return repo.now()
	}

	return time.Now()
}

func (repo *Repository) loggerOrDefault() *slog.Logger {
	if repo == nil || repo.logger == nil {
		return slog.Default()
	}

	return repo.logger
}

// comparableHandle reports whether a client handle can be compared with == without panicking; nil is
// comparable. Non-comparable handles (for example structs holding slices) fail the binding closed.
func comparableHandle(client Client) bool {
	return client == nil || reflect.ValueOf(client).Comparable()
}
