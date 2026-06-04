package dynamo

import (
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"

	"github.com/photon-grove/evt"
)

// Repository wires DynamoDB access for the event store.
type Repository struct {
	EventsTable string

	client         Client
	encoder        *attributevalue.Encoder
	decoder        *attributevalue.Decoder
	consistentRead bool // default true for backward compatibility
	scanSegments   int  // parallel Scan segments for table-wide reads; <=1 means a single sequential scan
	retention      Retention
	now            func() time.Time // clock for TTL expiry; nil means time.Now
	logger         *slog.Logger
}

// Retention maps entity types to how long their committed events (and inline snapshots) are kept
// before DynamoDB TTL expires them. Only entity types present in this map are ever stamped with a
// `ttl` attribute; every other row is written without one and is never auto-expired.
//
// Use this ONLY for transient/process entity types whose events no projection rebuild depends on.
// Never policy a type whose events a view is reconstructed from by replay — DynamoDB would silently
// delete history the rebuild needs. The events table must have TTL enabled on the `ttl` attribute.
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

	return &Repository{EventsTable: eventsTable, client: client, encoder: encoder, decoder: decoder, consistentRead: true}
}

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
