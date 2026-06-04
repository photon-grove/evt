package dynamo

import (
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
)

// Repository wires DynamoDB access for the event store.
type Repository struct {
	EventsTable string

	client         Client
	encoder        *attributevalue.Encoder
	decoder        *attributevalue.Decoder
	consistentRead bool // default true for backward compatibility
	scanSegments   int  // parallel Scan segments for table-wide reads; <=1 means a single sequential scan
	logger         *slog.Logger
}

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

func (repo *Repository) loggerOrDefault() *slog.Logger {
	if repo == nil || repo.logger == nil {
		return slog.Default()
	}

	return repo.logger
}
