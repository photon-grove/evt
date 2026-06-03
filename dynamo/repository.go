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

func (repo *Repository) loggerOrDefault() *slog.Logger {
	if repo == nil || repo.logger == nil {
		return slog.Default()
	}

	return repo.logger
}
