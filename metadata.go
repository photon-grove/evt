package evt

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Metadata is a set of additional data carried along with an Event. Any changes should be backwards
// compatible, and bad data should always be tested.
type Metadata struct {
	CommandID *CommandID              `json:"commandId"`
	Trace     *propagation.MapCarrier `json:"trace"`
	Origin    *Origin                 `json:"origin"`
	Address   *string                 `json:"address"`
	Region    string                  `json:"region"`
	Timestamp string                  `json:"timestamp"`
}

// A Source is the system that initiated the request
type Source string

// An Endpoint is the entrypoint the request came in through
type Endpoint string

// An Origin is the combination of the source and the endpoint
type Origin struct {
	Source   Source   `json:"source"`
	Endpoint Endpoint `json:"endpoint"`
}

// MetadataOption is used to add properties to metadata
type MetadataOption func(Metadata) Metadata

// WithCommandID adds the provided commandId to metadata
func WithCommandID(cmdid CommandID) MetadataOption {
	return func(m Metadata) Metadata {
		m.CommandID = &cmdid

		return m
	}
}

// WithTrace adds trace context to metadata
func WithTrace(ctx context.Context) MetadataOption {
	return func(m Metadata) Metadata {
		carrier := propagation.MapCarrier{}
		otel.GetTextMapPropagator().Inject(ctx, carrier)
		m.Trace = &carrier

		return m
	}
}

// WithOrigin adds the Origin to the request Metadata
func WithOrigin(origin Origin) MetadataOption {
	return func(m Metadata) Metadata {
		m.Origin = &origin

		return m
	}
}

// WithAddress adds the caller-provided address to metadata.
func WithAddress(address string) MetadataOption {
	return func(m Metadata) Metadata {
		if address == "" {
			m.Address = nil

			return m
		}

		m.Address = &address

		return m
	}
}

// NewMetadata initializes a Metadata k-v store
func NewMetadata(_ context.Context, region *string, opts ...MetadataOption) Metadata {
	m := Metadata{}

	// Add the Region, which should always be in the Metadata
	if region != nil {
		m.Region = *region
	}

	// Add a Timestamp in UTC using the RFC3339 format
	m.Timestamp = time.Now().In(time.UTC).Format(time.RFC3339)

	for _, opt := range opts {
		m = opt(m)
	}

	return m
}
