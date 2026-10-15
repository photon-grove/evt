package evt

import (
	"context"
	"encoding/json"
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

	// v0.9 metadata (ADR 0004, C6), appended after the existing keys so stored events written
	// before v0.9 still decode: missing keys decode to their empty values and unknown keys are
	// ignored.
	PrincipalID *PrincipalID      `json:"principalId"`
	CausationID *CommandID        `json:"causationId"`
	Extensions  map[string]string `json:"extensions"`
}

// A PrincipalID identifies the caller that issued a command, for audit and per-principal checks.
type PrincipalID string

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

// WithPrincipalID adds the caller's identity to metadata. An empty principal is left unset.
func WithPrincipalID(principal string) MetadataOption {
	return func(m Metadata) Metadata {
		if principal == "" {
			m.PrincipalID = nil

			return m
		}

		id := PrincipalID(principal)
		m.PrincipalID = &id

		return m
	}
}

// WithCausationID adds the command that caused this command to metadata. When unset, the stored
// causationId defaults to the commandId at encode time. An empty causation is left unset.
func WithCausationID(causation CommandID) MetadataOption {
	return func(m Metadata) Metadata {
		if causation == "" {
			m.CausationID = nil

			return m
		}

		m.CausationID = &causation

		return m
	}
}

// WithExtension adds one application-specific extension to metadata.
func WithExtension(key, value string) MetadataOption {
	return func(m Metadata) Metadata {
		if m.Extensions == nil {
			m.Extensions = map[string]string{}
		}

		m.Extensions[key] = value

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

// MarshalJSON encodes metadata with the storage format's rules: every key is always written, and
// the v0.9 keys carry their effective values — causationId is the explicitly set causation,
// otherwise the commandId, otherwise null, and extensions is an object, {} when unset or empty.
// Decoding stays faithful to the stored bytes and does not apply the defaults.
func (m Metadata) MarshalJSON() ([]byte, error) {
	type metadataWire Metadata

	wire := metadataWire(m)
	if wire.CausationID == nil && wire.CommandID != nil {
		causation := *wire.CommandID
		wire.CausationID = &causation
	}

	if wire.Extensions == nil {
		wire.Extensions = map[string]string{}
	}

	return json.Marshal(wire)
}
