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

	// v0.9 metadata (ADR 0004, C6), appended after the existing keys so stored events written
	// before v0.9 still decode: missing keys decode to their empty values and unknown keys are
	// ignored. Because of the appended keys (and the Extensions map), Metadata is no longer
	// comparable with ==; use Equal instead.
	PrincipalID *PrincipalID `json:"principalId"`
	CausationID *CommandID   `json:"causationId"`

	// Extensions carries application-specific metadata. The framework's storage and stream codecs
	// encode unset or empty extensions as {}. A stored object decodes to a non-nil map, while a
	// pre-v0.9 row without the key decodes to nil. Equal treats the two as the same.
	Extensions map[string]string `json:"extensions"`

	// CommitID identifies the commit that wrote the event. The engine sets it once per commit, so
	// every event of a commit — across every stream a multi-stream commit touches — carries the
	// same value (ADR 0004, C1). The default is the command ID when present, otherwise a generated
	// ULID. Pre-v0.9 rows without the key decode to the empty string.
	CommitID string `json:"commitId"`
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
func WithPrincipalID(principal PrincipalID) MetadataOption {
	return func(m Metadata) Metadata {
		if principal == "" {
			m.PrincipalID = nil

			return m
		}

		id := principal
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

// Equal reports whether two metadata values carry the same effective data. An unset CausationID
// defaults to CommandID, and nil and empty Extensions maps compare equal, matching their encoding.
func (m Metadata) Equal(other Metadata) bool {
	return equalPtr(m.CommandID, other.CommandID) &&
		carrierEqual(m.Trace, other.Trace) &&
		equalPtr(m.Origin, other.Origin) &&
		equalPtr(m.Address, other.Address) &&
		m.Region == other.Region &&
		m.Timestamp == other.Timestamp &&
		equalPtr(m.PrincipalID, other.PrincipalID) &&
		equalPtr(m.effectiveCausationID(), other.effectiveCausationID()) &&
		extensionsEqual(m.Extensions, other.Extensions) &&
		m.CommitID == other.CommitID
}

func (m Metadata) effectiveCausationID() *CommandID {
	if m.CausationID != nil {
		return m.CausationID
	}

	return m.CommandID
}

// carrierEqual compares two optional trace carriers by content, with nil only equal to nil.
func carrierEqual(a, b *propagation.MapCarrier) bool {
	if a == nil || b == nil {
		return a == b
	}

	return extensionsEqual(*a, *b)
}

// equalPtr compares two optional values by their pointees, with nil only equal to nil.
func equalPtr[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}

	return *a == *b
}

// extensionsEqual compares two extension maps, treating nil and empty as equal.
func extensionsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}

	for key, value := range a {
		if other, ok := b[key]; !ok || other != value {
			return false
		}
	}

	return true
}
