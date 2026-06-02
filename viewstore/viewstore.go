// Package viewstore provides a typed wrapper around evt.ViewRepository that
// binds a JSON codec, an evt.EntityType, and an optional value factory so
// individual stores only need to express their domain key conventions
// (pk/sk builders) rather than re-implementing JSON encode/decode plumbing.
//
// Two shapes are supported:
//
//   - Codec[T]: low-level binding that accepts caller-supplied pk/sk/entityID
//     on every call. Use this when key builders take more than one argument
//     (e.g., compound keys derived from world+location+object).
//
//   - Single[K, T]: a Codec[T] with a bound pk function pkFor(K) and an
//     optional entity-ID extractor entityIDOf(T). Use this for the common
//     "one PK per entity ID" view-store shape.
//
// All List methods skip payloads that fail to decode and forward the error
// to an optional onDecodeError callback. Callers needing strict behavior
// can fall back to evt.ListJSONViewsByPK / evt.ListJSONViewsByEntityType
// directly.
package viewstore

import (
	"context"

	"github.com/photon-grove/evt"
)

// DecodeErrorHandler is invoked for each view skipped during a List call.
// view may be nil if the view itself was nil; err is the underlying decode
// error (typically *evt.JSONViewDecodeError).
type DecodeErrorHandler func(view *evt.SerializedView, err error)

// Codec binds an evt.ViewRepository to a JSON-encoded value type T plus an
// EntityType. Callers supply pk/sk/entityID per call.
type Codec[T any] struct {
	repo       evt.ViewRepository
	entityType evt.EntityType
	factory    func() T
}

// New constructs a Codec for value-type T. Use NewWithFactory when T is a
// pointer-backed entity that needs constructor-injected dependencies.
func New[T any](repo evt.ViewRepository, entityType evt.EntityType) *Codec[T] {
	return &Codec[T]{repo: repo, entityType: entityType}
}

// NewWithFactory constructs a Codec that uses factory to produce decode
// destinations. This is required when T is a pointer to an aggregate that
// needs projector wiring before json.Unmarshal runs.
func NewWithFactory[T any](repo evt.ViewRepository, entityType evt.EntityType, factory func() T) *Codec[T] {
	return &Codec[T]{repo: repo, entityType: entityType, factory: factory}
}

// Repo returns the underlying view repository for callers that need raw
// access (e.g., existence probes that should not pay decode cost).
func (c *Codec[T]) Repo() evt.ViewRepository { return c.repo }

// EntityType returns the entity type bound to this codec.
func (c *Codec[T]) EntityType() evt.EntityType { return c.entityType }

// Get retrieves and decodes the view at pk. Returns (zero, false, nil) when
// no view exists.
func (c *Codec[T]) Get(ctx context.Context, pk string) (T, bool, error) {
	return evt.GetJSONView[T](ctx, c.repo, pk, c.factory)
}

// Put writes value at the given pk/sk with the supplied entity ID under the
// bound EntityType.
func (c *Codec[T]) Put(ctx context.Context, pk, sk string, entityID evt.EntityID, value T) error {
	return evt.PutJSONView(ctx, c.repo, &evt.SerializedView{
		PK:         pk,
		SK:         sk,
		EntityID:   entityID,
		EntityType: c.entityType,
	}, value)
}

// PutAt writes value at the given pk with an empty sort key.
func (c *Codec[T]) PutAt(ctx context.Context, pk string, entityID evt.EntityID, value T) error {
	return c.Put(ctx, pk, "", entityID, value)
}

// ListByPK returns valid views matching pk, skipping payloads that fail to
// decode. onDecodeError, if non-nil, is invoked for each skipped payload.
func (c *Codec[T]) ListByPK(ctx context.Context, pk string, onDecodeError DecodeErrorHandler) ([]T, error) {
	return evt.ListValidJSONViewsByPK[T](ctx, c.repo, pk, c.factory, onDecodeErrorAdapter(onDecodeError))
}

// ListAll returns all valid views for the bound entity type, skipping
// payloads that fail to decode. onDecodeError, if non-nil, is invoked for
// each skipped payload.
func (c *Codec[T]) ListAll(ctx context.Context, onDecodeError DecodeErrorHandler) ([]T, error) {
	return evt.ListValidJSONViewsByEntityType[T](ctx, c.repo, c.entityType, c.factory, onDecodeErrorAdapter(onDecodeError))
}

// Single is a Codec[T] augmented with caller-supplied pkFor / entityIDOf
// functions, enabling concise Get(id) / Put(value) helpers for the common
// "one PK per entity ID" view shape.
type Single[K any, T any] struct {
	codec      *Codec[T]
	pkFor      func(K) string
	entityIDOf func(T) evt.EntityID
}

// NewSingle constructs a Single store. pkFor must be non-nil; entityIDOf is
// optional and only required by the value-driven Put method.
func NewSingle[K any, T any](
	repo evt.ViewRepository,
	entityType evt.EntityType,
	pkFor func(K) string,
	entityIDOf func(T) evt.EntityID,
) *Single[K, T] {
	return &Single[K, T]{
		codec:      New[T](repo, entityType),
		pkFor:      pkFor,
		entityIDOf: entityIDOf,
	}
}

// NewSingleWithFactory is NewSingle plus a factory for pointer-backed T.
func NewSingleWithFactory[K any, T any](
	repo evt.ViewRepository,
	entityType evt.EntityType,
	pkFor func(K) string,
	entityIDOf func(T) evt.EntityID,
	factory func() T,
) *Single[K, T] {
	return &Single[K, T]{
		codec:      NewWithFactory[T](repo, entityType, factory),
		pkFor:      pkFor,
		entityIDOf: entityIDOf,
	}
}

// Codec exposes the underlying Codec for callers that need to issue lookups
// outside the bound pkFor (e.g., legacy-key fallback reads).
func (s *Single[K, T]) Codec() *Codec[T] { return s.codec }

// Get retrieves the view keyed by id.
func (s *Single[K, T]) Get(ctx context.Context, id K) (T, bool, error) {
	return s.codec.Get(ctx, s.pkFor(id))
}

// Put writes value, deriving pk from entityIDOf(value) (which must be set).
// Use PutAt when the pk derives from inputs other than the value.
func (s *Single[K, T]) Put(ctx context.Context, id K, value T) error {
	var entityID evt.EntityID
	if s.entityIDOf != nil {
		entityID = s.entityIDOf(value)
	}
	return s.codec.PutAt(ctx, s.pkFor(id), entityID, value)
}

// ListAll returns all valid views for the bound entity type.
func (s *Single[K, T]) ListAll(ctx context.Context, onDecodeError DecodeErrorHandler) ([]T, error) {
	return s.codec.ListAll(ctx, onDecodeError)
}

func onDecodeErrorAdapter(h DecodeErrorHandler) func(*evt.SerializedView, error) {
	if h == nil {
		return nil
	}
	return func(v *evt.SerializedView, err error) { h(v, err) }
}
