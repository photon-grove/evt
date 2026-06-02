package evt

import "context"

// DefaultViewSK is the default sort key for single-item views.
const DefaultViewSK = "VIEW"

// SerializedView is a simple representation of a materialized entity view stored in the backing store.
type SerializedView struct {
	PK         string     `json:"pk"`
	SK         string     `json:"sk"`
	EntityID   EntityID   `json:"entityID"`
	EntityType EntityType `json:"entityType"`
	Payload    []byte     `json:"payload"`
	TTL        int64      `json:"ttl,omitempty"`
}

// PagedResult wraps a page of views with an optional cursor for the next page.
type PagedResult struct {
	Views      []*SerializedView
	NextCursor string
}

// ViewRepository is a lightweight repository interface for reading/writing entity views.
type ViewRepository interface {
	// GetView retrieves a view by partition key (pk). Returns nil if not found.
	GetView(ctx context.Context, pk string) (*SerializedView, error)

	// PutView writes or replaces the given view.
	PutView(ctx context.Context, view *SerializedView) error

	// ListViewsByEntityType returns serialized views for the provided entity type.
	ListViewsByEntityType(ctx context.Context, entityType EntityType) ([]*SerializedView, error)

	// ListViewsByEntityTypePaged returns a page of views for the provided entity type.
	// limit controls page size; cursor is an opaque pagination token (empty for the first page).
	ListViewsByEntityTypePaged(ctx context.Context, entityType EntityType, limit int, cursor string) (*PagedResult, error)

	// ListViewsByPK returns all serialized views for the given partition key.
	// Used for composite keys where pk identifies a collection (e.g., USER#<id>#teams).
	ListViewsByPK(ctx context.Context, pk string) ([]*SerializedView, error)

	// ListViewsByPKPaged returns a page of views for the given partition key.
	// limit controls page size; cursor is an opaque pagination token (empty for the first page).
	ListViewsByPKPaged(ctx context.Context, pk string, limit int, cursor string) (*PagedResult, error)
}
