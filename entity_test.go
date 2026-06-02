package evt

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewEntity(t *testing.T) {
	entityID := EntityID("test-entity-id")
	before := time.Now()

	base := NewEntity(entityID)

	after := time.Now()

	assert.Equal(t, entityID, base.ID)
	assert.True(t, base.IsActive)

	// Check timestamps are recent
	assert.True(t, base.CreatedAt.After(before) || base.CreatedAt.Equal(before))
	assert.True(t, base.CreatedAt.Before(after) || base.CreatedAt.Equal(after))

	assert.True(t, base.UpdatedAt.After(before) || base.UpdatedAt.Equal(before))
	assert.True(t, base.UpdatedAt.Before(after) || base.UpdatedAt.Equal(after))
}

func TestEntityType_String(t *testing.T) {
	et := EntityType("User")
	assert.Equal(t, "User", string(et))
}

func TestEntityID_String(t *testing.T) {
	id := EntityID("user-123")
	assert.Equal(t, "user-123", string(id))
}
