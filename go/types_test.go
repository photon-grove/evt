package evt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEntityType_String(t *testing.T) {
	et := EntityType("User")
	assert.Equal(t, "User", string(et))
}

func TestEntityID_String(t *testing.T) {
	id := EntityID("user-123")
	assert.Equal(t, "user-123", string(id))
}
