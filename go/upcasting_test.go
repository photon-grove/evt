package evt

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpcastError(t *testing.T) {
	origErr := errors.New("original error")
	serializedEvent := SerializedEvent{
		Type:    "TestEvent",
		Version: 1,
	}

	err := NewUpcastError(origErr, serializedEvent)

	assert.Equal(t, "original error", err.Error())
	assert.Equal(t, origErr, err.Err)
	assert.Equal(t, serializedEvent, err.SerializedEvent)
}

func TestUpcastError_NilError(t *testing.T) {
	serializedEvent := SerializedEvent{}
	err := NewUpcastError(nil, serializedEvent)

	assert.Nil(t, err.Err)
	assert.Equal(t, "upcast error", err.Error())
}

// Note: Upcaster logic integration is tested in serialize_test.go via DeserializeEvent
