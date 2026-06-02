package evt_test

import (
	"errors"
	"testing"

	"github.com/photon-grove/evt"
	evttest "github.com/photon-grove/evt/test"
)

func TestDeserializeEvent_UnknownTypeFailsClosed(t *testing.T) {
	entity := evttest.NewEntity("test-1")

	serialized := evt.SerializedEvent{
		Type:    "test:never_existed",
		Version: 1,
		Payload: []byte(`{}`),
	}

	_, err := evt.DeserializeEvent(serialized, entity)
	if err == nil {
		t.Fatal("expected unknown event to fail closed")
	}

	if !evt.IsReplayStrictnessErr(err) {
		t.Fatalf("expected ReplayStrictnessError, got %T: %v", err, err)
	}

	var replayErr *evt.ReplayStrictnessError
	if !errors.As(err, &replayErr) {
		t.Fatalf("expected ReplayStrictnessError, got %T", err)
	}
	if replayErr.Phase != "deserialize" {
		t.Fatalf("expected phase=deserialize, got %q", replayErr.Phase)
	}
	if replayErr.EventType != "test:never_existed" {
		t.Fatalf("expected EventType=test:never_existed, got %q", replayErr.EventType)
	}
}

func TestDeserializeEvent_KnownTypeSucceeds(t *testing.T) {
	entity := evttest.NewEntity("test-1")

	serialized := evt.SerializedEvent{
		Type:    evttest.CreatedEvent,
		Version: 1,
		Payload: []byte(`{"id":"test-1","value":"hello"}`),
	}

	event, err := evt.DeserializeEvent(serialized, entity)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if event == nil {
		t.Fatal("expected event, got nil")
	}
}

func TestApply_UnknownEventReturnsBadEventError(t *testing.T) {
	entity := evttest.NewEntity("test-1")

	err := entity.Apply(&evttest.FakeEvent{})
	if err == nil {
		t.Fatal("expected Apply to fail closed on unknown event")
	}

	var badEvent *evt.BadEventError
	if !errors.As(err, &badEvent) {
		t.Fatalf("expected BadEventError, got %T: %v", err, err)
	}
}
