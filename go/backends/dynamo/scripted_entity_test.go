package dynamo_test

import (
	"context"
	"encoding/json"

	evt "github.com/photon-grove/evt/go"
)

// scriptedCommand deposits an amount on a scriptedEntity.
type scriptedCommand struct{ Amount int }

func (*scriptedCommand) Type() evt.CommandType      { return "scripted:deposit" }
func (*scriptedCommand) EntityType() evt.EntityType { return "scripted" }

type scriptedEvent struct{ Amount int }

func (*scriptedEvent) Type() evt.EventType        { return "scripted:deposited" }
func (*scriptedEvent) Version() evt.EventVersion  { return 1 }
func (*scriptedEvent) EntityType() evt.EntityType { return "scripted" }
func (*scriptedEvent) EntityID() evt.EntityID     { return "" }

// scriptedEntity counts how many times its handler ran.
type scriptedEntity struct {
	id      evt.EntityID
	Total   int  `json:"total"`
	handled *int // shared, so a restore of the entity cannot reset the count
}

func newScriptedEntity(id evt.EntityID) *scriptedEntity {
	return &scriptedEntity{id: id, handled: new(int)}
}

func (e *scriptedEntity) Type() evt.EntityType                { return "scripted" }
func (e *scriptedEntity) GetID() evt.EntityID                 { return e.id }
func (e *scriptedEntity) SetID(id evt.EntityID)               { e.id = id }
func (e *scriptedEntity) EventUpcasters() []evt.EventUpcaster { return nil }
func (e *scriptedEntity) Projectors() []evt.EventProjector    { return nil }
func (e *scriptedEntity) Handle(_ context.Context, command evt.Command) (evt.CommandResult, error) {
	*e.handled++

	c, ok := command.(*scriptedCommand)
	if !ok {
		return evt.CommandResult{}, evt.NewBadCommandError(command)
	}

	return evt.CommandResult{Events: []evt.Event{&scriptedEvent{Amount: c.Amount}}}, nil
}

func (e *scriptedEntity) Apply(event evt.Event) error {
	deposited, ok := event.(*scriptedEvent)
	if !ok {
		return evt.NewBadEventError(event)
	}

	e.Total += deposited.Amount

	return nil
}

func (e *scriptedEntity) DeserializeEvent(serialized evt.SerializedEvent) (evt.Event, error) {
	var event scriptedEvent
	if err := json.Unmarshal(serialized.Payload, &event); err != nil {
		return nil, err
	}

	return &event, nil
}
