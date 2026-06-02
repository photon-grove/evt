package test

import "github.com/photon-grove/evt"

// FakeCommand is a dummy command that should not be recognized by the entity
type FakeCommand struct{}

// EntityType is a required method
func (f *FakeCommand) EntityType() evt.EntityType {
	return EntityType
}

// Type is a required method
func (f *FakeCommand) Type() evt.CommandType {
	return CreateCommand
}

// CreateCommand is the type for Create Commands for the Test Entity
const CreateCommand evt.CommandType = evt.CommandType(string(EntityType) + ":create")

// CreateEntity is the struct representing a Create Command
type CreateEntity struct {
	Value string
	Other *string
}

// EntityType returns the EntityType constant
func (c CreateEntity) EntityType() evt.EntityType {
	return EntityType
}

// Type returns the CreateCommand type constant
func (c CreateEntity) Type() evt.CommandType {
	return CreateCommand
}

// ReplaceCommand is the type for Update Commands for the Test Entity
const ReplaceCommand evt.CommandType = evt.CommandType(string(EntityType) + ":update")

// ReplaceEntity is the struct representing an Update Command
type ReplaceEntity struct {
	Value string
	Other *string
}

// EntityType returns the EntityType constant
func (c ReplaceEntity) EntityType() evt.EntityType {
	return EntityType
}

// Type returns the ReplaceCommand type constant
func (c ReplaceEntity) Type() evt.CommandType {
	return ReplaceCommand
}
