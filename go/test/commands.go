package test

import evt "github.com/photon-grove/evt/go"

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

// CreateCommand is the type for Create Commands for the Test Aggregate
const CreateCommand evt.CommandType = evt.CommandType(string(EntityType) + ":create")

// CreateAggregate is the struct representing a Create Command
type CreateAggregate struct {
	Value string
	Other *string
}

// EntityType returns the EntityType constant
func (c CreateAggregate) EntityType() evt.EntityType {
	return EntityType
}

// Type returns the CreateCommand type constant
func (c CreateAggregate) Type() evt.CommandType {
	return CreateCommand
}

// ReplaceCommand is the type for Update Commands for the Test Aggregate
const ReplaceCommand evt.CommandType = evt.CommandType(string(EntityType) + ":update")

// ReplaceAggregate is the struct representing an Update Command
type ReplaceAggregate struct {
	Value string
	Other *string
}

// EntityType returns the EntityType constant
func (c ReplaceAggregate) EntityType() evt.EntityType {
	return EntityType
}

// Type returns the ReplaceCommand type constant
func (c ReplaceAggregate) Type() evt.CommandType {
	return ReplaceCommand
}
