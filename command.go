package evt

// CommandType is a unique identifier for the type of a Command
type CommandType string

// CommandID is a unique identifier for a Command instance
type CommandID string

// Command is a base type that should be implemented by all Command types
type Command interface {
	Type() CommandType
	EntityType() EntityType
}

// A CommandResult is the outcome of executing a Command, and can optionally contain a Transaction
type CommandResult struct {
	Events      []Event     `json:"events"`
	Transaction Transaction `json:"transaction"`
}
