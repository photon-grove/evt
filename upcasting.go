package evt

// An EventUpcaster migrates a serialized event from an older version to a newer
// version before aggregate replay.
type EventUpcaster interface {
	CanUpcast(EventType, EventVersion) bool
	Upcast(SerializedEvent) (SerializedEvent, error)
}

// UpcastError wraps a failure that occurred while upcasting a serialized event.
type UpcastError struct {
	Err             error           `json:"err"`
	SerializedEvent SerializedEvent `json:"serializedEvent"`
}

// NewUpcastError creates a new UpcastError instance
func NewUpcastError(err error, serializedEvent SerializedEvent) *UpcastError {
	return &UpcastError{err, serializedEvent}
}

// Error returns the wrapped error message.
func (e *UpcastError) Error() string {
	if e == nil || e.Err == nil {
		return "upcast error"
	}

	return e.Err.Error()
}
