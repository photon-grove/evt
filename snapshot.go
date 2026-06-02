package evt

// A SerializedSnapshot represents a snapshot of current Entity state that is ready
// to be committed to an Repository.
type SerializedSnapshot struct {
	EntityType    EntityType    `json:"entityType"`
	EntityID      EntityID      `json:"entityID"`
	Sequence      EventSequence `json:"sequence"`
	EventSequence EventSequence `json:"eventSequence"`
	Payload       []byte        `json:"payload"`
}
