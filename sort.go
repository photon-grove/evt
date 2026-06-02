package evt

// ByCommandID allows you to sort SerializedEvents by the CommandID in the Metadata, falling back to
// sorting by EntityID if the CommandID is empty.
type ByCommandID []SerializedEvent

// Len returns the length of the slice
func (events ByCommandID) Len() int {
	return len(events)
}

// Swap swaps the elements with indexes i and j
func (events ByCommandID) Swap(i, j int) {
	events[i], events[j] = events[j], events[i]
}

// Less reports whether the element with index i should sort before the element with index j
func (events ByCommandID) Less(i, j int) bool {
	// Try to see if both have CommandIDs
	if cmd := events[i].Metadata.CommandID; cmd != nil {
		if otherCmd := events[j].Metadata.CommandID; otherCmd != nil {
			return *cmd < *otherCmd
		}
	}

	// Fall back to the EntityIDs
	return events[i].ID < events[j].ID
}
