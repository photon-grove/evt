package mem

import "github.com/photon-grove/evt"

// AppendWithoutHead stores an event the way a writer that does not maintain stream heads would,
// leaving the stream's head behind its log. It exists to test drift detection.
func AppendWithoutHead(repo *Repository, event evt.SerializedEvent) {
	repo.features.mu.Lock()
	defer repo.features.mu.Unlock()

	id := string(event.EntityID)
	repo.events[id] = append(repo.events[id], event)
}
