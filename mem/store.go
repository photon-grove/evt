package mem

import (
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/snapshots"
)

// NewStore constructs a SnapshotStore using the in-memory repo
func NewStore() evt.Store {
	repo := NewRepository()
	return snapshots.NewStore(repo, 5)
}

// NewStoreFromRepo constructs a SnapshotStore wrapping the given Repository.
func NewStoreFromRepo(repo evt.Repository) evt.Store {
	return snapshots.NewStore(repo, 5)
}
