package postgres

import (
	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/snapshots"
)

// DefaultSnapshotInterval is the number of events between durable snapshots for stores constructed
// with NewStore. It mirrors the in-memory and DynamoDB defaults.
const DefaultSnapshotInterval = 5

// NewStore constructs a snapshot-taking evt.Store over a fresh PostgreSQL Repository.
func NewStore(db DB, opts ...Option) evt.Store {
	return NewStoreFromRepo(NewRepository(db, opts...))
}

// NewStoreFromRepo wraps an existing Repository in a snapshot-taking evt.Store.
func NewStoreFromRepo(repo evt.Repository) evt.Store {
	return snapshots.NewStore(repo, DefaultSnapshotInterval)
}
