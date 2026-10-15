package mem

import (
	"github.com/photon-grove/evt"
	do "github.com/samber/do/v2"
)

// ProvideStore uses the Dependency Injection system to create a new in-memory EventStore
//
// Deprecated: Cut in ADR-0006 (K7): the do.go DI providers go away; construct mem.NewStore
// directly. It will be removed in the next minor release.
func ProvideStore(_ *do.Injector) (evt.Store, error) {
	store := NewStore()

	return store, nil
}

// ProvideRepository uses the Dependency Injection system to create a new in-memory EventRepository
//
// Deprecated: Cut in ADR-0006 (K7): the do.go DI providers go away; construct mem.NewRepository
// directly. It will be removed in the next minor release.
func ProvideRepository(_ *do.Injector) (evt.Repository, error) {
	repo := NewRepository()

	return repo, nil
}
