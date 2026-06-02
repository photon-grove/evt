package mem

import (
	"github.com/photon-grove/evt"
	do "github.com/samber/do/v2"
)

// ProvideStore uses the Dependency Injection system to create a new in-memory EventStore
func ProvideStore(_ *do.Injector) (evt.Store, error) {
	store := NewStore()

	return store, nil
}

// ProvideRepository uses the Dependency Injection system to create a new in-memory EventRepository
func ProvideRepository(_ *do.Injector) (evt.Repository, error) {
	repo := NewRepository()

	return repo, nil
}
