package snapshots

import (
	"github.com/photon-grove/evt"
	do "github.com/samber/do/v2"
)

// ProvideStore uses the Dependency Injection system to create a new Snapshot Store
func ProvideStore(i do.Injector) (evt.Store, error) {
	repo, err := do.Invoke[evt.Repository](i)
	if err != nil {
		return nil, err
	}

	store := NewStore(repo, 5)

	return store, nil
}
