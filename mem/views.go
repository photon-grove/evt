package mem

import (
	"bytes"
	"slices"
	"strings"

	"github.com/photon-grove/evt"
)

// viewKey identifies a view row.
type viewKey struct{ pk, sk string }

// View returns the row at pk and sk that commit-wide view operations wrote (ADR 0004, C7), or false
// when there is none. The returned row shares nothing with the repository.
func (repo Repository) View(pk, sk string) (evt.SerializedView, bool) {
	repo.features.mu.Lock()
	defer repo.features.mu.Unlock()

	view, ok := repo.features.views[viewKey{pk, sk}]
	view.Payload = bytes.Clone(view.Payload)

	return view, ok
}

// Views returns every row commit-wide view operations wrote, ordered by pk then sk bytes.
func (repo Repository) Views() []evt.SerializedView {
	repo.features.mu.Lock()
	defer repo.features.mu.Unlock()

	views := make([]evt.SerializedView, 0, len(repo.features.views))
	for _, view := range repo.features.views {
		view.Payload = bytes.Clone(view.Payload)
		views = append(views, view)
	}

	slices.SortFunc(views, func(a, b evt.SerializedView) int {
		if byPK := strings.Compare(a.PK, b.PK); byPK != 0 {
			return byPK
		}

		return strings.Compare(a.SK, b.SK)
	})

	return views
}

// checkViewConditions evaluates each operation's condition, in order, against the rows as they
// stand before the commit. The first that does not hold is the failure; the caller holds the lock.
func (repo Repository) checkViewConditions(views []evt.ProjectedView) error {
	for _, view := range views {
		_, exists := repo.features.views[viewKey{view.Op.PK, view.Op.SK}]

		switch {
		case view.Op.Condition == evt.ViewAbsent && exists, view.Op.Condition == evt.ViewPresent && !exists:
			return evt.NewProjectorOperationError(view, evt.ReasonConditionFailed, nil)
		}
	}

	return nil
}

// applyViews writes the rows of a commit whose conditions held; the caller holds the lock.
func (repo Repository) applyViews(views []evt.ProjectedView) {
	if len(views) == 0 {
		return
	}

	if repo.features.views == nil {
		repo.features.views = make(map[viewKey]evt.SerializedView)
	}

	for _, view := range views {
		key := viewKey{view.Op.PK, view.Op.SK}

		if view.Op.Kind == evt.ViewDelete {
			delete(repo.features.views, key)

			continue
		}

		repo.features.views[key] = evt.SerializedView{
			PK: view.Op.PK, SK: view.Op.SK, EntityID: view.Op.EntityID, EntityType: view.Op.EntityType,
			Payload: bytes.Clone(view.Op.Payload),
		}
	}
}
