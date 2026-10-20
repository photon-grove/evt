package compat

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/photon-grove/evt"
)

// RebuildViewPK is the view partition a scenario's guarded rebuild writes go to, and
// RebuildViewType the entity type of the rows (storage spec, Compat Scenarios).
func RebuildViewPK(base evt.EntityID) string { return "rebuild-" + string(base) }

// RebuildViewType is the entity type of the view rows a rebuild step writes.
const RebuildViewType evt.EntityType = "rebuild"

// scopedOwner scopes an owner token to the run and the scenario, like a command ID: the lock is
// global to the table, so a bare token could collide across runs.
func (r Runner) scopedOwner(entityID evt.EntityID, owner string) string {
	return string(entityID) + "-" + owner
}

// rebuild runs one lifecycle action of the rebuild op (C10) and reports what it observed: the
// result, the generation delta across the step, the force-release flag after it, and (writeViews)
// the rows the scenario's partition holds.
func (r Runner) rebuild(ctx context.Context, entityID evt.EntityID, step Step) (Outcome, error) {
	driver := r.Rebuild
	if driver == nil {
		return Outcome{}, errors.New("rebuild step on a backend with no rebuild driver")
	}

	before, err := driver.Fence.Generation(ctx)
	if err != nil {
		return Outcome{}, err
	}

	owner := r.scopedOwner(entityID, step.Owner)
	lease := time.Duration(step.LeaseSeconds) * time.Second

	var (
		result string
		call   error
		rows   int
	)

	switch step.Action {
	case "begin":
		_, call = driver.Fence.Begin(ctx, owner, lease)
	case "renew":
		_, call = driver.Fence.Renew(ctx, owner, lease)
	case "end":
		_, call = driver.Fence.End(ctx, owner)
	case "forceRelease":
		_, call = driver.Fence.ForceRelease(ctx, owner)
	case "generation":
		_, call = evt.ObserveRebuildEpoch(ctx, driver.Fence)
	case "wait":
		call = driver.Wait(ctx, step.Seconds)
	case "writeViews":
		call = driver.Fence.WriteViews(ctx, owner, []evt.ProjectedView{{
			Projector: "rebuild",
			Op: evt.PutView(RebuildViewPK(entityID), owner, evt.EntityID(owner), RebuildViewType,
				[]byte(fmt.Sprintf(`{"owner":%q}`, owner))),
		}})
	default:
		return Outcome{}, fmt.Errorf("unknown rebuild action %q", step.Action)
	}

	switch {
	case call == nil && step.Action == "generation":
		result = ResultSettled
	case call == nil:
		result = ResultOK
	case evt.IsRebuildLockHeldErr(call):
		result = ResultHeld
	case evt.IsRebuildLeaseLostErr(call):
		result = ResultLeaseLost
	case evt.IsRebuildUnavailableErr(call):
		result = ResultUnavailable
	default:
		return Outcome{}, fmt.Errorf("rebuild %s: %w", step.Action, call)
	}

	after, err := driver.Fence.Generation(ctx)
	if err != nil {
		return Outcome{}, err
	}

	if step.Action == "writeViews" {
		if rows, err = driver.Rows(ctx, RebuildViewPK(entityID)); err != nil {
			return Outcome{}, err
		}
	}

	return Outcome{
		Result: result, Delta: after.Value - before.Value, ForceReleased: after.ForceReleased, Rows: rows,
	}, nil
}
