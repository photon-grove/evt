package policy

import (
	"github.com/photon-grove/evt"
)

// FenceConflict returns the error for a failed read fence (ADR 0004, C2). A conflict on an
// engine-filled fence is classified transient — the engine can reload and re-decide, so
// a retry treats it exactly like an event-put conflict; a conflict on a caller-stamped
// fence is a stale-fence rejection and is not retryable. Both keep matching
// *evt.FenceConflictError through Unwrap, so callers can inspect the fenced stream, its head,
// and its provenance. The internal transaction item index a backend classifies the failure by
// stays internal.
func FenceConflict(fence evt.ReadFence) error {
	conflict := &evt.FenceConflictError{Fence: fence}
	if fence.CallerStamped {
		return conflict
	}

	return &ClassifiedError{Class: ClassTransient, Err: conflict}
}
