package policy

import (
	"math/rand"
	"sync"
	"time"
)

var (
	commitRandMu sync.Mutex
	commitRand   = rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec // non-crypto jitter for retry backoff
)

// DefaultJitter returns a random duration in [0, jitterMax] for retry backoff. It is safe for
// concurrent use and is the jitter source snapshots.DefaultRetryPolicy uses.
func DefaultJitter(jitterMax time.Duration) time.Duration {
	if jitterMax <= 0 {
		return 0
	}

	commitRandMu.Lock()
	defer commitRandMu.Unlock()

	return time.Duration(commitRand.Int63n(int64(jitterMax) + 1)) //nolint:gosec // non-crypto jitter
}
