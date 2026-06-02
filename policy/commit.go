package policy

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/photon-grove/evt"
)

const (
	// DefaultMaxAttempts is the default number of retry attempts for commit paths.
	DefaultMaxAttempts = 5

	// DefaultBaseDelay is the default base delay between retries.
	DefaultBaseDelay = 100 * time.Millisecond

	// DefaultMaxDelay is the default maximum delay between retries.
	DefaultMaxDelay = 2 * time.Second
)

var (
	commitRandMu sync.Mutex
	commitRand   = rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec // non-crypto jitter for retry backoff
)

// DefaultConfig returns a Config with sensible defaults for
// optimistic-concurrency retry on evt Store commit paths.
func DefaultConfig() Config {
	return Config{
		MaxAttempts: DefaultMaxAttempts,
		BaseDelay:   DefaultBaseDelay,
		MaxDelay:    DefaultMaxDelay,
		Jitter: func(jitterMax time.Duration) time.Duration {
			if jitterMax <= 0 {
				return 0
			}
			commitRandMu.Lock()
			defer commitRandMu.Unlock()
			return time.Duration(commitRand.Int63n(int64(jitterMax) + 1)) //nolint:gosec // non-crypto jitter
		},
		Sleep: SleepWithContext,
	}
}

// ExecuteWithRetry runs Store.Execute inside a bounded retry loop. On transient
// commit failures the entity is reloaded from scratch via the factory before the
// next attempt. Errors returned on give-up are wrapped in ClassifiedError.
func ExecuteWithRetry(
	ctx context.Context,
	store evt.Store,
	factory evt.EntityFactory,
	entityID evt.EntityID,
	command evt.Command,
	metadata evt.Metadata,
	cfg Config,
	hooks Hooks,
) (evt.Entity, error) {
	var entity evt.Entity

	err := Execute(ctx, cfg, func() error {
		e, execErr := evt.ExecuteWithFactory(ctx, store, factory, entityID, command, metadata)
		if execErr == nil {
			entity = e
		}
		return execErr
	}, hooks)

	return entity, err
}
