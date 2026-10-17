package snapshots

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
)

// Defaults of the fence-aware retry policy (ADR 0004, C5; storage spec, Fence-Aware Retry).
const (
	// DefaultRetryAttempts is the default number of attempts in total, the first one included.
	DefaultRetryAttempts = 5

	// DefaultRetryBaseDelay is the default delay before the first retry, before jitter.
	DefaultRetryBaseDelay = 25 * time.Millisecond

	// DefaultRetryMaxDelay is the default upper bound for any delay.
	DefaultRetryMaxDelay = time.Second
)

// RetryPolicy bounds the fence-aware retry a store with outcomes runs when a command's commit
// conflicts with another writer (ADR 0004, C5). A command is attempted at most MaxAttempts times;
// each retry re-prepares it — the entity is loaded again and the handler runs again against the
// fresh state — after an exponentially growing delay. When the last attempt also conflicts, the
// engine records a stale-fence rejection as the command's outcome (see
// evt.RetriesExhaustedRejection) instead of returning the conflict.
//
// Only a failure that means another writer moved state the attempt read or wrote is re-prepared.
// A transient backend failure (throttling, an ambiguous write) is retried the same way but, when
// attempts run out, is returned as an error: nothing says the state went stale. Every other error
// is returned at once.
type RetryPolicy struct {
	// MaxAttempts is the number of attempts in total, including the first. A value below 1 means 1:
	// the first conflict is recorded as a stale-fence rejection without a retry.
	MaxAttempts int

	// BaseDelay is the delay before the first retry, before jitter. Delays double from it up to
	// MaxDelay, and jitter keeps them in [delay/2, delay].
	BaseDelay time.Duration

	// MaxDelay is the upper bound for any delay. A BaseDelay or MaxDelay of zero or less retries
	// without waiting, as in Rust.
	MaxDelay time.Duration

	// Jitter returns a value in [0, limit] used to spread a delay across [delay/2, delay]. Nil
	// uses delay/2, the bottom of the range, which keeps delays deterministic.
	Jitter func(limit time.Duration) time.Duration

	// Sleep waits for the delay between attempts and returns early with the context's error when
	// it ends. Nil uses policy.SleepWithContext; tests inject a recorder.
	Sleep func(ctx context.Context, delay time.Duration) error
}

// DefaultRetryPolicy returns the default policy: DefaultRetryAttempts attempts, delays from
// DefaultRetryBaseDelay doubling up to DefaultRetryMaxDelay, with jitter.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: DefaultRetryAttempts,
		BaseDelay:   DefaultRetryBaseDelay,
		MaxDelay:    DefaultRetryMaxDelay,
		Jitter:      policy.DefaultConfig().Jitter,
	}
}

// WithRetry replaces the store's fence-aware retry policy, DefaultRetryPolicy by default. It
// applies to ExecuteWithOutcome, Execute on a store with outcomes, and ExecuteTransaction. Like
// the other With options it must be called during Store initialization, before concurrent use.
func (store *Store) WithRetry(retry RetryPolicy) *Store {
	store.retry = retry

	return store
}

func (p RetryPolicy) attempts() int { return max(p.MaxAttempts, 1) }

// wait sleeps before retry number retry (1 for the first retry).
func (p RetryPolicy) wait(ctx context.Context, retry int) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if p.BaseDelay <= 0 || p.MaxDelay <= 0 {
		return nil
	}

	sleep := p.Sleep
	if sleep == nil {
		sleep = policy.SleepWithContext
	}

	return sleep(ctx, policy.ExponentialJitterDelay(p.BaseDelay, p.MaxDelay, retry-1, p.Jitter))
}

// retryKind says what a failed attempt means for the retry loop.
type retryKind int

const (
	// retryNever: return the error.
	retryNever retryKind = iota

	// retryTransient: run again after a delay, but return the error when attempts run out.
	retryTransient

	// retryConflict: another writer moved state the attempt used. Run again after a delay, and
	// record a stale-fence rejection when attempts run out.
	retryConflict
)

// internalFailure marks an error the retry loop must return as is, whatever it looks like: a
// handler's own error, a projector's, an event the entity cannot apply. A domain ConflictError a
// handler returns is not an optimistic-concurrency conflict.
type internalFailure struct{ err error }

func (e *internalFailure) Error() string { return e.err.Error() }
func (e *internalFailure) Unwrap() error { return e.err }

// internal marks err as an internal failure, or returns nil for nil.
func internal(err error) error {
	if err == nil {
		return nil
	}

	return &internalFailure{err: err}
}

// loadFailure marks an error from loading the state a command is decided on. A load that raced a
// compaction finds a sequence gap, which reloading resolves, so it is retried; but it says the
// read was inconsistent (or, when it persists, that events expired), not that a decision went
// stale, so it never exhausts into a stale-fence rejection.
type loadFailure struct{ err error }

func (e *loadFailure) Error() string { return e.err.Error() }
func (e *loadFailure) Unwrap() error { return e.err }

// load marks err as a load failure, or returns nil for nil.
func load(err error) error {
	if err == nil {
		return nil
	}

	return &loadFailure{err: err}
}

// classifyAttempt decides whether a failed attempt is worth another.
func classifyAttempt(err error) retryKind {
	var (
		marked *internalFailure
		loaded *loadFailure
	)

	switch {
	case err == nil, errors.As(err, &marked):
		return retryNever

	case errors.As(err, &loaded):
		// Retried like a transient failure, whatever the underlying error looks like.
		if evt.IsOptimisticConflict(loaded.err) || policy.IsTransient(loaded.err) {
			return retryTransient
		}

		return retryNever

	case errors.Is(err, evt.ErrOutcomeConflict), errors.Is(err, evt.ErrIdempotencyKeyReuse), errors.Is(err, evt.ErrCorruptOutcome):
		// Resolved by the outcome row, never by running the command again.
		return retryNever

	case failedCallerFence(err):
		// A caller-stamped fence cannot be re-derived: the engine records a stale-fence rejection
		// instead (Transaction.Complete), and a raw one is never retried.
		return retryNever

	case evt.IsOptimisticConflict(err):
		return retryConflict

	case policy.IsTransient(err):
		return retryTransient
	}

	return retryNever
}

func failedCallerFence(err error) bool {
	var fence *evt.FenceConflictError

	return errors.As(err, &fence) && fence.Fence.CallerStamped
}

// attemptLoop runs the attempts of one command.
type attemptLoop struct {
	retry RetryPolicy

	// once is set when the command cannot be re-prepared, so a failed attempt is final and its
	// error is returned unchanged.
	once bool
}

// run calls attempt until it succeeds, fails for good, or the policy runs out of attempts. attempt
// receives the number of the attempt, starting at 1, and must prepare the command from scratch.
// exhausted records the stale-fence rejection when the last attempt conflicted; it receives the
// number of attempts made.
func (l attemptLoop) run(
	ctx context.Context,
	attempt func(n int) (executed, error),
	exhausted func(ctx context.Context, attempts int) (executed, error),
) (executed, error) {
	limit := l.retry.attempts()
	if l.once {
		limit = 1
	}

	for n := 1; ; n++ {
		result, err := attempt(n)

		var marked *internalFailure
		if errors.As(err, &marked) {
			return result, marked.err
		}

		kind := classifyAttempt(err)
		if kind == retryNever || l.once {
			return result, unmarkLoad(err)
		}

		if n >= limit {
			if kind == retryConflict {
				return exhausted(ctx, n)
			}

			return result, unmarkLoad(err)
		}

		if werr := l.retry.wait(ctx, n); werr != nil {
			return result, fmt.Errorf("%w (after attempt %d: %w)", werr, n, unmarkLoad(err))
		}
	}
}

// unmarkLoad returns the error a load failure wraps, so callers see the original.
func unmarkLoad(err error) error {
	var loaded *loadFailure
	if errors.As(err, &loaded) {
		return loaded.err
	}

	return err
}
