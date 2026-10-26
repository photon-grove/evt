package policy

import (
	"context"
	"time"
)

// ExponentialJitterDelay returns bounded exponential backoff with jitter in [base/2, base].
func ExponentialJitterDelay(baseDelay, maxDelay time.Duration, attempt int, jitter func(time.Duration) time.Duration) time.Duration {
	if baseDelay <= 0 {
		baseDelay = time.Millisecond
	}
	if maxDelay <= 0 {
		maxDelay = baseDelay
	}

	delay := baseDelay
	for i := 0; i < attempt; i++ {
		delay *= 2
		if delay >= maxDelay || delay <= 0 {
			delay = maxDelay
			break
		}
	}
	if delay > maxDelay {
		delay = maxDelay
	}

	half := delay / 2
	if half <= 0 {
		return delay
	}

	if jitter == nil {
		return half
	}
	j := jitter(delay - half)
	if j < 0 {
		j = 0
	}
	if j > delay-half {
		j = delay - half
	}

	return half + j
}

// SleepWithContext blocks for delay unless context is canceled.
func SleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
