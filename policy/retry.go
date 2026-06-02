package policy

import (
	"context"
	"time"
)

// Attempt contains retry telemetry fields emitted by Execute.
type Attempt struct {
	Attempt int
	Delay   time.Duration
	Class   Class
	Err     error
}

// Hooks emits retry lifecycle callbacks for telemetry/logging.
type Hooks struct {
	OnRetry  func(Attempt)
	OnGiveUp func(Attempt)
}

// Config controls retry behavior for optimistic-concurrency commit paths.
type Config struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Jitter      func(time.Duration) time.Duration
	Sleep       func(context.Context, time.Duration) error
	IsRetryable func(error) bool
}

// Execute runs op with bounded retries using the configured policy.
func Execute(ctx context.Context, cfg Config, op func() error, hooks Hooks) error {
	maxAttempts := cfg.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	isRetryable := cfg.IsRetryable
	if isRetryable == nil {
		isRetryable = IsTransient
	}

	sleep := cfg.Sleep
	if sleep == nil {
		sleep = SleepWithContext
	}

	for attempt := 1; ; attempt++ {
		err := op()
		if err == nil {
			return nil
		}

		class := Classify(err)
		if attempt >= maxAttempts || !isRetryable(err) {
			if hooks.OnGiveUp != nil {
				hooks.OnGiveUp(Attempt{Attempt: attempt, Class: class, Err: err})
			}
			return &ClassifiedError{Class: class, Err: err}
		}

		delay := ExponentialJitterDelay(cfg.BaseDelay, cfg.MaxDelay, attempt-1, cfg.Jitter)
		if hooks.OnRetry != nil {
			hooks.OnRetry(Attempt{Attempt: attempt, Delay: delay, Class: class, Err: err})
		}

		if err := sleep(ctx, delay); err != nil {
			return err
		}
	}
}

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
