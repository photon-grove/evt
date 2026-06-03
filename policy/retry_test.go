package policy

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestExecute_RetriesTransientThenSucceeds(t *testing.T) {
	calls := 0
	retries := 0
	transient := errors.New("transient")

	err := Execute(
		context.Background(),
		Config{
			MaxAttempts: 3,
			BaseDelay:   10 * time.Millisecond,
			MaxDelay:    100 * time.Millisecond,
			Jitter:      func(time.Duration) time.Duration { return 0 },
			Sleep:       func(context.Context, time.Duration) error { return nil },
		},
		func() error {
			calls++
			if calls < 3 {
				return &ClassifiedError{Class: ClassTransient, Err: transient}
			}
			return nil
		},
		Hooks{
			OnRetry: func(Attempt) { retries++ },
		},
	)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
	if retries != 2 {
		t.Fatalf("expected 2 retries, got %d", retries)
	}
}

func TestExecute_DefaultConfigRetriesClassifiedTransient(t *testing.T) {
	calls := 0
	transient := errors.New("transient")
	cfg := DefaultConfig()
	cfg.Sleep = func(context.Context, time.Duration) error { return nil }

	err := Execute(
		context.Background(),
		cfg,
		func() error {
			calls++
			if calls < 2 {
				return &ClassifiedError{Class: ClassTransient, Err: transient}
			}

			return nil
		},
		Hooks{},
	)
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}
}

func TestExecute_GiveUpOnPermanent(t *testing.T) {
	calls := 0
	giveUps := 0
	permanent := errors.New("permanent")

	err := Execute(
		context.Background(),
		Config{
			MaxAttempts: 5,
			IsRetryable: func(error) bool { return false },
		},
		func() error {
			calls++
			return permanent
		},
		Hooks{
			OnGiveUp: func(Attempt) { giveUps++ },
		},
	)
	if !errors.Is(err, permanent) {
		t.Fatalf("expected permanent error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
	if giveUps != 1 {
		t.Fatalf("expected 1 give-up callback, got %d", giveUps)
	}
}

func TestExecute_ReturnsClassifiedErrorOnGiveUp(t *testing.T) {
	transient := errors.New("transient")

	err := Execute(
		context.Background(),
		Config{
			MaxAttempts: 2,
			BaseDelay:   time.Millisecond,
			Sleep:       func(context.Context, time.Duration) error { return nil },
		},
		func() error {
			return &ClassifiedError{Class: ClassTransient, Err: transient}
		},
		Hooks{},
	)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	var ce *ClassifiedError
	if !errors.As(err, &ce) {
		t.Fatalf("expected ClassifiedError, got %T", err)
	}
	if ce.Class != ClassTransient {
		t.Fatalf("expected transient class, got %s", ce.Class)
	}
	if !errors.Is(ce.Err, transient) {
		t.Fatalf("expected transient error, got %v", ce.Err)
	}
}

func TestExponentialJitterDelay_Bounded(t *testing.T) {
	d0 := ExponentialJitterDelay(100*time.Millisecond, 2*time.Second, 0, func(time.Duration) time.Duration { return 0 })
	if d0 < 50*time.Millisecond || d0 > 100*time.Millisecond {
		t.Fatalf("attempt 0 out of bounds: %s", d0)
	}

	dMax := ExponentialJitterDelay(100*time.Millisecond, 2*time.Second, 99, func(time.Duration) time.Duration { return 0 })
	if dMax < time.Second || dMax > 2*time.Second {
		t.Fatalf("max out of bounds: %s", dMax)
	}
}
