package policy

import (
	"testing"
	"time"
)

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

func TestDefaultJitter_Bounded(t *testing.T) {
	if got := DefaultJitter(0); got != 0 {
		t.Fatalf("zero max must give zero jitter, got %s", got)
	}

	for range 100 {
		if got := DefaultJitter(10 * time.Millisecond); got < 0 || got > 10*time.Millisecond {
			t.Fatalf("jitter out of bounds: %s", got)
		}
	}
}
