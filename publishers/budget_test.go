package publishers

import (
	"testing"
	"time"
)

func TestBucketAllowUnlimited(t *testing.T) {
	b := newBucket(0, time.Unix(0, 0).UTC())
	for range 1000 {
		if !b.allow(time.Unix(0, 0).UTC()) {
			t.Fatal("unlimited bucket should always allow")
		}
	}
}

func TestBucketAllowAndRefill(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	b := newBucket(2, start)

	if !b.allow(start) {
		t.Fatal("first token should allow")
	}
	if !b.allow(start) {
		t.Fatal("second token should allow")
	}
	if b.allow(start) {
		t.Fatal("third token should be denied before refill")
	}

	// Refill one token after 30s at 2/min rate.
	if !b.allow(start.Add(30 * time.Second)) {
		t.Fatal("expected a token after partial refill window")
	}
	if b.allow(start.Add(30 * time.Second)) {
		t.Fatal("expected denial after consuming partial refill")
	}
}

func TestBudgetControllerDecisions(t *testing.T) {
	now := time.Unix(0, 0).UTC()
	c := NewBudgetController(1, 1, now)

	if got := c.AllowEvent(now); got != DecisionAllow {
		t.Fatalf("event expected allow, got %s", got)
	}
	if got := c.AllowEvent(now); got != DecisionDrop {
		t.Fatalf("second event expected drop, got %s", got)
	}

	if got := c.AllowRetry(now); got != DecisionAllow {
		t.Fatalf("retry expected allow, got %s", got)
	}
	if got := c.AllowRetry(now); got != DecisionDrop {
		t.Fatalf("second retry expected drop, got %s", got)
	}
}
