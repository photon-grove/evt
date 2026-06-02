package ingressbudget

import (
	"testing"
	"time"
)

func TestBucketAllowUnlimited(t *testing.T) {
	b := NewBucket(0, time.Unix(0, 0).UTC())
	for range 1000 {
		if !b.Allow(time.Unix(0, 0).UTC()) {
			t.Fatal("unlimited bucket should always allow")
		}
	}
}

func TestBucketAllowAndRefill(t *testing.T) {
	start := time.Unix(0, 0).UTC()
	b := NewBucket(2, start)

	if !b.Allow(start) {
		t.Fatal("first token should allow")
	}
	if !b.Allow(start) {
		t.Fatal("second token should allow")
	}
	if b.Allow(start) {
		t.Fatal("third token should be denied before refill")
	}

	// Refill one token after 30s at 2/min rate.
	if !b.Allow(start.Add(30 * time.Second)) {
		t.Fatal("expected a token after partial refill window")
	}
	if b.Allow(start.Add(30 * time.Second)) {
		t.Fatal("expected denial after consuming partial refill")
	}
}
