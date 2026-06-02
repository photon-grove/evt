package ingressbudget

import (
	"testing"
	"time"
)

func TestControllerDecisions(t *testing.T) {
	now := time.Unix(0, 0).UTC()
	c := NewController(1, 1, now)

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
