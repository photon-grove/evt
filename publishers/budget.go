package publishers

import (
	"sync"
	"time"
)

// BudgetDecision captures a publisher budget result.
type BudgetDecision string

const (
	// DecisionAllow means budget permitted the operation.
	DecisionAllow BudgetDecision = "allow"
	// DecisionDrop means budget denied the operation.
	DecisionDrop BudgetDecision = "drop"
)

// BudgetController exposes ingress and retry budget checks.
type BudgetController interface {
	AllowEvent(now time.Time) BudgetDecision
	AllowRetry(now time.Time) BudgetDecision
}

type budgetController struct {
	events *bucket
	retry  *bucket
}

// NewBudgetController creates a publisher budget controller with events/min and retries/min limits.
func NewBudgetController(eventsPerMinute, retriesPerMinute int, now time.Time) BudgetController {
	return &budgetController{
		events: newBucket(eventsPerMinute, now),
		retry:  newBucket(retriesPerMinute, now),
	}
}

func (c *budgetController) AllowEvent(now time.Time) BudgetDecision {
	if c.events.allow(now) {
		return DecisionAllow
	}

	return DecisionDrop
}

func (c *budgetController) AllowRetry(now time.Time) BudgetDecision {
	if c.retry.allow(now) {
		return DecisionAllow
	}

	return DecisionDrop
}

type bucket struct {
	ratePerMinute int

	mu          sync.Mutex
	tokens      float64
	lastRefill  time.Time
	capacity    float64
	refillPerNS float64
}

func newBucket(ratePerMinute int, now time.Time) *bucket {
	if now.IsZero() {
		now = time.Now().UTC()
	}

	b := &bucket{
		ratePerMinute: ratePerMinute,
		lastRefill:    now,
	}

	if ratePerMinute > 0 {
		b.capacity = float64(ratePerMinute)
		b.tokens = b.capacity
		b.refillPerNS = b.capacity / float64(time.Minute)
	}

	return b
}

func (b *bucket) allow(now time.Time) bool {
	if b.ratePerMinute <= 0 {
		return true
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.refill(now)
	if b.tokens < 1 {
		return false
	}
	b.tokens--

	return true
}

func (b *bucket) refill(now time.Time) {
	if !now.After(b.lastRefill) {
		return
	}

	elapsed := now.Sub(b.lastRefill)
	b.tokens += float64(elapsed.Nanoseconds()) * b.refillPerNS
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.lastRefill = now
}
