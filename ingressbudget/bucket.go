// Package ingressbudget provides token-bucket controls for ingestion safety.
package ingressbudget

import (
	"sync"
	"time"
)

// Bucket is a simple token bucket limiter with minute-based refill semantics.
// A rate <= 0 means "unlimited".
type Bucket struct {
	ratePerMinute int

	mu          sync.Mutex
	tokens      float64
	lastRefill  time.Time
	capacity    float64
	refillPerNS float64
}

// NewBucket creates a new bucket.
func NewBucket(ratePerMinute int, now time.Time) *Bucket {
	if now.IsZero() {
		now = time.Now().UTC()
	}

	b := &Bucket{
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

// Allow returns true when at least one token can be consumed.
func (b *Bucket) Allow(now time.Time) bool {
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

func (b *Bucket) refill(now time.Time) {
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
