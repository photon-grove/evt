package ingressbudget

import "time"

// Decision captures a budget result.
type Decision string

const (
	// DecisionAllow means budget permitted the operation.
	DecisionAllow Decision = "allow"
	// DecisionDrop means budget denied the operation.
	DecisionDrop Decision = "drop"
)

// Controller coordinates ingress and retry budgets.
type Controller struct {
	events *Bucket
	retry  *Bucket
}

// NewController creates a controller with events/min and retries/min limits.
func NewController(eventsPerMinute, retriesPerMinute int, now time.Time) *Controller {
	return &Controller{
		events: NewBucket(eventsPerMinute, now),
		retry:  NewBucket(retriesPerMinute, now),
	}
}

// AllowEvent evaluates the ingress event budget.
func (c *Controller) AllowEvent(now time.Time) Decision {
	if c.events.Allow(now) {
		return DecisionAllow
	}
	return DecisionDrop
}

// AllowRetry evaluates the retry budget.
func (c *Controller) AllowRetry(now time.Time) Decision {
	if c.retry.Allow(now) {
		return DecisionAllow
	}
	return DecisionDrop
}
