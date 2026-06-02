package publishers

import (
	"fmt"
	"os"
	"strconv"
)

const (
	defaultEventsPerMinute      = 300
	defaultRetryBudgetPerMinute = 60
)

// Config holds runtime settings for a DynamoDB stream event publisher Lambda.
type Config struct {
	EventsTopicARN       string
	EventsFIFOTopicARN   string // optional companion FIFO topic for realtime fanout
	Source               string
	EventsPerMinute      int
	RetryBudgetPerMinute int
}

// LoadConfigFromEnv loads publisher settings from environment variables.
// defaultSource is used when EVENT_SOURCE is not set.
func LoadConfigFromEnv(defaultSource string) (*Config, error) {
	eventsTopicARN := os.Getenv("EVENTS_TOPIC_ARN")
	if eventsTopicARN == "" {
		return nil, fmt.Errorf("EVENTS_TOPIC_ARN is required")
	}

	source := os.Getenv("EVENT_SOURCE")
	if source == "" {
		source = defaultSource
	}
	if source == "" {
		return nil, fmt.Errorf("EVENT_SOURCE is required")
	}

	eventsPerMinute, err := parseIntEnv("INGRESS_EVENTS_PER_MINUTE", defaultEventsPerMinute)
	if err != nil {
		return nil, err
	}

	retryBudgetPerMinute, err := parseIntEnv("INGRESS_RETRY_BUDGET_PER_MINUTE", defaultRetryBudgetPerMinute)
	if err != nil {
		return nil, err
	}

	return &Config{
		EventsTopicARN:       eventsTopicARN,
		EventsFIFOTopicARN:   os.Getenv("EVENTS_FIFO_TOPIC_ARN"),
		Source:               source,
		EventsPerMinute:      eventsPerMinute,
		RetryBudgetPerMinute: retryBudgetPerMinute,
	}, nil
}

func parseIntEnv(name string, defaultValue int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return defaultValue, nil
	}

	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a whole integer: %w", name, err)
	}
	if value < 0 {
		return 0, fmt.Errorf("%s must be >= 0", name)
	}

	return value, nil
}
