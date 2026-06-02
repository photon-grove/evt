package publishers_test

import (
	"testing"

	"github.com/photon-grove/evt/publishers"
	"github.com/stretchr/testify/require"
)

func TestLoadConfigFromEnv_Defaults(t *testing.T) {
	t.Setenv("EVENTS_TOPIC_ARN", "arn:aws:sns:us-west-2:123456789012:evt-example-events")
	t.Setenv("EVENT_SOURCE", "")
	t.Setenv("INGRESS_EVENTS_PER_MINUTE", "")
	t.Setenv("INGRESS_RETRY_BUDGET_PER_MINUTE", "")

	cfg, err := publishers.LoadConfigFromEnv("example.events")
	require.NoError(t, err)
	require.Equal(t, "arn:aws:sns:us-west-2:123456789012:evt-example-events", cfg.EventsTopicARN)
	require.Equal(t, "example.events", cfg.Source)
	require.Equal(t, 300, cfg.EventsPerMinute)
	require.Equal(t, 60, cfg.RetryBudgetPerMinute)
}

func TestLoadConfigFromEnv_ParsesOverrides(t *testing.T) {
	t.Setenv("EVENTS_TOPIC_ARN", "topic")
	t.Setenv("EVENT_SOURCE", "custom.source")
	t.Setenv("INGRESS_EVENTS_PER_MINUTE", "90")
	t.Setenv("INGRESS_RETRY_BUDGET_PER_MINUTE", "30")

	cfg, err := publishers.LoadConfigFromEnv("example.events")
	require.NoError(t, err)
	require.Equal(t, "custom.source", cfg.Source)
	require.Equal(t, 90, cfg.EventsPerMinute)
	require.Equal(t, 30, cfg.RetryBudgetPerMinute)
}

func TestLoadConfigFromEnv_RequiresTopicARN(t *testing.T) {
	t.Setenv("EVENTS_TOPIC_ARN", "")
	_, err := publishers.LoadConfigFromEnv("example.events")
	require.Error(t, err)
}

func TestLoadConfigFromEnv_RequiresSourceWithoutDefault(t *testing.T) {
	t.Setenv("EVENTS_TOPIC_ARN", "topic")
	t.Setenv("EVENT_SOURCE", "")

	_, err := publishers.LoadConfigFromEnv("")
	require.Error(t, err)
}

func TestLoadConfigFromEnv_RejectsInvalidInts(t *testing.T) {
	t.Setenv("EVENTS_TOPIC_ARN", "topic")
	t.Setenv("INGRESS_EVENTS_PER_MINUTE", "not-an-int")
	_, err := publishers.LoadConfigFromEnv("example.events")
	require.Error(t, err)
}

func TestLoadConfigFromEnv_RejectsNegativeInts(t *testing.T) {
	t.Setenv("EVENTS_TOPIC_ARN", "topic")
	t.Setenv("INGRESS_EVENTS_PER_MINUTE", "-1")
	_, err := publishers.LoadConfigFromEnv("example.events")
	require.Error(t, err)
}

func TestLoadConfigFromEnv_PicksUpOptionalFIFOARN(t *testing.T) {
	t.Setenv("EVENTS_TOPIC_ARN", "arn:aws:sns:us-west-2:123:evt-example-events")
	t.Setenv("EVENTS_FIFO_TOPIC_ARN", "arn:aws:sns:us-west-2:123:evt-example-events-fifo.fifo")

	cfg, err := publishers.LoadConfigFromEnv("example.events")
	require.NoError(t, err)
	require.Equal(t, "arn:aws:sns:us-west-2:123:evt-example-events-fifo.fifo", cfg.EventsFIFOTopicARN)
}

func TestLoadConfigFromEnv_DefaultsFIFOARNEmpty(t *testing.T) {
	t.Setenv("EVENTS_TOPIC_ARN", "arn:aws:sns:us-west-2:123:evt-example-events")
	t.Setenv("EVENTS_FIFO_TOPIC_ARN", "")

	cfg, err := publishers.LoadConfigFromEnv("example.events")
	require.NoError(t, err)
	require.Empty(t, cfg.EventsFIFOTopicARN)
}
