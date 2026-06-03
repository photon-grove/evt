package evt

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
)

func Test_Metadata_NewMetadata(t *testing.T) {
	ctx := context.Background()
	region := "us-east-1"

	metadata := NewMetadata(ctx, &region)

	require.Equal(t, region, metadata.Region)
	require.NotEmpty(t, metadata.Timestamp)
	require.Nil(t, metadata.CommandID)
	require.Nil(t, metadata.Trace)
	require.Nil(t, metadata.Origin)
}

func Test_Metadata_NewMetadataWithOptions(t *testing.T) {
	ctx := context.Background()
	region := "us-west-2"
	cmdID := CommandID("cmd-123")
	origin := Origin{
		Source:   "api",
		Endpoint: "/users",
	}

	metadata := NewMetadata(ctx, &region,
		WithCommandID(cmdID),
		WithOrigin(origin),
		WithTrace(ctx),
	)

	require.Equal(t, region, metadata.Region)
	require.NotEmpty(t, metadata.Timestamp)
	require.NotNil(t, metadata.CommandID)
	require.Equal(t, cmdID, *metadata.CommandID)
	require.NotNil(t, metadata.Origin)
	require.Equal(t, origin, *metadata.Origin)
	require.NotNil(t, metadata.Trace)
}

func Test_Metadata_WithNilRegion(t *testing.T) {
	ctx := context.Background()

	metadata := NewMetadata(ctx, nil)

	require.Empty(t, metadata.Region)
	require.NotEmpty(t, metadata.Timestamp)
}

func Test_Metadata_TimestampFormat(t *testing.T) {
	ctx := context.Background()
	region := "us-east-1"

	before := time.Now().UTC()
	metadata := NewMetadata(ctx, &region)
	after := time.Now().UTC()

	// Parse the timestamp
	parsedTime, err := time.Parse(time.RFC3339, metadata.Timestamp)
	require.NoError(t, err)

	// Verify it's within our time window
	require.True(t, parsedTime.After(before.Add(-1*time.Second)))
	require.True(t, parsedTime.Before(after.Add(1*time.Second)))

	// Verify it's in UTC
	require.Equal(t, time.UTC, parsedTime.Location())
}

func Test_Metadata_WithCommandID(t *testing.T) {
	testCases := []struct {
		name  string
		cmdID CommandID
	}{
		{"Simple ID", "cmd-123"},
		{"UUID", "550e8400-e29b-41d4-a716-446655440000"},
		{"Empty ID", ""},
		{"Complex ID", "user:123:action:create"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			region := "us-east-1"

			metadata := NewMetadata(ctx, &region, WithCommandID(tc.cmdID))

			require.NotNil(t, metadata.CommandID)
			require.Equal(t, tc.cmdID, *metadata.CommandID)
		})
	}
}

func Test_Metadata_WithOrigin(t *testing.T) {
	testCases := []struct {
		name   string
		origin Origin
	}{
		{
			"API Origin",
			Origin{Source: "api", Endpoint: "/users"},
		},
		{
			"Web Origin",
			Origin{Source: "web", Endpoint: "/dashboard"},
		},
		{
			"CLI Origin",
			Origin{Source: "cli", Endpoint: "user-create"},
		},
		{
			"Empty Source",
			Origin{Source: "", Endpoint: "/test"},
		},
		{
			"Empty Endpoint",
			Origin{Source: "test", Endpoint: ""},
		},
		{
			"Both Empty",
			Origin{Source: "", Endpoint: ""},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			region := "us-east-1"

			metadata := NewMetadata(ctx, &region, WithOrigin(tc.origin))

			require.NotNil(t, metadata.Origin)
			require.Equal(t, tc.origin, *metadata.Origin)
		})
	}
}

func Test_Metadata_WithTrace(t *testing.T) {
	ctx := context.Background()
	region := "us-east-1"

	metadata := NewMetadata(ctx, &region, WithTrace(ctx))

	require.NotNil(t, metadata.Trace)
	// The trace carrier should be created, even if empty
	require.IsType(t, &propagation.MapCarrier{}, metadata.Trace)
}

func Test_Metadata_WithAddress(t *testing.T) {
	ctx := context.Background()
	region := "us-east-1"

	metadata := NewMetadata(ctx, &region, WithAddress("203.0.113.1"))

	require.NotNil(t, metadata.Address)
	require.Equal(t, "203.0.113.1", *metadata.Address)
}

func Test_Metadata_WithAddressEmptyClearsAddress(t *testing.T) {
	ctx := context.Background()
	region := "us-east-1"

	metadata := NewMetadata(ctx, &region,
		WithAddress("203.0.113.1"),
		WithAddress(""),
	)

	require.Nil(t, metadata.Address)
}

func Test_Metadata_MultipleOptions(t *testing.T) {
	ctx := context.Background()
	region := "eu-west-1"
	cmdID := CommandID("batch-456")
	origin := Origin{Source: "batch", Endpoint: "import"}

	metadata := NewMetadata(ctx, &region,
		WithCommandID(cmdID),
		WithOrigin(origin),
		WithTrace(ctx),
		WithAddress("203.0.113.2"),
	)

	// Verify all fields are set
	require.Equal(t, region, metadata.Region)
	require.NotNil(t, metadata.CommandID)
	require.Equal(t, cmdID, *metadata.CommandID)
	require.NotNil(t, metadata.Origin)
	require.Equal(t, origin, *metadata.Origin)
	require.NotNil(t, metadata.Trace)
	require.NotNil(t, metadata.Address)
	require.Equal(t, "203.0.113.2", *metadata.Address)
	require.NotEmpty(t, metadata.Timestamp)
}

func Test_Metadata_OptionsModification(t *testing.T) {
	// Test that options don't interfere with each other
	ctx := context.Background()
	region := "us-central-1"

	cmdID1 := CommandID("cmd-1")
	cmdID2 := CommandID("cmd-2")

	opt1 := WithCommandID(cmdID1)
	opt2 := WithCommandID(cmdID2)

	// First metadata with first option
	metadata1 := NewMetadata(ctx, &region, opt1)
	require.Equal(t, cmdID1, *metadata1.CommandID)

	// Second metadata with second option
	metadata2 := NewMetadata(ctx, &region, opt2)
	require.Equal(t, cmdID2, *metadata2.CommandID)

	// They should be different
	require.NotEqual(t, *metadata1.CommandID, *metadata2.CommandID)
}

func Test_Metadata_StructFields(t *testing.T) {
	// Test direct struct field access
	ctx := context.Background()
	region := "test-region"

	metadata := NewMetadata(ctx, &region)

	// Test that we can access all fields
	require.IsType(t, (*CommandID)(nil), metadata.CommandID)
	require.IsType(t, (*propagation.MapCarrier)(nil), metadata.Trace)
	require.IsType(t, (*Origin)(nil), metadata.Origin)
	require.IsType(t, (*string)(nil), metadata.Address)
	require.IsType(t, "", metadata.Region)
	require.IsType(t, "", metadata.Timestamp)
}

func Test_Origin_StructFields(t *testing.T) {
	origin := Origin{
		Source:   Source("test-source"),
		Endpoint: Endpoint("test-endpoint"),
	}

	require.Equal(t, "test-source", string(origin.Source))
	require.Equal(t, "test-endpoint", string(origin.Endpoint))
}

func Test_Source_TypeDefinition(t *testing.T) {
	var source Source = "api"
	require.Equal(t, "api", string(source))

	// Test different sources
	sources := []Source{"web", "cli", "batch", "event", ""}
	for _, s := range sources {
		require.Equal(t, s, s)
	}
}

func Test_Endpoint_TypeDefinition(t *testing.T) {
	var endpoint Endpoint = "/users/create"
	require.Equal(t, "/users/create", string(endpoint))

	// Test different endpoints
	endpoints := []Endpoint{"/api/v1/users", "user-command", "", "/"}
	for _, e := range endpoints {
		require.Equal(t, e, e)
	}
}

func Test_Metadata_EmptyOptions(t *testing.T) {
	ctx := context.Background()
	region := "us-test-1"

	// Test with no options
	metadata := NewMetadata(ctx, &region)

	require.Equal(t, region, metadata.Region)
	require.NotEmpty(t, metadata.Timestamp)
	require.Nil(t, metadata.CommandID)
	require.Nil(t, metadata.Trace)
	require.Nil(t, metadata.Origin)
	require.Nil(t, metadata.Address) // No address in plain context
}

func Test_Metadata_OptionsFunctionSignature(t *testing.T) {
	// Test that options are functions that take and return Metadata
	ctx := context.Background()
	region := "test"

	// Create a custom option function
	customOption := func(m Metadata) Metadata {
		m.Region = "overridden-region"
		return m
	}

	metadata := NewMetadata(ctx, &region, customOption)

	// The custom option should have overridden the region
	require.Equal(t, "overridden-region", metadata.Region)
}

func Test_Metadata_NestedContextValues(t *testing.T) {
	// Test with nested context values
	baseCtx := context.Background()
	type testKeyType struct{}
	ctxWithValues := context.WithValue(baseCtx, testKeyType{}, "testValue")

	region := "nested-region"
	metadata := NewMetadata(ctxWithValues, &region)

	require.Equal(t, region, metadata.Region)
	require.Nil(t, metadata.Address)
}

func Test_Metadata_JSONKeys(t *testing.T) {
	ctx := context.Background()
	region := "us-east-1"
	cmdID := CommandID("cmd-test")
	origin := Origin{Source: "api", Endpoint: "/test"}

	metadata := NewMetadata(ctx, &region,
		WithCommandID(cmdID),
		WithOrigin(origin),
	)

	data, err := json.Marshal(metadata)
	require.NoError(t, err)

	var raw map[string]json.RawMessage
	err = json.Unmarshal(data, &raw)
	require.NoError(t, err)

	// All keys must be lowercase camelCase, matching the TypeScript consumer
	require.Contains(t, raw, "timestamp", "Timestamp field must serialize as lowercase 'timestamp'")
	require.Contains(t, raw, "region", "Region field must serialize as lowercase 'region'")
	require.Contains(t, raw, "commandId")
	require.Contains(t, raw, "origin")

	// Verify capital-case keys are NOT present
	require.NotContains(t, raw, "Timestamp")
	require.NotContains(t, raw, "Region")
}

func Test_Metadata_ConcurrentCreation(t *testing.T) {
	// Test that concurrent metadata creation works correctly
	const numRoutines = 10
	results := make(chan Metadata, numRoutines)

	ctx := context.Background()
	region := "concurrent-test"

	for i := 0; i < numRoutines; i++ {
		go func(id int) {
			cmdID := CommandID("cmd-" + string(rune('0'+id)))
			metadata := NewMetadata(ctx, &region, WithCommandID(cmdID))
			results <- metadata
		}(i)
	}

	// Collect results
	metadatas := make([]Metadata, 0, numRoutines)
	for i := 0; i < numRoutines; i++ {
		metadatas = append(metadatas, <-results)
	}

	// Verify all are valid and different
	for i, metadata := range metadatas {
		require.Equal(t, region, metadata.Region)
		require.NotEmpty(t, metadata.Timestamp)
		require.NotNil(t, metadata.CommandID)

		// Each should have a different command ID
		for j, other := range metadatas {
			if i != j {
				require.NotEqual(t, *metadata.CommandID, *other.CommandID)
			}
		}
	}
}
