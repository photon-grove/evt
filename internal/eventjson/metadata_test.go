package eventjson

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"

	"github.com/photon-grove/evt"
)

func Test_Metadata_V09FieldsEncode(t *testing.T) {
	// The storage format's metadata example (docs/spec/storage-format.md#metadata): causationId is
	// unset and defaults to the commandId, extensions is set, principalId is the caller's
	// identity, and the engine-assigned commitId defaults to the commandId.
	metadata := evt.NewMetadata(context.Background(), ptr("us-west-2"),
		evt.WithCommandID("cmd-0001"),
		evt.WithOrigin(evt.Origin{Source: "billing-api", Endpoint: "POST /accounts"}),
		evt.WithAddress("203.0.113.7"),
		evt.WithPrincipalID("user:42"),
		evt.WithExtension("requestSource", "cli"),
	)
	metadata.Timestamp = "2026-10-12T00:00:00Z"
	metadata.Trace = &propagation.MapCarrier{
		"baggage":     "tenant=example",
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"tracestate":  "vendor=value",
	}
	// The engine stamps the commit ID before encoding; with a command ID present it is the
	// command ID itself.
	metadata = evt.CommitMetadata(metadata)

	data, err := MarshalMetadata(metadata)
	require.NoError(t, err)

	require.JSONEq(t, `{
		"commandId": "cmd-0001",
		"trace": {"baggage": "tenant=example", "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "tracestate": "vendor=value"},
		"origin": {"source": "billing-api", "endpoint": "POST /accounts"},
		"address": "203.0.113.7",
		"region": "us-west-2",
		"timestamp": "2026-10-12T00:00:00Z",
		"principalId": "user:42",
		"causationId": "cmd-0001",
		"extensions": {"requestSource": "cli"},
		"commitId": "cmd-0001"
	}`, string(data))
}

func Test_Metadata_CausationIDDefaults(t *testing.T) {
	testCases := []struct {
		name     string
		metadata evt.Metadata
		expected any
	}{
		{"unset falls back to the command ID", evt.Metadata{CommandID: ptr(evt.CommandID("cmd-0001"))}, "cmd-0001"},
		{"explicit causation wins", evt.Metadata{CommandID: ptr(evt.CommandID("cmd-0002")), CausationID: ptr(evt.CommandID("cmd-0001"))}, "cmd-0001"},
		{"neither set", evt.Metadata{}, nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := MarshalMetadata(tc.metadata)
			require.NoError(t, err)

			var raw struct {
				CausationID *string `json:"causationId"`
			}
			require.NoError(t, json.Unmarshal(data, &raw))

			if tc.expected == nil {
				require.Nil(t, raw.CausationID)

				return
			}
			require.NotNil(t, raw.CausationID)
			require.Equal(t, tc.expected, *raw.CausationID)
		})
	}
}

func Test_Metadata_ExtensionsAlwaysWritten(t *testing.T) {
	testCases := []struct {
		name     string
		metadata evt.Metadata
		expected string
	}{
		{"unset", evt.Metadata{}, `"extensions":{}`},
		{"empty", evt.Metadata{Extensions: map[string]string{}}, `"extensions":{}`},
		{"keys sorted by encoded string", evt.Metadata{Extensions: map[string]string{"b": "2", "a": "1", "c": "3"}}, `"extensions":{"a":"1","b":"2","c":"3"}`},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := MarshalMetadata(tc.metadata)
			require.NoError(t, err)
			require.Contains(t, string(data), tc.expected)
		})
	}
}

func Test_Metadata_CommitIDAlwaysWritten(t *testing.T) {
	data, err := MarshalMetadata(evt.Metadata{})
	require.NoError(t, err)
	require.Contains(t, string(data), `"commitId":""`)

	// A pre-v0.9 row without the key decodes with an empty commit ID.
	var decoded evt.Metadata
	require.NoError(t, UnmarshalMetadata([]byte(`{"region":"us-west-2"}`), &decoded))
	require.Empty(t, decoded.CommitID)
}

func Test_Metadata_EqualRoundTrip(t *testing.T) {
	cases := []struct {
		name     string
		metadata evt.Metadata
	}{
		{"unset", evt.Metadata{}},
		{"empty extensions", evt.Metadata{Extensions: map[string]string{}}},
		{"default causation", evt.Metadata{CommandID: ptr(evt.CommandID("c-1"))}},
		{"explicit causation", evt.Metadata{
			CommandID:   ptr(evt.CommandID("c-1")),
			PrincipalID: ptr(evt.PrincipalID("user:42")),
			CausationID: ptr(evt.CommandID("c-0")),
			Extensions:  map[string]string{"source": "cli", "empty": ""},
		}},
		{"causation without command", evt.Metadata{CausationID: ptr(evt.CommandID("c-0"))}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := MarshalMetadata(tc.metadata)
			require.NoError(t, err)

			var decoded evt.Metadata
			require.NoError(t, UnmarshalMetadata(data, &decoded))
			require.True(t, tc.metadata.Equal(decoded))
			require.True(t, decoded.Equal(tc.metadata))
		})
	}

	defaulted := evt.Metadata{CommandID: ptr(evt.CommandID("c-1"))}
	explicit := defaulted
	explicit.CausationID = ptr(evt.CommandID("c-1"))
	require.True(t, defaulted.Equal(explicit))
	require.True(t, explicit.Equal(defaulted))

	explicit.CausationID = ptr(evt.CommandID("c-0"))
	require.False(t, defaulted.Equal(explicit))
	require.False(t, explicit.Equal(defaulted))
	require.False(t, evt.Metadata{}.Equal(evt.Metadata{CausationID: ptr(evt.CommandID("c-0"))}))
}

func ptr[T any](value T) *T { return &value }
