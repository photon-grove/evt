package evt_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
)

// The worked examples in the storage spec (Identity digest) are the contract both languages
// reproduce.
func TestDefaultDigestReproducesTheSpecExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input evt.DigestInput
		want  string
	}{
		{
			name: "single stream",
			input: evt.DigestInput{
				CommandType: "account:open", Streams: []evt.EntityID{"acct-1"},
				Payload: []byte(`{"owner":"Ada"}`), Principal: "user:42",
			},
			want: "08f63bf06ef7167f79dda7f5de9307e1ffb626de015ecbc1c1482d1e67e5abd6",
		},
		{
			name: "multi stream, declaration order does not matter",
			input: evt.DigestInput{
				CommandType: "claim-username", Streams: []evt.EntityID{"profile#u-1", "claim#ada"},
				Payload: []byte(`{"handle":"ada"}`),
			},
			want: "21c04bf818ab67db244dc184ae98197928309a4b3a3533acba69dacf992b6d79",
		},
		{
			name: "multi stream with a principal",
			input: evt.DigestInput{
				CommandType: "claim-username", Streams: []evt.EntityID{"claim#ada", "profile#u-1"},
				Payload: []byte(`{"handle":"ada"}`), Principal: "user:42",
			},
			want: "56d01f5b9788b60e12a686e40807c28d9dfde802d82faf46888adda0acee7050",
		},
		{
			name:  "absent payload",
			input: evt.DigestInput{CommandType: "account:ping", Streams: []evt.EntityID{"acct-1"}, Principal: "user:42"},
			want:  "d24c7d5ce955a76c936a83e6eca2c3bc277d936c88a76460ed2d221e285a215a",
		},
		{
			name: "stale fence",
			input: evt.DigestInput{
				CommandType: "account:rename", Streams: []evt.EntityID{"acct-1"},
				Payload: []byte(`{"name":"Grace"}`), Principal: "user:42",
			},
			want: "ba4a6a859dcf103a00261515d7e309b31e8f07ca21732a4b7872b7fdebb92c1b",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			got, err := evt.DefaultDigest(test.input)
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestCanonicalDigestBytesMatchTheSpecLayout(t *testing.T) {
	t.Parallel()

	bytes := evt.CanonicalDigestBytes(evt.DigestInput{
		CommandType: "account:open", Streams: []evt.EntityID{"acct-1"},
		Payload: []byte(`{"owner":"Ada"}`), Principal: "user:42",
	})

	assert.Len(t, bytes, 78, "the spec's worked example is 78 bytes")
	assert.True(t, strings.HasPrefix(string(bytes), "evt-cmd-digest-v1\x00"))
}

func TestDigestDeduplicatesAndSortsStreams(t *testing.T) {
	t.Parallel()

	a := evt.CanonicalDigestBytes(evt.DigestInput{CommandType: "t", Streams: []evt.EntityID{"b", "a", "b"}})
	b := evt.CanonicalDigestBytes(evt.DigestInput{CommandType: "t", Streams: []evt.EntityID{"a", "b"}})

	assert.Equal(t, b, a)
}

func TestCommandIdentityEncodesThePayloadCanonically(t *testing.T) {
	t.Parallel()

	principal := evt.PrincipalID("user:42")
	input, err := evt.CommandIdentity{
		Type:    "account:open",
		Streams: []evt.EntityID{"acct-1"},
		Payload: map[string]string{"owner": "Ada"},
	}.DigestInput(evt.Metadata{PrincipalID: &principal})
	require.NoError(t, err)

	got, err := evt.DefaultDigest(input)
	require.NoError(t, err)
	assert.Equal(t, "08f63bf06ef7167f79dda7f5de9307e1ffb626de015ecbc1c1482d1e67e5abd6", got)

	none, err := evt.CommandIdentity{Type: "account:ping", Streams: []evt.EntityID{"acct-1"}}.DigestInput(evt.Metadata{PrincipalID: &principal})
	require.NoError(t, err)
	assert.Nil(t, none.Payload, "a nil payload digests as the zero-length byte string")
}

func TestRejectionEncodingMatchesTheSpec(t *testing.T) {
	t.Parallel()

	rejection := evt.Rejection{
		Code: "username-taken", Message: "handle ada is taken",
		Details: map[string]any{"handle": "ada"},
	}

	encoded, err := evt.EncodeRejection(rejection)
	require.NoError(t, err)
	assert.JSONEq(t, `{"code":"username-taken","message":"handle ada is taken","details":{"handle":"ada"}}`, string(encoded))
	assert.Equal(t, `{"code":"username-taken","message":"handle ada is taken","details":{"handle":"ada"}}`, string(encoded))

	bare, err := evt.EncodeRejection(evt.Rejection{Code: evt.RejectionStaleFence, Message: "a caller-stamped fence went stale"})
	require.NoError(t, err)
	assert.Equal(t, `{"code":"stale-fence","message":"a caller-stamped fence went stale","details":null}`, string(bare))

	html, err := evt.EncodeRejection(evt.Rejection{Code: "c", Message: "a<b>&c"})
	require.NoError(t, err)
	assert.Contains(t, string(html), `a\u003cb\u003e\u0026c`)
}

func TestDecodeRejectionKeepsNumbersVerbatim(t *testing.T) {
	t.Parallel()

	stored := `{"code":"limit","message":"too many","details":{"limit":9007199254740993,"n":1.50}}`

	rejection, err := evt.DecodeRejection([]byte(stored))
	require.NoError(t, err)

	reencoded, err := evt.EncodeRejection(rejection)
	require.NoError(t, err)
	assert.Equal(t, stored, string(reencoded), "a replayed rejection re-encodes byte for byte")
}

func TestRejectionOfMapsEngineErrors(t *testing.T) {
	t.Parallel()

	rejection, ok := evt.RejectionOf(evt.NewNotFoundError("no such account"))
	require.True(t, ok)
	assert.Equal(t, evt.Rejection{Code: evt.RejectionNotFound, Message: "no such account"}, rejection)

	wrapped := errors.Join(errors.New("context"), evt.NewNotFoundError("gone"))
	rejection, ok = evt.RejectionOf(wrapped)
	require.True(t, ok)
	assert.Equal(t, evt.RejectionNotFound, rejection.Code)

	rejection, ok = evt.RejectionOf(evt.NewBadCommandError(nil))
	require.True(t, ok)
	assert.Equal(t, evt.RejectionBadCommand, rejection.Code)

	_, ok = evt.RejectionOf(evt.NewBadEventError(nil))
	assert.False(t, ok, "a bad event is an internal failure, never a durable rejection")

	_, ok = evt.RejectionOf(errors.New("storage unavailable"))
	assert.False(t, ok)
}

func TestCheckOutcomeBoundsAreOrdered(t *testing.T) {
	t.Parallel()

	long := evt.CommandID(strings.Repeat("a", evt.MaxCommandIDBytes+1))
	oversized := make([]byte, evt.MaxOutcomeBytes+1)
	base := evt.Outcome{CommandID: "cmd-1", Status: evt.OutcomeAccepted, Digest: "d"}

	// The command ID is checked first, before the response bound.
	tooLong := base
	tooLong.CommandID, tooLong.Response = long, oversized
	var idErr *evt.CommandIDTooLongError
	require.ErrorAs(t, evt.CheckOutcome(&tooLong), &idErr)
	assert.Equal(t, evt.MaxCommandIDBytes+1, idErr.Length)

	// The limit is bytes, not characters.
	multibyte := base
	multibyte.CommandID = evt.CommandID(strings.Repeat("é", evt.MaxCommandIDBytes/2+1))
	require.ErrorAs(t, evt.CheckOutcome(&multibyte), &idErr)

	exact := base
	exact.CommandID = evt.CommandID(strings.Repeat("a", evt.MaxCommandIDBytes))
	exact.Response = make([]byte, evt.MaxOutcomeBytes)
	require.NoError(t, evt.CheckOutcome(&exact))

	tooBig := base
	tooBig.Response = oversized
	var sizeErr *evt.OutcomeTooLargeError
	require.ErrorAs(t, evt.CheckOutcome(&tooBig), &sizeErr)
	assert.Equal(t, "response", sizeErr.What)

	rejected := evt.Outcome{
		CommandID: "cmd-1", Status: evt.OutcomeRejected, Digest: "d",
		Rejection: &evt.Rejection{Code: "c", Message: strings.Repeat("x", evt.MaxOutcomeBytes)},
	}
	require.ErrorAs(t, evt.CheckOutcome(&rejected), &sizeErr)
	assert.Equal(t, "rejection", sizeErr.What)

	missing := base
	missing.CommandID = ""
	require.ErrorIs(t, evt.CheckOutcome(&missing), evt.ErrInvalidArgument)
}

func TestOutcomeAttributesMatchTheSpecRows(t *testing.T) {
	t.Parallel()

	accepted, err := evt.OutcomeAttributes(evt.Outcome{
		CommandID: "cmd-0001", Status: evt.OutcomeAccepted,
		Digest:   "08f63bf06ef7167f79dda7f5de9307e1ffb626de015ecbc1c1482d1e67e5abd6",
		Response: []byte(`{"accountId":"acct-1","balanceCents":0}`),
		EventIDs: []evt.EventID{"acct-1:1"},
	})
	require.NoError(t, err)

	row := map[string]string{}
	for _, attribute := range accepted {
		row[attribute.Name] = attribute.Value
	}

	assert.Equal(t, "command-outcome#cmd-0001", row["pk"])
	assert.Equal(t, "1", row["sk"])
	assert.Equal(t, "eyJhY2NvdW50SWQiOiJhY2N0LTEiLCJiYWxhbmNlQ2VudHMiOjB9", row["response"])
	assert.Equal(t, `["acct-1:1"]`, row["events"])
	assert.NotContains(t, row, "rejection")
	assert.NotContains(t, row, "code")

	noop, err := evt.OutcomeAttributes(evt.Outcome{CommandID: "cmd-0003", Status: evt.OutcomeAccepted, Digest: "d"})
	require.NoError(t, err)
	assert.Len(t, noop, 4, "an accepted no-op has neither events nor response")

	rejected, err := evt.OutcomeAttributes(evt.Outcome{
		CommandID: "cmd-0002", Status: evt.OutcomeRejected, Digest: "d",
		Rejection: &evt.Rejection{Code: "username-taken", Message: "handle ada is taken", Details: map[string]any{"handle": "ada"}},
	})
	require.NoError(t, err)

	names := make([]string, 0, len(rejected))
	for _, attribute := range rejected {
		names = append(names, attribute.Name)
	}
	assert.Equal(t, []string{"pk", "sk", "status", "digest", "rejection", "code"}, names)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(rejected[4].Value), &decoded))
	assert.Equal(t, "username-taken", decoded["code"])
}

func TestOutcomeFromAttributesRoundTrips(t *testing.T) {
	t.Parallel()

	original := evt.Outcome{
		CommandID: "cmd-1", Status: evt.OutcomeAccepted, Digest: "d",
		Response: []byte{0, 1, 2, 255}, EventIDs: []evt.EventID{"a:1", "b:1"},
	}

	attributes, err := evt.OutcomeAttributes(original)
	require.NoError(t, err)

	byName := map[string]string{}
	for _, attribute := range attributes {
		byName[attribute.Name] = attribute.Value
	}

	restored, err := evt.OutcomeFromAttributes("cmd-1", byName["status"], byName["digest"], byName["response"], byName["events"], byName["rejection"])
	require.NoError(t, err)
	assert.Equal(t, original, restored)
}

func TestNormalizeCommitDerivesOutcomeEventIDsInCanonicalOrder(t *testing.T) {
	t.Parallel()

	event := func(stream string, sequence int) evt.SerializedEvent {
		return evt.SerializedEvent{
			ID: evt.GetEventID(evt.EntityID(stream), evt.EventSequence(sequence)), Sequence: evt.EventSequence(sequence),
			EntityID: evt.EntityID(stream), EntityType: "t", Type: "t:e", Version: 1,
		}
	}

	result := evt.SerializedResult{
		Events:  []evt.SerializedEvent{event("profile#u-1", 1), event("claim#ada", 1), event("claim#ada", 2)},
		Outcome: &evt.Outcome{CommandID: "cmd-4", Status: evt.OutcomeAccepted, Digest: "d"},
	}

	normalized, err := evt.NormalizeCommit(result)
	require.NoError(t, err)

	assert.Equal(t, []evt.EventID{"claim#ada:1", "claim#ada:2", "profile#u-1:1"}, normalized.Outcome.EventIDs)
	assert.Empty(t, result.Outcome.EventIDs, "the caller's outcome is never mutated")
}

func TestNormalizeCommitRefusesBadOutcomes(t *testing.T) {
	t.Parallel()

	rejectedWithEvents := evt.SerializedResult{
		Events: []evt.SerializedEvent{{
			ID: "a:1", Sequence: 1, EntityID: "a", EntityType: "t", Type: "t:e", Version: 1,
		}},
		Outcome: &evt.Outcome{
			CommandID: "cmd", Status: evt.OutcomeRejected, Digest: "d",
			Rejection: &evt.Rejection{Code: "c", Message: "m"},
		},
	}

	_, err := evt.NormalizeCommit(rejectedWithEvents)
	require.ErrorIs(t, err, evt.ErrInvalidArgument)

	_, err = evt.NormalizeCommit(evt.SerializedResult{Outcome: &evt.Outcome{CommandID: "", Status: evt.OutcomeAccepted, Digest: "d"}})
	require.ErrorIs(t, err, evt.ErrInvalidArgument)
}

func TestIdempotencyKeyReuseMatchesWithErrorsIs(t *testing.T) {
	t.Parallel()

	err := error(&evt.IdempotencyKeyReuseError{CommandID: "cmd-1", Prior: evt.Outcome{CommandID: "cmd-1", Digest: "old"}})

	require.ErrorIs(t, err, evt.ErrIdempotencyKeyReuse)

	var reuse *evt.IdempotencyKeyReuseError
	require.ErrorAs(t, err, &reuse)
	assert.Equal(t, "old", reuse.Prior.Digest)
}

func TestDecodeRejectionRequiresTheWholeDocumentAndObjectDetails(t *testing.T) {
	t.Parallel()

	for name, stored := range map[string]string{
		"a second value":   `{"code":"c","message":"m","details":null} {}`,
		"trailing garbage": `{"code":"c","message":"m","details":null}x`,
		"array details":    `{"code":"c","message":"m","details":["x"]}`,
		"string details":   `{"code":"c","message":"m","details":"x"}`,
		"number details":   `{"code":"c","message":"m","details":1}`,
	} {
		_, err := evt.DecodeRejection([]byte(stored))
		require.Error(t, err, name)
	}

	_, err := evt.DecodeRejection([]byte(`{"code":"c","message":"m","details":{"a":1}} `))
	require.NoError(t, err, "trailing whitespace is not data")
}

func TestNormalizeCommitRefusesNonCanonicalEventIDs(t *testing.T) {
	t.Parallel()

	event := func(id evt.EventID) evt.SerializedEvent {
		return evt.SerializedEvent{
			ID: id, Sequence: 1, EntityID: "a", EntityType: "t", Type: "t:e", Version: 1,
		}
	}
	outcome := &evt.Outcome{CommandID: "cmd", Status: evt.OutcomeAccepted, Digest: "d"}

	// A mismatched ID is refused with or without an outcome, before any receipt is derived.
	for _, withOutcome := range []bool{false, true} {
		result := evt.SerializedResult{Events: []evt.SerializedEvent{event("b:9")}}
		if withOutcome {
			result.Outcome = outcome
		}

		_, err := evt.NormalizeCommit(result)

		var invalid *evt.InvalidCommitError
		require.ErrorAs(t, err, &invalid)
	}

	// An absent ID is tolerated without an outcome (as before v0.9) and refused with one, because
	// the receipt names events by ID.
	_, err := evt.NormalizeCommit(evt.SerializedResult{Events: []evt.SerializedEvent{event("")}})
	require.NoError(t, err)

	_, err = evt.NormalizeCommit(evt.SerializedResult{Events: []evt.SerializedEvent{event("")}, Outcome: outcome})
	var invalid *evt.InvalidCommitError
	require.ErrorAs(t, err, &invalid)
}
