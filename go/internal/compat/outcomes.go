package compat

import (
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/snapshots"
)

// OutcomeFixtureFile holds the shared C3/C4 vectors: identity digests, canonical digest bytes,
// and the exact encodings of outcome rows.
const OutcomeFixtureFile = "outcomes.json"

// OutcomeFixtures pins the identity digest and the outcome row in every language. JSON texts are
// strings so a consumer compares bytes, not parsed values.
type OutcomeFixtures struct {
	// Limits are the bounds checked before any write.
	Limits OutcomeLimits `json:"limits"`

	// Retry are the fence-aware retry policy defaults (C5), which both languages ship.
	Retry RetryDefaults `json:"retry"`

	Digests []DigestVector `json:"digests"`
	Rows    []RowVector    `json:"rows"`

	// MalformedRows are outcome items no reader may replay: each must fail to decode in every
	// language as a corrupt record.
	MalformedRows []MalformedRow `json:"malformedRows"`

	// IdentityBounds pin the recorded identity's bound, escapes counted.
	IdentityBounds []IdentityBoundVector `json:"identityBounds"`

	// RejectionNumbers pin the canonical text of the numbers in a stored rejection.
	RejectionNumbers []RejectionNumberVector `json:"rejectionNumbers"`
}

// RejectionNumberVector is a rejection as an encoder may write it, before its numbers are made
// canonical, and either the stored text every language writes for it or the error every language
// refuses it with: "invalid-argument" for a number PostgreSQL's numeric type cannot hold,
// "outcome-too-large" for a canonical form over the outcome bound.
type RejectionNumberVector struct {
	Name   string `json:"name"`
	Input  string `json:"input"`
	Stored string `json:"stored,omitempty"`
	Error  string `json:"error,omitempty"`
}

// MalformedRow is a stored outcome item that is not a valid outcome, with the reason.
type MalformedRow struct {
	Name       string         `json:"name"`
	DynamoItem map[string]any `json:"dynamoItem"`
}

// RetryDefaults mirrors the default fence-aware retry policy: attempts in total, and the delay
// before the first retry and the upper bound for any delay, in milliseconds.
type RetryDefaults struct {
	MaxAttempts int    `json:"maxAttempts"`
	BaseDelayMs int    `json:"baseDelayMs"`
	MaxDelayMs  int    `json:"maxDelayMs"`
	StaleCode   string `json:"staleCode"`
}

// OutcomeLimits mirrors evt.MaxCommandIDBytes and evt.MaxOutcomeBytes.
type OutcomeLimits struct {
	MaxCommandIDBytes int `json:"maxCommandIdBytes"`
	MaxOutcomeBytes   int `json:"maxOutcomeBytes"`
}

// DigestInputVector is a command identity. Payload is the canonical JSON text, null for a command
// with no payload.
type DigestInputVector struct {
	CommandType string   `json:"commandType"`
	Targets     []string `json:"targets"`
	Payload     *string  `json:"payload"`
	Principal   string   `json:"principal"`
}

// DigestVector is one identity, its canonical bytes (hex), and its digest.
type DigestVector struct {
	Name         string            `json:"name"`
	Input        DigestInputVector `json:"input"`
	CanonicalHex string            `json:"canonicalHex"`
	Digest       string            `json:"digest"`
}

// OutcomeInputVector is the outcome a commit writes. Response is the raw text of the response
// bytes (UTF-8), Rejection the rejection's canonical JSON text, and EventIDs already in canonical
// order.
type OutcomeInputVector struct {
	CommandID string   `json:"commandId"`
	Status    string   `json:"status"`
	Digest    string   `json:"digest"`
	Response  *string  `json:"response"`
	EventIDs  []string `json:"eventIds"`
	Rejection *string  `json:"rejection"`

	// Identity is the recorded identity, null for a row written before outcomes recorded it.
	Identity *IdentityVector `json:"identity"`
}

// IdentityVector is an outcome's recorded identity: targets already deduplicated and sorted, and
// Principal null when the command had none.
type IdentityVector struct {
	CommandType string   `json:"commandType"`
	Targets     []string `json:"targets"`
	Principal   *string  `json:"principal"`
}

// RowVector is one outcome and every encoding of it: the DynamoDB item, the item's budget size,
// and the PostgreSQL row fields (response as base64 of the bytea, events and rejection as text).
type RowVector struct {
	Name         string             `json:"name"`
	Input        OutcomeInputVector `json:"input"`
	EventsJSON   string             `json:"eventsJson,omitempty"`
	RejectionRaw string             `json:"rejectionJson,omitempty"`
	DynamoItem   map[string]any     `json:"dynamoItem"`
	ItemSize     int                `json:"itemSize"`
	PostgresRow  map[string]any     `json:"postgresRow"`
}

// GenerateOutcomeFixtures builds the C3/C4 vectors from the Go implementation.
func GenerateOutcomeFixtures() (OutcomeFixtures, error) {
	fixtures := OutcomeFixtures{
		Limits: OutcomeLimits{MaxCommandIDBytes: evt.MaxCommandIDBytes, MaxOutcomeBytes: evt.MaxOutcomeBytes},
		Retry: RetryDefaults{
			MaxAttempts: snapshots.DefaultRetryAttempts,
			BaseDelayMs: int(snapshots.DefaultRetryBaseDelay / time.Millisecond),
			MaxDelayMs:  int(snapshots.DefaultRetryMaxDelay / time.Millisecond),
			StaleCode:   string(evt.RejectionStaleFence),
		},
	}

	payload := func(text string) *string { return &text }

	digests := []struct {
		name  string
		input evt.DigestInput
	}{
		{"single stream", evt.DigestInput{CommandType: "account:open", Targets: []evt.EntityID{"acct-1"}, Payload: []byte(`{"owner":"Ada"}`), Principal: "user:42"}},
		{"multi stream, sorted", evt.DigestInput{CommandType: "claim-username", Targets: []evt.EntityID{"profile#u-1", "claim#ada"}, Payload: []byte(`{"handle":"ada"}`)}},
		{"multi stream with principal", evt.DigestInput{CommandType: "claim-username", Targets: []evt.EntityID{"claim#ada", "profile#u-1"}, Payload: []byte(`{"handle":"ada"}`), Principal: "user:42"}},
		// A rename releases whichever handle the profile currently holds, so the streams it
		// writes (the released claim, the new claim, the profile) come from state. Only what the
		// request names is identity: the same digest whichever write set an attempt derives.
		{"rename, state-derived write set is not identity", evt.DigestInput{CommandType: "profile:rename", Targets: []evt.EntityID{"profile#u-1"}, Payload: []byte(`{"handle":"grace"}`), Principal: "user:42"}},
		{"absent payload", evt.DigestInput{CommandType: "account:ping", Targets: []evt.EntityID{"acct-1"}, Principal: "user:42"}},
		{"stale fence", evt.DigestInput{CommandType: "account:rename", Targets: []evt.EntityID{"acct-1"}, Payload: []byte(`{"name":"Grace"}`), Principal: "user:42"}},
		{"duplicate streams collapse", evt.DigestInput{CommandType: "t", Targets: []evt.EntityID{"b", "a", "b"}, Payload: []byte(`null`)}},
		{"outcome-only, no streams", evt.DigestInput{CommandType: "account:audit", Payload: []byte(`{"note":"checked"}`)}},
		{"non-ASCII and HTML-sensitive payload", evt.DigestInput{CommandType: "note:add", Targets: []evt.EntityID{"naïve#1"}, Payload: []byte(`{"text":"a<b> & é"}`), Principal: "user:é"}},
	}

	for _, test := range digests {
		digest, err := evt.DefaultDigest(test.input)
		if err != nil {
			return fixtures, err
		}

		vector := DigestVector{
			Name:         test.name,
			CanonicalHex: hex.EncodeToString(evt.CanonicalDigestBytes(test.input)),
			Digest:       digest,
			Input: DigestInputVector{
				CommandType: string(test.input.CommandType),
				Targets:     make([]string, 0, len(test.input.Targets)),
				Principal:   test.input.Principal,
			},
		}

		for _, stream := range test.input.Targets {
			vector.Input.Targets = append(vector.Input.Targets, string(stream))
		}

		if test.input.Payload != nil {
			vector.Input.Payload = payload(string(test.input.Payload))
		}

		fixtures.Digests = append(fixtures.Digests, vector)
	}

	// The rename command's digest is computed, not copied: it is the digest vector above.
	renameDigest, err := evt.DefaultDigest(evt.DigestInput{
		CommandType: "profile:rename", Targets: []evt.EntityID{"profile#u-1"}, Payload: []byte(`{"handle":"grace"}`), Principal: "user:42",
	})
	if err != nil {
		return fixtures, err
	}

	callerFence, exhausted := evt.CallerFenceStaleRejection(), evt.RetriesExhaustedRejection(5)

	// identity is what a row records for a digest vector's input, so the two stay in step.
	identity := func(name string) *evt.RecordedIdentity {
		for _, digest := range digests {
			if digest.name == name {
				return evt.RecordedIdentityOf(digest.input)
			}
		}

		panic("no digest vector " + name)
	}

	auditDigest, err := evt.DefaultDigest(evt.DigestInput{
		CommandType: "account:audit", Payload: []byte(`{"note":"checked"}`),
	})
	if err != nil {
		return fixtures, err
	}

	rows := []struct {
		name    string
		outcome evt.Outcome
	}{
		{"accepted with events and response", evt.Outcome{
			CommandID: "cmd-0001", Status: evt.OutcomeAccepted,
			Identity: identity("single stream"),
			Digest:   "08f63bf06ef7167f79dda7f5de9307e1ffb626de015ecbc1c1482d1e67e5abd6",
			Response: []byte(`{"accountId":"acct-1","balanceCents":0}`), EventIDs: []evt.EventID{"acct-1:1"},
		}},
		{"accepted multi-stream", evt.Outcome{
			CommandID: "cmd-0004", Status: evt.OutcomeAccepted,
			Identity: identity("multi stream with principal"),
			Digest:   "56d01f5b9788b60e12a686e40807c28d9dfde802d82faf46888adda0acee7050",
			EventIDs: []evt.EventID{"claim#ada:1", "profile#u-1:1"},
		}},
		{"accepted no-op", evt.Outcome{
			CommandID: "cmd-0003", Status: evt.OutcomeAccepted,
			Identity: identity("absent payload"),
			Digest:   "d24c7d5ce955a76c936a83e6eca2c3bc277d936c88a76460ed2d221e285a215a",
		}},
		{"accepted binary response", evt.Outcome{
			CommandID: "cmd-0006", Status: evt.OutcomeAccepted, Digest: "d",
			Response: []byte{0, 1, 2, 253, 254, 255}, EventIDs: []evt.EventID{"a:1", "a:2"},
		}},
		{"rejected", evt.Outcome{
			CommandID: "cmd-0002", Status: evt.OutcomeRejected,
			Identity: identity("multi stream, sorted"),
			Digest:   "21c04bf818ab67db244dc184ae98197928309a4b3a3533acba69dacf992b6d79",
			Rejection: &evt.Rejection{
				Code: "username-taken", Message: "handle ada is taken", Details: map[string]any{"handle": "ada"},
			},
		}},
		{"rejected stale fence", evt.Outcome{
			CommandID: "cmd-0005", Status: evt.OutcomeRejected,
			Identity:  identity("stale fence"),
			Digest:    "ba4a6a859dcf103a00261515d7e309b31e8f07ca21732a4b7872b7fdebb92c1b",
			Rejection: &callerFence,
		}},
		{"accepted rename, state-derived write set", evt.Outcome{
			CommandID: "cmd-0008", Status: evt.OutcomeAccepted, Digest: renameDigest,
			Identity: identity("rename, state-derived write set is not identity"),
			EventIDs: []evt.EventID{"claim#ada:2", "claim#grace:1", "profile#u-1:2"},
		}},
		// Identities without a principal or a target, and with non-ASCII text in every field.
		{"accepted outcome-only, recorded identity without principal or targets", evt.Outcome{
			CommandID: "cmd-0012", Status: evt.OutcomeAccepted,
			Digest:   auditDigest,
			Identity: identity("outcome-only, no streams"),
		}},
		{"accepted, non-ASCII recorded identity", evt.Outcome{
			CommandID: "cmd-0013", Status: evt.OutcomeAccepted, Digest: "d",
			Identity: identity("non-ASCII and HTML-sensitive payload"),
		}},
		// The targets JSON escapes <, >, & and U+2028 exactly as the shared JSON rules do.
		{"accepted, HTML-sensitive recorded identity", evt.Outcome{
			CommandID: "cmd-0014", Status: evt.OutcomeAccepted, Digest: "d",
			Identity: evt.RecordedIdentityOf(evt.DigestInput{
				CommandType: "note:add<é>",
				Targets:     []evt.EntityID{"x\u2028y", "a<b>&c", "naïve#1"},
				Principal:   "user:<é>",
			}),
		}},
		{"rejected stale fence, retries exhausted", evt.Outcome{
			CommandID: "cmd-0009", Status: evt.OutcomeRejected, Digest: renameDigest, Rejection: &exhausted,
			Identity: identity("rename, state-derived write set is not identity"),
		}},
		// A rejection recorded with its failure events (RejectWithEvents): the same row as an
		// accepted outcome's, with the rejection and code attributes beside the events.
		{"rejected with events", evt.Outcome{
			CommandID: "cmd-0010", Status: evt.OutcomeRejected,
			Identity: identity("multi stream, sorted"),
			Digest:   "21c04bf818ab67db244dc184ae98197928309a4b3a3533acba69dacf992b6d79",
			Rejection: &evt.Rejection{
				Code: "apply-conflict", Message: "the proposal was decided on a stale base", Details: map[string]any{"base": 3},
			},
			EventIDs: []evt.EventID{"proposal#p-1:4", "proposal#p-1:5"},
		}},
		{"rejected with escaped and nested details", evt.Outcome{
			CommandID: "cmd-0007", Status: evt.OutcomeRejected, Digest: "d",
			Rejection: &evt.Rejection{
				Code: "invalid", Message: "a<b>&c é  ",
				Details: map[string]any{"z": []any{1, "two", nil}, "a": map[string]any{"n": 10, "m": 2}},
			},
		}},
		{"rejected with float details", evt.Outcome{
			CommandID: "cmd-0008", Status: evt.OutcomeRejected, Digest: "d",
			Rejection: &evt.Rejection{
				Code: "limits", Message: "numbers Go writes with an exponent or a signed zero",
				Details: map[string]any{"big": 1e21, "huge": 6.805598656397509e25, "tiny": 1.5e-7, "negzero": math.Copysign(0, -1), "half": 2.5},
			},
		}},
	}

	for _, test := range rows {
		vector, rowErr := rowVector(test.name, test.outcome)
		if rowErr != nil {
			return fixtures, fmt.Errorf("%s: %w", test.name, rowErr)
		}

		fixtures.Rows = append(fixtures.Rows, vector)
	}

	fixtures.MalformedRows = malformedRows()

	if fixtures.IdentityBounds, err = identityBounds(); err != nil {
		return fixtures, err
	}

	numbers, err := rejectionNumbers()
	if err != nil {
		return fixtures, err
	}

	fixtures.RejectionNumbers = numbers

	return fixtures, nil
}

// IdentityBoundVector is a recorded identity at the edge of its bound: Size is the bytes the bound
// counts (the command type, the principal, and the encoded targets, escapes included), and Error is
// "outcome-too-large" when CheckOutcome refuses it, empty when it passes.
type IdentityBoundVector struct {
	Name     string         `json:"name"`
	Identity IdentityVector `json:"identity"`
	Size     int            `json:"size"`
	Error    string         `json:"error,omitempty"`
}

// identityBounds pins the bound at exactly MaxOutcomeBytes and one byte past it, with a thousand
// < characters whose escapes (\u003c, six bytes each) count toward it.
func identityBounds() ([]IdentityBoundVector, error) {
	vectors := make([]IdentityBoundVector, 0, 2)

	for _, test := range []struct {
		name  string
		extra int
	}{{"at the bound", 0}, {"one byte past the bound", 1}} {
		// "t" + `["` + 1000 × \u003c + filler + `"]` = 1 + 4 + 6000 + filler.
		target := strings.Repeat("<", 1000) + strings.Repeat("a", evt.MaxOutcomeBytes-6005+test.extra)
		identity := &evt.RecordedIdentity{Type: "t", Targets: []evt.EntityID{evt.EntityID(target)}}

		encoded, err := identity.EncodeTargets()
		if err != nil {
			return nil, err
		}

		vector := IdentityBoundVector{
			Name:     test.name,
			Identity: IdentityVector{CommandType: "t", Targets: []string{target}},
			Size:     len(identity.Type) + len(encoded),
		}

		err = evt.CheckOutcome(&evt.Outcome{CommandID: "c", Status: evt.OutcomeAccepted, Digest: "d", Identity: identity})

		var tooLarge *evt.OutcomeTooLargeError

		switch {
		case err == nil:
		case errors.As(err, &tooLarge) && tooLarge.What == "identity":
			vector.Error = "outcome-too-large"
		default:
			return nil, fmt.Errorf("%s: %w", test.name, err)
		}

		vectors = append(vectors, vector)
	}

	return vectors, nil
}

// rejectionNumbers builds the canonical-number vectors from literal inputs, which DecodeRejection
// keeps verbatim, so each one exercises the exact text an encoder could have written.
func rejectionNumbers() ([]RejectionNumberVector, error) {
	details := func(body string) string { return `{"code":"c","message":"m","details":{` + body + `}}` }

	inputs := []struct{ name, input string }{
		{"exponent forms", details(`"bare":1e21,"big":1e+21,"down":12.5e-1,"negative":-1.5e3,"scaled":1.50e1,"shifted":0.0010e2,"tiny":1.5e-7,"upper":1E2,"zero":0e5`)},
		{"signed zeros", details(`"exp":-0.000e2,"frac":-0.0,"int":-0`)},
		{"already canonical", details(`"frac":100.00,"int":123,"negative":-7,"precise":9007199254740993`)},
		{"nested", details(`"a":[1e2,{"b":-0e0}],"c":{"d":[[2.5e1]]}`)},
		{"number-like strings and keys", `{"code":"1e5","message":"-0 \"2e3\" \\","details":{"1e5":"-0.0e1","n":-0}}`},
		{"zero with a huge exponent", details(`"n":0e99999999999`)},
		{"scale at the limit", details(`"n":1.000e-16380`)},
		{"huge exponent", details(`"n":1e99999999999`)},
		{"huge negative exponent", details(`"n":1e-99999999999`)},
		{"scale past the limit", details(`"n":1e-16384`)},
		{"zero scale past the limit", details(`"n":0.0e-16383`)},
		{"integer digits past the limit", details(`"n":1e131072`)},
		{"integer digits at the limit", details(`"n":1e131071`)},
		{"expansion past the outcome bound", details(`"n":1e65536`)},
	}

	vectors := make([]RejectionNumberVector, 0, len(inputs))

	for _, test := range inputs {
		rejection, err := evt.DecodeRejection([]byte(test.input))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", test.name, err)
		}

		vector := RejectionNumberVector{Name: test.name, Input: test.input}

		stored, err := evt.EncodeRejection(rejection)

		var tooLarge *evt.OutcomeTooLargeError

		switch {
		case err == nil:
			vector.Stored = string(stored)

		case errors.Is(err, evt.ErrInvalidArgument):
			vector.Error = "invalid-argument"

		case errors.As(err, &tooLarge):
			vector.Error = "outcome-too-large"

		default:
			return nil, fmt.Errorf("%s: %w", test.name, err)
		}

		vectors = append(vectors, vector)
	}

	return vectors, nil
}

// malformedRows builds items that violate one rule of a valid outcome each, starting from a valid
// accepted row and a valid rejected row.
func malformedRows() []MalformedRow {
	text := func(value string) map[string]any { return map[string]any{"S": value} }

	accepted := func(overrides map[string]any) map[string]any {
		item := map[string]any{
			"pk": text("command-outcome#bad"), "sk": map[string]any{"N": "1"},
			"status": text("accepted"), "digest": text("digest"), "events": text(`["a:1"]`),
		}
		for name, value := range overrides {
			if value == nil {
				delete(item, name)
			} else {
				item[name] = value
			}
		}

		return item
	}

	rejection := text(`{"code":"c","message":"m","details":null}`)

	rejectedRow := func(stored map[string]any) map[string]any {
		return accepted(map[string]any{"status": text("rejected"), "events": nil, "code": text("c"), "rejection": stored})
	}

	return []MalformedRow{
		{"unknown status", accepted(map[string]any{"status": text("pending")})},
		{"missing status", accepted(map[string]any{"status": nil})},
		{"missing digest", accepted(map[string]any{"digest": nil})},
		{"empty digest", accepted(map[string]any{"digest": text("")})},
		{"accepted with a rejection", accepted(map[string]any{"rejection": rejection, "code": text("c")})},
		{"rejected without a rejection", accepted(map[string]any{"status": text("rejected"), "events": nil})},
		{"rejected with an empty code", accepted(map[string]any{
			"status": text("rejected"), "events": nil, "code": text(""),
			"rejection": text(`{"code":"","message":"m","details":null}`),
		})},
		{"rejected with a malformed event ID", accepted(map[string]any{
			"status": text("rejected"), "events": text(`["a:0"]`), "rejection": rejection, "code": text("c"),
		})},
		{"rejected whose code attribute disagrees", accepted(map[string]any{
			"status": text("rejected"), "events": nil, "rejection": rejection, "code": text("other"),
		})},
		{"event ID without a sequence", accepted(map[string]any{"events": text(`["a"]`)})},
		{"event ID with a zero sequence", accepted(map[string]any{"events": text(`["a:0"]`)})},
		{"event ID with a non-canonical sequence", accepted(map[string]any{"events": text(`["a:01"]`)})},
		{"event ID without an entity", accepted(map[string]any{"events": text(`[":1"]`)})},
		{"events that are not an array of strings", accepted(map[string]any{"events": text(`{"a":1}`)})},
		{"response that is not base64", accepted(map[string]any{"response": text("not base64!")})},
		{"status stored as a number", accepted(map[string]any{"status": map[string]any{"N": "1"}})},
		{"digest stored as a number", accepted(map[string]any{"digest": map[string]any{"N": "7"}})},
		{"events stored as a number", accepted(map[string]any{"events": map[string]any{"N": "1"}})},
		{"response stored as binary", accepted(map[string]any{"response": map[string]any{"B": "eA=="}})},
		{"rejection stored as binary", accepted(map[string]any{
			"status": text("rejected"), "events": nil, "code": text("c"), "rejection": map[string]any{"B": "eA=="},
		})},
		{"accepted with a stray code attribute", accepted(map[string]any{"code": text("c")})},
		{"accepted with an empty code attribute", accepted(map[string]any{"code": text("")})},
		{"rejection details that are an array", rejectedRow(text(`{"code":"c","message":"m","details":["x"]}`))},
		{"rejection details that are a string", rejectedRow(text(`{"code":"c","message":"m","details":"x"}`))},
		{"rejection details that are a number", rejectedRow(text(`{"code":"c","message":"m","details":1}`))},
		{"rejection followed by another value", rejectedRow(text(`{"code":"c","message":"m","details":null} {}`))},
		{"rejection followed by garbage", rejectedRow(text(`{"code":"c","message":"m","details":null}x`))},
		{"command type without targets", accepted(map[string]any{"commandType": text("t")})},
		{"targets without a command type", accepted(map[string]any{"targets": text(`["a"]`)})},
		{"principal without a command type", accepted(map[string]any{"principal": text("user:1")})},
		{"targets that are not an array of strings", accepted(map[string]any{"commandType": text("t"), "targets": text(`{"a":1}`)})},
		{"targets that are null", accepted(map[string]any{"commandType": text("t"), "targets": text(`null`)})},
		{"targets stored as a number", accepted(map[string]any{"commandType": text("t"), "targets": map[string]any{"N": "1"}})},
		{"code stored as a number", accepted(map[string]any{
			"status": text("rejected"), "events": nil, "code": map[string]any{"N": "1"},
			"rejection": text(`{"code":"c","message":"m","details":null}`),
		})},
	}
}

func rowVector(name string, outcome evt.Outcome) (RowVector, error) {
	attributes, err := evt.OutcomeAttributes(outcome)
	if err != nil {
		return RowVector{}, err
	}

	item := map[string]types.AttributeValue{}
	for _, attribute := range attributes {
		if attribute.Number {
			item[attribute.Name] = &types.AttributeValueMemberN{Value: attribute.Value}
		} else {
			item[attribute.Name] = &types.AttributeValueMemberS{Value: attribute.Value}
		}
	}

	size, err := evt.OutcomeItemSize(outcome, eventsOf(outcome))
	if err != nil {
		return RowVector{}, err
	}

	vector := RowVector{
		Name: name,
		Input: OutcomeInputVector{
			CommandID: string(outcome.CommandID),
			Status:    string(outcome.Status),
			Digest:    outcome.Digest,
			EventIDs:  make([]string, 0, len(outcome.EventIDs)),
		},
		DynamoItem: DynamoItemJSON(item),
		ItemSize:   size,
	}

	for _, id := range outcome.EventIDs {
		vector.Input.EventIDs = append(vector.Input.EventIDs, string(id))
	}

	pg := map[string]any{
		"command_id": string(outcome.CommandID),
		"status":     string(outcome.Status),
		"digest":     outcome.Digest,
		"response":   nil,
		"events":     "[]",
		"rejection":  nil,
		"code":       nil,
	}

	if len(outcome.Response) > 0 {
		text := string(outcome.Response)
		vector.Input.Response = &text
		pg["response"] = base64.StdEncoding.EncodeToString(outcome.Response)
	}

	if len(outcome.EventIDs) > 0 {
		events, err := evt.EncodeEventIDs(outcome.EventIDs)
		if err != nil {
			return RowVector{}, err
		}

		vector.EventsJSON = events
		pg["events"] = events
	}

	if outcome.Rejection != nil {
		rejection, err := evt.EncodeRejection(*outcome.Rejection)
		if err != nil {
			return RowVector{}, err
		}

		text := string(rejection)
		vector.Input.Rejection = &text
		vector.RejectionRaw = text
		pg["rejection"] = text
		pg["code"] = string(outcome.Rejection.Code)
	}

	pg["command_type"], pg["targets"], pg["principal"] = nil, nil, nil

	if outcome.Identity != nil {
		targets, err := outcome.Identity.EncodeTargets()
		if err != nil {
			return RowVector{}, err
		}

		input := &IdentityVector{CommandType: string(outcome.Identity.Type), Targets: make([]string, 0, len(outcome.Identity.Targets))}
		for _, target := range outcome.Identity.Targets {
			input.Targets = append(input.Targets, string(target))
		}

		pg["command_type"], pg["targets"] = string(outcome.Identity.Type), targets

		if outcome.Identity.Principal != nil {
			principal := string(*outcome.Identity.Principal)
			input.Principal = &principal
			pg["principal"] = principal
		}

		vector.Input.Identity = input
	}

	vector.PostgresRow = pg

	return vector, nil
}

// eventsOf builds the event list OutcomeItemSize expects from an outcome's event IDs.
func eventsOf(outcome evt.Outcome) []evt.SerializedEvent {
	events := make([]evt.SerializedEvent, 0, len(outcome.EventIDs))
	for _, id := range outcome.EventIDs {
		events = append(events, evt.SerializedEvent{ID: id})
	}

	return events
}
