package compat

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/photon-grove/evt"
)

// OutcomeFixtureFile holds the shared C3/C4 vectors: identity digests, canonical digest bytes,
// and the exact encodings of outcome rows.
const OutcomeFixtureFile = "outcomes.json"

// OutcomeFixtures pins the identity digest and the outcome row in every language. JSON texts are
// strings so a consumer compares bytes, not parsed values.
type OutcomeFixtures struct {
	// Limits are the bounds checked before any write.
	Limits OutcomeLimits `json:"limits"`

	Digests []DigestVector `json:"digests"`
	Rows    []RowVector    `json:"rows"`

	// MalformedRows are outcome items no reader may replay: each must fail to decode in every
	// language as a corrupt record.
	MalformedRows []MalformedRow `json:"malformedRows"`
}

// MalformedRow is a stored outcome item that is not a valid outcome, with the reason.
type MalformedRow struct {
	Name       string         `json:"name"`
	DynamoItem map[string]any `json:"dynamoItem"`
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
	Streams     []string `json:"streams"`
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
	}

	payload := func(text string) *string { return &text }

	digests := []struct {
		name  string
		input evt.DigestInput
	}{
		{"single stream", evt.DigestInput{CommandType: "account:open", Streams: []evt.EntityID{"acct-1"}, Payload: []byte(`{"owner":"Ada"}`), Principal: "user:42"}},
		{"multi stream, sorted", evt.DigestInput{CommandType: "claim-username", Streams: []evt.EntityID{"profile#u-1", "claim#ada"}, Payload: []byte(`{"handle":"ada"}`)}},
		{"multi stream with principal", evt.DigestInput{CommandType: "claim-username", Streams: []evt.EntityID{"claim#ada", "profile#u-1"}, Payload: []byte(`{"handle":"ada"}`), Principal: "user:42"}},
		{"absent payload", evt.DigestInput{CommandType: "account:ping", Streams: []evt.EntityID{"acct-1"}, Principal: "user:42"}},
		{"stale fence", evt.DigestInput{CommandType: "account:rename", Streams: []evt.EntityID{"acct-1"}, Payload: []byte(`{"name":"Grace"}`), Principal: "user:42"}},
		{"duplicate streams collapse", evt.DigestInput{CommandType: "t", Streams: []evt.EntityID{"b", "a", "b"}, Payload: []byte(`null`)}},
		{"outcome-only, no streams", evt.DigestInput{CommandType: "account:audit", Payload: []byte(`{"note":"checked"}`)}},
		{"non-ASCII and HTML-sensitive payload", evt.DigestInput{CommandType: "note:add", Streams: []evt.EntityID{"naïve#1"}, Payload: []byte(`{"text":"a<b> & é"}`), Principal: "user:é"}},
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
				Streams:     make([]string, 0, len(test.input.Streams)),
				Principal:   test.input.Principal,
			},
		}

		for _, stream := range test.input.Streams {
			vector.Input.Streams = append(vector.Input.Streams, string(stream))
		}

		if test.input.Payload != nil {
			vector.Input.Payload = payload(string(test.input.Payload))
		}

		fixtures.Digests = append(fixtures.Digests, vector)
	}

	rows := []struct {
		name    string
		outcome evt.Outcome
	}{
		{"accepted with events and response", evt.Outcome{
			CommandID: "cmd-0001", Status: evt.OutcomeAccepted,
			Digest:   "08f63bf06ef7167f79dda7f5de9307e1ffb626de015ecbc1c1482d1e67e5abd6",
			Response: []byte(`{"accountId":"acct-1","balanceCents":0}`), EventIDs: []evt.EventID{"acct-1:1"},
		}},
		{"accepted multi-stream", evt.Outcome{
			CommandID: "cmd-0004", Status: evt.OutcomeAccepted,
			Digest:   "56d01f5b9788b60e12a686e40807c28d9dfde802d82faf46888adda0acee7050",
			EventIDs: []evt.EventID{"claim#ada:1", "profile#u-1:1"},
		}},
		{"accepted no-op", evt.Outcome{
			CommandID: "cmd-0003", Status: evt.OutcomeAccepted,
			Digest: "d24c7d5ce955a76c936a83e6eca2c3bc277d936c88a76460ed2d221e285a215a",
		}},
		{"accepted binary response", evt.Outcome{
			CommandID: "cmd-0006", Status: evt.OutcomeAccepted, Digest: "d",
			Response: []byte{0, 1, 2, 253, 254, 255}, EventIDs: []evt.EventID{"a:1", "a:2"},
		}},
		{"rejected", evt.Outcome{
			CommandID: "cmd-0002", Status: evt.OutcomeRejected,
			Digest: "21c04bf818ab67db244dc184ae98197928309a4b3a3533acba69dacf992b6d79",
			Rejection: &evt.Rejection{
				Code: "username-taken", Message: "handle ada is taken", Details: map[string]any{"handle": "ada"},
			},
		}},
		{"rejected stale fence", evt.Outcome{
			CommandID: "cmd-0005", Status: evt.OutcomeRejected,
			Digest: "ba4a6a859dcf103a00261515d7e309b31e8f07ca21732a4b7872b7fdebb92c1b",
			Rejection: &evt.Rejection{
				Code: evt.RejectionStaleFence, Message: "a caller-stamped fence went stale",
			},
		}},
		{"rejected with escaped and nested details", evt.Outcome{
			CommandID: "cmd-0007", Status: evt.OutcomeRejected, Digest: "d",
			Rejection: &evt.Rejection{
				Code: "invalid", Message: "a<b>&c é  ",
				Details: map[string]any{"z": []any{1, "two", nil}, "a": map[string]any{"n": 10, "m": 2}},
			},
		}},
	}

	for _, test := range rows {
		vector, err := rowVector(test.name, test.outcome)
		if err != nil {
			return fixtures, fmt.Errorf("%s: %w", test.name, err)
		}

		fixtures.Rows = append(fixtures.Rows, vector)
	}

	fixtures.MalformedRows = malformedRows()

	return fixtures, nil
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
		{"rejected with events", accepted(map[string]any{"status": text("rejected"), "rejection": rejection, "code": text("c")})},
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
