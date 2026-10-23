package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"go.opentelemetry.io/otel/propagation"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/dynamo"
	"github.com/photon-grove/evt/go/internal/eventjson"
)

type namedEvent struct {
	name  string
	event evt.SerializedEvent
}

type namedSnapshot struct {
	name     string
	snapshot evt.SerializedSnapshot
}

func ptr[T any](value T) *T {
	return &value
}

func codecEvent(id evt.EntityID, sequence evt.EventSequence, eventType evt.EventType, version evt.EventVersion,
	payload string, metadata evt.Metadata,
) evt.SerializedEvent {
	return evt.SerializedEvent{
		ID:         evt.GetEventID(id, sequence),
		Sequence:   sequence,
		Type:       eventType,
		Version:    version,
		EntityID:   id,
		EntityType: AccountType,
		Payload:    []byte(payload),
		Metadata:   metadata,
	}
}

// codecEvents are the encoding edge cases pinned by compat/fixtures/codec.json.
func codecEvents() []namedEvent {
	base := evt.Metadata{Region: Region, Timestamp: DefaultTimestamp}

	// full carries the storage format's metadata example: the causationId is unset and defaults to
	// the commandId at encode time.
	full := evt.Metadata{
		CommandID: ptr(evt.CommandID("cmd-0001")),
		Trace: &propagation.MapCarrier{
			"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			"tracestate":  "vendor=value",
			"baggage":     "tenant=example",
		},
		Origin:      &evt.Origin{Source: "billing-api", Endpoint: "POST /accounts"},
		Address:     ptr("203.0.113.7"),
		Region:      Region,
		Timestamp:   DefaultTimestamp,
		PrincipalID: ptr(evt.PrincipalID("user:42")),
		Extensions:  map[string]string{"requestSource": "cli"},
	}

	// explicitCausation keeps its own causationId instead of defaulting to the commandId, and pins
	// the extension-key sort order.
	explicitCausation := evt.Metadata{
		CommandID:   ptr(evt.CommandID("cmd-0002")),
		CausationID: ptr(evt.CommandID("cmd-0001")),
		Region:      Region,
		Timestamp:   DefaultTimestamp,
		Extensions:  map[string]string{"requestSource": "cli", "tenant": "example", "attempt": "2"},
	}

	escaping := evt.Metadata{
		CommandID:   ptr(evt.CommandID("cmd <&> \u2028 \u2029")),
		Origin:      &evt.Origin{Source: "<script>alert('x')</script>", Endpoint: "tab\tnew\nline \"quote\" \\ \u0001 \u007f"},
		Address:     ptr("é ü 漢字 🚀"),
		Region:      Region,
		Timestamp:   DefaultTimestamp,
		PrincipalID: ptr(evt.PrincipalID("user<&> \u2028")),
		CausationID: ptr(evt.CommandID("cmd\t\"quoted\"")),
		Extensions:  map[string]string{"<k>&ey": "v<&> \u2028", "é": "漢字"},
	}

	empty := evt.Metadata{
		CommandID: ptr(evt.CommandID("")),
		Trace:     &propagation.MapCarrier{},
		Origin:    &evt.Origin{},
	}

	traceOnly := evt.Metadata{
		Trace:     &propagation.MapCarrier{"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
		Region:    Region,
		Timestamp: DefaultTimestamp,
	}

	return []namedEvent{
		{"minimal", codecEvent("acct-1", 1, OpenedEvent, 1,
			`{"owner":"Ada","openedAt":"2026-01-02T03:04:05.123Z"}`, base)},
		{"full-metadata", codecEvent("acct-1", 2, DepositedEvent, 1,
			`{"amountCents":2500,"memo":"rent"}`, full)},
		{"explicit-causation", codecEvent("acct-1", 3, DepositedEvent, 1,
			`{"amountCents":100}`, explicitCausation)},
		{"escaping", codecEvent("acct-1", 4, DepositedEvent, 1,
			"{\"amountCents\":1,\"memo\":\"Coffee & <cake> \u2028 \u2029 \u2615 \\\"q\\\" \\\\ \\t\"}", escaping)},
		{"empty-values", codecEvent("acct-2", 1, ClosedEvent, 1, `{}`, empty)},
		{"trace-without-command", codecEvent("acct-2", 2, ClosedEvent, 1, `{"reason":null}`, traceOnly)},
		{"large-numbers", codecEvent("acct-3", 9007199254740993, RenamedEvent, 12,
			`{"owner":"Grace"}`, base)},
		{"unicode-entity-id", codecEvent("konto-ø-漢-1", 1, TaggedEvent, 1,
			`{"tags":["vip","ü"]}`, base)},
		{"raw-payload-formatting", codecEvent("acct-4", 1, LabeledEvent, 1,
			"{ \"labels\" : { \"b\": \"2\", \"a\": \"1\" } }\n", base)},
	}
}

func codecSnapshots() []namedSnapshot {
	return []namedSnapshot{
		{"basic", evt.SerializedSnapshot{
			EntityType:    AccountType,
			EntityID:      "acct-1",
			Sequence:      2,
			EventSequence: 10,
			Payload:       []byte(`{"owner":"Ada","status":"open","balanceCents":1200}`),
		}},
		{"escaping", evt.SerializedSnapshot{
			EntityType:    AccountType,
			EntityID:      "konto-ø-1",
			Sequence:      1,
			EventSequence: 3,
			Payload:       []byte("{\"lastMemo\":\"<b> & \u2028\"}"),
		}},
	}
}

var metadataDecodeInputs = []struct{ name, input string }{
	{"empty-object", `{}`},
	{"nulls", `{"commandId":null,"trace":null,"origin":null,"address":null,"region":null,"timestamp":null}`},
	{"unknown-fields-ignored", `{"commandId":"c-1","unknown":true,"origin":{"source":"s","extra":1}}`},
	{"trace-keys-sorted", `{"trace":{"b":"2","a":"1","c":"3"},"region":"eu-west-1"}`},
	{"empty-address-kept", `{"address":"","origin":{"source":null,"endpoint":"e"}}`},
	{"whitespace-and-escapes", "{ \"region\" : \"us\\u002dwest\\u002d2\", \"timestamp\":\"t\" }"},
	{"null-trace-value", `{"trace":{"traceparent":null,"tracestate":"a"}}`},
	// The v0.9 keys (C6). A pre-v0.9 object without them decodes leniently and re-encodes with the
	// defaults; causationId re-encodes as the commandId when it was not stored.
	{"v09-principal-causation-extensions", `{"commandId":"c-1","principalId":"user:7","causationId":"c-0","extensions":{"b":"2","a":"1"},"region":"r"}`},
	{"legacy-without-new-keys", `{"commandId":"c-1","region":"r","timestamp":"t"}`},
	{"extensions-null-value", `{"extensions":{"a":null,"b":"x"}}`},
	{"extensions-empty-object", `{"extensions":{}}`},
	{"extensions-null-map", `{"extensions":null}`},
}

var eventDecodeInputs = []struct{ name, input string }{
	{"null-payload-and-metadata", `{"id":"a:1","sequence":1,"type":"t","version":1,"entityId":"a","entityType":"x","payload":null,"metadata":null}`},
	{"missing-fields", `{"id":"a:2","sequence":2,"payload":"e30=","metadata":{"region":"r"},"extra":"ignored"}`},
	{"empty-payload", `{"id":"a:3","sequence":3,"type":"t","version":2,"entityId":"a","entityType":"x","payload":"","metadata":{}}`},
	{"null-identifiers", `{"id":null,"sequence":null,"type":null,"version":null,"entityId":null,"entityType":null,"payload":"","metadata":{}}`},
}

// GenerateCodecFixtures encodes every codec case through the Go implementation: JSON via
// encoding/json, DynamoDB items via the real repository, and SNS messages via the real publisher.
func GenerateCodecFixtures(ctx context.Context) (CodecFixtures, error) {
	fixtures := CodecFixtures{}

	for _, c := range codecEvents() {
		vector, err := eventVector(ctx, c)
		if err != nil {
			return fixtures, fmt.Errorf("event %s: %w", c.name, err)
		}
		fixtures.Events = append(fixtures.Events, vector)
	}

	for _, c := range codecSnapshots() {
		vector, err := snapshotVector(ctx, c)
		if err != nil {
			return fixtures, fmt.Errorf("snapshot %s: %w", c.name, err)
		}
		fixtures.Snapshots = append(fixtures.Snapshots, vector)
	}

	for _, c := range metadataDecodeInputs {
		var metadata evt.Metadata
		if err := eventjson.UnmarshalMetadata([]byte(c.input), &metadata); err != nil {
			return fixtures, fmt.Errorf("metadata %s: %w", c.name, err)
		}
		canonical, err := eventjson.MarshalMetadata(metadata)
		if err != nil {
			return fixtures, err
		}
		fixtures.MetadataDecode = append(fixtures.MetadataDecode, DecodeVector{c.name, c.input, string(canonical)})
	}

	for _, c := range eventDecodeInputs {
		var event evt.SerializedEvent
		if err := eventjson.UnmarshalEvent([]byte(c.input), &event); err != nil {
			return fixtures, fmt.Errorf("event decode %s: %w", c.name, err)
		}
		canonical, err := eventjson.MarshalEvent(event)
		if err != nil {
			return fixtures, err
		}
		fixtures.EventDecode = append(fixtures.EventDecode, DecodeVector{c.name, c.input, string(canonical)})
	}

	// Go sorts map keys by their encoded string, so integer keys sort as text.
	intKeyMap, err := json.Marshal(map[int64]string{-1: "minus one", 2: "two", 10: "ten"})
	if err != nil {
		return fixtures, err
	}
	fixtures.IntKeyMap = string(intKeyMap)

	ttl, err := ttlVector(ctx)
	if err != nil {
		return fixtures, fmt.Errorf("ttl: %w", err)
	}
	fixtures.TTL = ttl

	view, err := viewVector(ctx)
	if err != nil {
		return fixtures, fmt.Errorf("view: %w", err)
	}
	fixtures.Views = append(fixtures.Views, view)

	for _, interval := range []int{0, 1, 2, 3, 5} {
		for current := int64(0); current <= 11; current++ {
			for count := 0; count <= 6; count++ {
				expected := evt.CalculateAdditionalEvents(evt.EventSequence(current), count, interval)
				fixtures.SnapshotPolicy = append(fixtures.SnapshotPolicy,
					[4]int64{current, int64(count), int64(interval), int64(expected)})
			}
		}
	}

	return fixtures, nil
}

// seedHead declares stream-heads on an empty fake table and stores a stream's head item, the state a
// stream is in after a head-aware writer committed up to head.
func seedHead(
	ctx context.Context,
	fake *FakeDynamo,
	repo *dynamo.Repository,
	entityID evt.EntityID,
	head evt.EventSequence,
) error {
	if _, err := repo.FeatureMarker(nil).DeclareStreamHeads(ctx); err != nil {
		return err
	}

	_, err := fake.PutItem(ctx, &dynamodb.PutItemInput{Item: map[string]types.AttributeValue{
		"pk":   &types.AttributeValueMemberS{Value: string(entityID)},
		"sk":   &types.AttributeValueMemberN{Value: "-1"},
		"head": &types.AttributeValueMemberN{Value: strconv.FormatInt(int64(head), 10)},
	}})

	return err
}

func eventVector(ctx context.Context, c namedEvent) (EventVector, error) {
	serialized, err := eventjson.MarshalEvent(c.event)
	if err != nil {
		return EventVector{}, err
	}

	metadata, err := eventjson.MarshalMetadata(c.event.Metadata)
	if err != nil {
		return EventVector{}, err
	}

	fake := NewFakeDynamo()
	repo := dynamo.NewRepository(fake, fakeTable)

	// A commit must continue from the stored head (ADR 0007), so a vector above sequence 1 is
	// committed onto a stream whose head item already says the previous event is stored.
	if c.event.Sequence > 1 {
		if err = seedHead(ctx, fake, repo, c.event.EntityID, c.event.Sequence-1); err != nil {
			return EventVector{}, err
		}
	}

	err = repo.Commit(ctx, evt.SerializedResult{Events: []evt.SerializedEvent{c.event}})
	if err != nil {
		return EventVector{}, err
	}

	var items []map[string]types.AttributeValue
	for _, item := range fake.Items(string(c.event.EntityID)) {
		if sk, ok := item["sk"].(*types.AttributeValueMemberN); ok && sk.Value == strconv.FormatInt(int64(c.event.Sequence), 10) {
			items = append(items, item)
		}
	}
	if len(items) != 1 {
		return EventVector{}, fmt.Errorf("expected one stored item, got %d", len(items))
	}

	message, attributes, err := SNSMessage(items[0])
	if err != nil {
		return EventVector{}, err
	}

	return EventVector{
		Name: c.name,
		Input: EventInput{
			ID:           c.event.ID,
			Sequence:     c.event.Sequence,
			Type:         c.event.Type,
			Version:      c.event.Version,
			EntityID:     c.event.EntityID,
			EntityType:   c.event.EntityType,
			Payload:      string(c.event.Payload),
			MetadataJSON: string(metadata),
		},
		SerializedEventJSON: string(serialized),
		DynamoItem:          DynamoItemJSON(items[0]),
		SNSMessage:          message,
		SNSAttributes:       attributes,
	}, nil
}

func snapshotVector(ctx context.Context, c namedSnapshot) (SnapshotVector, error) {
	serialized, err := json.Marshal(c.snapshot)
	if err != nil {
		return SnapshotVector{}, err
	}

	// A snapshot never covers more than the stream's head (H3), so the stream's head item is
	// stored first.
	fake := NewFakeDynamo()
	repo := dynamo.NewRepository(fake, fakeTable)
	if err = seedHead(ctx, fake, repo, c.snapshot.EntityID, c.snapshot.EventSequence); err != nil {
		return SnapshotVector{}, err
	}

	err = repo.PutSnapshot(ctx, c.snapshot.EntityType, c.snapshot.EntityID, c.snapshot.Payload,
		c.snapshot.Sequence, c.snapshot.EventSequence)
	if err != nil {
		return SnapshotVector{}, err
	}

	var items []map[string]types.AttributeValue
	for _, item := range fake.Items(string(c.snapshot.EntityID)) {
		if sk, ok := item["sk"].(*types.AttributeValueMemberN); ok && sk.Value == "0" {
			items = append(items, item)
		}
	}
	if len(items) != 1 {
		return SnapshotVector{}, fmt.Errorf("expected one stored snapshot item, got %d", len(items))
	}

	return SnapshotVector{
		Name: c.name,
		Input: SnapshotInput{
			EntityType:    c.snapshot.EntityType,
			EntityID:      c.snapshot.EntityID,
			Sequence:      c.snapshot.Sequence,
			EventSequence: c.snapshot.EventSequence,
			Payload:       string(c.snapshot.Payload),
		},
		SerializedSnapshotJSON: string(serialized),
		DynamoItem:             DynamoItemJSON(items[0]),
	}, nil
}

// viewVector projects an account through the real view projector and records the item it writes.
func viewVector(ctx context.Context) (ViewVector, error) {
	account := NewAccount("acct-1")
	account.Owner = "Ada <&> \u2028"
	account.Status = "open"
	account.BalanceCents = 1200
	account.Deposits = 2
	account.OpenedAt = time.Date(2026, 1, 2, 3, 4, 5, 123_000_000, time.UTC)
	account.Tags = []string{"vip"}
	account.Labels = map[string]string{"tier": "gold", "region": "emea"}

	state, err := json.Marshal(account)
	if err != nil {
		return ViewVector{}, err
	}

	group, err := dynamo.NewViewProjector("views").Project(ctx, account, nil)
	if err != nil {
		return ViewVector{}, err
	}

	transaction, ok := group.(dynamo.TransactionGroup)
	if !ok {
		return ViewVector{}, fmt.Errorf("want a dynamo.TransactionGroup, got %T", group)
	}

	items := transaction.ToWriteItems()
	if len(items) != 1 || items[0].Put == nil {
		return ViewVector{}, fmt.Errorf("want one put, got %d items", len(items))
	}

	return ViewVector{
		Name:       "account",
		EntityID:   account.GetID(),
		State:      string(state),
		DynamoItem: DynamoItemJSON(items[0].Put.Item),
	}, nil
}

// ttlVector commits streams through a repository with a fixed clock and a retention policy. The
// clock and the retention both have fractional seconds, so the vector shows that the expiry is
// rounded down after adding them.
func ttlVector(ctx context.Context) (TTLVector, error) {
	clock := time.Unix(1_791_763_200, 900_000_000)
	retention := 24*time.Hour + 500*time.Millisecond

	fake := NewFakeDynamo()
	repo := dynamo.NewRepository(fake, fakeTable).
		WithClock(func() time.Time { return clock }).
		WithRetention(dynamo.Retention{"scratch": retention, "disabled": 0})

	vector := TTLVector{ClockUnixNano: clock.UnixNano()}

	for _, c := range []struct {
		name       string
		entityType evt.EntityType
		retention  time.Duration
	}{
		{"retained", "scratch", retention},
		{"zero-retention", "disabled", 0},
		{"no-policy", AccountType, 0},
	} {
		id := evt.EntityID("ttl-" + c.name)
		events := []evt.SerializedEvent{
			codecEvent(id, 1, OpenedEvent, 1, `{"owner":"Ada"}`, evt.Metadata{Region: Region, Timestamp: DefaultTimestamp}),
			codecEvent(id, 2, DepositedEvent, 1, `{"amountCents":5}`, evt.Metadata{Region: Region, Timestamp: DefaultTimestamp}),
		}
		for i := range events {
			events[i].EntityType = c.entityType
		}
		snapshot := evt.SerializedSnapshot{
			EntityType: c.entityType, EntityID: id, Sequence: 1, EventSequence: 2, Payload: []byte(`{"owner":"Ada"}`),
		}

		err := repo.CommitWithSnapshot(ctx, evt.SerializedResult{Events: events}, c.entityType, id, snapshot.Payload, 1)
		if err != nil {
			return vector, err
		}

		stream := TTLStream{Name: c.name, RetentionNanos: int64(c.retention)}
		stream.Events, err = encodeEvents(events)
		if err != nil {
			return vector, err
		}
		encoded, err := json.Marshal(snapshot)
		if err != nil {
			return vector, err
		}
		stream.Snapshot = string(encoded)
		for _, item := range fake.Items(string(id)) {
			stream.DynamoItems = append(stream.DynamoItems, DynamoItemJSON(item))
		}

		vector.Streams = append(vector.Streams, stream)
	}

	return vector, nil
}
