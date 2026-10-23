package compat

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/dynamo"
	"github.com/photon-grove/evt/go/internal/eventjson"
)

// StreamHeadVector pins the DynamoDB requests of the per-stream head item (ADR 0007): the exact
// actions, expressions, name placeholders, and values a commit or an out-of-band snapshot write
// sends, in order. Both languages build the Request and compare their requests with Output.
type StreamHeadVector struct {
	Name    string             `json:"name"`
	Request StreamHeadRequest  `json:"request"`
	Output  []FeatureWriteItem `json:"output"`

	// Sizes is the commit budget's item estimate (evt.TransactionItemSizesWithTTL) for a commit
	// request: the marker check, the event puts, the snapshot puts, one head update per written
	// stream, and one check per fence. Both languages must estimate the same bytes, so that
	// CommitTooLarge fires at the same boundary on every backend. Absent for a snapshot write.
	Sizes []int `json:"sizes,omitempty"`
}

// StreamHeadRequest describes the store before the request and the request itself. The store
// declares stream-heads at version 1 and holds the seeded heads and snapshots. Events carry the
// payload StreamHeadPayload and the metadata StreamHeadMetadata describes.
type StreamHeadRequest struct {
	// ClockUnixNano and RetentionNanos configure the writer's retention (entity type -> duration),
	// as in the codec ttl vector.
	ClockUnixNano  int64            `json:"clockUnixNano,omitempty"`
	RetentionNanos map[string]int64 `json:"retentionNanos,omitempty"`

	Seeded      []SeededStream      `json:"seeded,omitempty"`
	Streams     []HeadStreamRequest `json:"streams,omitempty"`
	Fences      []HeadFenceRequest  `json:"fences,omitempty"`
	PutSnapshot *PutSnapshotRequest `json:"putSnapshot,omitempty"`
}

// SeededStream is a stream's stored head item, and optionally the snapshot item beside it.
type SeededStream struct {
	EntityID string `json:"entityId"`
	Head     int64  `json:"head"`
	TTL      int64  `json:"ttl,omitempty"`

	// SnapshotSequence and SnapshotEventSequence seed the snapshot item when SnapshotSequence > 0.
	SnapshotSequence      int64 `json:"snapshotSequence,omitempty"`
	SnapshotEventSequence int64 `json:"snapshotEventSequence,omitempty"`
}

// HeadStreamRequest is one written stream: Count events from First, and a snapshot with the given
// sequence when SnapshotSequence > 0.
type HeadStreamRequest struct {
	EntityID         string `json:"entityId"`
	EntityType       string `json:"entityType"`
	First            int64  `json:"first"`
	Count            int    `json:"count"`
	SnapshotSequence int64  `json:"snapshotSequence,omitempty"`
}

// HeadFenceRequest is one fenced read-only stream.
type HeadFenceRequest struct {
	EntityID string `json:"entityId"`
	Head     int64  `json:"head"`
}

// PutSnapshotRequest is an out-of-band snapshot write.
type PutSnapshotRequest struct {
	EntityID         string `json:"entityId"`
	EntityType       string `json:"entityType"`
	SnapshotSequence int64  `json:"snapshotSequence"`
	EventSequence    int64  `json:"eventSequence"`
}

// Fixed values of the vectors' events: every event is an account deposit with this payload, this
// commit ID, and the shared region and timestamp.
const (
	StreamHeadPayload  = `{"amountCents":1}`
	StreamHeadCommitID = "compat-head-vector"
)

const (
	headVectorClock     = int64(1_791_763_200_000_000_000)
	headVectorRetention = int64(24 * time.Hour)
)

// StreamHeadMetadata is the metadata of every vector event.
func StreamHeadMetadata() evt.Metadata {
	return evt.Metadata{Region: Region, Timestamp: DefaultTimestamp, CommitID: StreamHeadCommitID}
}

// streamHeadVectors are the requests pinned by compat/fixtures/feature-marker.json.
func streamHeadVectors() []StreamHeadVector {
	retained := map[string]int64{"scratch": headVectorRetention}

	return []StreamHeadVector{
		{Name: "create", Request: StreamHeadRequest{
			Streams: []HeadStreamRequest{{EntityID: "acct-1", EntityType: "account", First: 1, Count: 1}},
		}},
		{Name: "append", Request: StreamHeadRequest{
			Seeded:  []SeededStream{{EntityID: "acct-1", Head: 7}},
			Streams: []HeadStreamRequest{{EntityID: "acct-1", EntityType: "account", First: 8, Count: 2}},
		}},
		{Name: "create-with-snapshot", Request: StreamHeadRequest{
			Streams: []HeadStreamRequest{{EntityID: "acct-1", EntityType: "account", First: 1, Count: 5, SnapshotSequence: 1}},
		}},
		{Name: "append-with-later-snapshot", Request: StreamHeadRequest{
			Seeded: []SeededStream{{EntityID: "acct-1", Head: 5, SnapshotSequence: 1, SnapshotEventSequence: 5}},
			Streams: []HeadStreamRequest{
				{EntityID: "acct-1", EntityType: "account", First: 6, Count: 1, SnapshotSequence: 2},
			},
		}},
		// The storage spec's worked example: appends to a (with a snapshot) and b, fencing m and z.
		{Name: "streams-and-fences", Request: StreamHeadRequest{
			Seeded: []SeededStream{
				{EntityID: "a", Head: 7, SnapshotSequence: 1, SnapshotEventSequence: 6},
				{EntityID: "m", Head: 3},
			},
			Streams: []HeadStreamRequest{
				{EntityID: "b", EntityType: "account", First: 1, Count: 1},
				{EntityID: "a", EntityType: "account", First: 8, Count: 1, SnapshotSequence: 2},
			},
			Fences: []HeadFenceRequest{{EntityID: "z", Head: 0}, {EntityID: "m", Head: 3}},
		}},
		{Name: "retained-create", Request: StreamHeadRequest{
			ClockUnixNano: headVectorClock, RetentionNanos: retained,
			Streams: []HeadStreamRequest{{EntityID: "session-9", EntityType: "scratch", First: 1, Count: 3}},
		}},
		{Name: "retained-append", Request: StreamHeadRequest{
			ClockUnixNano: headVectorClock, RetentionNanos: retained,
			Seeded:  []SeededStream{{EntityID: "session-9", Head: 3, TTL: 1_791_763_000}},
			Streams: []HeadStreamRequest{{EntityID: "session-9", EntityType: "scratch", First: 4, Count: 1}},
		}},
		{Name: "put-snapshot", Request: StreamHeadRequest{
			Seeded: []SeededStream{{EntityID: "acct-1", Head: 6}},
			PutSnapshot: &PutSnapshotRequest{
				EntityID: "acct-1", EntityType: "account", SnapshotSequence: 1, EventSequence: 6,
			},
		}},
		{Name: "put-snapshot-retained", Request: StreamHeadRequest{
			ClockUnixNano: headVectorClock, RetentionNanos: retained,
			Seeded: []SeededStream{{EntityID: "session-9", Head: 6, TTL: 1_791_763_000}},
			PutSnapshot: &PutSnapshotRequest{
				EntityID: "session-9", EntityType: "scratch", SnapshotSequence: 1, EventSequence: 6,
			},
		}},
		{Name: "put-snapshot-empty-stream", Request: StreamHeadRequest{
			PutSnapshot: &PutSnapshotRequest{
				EntityID: "acct-1", EntityType: "account", SnapshotSequence: 1, EventSequence: 0,
			},
		}},
	}
}

// GenerateStreamHeadVectors sends every request through the real DynamoDB repository against a
// fake table and records the actions it sent.
func GenerateStreamHeadVectors(ctx context.Context) ([]StreamHeadVector, error) {
	vectors := streamHeadVectors()

	for i := range vectors {
		output, err := runStreamHeadRequest(ctx, vectors[i].Request)
		if err != nil {
			return nil, fmt.Errorf("stream head vector %s: %w", vectors[i].Name, err)
		}

		vectors[i].Output = output

		if vectors[i].Request.PutSnapshot == nil {
			vectors[i].Sizes, err = budgetSizes(vectors[i].Request)
			if err != nil {
				return nil, fmt.Errorf("stream head vector %s: %w", vectors[i].Name, err)
			}
		}
	}

	return vectors, nil
}

// budgetSizes is the commit budget's item estimate for a commit request, with the writer's
// retention applied the way a repository with that retention and clock stamps ttls.
func budgetSizes(request StreamHeadRequest) ([]int, error) {
	clock := time.Unix(0, request.ClockUnixNano)

	return evt.TransactionItemSizesWithTTL(headVectorCommit(request), func(entityType evt.EntityType) int64 {
		nanos := request.RetentionNanos[string(entityType)]
		if nanos <= 0 {
			return 0
		}

		return clock.Add(time.Duration(nanos)).Unix()
	})
}

func runStreamHeadRequest(ctx context.Context, request StreamHeadRequest) ([]FeatureWriteItem, error) {
	capture := &featureCaptureDynamo{FakeDynamo: NewFakeDynamo()}
	repo := dynamo.NewRepository(capture, fakeTable)

	if len(request.RetentionNanos) > 0 {
		retention := dynamo.Retention{}
		for entityType, nanos := range request.RetentionNanos {
			retention[evt.EntityType(entityType)] = time.Duration(nanos)
		}

		clock := time.Unix(0, request.ClockUnixNano)
		repo = repo.WithRetention(retention).WithClock(func() time.Time { return clock })
	}

	// The store declares stream-heads, then holds the seeded streams.
	if _, err := repo.FeatureMarker(nil).DeclareStreamHeads(ctx); err != nil {
		return nil, err
	}

	for _, seed := range request.Seeded {
		if err := seedStream(ctx, capture.FakeDynamo, seed); err != nil {
			return nil, err
		}
	}

	capture.writes = nil

	switch {
	case request.PutSnapshot != nil:
		put := request.PutSnapshot
		err := repo.PutSnapshot(ctx, evt.EntityType(put.EntityType), evt.EntityID(put.EntityID), []byte(`{}`),
			evt.EventSequence(put.SnapshotSequence), evt.EventSequence(put.EventSequence))
		if err != nil {
			return nil, err
		}

	default:
		if err := repo.Commit(ctx, headVectorCommit(request)); err != nil {
			return nil, err
		}
	}

	return capture.writes, nil
}

func seedStream(ctx context.Context, fake *FakeDynamo, seed SeededStream) error {
	head := map[string]types.AttributeValue{
		"pk":   &types.AttributeValueMemberS{Value: seed.EntityID},
		"sk":   &types.AttributeValueMemberN{Value: "-1"},
		"head": &types.AttributeValueMemberN{Value: strconv.FormatInt(seed.Head, 10)},
	}
	if seed.TTL != 0 {
		head["ttl"] = &types.AttributeValueMemberN{Value: strconv.FormatInt(seed.TTL, 10)}
	}

	if _, err := fake.PutItem(ctx, &dynamodb.PutItemInput{Item: head}); err != nil {
		return err
	}

	if seed.SnapshotSequence == 0 {
		return nil
	}

	_, err := fake.PutItem(ctx, &dynamodb.PutItemInput{Item: map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: seed.EntityID},
		"sk":         &types.AttributeValueMemberN{Value: "0"},
		"seq":        &types.AttributeValueMemberN{Value: strconv.FormatInt(seed.SnapshotSequence, 10)},
		"eventSeq":   &types.AttributeValueMemberN{Value: strconv.FormatInt(seed.SnapshotEventSequence, 10)},
		"entityType": &types.AttributeValueMemberS{Value: string(AccountType)},
		"payload":    &types.AttributeValueMemberS{Value: "{}"},
	}})

	return err
}

// headVectorCommit builds the commit a request describes.
func headVectorCommit(request StreamHeadRequest) evt.SerializedResult {
	var result evt.SerializedResult

	for _, stream := range request.Streams {
		id := evt.EntityID(stream.EntityID)
		last := evt.EventSequence(stream.First + int64(stream.Count) - 1)

		for sequence := evt.EventSequence(stream.First); sequence <= last; sequence++ {
			event := codecEvent(id, sequence, DepositedEvent, 1, StreamHeadPayload, StreamHeadMetadata())
			event.EntityType = evt.EntityType(stream.EntityType)
			result.Events = append(result.Events, event)
		}

		if stream.SnapshotSequence > 0 {
			result.Snapshots = append(result.Snapshots, evt.SerializedSnapshot{
				EntityType: evt.EntityType(stream.EntityType), EntityID: id,
				Sequence: evt.EventSequence(stream.SnapshotSequence), EventSequence: last, Payload: []byte(`{}`),
			})
		}
	}

	for _, fence := range request.Fences {
		result.Fences = append(result.Fences, evt.ReadFence{
			EntityID: evt.EntityID(fence.EntityID), EntityType: AccountType, Head: evt.EventSequence(fence.Head),
		})
	}

	return result
}

// dynamoPutter is the PutItem call of a DynamoDB client.
type dynamoPutter interface {
	PutItem(
		ctx context.Context,
		params *dynamodb.PutItemInput,
		optFns ...func(*dynamodb.Options),
	) (*dynamodb.PutItemOutput, error)
}

// DynamoBypassHead returns the Runner.BypassHead of a DynamoDB table: a put of the event item
// alone, with none of the head update a commit carries.
func DynamoBypassHead(client dynamoPutter, table string) func(context.Context, evt.SerializedEvent) error {
	return func(ctx context.Context, event evt.SerializedEvent) error {
		metadata, err := eventjson.MarshalMetadata(event.Metadata)
		if err != nil {
			return err
		}

		item, err := attributevalue.MarshalMapWithOptions(dynamo.Event{
			PK:         event.EntityID,
			SK:         event.Sequence,
			Type:       event.Type,
			Version:    event.Version,
			EntityType: event.EntityType,
			Payload:    string(event.Payload),
			Metadata:   string(metadata),
		}, func(options *attributevalue.EncoderOptions) { options.TagKey = "json" })
		if err != nil {
			return err
		}

		_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName:           &table,
			Item:                item,
			ConditionExpression: aws.String("attribute_not_exists(sk)"),
		})

		return err
	}
}
