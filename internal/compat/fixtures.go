package compat

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/snapshots"
	"github.com/photon-grove/evt/stream"
)

// Fixture file names under compat/fixtures.
const (
	CodecFixtureFile    = "codec.json"
	ScenarioFixtureFile = "scenarios.json"
	fakeTable           = "evt-compat"
)

// ScenarioFixtures is the golden output of every shared scenario, produced by the Go
// implementation and asserted by both languages.
type ScenarioFixtures struct {
	Run       string                `json:"run"`
	Source    string                `json:"source"`
	Scenarios []ScenarioExpectation `json:"scenarios"`
}

// ScenarioExpectation is the stored and loaded state of a scenario's stream after all steps.
type ScenarioExpectation struct {
	Name             string           `json:"name"`
	EntityID         evt.EntityID     `json:"entityId"`
	Sequence         int64            `json:"sequence"`
	SnapshotSequence int64            `json:"snapshotSequence"`
	State            string           `json:"state"`
	Events           []string         `json:"events"`
	Snapshot         *string          `json:"snapshot"`
	DynamoItems      []map[string]any `json:"dynamoItems"`
	SNSMessages      []string         `json:"snsMessages"`
}

// ForRun rewrites the expectation's entity IDs from the fixture run to another run.
func (e ScenarioExpectation) ForRun(run string) (ScenarioExpectation, error) {
	from := fmt.Sprintf("compat-%s-", FixtureRun)
	to := fmt.Sprintf("compat-%s-", run)

	data, err := json.Marshal(e)
	if err != nil {
		return e, err
	}

	var rewritten ScenarioExpectation
	err = json.Unmarshal(bytes.ReplaceAll(data, []byte(from), []byte(to)), &rewritten)

	return rewritten, err
}

// CodecFixtures pins the encoding of individual events, snapshots, and metadata.
type CodecFixtures struct {
	Events         []EventVector    `json:"events"`
	Snapshots      []SnapshotVector `json:"snapshots"`
	MetadataDecode []DecodeVector   `json:"metadataDecode"`
	EventDecode    []DecodeVector   `json:"eventDecode"`
	SnapshotPolicy [][4]int64       `json:"snapshotPolicy"`
	Views          []ViewVector     `json:"views"`
	IntKeyMap      string           `json:"intKeyMap"`
	TTL            TTLVector        `json:"ttl"`
}

// TTLVector pins the retention policy: streams committed with a snapshot through a repository with
// a fixed clock, one stream per policy (a duration, zero, and none).
type TTLVector struct {
	ClockUnixNano int64       `json:"clockUnixNano"`
	Streams       []TTLStream `json:"streams"`
}

// TTLStream is one committed stream and the items it produced, snapshot first.
type TTLStream struct {
	Name           string           `json:"name"`
	RetentionNanos int64            `json:"retentionNanos"`
	Events         []string         `json:"events"`
	Snapshot       string           `json:"snapshot"`
	DynamoItems    []map[string]any `json:"dynamoItems"`
}

// ViewVector is the single-item view row dynamo.NewViewProjector writes for an account.
type ViewVector struct {
	Name       string         `json:"name"`
	EntityID   evt.EntityID   `json:"entityId"`
	State      string         `json:"state"`
	DynamoItem map[string]any `json:"dynamoItem"`
}

// EventInput describes a serialized event field by field. Metadata is given as its Go encoding.
type EventInput struct {
	ID           evt.EventID       `json:"id"`
	Sequence     evt.EventSequence `json:"sequence"`
	Type         evt.EventType     `json:"type"`
	Version      evt.EventVersion  `json:"version"`
	EntityID     evt.EntityID      `json:"entityId"`
	EntityType   evt.EntityType    `json:"entityType"`
	Payload      string            `json:"payload"`
	MetadataJSON string            `json:"metadataJson"`
}

// EventVector is one event and every encoding of it.
type EventVector struct {
	Name                string            `json:"name"`
	Input               EventInput        `json:"input"`
	SerializedEventJSON string            `json:"serializedEventJson"`
	DynamoItem          map[string]any    `json:"dynamoItem"`
	SNSMessage          string            `json:"snsMessage"`
	SNSAttributes       map[string]string `json:"snsAttributes"`
}

// SnapshotInput describes a serialized snapshot.
type SnapshotInput struct {
	EntityType    evt.EntityType    `json:"entityType"`
	EntityID      evt.EntityID      `json:"entityId"`
	Sequence      evt.EventSequence `json:"sequence"`
	EventSequence evt.EventSequence `json:"eventSequence"`
	Payload       string            `json:"payload"`
}

// SnapshotVector is one snapshot and its encodings.
type SnapshotVector struct {
	Name                   string         `json:"name"`
	Input                  SnapshotInput  `json:"input"`
	SerializedSnapshotJSON string         `json:"serializedSnapshotJson"`
	DynamoItem             map[string]any `json:"dynamoItem"`
}

// DecodeVector is a JSON input and its canonical re-encoding after a lenient decode.
type DecodeVector struct {
	Name      string `json:"name"`
	Input     string `json:"input"`
	Canonical string `json:"canonical"`
}

// DynamoJSON renders an attribute value as DynamoDB JSON.
func DynamoJSON(value types.AttributeValue) any {
	switch v := value.(type) {
	case *types.AttributeValueMemberS:
		return map[string]any{"S": v.Value}
	case *types.AttributeValueMemberN:
		return map[string]any{"N": v.Value}
	case *types.AttributeValueMemberB:
		return map[string]any{"B": base64.StdEncoding.EncodeToString(v.Value)}
	case *types.AttributeValueMemberBOOL:
		return map[string]any{"BOOL": v.Value}
	case *types.AttributeValueMemberNULL:
		return map[string]any{"NULL": v.Value}
	case *types.AttributeValueMemberM:
		return map[string]any{"M": DynamoItemJSON(v.Value)}
	case *types.AttributeValueMemberL:
		list := make([]any, 0, len(v.Value))
		for _, item := range v.Value {
			list = append(list, DynamoJSON(item))
		}
		return map[string]any{"L": list}
	case *types.AttributeValueMemberSS:
		return map[string]any{"SS": v.Value}
	case *types.AttributeValueMemberNS:
		return map[string]any{"NS": v.Value}
	}

	return map[string]any{"unsupported": fmt.Sprintf("%T", value)}
}

// DynamoItemJSON renders an item as DynamoDB JSON.
func DynamoItemJSON(item map[string]types.AttributeValue) map[string]any {
	out := make(map[string]any, len(item))
	for name, value := range item {
		out[name] = DynamoJSON(value)
	}

	return out
}

// captureSNS records published messages in place of SNS.
type captureSNS struct {
	entries []snstypes.PublishBatchRequestEntry
}

func (c *captureSNS) PublishBatch(
	_ context.Context,
	params *sns.PublishBatchInput,
	_ ...func(*sns.Options),
) (*sns.PublishBatchOutput, error) {
	c.entries = append(c.entries, params.PublishBatchRequestEntries...)

	successful := make([]snstypes.PublishBatchResultEntry, 0, len(params.PublishBatchRequestEntries))
	for _, entry := range params.PublishBatchRequestEntries {
		successful = append(successful, snstypes.PublishBatchResultEntry{Id: entry.Id})
	}

	return &sns.PublishBatchOutput{Successful: successful}, nil
}

// SNSMessage publishes an event-log item through the real stream.SNSPublisher (as a DynamoDB
// stream INSERT) and returns the message body and string attributes it sent.
func SNSMessage(ctx context.Context, item map[string]types.AttributeValue) (string, map[string]string, error) {
	image := make(map[string]events.DynamoDBAttributeValue, len(item))
	for name, value := range item {
		switch v := value.(type) {
		case *types.AttributeValueMemberS:
			image[name] = events.NewStringAttribute(v.Value)
		case *types.AttributeValueMemberN:
			image[name] = events.NewNumberAttribute(v.Value)
		default:
			return "", nil, fmt.Errorf("unsupported attribute %s (%T)", name, value)
		}
	}

	capture := &captureSNS{}
	publisher, err := stream.NewSNSPublisher( //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
		capture,
		"arn:aws:sns:us-west-2:000000000000:evt-compat",
		OriginSource,
		stream.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))), //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
	)
	if err != nil {
		return "", nil, err
	}

	record := events.DynamoDBEventRecord{
		EventName: "INSERT",
		Change:    events.DynamoDBStreamRecord{NewImage: image},
	}

	result, err := publisher.Publish(ctx, []events.DynamoDBEventRecord{record})
	if err != nil {
		return "", nil, err
	}
	if result.PublishedCount != 1 || len(capture.entries) != 1 {
		return "", nil, fmt.Errorf("expected one published message, got %d", result.PublishedCount)
	}

	entry := capture.entries[0]
	attributes := make(map[string]string, len(entry.MessageAttributes))
	for name, value := range entry.MessageAttributes {
		attributes[name] = aws.ToString(value.StringValue)
	}

	return aws.ToString(entry.Message), attributes, nil
}

// newFakeRunner returns a runner over the real DynamoDB repository backed by a FakeDynamo.
func newFakeRunner(interval int) (Runner, *FakeDynamo, *dynamo.Repository) {
	fake := NewFakeDynamo()
	repo := dynamo.NewRepository(fake, fakeTable)

	return Runner{Store: snapshots.NewStore(repo, interval), Repo: repo, SnapshotInterval: interval}, fake, repo
}

// Observe reads a stream back through the repository and store: the stored events and snapshot,
// and the loaded state. DynamoItems and SNSMessages are left empty.
func Observe(ctx context.Context, runner Runner, name string, entityID evt.EntityID) (ScenarioExpectation, error) {
	expectation := ScenarioExpectation{Name: name, EntityID: entityID, Events: []string{}}

	stored, err := runner.Repo.GetEvents(ctx, entityID)
	if err != nil {
		return expectation, err
	}

	for _, event := range stored {
		data, err := json.Marshal(event)
		if err != nil {
			return expectation, err
		}
		expectation.Events = append(expectation.Events, string(data))
	}

	snapshot, err := runner.Repo.GetSnapshot(ctx, entityID)
	if err != nil {
		return expectation, err
	}
	if snapshot != nil {
		data, err := json.Marshal(snapshot)
		if err != nil {
			return expectation, err
		}
		encoded := string(data)
		expectation.Snapshot = &encoded
	}

	entity := NewAccount(entityID)
	eventContext, err := runner.Store.LoadEntity(ctx, entity, entityID)
	if err != nil {
		return expectation, err
	}

	state, err := json.Marshal(entity)
	if err != nil {
		return expectation, err
	}

	expectation.State = string(state)
	expectation.Sequence = int64(*eventContext.CurrentSequence)
	expectation.SnapshotSequence = int64(*eventContext.CurrentSnapshot)

	return expectation, nil
}

// GenerateScenarioFixtures runs every scenario through the Go implementation against a fake
// DynamoDB table, checks each step's expected outcome, and records the final state.
func GenerateScenarioFixtures(ctx context.Context, scenarios Scenarios) (ScenarioFixtures, error) {
	fixtures := ScenarioFixtures{Run: FixtureRun, Source: OriginSource}

	for _, scenario := range scenarios.Scenarios {
		runner, fake, _ := newFakeRunner(scenario.SnapshotInterval)
		entityID := EntityID(FixtureRun, scenario)

		for index, step := range scenario.Steps {
			outcome, err := runner.Execute(ctx, entityID, step)
			if err != nil {
				return fixtures, fmt.Errorf("%s step %d: %w", scenario.Name, index, err)
			}
			if outcome != step.Expect {
				return fixtures, fmt.Errorf("%s step %d: got %s, want %s", scenario.Name, index, outcome, step.Expect)
			}
		}

		expectation, err := Observe(ctx, runner, scenario.Name, entityID)
		if err != nil {
			return fixtures, fmt.Errorf("%s: %w", scenario.Name, err)
		}

		expectation.DynamoItems = []map[string]any{}
		expectation.SNSMessages = []string{}

		for _, item := range fake.Items(string(entityID)) {
			expectation.DynamoItems = append(expectation.DynamoItems, DynamoItemJSON(item))

			if sk, ok := item["sk"].(*types.AttributeValueMemberN); ok && sk.Value == "0" {
				continue
			}

			message, _, err := SNSMessage(ctx, item)
			if err != nil {
				return fixtures, fmt.Errorf("%s: %w", scenario.Name, err)
			}
			expectation.SNSMessages = append(expectation.SNSMessages, message)
		}

		fixtures.Scenarios = append(fixtures.Scenarios, expectation)
	}

	return fixtures, nil
}

// MarshalFixture encodes a fixture file: two-space indent, no HTML escaping, trailing newline.
func MarshalFixture(value any) ([]byte, error) {
	var buffer bytes.Buffer

	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")

	if err := encoder.Encode(value); err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// FixturePath returns the path of a fixture file.
func FixturePath(name string) string {
	return filepath.Join(Dir(), "fixtures", name)
}

// LoadScenarioFixtures reads compat/fixtures/scenarios.json.
func LoadScenarioFixtures() (ScenarioFixtures, error) {
	var fixtures ScenarioFixtures

	data, err := os.ReadFile(FixturePath(ScenarioFixtureFile))
	if err != nil {
		return fixtures, err
	}

	err = json.Unmarshal(data, &fixtures)

	return fixtures, err
}
