// Command evt-compat is the Go driver for the cross-language integration run
// (scripts/compat-integration.sh).
//
//	evt-compat steps
//	evt-compat step   -backend dynamo|postgres -run RUN -index N
//	evt-compat verify -backend dynamo|postgres -run RUN
//
// step executes step N of every scenario whose writer is "go", against streams the Rust driver
// also writes to. verify checks every stream against the golden fixtures: the raw stored items or
// rows, the events and snapshot read through the repository, and the loaded state.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/internal/compat"
	"github.com/photon-grove/evt/internal/eventjson"
	"github.com/photon-grove/evt/postgres"
	"github.com/photon-grove/evt/snapshots"
)

type backend struct {
	repo     evt.Repository
	dynamo   *dynamodb.Client
	table    string
	postgres *pgxpool.Pool
}

func main() {
	if len(os.Args) < 2 {
		fail("missing command: steps | step | verify")
	}

	command := os.Args[1]
	flags := flag.NewFlagSet(command, flag.ExitOnError)
	backendName := flags.String("backend", "", "dynamo or postgres")
	run := flags.String("run", "", "run identifier")
	index := flags.Int("index", 0, "step index")
	check(flags.Parse(os.Args[2:]))

	ctx := context.Background()

	if command == "steps" {
		scenarios, err := compat.LoadScenarios()
		check(err)

		maxSteps := 0
		for _, scenario := range scenarios.Scenarios {
			maxSteps = max(maxSteps, len(scenario.Steps))
		}
		fmt.Println(maxSteps)

		return
	}

	if *run == "" {
		fail("-run is required")
	}

	b, err := connect(ctx, *backendName)
	check(err)

	var failures []string

	switch command {
	case "step":
		failures, err = step(ctx, b, *run, *index)
	case "verify":
		failures, err = verify(ctx, b, *run)
	default:
		fail("unknown command " + command)
	}
	check(err)

	for _, failure := range failures {
		fmt.Fprintln(os.Stderr, "FAIL "+failure)
	}
	if len(failures) > 0 {
		os.Exit(1)
	}
}

func connect(ctx context.Context, name string) (backend, error) {
	switch name {
	case "dynamo":
		endpoint := envOr("AWS_ENDPOINT_URL", "http://localhost:4566")
		table := envOr("EVT_COMPAT_DYNAMO_TABLE", "evt-local-event-log")

		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion("us-west-2"),
			config.WithRetryMaxAttempts(1),
			config.WithBaseEndpoint(endpoint),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		)
		if err != nil {
			return backend{}, err
		}

		client := dynamodb.NewFromConfig(cfg)

		return backend{repo: dynamo.NewRepository(client, table), dynamo: client, table: table}, nil

	case "postgres":
		pool, err := pgxpool.New(ctx, envOr("DATABASE_URL", "postgres://evt:evt@localhost:5432/evt_local?sslmode=disable"))
		if err != nil {
			return backend{}, err
		}

		repo := postgres.NewRepository(pool)
		if err := repo.EnsureSchema(ctx); err != nil {
			return backend{}, err
		}

		return backend{repo: repo, postgres: pool}, nil
	}

	return backend{}, fmt.Errorf("unknown backend %q", name)
}

func step(ctx context.Context, b backend, run string, index int) ([]string, error) {
	scenarios, err := compat.LoadScenarios()
	if err != nil {
		return nil, err
	}

	var failures []string

	for _, scenario := range scenarios.Scenarios {
		if index >= len(scenario.Steps) || scenario.Steps[index].By != compat.WriterGo {
			continue
		}

		s := scenario.Steps[index]
		runner := compat.Runner{Store: snapshots.NewStore(b.repo, scenario.SnapshotInterval), Repo: b.repo}

		outcome, err := runner.Execute(ctx, compat.EntityID(run, scenario), s)
		if err != nil {
			return nil, fmt.Errorf("%s step %d: %w", scenario.Name, index, err)
		}

		if outcome != s.Expect {
			failures = append(failures, fmt.Sprintf("%s step %d: got %s, want %s", scenario.Name, index, outcome, s.Expect))
			continue
		}

		fmt.Printf("go step %d %s: %s\n", index, scenario.Name, outcome.Result)
	}

	return failures, nil
}

func verify(ctx context.Context, b backend, run string) ([]string, error) {
	scenarios, err := compat.LoadScenarios()
	if err != nil {
		return nil, err
	}

	fixtures, err := compat.LoadScenarioFixtures()
	if err != nil {
		return nil, err
	}

	byName := make(map[string]compat.ScenarioExpectation, len(fixtures.Scenarios))
	for _, expectation := range fixtures.Scenarios {
		byName[expectation.Name] = expectation
	}

	var failures []string

	for _, scenario := range scenarios.Scenarios {
		expected, err := byName[scenario.Name].ForRun(run)
		if err != nil {
			return nil, err
		}

		before := len(failures)
		runner := compat.Runner{Store: snapshots.NewStore(b.repo, scenario.SnapshotInterval), Repo: b.repo}

		observed, err := compat.Observe(ctx, runner, scenario.Name, expected.EntityID)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", scenario.Name, err)
		}

		compare := func(what string, got, want any) {
			if !reflect.DeepEqual(got, want) {
				failures = append(failures, fmt.Sprintf("%s: %s\n  got:  %v\n  want: %v", scenario.Name, what, got, want))
			}
		}

		compare("state", observed.State, expected.State)
		compare("sequence", observed.Sequence, expected.Sequence)
		compare("snapshot sequence", observed.SnapshotSequence, expected.SnapshotSequence)
		compare("events", observed.Events, expected.Events)
		compare("snapshot", deref(observed.Snapshot), deref(expected.Snapshot))

		if b.dynamo != nil {
			items, err := partitionItems(ctx, b, string(expected.EntityID))
			if err != nil {
				return nil, err
			}
			compare("dynamo items", normalize(items), normalize(expected.DynamoItems))

			messages := make([]string, 0, len(items))
			for _, item := range items {
				if sk, ok := item["sk"].(*types.AttributeValueMemberN); ok && sk.Value == "0" {
					continue
				}
				message, _, err := compat.SNSMessage(ctx, item)
				if err != nil {
					return nil, err
				}
				messages = append(messages, message)
			}
			compare("sns messages", messages, expected.SNSMessages)
		}

		if b.postgres != nil {
			rows, snapshot, err := postgresRows(ctx, b.postgres, expected.EntityID)
			if err != nil {
				return nil, err
			}

			wantRows, wantSnapshot, err := expectedRows(expected)
			if err != nil {
				return nil, err
			}

			compare("event rows", rows, wantRows)
			compare("snapshot row", snapshot, wantSnapshot)
		}

		if len(failures) == before {
			fmt.Printf("go verified %s\n", scenario.Name)
		}
	}

	return failures, nil
}

func partitionItems(ctx context.Context, b backend, pk string) ([]map[string]types.AttributeValue, error) {
	paginator := dynamodb.NewQueryPaginator(b.dynamo, &dynamodb.QueryInput{
		TableName:                 aws.String(b.table),
		ConsistentRead:            aws.Bool(true),
		KeyConditionExpression:    aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{":pk": &types.AttributeValueMemberS{Value: pk}},
	})

	var items []map[string]types.AttributeValue
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		items = append(items, page.Items...)
	}

	return items, nil
}

// normalize renders DynamoDB items to comparable JSON values.
func normalize(items any) any {
	if typed, ok := items.([]map[string]types.AttributeValue); ok {
		rendered := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			rendered = append(rendered, compat.DynamoItemJSON(item))
		}
		items = rendered
	}

	data, err := json.Marshal(items)
	check(err)

	var out any
	check(json.Unmarshal(data, &out))

	return out
}

// row is an event or snapshot row with metadata parsed, since jsonb normalizes its formatting.
type row struct {
	EntityID      string
	Sequence      int64
	EventID       string
	EventType     string
	Version       int64
	EntityType    string
	Payload       string
	Metadata      any
	EventSequence int64
}

func postgresRows(ctx context.Context, pool *pgxpool.Pool, entityID evt.EntityID) ([]row, *row, error) {
	rows, err := pool.Query(ctx, `SELECT entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata::text
		FROM evt_events WHERE entity_id = $1 ORDER BY sequence`, string(entityID))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	events := []row{}
	for rows.Next() {
		var (
			r        row
			payload  []byte
			metadata string
		)
		if err = rows.Scan(&r.EntityID, &r.Sequence, &r.EventID, &r.EventType, &r.Version, &r.EntityType, &payload, &metadata); err != nil {
			return nil, nil, err
		}
		r.Payload = string(payload)
		if err = json.Unmarshal([]byte(metadata), &r.Metadata); err != nil {
			return nil, nil, err
		}
		events = append(events, r)
	}
	if err = rows.Err(); err != nil {
		return nil, nil, err
	}

	var (
		snapshot row
		payload  []byte
	)
	err = pool.QueryRow(ctx, `SELECT entity_id, entity_type, sequence, event_sequence, payload
		FROM evt_snapshots WHERE entity_id = $1`, string(entityID)).
		Scan(&snapshot.EntityID, &snapshot.EntityType, &snapshot.Sequence, &snapshot.EventSequence, &payload)
	if err != nil {
		return events, nil, ignoreNoRows(err)
	}
	snapshot.Payload = string(payload)

	return events, &snapshot, nil
}

func expectedRows(expected compat.ScenarioExpectation) ([]row, *row, error) {
	events := []row{}
	for _, encoded := range expected.Events {
		var event evt.SerializedEvent
		if err := eventjson.UnmarshalEvent([]byte(encoded), &event); err != nil {
			return nil, nil, err
		}

		metadata, err := eventjson.MarshalMetadata(event.Metadata)
		if err != nil {
			return nil, nil, err
		}

		r := row{
			EntityID:   string(event.EntityID),
			Sequence:   int64(event.Sequence),
			EventID:    string(event.ID),
			EventType:  string(event.Type),
			Version:    int64(event.Version),
			EntityType: string(event.EntityType),
			Payload:    string(event.Payload),
		}
		if err := json.Unmarshal(metadata, &r.Metadata); err != nil {
			return nil, nil, err
		}
		events = append(events, r)
	}

	if expected.Snapshot == nil {
		return events, nil, nil
	}

	var snapshot evt.SerializedSnapshot
	if err := json.Unmarshal([]byte(*expected.Snapshot), &snapshot); err != nil {
		return nil, nil, err
	}

	return events, &row{
		EntityID:      string(snapshot.EntityID),
		EntityType:    string(snapshot.EntityType),
		Sequence:      int64(snapshot.Sequence),
		EventSequence: int64(snapshot.EventSequence),
		Payload:       string(snapshot.Payload),
	}, nil
}

func ignoreNoRows(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}

	return err
}

func deref(value *string) string {
	if value == nil {
		return "<none>"
	}

	return *value
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

func check(err error) {
	if err != nil {
		fail(err.Error())
	}
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, "evt-compat: "+message)
	os.Exit(2)
}
