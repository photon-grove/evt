// Command evt-compat is the Go driver for the cross-language integration run
// (scripts/compat-integration.sh).
//
//	evt-compat steps [-only SCENARIO]
//	evt-compat exclusive
//	evt-compat settle -backend dynamo|postgres
//	evt-compat step   -backend dynamo|postgres -run RUN -index N [-only SCENARIO]
//	evt-compat verify -backend dynamo|postgres -run RUN
//	evt-compat enumerate -backend dynamo|postgres -run RUN
//
// step executes step N of every scenario whose writer is "go", against streams the Rust driver
// also writes to. A scenario marked exclusive holds store-wide state across steps (a rebuild lock
// refuses every other scenario's commits), so the interleaved run skips it: it runs alone, one step
// at a time, with -only. steps prints how many steps the interleaved run has, or how many the named
// scenario has, and exclusive lists the exclusive scenarios. verify checks every stream against the golden fixtures: the raw stored items or
// rows, the events and snapshot read through the repository, and the loaded state. enumerate prints
// the run's events as StreamAllEvents yields them (unfiltered and filtered by entity type), one
// sorted line each, for the integration script to compare with the Rust driver's output.
package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

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
)

type backend struct {
	repo       evt.Repository
	dynamo     *dynamodb.Client
	table      string
	viewsTable string
	postgres   *pgxpool.Pool
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
	only := flags.String("only", "", "run only this (exclusive) scenario")
	check(flags.Parse(os.Args[2:]))

	ctx := context.Background()

	if command == "settle" {
		b, err := connect(ctx, *backendName)
		check(err)
		check(settle(ctx, b))

		return
	}

	switch command {
	case "steps":
		scenarios, err := compat.LoadScenarios()
		check(err)

		maxSteps := 0
		for _, scenario := range scenarios.Scenarios {
			if (*only == "") == scenario.Exclusive || (*only != "" && scenario.Name != *only) {
				continue
			}

			maxSteps = max(maxSteps, len(scenario.Steps))
		}
		fmt.Println(maxSteps)

		return

	case "exclusive":
		scenarios, err := compat.LoadScenarios()
		check(err)

		for _, scenario := range scenarios.Scenarios {
			if scenario.Exclusive {
				fmt.Println(scenario.Name)
			}
		}

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
		failures, err = step(ctx, b, *run, *index, *only)
	case "verify":
		failures, err = verify(ctx, b, *run)
	case "enumerate":
		err = enumerate(ctx, b, *run)
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

// enumerate prints every event of the run that StreamAllEvents yields, once with a zero filter
// and once narrowed to the account entity type, as sorted "section entity sequence type" lines.
func enumerate(ctx context.Context, b backend, run string) error {
	var lines []string

	for _, section := range []struct {
		name   string
		filter evt.StreamFilter
	}{
		{"all", evt.StreamFilter{}},
		{"typed", evt.StreamFilter{EntityType: compat.AccountType}},
	} {
		for batch := range b.repo.StreamAllEvents(ctx, section.filter) {
			events, err := batch.Unwrap()
			if err != nil {
				return fmt.Errorf("stream %s: %w", section.name, err)
			}

			for _, event := range events {
				if !strings.Contains(string(event.EntityID), run) {
					continue
				}

				lines = append(lines, fmt.Sprintf("%s %s %d %s", section.name, event.EntityID, event.Sequence, event.Type))
			}
		}
	}

	sort.Strings(lines)

	for _, line := range lines {
		fmt.Println(line)
	}

	return nil
}

// settle ends a rebuild a failed exclusive step left behind, so a later run starts from a settled
// fence: it force-releases a lock held by a compat owner, then runs and ends one rebuild if the
// generation is still odd (a force-release leaves it odd). Locks of other owners are left alone.
func settle(ctx context.Context, b backend) error {
	provider, ok := b.repo.(evt.RebuildFenceProvider)
	if !ok {
		return nil
	}

	fence := provider.RebuildFence()

	state, err := fence.State(ctx)
	if err != nil {
		return err
	}

	if state.Lock != nil {
		if !strings.HasPrefix(state.Lock.Owner, "compat-") {
			return fmt.Errorf("the rebuild lock is held by %q, which is not a compat owner", state.Lock.Owner)
		}

		if _, err = fence.ForceRelease(ctx, state.Lock.Owner); err != nil {
			return err
		}
	}

	if state.Generation.Settled() && state.Lock == nil {
		return nil
	}

	lease, err := fence.Begin(ctx, "compat-settle", time.Minute)
	if err != nil {
		return err
	}

	_, err = fence.End(ctx, lease)

	return err
}

func connect(ctx context.Context, name string) (backend, error) {
	switch name {
	case "dynamo":
		endpoint := envOr("AWS_ENDPOINT_URL", "http://localhost:4566")
		table := envOr("EVT_COMPAT_DYNAMO_TABLE", "evt-local-event-log")
		viewsTable := envOr("EVT_COMPAT_DYNAMO_VIEWS_TABLE", "evt-local-entity-views")

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

		return backend{
			repo:   dynamo.NewRepository(client, table).WithLazyInitialization(true).WithViewsTable(viewsTable),
			dynamo: client, table: table, viewsTable: viewsTable,
		}, nil

	case "postgres":
		pool, err := pgxpool.New(ctx, envOr("DATABASE_URL", "postgres://evt:evt@localhost:5432/evt_local?sslmode=disable"))
		if err != nil {
			return backend{}, err
		}

		repo := postgres.NewRepository(pool).WithViewWriter(compat.PostgresViewWriter)
		if err := repo.EnsureSchema(ctx); err != nil {
			return backend{}, err
		}

		// evt defines no view schema: the application owns its view tables, and so does this driver.
		if _, err := pool.Exec(ctx, compat.PostgresViewDDL); err != nil {
			return backend{}, err
		}

		return backend{repo: repo, postgres: pool}, nil
	}

	return backend{}, fmt.Errorf("unknown backend %q", name)
}

func step(ctx context.Context, b backend, run string, index int, only string) ([]string, error) {
	scenarios, err := compat.LoadScenarios()
	if err != nil {
		return nil, err
	}

	var failures []string

	for _, scenario := range scenarios.Scenarios {
		// The interleaved run skips exclusive scenarios; a run with -only executes that one alone.
		if scenario.Exclusive != (only != "") || (only != "" && scenario.Name != only) {
			continue
		}

		if index >= len(scenario.Steps) || scenario.Steps[index].By != compat.WriterGo {
			continue
		}

		s := scenario.Steps[index]
		runner := newRunner(b, scenario, run)

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
		runner := newRunner(b, scenario, run)

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

		// Every other stream the scenario touches, written or only fenced.
		for _, sibling := range expected.Siblings {
			observedSibling, err := compat.ObserveSibling(ctx, runner, sibling.EntityID)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", scenario.Name, err)
			}

			compare("sibling "+string(sibling.EntityID)+" events", observedSibling.Events, sibling.Events)
			compare("sibling "+string(sibling.EntityID)+" snapshot", deref(observedSibling.Snapshot), deref(sibling.Snapshot))
		}

		if b.dynamo != nil {
			items, err := partitionItems(ctx, b, string(expected.EntityID))
			if err != nil {
				return nil, err
			}
			compare("dynamo items", normalize(items), normalize(expected.DynamoItems))

			messages := make([]string, 0, len(items))
			for _, item := range items {
				if sk, ok := item["sk"].(*types.AttributeValueMemberN); ok && (sk.Value == "0" || sk.Value == "-1") {
					continue
				}
				message, _, err := compat.SNSMessage(ctx, item)
				if err != nil {
					return nil, err
				}
				messages = append(messages, message)
			}
			compare("sns messages", messages, expected.SNSMessages)

			for _, sibling := range expected.Siblings {
				siblingItems, err := partitionItems(ctx, b, string(sibling.EntityID))
				if err != nil {
					return nil, err
				}
				compare("sibling "+string(sibling.EntityID)+" dynamo items", normalize(siblingItems), normalize(sibling.DynamoItems))
			}
		}

		if b.postgres != nil {
			rows, snapshot, err := postgresRows(ctx, b.postgres, expected.EntityID)
			if err != nil {
				return nil, err
			}

			wantRows, wantSnapshot, err := expectedRows(expected.Events, expected.Snapshot)
			if err != nil {
				return nil, err
			}

			compare("event rows", rows, wantRows)
			compare("snapshot row", snapshot, wantSnapshot)

			head, err := postgresHead(ctx, b.postgres, expected.EntityID)
			if err != nil {
				return nil, err
			}
			compare("head row", head, expected.Head)

			for _, sibling := range expected.Siblings {
				siblingRows, siblingSnapshot, err := postgresRows(ctx, b.postgres, sibling.EntityID)
				if err != nil {
					return nil, err
				}

				wantSiblingRows, wantSiblingSnapshot, err := expectedRows(sibling.Events, sibling.Snapshot)
				if err != nil {
					return nil, err
				}

				compare("sibling "+string(sibling.EntityID)+" event rows", siblingRows, wantSiblingRows)
				compare("sibling "+string(sibling.EntityID)+" snapshot row", siblingSnapshot, wantSiblingSnapshot)

				siblingHead, err := postgresHead(ctx, b.postgres, sibling.EntityID)
				if err != nil {
					return nil, err
				}
				compare("sibling "+string(sibling.EntityID)+" head row", siblingHead, sibling.Head)
			}
		}

		viewFailures, viewErr := verifyViews(ctx, b, scenario, expected)
		if viewErr != nil {
			return nil, viewErr
		}

		failures = append(failures, viewFailures...)

		if scenario.Outcomes {
			outcomeFailures, err := verifyOutcomes(ctx, b, scenario, expected)
			if err != nil {
				return nil, err
			}

			failures = append(failures, outcomeFailures...)
		}

		if len(failures) == before {
			fmt.Printf("go verified %s\n", scenario.Name)
		}
	}

	return failures, nil
}

// newRunner builds the scenario's runner: a store with command outcomes for an outcomes scenario.
func newRunner(b backend, scenario compat.Scenario, run string) compat.Runner {
	var runner compat.Runner

	if scenario.Outcomes {
		runner = compat.NewOutcomeRunner(b.repo, scenario, run)
	} else {
		runner = compat.NewRunner(b.repo, scenario)
	}

	switch {
	case b.dynamo != nil:
		runner.BypassHead = compat.DynamoBypassHead(b.dynamo, b.table)
	case b.postgres != nil:
		runner.BypassHead = postgresBypassHead(b.postgres)
	}

	runner.Rebuild = b.rebuildDriver()

	return runner
}

// rebuildDriver backs the rebuild op against the live backend: the repository's fence, a wait that
// really sleeps, and a count of the compat view rows of a partition.
func (b backend) rebuildDriver() *compat.RebuildDriver {
	provider, ok := b.repo.(evt.RebuildFenceProvider)
	if !ok {
		return nil
	}

	return &compat.RebuildDriver{
		Fence: provider.RebuildFence(),
		Wait: func(ctx context.Context, seconds int64) error {
			select {
			case <-time.After(time.Duration(seconds) * time.Second):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
		Rows: func(ctx context.Context, pk string) (int, error) {
			if b.postgres != nil {
				var rows int

				err := b.postgres.QueryRow(ctx, `SELECT count(*) FROM `+compat.PostgresViewTable+` WHERE pk = $1`, pk).Scan(&rows)

				return rows, err
			}

			items, err := partitionItemsIn(ctx, b, b.viewsTable, pk)

			return len(items), err
		},
	}
}

// postgresBypassHead inserts an event row directly, leaving the stream's head row untouched.
func postgresBypassHead(pool *pgxpool.Pool) func(context.Context, evt.SerializedEvent) error {
	return func(ctx context.Context, event evt.SerializedEvent) error {
		metadata, err := eventjson.MarshalMetadata(event.Metadata)
		if err != nil {
			return err
		}

		_, err = pool.Exec(ctx, `INSERT INTO evt_events
			(entity_id, sequence, event_id, event_type, version, entity_type, payload, metadata)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
			string(event.EntityID), int64(event.Sequence), string(event.ID), string(event.Type),
			int64(event.Version), string(event.EntityType), event.Payload, metadata)

		return err
	}
}

// postgresHead reads a stream's head row, or nil when it has none.
func postgresHead(ctx context.Context, pool *pgxpool.Pool, entityID evt.EntityID) (*int64, error) {
	var head int64

	err := pool.QueryRow(ctx, `SELECT head FROM evt_events__evt_stream_heads WHERE entity_id = $1`,
		string(entityID)).Scan(&head)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return &head, nil
}

// verifyViews checks the view rows commit-wide projectors left in the scenario's partitions: the raw
// DynamoDB items, or the PostgreSQL rows. Partitions shared between runs are filtered to the rows
// that belong to this run.
func verifyViews(
	ctx context.Context,
	b backend,
	scenario compat.Scenario,
	expected compat.ScenarioExpectation,
) ([]string, error) {
	partitions := scenario.ViewPartitions(expected.EntityID)
	if len(partitions) == 0 {
		return nil, nil
	}

	owns := func(pk, sk string) bool { return compat.OwnsViewRow(expected.EntityID, pk, sk) }

	var (
		got  any
		want any
	)

	switch {
	case b.dynamo != nil:
		var items []map[string]types.AttributeValue

		for _, pk := range partitions {
			rows, err := partitionItemsIn(ctx, b, b.viewsTable, pk)
			if err != nil {
				return nil, err
			}

			for _, item := range rows {
				if sk, ok := item["sk"].(*types.AttributeValueMemberS); ok && owns(pk, sk.Value) {
					items = append(items, item)
				}
			}
		}

		var wantItems []map[string]any
		for _, view := range expected.Views {
			wantItems = append(wantItems, view.DynamoItem)
		}

		got, want = normalize(items), normalize(wantItems)

	case b.postgres != nil:
		rows, err := postgresViews(ctx, b.postgres, partitions, owns)
		if err != nil {
			return nil, err
		}

		wantRows := []compat.ViewRow{}
		for _, view := range expected.Views {
			wantRows = append(wantRows, view.Row())
		}

		got, want = rows, wantRows
	}

	if reflect.DeepEqual(got, want) {
		return nil, nil
	}

	return []string{fmt.Sprintf("%s: view rows\n  got:  %v\n  want: %v", scenario.Name, got, want)}, nil
}

// postgresViews reads the view rows of the given partitions that the run owns, ordered by partition
// then sort key bytes.
func postgresViews(
	ctx context.Context,
	pool *pgxpool.Pool,
	partitions []string,
	owns func(pk, sk string) bool,
) ([]compat.ViewRow, error) {
	rows, err := pool.Query(ctx, `SELECT pk, sk, entity_id, entity_type, payload FROM `+compat.PostgresViewTable+`
		WHERE pk = ANY($1) ORDER BY pk COLLATE "C", sk COLLATE "C"`, partitions)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	views := []compat.ViewRow{}

	for rows.Next() {
		var view compat.ViewRow
		if err = rows.Scan(&view.PK, &view.SK, &view.EntityID, &view.EntityType, &view.Payload); err != nil {
			return nil, err
		}

		if owns(view.PK, view.SK) {
			views = append(views, view)
		}
	}

	return views, rows.Err()
}

// verifyOutcomes checks the command-outcome rows of an outcomes scenario against the fixtures:
// the stored DynamoDB item, or the PostgreSQL row with its jsonb fields compared as JSON.
func verifyOutcomes(
	ctx context.Context,
	b backend,
	scenario compat.Scenario,
	expected compat.ScenarioExpectation,
) ([]string, error) {
	var failures []string

	for _, outcome := range expected.Outcomes {
		switch {
		case b.dynamo != nil:
			items, err := partitionItems(ctx, b, evt.OutcomeKey(evt.CommandID(outcome.CommandID)))
			if err != nil {
				return nil, err
			}

			got, want := normalize(items), normalize([]map[string]any{outcome.DynamoItem})
			if !reflect.DeepEqual(got, want) {
				failures = append(failures, fmt.Sprintf("%s: outcome %s item\n  got:  %v\n  want: %v", scenario.Name, outcome.CommandID, got, want))
			}

		case b.postgres != nil:
			got, err := postgresOutcome(ctx, b.postgres, outcome.CommandID)
			if err != nil {
				return nil, err
			}

			want, err := expectedOutcomeRow(outcome)
			if err != nil {
				return nil, err
			}

			if !reflect.DeepEqual(got, want) {
				failures = append(failures, fmt.Sprintf("%s: outcome %s row\n  got:  %v\n  want: %v", scenario.Name, outcome.CommandID, got, want))
			}
		}
	}

	return failures, nil
}

// outcomeRow is a PostgreSQL outcome row with its jsonb columns parsed, since jsonb normalizes
// formatting.
type outcomeRow struct {
	Status    string
	Digest    string
	Response  string
	Events    any
	Rejection any
	Code      string
}

func postgresOutcome(ctx context.Context, pool *pgxpool.Pool, commandID string) (*outcomeRow, error) {
	var (
		row       outcomeRow
		response  []byte
		events    string
		rejection *string
		code      *string
	)

	err := pool.QueryRow(ctx, `SELECT status, digest, response, events::text, rejection::text, code
		FROM evt_events__evt_command_outcomes WHERE command_id = $1`, commandID).
		Scan(&row.Status, &row.Digest, &response, &events, &rejection, &code)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	row.Response = base64.StdEncoding.EncodeToString(response)
	if err = json.Unmarshal([]byte(events), &row.Events); err != nil {
		return nil, err
	}

	if rejection != nil {
		if err = json.Unmarshal([]byte(*rejection), &row.Rejection); err != nil {
			return nil, err
		}
	}

	if code != nil {
		row.Code = *code
	}

	if len(response) == 0 {
		row.Response = ""
	}

	return &row, nil
}

// expectedOutcomeRow derives the PostgreSQL row from a fixture's DynamoDB item.
func expectedOutcomeRow(outcome compat.OutcomeExpectation) (*outcomeRow, error) {
	text := func(name string) string {
		attribute, ok := outcome.DynamoItem[name].(map[string]any)
		if !ok {
			return ""
		}

		if value, isString := attribute["S"].(string); isString {
			return value
		}

		return ""
	}

	row := outcomeRow{Status: text("status"), Digest: text("digest"), Response: text("response"), Events: []any{}, Code: text("code")}

	if events := text("events"); events != "" {
		if err := json.Unmarshal([]byte(events), &row.Events); err != nil {
			return nil, err
		}
	}

	if rejection := text("rejection"); rejection != "" {
		if err := json.Unmarshal([]byte(rejection), &row.Rejection); err != nil {
			return nil, err
		}
	}

	return &row, nil
}

func partitionItems(ctx context.Context, b backend, pk string) ([]map[string]types.AttributeValue, error) {
	return partitionItemsIn(ctx, b, b.table, pk)
}

func partitionItemsIn(ctx context.Context, b backend, table, pk string) ([]map[string]types.AttributeValue, error) {
	paginator := dynamodb.NewQueryPaginator(b.dynamo, &dynamodb.QueryInput{
		TableName:                 aws.String(table),
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

func expectedRows(encodedEvents []string, encodedSnapshot *string) ([]row, *row, error) {
	events := []row{}
	for _, encoded := range encodedEvents {
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

	if encodedSnapshot == nil {
		return events, nil, nil
	}

	var snapshot evt.SerializedSnapshot
	if err := json.Unmarshal([]byte(*encodedSnapshot), &snapshot); err != nil {
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
