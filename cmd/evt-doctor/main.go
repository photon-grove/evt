// Command evt-doctor is a CLI for inspecting and diagnosing an evt event log: the table schema,
// the pre-existing streams in reserved namespaces that must be renamed (migrated) before the
// v0.9 features are enabled (storage spec, Reserved Namespaces), and, once a store declares
// stream-heads, drift between each stream's head item and its log. It runs against DynamoDB by
// default, or against PostgreSQL with -backend postgres.
//
// The migrate-heads subcommand migrates a store written by v0.8 to per-stream head items (storage
// spec, Stream Heads, Migration): backfill, verify, repair, and declare. Run
// "evt-doctor migrate-heads -h" for its procedure and flags.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/photon-grove/evt/postgres"
)

// callTimeout bounds every AWS call the tool makes.
const callTimeout = time.Minute

type tableSpec struct {
	Name       string   `json:"name"`
	HashKey    string   `json:"hashKey"`
	RangeKey   string   `json:"rangeKey"`
	RangeType  string   `json:"rangeType"`
	GSI        []string `json:"gsi,omitempty"`
	Stream     bool     `json:"stream"`
	SnapshotSK string   `json:"snapshotSk,omitempty"`
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == migrateHeadsCommand {
		os.Exit(runMigrateHeads(context.Background(), os.Args[2:], os.Stdout, os.Stderr))
	}

	var (
		region      = flag.String("region", envOr("AWS_REGION", "us-west-2"), "AWS region (DynamoDB backend)")
		endpoint    = flag.String("endpoint", resolveLocalEndpoint(), "AWS emulator endpoint (DynamoDB backend)")
		eventsTable = flag.String("events-table", "", "event-log table name (defaults depend on the backend)")
		viewsTable  = flag.String("views-table", "evt-local-entity-views", "entity-views table name")
		snapTable   = flag.String("snapshots-table", "evt_snapshots", "snapshot table name (PostgreSQL backend)")
		dsn         = flag.String("dsn", envOr("DATABASE_URL", "postgres://evt:evt@localhost:5432/evt_local?sslmode=disable"), "PostgreSQL connection string (PostgreSQL backend)")
		backend     = flag.String("backend", "dynamodb", "backend to diagnose: \"dynamodb\" or \"postgres\"")
		printSchema = flag.Bool("schema", false, "print required DynamoDB table schema and exit")
	)
	flag.Parse()

	if flag.NArg() > 0 {
		exitf("unexpected argument %q; the %s subcommand must come first, before any flag", flag.Arg(0), migrateHeadsCommand)
	}

	if *printSchema {
		if err := json.NewEncoder(os.Stdout).Encode(requiredSchema(*eventsTable, *viewsTable)); err != nil {
			exitf("encode schema: %v", err)
		}

		return
	}

	ctx := context.Background()

	switch *backend {
	case "dynamodb", "":
		events := *eventsTable
		if events == "" {
			events = "evt-local-event-log"
		}

		checkDynamo(ctx, checkConfig{
			region:      *region,
			endpoint:    *endpoint,
			eventsTable: events,
			viewsTable:  *viewsTable,
		})
	case "postgres":
		events := *eventsTable
		if events == "" {
			events = "evt_events"
		}

		checkPostgres(ctx, checkConfig{
			dsn:            *dsn,
			eventsTable:    events,
			snapshotsTable: *snapTable,
		})
	default:
		exitf("unknown backend %q; expected \"dynamodb\" or \"postgres\"", *backend)
	}
}

// checkConfig carries the resolved flags for one doctor run.
type checkConfig struct {
	region         string
	endpoint       string
	dsn            string
	eventsTable    string
	viewsTable     string
	snapshotsTable string
}

// checkDynamo describes the configured tables and reports pre-existing streams in reserved
// namespaces (storage spec, Reserved Namespaces).
func checkDynamo(ctx context.Context, cfg checkConfig) {
	awsCfg, err := newAWSConfig(ctx, cfg.region, cfg.endpoint)
	if err != nil {
		exitf("load AWS config: %v", err)
	}

	client := dynamodb.NewFromConfig(*awsCfg)
	for _, table := range []string{cfg.eventsTable, cfg.viewsTable} {
		out, derr := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &table})
		if derr != nil {
			exitf("describe %s: %v", table, derr)
		}
		if out.Table == nil {
			exitf("describe %s: empty table response", table)
		}

		fmt.Printf("%s: %s\n", table, out.Table.TableStatus)
	}

	ids, err := reservedStreamIDs(ctx, client, cfg.eventsTable)
	if err != nil {
		exitf("reserved-namespace check: %v", err)
	}

	reportReservedStreams(ids)

	if !reportDynamoHeadDrift(ctx, client, cfg.eventsTable, os.Stdout) {
		os.Exit(1)
	}
}

// checkPostgres reports pre-existing streams in reserved namespaces against the configured events
// and snapshots tables, using the queries the storage spec pins:
//
//	SELECT DISTINCT entity_id FROM evt_events WHERE entity_id LIKE 'SYSTEM#%' OR entity_id LIKE 'command-outcome#%';
//	SELECT entity_id FROM evt_snapshots WHERE entity_id LIKE 'SYSTEM#%' OR entity_id LIKE 'command-outcome#%';
//
// Engine-written rows are exempt structurally: the engine-row relations derive from the events
// table name through the reserved infix, so the two queries never see them.
func checkPostgres(ctx context.Context, cfg checkConfig) {
	pool, err := pgxpool.New(ctx, cfg.dsn)
	if err != nil {
		exitf("connect to PostgreSQL: %v", err)
	}
	defer pool.Close()

	err = pool.Ping(ctx)
	if err != nil {
		exitf("ping PostgreSQL: %v", err)
	}

	ids, err := postgresReservedStreamIDs(ctx, pool, cfg.eventsTable, cfg.snapshotsTable)
	if err != nil {
		exitf("reserved-namespace check: %v", err)
	}

	reportReservedStreams(ids)

	repo := postgres.NewRepository(pool,
		postgres.WithEventsTable(cfg.eventsTable), postgres.WithSnapshotsTable(cfg.snapshotsTable))
	if !reportPostgresHeadDrift(ctx, repo, os.Stdout) {
		os.Exit(1)
	}
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}

	return fallback
}

func resolveLocalEndpoint() string {
	if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
		return endpoint
	}

	return os.Getenv("LOCALSTACK_ENDPOINT")
}

func newAWSConfig(ctx context.Context, region, localEndpoint string) (*aws.Config, error) {
	// No call outlives callTimeout: a stalled endpoint fails the run instead of hanging it.
	httpClient := awshttp.NewBuildableClient().WithTimeout(callTimeout)

	if localEndpoint == "" {
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region), config.WithHTTPClient(httpClient))
		if err != nil {
			return nil, err
		}

		return &cfg, nil
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithRetryMaxAttempts(1),
		config.WithHTTPClient(httpClient),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "test",
				SecretAccessKey: "test",
				SessionToken:    "test",
				Source:          "local emulator credentials",
			},
		}),
		config.WithBaseEndpoint(localEndpoint),
	)
	if err != nil {
		return nil, err
	}

	return &cfg, nil
}

func requiredSchema(eventsTable, viewsTable string) []tableSpec {
	return []tableSpec{
		{
			Name:       eventsTable,
			HashKey:    "pk",
			RangeKey:   "sk",
			RangeType:  "N",
			Stream:     true,
			SnapshotSK: "0",
		},
		{
			Name:      viewsTable,
			HashKey:   "pk",
			RangeKey:  "sk",
			RangeType: "S",
			GSI:       []string{"entityType-index"},
			Stream:    false,
		},
	}
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
