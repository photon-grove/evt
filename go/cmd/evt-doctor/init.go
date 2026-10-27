package main

import (
	"context"
	"errors"
	"flag"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/jackc/pgx/v5/pgxpool"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/dynamo"
	"github.com/photon-grove/evt/go/backends/postgres"
)

// initCommand is the subcommand that initializes a fresh store.
const initCommand = "init"

const initUsage = `usage: evt-doctor init [flags]

Initializes a fresh store: declares the required stream-heads feature on a store that holds no
data (storage spec, Stream Heads, Required feature). Run it once, at deploy time, before any
writer starts. It is idempotent: a store that already declares the feature is left alone.

DynamoDB writers refuse a store that is not initialized (StoreNotInitialized) instead of scanning
it, so their roles need no dynamodb:Scan. This command does scan, strongly consistently, to verify
the table holds nothing but engine rows, so it needs dynamodb:GetItem, dynamodb:Scan, and
dynamodb:PutItem on the event-log table. A store that already holds data without the declaration
is refused: migrate it with "evt-doctor migrate-heads" instead.

On PostgreSQL it runs the schema setup first, then declares the feature when the events and
snapshots tables are empty. PostgreSQL writers also initialize a fresh store themselves.

Flags:
`

// initFlags are the parsed flags of the subcommand.
type initFlags struct {
	backend, region, endpoint, eventsTable, snapshotsTable, dsn string
}

// runInit is the init entry point. It returns the process exit status.
func runInit(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	var flags initFlags

	set := flag.NewFlagSet(initCommand, flag.ContinueOnError)
	set.SetOutput(stderr)
	set.Usage = func() {
		say(stderr, "%s", initUsage)
		set.PrintDefaults()
	}

	set.StringVar(&flags.backend, "backend", "dynamodb", "backend to initialize: \"dynamodb\" or \"postgres\"")
	set.StringVar(&flags.region, "region", envOr("AWS_REGION", "us-west-2"), "AWS region (DynamoDB backend)")
	set.StringVar(&flags.endpoint, "endpoint", resolveLocalEndpoint(), "AWS emulator endpoint (DynamoDB backend)")
	set.StringVar(&flags.eventsTable, "events-table", "", "event-log table name (defaults depend on the backend)")
	set.StringVar(&flags.snapshotsTable, "snapshots-table", "evt_snapshots", "snapshot table name (PostgreSQL backend)")
	set.StringVar(&flags.dsn, "dsn", envOr("DATABASE_URL", "postgres://evt:evt@localhost:5432/evt_local?sslmode=disable"), "PostgreSQL connection string (PostgreSQL backend)")

	if err := set.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}

		return exitUsage
	}

	if set.NArg() > 0 {
		say(stderr, "unexpected argument %q; init takes flags only\n", set.Arg(0))

		return exitUsage
	}

	switch flags.backend {
	case "dynamodb", "":
		return initDynamo(ctx, &flags, stdout, stderr)
	case "postgres":
		return initPostgres(ctx, &flags, stdout, stderr)
	default:
		say(stderr, "unknown backend %q; expected \"dynamodb\" or \"postgres\"\n", flags.backend)

		return exitUsage
	}
}

func initDynamo(ctx context.Context, flags *initFlags, stdout, stderr io.Writer) int {
	table := flags.eventsTable
	if table == "" {
		table = "evt-local-event-log"
	}

	awsCfg, err := newAWSConfig(ctx, flags.region, flags.endpoint)
	if err != nil {
		say(stderr, "load AWS config: %v\n", err)

		return exitFailed
	}

	repo := dynamo.NewRepository(dynamodb.NewFromConfig(*awsCfg), table)
	declared, err := repo.Initialize(ctx)

	return reportInit(stdout, stderr, table, declared, err)
}

func initPostgres(ctx context.Context, flags *initFlags, stdout, stderr io.Writer) int {
	events := flags.eventsTable
	if events == "" {
		events = "evt_events"
	}

	ctx, cancel := context.WithTimeout(ctx, pgOpTimeout)
	defer cancel()

	pool, err := pgxpool.New(ctx, flags.dsn)
	if err != nil {
		say(stderr, "connect to PostgreSQL: %v\n", err)

		return exitFailed
	}
	defer pool.Close()

	repo := postgres.NewRepository(pool,
		postgres.WithEventsTable(events), postgres.WithSnapshotsTable(flags.snapshotsTable))

	if err = repo.EnsureSchema(ctx); err != nil {
		say(stderr, "init: schema setup: %v\n", err)

		return exitFailed
	}

	declared, err := repo.Initialize(ctx)

	return reportInit(stdout, stderr, events, declared, err)
}

// reportInit prints the outcome of an initialization and returns the exit status.
func reportInit(stdout, stderr io.Writer, store string, declared bool, err error) int {
	switch {
	case evt.IsStreamHeadsNotMigratedErr(err):
		say(stderr, "init: %s holds data but does not declare stream-heads; it was written before "+
			"stream heads. Migrate it with \"evt-doctor migrate-heads\" instead. Nothing was declared.\n", store)

		return exitFailed
	case err != nil:
		say(stderr, "init: %v\n", err)

		return exitFailed
	case declared:
		say(stdout, "init: declared stream-heads on %s; writers may start\n", store)
	default:
		say(stdout, "init: %s already declares stream-heads; nothing to do\n", store)
	}

	return exitOK
}
