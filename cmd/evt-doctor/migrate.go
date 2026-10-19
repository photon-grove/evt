package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/postgres"
)

// migrateHeadsCommand is the subcommand that migrates a v0.8 store to per-stream head items.
const migrateHeadsCommand = "migrate-heads"

// Exit statuses of migrate-heads.
const (
	exitOK      = 0
	exitFailed  = 1
	exitUsage   = 2
	maxListed   = 50
	pgOpTimeout = 30 * time.Minute
)

const migrateHeadsUsage = `usage: evt-doctor migrate-heads [flags]

Migrates a store written by v0.8 (streams with no head items) to per-stream head items and
declares the required stream-heads feature (storage spec, Stream Heads, Migration). The migration
is stop-the-world and one-way; this tool runs steps 3 to 5.

  1. Stop writers and take a backup. Stop every v0.8 writer and start no head-aware writer.
     Enforce it: an IAM deny on PutItem, UpdateItem, DeleteItem, BatchWriteItem, and
     ConditionCheckItem (the actions TransactWriteItems is authorized by) for every principal except
     this tool's role (an identity policy, a table resource policy, or a service control policy),
     held until step 6. Take an on-demand backup or
     confirm point-in-time recovery; rollback is a restore.
  2. Upgrade readers: scan and rebuild jobs, evt-doctor, and every Streams, ESM, or Pipes
     consumer (skip sk < 1), before any head item exists.
  3. Backfill   evt-doctor migrate-heads -confirm-writers-stopped
  4. Verify     evt-doctor migrate-heads -verify
  5. Declare    the default run backfills, verifies, verifies again, declares, and verifies once more.
  6. Start the new writers, then lift the step-1 deny. No v0.8 writer may ever run again.

Modes:
  (default)   backfill, verify, re-verify, declare stream-heads, verify. Needs -confirm-writers-stopped.
  -verify     read-only: every stream's head must equal its derived head. Exits 1 otherwise.
  -repair     raise a low head, create a missing one, fix a low ttl. Needs -confirm-writers-stopped.
  -repair -allow-lower
              also lower a head above the derived head. Every writer must be stopped.
  -dry-run    with the default mode or -repair: scan and count, write nothing.

Flags:
`

// migrateHeadsFlags are the parsed flags of the subcommand.
type migrateHeadsFlags struct {
	backend, region, endpoint, eventsTable, snapshotsTable, dsn string

	verify, repair, allowLower, dryRun, confirm bool

	pageSize, concurrency, maxFindings int
}

// runMigrateHeads is the migrate-heads entry point. It returns the process exit status.
func runMigrateHeads(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	var flags migrateHeadsFlags

	set := flag.NewFlagSet(migrateHeadsCommand, flag.ContinueOnError)
	set.SetOutput(stderr)
	set.Usage = func() {
		say(stderr, "%s", migrateHeadsUsage)
		set.PrintDefaults()
	}

	set.StringVar(&flags.backend, "backend", "dynamodb", "backend to migrate: \"dynamodb\" or \"postgres\"")
	set.StringVar(&flags.region, "region", envOr("AWS_REGION", "us-west-2"), "AWS region (DynamoDB backend)")
	set.StringVar(&flags.endpoint, "endpoint", resolveLocalEndpoint(), "AWS emulator endpoint (DynamoDB backend)")
	set.StringVar(&flags.eventsTable, "events-table", "", "event-log table name (defaults depend on the backend)")
	set.StringVar(&flags.snapshotsTable, "snapshots-table", "evt_snapshots", "snapshot table name (PostgreSQL backend)")
	set.StringVar(&flags.dsn, "dsn", envOr("DATABASE_URL", "postgres://evt:evt@localhost:5432/evt_local?sslmode=disable"), "PostgreSQL connection string (PostgreSQL backend)")
	set.BoolVar(&flags.verify, "verify", false, "read-only: require every stream's head to equal its derived head")
	set.BoolVar(&flags.repair, "repair", false, "raise low heads, create missing ones, fix low ttls")
	set.BoolVar(&flags.allowLower, "allow-lower", false, "with -repair: also lower heads above the derived head")
	set.BoolVar(&flags.dryRun, "dry-run", false, "scan and count without writing")
	set.BoolVar(&flags.confirm, "confirm-writers-stopped", false, "confirm every older writer is stopped and fenced (migration step 1)")
	set.IntVar(&flags.pageSize, "page-size", 0, "DynamoDB scan page size in items (0: DynamoDB's 1 MB default)")
	set.IntVar(&flags.concurrency, "concurrency", 0, "concurrent DynamoDB head writes (0: 8)")
	set.IntVar(&flags.maxFindings, "max-findings", maxListed, "findings to list before summarizing the rest")

	if err := set.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}

		return exitUsage
	}

	if set.NArg() > 0 {
		say(stderr, "unexpected argument %q; migrate-heads takes flags only\n", set.Arg(0))

		return exitUsage
	}

	if status := validateMigrateFlags(&flags, stderr); status != exitOK {
		return status
	}

	switch flags.backend {
	case "dynamodb", "":
		return migrateDynamo(ctx, &flags, stdout, stderr)
	case "postgres":
		return migratePostgres(ctx, &flags, stdout, stderr)
	default:
		say(stderr, "unknown backend %q; expected \"dynamodb\" or \"postgres\"\n", flags.backend)

		return exitUsage
	}
}

func validateMigrateFlags(flags *migrateHeadsFlags, stderr io.Writer) int {
	switch {
	case flags.verify && (flags.repair || flags.allowLower || flags.dryRun):
		sayln(stderr, "-verify is read-only and takes no other mode flag")
	case flags.allowLower && !flags.repair:
		sayln(stderr, "-allow-lower needs -repair")
	case !flags.verify && !flags.dryRun && !flags.confirm:
		sayln(stderr, evt.ErrWritersNotConfirmed.Error()+
			": stop every older writer, enforce it with an access deny, take a backup, and pass "+
			"-confirm-writers-stopped (run with -h for the procedure, or -dry-run to only count)")
	default:
		return exitOK
	}

	return exitUsage
}

// printChecklist lists the operator steps the tool cannot perform or check.
func printChecklist(w io.Writer, flags *migrateHeadsFlags) {
	sayln(w, "migrate-heads: operator steps this tool cannot check, confirmed by -confirm-writers-stopped:")

	if flags.backend == "postgres" {
		sayln(w, "  - every v0.8 writer is stopped, and none will start until the new writers do")
		sayln(w, "  - a backup exists (rollback is a restore)")
	} else {
		sayln(w, "  - every v0.8 writer is stopped and an access deny on PutItem, UpdateItem, DeleteItem,")
		sayln(w, "    BatchWriteItem, and ConditionCheckItem holds for every principal except this role")
		sayln(w, "  - an on-demand backup exists or point-in-time recovery is on (rollback is a restore)")
	}

	sayln(w, "  - readers are upgraded to skip head items (sk < 1), including Streams consumers")
}

// printFindings lists findings up to the limit and summarizes the rest.
func printFindings(w io.Writer, label string, findings []evt.HeadFinding, total, limit int) {
	if total == 0 {
		return
	}

	say(w, "%s: %d\n", label, total)

	for i, finding := range findings {
		if i >= limit {
			break
		}

		say(w, "  %s\n", finding)
	}

	if listed := min(len(findings), limit); total > listed {
		say(w, "  ... and %d more\n", total-listed)
	}
}

func migrateDynamo(ctx context.Context, flags *migrateHeadsFlags, stdout, stderr io.Writer) int {
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
	migration := repo.HeadsMigration(dynamo.HeadsMigrationConfig{
		PageSize:    int32(max(0, min(flags.pageSize, 1_000_000))), //nolint:gosec // clamped to a small positive range
		Concurrency: flags.concurrency,
		Progress: func(phase dynamo.HeadsMigrationPhase) {
			say(stdout, "migrate-heads: %s\n", phase)
		},
	})

	switch {
	case flags.verify:
		return verifyDynamo(ctx, migration, flags, stdout, stderr)
	case flags.repair:
		return repairDynamo(ctx, migration, flags, stdout, stderr)
	}

	if !flags.dryRun {
		printChecklist(stdout, flags)
	}

	report, err := migration.Migrate(ctx, dynamo.HeadsMigrateOptions{
		ConfirmWritersStopped: flags.confirm,
		DryRun:                flags.dryRun,
	})
	if errors.Is(err, evt.ErrHeadsAlreadyDeclared) {
		sayln(stdout, alreadyDeclaredNotice)

		return verifyDynamo(ctx, migration, flags, stdout, stderr)
	}

	return reportDynamoMigration(stdout, stderr, flags, report, err)
}

// alreadyDeclaredNotice explains why a migration of a migrated store only verifies it. Rerunning
// the migration after an interruption that came after the declaration is safe and ends here.
const alreadyDeclaredNotice = "migrate-heads: the store already declares stream-heads; nothing to migrate. " +
	"Verifying it instead (use -repair if a stray older writer left drift)."

func verifyDynamo(
	ctx context.Context,
	migration *dynamo.HeadsMigration,
	flags *migrateHeadsFlags,
	stdout, stderr io.Writer,
) int {
	report, err := migration.Verify(ctx)
	if err != nil {
		say(stderr, "migrate-heads: verify: %v\n", err)

		return exitFailed
	}

	return printDynamoVerify(stdout, report, flags.maxFindings)
}

func reportDynamoMigration(
	stdout, stderr io.Writer,
	flags *migrateHeadsFlags,
	report dynamo.HeadsMigrationReport,
	err error,
) int {
	b := report.Backfill

	if b.Streams > 0 || err == nil {
		verb := "wrote"
		if b.DryRun {
			verb = "would write"
		}

		say(stdout,
			"migrate-heads: %d stream(s): %s %d head item(s); %d already had one, %d empty or expired, %d at head 0, %d raced, %d without a ttl because their items mix ttls\n",
			b.Streams, verb, b.Written, b.Present, b.Empty, b.ZeroHead, b.Raced, b.MixedTTL)
	}

	var unverified *evt.HeadsNotVerifiedError

	switch {
	case errors.As(err, &unverified):
		printFindings(stderr, "migrate-heads: "+unverified.Phase+": streams whose head does not match the log",
			unverified.Findings, unverified.Total, flags.maxFindings)

		if report.Declared {
			sayln(stderr, "migrate-heads: stream-heads WAS declared; a stray older writer may be running.")
			sayln(stderr, "  stop it, then run -repair (and -repair -allow-lower for higher heads) and -verify")
		} else {
			sayln(stderr, "migrate-heads: stream-heads was NOT declared. Fix the streams above (-repair), then rerun.")
		}

		return exitFailed
	case err != nil:
		say(stderr, "migrate-heads: %v\n", err)

		if report.Declared {
			sayln(stderr, "migrate-heads: stream-heads WAS declared before this failure; run -verify")
		}

		return exitFailed
	case flags.dryRun:
		sayln(stdout, "migrate-heads: dry run; nothing was written")

		return exitOK
	}

	say(stdout, "migrate-heads: declared %s (marker version %d); every stream verified before and after\n",
		evt.FeatureStreamHeads, report.Manifest.Version)
	sayln(stdout, "migrate-heads: next, start the head-aware writers, then lift the access deny")

	printFindings(stdout, "migrate-heads: warnings", report.Verify.Warnings, len(report.Verify.Warnings), flags.maxFindings)

	return exitOK
}

func printDynamoVerify(stdout io.Writer, report dynamo.HeadsVerifyReport, limit int) int {
	say(stdout, "migrate-heads: verified %d stream(s)\n", report.Streams)

	printFindings(stdout, "migrate-heads: warnings", report.Warnings, len(report.Warnings), limit)

	if report.OK() {
		sayln(stdout, "migrate-heads: every stream's head matches its log")

		return exitOK
	}

	printFindings(stdout, "migrate-heads: streams whose head does not match the log", report.Findings, len(report.Findings), limit)

	return exitFailed
}

func repairDynamo(
	ctx context.Context,
	migration *dynamo.HeadsMigration,
	flags *migrateHeadsFlags,
	stdout, stderr io.Writer,
) int {
	if !flags.dryRun {
		printChecklist(stdout, flags)
	}

	report, err := migration.Repair(ctx, dynamo.HeadsRepairOptions{
		ConfirmWritersStopped: flags.confirm,
		AllowLower:            flags.allowLower,
		DryRun:                flags.dryRun,
	})
	if err != nil {
		say(stderr, "migrate-heads: repair: %v\n", err)

		return exitFailed
	}

	verb := "repaired"
	if report.DryRun {
		verb = "would repair"
	}

	say(stdout, "migrate-heads: %s %d stream(s); %d raced and were left\n", verb, len(report.Repaired), report.Raced)
	printFindings(stdout, "migrate-heads: "+verb, report.Repaired, len(report.Repaired), flags.maxFindings)

	if len(report.Left) > 0 {
		printFindings(stderr, "migrate-heads: left untouched (use -allow-lower for higher heads; an orphan head item needs a manual look)",
			report.Left, len(report.Left), flags.maxFindings)

		return exitFailed
	}

	return exitOK
}

func migratePostgres(ctx context.Context, flags *migrateHeadsFlags, stdout, stderr io.Writer) int {
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

	if err = pool.Ping(ctx); err != nil {
		say(stderr, "ping PostgreSQL: %v\n", err)

		return exitFailed
	}

	repo := postgres.NewRepository(pool,
		postgres.WithEventsTable(events), postgres.WithSnapshotsTable(flags.snapshotsTable))

	switch {
	case flags.verify:
		return verifyPostgres(ctx, repo, flags, stdout, stderr)
	case flags.repair:
		return repairPostgres(ctx, repo, flags, stdout, stderr)
	}

	if !flags.dryRun {
		printChecklist(stdout, flags)
	}

	report, err := repo.MigrateStreamHeads(ctx, postgres.StreamHeadsMigrateOptions{
		ConfirmWritersStopped: flags.confirm,
		DryRun:                flags.dryRun,
	})
	if errors.Is(err, evt.ErrHeadsAlreadyDeclared) {
		sayln(stdout, alreadyDeclaredNotice)

		return verifyPostgres(ctx, repo, flags, stdout, stderr)
	}

	var unverified *evt.HeadsNotVerifiedError

	switch {
	case errors.As(err, &unverified):
		printFindings(stderr, "migrate-heads: the transaction was aborted and nothing was changed; streams whose head does not match the log",
			unverified.Findings, unverified.Total, flags.maxFindings)
		sayln(stderr, "  fix them with -repair (and -allow-lower for higher heads), then rerun")

		return exitFailed
	case err != nil:
		say(stderr, "migrate-heads: %v\n", err)

		return exitFailed
	case report.DryRun:
		say(stdout, "migrate-heads: dry run: %d stream(s); would insert %d head row(s); nothing was written\n",
			report.Streams, report.Inserted)

		return exitOK
	}

	say(stdout, "migrate-heads: %d stream(s); inserted %d head row(s); declared %s (marker version %d) in one transaction\n",
		report.Streams, report.Inserted, evt.FeatureStreamHeads, report.Manifest.Version)
	sayln(stdout, "migrate-heads: next, start the head-aware writers. No v0.8 writer may run again.")

	return exitOK
}

func verifyPostgres(ctx context.Context, repo *postgres.Repository, flags *migrateHeadsFlags, stdout, stderr io.Writer) int {
	report, err := repo.VerifyStreamHeads(ctx)
	if err != nil {
		say(stderr, "migrate-heads: verify: %v\n", err)

		return exitFailed
	}

	say(stdout, "migrate-heads: verified %d stream(s)\n", report.Streams)

	if report.OK() {
		sayln(stdout, "migrate-heads: every stream's head matches its log")

		return exitOK
	}

	printFindings(stdout, "migrate-heads: streams whose head does not match the log",
		report.Findings, int(report.Total), flags.maxFindings)

	return exitFailed
}

func repairPostgres(ctx context.Context, repo *postgres.Repository, flags *migrateHeadsFlags, stdout, stderr io.Writer) int {
	if !flags.dryRun {
		printChecklist(stdout, flags)
	}

	report, err := repo.RepairStreamHeads(ctx, postgres.StreamHeadsRepairOptions{
		ConfirmWritersStopped: flags.confirm,
		AllowLower:            flags.allowLower,
		DryRun:                flags.dryRun,
	})
	if err != nil {
		say(stderr, "migrate-heads: repair: %v\n", err)

		return exitFailed
	}

	if report.DryRun {
		printFindings(stdout, "migrate-heads: would repair", report.Repaired, len(report.Repaired), flags.maxFindings)

		if len(report.Left) > 0 {
			printFindings(stderr, "migrate-heads: would leave untouched (use -allow-lower for higher heads; an orphan head row needs a manual look)",
				report.Left, len(report.Left), flags.maxFindings)

			return exitFailed
		}

		return exitOK
	}

	say(stdout, "migrate-heads: inserted %d, raised %d, lowered %d head row(s)\n",
		report.Inserted, report.Raised, report.Lowered)

	if len(report.Left) > 0 {
		printFindings(stderr, "migrate-heads: left untouched (use -allow-lower for higher heads; an orphan head row needs a manual look)",
			report.Left, len(report.Left), flags.maxFindings)

		return exitFailed
	}

	return exitOK
}

// reportDynamoHeadDrift is the read-only head-drift check of a plain evt-doctor run: when the
// store declares stream-heads it compares every stream's head item with its log. It returns false
// when drift was found. A store that has not declared the feature, or declares one the migration
// does not model, is reported and does not fail the run.
func reportDynamoHeadDrift(ctx context.Context, client dynamo.Client, eventsTable string, out io.Writer) bool {
	migration := dynamo.NewRepository(client, eventsTable).HeadsMigration(dynamo.HeadsMigrationConfig{})

	manifest, err := migration.CheckPreconditions(ctx)
	if err != nil {
		return driftCheckFailed(out, err)
	}

	if manifest == nil || !manifest.DeclaresStreamHeads() {
		sayln(out, "head-drift check: stream-heads is not declared; run \"evt-doctor migrate-heads\" to migrate this store")

		return true
	}

	report, err := migration.Verify(ctx)
	if err != nil {
		return driftCheckFailed(out, err)
	}

	return printHeadDrift(out, report.Streams, report.Findings, len(report.Findings), report.Warnings)
}

// reportPostgresHeadDrift is reportDynamoHeadDrift for PostgreSQL.
func reportPostgresHeadDrift(ctx context.Context, repo *postgres.Repository, out io.Writer) bool {
	manifest, err := repo.FeatureMarker(nil).Manifest(ctx)
	if err != nil {
		return driftCheckFailed(out, err)
	}

	if manifest == nil || !manifest.DeclaresStreamHeads() {
		sayln(out, "head-drift check: stream-heads is not declared; run \"evt-doctor migrate-heads\" to migrate this store")

		return true
	}

	report, err := repo.VerifyStreamHeads(ctx)
	if err != nil {
		return driftCheckFailed(out, err)
	}

	return printHeadDrift(out, int(report.Streams), report.Findings, int(report.Total), nil)
}

// driftCheckFailed reports why the head-drift check could not run and returns whether the run may
// still succeed. Only a store that declares a feature the check does not model (journal) is
// skipped: every other failure (a denied or failed read, say) means nothing was checked, so the run
// fails rather than reporting a clean bill of health.
func driftCheckFailed(out io.Writer, err error) bool {
	if evt.IsUnsupportedFeatureErr(err) {
		say(out, "head-drift check: skipped: %v\n", err)

		return true
	}

	say(out, "head-drift check: could not run: %v\n", err)

	return false
}

func printHeadDrift(out io.Writer, streams int, findings []evt.HeadFinding, total int, warnings []evt.HeadFinding) bool {
	printFindings(out, "head-drift check: warnings", warnings, len(warnings), maxListed)

	if total == 0 {
		say(out, "head-drift check: %d stream(s), every head matches its log\n", streams)

		return true
	}

	say(out, "head-drift check: %d stream(s), %d with drift (repair with \"evt-doctor migrate-heads -repair\", every writer stopped)\n", streams, total)
	printFindings(out, "head-drift check: drift", findings, total, maxListed)

	return false
}

// say and sayln write to the operator's terminal. A failed write has nowhere to be reported, so it
// is dropped.
func say(w io.Writer, format string, args ...any) {
	fmt.Fprintf(w, format, args...) //nolint:errcheck // a failed terminal write has nowhere to be reported
}

func sayln(w io.Writer, args ...any) {
	fmt.Fprintln(w, args...) //nolint:errcheck // a failed terminal write has nowhere to be reported
}
