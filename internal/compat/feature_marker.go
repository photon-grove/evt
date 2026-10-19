package compat

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/postgres"
)

// FeatureMarkerFixtureFile holds the shared C9 storage and transaction vectors.
const FeatureMarkerFixtureFile = "feature-marker.json"

// FeatureMarkerFixtures pins declarations and ordered commit requests, not SDK struct JSON.
// JSON texts remain strings so consumers must compare their bytes, not just parsed values.
type FeatureMarkerFixtures struct {
	Declarations    []FeatureDeclarationVector `json:"declarations"`
	Commits         []FeatureCommitVector      `json:"commits"`
	StreamHeads     []StreamHeadVector         `json:"streamHeads"`
	PostgresSchemas []FeatureSchemaVector      `json:"postgresSchemas"`
}

// FeatureDeclarationInput replaces the three manifest collections in one Declare callback.
// Declarations run sequentially against the same store, starting without a marker.
type FeatureDeclarationInput struct {
	Features         map[string]evt.FeatureFormat `json:"features"`
	HistoryDependent []evt.EntityType             `json:"historyDependent"`
	Retention        []evt.EntityType             `json:"retention"`
}

func (in FeatureDeclarationInput) apply(m *evt.FeatureManifest) {
	m.Features = in.Features
	m.HistoryDependent = in.HistoryDependent
	m.Retention = in.Retention
}

// FeatureDeclarationVector pins one sequential declaration and its stored encodings.
type FeatureDeclarationVector struct {
	Name   string                   `json:"name"`
	Input  FeatureDeclarationInput  `json:"input"`
	Output FeatureDeclarationOutput `json:"output"`
}

// FeatureDeclarationOutput preserves the exact manifest texts and backend row fields.
type FeatureDeclarationOutput struct {
	FeaturesJSON         string           `json:"featuresJson"`
	HistoryDependentJSON string           `json:"historyDependentJson"`
	RetentionJSON        string           `json:"retentionJson"`
	DynamoItem           map[string]any   `json:"dynamoItem"`
	PostgresRow          FeatureMarkerRow `json:"postgresRow"`
	PostgresWrite        FeatureSQLWrite  `json:"postgresWrite"`
}

// FeatureMarkerRow uses the actual PostgreSQL column names, with text columns left unparsed.
type FeatureMarkerRow struct {
	ID               int64  `json:"id"`
	Version          int64  `json:"version"`
	Features         string `json:"features"`
	HistoryDependent string `json:"history_dependent"`
	Retention        string `json:"retention"`
}

// FeatureSQLWrite captures SQL and bound arguments from the declaration write.
type FeatureSQLWrite struct {
	SQL  string `json:"sql"`
	Args []any  `json:"args"`
}

// FeatureCommitVector pins the ordered transaction emitted by a repository commit. For a fresh
// store (no marker declaration) the writer declares stream-heads itself before its first commit:
// Declared is the marker item it stored, and the commit's marker check conditions on its version.
type FeatureCommitVector struct {
	Name     string             `json:"name"`
	Input    FeatureCommitInput `json:"input"`
	Declared map[string]any     `json:"declared,omitempty"`
	Output   []FeatureWriteItem `json:"output"`
}

// FeatureCommitInput describes a commit; MarkerDeclaration names a declaration above, or
// is empty for a fresh store without a marker.
// Each commit uses a fresh store. ViewState is projected by the real DynamoDB view projector.
type FeatureCommitInput struct {
	MarkerDeclaration string         `json:"markerDeclaration"`
	EventsTable       string         `json:"eventsTable"`
	ViewsTable        string         `json:"viewsTable"`
	Event             EventInput     `json:"event"`
	ViewState         string         `json:"viewState"`
	Snapshot          *SnapshotInput `json:"snapshot"`
}

// FeatureWriteItem preserves request order and the complete Put/ConditionCheck wire fields.
type FeatureWriteItem struct {
	Operation                 string            `json:"operation"`
	TableName                 string            `json:"tableName"`
	Item                      map[string]any    `json:"item,omitempty"`
	Key                       map[string]any    `json:"key,omitempty"`
	UpdateExpression          string            `json:"updateExpression,omitempty"`
	ConditionExpression       string            `json:"conditionExpression,omitempty"`
	ExpressionAttributeNames  map[string]string `json:"expressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]any    `json:"expressionAttributeValues,omitempty"`
}

// featureCaptureDynamo delegates execution to the shared fake and records actual repository requests.
type featureCaptureDynamo struct {
	*FakeDynamo
	writes []FeatureWriteItem
}

func (c *featureCaptureDynamo) TransactWriteItems(ctx context.Context, in *dynamodb.TransactWriteItemsInput, opts ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	for _, item := range in.TransactItems {
		var out FeatureWriteItem
		switch {
		case item.Put != nil:
			p := item.Put
			out = FeatureWriteItem{Operation: "Put", TableName: aws.ToString(p.TableName), Item: DynamoItemJSON(p.Item), ConditionExpression: aws.ToString(p.ConditionExpression), ExpressionAttributeNames: p.ExpressionAttributeNames, ExpressionAttributeValues: DynamoItemJSON(p.ExpressionAttributeValues)}
		case item.ConditionCheck != nil:
			p := item.ConditionCheck
			out = FeatureWriteItem{Operation: "ConditionCheck", TableName: aws.ToString(p.TableName), Key: DynamoItemJSON(p.Key), ConditionExpression: aws.ToString(p.ConditionExpression), ExpressionAttributeNames: p.ExpressionAttributeNames, ExpressionAttributeValues: DynamoItemJSON(p.ExpressionAttributeValues)}
		case item.Update != nil:
			p := item.Update
			out = FeatureWriteItem{Operation: "Update", TableName: aws.ToString(p.TableName), Key: DynamoItemJSON(p.Key), UpdateExpression: aws.ToString(p.UpdateExpression), ConditionExpression: aws.ToString(p.ConditionExpression), ExpressionAttributeNames: p.ExpressionAttributeNames, ExpressionAttributeValues: DynamoItemJSON(p.ExpressionAttributeValues)}
		default:
			return nil, fmt.Errorf("feature fixture: unsupported transaction action")
		}
		c.writes = append(c.writes, out)
	}

	// The event-log fake requires numeric sk keys; the real view projector uses a
	// single hash key. Preserve its Put in the fixture, but execute only event-log
	// actions against FakeDynamo. These unconditional view puts cannot reject a commit.
	request := *in
	request.TransactItems = nil
	for _, item := range in.TransactItems {
		if item.Put != nil && aws.ToString(item.Put.TableName) == "views" {
			if item.Put.ConditionExpression != nil {
				return nil, fmt.Errorf("feature fixture: conditional view puts need fake support")
			}
			continue
		}
		request.TransactItems = append(request.TransactItems, item)
	}

	return c.FakeDynamo.TransactWriteItems(ctx, &request, opts...)
}

// featurePG captures the declaration's SQL and bound values. Embedded interfaces deliberately
// leave unrelated DB/Tx methods unimplemented: an unexpected production call fails the generator.
type featurePG struct {
	postgres.DB
	row   *FeatureMarkerRow
	write FeatureSQLWrite
}

type featurePGTx struct {
	pgx.Tx
	db      *featurePG
	pending *FeatureMarkerRow
}

func (db *featurePG) Begin(context.Context) (pgx.Tx, error) {
	return &featurePGTx{db: db}, nil
}

func (tx *featurePGTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	return featurePGScan{row: tx.db.row, existsOnly: strings.HasPrefix(sql, "SELECT to_regclass(")}
}

func (tx *featurePGTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	switch {
	case strings.HasPrefix(sql, "SELECT pg_advisory_xact_lock("):
		return pgconn.NewCommandTag("SELECT 1"), nil
	case strings.HasPrefix(sql, "INSERT INTO ") && len(args) == 3:
		tx.pending = &FeatureMarkerRow{ID: 0, Version: 1, Features: featureArgument[string](args[0]), HistoryDependent: featureArgument[string](args[1]), Retention: featureArgument[string](args[2])}
	case strings.HasPrefix(sql, "UPDATE ") && len(args) == 5:
		if tx.db.row == nil || tx.db.row.Version != featureArgument[int64](args[4]) {
			return pgconn.CommandTag{}, fmt.Errorf("feature fixture: stale PostgreSQL declaration")
		}
		tx.pending = &FeatureMarkerRow{ID: 0, Version: featureArgument[int64](args[0]), Features: featureArgument[string](args[1]), HistoryDependent: featureArgument[string](args[2]), Retention: featureArgument[string](args[3])}
	default:
		return pgconn.CommandTag{}, fmt.Errorf("feature fixture: unexpected SQL %q", sql)
	}

	tx.db.write = FeatureSQLWrite{SQL: sql, Args: append([]any(nil), args...)}

	return pgconn.NewCommandTag("UPDATE 1"), nil
}

func (tx *featurePGTx) Commit(context.Context) error {
	if tx.pending == nil {
		return fmt.Errorf("feature fixture: declaration committed without a write")
	}
	tx.db.row = tx.pending

	return nil
}

func (tx *featurePGTx) Rollback(context.Context) error { return nil }

type featurePGScan struct {
	row        *FeatureMarkerRow
	existsOnly bool
}

func (s featurePGScan) Scan(dest ...any) error {
	if s.existsOnly {
		*featureArgument[*bool](dest[0]) = true

		return nil
	}
	if s.row == nil {
		return pgx.ErrNoRows
	}
	*featureArgument[*int64](dest[0]) = s.row.Version
	*featureArgument[*string](dest[1]) = s.row.Features
	*featureArgument[*string](dest[2]) = s.row.HistoryDependent
	*featureArgument[*string](dest[3]) = s.row.Retention

	return nil
}

// Fail the generator loudly if the production driver argument types change.
func featureArgument[T any](value any) T {
	argument, ok := value.(T)
	if !ok {
		panic(fmt.Sprintf("feature fixture: unexpected driver argument %T", value))
	}

	return argument
}

// GenerateFeatureMarkerFixtures exercises the production declaration and repository write paths.
func GenerateFeatureMarkerFixtures(ctx context.Context) (FeatureMarkerFixtures, error) {
	var fixtures FeatureMarkerFixtures
	fake := NewFakeDynamo()
	pg := &featurePG{}
	dm := dynamo.NewRepository(fake, fakeTable).FeatureMarker(nil)
	pm := postgres.NewRepository(pg).FeatureMarker(nil)
	inputs := []FeatureDeclarationVector{
		{Name: "empty", Input: FeatureDeclarationInput{Features: map[string]evt.FeatureFormat{}, HistoryDependent: []evt.EntityType{}, Retention: []evt.EntityType{}}},
		{Name: "sorted-deduplicated", Input: FeatureDeclarationInput{
			Features:         map[string]evt.FeatureFormat{evt.FeatureJournalCompaction: 2, evt.FeatureJournal: 1, evt.FeatureStreamHeads: 1},
			HistoryDependent: []evt.EntityType{"order", "account", "order", "account"},
			Retention:        []evt.EntityType{"session", "account", "session"},
		}},
		{Name: "clear-sets", Input: FeatureDeclarationInput{Features: map[string]evt.FeatureFormat{}, HistoryDependent: []evt.EntityType{}, Retention: []evt.EntityType{}}},
	}

	for _, vector := range inputs {
		manifest, err := dm.Declare(ctx, vector.Input.apply)
		if err != nil {
			return fixtures, err
		}
		if _, pgErr := pm.Declare(ctx, vector.Input.apply); pgErr != nil {
			return fixtures, pgErr
		}
		out := FeatureDeclarationOutput{DynamoItem: DynamoItemJSON(fake.Items("SYSTEM#FEATURES")[0]), PostgresRow: *pg.row, PostgresWrite: pg.write}
		if out.FeaturesJSON, err = manifest.FeaturesJSON(); err != nil {
			return fixtures, err
		}
		if out.HistoryDependentJSON, err = manifest.HistoryDependentJSON(); err != nil {
			return fixtures, err
		}
		if out.RetentionJSON, err = manifest.RetentionJSON(); err != nil {
			return fixtures, err
		}
		vector.Output = out
		fixtures.Declarations = append(fixtures.Declarations, vector)
	}

	for _, name := range []string{"declared-with-view", "declared-with-view-and-snapshot", "absent-with-view"} {
		vector, err := featureCommitVector(ctx, name, inputs)
		if err != nil {
			return fixtures, err
		}
		fixtures.Commits = append(fixtures.Commits, vector)
	}

	streamHeads, err := GenerateStreamHeadVectors(ctx)
	if err != nil {
		return fixtures, err
	}
	fixtures.StreamHeads = streamHeads

	for _, tables := range [][2]string{{"orders", "orders_entity_type_idx"}, {"evt_events", "evt_snapshots"}} {
		capture := &featureSchemaCapture{}
		repo := postgres.NewRepository(capture, postgres.WithEventsTable(tables[0]), postgres.WithSnapshotsTable(tables[1]))
		if err := repo.EnsureSchema(ctx); err != nil {
			return fixtures, err
		}
		fixtures.PostgresSchemas = append(fixtures.PostgresSchemas, FeatureSchemaVector{EventsTable: tables[0], SnapshotsTable: tables[1], DDL: capture.ddl})
	}

	return fixtures, nil
}

// FeatureSchemaVector supplies executable Go DDL for cross-language schema/migration tests.
// Consumers create any legacy index themselves before executing this generated DDL.
type FeatureSchemaVector struct {
	EventsTable    string `json:"eventsTable"`
	SnapshotsTable string `json:"snapshotsTable"`
	DDL            string `json:"ddl"`
}

type featureSchemaCapture struct {
	postgres.DB
	ddl string
}

func (c *featureSchemaCapture) Begin(context.Context) (pgx.Tx, error) {
	return &featureSchemaTx{capture: c}, nil
}

type featureSchemaTx struct {
	pgx.Tx
	capture *featureSchemaCapture
}

func (tx *featureSchemaTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if strings.HasPrefix(sql, "SELECT pg_advisory_xact_lock(") {
		return pgconn.NewCommandTag("SELECT 1"), nil
	}

	return tx.capture.Exec(ctx, sql, args...)
}

func (tx *featureSchemaTx) Commit(context.Context) error   { return nil }
func (tx *featureSchemaTx) Rollback(context.Context) error { return nil }

func (c *featureSchemaCapture) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if c.ddl != "" || len(args) != 0 {
		return pgconn.CommandTag{}, fmt.Errorf("feature fixture: unexpected schema execution")
	}
	c.ddl = sql

	return pgconn.NewCommandTag("CREATE TABLE"), nil
}

func featureCommitVector(ctx context.Context, name string, declarations []FeatureDeclarationVector) (FeatureCommitVector, error) {
	vector := FeatureCommitVector{Name: name}
	capture := &featureCaptureDynamo{FakeDynamo: NewFakeDynamo()}
	supported := evt.SupportedFeatures{evt.FeatureJournal: 1, evt.FeatureJournalCompaction: 2}
	marker := dynamo.NewRepository(capture, fakeTable).FeatureMarker(supported)
	if !strings.HasPrefix(name, "absent") {
		// Reproduce the named declaration's version as well as its contents.
		for _, declaration := range declarations[:2] {
			if _, err := marker.Declare(ctx, declaration.Input.apply); err != nil {
				return vector, err
			}
		}
		vector.Input.MarkerDeclaration = declarations[1].Name
	}

	account := NewAccount("compat-feature-account")
	account.Owner = "Ada"
	account.Status = "open"
	state, err := json.Marshal(account)
	if err != nil {
		return vector, err
	}
	group, err := dynamo.NewViewProjector("views").Project(ctx, account, nil)
	if err != nil {
		return vector, err
	}
	event := codecEvent(account.GetID(), 1, OpenedEvent, 1, `{"owner":"Ada"}`, StepMetadata(Step{By: WriterGo}))
	metadata, err := json.Marshal(event.Metadata)
	if err != nil {
		return vector, err
	}
	vector.Input.EventsTable = fakeTable
	vector.Input.ViewsTable = "views"
	vector.Input.ViewState = string(state)
	vector.Input.Event = EventInput{ID: event.ID, Sequence: event.Sequence, Type: event.Type, Version: event.Version, EntityID: event.EntityID, EntityType: event.EntityType, Payload: string(event.Payload), MetadataJSON: string(metadata)}
	res := evt.SerializedResult{Events: []evt.SerializedEvent{event}, Transaction: evt.Transaction{group}}
	repo := dynamo.NewRepository(capture, fakeTable).WithFeatureMarker(marker)
	if strings.HasSuffix(name, "snapshot") {
		vector.Input.Snapshot = &SnapshotInput{EntityType: AccountType, EntityID: account.GetID(), Sequence: 1, EventSequence: 1, Payload: string(state)}
		err = repo.CommitWithSnapshot(ctx, res, AccountType, account.GetID(), state, 1)
	} else {
		err = repo.Commit(ctx, res)
	}
	if err != nil {
		return vector, err
	}
	vector.Output = capture.writes

	// A fresh store has no marker row, so the writer declared stream-heads itself on its first
	// commit: record the row it stored.
	if strings.HasPrefix(name, "absent") {
		items := capture.Items(featureMarkerPK)
		if len(items) != 1 {
			return vector, fmt.Errorf("feature fixture: the writer stored %d marker rows, want 1", len(items))
		}
		vector.Declared = DynamoItemJSON(items[0])
	}

	return vector, nil
}

// featureMarkerPK is the partition of the feature-marker row.
const featureMarkerPK = "SYSTEM#FEATURES"

var _ dynamo.Client = (*featureCaptureDynamo)(nil)
var _ pgx.Tx = (*featurePGTx)(nil)
var _ postgres.DB = (*featurePG)(nil)
