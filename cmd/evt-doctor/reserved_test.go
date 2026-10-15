package main

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/jackc/pgx/v5"

	"github.com/photon-grove/evt"
)

type scanFunc func(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)

func (f scanFunc) Scan(ctx context.Context, in *dynamodb.ScanInput, opts ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	return f(ctx, in, opts...)
}

func reservedItem(id, sk string, attrs ...string) map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: id},
		"sk": &types.AttributeValueMemberN{Value: sk},
	}
	for _, attr := range attrs {
		item[attr] = &types.AttributeValueMemberS{Value: "engine-value"}
	}

	return item
}

func TestReservedStreamIDsPagination(t *testing.T) {
	ctx := context.Background()
	key := reservedItem("SYSTEM#legacy", "1")
	pages := []*dynamodb.ScanOutput{
		{Items: []map[string]types.AttributeValue{
			reservedItem("account-1", "1"),
			reservedItem("SYSTEM#legacy", "0"),
			reservedItem("SYSTEM#legacy", "1"),
			reservedItem("command-outcome#legacy", "0"),
			reservedItem("SYSTEM#FEATURES", "0", "version"),
			reservedItem("SYSTEM#REBUILD", "0", "owner", "leaseExpiresAt"),
			reservedItem("SYSTEM#REBUILD#GENERATION", "0", "value"),
			reservedItem("command-outcome#engine", "1", "status"),
			{"pk": &types.AttributeValueMemberN{Value: "1"}},
			{},
		}, LastEvaluatedKey: key},
		{Items: []map[string]types.AttributeValue{
			reservedItem("SYSTEM#legacy", "2"),
			reservedItem("command-outcome#legacy", "1"),
			reservedItem("SYSTEM#later-page", "1"),
		}},
	}
	calls := 0
	client := scanFunc(func(gotCtx context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
		if gotCtx != ctx || aws.ToString(in.TableName) != "events" {
			t.Fatalf("unexpected scan context/table: %v", in)
		}
		if calls >= len(pages) {
			t.Fatal("unexpected extra scan")
		}
		if calls == 0 && len(in.ExclusiveStartKey) != 0 || calls == 1 && !reflect.DeepEqual(in.ExclusiveStartKey, key) {
			t.Fatalf("page %d start key = %v", calls, in.ExclusiveStartKey)
		}
		// Exercise the actual projected response, so dropping an engine-shape attribute
		// from the scan cannot silently turn engine rows into migration candidates.
		projection := map[string]bool{}
		for _, name := range strings.Split(aws.ToString(in.ProjectionExpression), ", ") {
			if alias, ok := in.ExpressionAttributeNames[name]; ok {
				name = alias
			}
			projection[name] = true
		}
		page := pages[calls]
		for _, item := range page.Items {
			for name := range item {
				if !projection[name] {
					delete(item, name)
				}
			}
		}
		calls++

		return page, nil
	})

	ids, err := reservedStreamIDs(ctx, client, "events")
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, ids, "SYSTEM#later-page", "SYSTEM#legacy", "command-outcome#legacy")
	if calls != 2 {
		t.Fatalf("scanned %d pages, want 2", calls)
	}
}

func TestReservedStreamIDsEngineCollisions(t *testing.T) {
	shapes := []struct {
		id    string
		sk    string
		attrs []string
	}{
		{"SYSTEM#FEATURES", "0", []string{"version"}},
		{"SYSTEM#REBUILD", "0", []string{"owner", "leaseExpiresAt"}},
		{"SYSTEM#REBUILD#GENERATION", "0", []string{"value"}},
		{"command-outcome#command", "1", []string{"status"}},
	}
	for _, shape := range shapes {
		t.Run(shape.id, func(t *testing.T) {
			cases := map[string]map[string]types.AttributeValue{
				"engine":              reservedItem(shape.id, shape.sk, shape.attrs...),
				"wrong sort key":      reservedItem(shape.id, "2", shape.attrs...),
				"legacy collision":    reservedItem(shape.id, shape.sk),
				"wrong partition key": reservedItem("SYSTEM#other", shape.sk, shape.attrs...),
				"invalid number":      reservedItem(shape.id, "bad", shape.attrs...),
			}
			for _, attr := range shape.attrs {
				item := reservedItem(shape.id, shape.sk, shape.attrs...)
				delete(item, attr)
				cases["missing "+attr] = item
			}
			for _, variant := range []string{"missing", "string"} {
				item := reservedItem(shape.id, shape.sk, shape.attrs...)
				delete(item, "sk")
				if variant == "string" {
					item["sk"] = &types.AttributeValueMemberS{Value: shape.sk}
				}
				cases[variant+" sort key"] = item
			}
			for name, item := range cases {
				t.Run(name, func(t *testing.T) {
					client := scanFunc(func(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
						return &dynamodb.ScanOutput{Items: []map[string]types.AttributeValue{item}}, nil
					})
					ids, err := reservedStreamIDs(context.Background(), client, "events")
					if err != nil {
						t.Fatal(err)
					}
					if name == "engine" {
						assertIDs(t, ids)
					} else {
						id := evt.EntityID(shape.id)
						if name == "wrong partition key" {
							id = "SYSTEM#other"
						}
						assertIDs(t, ids, id)
					}
				})
			}
		})
	}
}

func TestReservedStreamIDsScanError(t *testing.T) {
	failure := errors.New("scan failed")
	calls := 0
	client := scanFunc(func(context.Context, *dynamodb.ScanInput, ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
		calls++
		if calls == 1 {
			return &dynamodb.ScanOutput{Items: []map[string]types.AttributeValue{reservedItem("SYSTEM#legacy", "1")}, LastEvaluatedKey: reservedItem("SYSTEM#legacy", "1")}, nil
		}

		return nil, failure
	})
	ids, err := reservedStreamIDs(context.Background(), client, "events")
	if !errors.Is(err, failure) || !strings.Contains(err.Error(), "scan events:") || ids != nil {
		t.Fatalf("got ids=%v err=%v; want wrapped scan error and no partial result", ids, err)
	}
}

type queryFunc func(context.Context, string, ...any) (pgx.Rows, error)

func (f queryFunc) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return f(ctx, sql, args...)
}

// Embed the unused pgx.Rows methods; the diagnostic only iterates and scans entity IDs.
type idRows struct {
	pgx.Rows
	ids     []evt.EntityID
	index   int
	closed  bool
	scanErr error
	rowsErr error
}

func (r *idRows) Next() bool { return r.index < len(r.ids) }
func (r *idRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	id, ok := dest[0].(*evt.EntityID)
	if !ok {
		return errors.New("expected EntityID scan destination")
	}
	*id = r.ids[r.index]
	r.index++

	return nil
}
func (r *idRows) Err() error { return r.rowsErr }
func (r *idRows) Close()     { r.closed = true }

func TestPostgresReservedStreamIDs(t *testing.T) {
	events := &idRows{ids: []evt.EntityID{"SYSTEM#legacy", "command-outcome#legacy", "SYSTEM#legacy"}}
	snapshots := &idRows{ids: []evt.EntityID{"SYSTEM#legacy", "SYSTEM#snapshot-only"}}
	// Only the two user relations are queried, never the derived engine relations.
	queries := map[string]*idRows{
		"SELECT DISTINCT entity_id FROM custom_events WHERE entity_id LIKE 'SYSTEM#%' OR entity_id LIKE 'command-outcome#%'": events,
		"SELECT entity_id FROM custom_snapshots WHERE entity_id LIKE 'SYSTEM#%' OR entity_id LIKE 'command-outcome#%'":       snapshots,
	}
	calls := 0
	client := queryFunc(func(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
		rows, ok := queries[sql]
		if !ok || len(args) != 0 || rows.closed {
			t.Fatalf("unexpected query: %q args=%v", sql, args)
		}
		calls++

		return rows, nil
	})
	ids, err := postgresReservedStreamIDs(context.Background(), client, "custom_events", "custom_snapshots")
	if err != nil {
		t.Fatal(err)
	}
	assertIDs(t, ids, "SYSTEM#legacy", "SYSTEM#snapshot-only", "command-outcome#legacy")
	if calls != 2 || !events.closed || !snapshots.closed {
		t.Fatalf("calls=%d closed=%v/%v", calls, events.closed, snapshots.closed)
	}
}

func TestPostgresReservedStreamIDsInvalidIdentifiers(t *testing.T) {
	client := queryFunc(func(context.Context, string, ...any) (pgx.Rows, error) {
		t.Fatal("invalid identifiers must fail before I/O")
		return nil, nil
	})
	for _, invalid := range []string{"", strings.Repeat("a", 64), "1events", "public.events", "events;DROP TABLE events", "events-name", "événements", "events\""} {
		for _, tables := range [][2]string{{invalid, "snapshots"}, {"events", invalid}} {
			ids, err := postgresReservedStreamIDs(context.Background(), client, tables[0], tables[1])
			if err == nil || !strings.Contains(err.Error(), "invalid table name") || ids != nil {
				t.Fatalf("tables=%q: ids=%v err=%v", tables, ids, err)
			}
		}
	}
}

func TestPostgresReservedStreamIDsErrors(t *testing.T) {
	failure := errors.New("database failure")
	for _, stage := range []string{"query", "scan", "rows"} {
		for _, failedQuery := range []int{0, 1} {
			t.Run(stage+strconv.Itoa(failedQuery), func(t *testing.T) {
				rows := []*idRows{{ids: []evt.EntityID{"SYSTEM#legacy"}}, {ids: []evt.EntityID{"SYSTEM#snapshot"}}}
				switch stage {
				case "scan":
					rows[failedQuery].scanErr = failure
				case "rows":
					rows[failedQuery].rowsErr = failure
				}
				calls := 0
				client := queryFunc(func(context.Context, string, ...any) (pgx.Rows, error) {
					i := calls
					calls++
					if i == failedQuery && stage == "query" {
						return nil, failure
					}

					return rows[i], nil
				})
				ids, err := postgresReservedStreamIDs(context.Background(), client, "events", "snapshots")
				if !errors.Is(err, failure) || ids != nil || calls != failedQuery+1 {
					t.Fatalf("ids=%v err=%v calls=%d", ids, err, calls)
				}
				for i := 0; i <= failedQuery; i++ {
					if (i != failedQuery || stage != "query") && !rows[i].closed {
						t.Errorf("rows %d not closed", i)
					}
				}
			})
		}
	}
}

func assertIDs(t *testing.T, got []evt.EntityID, want ...evt.EntityID) {
	t.Helper()
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("IDs = %v, want %v", got, want)
	}
}
