// Command evt-doctor is a CLI for inspecting and diagnosing an evt DynamoDB event log.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

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
	var (
		region      = flag.String("region", envOr("AWS_REGION", "us-west-2"), "AWS region")
		endpoint    = flag.String("endpoint", resolveLocalEndpoint(), "AWS emulator endpoint")
		eventsTable = flag.String("events-table", "evt-local-event-log", "event-log table name")
		viewsTable  = flag.String("views-table", "evt-local-entity-views", "entity-views table name")
		printSchema = flag.Bool("schema", false, "print required DynamoDB table schema and exit")
	)
	flag.Parse()

	if *printSchema {
		if err := json.NewEncoder(os.Stdout).Encode(requiredSchema(*eventsTable, *viewsTable)); err != nil {
			exitf("encode schema: %v", err)
		}

		fmt.Fprintf(os.Stderr, "\nreserved pk namespaces arriving in v0.9 (ADR-0004): SYSTEM#FEATURES "+
			"(C9 feature marker), SYSTEM#REBUILD (C10 rebuild fence), command-outcome#<commandId> (C3 outcomes)\n")

		return
	}

	ctx := context.Background()
	cfg, err := newAWSConfig(ctx, *region, *endpoint)
	if err != nil {
		exitf("load AWS config: %v", err)
	}

	client := dynamodb.NewFromConfig(*cfg)
	for _, table := range []string{*eventsTable, *viewsTable} {
		out, describeErr := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &table})
		if describeErr != nil {
			exitf("describe %s: %v", table, describeErr)
		}
		if out.Table == nil {
			exitf("describe %s: empty table response", table)
		}

		fmt.Printf("%s: %s\n", table, out.Table.TableStatus)
	}

	found, err := findReservedStreams(ctx, client, *eventsTable)
	if err != nil {
		exitf("scan %s for reserved namespaces: %v", *eventsTable, err)
	}
	for _, stream := range found {
		fmt.Printf("reserved namespace: %s (%d rows)\n", stream.prefix+stream.id, stream.rows)
	}
	if len(found) > 0 {
		exitf("rename the streams above before enabling the v0.9 reserved namespaces " +
			"(SYSTEM#FEATURES, SYSTEM#REBUILD, command-outcome#); see docs/adr/0004")
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
	if localEndpoint == "" {
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
		if err != nil {
			return nil, err
		}

		return &cfg, nil
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithRetryMaxAttempts(1),
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

// reservedPrefixes are the pk namespaces the v0.9 core reshape reserves for engine-managed rows
// (ADR-0004): the SYSTEM#FEATURES feature marker (C9), the SYSTEM#REBUILD fence (C10), and the
// command-outcome#<id> outcome stream (C3). Enumeration, heads, and rebuild skip these
// namespaces, so a pre-existing entity whose ID lands in one would become silently invisible.
var reservedPrefixes = []string{"SYSTEM#", "command-outcome#"}

// engineReservedIDs are reserved-namespace pks the engine itself writes, so they are not
// pre-existing streams to rename. The set grows as the core reshape lands.
var engineReservedIDs = map[string]bool{
	"SYSTEM#FEATURES": true,
	"SYSTEM#REBUILD":  true,
}

type reservedStream struct {
	prefix string
	id     string
	rows   int
}

// classifyReserved reports whether a pk in the given reserved prefix is a pre-existing stream
// (needing rename) rather than an engine-written row, and returns its reservedStream form.
func classifyReserved(pk, prefix string, rows int) (reservedStream, bool) {
	if !strings.HasPrefix(pk, prefix) || engineReservedIDs[pk] {
		return reservedStream{}, false
	}

	return reservedStream{prefix: prefix, id: strings.TrimPrefix(pk, prefix), rows: rows}, true
}

// findReservedStreams scans the events table for pks in a reserved namespace (a diagnostic
// flow, the one sanctioned table scan) and reports pre-existing streams that would be silently
// skipped by v0.9 enumeration. Engine-written rows (SYSTEM#FEATURES, SYSTEM#REBUILD) are
// excluded; command-outcome# rows are all reported, since no outcome row can predate the
// feature that writes them.
func findReservedStreams(
	ctx context.Context,
	client *dynamodb.Client,
	eventsTable string,
) ([]reservedStream, error) {
	filter := "begins_with(pk, :system) OR begins_with(pk, :outcome)"
	input := dynamodb.ScanInput{
		TableName:            &eventsTable,
		ProjectionExpression: aws.String("pk"),
		FilterExpression:     &filter,
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":system":  &types.AttributeValueMemberS{Value: reservedPrefixes[0]},
			":outcome": &types.AttributeValueMemberS{Value: reservedPrefixes[1]},
		},
	}

	counts := map[string]int{}
	p := dynamodb.NewScanPaginator(client, &input)
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, item := range out.Items {
			pkItem, ok := item["pk"]
			if !ok {
				continue
			}

			var pk string
			if err := attributevalue.Unmarshal(pkItem, &pk); err != nil {
				return nil, fmt.Errorf("unmarshal pk: %w", err)
			}

			counts[pk]++
		}
	}

	var found []reservedStream
	for pk, rows := range counts {
		if engineReservedIDs[pk] {
			continue
		}

		for _, prefix := range reservedPrefixes {
			if stream, ok := classifyReserved(pk, prefix, rows); ok {
				found = append(found, stream)
			}
		}
	}

	sort.Slice(found, func(i, j int) bool { return found[i].prefix+found[i].id < found[j].prefix+found[j].id })

	return found, nil
}
