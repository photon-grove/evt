package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
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

		return
	}

	ctx := context.Background()
	cfg, err := newAWSConfig(ctx, *region, *endpoint)
	if err != nil {
		exitf("load AWS config: %v", err)
	}

	client := dynamodb.NewFromConfig(*cfg)
	for _, table := range []string{*eventsTable, *viewsTable} {
		out, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &table})
		if err != nil {
			exitf("describe %s: %v", table, err)
		}
		if out.Table == nil {
			exitf("describe %s: empty table response", table)
		}

		fmt.Printf("%s: %s\n", table, out.Table.TableStatus)
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
