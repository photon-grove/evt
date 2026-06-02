package awsclients

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Dynamo is an interface wrapping the DynamoDB client so that it can be easily mocked for testing
type Dynamo interface {
	TransactWriteItems(
		ctx context.Context,
		params *dynamodb.TransactWriteItemsInput,
		options ...func(*dynamodb.Options),
	) (*dynamodb.TransactWriteItemsOutput, error)

	BatchWriteItem(
		ctx context.Context,
		params *dynamodb.BatchWriteItemInput,
		options ...func(*dynamodb.Options),
	) (*dynamodb.BatchWriteItemOutput, error)

	Query(
		ctx context.Context,
		params *dynamodb.QueryInput,
		options ...func(*dynamodb.Options),
	) (*dynamodb.QueryOutput, error)

	Scan(
		ctx context.Context,
		params *dynamodb.ScanInput,
		options ...func(*dynamodb.Options),
	) (*dynamodb.ScanOutput, error)

	GetItem(
		ctx context.Context,
		params *dynamodb.GetItemInput,
		options ...func(*dynamodb.Options),
	) (*dynamodb.GetItemOutput, error)

	BatchGetItem(
		ctx context.Context,
		params *dynamodb.BatchGetItemInput,
		options ...func(*dynamodb.Options),
	) (*dynamodb.BatchGetItemOutput, error)

	PutItem(
		ctx context.Context,
		params *dynamodb.PutItemInput,
		options ...func(*dynamodb.Options),
	) (*dynamodb.PutItemOutput, error)

	DeleteItem(
		ctx context.Context,
		params *dynamodb.DeleteItemInput,
		options ...func(*dynamodb.Options),
	) (*dynamodb.DeleteItemOutput, error)

	UpdateItem(
		ctx context.Context,
		params *dynamodb.UpdateItemInput,
		options ...func(*dynamodb.Options),
	) (*dynamodb.UpdateItemOutput, error)

	ExportTableToPointInTime(
		ctx context.Context,
		params *dynamodb.ExportTableToPointInTimeInput,
		options ...func(*dynamodb.Options),
	) (*dynamodb.ExportTableToPointInTimeOutput, error)
}

// NewDynamoDBClient creates a new DynamoDB client
func NewDynamoDBClient(cfg *aws.Config, optsCallback ...func(*dynamodb.Options)) (Dynamo, error) {
	client := dynamodb.NewFromConfig(*cfg, optsCallback...)

	return client, nil
}
