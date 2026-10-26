package dynamo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Client is the subset of DynamoDB operations used by evt's DynamoDB backend.
type Client interface {
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
