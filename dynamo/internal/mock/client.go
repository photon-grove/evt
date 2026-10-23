// Package mock provides DynamoDB client mocks
// nolint:errcheck
package mock

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/mock"
)

// Client is a mocked DynamoDB client that satisfies the `Dynamo` interface
type Client struct {
	mock.Mock
}

// NewClient automatically initializes a testify mock on creating a new instance
func NewClient() *Client {
	return new(Client)
}

// TransactWriteItems mocks the corresponding method on the DynamoDB Client
func (client *Client) TransactWriteItems(
	ctx context.Context,
	input *dynamodb.TransactWriteItemsInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.TransactWriteItemsOutput, error) {
	args := client.Called(ctx, input, options)

	return args.Get(0).(*dynamodb.TransactWriteItemsOutput), args.Error(1)
}

// Query mocks the corresponding method on the DynamoDB Client
func (client *Client) Query(
	ctx context.Context,
	input *dynamodb.QueryInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.QueryOutput, error) {
	args := client.Called(ctx, input, options)

	return args.Get(0).(*dynamodb.QueryOutput), args.Error(1)
}

// BatchWriteItem mocks the corresponding method on the DynamoDB Client
func (client *Client) BatchWriteItem(
	ctx context.Context,
	input *dynamodb.BatchWriteItemInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.BatchWriteItemOutput, error) {
	args := client.Called(ctx, input, options)

	return args.Get(0).(*dynamodb.BatchWriteItemOutput), args.Error(1)
}

// PutItem mock impl
func (client *Client) PutItem(
	ctx context.Context,
	input *dynamodb.PutItemInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.PutItemOutput, error) {
	args := client.Called(ctx, input, options)
	return args.Get(0).(*dynamodb.PutItemOutput), args.Error(1)
}

// DeleteItem mock impl
func (client *Client) DeleteItem(
	ctx context.Context,
	input *dynamodb.DeleteItemInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.DeleteItemOutput, error) {
	args := client.Called(ctx, input, options)
	return args.Get(0).(*dynamodb.DeleteItemOutput), args.Error(1)
}

// UpdateItem mock impl
func (client *Client) UpdateItem(
	ctx context.Context,
	input *dynamodb.UpdateItemInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.UpdateItemOutput, error) {
	args := client.Called(ctx, input, options)
	return args.Get(0).(*dynamodb.UpdateItemOutput), args.Error(1)
}

// GetItem mock impl
func (client *Client) GetItem(
	ctx context.Context,
	input *dynamodb.GetItemInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.GetItemOutput, error) {
	args := client.Called(ctx, input, options)
	return args.Get(0).(*dynamodb.GetItemOutput), args.Error(1)
}

// BatchGetItem mock impl
func (client *Client) BatchGetItem(
	ctx context.Context,
	input *dynamodb.BatchGetItemInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.BatchGetItemOutput, error) {
	args := client.Called(ctx, input, options)
	return args.Get(0).(*dynamodb.BatchGetItemOutput), args.Error(1)
}

// Scan mocks the corresponding method on the DynamoDB Client
func (client *Client) Scan(
	ctx context.Context,
	input *dynamodb.ScanInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.ScanOutput, error) {
	args := client.Called(ctx, input, options)

	return args.Get(0).(*dynamodb.ScanOutput), args.Error(1)
}

// ExportTableToPointInTime mocks the corresponding method on the DynamoDB Client
func (client *Client) ExportTableToPointInTime(
	ctx context.Context,
	input *dynamodb.ExportTableToPointInTimeInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.ExportTableToPointInTimeOutput, error) {
	args := client.Called(ctx, input, options)

	return args.Get(0).(*dynamodb.ExportTableToPointInTimeOutput), args.Error(1)
}
