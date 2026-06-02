package mock

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_Client_BatchWriteItem(t *testing.T) {
	c := NewClient()
	out := &dynamodb.BatchWriteItemOutput{}

	c.On("BatchWriteItem", mock.Anything, mock.Anything, mock.Anything).Return(out, nil).Once()

	got, err := c.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{})
	require.NoError(t, err)
	require.Equal(t, out, got)
	c.AssertExpectations(t)
}

func Test_Client_BatchGetItem(t *testing.T) {
	c := NewClient()
	out := &dynamodb.BatchGetItemOutput{}

	c.On("BatchGetItem", mock.Anything, mock.Anything, mock.Anything).Return(out, nil).Once()

	got, err := c.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{})
	require.NoError(t, err)
	require.Equal(t, out, got)
	c.AssertExpectations(t)
}

func Test_Client_ExportTableToPointInTime(t *testing.T) {
	c := NewClient()
	out := &dynamodb.ExportTableToPointInTimeOutput{}

	c.On("ExportTableToPointInTime", mock.Anything, mock.Anything, mock.Anything).Return(out, nil).Once()

	got, err := c.ExportTableToPointInTime(context.Background(), &dynamodb.ExportTableToPointInTimeInput{})
	require.NoError(t, err)
	require.Equal(t, out, got)
	c.AssertExpectations(t)
}
