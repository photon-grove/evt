package test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	dynamomock "github.com/photon-grove/evt/dynamo/mock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func Test_ViewRepository_GetView(t *testing.T) {
	ctx := context.Background()
	client := dynamomock.NewClient()
	repo := dynamo.NewViewRepository(client, "views")

	// not found
	client.On("GetItem", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{}}, nil).Once()

	view, err := repo.GetView(ctx, "pk1")
	require.NoError(t, err)
	require.Nil(t, view)

	// found
	client.On("GetItem", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.GetItemOutput{Item: map[string]types.AttributeValue{
			"pk":         &types.AttributeValueMemberS{Value: "pk2"},
			"entityID":   &types.AttributeValueMemberS{Value: "e2"},
			"entityType": &types.AttributeValueMemberS{Value: "t2"},
			"payload":    &types.AttributeValueMemberS{Value: `{"x":2}`},
		}}, nil).Once()

	view, err = repo.GetView(ctx, "pk2")
	require.NoError(t, err)
	require.NotNil(t, view)
	require.Equal(t, "pk2", view.PK)
	require.Equal(t, evt.EntityID("e2"), view.EntityID)
	require.Equal(t, evt.EntityType("t2"), view.EntityType)
	require.Equal(t, []byte(`{"x":2}`), view.Payload)

	client.AssertExpectations(t)
}

func Test_ViewRepository_PutView(t *testing.T) {
	ctx := context.Background()
	client := dynamomock.NewClient()
	repo := dynamo.NewViewRepository(client, "views")

	client.On(
		"BatchWriteItem",
		mock.Anything,
		mock.MatchedBy(func(in *dynamodb.BatchWriteItemInput) bool {
			if in == nil || len(in.RequestItems) != 1 {
				return false
			}
			items := in.RequestItems["views"]
			if len(items) != 1 || items[0].PutRequest == nil {
				return false
			}
			put := items[0].PutRequest.Item
			pk, ok := put["pk"].(*types.AttributeValueMemberS)
			return ok && pk.Value == "pk1"
		}),
		mock.Anything,
	).Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()

	err := repo.PutView(ctx, &evt.SerializedView{
		PK:         "pk1",
		EntityID:   "e1",
		EntityType: "t1",
		Payload:    []byte(`{"x":1}`),
	})
	require.NoError(t, err)

	client.AssertExpectations(t)
}

func Test_ViewRepository_DeleteView(t *testing.T) {
	ctx := context.Background()
	client := dynamomock.NewClient()
	repo := dynamo.NewViewRepository(client, "views")

	client.On(
		"BatchWriteItem",
		mock.Anything,
		mock.MatchedBy(func(in *dynamodb.BatchWriteItemInput) bool {
			if in == nil || len(in.RequestItems) != 1 {
				return false
			}
			items := in.RequestItems["views"]
			if len(items) != 1 || items[0].DeleteRequest == nil {
				return false
			}
			key := items[0].DeleteRequest.Key
			pk, pkOK := key["pk"].(*types.AttributeValueMemberS)
			sk, skOK := key["sk"].(*types.AttributeValueMemberS)
			return pkOK && skOK && pk.Value == "pk1" && sk.Value == "STATUS#old"
		}),
		mock.Anything,
	).Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()

	deleter, ok := repo.(interface {
		DeleteView(context.Context, string, string) error
	})
	require.True(t, ok)
	err := deleter.DeleteView(ctx, "pk1", "STATUS#old")
	require.NoError(t, err)

	client.AssertExpectations(t)
}

func Test_ViewRepository_PutView_Error(t *testing.T) {
	ctx := context.Background()
	client := dynamomock.NewClient()
	repo := dynamo.NewViewRepository(client, "views")

	client.On("BatchWriteItem", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.BatchWriteItemOutput{}, errors.New("write failed")).Once()

	err := repo.PutView(ctx, &evt.SerializedView{PK: "pk1"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "write failed")
}

func Test_ViewRepository_PutView_RetriesUnprocessedItems(t *testing.T) {
	ctx := context.Background()
	client := dynamomock.NewClient()
	repo := dynamo.NewViewRepository(client, "views")

	firstResponse := &dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]types.WriteRequest{
			"views": {
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "pk1"},
						},
					},
				},
			},
		},
	}
	secondResponse := &dynamodb.BatchWriteItemOutput{}

	client.On("BatchWriteItem", mock.Anything, mock.Anything, mock.Anything).
		Return(firstResponse, nil).Once()
	client.On("BatchWriteItem", mock.Anything, mock.Anything, mock.Anything).
		Return(secondResponse, nil).Once()

	err := repo.PutView(ctx, &evt.SerializedView{PK: "pk1"})
	require.NoError(t, err)

	client.AssertExpectations(t)
}

func Test_ViewRepository_PutView_UnprocessedItemsExhausted(t *testing.T) {
	ctx := context.Background()
	client := dynamomock.NewClient()
	repo := dynamo.NewViewRepository(client, "views")

	response := &dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]types.WriteRequest{
			"views": {
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "pk1"},
						},
					},
				},
			},
		},
	}

	client.On("BatchWriteItem", mock.Anything, mock.Anything, mock.Anything).
		Return(response, nil).Times(3)

	err := repo.PutView(ctx, &evt.SerializedView{PK: "pk1"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unprocessed items")

	client.AssertExpectations(t)
}

func Test_ViewRepository_ListViewsByEntityType_Pagination(t *testing.T) {
	ctx := context.Background()
	client := dynamomock.NewClient()
	repo := dynamo.NewViewRepository(client, "views")

	out1 := &dynamodb.QueryOutput{
		Items: []map[string]types.AttributeValue{
			{
				"pk":         &types.AttributeValueMemberS{Value: "pk1"},
				"entityID":   &types.AttributeValueMemberS{Value: "e1"},
				"entityType": &types.AttributeValueMemberS{Value: "t1"},
				"payload":    &types.AttributeValueMemberS{Value: `{}`},
			},
		},
		LastEvaluatedKey: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "pk1"},
		},
	}
	out2 := &dynamodb.QueryOutput{
		Items: []map[string]types.AttributeValue{
			{
				"pk":         &types.AttributeValueMemberS{Value: "pk2"},
				"entityID":   &types.AttributeValueMemberS{Value: "e2"},
				"entityType": &types.AttributeValueMemberS{Value: "t1"},
				"payload":    &types.AttributeValueMemberS{Value: `{}`},
			},
		},
		LastEvaluatedKey: nil,
	}

	client.On(
		"Query",
		mock.Anything,
		mock.MatchedBy(func(in *dynamodb.QueryInput) bool {
			return in != nil &&
				in.TableName != nil && *in.TableName == "views" &&
				in.IndexName != nil && *in.IndexName == "entityType-index" &&
				in.KeyConditionExpression != nil && *in.KeyConditionExpression == "entityType = :entityType" &&
				in.ExpressionAttributeValues != nil &&
				in.ExpressionAttributeValues[":entityType"] != nil
		}),
		mock.Anything,
	).Return(out1, nil).Once()

	// paginator will set ExclusiveStartKey for the next page; accept any input
	client.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(out2, nil).Once()

	views, err := repo.ListViewsByEntityType(ctx, evt.EntityType("t1"))
	require.NoError(t, err)
	require.Len(t, views, 2)
	require.ElementsMatch(t, []string{"pk1", "pk2"}, []string{views[0].PK, views[1].PK})
}

func Test_ViewRepository_ListViewsByEntityType_Error(t *testing.T) {
	ctx := context.Background()
	client := dynamomock.NewClient()
	repo := dynamo.NewViewRepository(client, "views")

	client.On("Query", mock.Anything, mock.Anything, mock.Anything).
		Return((*dynamodb.QueryOutput)(nil), &types.ResourceNotFoundException{Message: aws.String("nope")}).Once()

	views, err := repo.ListViewsByEntityType(ctx, evt.EntityType("t1"))
	require.Error(t, err)
	require.Nil(t, views)
}

func Test_ViewRepository_ListViewsByPKPaged(t *testing.T) {
	ctx := context.Background()
	client := dynamomock.NewClient()
	repo := dynamo.NewViewRepository(client, "views")

	client.On(
		"Query",
		mock.Anything,
		mock.MatchedBy(func(in *dynamodb.QueryInput) bool {
			if in == nil || in.TableName == nil || *in.TableName != "views" {
				return false
			}
			if in.KeyConditionExpression == nil || *in.KeyConditionExpression != "pk = :pk" {
				return false
			}
			if in.Limit == nil || *in.Limit != 2 {
				return false
			}
			pk, ok := in.ExpressionAttributeValues[":pk"].(*types.AttributeValueMemberS)
			return ok && pk.Value == "pk1"
		}),
		mock.Anything,
	).Return(&dynamodb.QueryOutput{
		Items: []map[string]types.AttributeValue{
			{
				"pk":         &types.AttributeValueMemberS{Value: "pk1"},
				"sk":         &types.AttributeValueMemberS{Value: "SCORE#900#ITEM#one"},
				"entityID":   &types.AttributeValueMemberS{Value: "one"},
				"entityType": &types.AttributeValueMemberS{Value: "catalog_coverage_gap"},
				"payload":    &types.AttributeValueMemberS{Value: `{}`},
			},
		},
		LastEvaluatedKey: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "pk1"},
			"sk": &types.AttributeValueMemberS{Value: "SCORE#900#ITEM#one"},
		},
	}, nil).Once()

	lister, ok := repo.(interface {
		ListViewsByPKPaged(context.Context, string, int, string) (*evt.PagedResult, error)
	})
	require.True(t, ok)
	result, err := lister.ListViewsByPKPaged(ctx, "pk1", 2, "")
	require.NoError(t, err)
	require.Len(t, result.Views, 1)
	require.Equal(t, "SCORE#900#ITEM#one", result.Views[0].SK)
	require.NotEmpty(t, result.NextCursor)

	client.AssertExpectations(t)
}

func Test_ViewRepository_ListViewsByEntityTypeEach_StreamsAndStops(t *testing.T) {
	ctx := context.Background()
	client := dynamomock.NewClient()
	repo := dynamo.NewViewRepository(client, "views")

	// Two pages available. fn stops after the first item, so the second page must never be fetched.
	page1 := &dynamodb.QueryOutput{
		Items: []map[string]types.AttributeValue{
			{
				"pk":         &types.AttributeValueMemberS{Value: "pk1"},
				"entityID":   &types.AttributeValueMemberS{Value: "e1"},
				"entityType": &types.AttributeValueMemberS{Value: "t1"},
				"payload":    &types.AttributeValueMemberS{Value: `{}`},
			},
		},
		LastEvaluatedKey: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "pk1"},
		},
	}

	// Registered Once: a second Query (for page two) would be an unexpected call and fail the test.
	client.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(page1, nil).Once()

	stop := errors.New("stop")
	seen := make([]string, 0, 1)
	err := repo.ListViewsByEntityTypeEach(ctx, evt.EntityType("t1"), func(view *evt.SerializedView) error {
		seen = append(seen, view.PK)
		return stop
	})

	require.ErrorIs(t, err, stop)
	require.Equal(t, []string{"pk1"}, seen)
	client.AssertExpectations(t)
}

func Test_ViewRepository_ListViewsByPKEach_Streams(t *testing.T) {
	ctx := context.Background()
	client := dynamomock.NewClient()
	repo := dynamo.NewViewRepository(client, "views")

	out := &dynamodb.QueryOutput{
		Items: []map[string]types.AttributeValue{
			{"pk": &types.AttributeValueMemberS{Value: "pk1"}, "sk": &types.AttributeValueMemberS{Value: "a"}, "payload": &types.AttributeValueMemberS{Value: `{}`}},
			{"pk": &types.AttributeValueMemberS{Value: "pk1"}, "sk": &types.AttributeValueMemberS{Value: "b"}, "payload": &types.AttributeValueMemberS{Value: `{}`}},
		},
	}
	client.On("Query", mock.Anything, mock.Anything, mock.Anything).Return(out, nil).Once()

	seen := make([]string, 0, 2)
	err := repo.ListViewsByPKEach(ctx, "pk1", func(view *evt.SerializedView) error {
		seen = append(seen, view.SK)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, seen)
	client.AssertExpectations(t)
}
