package dynamo

import (
	"context"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/photon-grove/evt"
	dynamock "github.com/photon-grove/evt/dynamo/mock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const testViewsTable = "views"

func newBatchTestRepo(client Client) *ViewRepository {
	return &ViewRepository{
		ViewsTable: testViewsTable,
		client:     client,
		encoder:    attributevalue.NewEncoder(func(opts *attributevalue.EncoderOptions) { opts.TagKey = tagKey }),
		decoder:    attributevalue.NewDecoder(func(opts *attributevalue.DecoderOptions) { opts.TagKey = tagKey }),
	}
}

func Test_ViewRepository_PutViews_ChunksAt25(t *testing.T) {
	client := dynamock.NewClient()
	repo := newBatchTestRepo(client)

	var sizes []int
	client.On("BatchWriteItem", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.BatchWriteItemOutput{}, nil).
		Run(func(args mock.Arguments) {
			input, ok := args.Get(1).(*dynamodb.BatchWriteItemInput)
			require.True(t, ok)
			sizes = append(sizes, len(input.RequestItems[testViewsTable]))
		})

	views := make([]*evt.SerializedView, 0, 26)
	for i := 0; i < 26; i++ {
		views = append(views, &evt.SerializedView{PK: "REL#" + strconv.Itoa(i), Payload: []byte("{}")})
	}

	require.NoError(t, repo.PutViews(context.Background(), views))
	require.Equal(t, []int{25, 1}, sizes)
}

func Test_ViewRepository_PutViews_SkipsNilAndEmpty(t *testing.T) {
	client := dynamock.NewClient()
	repo := newBatchTestRepo(client)

	// All inputs nil => no write should be issued.
	require.NoError(t, repo.PutViews(context.Background(), []*evt.SerializedView{nil, nil}))
	client.AssertNotCalled(t, "BatchWriteItem", mock.Anything, mock.Anything, mock.Anything)
}

func Test_ViewRepository_PutViews_RetriesUnprocessedItems(t *testing.T) {
	client := dynamock.NewClient()
	repo := newBatchTestRepo(client)

	item := map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "REL#1"}}
	client.On("BatchWriteItem", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.BatchWriteItemOutput{
			UnprocessedItems: map[string][]types.WriteRequest{
				testViewsTable: {{PutRequest: &types.PutRequest{Item: item}}},
			},
		}, nil).Once()
	client.On("BatchWriteItem", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.BatchWriteItemOutput{}, nil).Once()

	err := repo.PutViews(context.Background(), []*evt.SerializedView{{PK: "REL#1", Payload: []byte("{}")}})
	require.NoError(t, err)
	client.AssertNumberOfCalls(t, "BatchWriteItem", 2)
}

func Test_ViewRepository_BatchGetViews_DedupsAndOmitsMissing(t *testing.T) {
	client := dynamock.NewClient()
	repo := newBatchTestRepo(client)

	item, err := repo.marshalViewItem(&evt.SerializedView{
		PK:         "REL#1",
		EntityID:   "e1",
		EntityType: "t1",
		Payload:    []byte(`{"x":1}`),
	})
	require.NoError(t, err)

	var requestedKeys int
	client.On("BatchGetItem", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.BatchGetItemOutput{
			Responses: map[string][]map[string]types.AttributeValue{testViewsTable: {item}},
		}, nil).
		Run(func(args mock.Arguments) {
			input, ok := args.Get(1).(*dynamodb.BatchGetItemInput)
			require.True(t, ok)
			requestedKeys = len(input.RequestItems[testViewsTable].Keys)
		})

	got, err := repo.BatchGetViews(context.Background(), []string{"REL#1", "REL#2", "REL#1", ""})
	require.NoError(t, err)

	// Duplicate "REL#1" and the empty string are dropped before the request.
	require.Equal(t, 2, requestedKeys)
	// REL#2 was not in the response, so it is omitted from the result.
	require.Len(t, got, 1)
	require.Equal(t, "REL#1", got[0].PK)
	require.Equal(t, []byte(`{"x":1}`), got[0].Payload)
}

func Test_ViewRepository_BatchGetViews_ChunksAt100(t *testing.T) {
	client := dynamock.NewClient()
	repo := newBatchTestRepo(client)

	var sizes []int
	client.On("BatchGetItem", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.BatchGetItemOutput{}, nil).
		Run(func(args mock.Arguments) {
			input, ok := args.Get(1).(*dynamodb.BatchGetItemInput)
			require.True(t, ok)
			sizes = append(sizes, len(input.RequestItems[testViewsTable].Keys))
		})

	pks := make([]string, 0, 101)
	for i := 0; i < 101; i++ {
		pks = append(pks, "REL#"+strconv.Itoa(i))
	}

	_, err := repo.BatchGetViews(context.Background(), pks)
	require.NoError(t, err)
	require.Equal(t, []int{100, 1}, sizes)
}

func Test_ViewRepository_BatchGetViews_RetriesUnprocessedKeys(t *testing.T) {
	client := dynamock.NewClient()
	repo := newBatchTestRepo(client)

	key := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "REL#1"},
		"sk": &types.AttributeValueMemberS{Value: evt.DefaultViewSK},
	}
	client.On("BatchGetItem", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.BatchGetItemOutput{
			UnprocessedKeys: map[string]types.KeysAndAttributes{
				testViewsTable: {Keys: []map[string]types.AttributeValue{key}},
			},
		}, nil).Once()
	client.On("BatchGetItem", mock.Anything, mock.Anything, mock.Anything).
		Return(&dynamodb.BatchGetItemOutput{}, nil).Once()

	_, err := repo.BatchGetViews(context.Background(), []string{"REL#1"})
	require.NoError(t, err)
	client.AssertNumberOfCalls(t, "BatchGetItem", 2)
}
