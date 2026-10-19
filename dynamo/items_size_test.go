package dynamo_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/internal/compat"
	"github.com/photon-grove/evt/result" //nolint:staticcheck // deprecated in ADR-0006 (K cuts), kept until the next-minor deletion
)

// projectorGroup is a DynamoDB projector group carrying arbitrary write items.
type projectorGroup struct{ items []types.TransactWriteItem }

func (g projectorGroup) ToWriteItems() []types.TransactWriteItem { return g.items }
func (g projectorGroup) MergeDynamo(dynamo.TransactionGroup) (dynamo.TransactionGroup, error) {
	return g, nil
}
func (g projectorGroup) TransactionType() evt.TransactionType { return "size-test" }
func (g projectorGroup) StorageType() evt.StorageType         { return dynamo.StorageType }
func (g projectorGroup) Len() int                             { return len(g.items) }
func (g projectorGroup) HandleError(err error, _ int) error   { return err }
func (g projectorGroup) Merge(evt.TransactionGroup) (evt.TransactionGroup, error) {
	return g, nil
}

func s(value string) types.AttributeValue { return &types.AttributeValueMemberS{Value: value} }

func TestTransactItemSize_CountsKeysExpressionsNamesAndValues(t *testing.T) {
	key := map[string]types.AttributeValue{"pk": s("view#1"), "sk": &types.AttributeValueMemberN{Value: "0"}}
	// "pk" (2) + "view#1" (6) + "sk" (2) + "0" (1)
	keyBytes := 11
	names := map[string]string{"#n": "name"}
	values := map[string]types.AttributeValue{":v": s("value")}
	// "#n" (2) + "name" (4) + ":v" (2) + "value" (5)
	placeholderBytes := 13

	update := types.TransactWriteItem{Update: &types.Update{
		Key:                       key,
		UpdateExpression:          aws.String("SET #n = :v"),
		ConditionExpression:       aws.String("attribute_exists(pk)"),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	}}
	require.Equal(t, keyBytes+len("SET #n = :v")+len("attribute_exists(pk)")+placeholderBytes, dynamo.TransactItemSize(update))

	deleted := types.TransactWriteItem{Delete: &types.Delete{
		Key:                       key,
		ConditionExpression:       aws.String("#n = :v"),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	}}
	require.Equal(t, keyBytes+len("#n = :v")+placeholderBytes, dynamo.TransactItemSize(deleted))

	check := types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
		Key:                       key,
		ConditionExpression:       aws.String("#n = :v"),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
	}}
	require.Equal(t, keyBytes+len("#n = :v")+placeholderBytes, dynamo.TransactItemSize(check))

	put := types.TransactWriteItem{Put: &types.Put{Item: key}}
	require.Equal(t, keyBytes, dynamo.TransactItemSize(put))

	require.Zero(t, dynamo.TransactItemSize(types.TransactWriteItem{}))
}

func TestCommit_PreflightSizesNonPutProjectorItems(t *testing.T) {
	large := strings.Repeat("x", 3_900_000)
	key := map[string]types.AttributeValue{"pk": s("view#1"), "sk": &types.AttributeValueMemberN{Value: "0"}}

	for name, item := range map[string]types.TransactWriteItem{
		"update": {Update: &types.Update{
			Key:                       key,
			UpdateExpression:          aws.String("SET v = :v"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":v": s(large)},
		}},
		"delete": {Delete: &types.Delete{
			Key:                       key,
			ConditionExpression:       aws.String("v = :v"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":v": s(large)},
		}},
		"condition check": {ConditionCheck: &types.ConditionCheck{
			Key:                       key,
			ConditionExpression:       aws.String("v = :v"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":v": s(large)},
		}},
	} {
		t.Run(name, func(t *testing.T) {
			fake := compat.NewFakeDynamo()
			repo := dynamo.NewRepository(fake, "events")

			err := repo.Commit(context.Background(), evt.SerializedResult{
				Events: []evt.SerializedEvent{{
					ID: "size-1:1", Sequence: 1, Type: "t:created", Version: 1,
					EntityID: "size-1", EntityType: "t", Payload: []byte(`{}`),
				}},
				Transaction: evt.Transaction{projectorGroup{items: []types.TransactWriteItem{item}}},
			})

			var tooLarge *evt.CommitTooLargeError
			require.ErrorAs(t, err, &tooLarge)
			require.Empty(t, fake.Transactions(), "the oversized commit is refused before any request")
		})
	}
}

func TestCommit_NamesTheStreamWhoseItemFailed(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	repo := dynamo.NewRepository(fake, "events")

	event := func(id string, sequence evt.EventSequence) evt.SerializedEvent {
		return evt.SerializedEvent{
			ID: evt.GetEventID(evt.EntityID(id), sequence), Sequence: sequence, Type: "t:created", Version: 1,
			EntityID: evt.EntityID(id), EntityType: "t", Payload: []byte(`{}`),
		}
	}

	// Stream "d" already holds sequence 1; "c" and "e" sort around it and are clean.
	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Events: []evt.SerializedEvent{event("name-d", 1)}}))

	err := repo.Commit(ctx, evt.SerializedResult{Events: []evt.SerializedEvent{
		event("name-c", 1), event("name-d", 1), event("name-e", 1),
	}})

	require.ErrorIs(t, err, dynamo.ErrSnapshotRaceCondition)
	require.ErrorContains(t, err, "stream name-d")
	require.NotContains(t, err.Error(), "name-c")
	require.NotContains(t, err.Error(), "name-e")
}

func TestCommitStream_Refuses(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	repo := dynamo.NewRepository(fake, "events")

	entry := evt.SerializedResult{Events: []evt.SerializedEvent{{
		ID: evt.GetEventID("batch-plain", 1), Sequence: 1, Type: "t:created", Version: 1,
		EntityID: "batch-plain", EntityType: "t", Payload: []byte(`{}`),
	}}}

	channel := make(chan result.Result[evt.SerializedResult], 2)
	channel <- result.Ok(entry)
	channel <- result.Ok(entry)
	close(channel)

	errs := repo.CommitStream(ctx, channel)

	require.Len(t, errs, 1, "the whole stream is refused once, and the channel is drained")
	require.ErrorIs(t, errs[0], dynamo.ErrCommitStreamRefused)

	var refused *dynamo.CommitStreamRefusedError
	require.ErrorAs(t, errs[0], &refused)
	require.Empty(t, fake.Transactions(), "the refusal is before any request")
	require.Empty(t, fake.Items("batch-plain"))
}

func TestCommit_RefusesFencesForLocallyRetainedTypesWithoutAManifest(t *testing.T) {
	ctx := context.Background()
	fake := compat.NewFakeDynamo()
	repo := dynamo.NewRepository(fake, "events").WithRetention(dynamo.Retention{"scratch": time.Hour})

	fenced := func(entityType evt.EntityType) evt.SerializedResult {
		return evt.SerializedResult{
			Events: []evt.SerializedEvent{{
				ID: "retained-writer:1", Sequence: 1, Type: "t:created", Version: 1,
				EntityID: "retained-writer", EntityType: "t", Payload: []byte(`{}`),
			}},
			Fences: []evt.ReadFence{{EntityID: "retained-watched", EntityType: entityType}},
		}
	}

	// No manifest exists, yet the repository's own WithRetention refuses the fence.
	err := repo.Commit(ctx, fenced("scratch"))
	var refused *evt.RetentionConflictError
	require.ErrorAs(t, err, &refused)
	require.Equal(t, evt.EntityType("scratch"), refused.EntityType)
	require.Empty(t, fake.Transactions(), "refused before any request")

	// A type outside the policy is still fenceable.
	require.NoError(t, repo.Commit(ctx, fenced("plain")))
}
