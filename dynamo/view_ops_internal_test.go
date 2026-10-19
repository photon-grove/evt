package dynamo

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/policy"
)

func projected(name string, index, operation int, op evt.ViewOperation) evt.ProjectedView {
	return evt.ProjectedView{Projector: name, ProjectorIndex: index, Operation: operation, Op: op}
}

func Test_buildViewItems_ShapesEveryOperationAsTheSpecPins(t *testing.T) {
	repo := NewRepository(nil, "events").WithViewsTable("views")

	items, err := repo.buildViewItems([]evt.ProjectedView{
		projected("p", 0, 0, evt.PutView("pk", "sk", "owner", "account", []byte(`{"v":1}`))),
		projected("p", 0, 1, evt.PutView("pk", "claim", "owner", "account", []byte(`{}`)).IfAbsent()),
		projected("p", 0, 2, evt.DeleteView("pk", "gone")),
		projected("p", 0, 3, evt.DeleteView("pk", "release").IfPresent()),
	})
	require.NoError(t, err)
	require.Len(t, items, 4)

	plain := items[0].Put
	require.NotNil(t, plain)
	require.Equal(t, "views", aws.ToString(plain.TableName))
	require.Nil(t, plain.ConditionExpression, "a put is unconditional unless it carries a condition")
	require.Equal(t, map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: "pk"},
		"sk":         &types.AttributeValueMemberS{Value: "sk"},
		"entityID":   &types.AttributeValueMemberS{Value: "owner"},
		"entityType": &types.AttributeValueMemberS{Value: "account"},
		"payload":    &types.AttributeValueMemberS{Value: `{"v":1}`},
	}, plain.Item, "all five attributes, and never a ttl")

	require.Equal(t, "attribute_not_exists(pk)", aws.ToString(items[1].Put.ConditionExpression))

	deletion := items[2].Delete
	require.NotNil(t, deletion)
	require.Equal(t, "views", aws.ToString(deletion.TableName))
	require.Equal(t, map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "pk"},
		"sk": &types.AttributeValueMemberS{Value: "gone"},
	}, deletion.Key)
	require.Nil(t, deletion.ConditionExpression)

	require.Equal(t, "attribute_exists(pk)", aws.ToString(items[3].Delete.ConditionExpression))
}

func Test_buildViewItems_AnEmptyEntityIDAndTypeAreStillWritten(t *testing.T) {
	repo := NewRepository(nil, "events").WithViewsTable("views")

	items, err := repo.buildViewItems([]evt.ProjectedView{projected("p", 0, 0, evt.PutView("pk", "sk", "", "", nil))})
	require.NoError(t, err)

	item := items[0].Put.Item
	require.Equal(t, &types.AttributeValueMemberS{}, item["entityID"], "entityID and entityType are always written")
	require.Equal(t, &types.AttributeValueMemberS{}, item["entityType"])
	require.Equal(t, &types.AttributeValueMemberS{}, item["payload"])
}

func Test_buildViewItems_RefusesOperationsWithoutAViewsTable(t *testing.T) {
	_, err := NewRepository(nil, "events").buildViewItems([]evt.ProjectedView{
		projected("p", 0, 0, evt.PutView("pk", "sk", "", "", nil)),
		projected("p", 0, 1, evt.DeleteView("pk", "other")),
	})

	var refused *evt.ViewsNotConfiguredError

	require.ErrorAs(t, err, &refused)
	require.Equal(t, "dynamodb", refused.Backend)
	require.Equal(t, 2, refused.Operations)

	items, err := NewRepository(nil, "events").buildViewItems(nil)
	require.NoError(t, err)
	require.Empty(t, items, "a commit with no view operations needs no views table")
}

func Test_theBudgetedSizeOfAViewOperationIsTheSizeOfItsItem(t *testing.T) {
	repo := NewRepository(nil, "events").WithViewsTable("views")

	for name, op := range map[string]evt.ViewOperation{
		"put":                  evt.PutView("pk", "sk", "owner", "account", []byte(`{"balance":10}`)),
		"conditioned put":      evt.PutView("pk", "sk", "owner", "account", []byte(`{}`)).IfAbsent(),
		"put without entity":   evt.PutView("pk", "sk", "", "", nil),
		"multi-byte keys":      evt.PutView("p€k", "s€k", "ö", "ü", []byte("€")),
		"delete":               evt.DeleteView("pk", "sk"),
		"conditioned delete":   evt.DeleteView("pk", "sk").IfPresent(),
		"large payload":        evt.PutView("pk", "sk", "o", "t", []byte(strings.Repeat("x", 10_000))),
		"conditioned put free": evt.PutView("pk", "sk", "o", "t", []byte(`{}`)).OnConflict(nil),
	} {
		item, err := repo.buildViewItem(op)
		require.NoError(t, err, name)
		require.Equal(t, transactItemSize(item), evt.ViewOperationSize(op), name)
	}
}

func Test_classifyCommitFailure_NamesTheProjectorOperationWhoseConditionFailed(t *testing.T) {
	repo := &Repository{}
	views := []evt.ProjectedView{
		projected("profile", 0, 0, evt.PutView("pk", "profile", "", "", nil)),
		projected("claims", 1, 0, evt.PutView("pk", "note", "", "", nil)),
		projected("claims", 1, 1, evt.PutView("pk", "ada", "", "", nil).IfAbsent()),
	}
	layout := commitLayout{views: views}

	cancelled := func(codes ...string) error {
		reasons := make([]types.CancellationReason, 0, len(codes))
		for _, code := range codes {
			reasons = append(reasons, types.CancellationReason{Code: aws.String(code)})
		}

		return &types.TransactionCanceledException{CancellationReasons: reasons}
	}

	// Index 0 is the marker check; the views occupy indexes 1 to 3.
	err := repo.classifyCommitFailure(context.Background(), nil, nil, layout,
		cancelled("None", "None", "None", "ConditionalCheckFailed"))

	var failed *evt.ProjectorOperationError

	require.ErrorAs(t, err, &failed)
	require.Equal(t, "claims", failed.Projector)
	require.Equal(t, 1, failed.ProjectorIndex)
	require.Equal(t, 1, failed.Operation)
	require.Equal(t, "ada", failed.SK)
	require.Equal(t, evt.ReasonConditionFailed, failed.Reason)
	require.False(t, policy.IsTransient(err), "re-preparing cannot fix a projector condition")
	require.False(t, evt.IsOptimisticConflict(err))

	// Where several fail, the first in transaction order is the one named.
	err = repo.classifyCommitFailure(context.Background(), nil, nil, layout,
		cancelled("None", "None", "ConditionalCheckFailed", "ConditionalCheckFailed"))
	require.ErrorAs(t, err, &failed)
	require.Equal(t, 0, failed.Operation)
	require.Equal(t, "claims", failed.Projector)
}

func Test_classifyCommitFailure_ItemLevelReasonsOnAViewAreTheProjectorsAndTransientOnesAreNot(t *testing.T) {
	repo := &Repository{}
	layout := commitLayout{views: []evt.ProjectedView{
		projected("p", 0, 0, evt.PutView("pk", "a", "", "", nil)),
		projected("p", 0, 1, evt.PutView("pk", "b", "", "", nil)),
	}}

	cancelled := func(codes ...string) error {
		reasons := make([]types.CancellationReason, 0, len(codes))
		for _, code := range codes {
			reasons = append(reasons, types.CancellationReason{Code: aws.String(code)})
		}

		return &types.TransactionCanceledException{CancellationReasons: reasons}
	}

	var failed *evt.ProjectorOperationError

	for _, reason := range []string{"ValidationError", "ItemCollectionSizeLimitExceeded"} {
		err := repo.classifyCommitFailure(context.Background(), nil, nil, layout, cancelled("None", "None", reason))
		require.ErrorAs(t, err, &failed, reason)
		require.Equal(t, 1, failed.Operation, reason)
		require.Equal(t, reason, failed.Reason, reason)
		require.Nil(t, failed.Err, "a mapped error belongs to a failed condition only")
		require.NotNil(t, failed.Cause, "the service's error stays matchable")
	}

	// A concurrent transaction or throttling on a view item keeps its transient classification.
	for _, reason := range []string{"TransactionConflict", "ThrottlingError"} {
		err := repo.classifyCommitFailure(context.Background(), nil, nil, layout, cancelled("None", "None", reason))
		require.NotErrorIs(t, err, evt.ErrProjectorOperation, reason)
		require.True(t, policy.IsTransient(err), reason)
	}
}
