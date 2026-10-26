package dynamo

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/policy"
)

func projected(name string, index, operation int, op evt.ViewOperation) evt.ProjectedView {
	return evt.ProjectedView{Projector: name, ProjectorIndex: index, Operation: operation, Op: op}
}

func Test_buildViewItems_ShapesEveryOperationAsTheSpecPins(t *testing.T) {
	repo := NewRepository(nil, "events").WithLazyInitialization(true).WithViewsTable("views")

	items, err := repo.buildViewItems([]evt.ProjectedView{
		projected("p", 0, 0, evt.PutView("pk", "sk", "owner", "account", []byte(`{"v":1}`))),
		projected("p", 0, 1, evt.PutView("pk", "claim", "owner", "account", []byte(`{}`)).IfAbsent()),
		projected("p", 0, 2, evt.DeleteView("pk", "gone", "owner")),
		projected("p", 0, 3, evt.DeleteView("pk", "release", "owner").IfPresent()),
	})
	require.NoError(t, err)
	require.Len(t, items, 4)

	plain := items[0].Put
	require.NotNil(t, plain)
	require.Equal(t, "views", aws.ToString(plain.TableName))
	require.Equal(t, "attribute_not_exists(pk) OR entityID = :owner", aws.ToString(plain.ConditionExpression),
		"an unconditional put still refuses a row another stream owns")
	require.Equal(t, owned("owner"), plain.ExpressionAttributeValues)
	require.Equal(t, map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: "pk"},
		"sk":         &types.AttributeValueMemberS{Value: "sk"},
		"entityID":   &types.AttributeValueMemberS{Value: "owner"},
		"entityType": &types.AttributeValueMemberS{Value: "account"},
		"payload":    &types.AttributeValueMemberS{Value: `{"v":1}`},
	}, plain.Item, "all five attributes, and never a ttl")

	require.Equal(t, "attribute_not_exists(pk)", aws.ToString(items[1].Put.ConditionExpression),
		"an absent row has no owner to check")
	require.Nil(t, items[1].Put.ExpressionAttributeValues)

	deletion := items[2].Delete
	require.NotNil(t, deletion)
	require.Equal(t, "views", aws.ToString(deletion.TableName))
	require.Equal(t, map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "pk"},
		"sk": &types.AttributeValueMemberS{Value: "gone"},
	}, deletion.Key)
	require.Equal(t, "attribute_not_exists(pk) OR entityID = :owner", aws.ToString(deletion.ConditionExpression),
		"an unconditional delete still refuses a row another stream owns")
	require.Equal(t, owned("owner"), deletion.ExpressionAttributeValues)

	require.Equal(t, "attribute_exists(pk) AND entityID = :owner", aws.ToString(items[3].Delete.ConditionExpression))
	require.Equal(t, owned("owner"), items[3].Delete.ExpressionAttributeValues)
}

func owned(owner string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{":owner": &types.AttributeValueMemberS{Value: owner}}
}

func Test_buildViewItems_AnEmptyEntityTypeIsStillWritten(t *testing.T) {
	repo := NewRepository(nil, "events").WithLazyInitialization(true).WithViewsTable("views")

	items, err := repo.buildViewItems([]evt.ProjectedView{projected("p", 0, 0, evt.PutView("pk", "sk", "owner", "", nil))})
	require.NoError(t, err)

	item := items[0].Put.Item
	require.Equal(t, &types.AttributeValueMemberS{Value: "owner"}, item["entityID"], "the owner is the row's entityID")
	require.Equal(t, &types.AttributeValueMemberS{}, item["entityType"], "entityType is always written")
	require.Equal(t, &types.AttributeValueMemberS{}, item["payload"])
}

func Test_buildViewItems_RefusesOperationsWithoutAViewsTable(t *testing.T) {
	_, err := NewRepository(nil, "events").WithLazyInitialization(true).buildViewItems([]evt.ProjectedView{
		projected("p", 0, 0, evt.PutView("pk", "sk", "owner", "", nil)),
		projected("p", 0, 1, evt.DeleteView("pk", "other", "owner")),
	})

	var refused *evt.ViewsNotConfiguredError

	require.ErrorAs(t, err, &refused)
	require.Equal(t, "dynamodb", refused.Backend)
	require.Equal(t, 2, refused.Operations)

	items, err := NewRepository(nil, "events").WithLazyInitialization(true).buildViewItems(nil)
	require.NoError(t, err)
	require.Empty(t, items, "a commit with no view operations needs no views table")
}

func Test_theBudgetedSizeOfAViewOperationIsTheSizeOfItsItem(t *testing.T) {
	repo := NewRepository(nil, "events").WithLazyInitialization(true).WithViewsTable("views")

	for name, op := range map[string]evt.ViewOperation{
		"put":                  evt.PutView("pk", "sk", "owner", "account", []byte(`{"balance":10}`)),
		"conditioned put":      evt.PutView("pk", "sk", "owner", "account", []byte(`{}`)).IfAbsent(),
		"put without type":     evt.PutView("pk", "sk", "owner", "", nil),
		"multi-byte keys":      evt.PutView("p€k", "s€k", "ö", "ü", []byte("€")),
		"delete":               evt.DeleteView("pk", "sk", "owner"),
		"conditioned delete":   evt.DeleteView("pk", "sk", "owner").IfPresent(),
		"multi-byte owner":     evt.DeleteView("pk", "sk", "ö€"),
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
		projected("profile", 0, 0, evt.PutView("pk", "profile", "o", "", nil)),
		projected("claims", 1, 0, evt.PutView("pk", "note", "o", "", nil)),
		projected("claims", 1, 1, evt.PutView("pk", "ada", "o", "", nil).IfAbsent()),
	}
	layout := commitLayout{views: views}

	cancelled := func(codes ...string) error {
		reasons := make([]types.CancellationReason, 0, len(codes))
		for _, code := range codes {
			reasons = append(reasons, types.CancellationReason{Code: aws.String(code)})
		}

		return &types.TransactionCanceledException{CancellationReasons: reasons}
	}

	// Indexes 0 and 1 are the marker and rebuild-lock checks; the views occupy indexes 2 to 4.
	err := repo.classifyCommitFailure(context.Background(), nil, nil, layout,
		cancelled("None", "None", "None", "None", "ConditionalCheckFailed"))

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
		cancelled("None", "None", "None", "ConditionalCheckFailed", "ConditionalCheckFailed"))
	require.ErrorAs(t, err, &failed)
	require.Equal(t, 0, failed.Operation)
	require.Equal(t, "claims", failed.Projector)
}

func Test_classifyCommitFailure_ItemLevelReasonsOnAViewAreTheProjectorsAndTransientOnesAreNot(t *testing.T) {
	repo := &Repository{}
	layout := commitLayout{views: []evt.ProjectedView{
		projected("p", 0, 0, evt.PutView("pk", "a", "o", "", nil)),
		projected("p", 0, 1, evt.PutView("pk", "b", "o", "", nil)),
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
		err := repo.classifyCommitFailure(context.Background(), nil, nil, layout, cancelled("None", "None", "None", reason))
		require.ErrorAs(t, err, &failed, reason)
		require.Equal(t, 1, failed.Operation, reason)
		require.Equal(t, reason, failed.Reason, reason)
		require.Nil(t, failed.Err, "a mapped error belongs to a failed condition only")
		require.NotNil(t, failed.Cause, "the service's error stays matchable")
	}

	// A concurrent transaction or throttling on a view item keeps its transient classification.
	for _, reason := range []string{"TransactionConflict", "ThrottlingError"} {
		err := repo.classifyCommitFailure(context.Background(), nil, nil, layout, cancelled("None", "None", "None", reason))
		require.NotErrorIs(t, err, evt.ErrProjectorOperation, reason)
		require.True(t, policy.IsTransient(err), reason)
	}
}

func Test_aDeleteIsBudgetedByTheConditionAndOwnerValueTheRepositorySends(t *testing.T) {
	keys := len("pk") + len("sk") + len("p") + len("s")

	unconditional := evt.DeleteView("p", "s", "own")
	require.Equal(t, keys+len(ViewOwnedCondition)+len(ViewOwnerValue)+len("own"), evt.ViewOperationSize(unconditional))

	present := unconditional.IfPresent()
	require.Equal(t, keys+len(ViewPresentCondition)+len(ViewOwnerValue)+len("own"), evt.ViewOperationSize(present))
}

func Test_DeclareHistoryDependent_ChecksTheFeatureMarkerBeforeAnyRead(t *testing.T) {
	repo := NewRepository(nil, "events").WithLazyInitialization(true).WithViewsTable("views")

	// A repository whose events table no longer matches its marker's is refused before the manifest
	// is read, like every other write path.
	moved := *repo
	moved.EventsTable = "other-events"

	var mismatch *evt.FeatureMarkerTableMismatchError

	require.ErrorAs(t, moved.DeclareHistoryDependent(context.Background(), "account"), &mismatch)
}
