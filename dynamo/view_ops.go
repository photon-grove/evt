package dynamo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/photon-grove/evt"
)

// The condition expressions of commit-wide view operations (storage spec, Views). Every view write
// is conditioned on the row's owner, the stream stored in its entityID attribute and given to the
// expression as :owner, so a row can never change hands: the write is refused when the row exists
// and belongs to another stream.
const (
	// ViewAbsentCondition is a view put that must not replace a row. The row is absent, so there is
	// no owner to check.
	ViewAbsentCondition = "attribute_not_exists(pk)"

	// ViewOwnedCondition is every other view put and every delete without a presence condition:
	// the row is absent or belongs to the operation's owner.
	ViewOwnedCondition = "attribute_not_exists(pk) OR entityID = :owner"

	// ViewPresentCondition is a view delete that must find a row: the row exists and belongs to
	// the operation's owner.
	ViewPresentCondition = "attribute_exists(pk) AND entityID = :owner"

	// ViewOwnerValue is the expression value naming the operation's owner.
	ViewOwnerValue = ":owner"
)

// WithViewsTable returns a shallow copy of the repository that writes commit-wide view operations
// (ADR 0004, C7) to the named table, in the same transaction as the events. The table has a string
// partition key pk and a string sort key sk, like the table behind NewViewRepository. Without it a
// commit that carries view operations fails closed with *evt.ViewsNotConfiguredError before any
// write. The original repository is unchanged.
func (repo *Repository) WithViewsTable(viewsTable string) *Repository {
	r := *repo
	r.viewsTable = viewsTable

	return &r
}

// buildViewItems builds the transaction items of a commit's view operations, in the order the
// commit writes them: one Put or Delete each, against the views table. A commit that carries
// operations and has no views table is refused with *evt.ViewsNotConfiguredError.
func (repo *Repository) buildViewItems(views []evt.ProjectedView) ([]types.TransactWriteItem, error) {
	if len(views) == 0 {
		return nil, nil
	}

	if repo.viewsTable == "" {
		return nil, &evt.ViewsNotConfiguredError{Backend: "dynamodb", Operations: len(views)}
	}

	items := make([]types.TransactWriteItem, 0, len(views))

	for _, view := range views {
		item, err := repo.buildViewItem(view.Op)
		if err != nil {
			return nil, err
		}

		items = append(items, item)
	}

	return items, nil
}

func (repo *Repository) buildViewItem(op evt.ViewOperation) (types.TransactWriteItem, error) {
	if op.Kind == evt.ViewDelete {
		deletion := types.Delete{
			TableName: aws.String(repo.viewsTable),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: op.PK},
				"sk": &types.AttributeValueMemberS{Value: op.SK},
			},
		}
		deletion.ConditionExpression = aws.String(ViewOwnedCondition)
		if op.Condition == evt.ViewPresent {
			deletion.ConditionExpression = aws.String(ViewPresentCondition)
		}

		deletion.ExpressionAttributeValues = ownerValue(op)

		return types.TransactWriteItem{Delete: &deletion}, nil
	}

	item, err := MarshalViewToItem(repo.encoder, &evt.SerializedView{
		PK: op.PK, SK: op.SK, EntityID: op.Owner, EntityType: op.EntityType, Payload: op.Payload,
	})
	if err != nil {
		return types.TransactWriteItem{}, err
	}

	put := types.Put{
		TableName:                 aws.String(repo.viewsTable),
		Item:                      item,
		ConditionExpression:       aws.String(ViewOwnedCondition),
		ExpressionAttributeValues: ownerValue(op),
	}
	if op.Condition == evt.ViewAbsent {
		put.ConditionExpression = aws.String(ViewAbsentCondition)
		put.ExpressionAttributeValues = nil
	}

	return types.TransactWriteItem{Put: &put}, nil
}

func ownerValue(op evt.ViewOperation) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{ViewOwnerValue: &types.AttributeValueMemberS{Value: string(op.Owner)}}
}

// viewFailure maps a failed view item of a cancelled commit to the typed error naming its projector
// and operation: a failed condition, or one of the item-level reasons that no retry can fix. It
// returns nil when the item did not fail for such a reason (it passed, or it failed transiently: a
// conflict or throttling keeps its classification).
func viewFailure(view evt.ProjectedView, code string, cause error) error {
	switch code {
	case evt.ReasonConditionFailed:
		// A failed condition has no cause beyond the condition itself.
		return evt.NewProjectorOperationError(view, code, nil)
	case "ValidationError", "ItemCollectionSizeLimitExceeded":
		return evt.NewProjectorOperationError(view, code, cause)
	}

	return nil
}

// WritesViews declares that Commit writes commit-wide view operations, or refuses them with
// *evt.ViewsNotConfiguredError when no views table is configured (evt.ViewWritingRepository).
func (repo *Repository) WritesViews() bool { return true }

// DeclareHistoryDependent durably adds the entity types to the feature manifest's historyDependent
// set, so CompactBelow refuses them in every process (evt.HistoryDeclarer). Types already declared
// are left alone and nothing is written. A store the writer refuses as uninitialized is refused
// here first, on the cheap validation the marker caches: the commit this declaration precedes
// would be refused without writing anything, and the declaration must not be the one thing that
// stays behind, forbidding compaction for types nothing ever committed.
func (repo *Repository) DeclareHistoryDependent(ctx context.Context, types ...evt.EntityType) error {
	if err := repo.checkFeatureMarkerTable(); err != nil {
		return err
	}

	if err := repo.features.Validate(ctx); err != nil {
		return err
	}

	manifest, err := repo.features.Manifest(ctx)
	if err != nil {
		return err
	}

	missing := evt.MissingHistoryDependent(manifest, types)
	if len(missing) == 0 {
		return nil
	}

	_, err = repo.features.Declare(ctx, func(m *evt.FeatureManifest) { m.AddHistoryDependent(missing...) })

	return err
}
