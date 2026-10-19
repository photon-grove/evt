package dynamo

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/photon-grove/evt"
)

// The condition expressions of commit-wide view operations (storage spec, Views): a put that must
// not replace a row, and a delete that must find one.
const (
	// ViewAbsentCondition is a conditional view put's condition.
	ViewAbsentCondition = "attribute_not_exists(pk)"

	// ViewPresentCondition is a conditional view delete's condition.
	ViewPresentCondition = "attribute_exists(pk)"
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
		if op.Condition == evt.ViewPresent {
			deletion.ConditionExpression = aws.String(ViewPresentCondition)
		}

		return types.TransactWriteItem{Delete: &deletion}, nil
	}

	item, err := MarshalViewToItem(repo.encoder, &evt.SerializedView{
		PK: op.PK, SK: op.SK, EntityID: op.EntityID, EntityType: op.EntityType, Payload: op.Payload,
	})
	if err != nil {
		return types.TransactWriteItem{}, err
	}

	put := types.Put{TableName: aws.String(repo.viewsTable), Item: item}
	if op.Condition == evt.ViewAbsent {
		put.ConditionExpression = aws.String(ViewAbsentCondition)
	}

	return types.TransactWriteItem{Put: &put}, nil
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
