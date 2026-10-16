package dynamo

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"

	"github.com/photon-grove/evt"
)

// OutcomeCondition guards the outcome put (storage spec, Command Outcomes): the row is written
// once, in its final state, so an existing row means another writer recorded the command first.
const OutcomeCondition = "attribute_not_exists(pk)"

var _ evt.OutcomeRepository = (*Repository)(nil)

// buildOutcomePut builds the outcome row's Put, the last action of the commit transaction. The
// item's shape comes from evt.OutcomeAttributes, which the commit budget also sizes.
func (repo *Repository) buildOutcomePut(outcome evt.Outcome) (types.TransactWriteItem, error) {
	attributes, err := evt.OutcomeAttributes(outcome)
	if err != nil {
		return types.TransactWriteItem{}, err
	}

	item := make(map[string]types.AttributeValue, len(attributes))
	for _, attribute := range attributes {
		if attribute.Number {
			item[attribute.Name] = &types.AttributeValueMemberN{Value: attribute.Value}
		} else {
			item[attribute.Name] = &types.AttributeValueMemberS{Value: attribute.Value}
		}
	}

	put := types.Put{
		TableName:           &repo.EventsTable,
		Item:                item,
		ConditionExpression: aws.String(OutcomeCondition),
	}

	return types.TransactWriteItem{Put: &put}, nil
}

// GetOutcome reads a command's outcome row with a strongly consistent GetItem, regardless of the
// repository's read-consistency setting: an eventually consistent read could return absent after
// the transaction applied, and the absent branch would then re-execute a committed command.
func (repo *Repository) GetOutcome(ctx context.Context, id evt.CommandID) (*evt.Outcome, error) {
	if err := evt.CheckCommandID(id); err != nil {
		return nil, err
	}

	output, err := repo.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: &repo.EventsTable,
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: evt.OutcomeKey(id)},
			"sk": &types.AttributeValueMemberN{Value: "1"},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, wrapClassifiedError(fmt.Errorf("dynamo: get outcome %s: %w", id, err))
	}

	if len(output.Item) == 0 {
		return nil, nil
	}

	text := func(name string) string {
		if value, ok := output.Item[name].(*types.AttributeValueMemberS); ok {
			return value.Value
		}

		return ""
	}

	outcome, err := evt.OutcomeFromAttributes(id, text("status"), text("digest"), text("response"), text("events"), text("rejection"))
	if err != nil {
		return nil, fmt.Errorf("dynamo: outcome %s: %w", id, err)
	}

	return &outcome, nil
}

// indeterminateCommit reports whether a TransactWriteItems failure leaves the caller unable to
// know whether the transaction applied: a timeout, a lost connection or response, or a server
// fault. A definitive refusal — a cancelled transaction (its reasons are known), throttling, or a
// client-side validation or authorization error — means nothing was written.
func indeterminateCommit(err error) bool {
	var canceled *types.TransactionCanceledException
	if err == nil || errors.As(err, &canceled) {
		return false
	}

	// Client faults (validation, access, throttling) are refused before anything is written. A
	// server fault may follow a partially processed request.
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorFault() != smithy.FaultClient
	}

	// Anything else — a deadline, a cancelled context, a reset connection, a truncated response —
	// is indeterminate. Recovery is a single consistent read, so an unnecessary one is harmless
	// where a missed one would report a committed command as failed.
	return true
}
