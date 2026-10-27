package dynamo

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	evt "github.com/photon-grove/evt/go"
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

	// An absent attribute reads as empty; a present one of the wrong DynamoDB type is a corrupt
	// row, not an empty value.
	var (
		texts   [6]string
		present [6]bool
	)

	for i, name := range []string{"status", "digest", "response", "events", "rejection", "code"} {
		attribute, exists := output.Item[name]
		if !exists {
			continue
		}

		value, ok := attribute.(*types.AttributeValueMemberS)
		if !ok {
			return nil, &evt.CorruptOutcomeError{CommandID: id, Reason: fmt.Sprintf("attribute %s is not a string (%T)", name, attribute)}
		}

		texts[i], present[i] = value.Value, true
	}

	outcome, err := evt.OutcomeFromAttributes(id, texts[0], texts[1], texts[2], texts[3], texts[4])
	if err != nil {
		return nil, err
	}

	// The code attribute exists exactly with a rejection, and repeats its code.
	if outcome.Rejection == nil && present[5] {
		return nil, &evt.CorruptOutcomeError{CommandID: id, Reason: "an accepted outcome carries a code attribute"}
	}

	if outcome.Rejection != nil && texts[5] != string(outcome.Rejection.Code) {
		return nil, &evt.CorruptOutcomeError{CommandID: id, Reason: "the code attribute disagrees with the rejection"}
	}

	return &outcome, nil
}

// indeterminateCommit reports whether a TransactWriteItems failure leaves the caller unable to
// know whether the transaction applied: a timeout or cancelled context, a connection or dispatch
// failure, a truncated response, a 5xx, or a server fault. Everything else is a definitive
// refusal and is returned as is, never recovered: a cancelled transaction (its reasons are
// known), throttling, a client-side validation, missing-table, or authorization error, a request
// that could not be built or signed.
func indeterminateCommit(err error) bool {
	var canceled *types.TransactionCanceledException
	if err == nil || errors.As(err, &canceled) {
		return false
	}

	// Any 5xx is a server or gateway fault whatever its error code: a proxy's 502 or 504 can arrive
	// after the transaction applied.
	var response *awshttp.ResponseError
	if errors.As(err, &response) && response.HTTPStatusCode() >= http.StatusInternalServerError {
		return true
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "InternalServerError", "ServiceUnavailable", "TransactionInProgressException",
			"IdempotentParameterMismatchException":
			return true
		}

		// Client faults (validation, access, throttling, a missing table) are refused before
		// anything is written.
		return apiErr.ErrorFault() == smithy.FaultServer
	}

	var send *smithyhttp.RequestSendError
	var netErr net.Error

	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.As(err, &send) || errors.As(err, &netErr)
}
