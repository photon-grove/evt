package dynamo_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"

	"github.com/photon-grove/evt"
	"github.com/photon-grove/evt/dynamo"
	"github.com/photon-grove/evt/internal/compat"
	"github.com/photon-grove/evt/policy"
	"github.com/photon-grove/evt/snapshots"
)

// scriptedClient is a FakeDynamo whose TransactWriteItems and GetItem can be scripted.
type scriptedClient struct {
	*compat.FakeDynamo

	transactErr error
	getInputs   []*dynamodb.GetItemInput

	// inProgress makes TransactWriteItems report a transaction that is still running: the request
	// is held, and applied for real once the outcome row has been polled landAfter times (zero
	// means it never lands within the test).
	inProgress bool
	landAfter  int
	held       *dynamodb.TransactWriteItemsInput
	polls      int
}

func (c *scriptedClient) TransactWriteItems(
	ctx context.Context,
	params *dynamodb.TransactWriteItemsInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.TransactWriteItemsOutput, error) {
	if c.transactErr != nil {
		return nil, c.transactErr
	}

	if c.inProgress {
		c.held = params

		return nil, &smithy.GenericAPIError{Code: "TransactionInProgressException", Fault: smithy.FaultClient}
	}

	return c.FakeDynamo.TransactWriteItems(ctx, params, options...)
}

func (c *scriptedClient) GetItem(
	ctx context.Context,
	params *dynamodb.GetItemInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.GetItemOutput, error) {
	c.getInputs = append(c.getInputs, params)

	if c.held != nil {
		c.polls++

		if c.landAfter > 0 && c.polls >= c.landAfter {
			if _, err := c.FakeDynamo.TransactWriteItems(ctx, c.held); err != nil {
				return nil, err
			}

			c.held = nil
		}
	}

	return c.FakeDynamo.GetItem(ctx, params, options...)
}

func outcomeEvent(id evt.EntityID, sequence evt.EventSequence) evt.SerializedEvent {
	return evt.SerializedEvent{
		ID: evt.GetEventID(id, sequence), Sequence: sequence, Type: "t:e", Version: 1,
		EntityID: id, EntityType: "t", Payload: []byte(`{}`),
	}
}

func accepted(id string) *evt.Outcome {
	return &evt.Outcome{CommandID: evt.CommandID(id), Status: evt.OutcomeAccepted, Digest: "digest"}
}

func TestOutcomeRowIsTheLastActionAndMatchesTheSharedItemShape(t *testing.T) {
	fake := compat.NewFakeDynamo()
	repo := dynamo.NewRepository(fake, "events")

	outcome := &evt.Outcome{
		CommandID: "cmd-1", Status: evt.OutcomeAccepted, Digest: "digest",
		Response: []byte{0, 255, 7},
	}
	require.NoError(t, repo.Commit(context.Background(), evt.SerializedResult{
		Events:  []evt.SerializedEvent{outcomeEvent("a", 1), outcomeEvent("a", 2)},
		Outcome: outcome,
	}))

	calls := fake.Transactions()
	require.Len(t, calls, 1)

	actions := calls[0]
	last := actions[len(actions)-1]
	require.Equal(t, "Put", last.Operation)
	require.Equal(t, "command-outcome#cmd-1", last.PK)
	require.EqualValues(t, 1, last.SK)
	require.Equal(t, "attribute_not_exists(pk)", last.Condition)

	// The stored item is exactly the attributes the shared helper produces, in the item's size.
	stored := fake.Items("command-outcome#cmd-1")
	require.Len(t, stored, 1)

	expected := *outcome
	expected.EventIDs = []evt.EventID{"a:1", "a:2"}

	attributes, err := evt.OutcomeAttributes(expected)
	require.NoError(t, err)
	require.Len(t, stored[0], len(attributes))

	for _, attribute := range attributes {
		if attribute.Number {
			require.Equal(t, &types.AttributeValueMemberN{Value: attribute.Value}, stored[0][attribute.Name])
		} else {
			require.Equal(t, &types.AttributeValueMemberS{Value: attribute.Value}, stored[0][attribute.Name])
		}
	}

	require.NotContains(t, stored[0], "ttl", "an outcome row never carries a ttl")

	size, err := evt.OutcomeItemSize(expected, []evt.SerializedEvent{outcomeEvent("a", 1), outcomeEvent("a", 2)})
	require.NoError(t, err)
	require.Equal(t, size, dynamo.ItemSize(stored[0]))
}

func TestOutcomeOnlyCommitIsTheMarkerCheckAndTheOutcomePut(t *testing.T) {
	fake := compat.NewFakeDynamo()
	repo := dynamo.NewRepository(fake, "events")

	require.NoError(t, repo.Commit(context.Background(), evt.SerializedResult{Outcome: accepted("cmd-1")}))

	calls := fake.Transactions()
	require.Len(t, calls, 1)
	require.Len(t, calls[0], 2)
	require.Equal(t, "SYSTEM#FEATURES", calls[0][0].PK)
	require.Equal(t, "command-outcome#cmd-1", calls[0][1].PK)
}

func TestLostOutcomePutOutranksOtherFailures(t *testing.T) {
	fake := compat.NewFakeDynamo()
	repo := dynamo.NewRepository(fake, "events")
	ctx := context.Background()

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{
		Events: []evt.SerializedEvent{outcomeEvent("a", 1)}, Outcome: accepted("cmd-1"),
	}))

	// The same command again collides on the event put AND the outcome put. The outcome put wins
	// the classification: the answer is in the row, not in re-preparing the command.
	err := repo.Commit(ctx, evt.SerializedResult{
		Events: []evt.SerializedEvent{outcomeEvent("a", 1)}, Outcome: accepted("cmd-1"),
	})

	var conflict *evt.OutcomeConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, evt.CommandID("cmd-1"), conflict.CommandID)
	require.ErrorIs(t, err, evt.ErrOutcomeConflict)
	require.False(t, evt.IsConflictErr(err), "a lost outcome put is not an optimistic-concurrency conflict")

	// An event collision under a fresh command ID is an ordinary conflict, and writes no outcome.
	err = repo.Commit(ctx, evt.SerializedResult{
		Events: []evt.SerializedEvent{outcomeEvent("a", 1)}, Outcome: accepted("cmd-2"),
	})
	require.ErrorIs(t, err, dynamo.ErrSnapshotRaceCondition)
	require.NotErrorIs(t, err, evt.ErrOutcomeConflict)
	require.Empty(t, fake.Items("command-outcome#cmd-2"))
}

func TestGetOutcomeReadsStronglyConsistentlyWhateverTheRepositorySetting(t *testing.T) {
	client := &scriptedClient{FakeDynamo: compat.NewFakeDynamo()}
	repo := dynamo.NewRepository(client, "events").WithConsistentRead(false)
	ctx := context.Background()

	require.NoError(t, repo.Commit(ctx, evt.SerializedResult{Outcome: accepted("cmd-1")}))

	stored, err := repo.GetOutcome(ctx, "cmd-1")
	require.NoError(t, err)
	require.Equal(t, "digest", stored.Digest)

	require.NotEmpty(t, client.getInputs)

	for _, input := range client.getInputs {
		require.NotNil(t, input.ConsistentRead)
		require.True(t, *input.ConsistentRead, "the outcome read is always strongly consistent")
	}

	absent, err := repo.GetOutcome(ctx, "never")
	require.NoError(t, err)
	require.Nil(t, absent)

	_, err = repo.GetOutcome(ctx, "")
	require.ErrorIs(t, err, evt.ErrInvalidArgument)
}

func TestIndeterminateCommitFailuresAreAmbiguousWrites(t *testing.T) {
	validation := &smithy.GenericAPIError{Code: "ValidationException", Fault: smithy.FaultClient}
	missingTable := &smithy.GenericAPIError{Code: "ResourceNotFoundException", Fault: smithy.FaultClient}
	denied := &smithy.GenericAPIError{Code: "AccessDeniedException", Fault: smithy.FaultClient}
	server := &smithy.GenericAPIError{Code: "InternalServerError", Fault: smithy.FaultServer}
	unavailable := &smithy.GenericAPIError{Code: "ServiceUnavailable", Fault: smithy.FaultUnknown}
	dispatch := &smithyhttp.RequestSendError{Err: errors.New("dial tcp: connection refused")}
	cancelled := &types.TransactionCanceledException{CancellationReasons: []types.CancellationReason{{Code: strPtr("None")}}}

	tests := []struct {
		name      string
		err       error
		ambiguous bool
	}{
		{"deadline", context.DeadlineExceeded, true},
		{"cancelled context", context.Canceled, true},
		{"server fault", server, true},
		{"service unavailable", unavailable, true},
		{"dispatch failure", dispatch, true},
		{"network error", &net.OpError{Op: "read", Err: errors.New("connection reset by peer")}, true},
		{"truncated response", io.ErrUnexpectedEOF, true},
		{"validation error", validation, false},
		{"missing table", missingTable, false},
		{"authorization error", denied, false},
		{"unrecognized failure", errors.New("failed to sign request"), false},
		{"cancelled transaction", cancelled, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &scriptedClient{FakeDynamo: compat.NewFakeDynamo(), transactErr: test.err}
			repo := dynamo.NewRepository(client, "events")

			err := repo.Commit(context.Background(), evt.SerializedResult{Outcome: accepted("cmd-1")})
			require.Error(t, err)

			var ambiguous *evt.AmbiguousWriteError
			if test.ambiguous {
				require.ErrorAs(t, err, &ambiguous)
				require.ErrorIs(t, err, test.err)
				require.Equal(t, evt.CommandID("cmd-1"), ambiguous.CommandID)
				require.False(t, ambiguous.NotApplied, "only recovery can say the commit did not apply")
			} else {
				require.NotErrorAs(t, err, &ambiguous)
			}

			// Without an outcome there is nothing to recover from, so the error is left as it was.
			plain := repo.Commit(context.Background(), evt.SerializedResult{Events: []evt.SerializedEvent{outcomeEvent("a", 1)}})
			require.NotErrorAs(t, plain, &ambiguous)
		})
	}
}

func TestCommitStreamRefusesOutcomes(t *testing.T) {
	err := evt.CheckBatchEntry(evt.SerializedResult{Outcome: accepted("cmd-1")})

	var unsupported *evt.UnsupportedBatchEntryError
	require.ErrorAs(t, err, &unsupported)
	require.Equal(t, 1, unsupported.Outcomes)
}

func strPtr(value string) *string { return &value }

func deposit(amount int) *scriptedCommand { return &scriptedCommand{Amount: amount} }

func TestTransactionInProgressWaitsForTheOutcomeRowInsteadOfCallingItNotApplied(t *testing.T) {
	client := &scriptedClient{FakeDynamo: compat.NewFakeDynamo(), inProgress: true, landAfter: 3}
	repo := dynamo.NewRepository(client, "events").WithCommitInProgressWait(5 * time.Second)
	store := snapshots.NewStore(repo, 0).WithOutcomes()
	ctx := context.Background()

	id := evt.CommandID("cmd-1")
	entity := newScriptedEntity("e-1")

	// The transaction commits while the engine is polling for its row: the command returns the
	// stored outcome and the handler ran exactly once.
	outcome, err := store.ExecuteWithOutcome(ctx, entity, "e-1", deposit(5), evt.Metadata{CommandID: &id})
	require.NoError(t, err)
	require.True(t, outcome.Replayed, "the delayed commit is found, not re-run")
	require.Equal(t, 1, entity.handled)
	require.GreaterOrEqual(t, client.polls, 3)

	events, err := repo.GetEvents(ctx, "e-1")
	require.NoError(t, err)
	require.Len(t, events, 1)
}

func TestTransactionInProgressThatNeverLandsIsRetryableNotAmbiguousNotApplied(t *testing.T) {
	client := &scriptedClient{FakeDynamo: compat.NewFakeDynamo(), inProgress: true}
	repo := dynamo.NewRepository(client, "events").WithCommitInProgressWait(40 * time.Millisecond)
	store := snapshots.NewStore(repo, 0).WithOutcomes()
	ctx := context.Background()

	id := evt.CommandID("cmd-1")

	_, err := store.ExecuteWithOutcome(ctx, newScriptedEntity("e-1"), "e-1", deposit(5), evt.Metadata{CommandID: &id})
	require.ErrorIs(t, err, evt.ErrCommitInProgress)
	require.NotErrorIs(t, err, evt.ErrAmbiguousWrite, "it must never be reported as known not to have applied")
	require.True(t, policy.IsTransient(err), "the retry is safe: every attempt reads the outcome row first")

	// The retry, once DynamoDB stops reporting the transaction as running, commits.
	client.inProgress = false

	outcome, err := store.ExecuteWithOutcome(ctx, newScriptedEntity("e-1"), "e-1", deposit(5), evt.Metadata{CommandID: &id})
	require.NoError(t, err)
	require.False(t, outcome.Replayed)
}
