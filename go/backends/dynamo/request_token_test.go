package dynamo_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	evt "github.com/photon-grove/evt/go"
	"github.com/photon-grove/evt/go/backends/dynamo"
	"github.com/photon-grove/evt/go/internal/compat"
	"github.com/photon-grove/evt/go/snapshots"
)

// tokenRecorder is a FakeDynamo that records the ClientRequestToken of every commit.
type tokenRecorder struct {
	*compat.FakeDynamo

	tokens []*string
}

func (c *tokenRecorder) TransactWriteItems(
	ctx context.Context,
	params *dynamodb.TransactWriteItemsInput,
	options ...func(*dynamodb.Options),
) (*dynamodb.TransactWriteItemsOutput, error) {
	c.tokens = append(c.tokens, params.ClientRequestToken)

	return c.FakeDynamo.TransactWriteItems(ctx, params, options...)
}

// Commits leave ClientRequestToken to the SDK, which fills in a fresh one per call and resends it
// on its own retries (storage spec, Ambiguous-write recovery). A token derived from the command ID would make a
// fence-aware retry, which resends the command with other items, fail with
// IdempotentParameterMismatch.
func TestCommitsLeaveTheRequestTokenToTheSDK(t *testing.T) {
	client := &tokenRecorder{FakeDynamo: compat.NewFakeDynamo()}
	repo := dynamo.NewRepository(client, "events").WithLazyInitialization(true)
	ctx := context.Background()

	// A snapshot on every commit, so the snapshot put rides along too.
	plain := snapshots.NewStore(repo, 1)
	recorded := snapshots.NewStore(repo, 1).WithOutcomes()

	require.NoError(t, plain.Execute(ctx, newScriptedAggregate("e-plain"), "e-plain", deposit(1), evt.Metadata{}))

	id := evt.CommandID("with-outcome")
	_, err := recorded.ExecuteWithOutcome(ctx, newScriptedAggregate("e-outcome"), "e-outcome", deposit(2), evt.Metadata{CommandID: &id})
	require.NoError(t, err)

	require.NotEmpty(t, client.tokens)
	for _, token := range client.tokens {
		require.Nil(t, token, "a commit never sets its own ClientRequestToken")
	}
}

// The SDK fills in a ClientRequestToken that the commit left unset and resends the same token when
// it retries, so a retry that reaches DynamoDB while its first attempt is still in flight is
// answered with TransactionInProgressException (indeterminate), never TransactionConflict, and one
// whose first attempt applied is answered with success. A new call gets a new token.
func TestTheSDKResendsItsRequestTokenOnARetry(t *testing.T) {
	var (
		mu     sync.Mutex
		tokens []string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct{ ClientRequestToken string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}

		mu.Lock()
		tokens = append(tokens, body.ClientRequestToken)
		first := len(tokens) == 1
		mu.Unlock()

		w.Header().Set("Content-Type", "application/x-amz-json-1.0")

		// The first attempt's response is lost to a server fault; the SDK retries it.
		answer := `{}`
		if first {
			w.WriteHeader(http.StatusInternalServerError)
			answer = `{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"lost"}`
		}

		if _, err := w.Write([]byte(answer)); err != nil {
			t.Errorf("write the answer: %v", err)
		}
	}))
	defer server.Close()

	client := dynamodb.New(dynamodb.Options{
		Region:       "us-west-2",
		BaseEndpoint: aws.String(server.URL),
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
		Retryer: retry.NewStandard(func(o *retry.StandardOptions) {
			o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) { return 0, nil })
		}),
	})

	commit := func() {
		_, err := client.TransactWriteItems(context.Background(), &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{{
				Put: &types.Put{
					TableName: aws.String("events"),
					Item:      map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "e-1"}},
				},
			}},
		})
		require.NoError(t, err)
	}

	commit()
	commit()

	require.Len(t, tokens, 3, "the first call is retried once, the second succeeds at once")
	require.NotEmpty(t, tokens[0], "the SDK fills in a token")
	require.Equal(t, tokens[0], tokens[1], "the retry resends the first attempt's token")
	require.NotEqual(t, tokens[0], tokens[2], "a new call gets a new token")
}
