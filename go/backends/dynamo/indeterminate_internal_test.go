package dynamo

import (
	"errors"
	"net/http"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"
)

// Any 5xx is an unknown result, whatever error code a gateway or proxy attached to it: the
// transaction may have applied before the response was lost.
func TestIndeterminateCommitTreatsAnyServerStatusAsUnknown(t *testing.T) {
	response := func(status int) error {
		return &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
				Err:      &smithy.GenericAPIError{Code: "SomeGatewayCode", Fault: smithy.FaultUnknown},
			},
		}
	}

	for _, status := range []int{500, 502, 503, 504} {
		require.True(t, indeterminateCommit(response(status)), "%d", status)
	}

	require.False(t, indeterminateCommit(response(400)))
	require.False(t, indeterminateCommit(errors.New("validation failed")))
}
