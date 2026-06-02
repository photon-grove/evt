package projectors_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/photon-grove/evt/projectors"
	"github.com/stretchr/testify/assert"
)

func TestClassifyError_Nil(t *testing.T) {
	assert.Equal(t, projectors.RetryTransient, projectors.ClassifyError(nil))
}

func TestClassifyError_PermanentError(t *testing.T) {
	err := projectors.NewPermanentError(errors.New("bad data"))
	assert.Equal(t, projectors.RetryPermanent, projectors.ClassifyError(err))
}

func TestClassifyError_WrappedPermanentError(t *testing.T) {
	inner := projectors.NewPermanentError(errors.New("bad data"))
	wrapped := errors.Join(errors.New("outer"), inner)
	assert.Equal(t, projectors.RetryPermanent, projectors.ClassifyError(wrapped))
}

func TestClassifyError_DeadlineExceeded(t *testing.T) {
	assert.Equal(t, projectors.RetryTransient, projectors.ClassifyError(context.DeadlineExceeded))
}

func TestClassifyError_Canceled(t *testing.T) {
	assert.Equal(t, projectors.RetryTransient, projectors.ClassifyError(context.Canceled))
}

func TestClassifyError_NetworkError(t *testing.T) {
	err := &net.OpError{Op: "dial", Err: errors.New("connection refused")}
	assert.Equal(t, projectors.RetryTransient, projectors.ClassifyError(err))
}

func TestClassifyError_UnexpectedEOF(t *testing.T) {
	assert.Equal(t, projectors.RetryTransient, projectors.ClassifyError(io.ErrUnexpectedEOF))
}

func TestClassifyError_Unknown(t *testing.T) {
	assert.Equal(t, projectors.RetryUnknown, projectors.ClassifyError(errors.New("something weird")))
}
