// Package address houses address-related utilities
package address

import (
	"context"
	"net/http"
	"strings"
)

type contextKey int

const addressContextKey contextKey = iota

// GetClientAddress first tries to get the x-forwarded-for header, then falls back to
// the RemoteAddr.
func GetClientAddress(_ context.Context, req *http.Request) string {
	remoteAddr := req.RemoteAddr
	fwdAddr := req.Header.Get("x-forwarded-for")

	if fwdAddr != "" {
		ips := strings.Split(fwdAddr, ", ")
		if len(ips) > 1 {
			return ips[0]
		}

		return fwdAddr
	}

	return remoteAddr
}

// With returns a new context with the given client IP address
func With(ctx context.Context, req *http.Request) context.Context {
	clientAddress := GetClientAddress(ctx, req)

	return context.WithValue(ctx, addressContextKey, clientAddress)
}

// Get returns the client IP address from the context
func Get(ctx context.Context) *string {
	clientAddress := ctx.Value(addressContextKey)
	if clientAddress != nil {
		if str, ok := clientAddress.(string); ok {
			return &str
		}
	}

	return nil
}
