package address

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_GetClientAddress(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		headers        map[string]string
		remoteAddr     string
		expectedResult string
	}{
		{
			name:           "x-forwarded-for single",
			headers:        map[string]string{"x-forwarded-for": "203.0.113.1"},
			remoteAddr:     "192.168.1.1:4000",
			expectedResult: "203.0.113.1",
		},
		{
			name:           "x-forwarded-for multiple takes first",
			headers:        map[string]string{"x-forwarded-for": "203.0.113.1, 203.0.113.2"},
			remoteAddr:     "192.168.1.1:4000",
			expectedResult: "203.0.113.1",
		},
		{
			name:           "fallback to RemoteAddr",
			headers:        map[string]string{},
			remoteAddr:     "192.168.1.1:4000",
			expectedResult: "192.168.1.1:4000",
		},
		{
			name:           "ipv6 forwarded",
			headers:        map[string]string{"x-forwarded-for": "2001:db8::1"},
			remoteAddr:     "[::1]:4000",
			expectedResult: "2001:db8::1",
		},
		{
			name:           "mixed ipv4/ipv6 uses first",
			headers:        map[string]string{"x-forwarded-for": "203.0.113.1, 2001:db8::1"},
			remoteAddr:     "192.168.1.1:4000",
			expectedResult: "203.0.113.1",
		},
		{
			name:           "malformed header returns as-is",
			headers:        map[string]string{"x-forwarded-for": "not-an-ip"},
			remoteAddr:     "192.168.1.1:4000",
			expectedResult: "not-an-ip",
		},
		{
			name:           "empty RemoteAddr",
			headers:        map[string]string{},
			remoteAddr:     "",
			expectedResult: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := mockHTTPRequest("GET", "http://example.com", "", tt.headers)
			req.RemoteAddr = tt.remoteAddr
			result := GetClientAddress(ctx, req)
			require.Equal(t, tt.expectedResult, result)
		})
	}
}

func Test_WithAndGet(t *testing.T) {
	base := context.Background()

	t.Run("stores and retrieves address", func(t *testing.T) {
		req := mockHTTPRequest("GET", "http://example.com", "", map[string]string{
			"x-forwarded-for": "203.0.113.1, 203.0.113.2",
		})
		req.RemoteAddr = "192.168.1.1:4000"

		ctx := With(base, req)
		addr := Get(ctx)
		require.NotNil(t, addr)
		require.Equal(t, "203.0.113.1", *addr)
	})

	t.Run("fallback remote addr stored", func(t *testing.T) {
		req := mockHTTPRequest("GET", "http://example.com", "", nil)
		req.RemoteAddr = "192.168.1.1:4000"

		ctx := With(base, req)
		addr := Get(ctx)
		require.NotNil(t, addr)
		require.Equal(t, "192.168.1.1:4000", *addr)
	})

	t.Run("context override replaces address", func(t *testing.T) {
		req1 := mockHTTPRequest("GET", "http://example.com", "", map[string]string{
			"x-forwarded-for": "203.0.113.1",
		})
		req1.RemoteAddr = "192.168.1.1:4000"
		req2 := mockHTTPRequest("GET", "http://example.com", "", map[string]string{
			"x-forwarded-for": "203.0.113.2",
		})
		req2.RemoteAddr = "192.168.1.2:4000"

		ctx1 := With(base, req1)
		ctx2 := With(ctx1, req2)

		addr := Get(ctx2)
		require.NotNil(t, addr)
		require.Equal(t, "203.0.113.2", *addr)
	})

	t.Run("Get returns nil when unset", func(t *testing.T) {
		addr := Get(base)
		require.Nil(t, addr)
	})
}

func Test_Concurrency(t *testing.T) {
	req := mockHTTPRequest("GET", "http://example.com", "", map[string]string{
		"x-forwarded-for": "203.0.113.1",
	})
	req.RemoteAddr = "192.168.1.1:4000"
	ctx := With(context.Background(), req)

	done := make(chan struct{}, 16)
	for i := 0; i < 16; i++ {
		go func() {
			a := Get(ctx)
			require.NotNil(t, a)
			require.Equal(t, "203.0.113.1", *a)
			done <- struct{}{}
		}()
	}
	for i := 0; i < 16; i++ {
		<-done
	}
}

// Single representative benchmark
func BenchmarkGetClientAddress(b *testing.B) {
	ctx := context.Background()
	req := mockHTTPRequest("GET", "http://example.com", "", map[string]string{
		"x-forwarded-for": "198.51.100.1, 198.51.100.2",
	})
	req.RemoteAddr = "192.168.1.1:4000"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetClientAddress(ctx, req)
	}
}

func mockHTTPRequest(method, url, body string, headers map[string]string) *http.Request {
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	return req
}
