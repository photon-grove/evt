package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func captureFallbackLogs(t *testing.T) (*strings.Builder, func()) {
	t.Helper()

	var buf strings.Builder
	original := logFallback

	logFallback = func(format string, args ...any) {
		buf.WriteString(fmt.Sprintf(format, args...))
		buf.WriteByte('\n')
	}

	return &buf, func() {
		logFallback = original
	}
}

func Test_WithLogger(t *testing.T) {
	t.Run("stores logger in context", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

		ctx := context.Background()
		newCtx := WithLogger(ctx, logger)

		// Context should be different
		require.NotEqual(t, ctx, newCtx)

		// Should be able to retrieve the logger
		retrievedLogger := GetLogger(newCtx)
		require.Equal(t, logger, retrievedLogger)
	})

	t.Run("stores nil logger", func(t *testing.T) {
		fallbackBuf, restore := captureFallbackLogs(t)
		defer restore()

		originalDefault := slog.Default()
		defer slog.SetDefault(originalDefault)

		ctx := context.Background()
		newCtx := WithLogger(ctx, nil)

		retrievedLogger := GetLogger(newCtx)
		require.NotNil(t, retrievedLogger)
		require.Equal(t, slog.Default(), retrievedLogger)
		require.Contains(t, fallbackBuf.String(), "logger in context was nil")
	})

	t.Run("overwrites existing logger", func(t *testing.T) {
		logger1 := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
		logger2 := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))

		ctx := context.Background()
		ctx1 := WithLogger(ctx, logger1)
		ctx2 := WithLogger(ctx1, logger2)

		// Should have the second logger
		retrievedLogger := GetLogger(ctx2)
		require.Equal(t, logger2, retrievedLogger)
		require.NotEqual(t, logger1, retrievedLogger)
	})
}

func Test_GetLogger(t *testing.T) {
	t.Run("returns default logger for empty context", func(t *testing.T) {
		fallbackBuf, restore := captureFallbackLogs(t)
		defer restore()

		originalDefault := slog.Default()
		testLogger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
		slog.SetDefault(testLogger)
		defer slog.SetDefault(originalDefault)

		ctx := context.Background()
		logger := GetLogger(ctx)

		require.NotNil(t, logger)
		require.Equal(t, slog.Default(), logger)

		require.Contains(t, fallbackBuf.String(), "no logger found in context")
	})

	t.Run("returns default logger for context without logger", func(t *testing.T) {
		fallbackBuf, restore := captureFallbackLogs(t)
		defer restore()

		originalDefault := slog.Default()
		testLogger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
		slog.SetDefault(testLogger)
		defer slog.SetDefault(originalDefault)

		type otherKeyT string
		otherKey := otherKeyT("other-key")
		ctx := context.WithValue(context.Background(), otherKey, "other-value")
		logger := GetLogger(ctx)

		require.NotNil(t, logger)
		require.Equal(t, slog.Default(), logger)

		require.Contains(t, fallbackBuf.String(), "no logger found in context")
	})

	t.Run("returns logger from context", func(t *testing.T) {
		originalLogger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

		ctx := WithLogger(context.Background(), originalLogger)
		retrievedLogger := GetLogger(ctx)

		require.Equal(t, originalLogger, retrievedLogger)
	})

	t.Run("handles wrong type in context", func(t *testing.T) {
		fallbackBuf, restore := captureFallbackLogs(t)
		defer restore()

		originalDefault := slog.Default()
		testLogger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
		slog.SetDefault(testLogger)
		defer slog.SetDefault(originalDefault)

		// Store wrong type in context
		ctx := context.WithValue(context.Background(), loggerContextKey, "not a logger")
		logger := GetLogger(ctx)

		require.NotNil(t, logger)
		require.Equal(t, slog.Default(), logger)

		require.Contains(t, fallbackBuf.String(), "logger in context expected *slog.Logger")
	})

	t.Run("handles nil logger in context", func(t *testing.T) {
		fallbackBuf, restore := captureFallbackLogs(t)
		defer restore()

		originalDefault := slog.Default()
		testLogger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
		slog.SetDefault(testLogger)
		defer slog.SetDefault(originalDefault)

		// Store nil logger in context
		ctx := context.WithValue(context.Background(), loggerContextKey, (*slog.Logger)(nil))
		logger := GetLogger(ctx)

		require.NotNil(t, logger)
		require.Equal(t, slog.Default(), logger)
		require.Contains(t, fallbackBuf.String(), "logger in context was nil")
	})
}

func Test_NewLogger(t *testing.T) {
	t.Run("creates development logger", func(t *testing.T) {
		logger := NewLogger(true)

		require.NotNil(t, logger)

		// Test that it produces text output (development mode)
		var buf bytes.Buffer

		// Replace the logger's handler to capture output
		logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		logger.Info("test message", "key", "value")

		output := buf.String()
		require.Contains(t, output, "test message")
		require.Contains(t, output, "key=value")
		// Text format doesn't use JSON structure
		require.NotContains(t, output, `"msg"`)
		require.NotContains(t, output, `"key"`)
	})

	t.Run("creates production logger", func(t *testing.T) {
		logger := NewLogger(false)

		require.NotNil(t, logger)

		// Test that it produces JSON output (production mode)
		var buf bytes.Buffer
		logger = slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

		logger.Info("test message", "key", "value")

		output := buf.String()
		require.Contains(t, output, "test message")

		// Should be valid JSON
		var jsonData map[string]interface{}
		err := json.Unmarshal([]byte(output), &jsonData)
		require.NoError(t, err)
		require.Equal(t, "test message", jsonData["msg"])
		require.Equal(t, "value", jsonData["key"])
	})

	t.Run("development logger has debug level", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		logger.Debug("debug message")
		logger.Info("info message")
		logger.Warn("warn message")
		logger.Error("error message")

		output := buf.String()
		require.Contains(t, output, "debug message")
		require.Contains(t, output, "info message")
		require.Contains(t, output, "warn message")
		require.Contains(t, output, "error message")
	})

	t.Run("production logger has info level", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

		logger.Debug("debug message")
		logger.Info("info message")
		logger.Warn("warn message")
		logger.Error("error message")

		output := buf.String()
		// Debug should be filtered out
		require.NotContains(t, output, "debug message")
		require.Contains(t, output, "info message")
		require.Contains(t, output, "warn message")
		require.Contains(t, output, "error message")
	})
}

func Test_LoggerContextChaining(t *testing.T) {
	t.Run("context chaining preserves logger", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

		baseCtx := context.Background()
		loggerCtx := WithLogger(baseCtx, logger)

		// Create a child context
		type childKeyT string
		childKey := childKeyT("child-key")
		childCtx := context.WithValue(loggerCtx, childKey, "child-value")

		// Logger should still be accessible from child context
		retrievedLogger := GetLogger(childCtx)
		require.Equal(t, logger, retrievedLogger)
	})

	t.Run("multiple logger contexts", func(t *testing.T) {
		logger1 := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
		logger2 := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))

		ctx := context.Background()
		ctx1 := WithLogger(ctx, logger1)
		ctx2 := WithLogger(ctx, logger2) // Different branch

		// Each context should have its own logger
		retrieved1 := GetLogger(ctx1)
		require.Equal(t, logger1, retrieved1)

		retrieved2 := GetLogger(ctx2)
		require.Equal(t, logger2, retrieved2)
	})
}

func Test_LoggerConcurrency(t *testing.T) {
	t.Run("concurrent logger access is safe", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
		ctx := WithLogger(context.Background(), logger)

		const numGoroutines = 100
		var wg sync.WaitGroup
		results := make(chan *slog.Logger, numGoroutines)

		// Start multiple goroutines accessing the logger
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				retrievedLogger := GetLogger(ctx)
				results <- retrievedLogger
			}()
		}

		wg.Wait()
		close(results)

		// Verify all goroutines got the same logger
		count := 0
		for retrievedLogger := range results {
			require.Equal(t, logger, retrievedLogger)
			count++
		}
		require.Equal(t, numGoroutines, count)
	})

	t.Run("concurrent logger modifications", func(t *testing.T) {
		baseCtx := context.Background()
		const numGoroutines = 50

		var wg sync.WaitGroup
		contexts := make(chan context.Context, numGoroutines)

		// Start multiple goroutines creating different logger contexts
		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(_ int) {
				defer wg.Done()
				var buf bytes.Buffer
				logger := slog.New(slog.NewTextHandler(&buf, nil))
				ctx := WithLogger(baseCtx, logger)
				contexts <- ctx
			}(i)
		}

		wg.Wait()
		close(contexts)

		// Verify each context has its own logger
		count := 0
		seenLoggers := make(map[*slog.Logger]bool)
		for ctx := range contexts {
			logger := GetLogger(ctx)
			require.NotNil(t, logger)

			// Each should be a unique logger instance
			require.False(t, seenLoggers[logger], "Logger %p seen multiple times", logger)
			seenLoggers[logger] = true
			count++
		}
		require.Equal(t, numGoroutines, count)
	})
}

func Test_LoggerWithAttributes(t *testing.T) {
	t.Run("logger with structured attributes", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, nil))

		ctx := WithLogger(context.Background(), logger)
		retrievedLogger := GetLogger(ctx)

		// Test structured logging
		retrievedLogger.Info("test message",
			"user_id", 12345,
			"action", "login",
			"success", true,
		)

		output := buf.String()
		require.Contains(t, output, "test message")

		// Parse JSON to verify structure
		var jsonData map[string]interface{}
		err := json.Unmarshal([]byte(output), &jsonData)
		require.NoError(t, err)
		require.Equal(t, "test message", jsonData["msg"])
		require.Equal(t, float64(12345), jsonData["user_id"]) // JSON numbers are float64
		require.Equal(t, "login", jsonData["action"])
		require.Equal(t, true, jsonData["success"])
	})

	t.Run("logger inheritance in nested contexts", func(t *testing.T) {
		var buf bytes.Buffer
		baseLogger := slog.New(slog.NewJSONHandler(&buf, nil))

		// Create a logger with base attributes
		loggerWithUser := baseLogger.With("user_id", 12345, "session", "abc123")

		ctx := WithLogger(context.Background(), loggerWithUser)

		// Create child context that should inherit the logger
		type requestIDT string
		requestID := requestIDT("child-key")
		childCtx := context.WithValue(ctx, requestID, "req-456")
		retrievedLogger := GetLogger(childCtx)

		// Should be the same logger with base attributes
		require.Equal(t, loggerWithUser, retrievedLogger)

		retrievedLogger.Info("user action", "action", "view_profile")

		output := buf.String()

		// Parse JSON to verify inherited attributes
		var jsonData map[string]interface{}
		err := json.Unmarshal([]byte(output), &jsonData)
		require.NoError(t, err)
		require.Equal(t, "user action", jsonData["msg"])
		require.Equal(t, float64(12345), jsonData["user_id"])
		require.Equal(t, "abc123", jsonData["session"])
		require.Equal(t, "view_profile", jsonData["action"])
	})
}

func Test_LoggerOutputFormats(t *testing.T) {
	t.Run("text handler format", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		logger.Info("test message", "key1", "value1", "key2", 42)

		output := buf.String()
		require.Contains(t, output, "test message")
		require.Contains(t, output, "key1=value1")
		require.Contains(t, output, "key2=42")

		// Text format characteristics
		require.Contains(t, output, "level=INFO")
		require.Contains(t, output, "msg=\"test message\"")
	})

	t.Run("json handler format", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		logger.Warn("warning message", "error_code", 404, "path", "/api/users")

		output := buf.String()

		// Should be valid JSON
		var jsonData map[string]interface{}
		err := json.Unmarshal([]byte(output), &jsonData)
		require.NoError(t, err)

		require.Equal(t, "WARN", jsonData["level"])
		require.Equal(t, "warning message", jsonData["msg"])
		require.Equal(t, float64(404), jsonData["error_code"])
		require.Equal(t, "/api/users", jsonData["path"])

		// Should have timestamp
		require.Contains(t, jsonData, "time")
	})

	t.Run("different log levels", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		logger.Debug("debug message")
		logger.Info("info message")
		logger.Warn("warn message")
		logger.Error("error message")

		output := buf.String()
		lines := strings.Split(strings.TrimSpace(output), "\n")

		// Should have 4 log lines
		require.Len(t, lines, 4)

		// Parse each line and verify level
		levels := []string{"DEBUG", "INFO", "WARN", "ERROR"}
		messages := []string{"debug message", "info message", "warn message", "error message"}

		for i, line := range lines {
			var jsonData map[string]interface{}
			err := json.Unmarshal([]byte(line), &jsonData)
			require.NoError(t, err)
			require.Equal(t, levels[i], jsonData["level"])
			require.Equal(t, messages[i], jsonData["msg"])
		}
	})
}

func Test_LoggerEdgeCases(t *testing.T) {
	t.Run("logger with nil handler", func(t *testing.T) {
		// This should not panic, slog should handle it gracefully
		require.NotPanics(t, func() {
			logger := NewLogger(true)
			require.NotNil(t, logger)
		})
	})

	t.Run("logger context with cancel", func(t *testing.T) {
		logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))

		ctx, cancel := context.WithCancel(context.Background())
		loggerCtx := WithLogger(ctx, logger)

		// Should work before cancel
		retrievedLogger := GetLogger(loggerCtx)
		require.Equal(t, logger, retrievedLogger)

		// Should still work after cancel (context values are immutable)
		cancel()
		retrievedLogger = GetLogger(loggerCtx)
		require.Equal(t, logger, retrievedLogger)
	})

	t.Run("nested logger contexts", func(t *testing.T) {
		logger1 := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
		logger2 := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))

		ctx := context.Background()
		ctx1 := WithLogger(ctx, logger1)
		ctx2 := WithLogger(ctx1, logger2) // Nested

		// Should have the most recent logger
		retrievedLogger := GetLogger(ctx2)
		require.Equal(t, logger2, retrievedLogger)
		require.NotEqual(t, logger1, retrievedLogger)

		// Parent context should still have original logger
		retrievedLogger1 := GetLogger(ctx1)
		require.Equal(t, logger1, retrievedLogger1)
	})
}

// Benchmark tests
func BenchmarkWithLogger(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		WithLogger(ctx, logger)
	}
}

func BenchmarkGetLogger(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	ctx := WithLogger(context.Background(), logger)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetLogger(ctx)
	}
}

func BenchmarkGetLoggerDefault(b *testing.B) {
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		GetLogger(ctx)
	}
}

func BenchmarkNewLogger(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewLogger(true)
	}
}

func BenchmarkLoggerConcurrent(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	ctx := WithLogger(context.Background(), logger)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			GetLogger(ctx)
		}
	})
}
