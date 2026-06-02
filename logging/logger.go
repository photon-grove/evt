package logging

import (
	"context"
	"fmt"
	"log/slog"
	"os"
)

type contextKey int

const loggerContextKey contextKey = iota

// WithLogger returns a child context with the provided logger for structured logging.
func WithLogger(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, loggerContextKey, logger)
}

// GetLogger returns the logger from the context or the default logger if missing.
func GetLogger(ctx context.Context) *slog.Logger {
	value := ctx.Value(loggerContextKey)
	if value == nil {
		logFallback("no logger found in context - falling back to the default logger")

		return slog.Default()
	}

	logger, ok := value.(*slog.Logger)
	if !ok {
		logFallback("logger in context expected *slog.Logger, found %T - falling back to the default logger", value)

		return slog.Default()
	}

	if logger == nil {
		logFallback("logger in context was nil - falling back to the default logger")

		return slog.Default()
	}

	return logger
}

// NewLogger creates a logger for development (text) or production (JSON).
func NewLogger(isDevelopment bool) *slog.Logger {
	var handler slog.Handler
	if isDevelopment {
		handler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
	} else {
		handler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	}

	logger := slog.New(handler)

	return logger
}

var logFallback = func(format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	fmt.Fprintln(os.Stderr, message)
}
