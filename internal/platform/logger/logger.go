// Package logger builds the structured logger used across Orbit.
//
// Logs are emitted as JSON by default so they can be shipped to any log
// aggregator without additional parsing configuration. Human-readable output
// remains available for local development.
package logger

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a slog.Logger configured from the supplied level and format.
// "json" produces structured output suitable for production; anything else
// produces text output intended for local development.
func New(level, format string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(level)}

	var handler slog.Handler
	if strings.EqualFold(format, "json") {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(handler)
}

// WithCorrelation returns a logger with correlation and service attributes
// attached. It is used at request and message boundaries.
func WithCorrelation(base *slog.Logger, service, correlationID string) *slog.Logger {
	return base.With(
		slog.String("service", service),
		slog.String("correlation_id", correlationID),
	)
}

func parseLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
