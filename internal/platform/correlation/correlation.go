// Package correlation carries the request/correlation identifier through
// context. The same identifier is attached to HTTP responses, log records,
// message headers, and event envelopes so a single workflow can be traced end
// to end.
package correlation

import (
	"context"

	"github.com/google/uuid"
)

type contextKey struct{}

var key contextKey

// With stores the correlation identifier in the context.
func With(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, key, id)
}

// From returns the correlation identifier stored in the context, or an empty
// string when none is present.
func From(ctx context.Context) string {
	if v, ok := ctx.Value(key).(string); ok {
		return v
	}
	return ""
}

// FromOrNew returns the correlation identifier in the context, generating and
// storing a new one when absent.
func FromOrNew(ctx context.Context) (context.Context, string) {
	if id := From(ctx); id != "" {
		return ctx, id
	}
	id := NewID()
	return With(ctx, id), id
}

// NewID generates a new correlation identifier.
func NewID() string { return uuid.NewString() }
