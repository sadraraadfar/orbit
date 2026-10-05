// Package apierror defines the canonical error type returned by Orbit HTTP
// handlers. Every handler maps failures to an *apierror.Error so that the HTTP
// layer can render a single, consistent JSON error shape.
package apierror

import (
	"errors"
	"fmt"
	"net/http"
)

// Code is a stable, machine-readable error identifier. Clients must branch on
// Code rather than on the human-readable Message.
type Code string

const (
	CodeInvalidArgument     Code = "invalid_argument"
	CodeNotFound            Code = "not_found"
	CodeConflict            Code = "conflict"
	CodeIdempotencyConflict Code = "idempotency_conflict"
	CodeFailedPrecondition  Code = "failed_precondition"
	CodeUnavailable         Code = "unavailable"
	CodeInternal            Code = "internal"
)

// Error is the shared API error representation.
type Error struct {
	Code    Code
	Message string
	Status  int
	Details map[string]any
	cause   error
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.cause)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap exposes the wrapped cause so errors.Is/As keep working.
func (e *Error) Unwrap() error { return e.cause }

// WithCause attaches an underlying error for logging without exposing it to
// clients.
func (e *Error) WithCause(err error) *Error {
	e.cause = err
	return e
}

// WithDetails attaches non-sensitive structured details to the response.
func (e *Error) WithDetails(details map[string]any) *Error {
	e.Details = details
	return e
}

// Invalid reports a client-side validation failure.
func Invalid(message string) *Error {
	return &Error{Code: CodeInvalidArgument, Message: message, Status: http.StatusBadRequest}
}

// NotFound reports a missing resource.
func NotFound(message string) *Error {
	return &Error{Code: CodeNotFound, Message: message, Status: http.StatusNotFound}
}

// Conflict reports a state conflict such as a duplicate resource.
func Conflict(message string) *Error {
	return &Error{Code: CodeConflict, Message: message, Status: http.StatusConflict}
}

// IdempotencyConflict reports reuse of an idempotency key with a different
// request body.
func IdempotencyConflict(message string) *Error {
	return &Error{Code: CodeIdempotencyConflict, Message: message, Status: http.StatusConflict}
}

// FailedPrecondition reports an action that is invalid for the current state.
func FailedPrecondition(message string) *Error {
	return &Error{Code: CodeFailedPrecondition, Message: message, Status: http.StatusUnprocessableEntity}
}

// Unavailable reports a dependency that is temporarily unavailable.
func Unavailable(message string) *Error {
	return &Error{Code: CodeUnavailable, Message: message, Status: http.StatusServiceUnavailable}
}

// Internal reports an unexpected server-side failure.
func Internal(message string) *Error {
	return &Error{Code: CodeInternal, Message: message, Status: http.StatusInternalServerError}
}

// From converts an arbitrary error into an *Error, defaulting to an internal
// error when the cause is not already typed.
func From(err error) *Error {
	if err == nil {
		return nil
	}
	var apiErr *Error
	if errors.As(err, &apiErr) {
		return apiErr
	}
	return Internal("unexpected error").WithCause(err)
}
