// Package consumer implements the reliability wrapper around message handling:
// error classification, bounded retries with backoff, idempotent processing,
// dead-letter routing, tracing, and metrics.
package consumer

import (
	"errors"
	"fmt"
)

// Class categorises a handler failure and drives the retry/dead-letter policy.
type Class string

const (
	// ClassTransient covers failures that may succeed on retry: database
	// unavailability, network errors, lock contention, timeouts.
	ClassTransient Class = "transient"
	// ClassPermanent covers deterministic business failures that will never
	// succeed if retried.
	ClassPermanent Class = "permanent"
	// ClassInvalid covers malformed or unknown messages that cannot be decoded.
	ClassInvalid Class = "invalid"
)

// classifiedError attaches a Class to an error.
type classifiedError struct {
	class Class
	err   error
}

func (e *classifiedError) Error() string { return fmt.Sprintf("%s: %v", e.class, e.err) }
func (e *classifiedError) Unwrap() error { return e.err }

// Transient marks err as retryable.
func Transient(err error) error { return &classifiedError{class: ClassTransient, err: err} }

// Permanent marks err as non-retryable.
func Permanent(err error) error { return &classifiedError{class: ClassPermanent, err: err} }

// ErrIgnore signals that the handler deliberately did not act on the event
// because it is not relevant to this consumer. The message is acknowledged and
// the processed marker is recorded so redelivery stays a no-op.
var ErrIgnore = errors.New("event ignored by handler")

// Ignore returns ErrIgnore. Handlers return it for known event types that are
// not part of their responsibility rather than treating them as failures.
func Ignore() error { return ErrIgnore }

// Invalid marks err as a malformed message.
func Invalid(err error) error { return &classifiedError{class: ClassInvalid, err: err} }

// Classify returns the class of err, defaulting to transient so that an
// unrecognised failure is retried and eventually dead-lettered rather than
// silently dropped.
func Classify(err error) Class {
	if err == nil {
		return ""
	}
	var ce *classifiedError
	if errors.As(err, &ce) {
		return ce.class
	}
	return ClassTransient
}
