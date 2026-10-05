// Package httpx contains the small HTTP helpers shared by Orbit services:
// JSON encoding/decoding, canonical error rendering, request middleware, and
// health endpoints.
package httpx

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/example/orbit/internal/platform/apierror"
	"github.com/example/orbit/internal/platform/correlation"
)

const maxBodyBytes = 1 << 20 // 1 MiB

// ErrorBody is the canonical error response shape.
type ErrorBody struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail carries the machine-readable code, a human message, the request
// identifier, and optional structured details.
type ErrorDetail struct {
	Code      apierror.Code  `json:"code"`
	Message   string         `json:"message"`
	RequestID string         `json:"request_id,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
}

// WriteJSON writes v as JSON with the supplied status code.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError renders err using the canonical error shape. The correlation ID is
// taken from the request context so every error can be tied to its request.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	apiErr := apierror.From(err)
	WriteJSON(w, apiErr.Status, ErrorBody{Error: ErrorDetail{
		Code:      apiErr.Code,
		Message:   apiErr.Message,
		RequestID: correlation.From(r.Context()),
		Details:   apiErr.Details,
	}})
}

// DecodeJSON decodes a JSON request body into dst, rejecting unknown fields and
// oversized payloads. It returns a client-facing *apierror.Error on failure.
func DecodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		switch {
		case errors.As(err, &maxErr):
			return apierror.Invalid("request body exceeds 1 MiB limit")
		case errors.Is(err, io.EOF):
			return apierror.Invalid("request body must not be empty")
		default:
			return apierror.Invalid("malformed JSON body: " + err.Error())
		}
	}
	// Reject trailing data after the first JSON value.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return apierror.Invalid("request body must contain a single JSON object")
	}
	return nil
}

// ClientIP extracts the peer address, honouring X-Forwarded-For when present.
func ClientIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		return strings.TrimSpace(strings.Split(fwd, ",")[0])
	}
	return r.RemoteAddr
}
