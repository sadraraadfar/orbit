package httpx

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/example/orbit/internal/platform/correlation"
)

// Middleware is a standard net/http middleware.
type Middleware func(http.Handler) http.Handler

// Chain applies middlewares so that the first argument is the outermost
// middleware.
func Chain(h http.Handler, mws ...Middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// RequestID ensures every request carries a correlation identifier. The
// identifier is read from X-Correlation-ID or X-Request-ID, generated when
// absent, echoed on the response, and stored in the request context so that
// logs, outbox records, and events can reuse it.
func RequestID() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get("X-Correlation-ID")
			if id == "" {
				id = r.Header.Get("X-Request-ID")
			}
			ctx, id := correlation.FromOrNew(correlation.With(r.Context(), id))
			w.Header().Set("X-Request-ID", id)
			w.Header().Set("X-Correlation-ID", id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// Recovery converts panics into canonical 500 responses so a single bad request
// cannot bring down the process.
func Recovery(log *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.ErrorContext(r.Context(), "panic recovered",
						slog.Any("panic", rec),
						slog.String("path", r.URL.Path),
					)
					WriteJSON(w, http.StatusInternalServerError, ErrorBody{Error: ErrorDetail{
						Code:      "internal",
						Message:   "internal server error",
						RequestID: correlation.From(r.Context()),
					}})
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// AccessLog logs one structured line per request including status, latency, and
// correlation identifier.
func AccessLog(log *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			log.InfoContext(r.Context(), "http request",
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.status),
				slog.Int("bytes", rec.bytes),
				slog.Duration("latency", time.Since(start)),
				slog.String("correlation_id", correlation.From(r.Context())),
			)
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Status returns the response status code, defaulting to 200.
func (r *statusRecorder) Status() int { return r.status }

func (r *statusRecorder) Write(b []byte) (int, error) {
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}
