package httpx

import (
	"context"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Check is a readiness probe for a dependency.
type Check func(ctx context.Context) error

// Health aggregates dependency checks and exposes liveness and readiness
// endpoints. Liveness never depends on external systems; readiness fails when
// any registered dependency is unhealthy.
type Health struct {
	mu     sync.RWMutex
	checks map[string]Check
}

// NewHealth returns an empty Health registry.
func NewHealth() *Health {
	return &Health{checks: make(map[string]Check)}
}

// Add registers a named readiness check.
func (h *Health) Add(name string, check Check) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.checks[name] = check
}

// Live reports process liveness. It always returns 200 unless the process is
// shutting down.
func (h *Health) Live(w http.ResponseWriter, _ *http.Request) {
	WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Ready runs every dependency check and reports per-dependency status.
func (h *Health) Ready(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	names := make([]string, 0, len(h.checks))
	for name := range h.checks {
		names = append(names, name)
	}
	checks := make(map[string]Check, len(h.checks))
	for k, v := range h.checks {
		checks[k] = v
	}
	h.mu.RUnlock()
	sort.Strings(names)

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	result := make(map[string]string, len(checks))
	healthy := true
	for _, name := range names {
		if err := checks[name](ctx); err != nil {
			result[name] = "unhealthy: " + err.Error()
			healthy = false
		} else {
			result[name] = "healthy"
		}
	}

	status := http.StatusOK
	if !healthy {
		status = http.StatusServiceUnavailable
	}
	WriteJSON(w, status, map[string]any{
		"status": map[bool]string{true: "ok", false: "degraded"}[healthy],
		"checks": result,
	})
}
