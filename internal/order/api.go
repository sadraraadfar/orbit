package order

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/example/orbit/internal/platform/apierror"
	"github.com/example/orbit/internal/platform/correlation"
	"github.com/example/orbit/internal/platform/httpx"
)

// API exposes the order HTTP endpoints.
type API struct {
	svc *Service
	log *slog.Logger
}

// NewAPI constructs the order API.
func NewAPI(svc *Service, log *slog.Logger) *API {
	return &API{svc: svc, log: log}
}

// Register mounts the order routes on mux.
func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/orders", a.createOrder)
	mux.HandleFunc("GET /v1/orders", a.listOrders)
	mux.HandleFunc("GET /v1/orders/{id}", a.getOrder)
	mux.HandleFunc("GET /v1/orders/{id}/timeline", a.timeline)
}

type createOrderResponse struct {
	Order             *Order `json:"order"`
	CorrelationID     string `json:"correlation_id"`
	IdempotencyReplay bool   `json:"idempotency_replayed"`
}

func (a *API) createOrder(w http.ResponseWriter, r *http.Request) {
	var req CreateOrderRequest
	if err := httpx.DecodeJSON(w, r, &req); err != nil {
		httpx.WriteError(w, r, err)
		return
	}

	idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(idempotencyKey) > 255 {
		httpx.WriteError(w, r, apierror.Invalid("Idempotency-Key must be at most 255 characters"))
		return
	}

	o, replayed, err := a.svc.CreateOrder(r.Context(), req, idempotencyKey)
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}

	w.Header().Set("Location", "/v1/orders/"+o.ID)
	if replayed {
		w.Header().Set("Idempotency-Replayed", "true")
	}
	httpx.WriteJSON(w, http.StatusCreated, createOrderResponse{
		Order:             o,
		CorrelationID:     correlation.From(r.Context()),
		IdempotencyReplay: replayed,
	})
}

func (a *API) getOrder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		httpx.WriteError(w, r, apierror.Invalid("order id must be a UUID"))
		return
	}
	o, err := a.svc.GetOrder(r.Context(), id)
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, o)
}

type listOrdersResponse struct {
	Orders     []*Order `json:"orders"`
	NextCursor string   `json:"next_cursor,omitempty"`
}

func (a *API) listOrders(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	limit := 20
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100 {
			httpx.WriteError(w, r, apierror.Invalid("limit must be an integer between 1 and 100"))
			return
		}
		limit = n
	}

	sort := strings.ToLower(q.Get("sort"))
	ascending := sort == "asc"
	if sort != "" && sort != "asc" && sort != "desc" {
		httpx.WriteError(w, r, apierror.Invalid("sort must be 'asc' or 'desc'"))
		return
	}

	orders, next, err := a.svc.ListOrders(r.Context(), ListFilter{
		Status:     q.Get("status"),
		CustomerID: q.Get("customer_id"),
		Limit:      limit,
		Cursor:     q.Get("cursor"),
		Ascending:  ascending,
	})
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	if orders == nil {
		orders = []*Order{}
	}
	httpx.WriteJSON(w, http.StatusOK, listOrdersResponse{Orders: orders, NextCursor: next})
}

type timelineResponse struct {
	OrderID string          `json:"order_id"`
	Entries []TimelineEntry `json:"entries"`
}

func (a *API) timeline(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		httpx.WriteError(w, r, apierror.Invalid("order id must be a UUID"))
		return
	}
	entries, err := a.svc.Timeline(r.Context(), id)
	if err != nil {
		httpx.WriteError(w, r, err)
		return
	}
	if entries == nil {
		entries = []TimelineEntry{}
	}
	httpx.WriteJSON(w, http.StatusOK, timelineResponse{OrderID: id, Entries: entries})
}
