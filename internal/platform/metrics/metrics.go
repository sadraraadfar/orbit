// Package metrics defines the Prometheus instrumentation exposed by Orbit
// services and the handler that serves the /metrics endpoint.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics bundles every collector owned by a service. Each service creates one
// instance with its own registry so the /metrics output is scoped to that
// process.
type Metrics struct {
	Registry *prometheus.Registry

	EventsProcessed   *prometheus.CounterVec
	EventsFailed      *prometheus.CounterVec
	EventRetries      *prometheus.CounterVec
	DLQPublished      *prometheus.CounterVec
	OutboxPublished   prometheus.Counter
	OutboxFailures    prometheus.Counter
	OutboxPending     prometheus.Gauge
	WorkflowDuration  *prometheus.HistogramVec
	HTTPRequestMillis *prometheus.HistogramVec
	ConsumerLag       *prometheus.GaugeVec
}

// New constructs and registers the standard collectors for a service.
func New(service string) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewGoCollector())
	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	m := &Metrics{
		Registry: reg,
		EventsProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "orbit",
			Name:        "events_processed_total",
			Help:        "Number of events processed successfully (including idempotent skips).",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"event_type", "result"}),
		EventsFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "orbit",
			Name:        "events_failed_total",
			Help:        "Number of event handling failures by class.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"event_type", "class"}),
		EventRetries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "orbit",
			Name:        "event_retries_total",
			Help:        "Number of in-process retries for event handling.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"event_type"}),
		DLQPublished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "orbit",
			Name:        "dlq_published_total",
			Help:        "Number of events routed to a dead-letter topic.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"event_type"}),
		OutboxPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "orbit",
			Name:        "outbox_published_total",
			Help:        "Number of outbox records published to the broker.",
			ConstLabels: prometheus.Labels{"service": service},
		}),
		OutboxFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace:   "orbit",
			Name:        "outbox_publish_failures_total",
			Help:        "Number of failed outbox publish attempts.",
			ConstLabels: prometheus.Labels{"service": service},
		}),
		OutboxPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "orbit",
			Name:        "outbox_pending_records",
			Help:        "Number of outbox records awaiting publication.",
			ConstLabels: prometheus.Labels{"service": service},
		}),
		WorkflowDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   "orbit",
			Name:        "workflow_duration_seconds",
			Help:        "Time from order creation to terminal workflow event.",
			ConstLabels: prometheus.Labels{"service": service},
			Buckets:     prometheus.DefBuckets,
		}, []string{"outcome"}),
		HTTPRequestMillis: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:   "orbit",
			Name:        "http_request_duration_seconds",
			Help:        "HTTP request duration.",
			ConstLabels: prometheus.Labels{"service": service},
			Buckets:     prometheus.DefBuckets,
		}, []string{"method", "route", "status"}),
		ConsumerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   "orbit",
			Name:        "consumer_lag",
			Help:        "Observed consumer group lag per topic.",
			ConstLabels: prometheus.Labels{"service": service},
		}, []string{"topic", "group"}),
	}

	reg.MustRegister(
		m.EventsProcessed,
		m.EventsFailed,
		m.EventRetries,
		m.DLQPublished,
		m.OutboxPublished,
		m.OutboxFailures,
		m.OutboxPending,
		m.WorkflowDuration,
		m.HTTPRequestMillis,
		m.ConsumerLag,
	)
	return m
}

// Handler returns the HTTP handler serving the Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
