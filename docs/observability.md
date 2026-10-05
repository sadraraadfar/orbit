# Observability

Orbit emits structured logs, Prometheus metrics, and OpenTelemetry traces, and
exposes a per-order distributed timeline. All four share a correlation ID so a
single workflow can be followed across services.

## Correlation and request IDs

- The HTTP middleware reads `X-Correlation-ID` / `X-Request-ID`, generates one
  when absent, stores it in the request context, and echoes both headers.
- Order creation puts the correlation ID in the envelope's `correlation_id` and
  the order row. Every downstream event copies `correlation_id` and sets
  `causation_id` to the triggering `event_id`.
- Consumers re-attach the correlation ID to their context, so every log line and
  span in the chain carries it.

## Structured logs

Logs are JSON by default (`ORBIT_LOG_FORMAT=json`) and include:

| Field | Meaning |
| --- | --- |
| `service` | Emitting service. |
| `env` | Deployment environment. |
| `correlation_id` | Workflow correlation ID. |
| `event_type`, `event_id` | For consumer logs. |
| `consumer`, `topic`, `offset` | Consumer identity. |
| `error` | Failure detail. |
| `method`, `path`, `status`, `latency`, `bytes` | HTTP access logs. |

Set `ORBIT_LOG_FORMAT=text` for human-readable local development.

## Traces

- HTTP handlers are instrumented with `otelhttp`.
- Consumers extract the W3C trace context from Kafka headers and start a
  `consumer` span; producers inject the current context into headers.
- Setting `ORBIT_OTLP_ENDPOINT` exports spans over OTLP gRPC (Jaeger in the
  observability overlay). Leaving it empty disables export; instrumentation is
  unchanged.

This yields a single trace spanning HTTP create → outbox publish → inventory →
payment → fulfillment → order, with a child span per consumed event.

## Metrics

All metrics are namespaced `orbit_` and exposed at `/metrics`.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `orbit_events_processed_total` | counter | `event_type`, `result` | Processed events (`ok`/`duplicate`). |
| `orbit_events_failed_total` | counter | `event_type`, `class` | Failures by class. |
| `orbit_event_retries_total` | counter | `event_type` | In-process retries. |
| `orbit_dlq_published_total` | counter | `event_type` | Records dead-lettered. |
| `orbit_outbox_published_total` | counter | — | Outbox records published. |
| `orbit_outbox_publish_failures_total` | counter | — | Failed outbox publishes. |
| `orbit_outbox_pending_records` | gauge | — | Unpublished outbox rows. |
| `orbit_workflow_duration_seconds` | histogram | `outcome` | Workflow duration. |
| `orbit_http_request_duration_seconds` | histogram | `method`, `route`, `status` | HTTP latency. |
| `orbit_consumer_lag` | gauge | `topic`, `group` | Observed consumer lag. |

### Consumer lag strategy

Lag is sampled every ten seconds per runner and exported as
`orbit_consumer_lag{topic,group}`. Alerting guidance:

- **Sustained lag > 0 while `events_processed_total` is flat**: a consumer is
  stuck. Check `events_failed_total` and logs for a poison message; if the
  dead-letter topic is filling, investigate that record.
- **Lag growing proportionally to traffic**: add partitions and consumer
  replicas. Because events are keyed by aggregate ID, scaling adds parallelism
  across orders while preserving per-order order.
- **`orbit_outbox_pending_records` rising**: the publisher cannot reach the
  broker, or publish acks time out. Check broker health and
  `outbox_publish_failures_total`.

## Distributed timeline

`GET /v1/orders/{id}/timeline` returns the ordered events the Order service
observed for one order, each with `event_type`, `occurred_at`, `correlation_id`,
`causation_id`, and a summary of the resulting status. This is the fastest way
to inspect a live workflow end to end without a tracing backend.

```sh
curl -sS http://localhost:8080/v1/orders/<order-id>/timeline | jq
```

## Health and readiness

- `GET /healthz` — process liveness; never touches dependencies.
- `GET /readyz` — checks PostgreSQL (`pool.Ping`) and the broker (a TCP dial).

Both are suitable as container probes; the Docker image wires `/healthz` as its
`HEALTHCHECK`.
