# Security considerations and threat model

This document states the assets Orbit protects, the trust boundaries it assumes,
and the threats it mitigates in code versus those a deployment must mitigate
around it.

## Assets

- **Order data** — customer ID, items, and totals.
- **Payment state** — authorization, failure, and refund records.
- **Inventory integrity** — stock levels and reservations.
- **Workflow integrity** — the sequence of events and terminal state of an order.

## Trust boundaries

1. **Client → Order API.** Untrusted input crosses here.
2. **Service → Broker.** Each service may publish and consume events.
3. **Service → its database.** Each service may access only its own schema.

## Threats and mitigations

| # | Threat | Mitigation in this repository | Deployment control |
| --- | --- | --- | --- |
| T1 | Unauthenticated access to the API | None — authentication is out of scope | Put the API behind an authenticating gateway / mTLS |
| T2 | Idempotency key replay or collision | Key scoped to a request hash; a different body returns 409; stored response is replayed | Per-account key namespaces, key expiry |
| T3 | Forged or injected broker messages | Envelope schema and registry validation; unknown types dead-lettered | Broker authentication and topic ACLs |
| T4 | Fault injection abused in production | Disabled by default; API rejects `faults` unless enabled | Keep `ORBIT_ENABLE_FAULT_INJECTION=false` outside development |
| T5 | Oversized or malformed requests | 1 MiB body limit; unknown JSON fields rejected; UUID path validation | Rate limiting at the edge |
| T6 | SQL injection | All queries are parameterized | Least-privilege database roles |
| T7 | Secret leakage | Only `.env.example` is committed; runtime config is environment-driven | Secret manager / sealed secrets |
| T8 | Sensitive data in logs | Logs carry identifiers and event metadata, not payloads | Log redaction policy, retention limits |
| T9 | Malicious replayed dead letters | Replay is an operator action over an authenticated broker | Restrict `orbitctl` access |

## Input validation at boundaries

- HTTP request bodies are strictly decoded with unknown fields rejected and a
  1 MiB cap (`internal/platform/httpx`).
- Order fields are validated in the service before any write: non-empty
  customer, at least one item, positive quantities, non-negative prices, a
  three-letter currency, and a known fault name.
- Every incoming event is decoded through the registry, which enforces the
  known type, the version, and payload validation rules before a handler runs.
- Path parameters are validated as UUIDs before use.

## Data isolation

- One database per service; no cross-service queries or foreign keys.
- Consumers write only to their own database.
- The event stream is the only integration surface, and it carries only the
  data a downstream service needs.

## Failure handling as a security property

- Poison messages are dead-lettered rather than causing retries that could
  exhaust resources.
- Bounded retries with backoff prevent a failing dependency from being hammered.
- Idempotent consumers prevent duplicate side effects from replays.

## Out of scope

Authentication, authorization, TLS termination, and multi-tenancy are not
implemented. A production deployment is expected to provide them at the edge
(API gateway, service mesh) and to configure broker access control. The code is
structured so these can be added without changing the domain logic.
