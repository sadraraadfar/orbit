# ADR-0006: One logical database per service

## Status

Accepted.

## Context

The services must not depend on each other's tables. Sharing a schema would
couple deployments and undermine the event-driven boundary.

## Decision

Give each service its own logical database (`orbit_order`, `orbit_inventory`,
`orbit_payment`, `orbit_fulfillment`, `orbit_notification`). Services access
only their own database and integrate exclusively through events. Locally the
databases live on a single PostgreSQL instance for convenience; the boundary is
enforced by configuration, not by physical hosts.

## Consequences

- Schema changes in one service cannot break another; there are no cross-service
  foreign keys or joins.
- Read consistency across services is eventual; the Order timeline provides a
  per-order read model assembled from observed events.
- Operationally, a single local PostgreSQL instance keeps the stack simple, while
  the same code supports separate instances per service in production.
- Integration tests create one database per service and drop them after the run.

## Alternatives considered

- **A shared database with a schema per service.** Easier locally but weakens
  the boundary; a bug or migration can reach across schemas.
- **Separate PostgreSQL instances.** Closer to production isolation but heavier
  for local development; supported by configuration without code changes.
