# ADR-0002: Transactional outbox for reliable publication

## Status

Accepted.

## Context

A domain change and the event that announces it must be consistent. A database
commit and a broker publish cannot be made atomic without distributed
transactions or a transactional producer, both of which add significant
operational complexity.

## Decision

Use the transactional outbox pattern. The domain change and an `outbox` row are
written in one database transaction. A separate publisher worker claims due
rows with `FOR UPDATE SKIP LOCKED`, publishes them with `acks=all`, and marks
them published. Publication order follows a monotonic `seq`.

## Consequences

- No committed domain write can lose its event; if the broker is down, events
  queue in the outbox and are published when it recovers.
- Publication is at-least-once. A crash between the broker acknowledgement and
  the database mark causes a duplicate publication, which consumers must
  tolerate (see ADR-0003).
- The publisher is horizontally scalable through `SKIP LOCKED`.
- The outbox table grows; a production deployment would archive or delete
  published rows older than a retention window. Orbit keeps them for audit and
  simplifies local operation.

## Alternatives considered

- **Publish inside the transaction.** Unsafe: the publish is not rolled back if
  the commit fails.
- **Change data capture (CDC).** Viable and common, but adds a connector and
  operational surface. The outbox keeps the mechanism inside the service.
- **Two-phase commit.** Rejected as too complex for the benefit.
