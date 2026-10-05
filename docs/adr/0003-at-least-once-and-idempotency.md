# ADR-0003: At-least-once delivery with idempotent consumers

## Status

Accepted.

## Context

Exactly-once delivery across a broker and independent databases is not
achievable without distributed transactions. The outbox and the broker both
provide at-least-once semantics in practice.

## Decision

Assume at-least-once delivery and make every consumer idempotent:

- Insert a `(consumer, event_id)` marker in `processed_messages` inside the
  handler's transaction. A conflict skips the handler.
- Encapsulate state changes in guarded, idempotent domain transitions.
- Add unique constraints on natural keys (`reservations.order_id`,
  `payments.order_id`, `fulfillments.order_id`, `notifications (order_id, topic)`)
  as a second line of defence.

## Consequences

- Duplicates and replays are safe and are observable
  (`orbit_events_processed_total{result="duplicate"}`).
- Exactly-once is unnecessary; a duplicate event results in zero additional
  effects.
- Each consumer pays one extra indexed insert per event. This is an acceptable
  cost for correctness.
- Handlers must be transactional and side effects that cannot be made idempotent
  should be modelled as state transitions, not external calls.

## Alternatives considered

- **Deduplicate at the broker.** The broker does not know about consumer-side
  state and cannot deduplicate against committed domain effects.
- **Require every downstream effect to be idempotent without markers.** Not
  always possible and easy to get subtly wrong; markers make the guarantee
  explicit and testable.
