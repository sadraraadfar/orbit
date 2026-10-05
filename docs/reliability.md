# Reliability

This document describes the mechanisms that make Orbit's delivery and
processing reliable, and the limits of those guarantees.

## The transactional outbox

Writing domain state and publishing an event cannot be atomic across a database
and a broker. Orbit uses the outbox pattern:

1. In one database transaction, the handler writes the domain change and an
   `outbox` row containing the fully encoded envelope.
2. A publisher worker claims due rows, publishes them, and marks them
   published.

The `outbox` table:

| Column | Meaning |
| --- | --- |
| `seq` | Monotonic insertion order; the publication order. |
| `payload` | The complete encoded envelope. |
| `attempts` | Number of publish attempts. |
| `available_at` | Earliest time the row may be retried (lease/backoff). |
| `published_at` | Set once the broker acknowledged the record. |
| `last_error` | Most recent publish error, for diagnosis. |

### Claiming work safely

The publisher claims rows with:

```sql
SELECT id FROM outbox
WHERE published_at IS NULL AND available_at <= now()
ORDER BY seq
FOR UPDATE SKIP LOCKED
LIMIT $1
```

and immediately extends `available_at` into the future (a lease). The row lock
is held only for the claim, so multiple publishers can run concurrently without
blocking each other.

### Why exactly-once is not assumed

The publisher receives a broker acknowledgement and then, in a separate
operation, marks the row published. If the process crashes after the
acknowledgement but before the mark, the row is published again on the next
poll. Because the two operations cannot be made atomic, Orbit guarantees
**at-least-once**, not exactly-once. The alternative — a transactional producer
or a distributed commit — adds substantial complexity and operator burden for
little practical benefit once consumers are idempotent.

The outbox also never drops events: a publish failure reschedules the row with
exponential backoff (capped at one minute) and records `last_error`. Failures
are surfaced by `orbit_outbox_publish_failures_total` and
`orbit_outbox_pending_records`.

## Idempotent consumers

Every consumer is safe under redelivery:

- **Processed-message marker.** Before invoking the handler, the consumer inserts
  `(consumer, event_id)` into `processed_messages` inside the handler's
  transaction. A primary-key conflict means the event was already processed; the
  handler is skipped and the message is acknowledged.
- **Atomic marker and state.** The marker and the state change commit together.
  If the handler fails, the transaction rolls back, so the marker is not
  persisted and the event can be retried.
- **Idempotent domain transitions.** Where possible, state transitions are
  naturally idempotent and backed by unique constraints (`reservations.order_id`,
  `payments.order_id`, `fulfillments.order_id`, `notifications (order_id, topic)`).
  This protects against effects that occur outside the marker's transaction and
  makes the domain robust even if the marker table is reset.

## Retries and dead-lettering

Handler errors are classified:

| Class | Examples | Action |
| --- | --- | --- |
| `transient` | database unavailable, lock contention, dependency timeout | retry with exponential backoff up to `ORBIT_CONSUMER_MAX_RETRIES`, then dead-letter |
| `permanent` | invalid state, unknown reference, provider rejection | dead-letter immediately |
| `invalid` | malformed or unknown message | dead-letter immediately |

An unclassified error defaults to `transient`, so an unexpected failure is
retried and eventually preserved rather than silently dropped.

## Dead-letter topics

Each stream has a paired dead-letter topic `<topic>.dlq`. A dead-lettered record
preserves the original bytes and adds headers:

| Header | Meaning |
| --- | --- |
| `x-orbit-original-topic` | Topic to replay to. |
| `x-orbit-original-key` | Original partition key. |
| `x-orbit-failure-class` | `transient`, `permanent`, or `invalid`. |
| `x-orbit-failure-reason` | Error message. |
| `x-orbit-consumer` | Consumer that dead-lettered the record. |

Because the payload is byte-for-byte identical, replay is exact. See
[`failure-recovery.md`](failure-recovery.md) for the runbook.

## Poison messages

A message that cannot be decoded (unknown type, version mismatch, invalid JSON,
failed validation) is classified `invalid`, dead-lettered, and acknowledged. It
never blocks the partition and it never crashes a consumer.

## Ordering

- Events are keyed by aggregate ID, so all events for one order share a
  partition and are delivered in order.
- The outbox publishes in `seq` order.
- Retries happen in-process and block the partition for the bounded retry
  window. This trades a short stall for simple, correct ordering. Dead-lettering
  unblocks the partition.

## Consumer lag

Each runner publishes `orbit_consumer_lag` per topic and group. Lag growing
means processing cannot keep up; the metric is the primary scaling signal along
with `orbit_events_failed_total`. See [`observability.md`](observability.md).

## What is guaranteed

- No committed domain write is ever lost from the event stream (outbox).
- No event is processed twice (idempotent consumers).
- Poison messages do not stop progress (dead-lettering).
- A workflow reaches a terminal state, or a message sits in a dead-letter topic
  for an operator to inspect and replay.
