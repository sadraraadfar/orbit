# Failure recovery

This document maps each expected failure to how it is detected and how the
system recovers, with operator runbooks.

## Failure matrix

| Failure | Detection | Automated recovery | Residual operator action |
| --- | --- | --- | --- |
| Insufficient inventory | `inventory.rejected.v1` in timeline | order cancelled | none |
| Payment rejected | `payment.failed.v1` | inventory released, order cancelled | none |
| Payment gateway timeout | retries then DLQ (`class=transient`) | bounded retries + dead-letter | replay after the dependency recovers |
| Fulfillment failure | `fulfillment.failed.v1` | refund + release, order cancelled | none |
| Duplicate message | `processed_messages` conflict; `result=duplicate` metric | handler skipped | none |
| Consumer restart | lag resets and drains | offset replay from last commit | none |
| Delayed event | out-of-order transition is a no-op | ignored until the expected state arrives | none |
| Poison message | `invalid` in DLQ | dead-lettered, partition unblocked | inspect and discard or fix |
| Broker unavailable | readiness fails, outbox backlog grows | outbox retries with backoff | restore broker; backlog drains |
| Database unavailable | readiness fails, consumer errors | transient retries | restore database |

## Consumer restart

Consumers commit offsets only after successful processing (or dead-lettering).
On restart a consumer resumes from the last committed offset and reprocesses any
messages that were in flight. The processed-message marker makes the
reprocessing a no-op. No manual action is required.

To demonstrate: restart any service container while a workflow is in flight
(`docker compose restart payment-service`) and observe that the order still
reaches its terminal state without duplicate side effects.

## Delayed and out-of-order events

Because events are keyed by order ID and produced with `acks=all`, ordering is
preserved per order. Guarded transitions handle the remaining cases (for example
a manual replay of an older event): the domain ignores a transition that does
not apply to the current state, and the timeline keeps the observed event.

## Duplicate events

At-least-once delivery means duplicates are normal. The marker
`(consumer, event_id)` is inserted in the handler's transaction; a conflict
skips the handler. Storage-level unique constraints are the second line of
defence. No operator action is required.

## Poison messages

A message that cannot be decoded is moved to `<topic>.dlq` with a failure class
of `invalid`. The partition continues. Inspect it:

```sh
docker compose run --rm orbitctl dlq list --topic orbit.inventory.events.dlq
```

If it is genuinely malformed, discard it by consuming past it (it is already
committed). If it is a legitimate event from an incompatible producer, fix the
producer and replay.

## Runbook: dead-letter inspection and replay

1. List the dead letters and read the failure class and reason:

   ```sh
   docker compose run --rm orbitctl dlq list --topic orbit.payment.events.dlq
   ```

2. Determine whether the cause is fixed:
   - `transient` (timeout, dependency down): retry after the dependency is
     healthy.
   - `permanent` (business rejection): do not replay blindly; it will fail again.
   - `invalid` (malformed): fix the producer or drop the record.

3. Replay, which republishes each record to its original topic using the
   `x-orbit-original-topic` header and commits the dead-letter offset:

   ```sh
   docker compose run --rm orbitctl dlq replay --topic orbit.payment.events.dlq
   ```

   Replay uses a consumer group, so re-running only processes new dead letters.
   Replayed events carry a new delivery but the same `event_id`; idempotent
   consumers make a replay safe.

## Runbook: outbox backlog

Symptoms: `orbit_outbox_pending_records` rising, `outbox_publish_failures_total`
increasing, broker logs show connection errors.

1. Check broker health: `docker compose ps redpanda`,
   `rpk cluster health`.
2. Fix the broker. The publisher resumes automatically; `available_at` backoff
   is capped at one minute, so recovery is prompt.
3. Confirm `orbit_outbox_pending_records` trends to zero.
4. Dead-lettered events are unaffected by outbox backlog: it only delays
   publication.

## Runbook: consumer lag

Symptoms: `orbit_consumer_lag` rising; `events_processed_total` flat.

1. Inspect logs for the affected `consumer` and `topic`.
2. If a single record is failing, it will be retried and then dead-lettered;
   check `dlq_published_total`.
3. If the workload exceeds capacity, add partitions and replicas. Per-order
   ordering is retained because partitioning is by aggregate ID.

## Compensation failures

If a compensation cannot complete (for example inventory release finds no
reservation), the consumer classifies the error and dead-letters it. The order
remains in `cancelling` rather than pretending to be cancelled. Resolve the
cause and replay; the guarded transition completes cancellation when the
compensation is observed.

## Guarantees under failure

- Domain writes and their events commit atomically, so an event is never lost
  once the write is committed.
- Redelivery never produces duplicate effects.
- A poison message never blocks a partition.
- A workflow either reaches a terminal state or leaves an inspectable record in
  a dead-letter topic for an operator.
