# ADR-0005: Kafka-compatible broker with Redpanda for local development

## Status

Accepted.

## Context

The system needs a durable, partitioned, replayable log with consumer groups.
It must run locally with a single command.

## Decision

Target the Kafka protocol and use Redpanda for local development. Redpanda is a
single binary with no ZooKeeper/KRaft controller to operate, which keeps the
local stack small while presenting a Kafka-compatible API. The client is
`segmentio/kafka-go`.

## Consequences

- The same code runs against any Kafka-compatible broker.
- Topic creation is best-effort and tolerant of pre-existing topics; Redpanda's
  auto-create is enabled locally and `orbitctl`/startup also ensure topics.
- `kafka-go` does not use a transactional/idempotent producer; this is
  consistent with the at-least-once decision in ADR-0003 and avoids pinning the
  project to a single broker implementation.
- Consumer offsets are committed manually, after processing or dead-lettering,
  so processing and offset advancement are explicit.

## Alternatives considered

- **Apache Kafka with KRaft.** Closer to a typical production deployment but a
  heavier local stack.
- **A cloud-managed broker.** Out of scope for a locally runnable system.
