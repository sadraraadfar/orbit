# ADR-0004: Versioned event contracts with a registry

## Status

Accepted.

## Context

Services evolve independently, so event schemas must change without breaking
existing consumers. Ad-hoc JSON changes are error-prone and hard to audit.

## Decision

- Every event type carries a major version in its name (`order.created.v1`) and
  in `event_version`; the two must agree.
- A central registry maps event type to owner, version, payload type, and
  validation rules. Unknown types and version mismatches are rejected and
  dead-lettered.
- Within a version, only backward-compatible changes are allowed: adding
  optional fields or relaxing validation. Renames, removals, type changes, and
  newly-required fields require a new version.
- A golden-fixture contract test freezes every registered payload. A breaking
  change fails the test.

## Consequences

- Producers and consumers can be deployed independently within a version.
- Breaking changes are explicit and supervised: `v2` is added alongside `v1`,
  producers migrate, consumers catch up, and `v1` is deprecated but remains
  decodable during the migration window.
- Adding an event requires registering it and adding a fixture, which keeps the
  catalogue honest.

## Alternatives considered

- **Schema registry with enforcing serializers.** Powerful, but adds a runtime
  dependency and operational component. The registry plus contract tests cover
  the same policy with less infrastructure; a schema registry remains a viable
  evolution.
- **Unversioned events.** Rejected: impossible to evolve safely.
