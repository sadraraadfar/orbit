# ADR-0001: Choreography over orchestration

## Status

Accepted.

## Context

The order workflow spans five services. It must be coordinated across services
without a distributed transaction. The two established options are
orchestration (a central component drives each step) and choreography (each
service reacts to events and emits the next).

## Decision

Use choreography. Each service subscribes to the event families it cares about
and decides its own next action. The Order service participates by maintaining
the order aggregate and reflecting workflow state; it is not a controller.

## Consequences

- Services stay decoupled and independently deployable; adding a participant
  requires no change to a central coordinator.
- The global flow is implicit. Mitigated by a small, documented event contract
  surface, guarded transitions, and the order timeline read model.
- Losing a service does not stall the whole workflow immediately; unconsumed
  events accumulate and are processed on recovery.
- Debugging requires correlation IDs and tracing, which are provided end to end.

## Alternatives considered

- **Orchestrator state machine.** Easier to reason about for complex branching,
  but introduces a coordination hotspot and duplicates routing already present
  in the event log. Revisit if the workflow grows many conditional branches.
