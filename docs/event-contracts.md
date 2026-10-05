# Event contracts

Every message on the broker is an envelope with a typed, versioned payload. The
registry in `internal/events/registry.go` is the single source of truth: it maps
an event type to its owner, version, payload type, and validation rules. Messages
whose type is unknown, whose version does not match, or whose payload fails
validation are rejected and dead-lettered.

## Envelope

```json
{
  "event_id": "0f2f...",           // UUID, unique per emission
  "event_type": "order.created.v1",
  "event_version": 1,
  "occurred_at": "2026-01-01T00:00:00Z",
  "correlation_id": "c0ff...",      // ties the whole workflow together
  "causation_id": "8a1b...",        // event_id that directly caused this one
  "aggregate_type": "order",
  "aggregate_id": "d4e5...",        // the order ID; also the partition key
  "producer": "order",
  "traceparent": "00-...",          // optional W3C trace context
  "payload": { }
}
```

| Field | Purpose | Required |
| --- | --- | --- |
| `event_id` | Deduplication key for consumers. | yes |
| `event_type` | Selects the contract and handler. | yes |
| `event_version` | Must equal the registered version. | yes |
| `occurred_at` | Domain time; used for timeline ordering. | yes |
| `correlation_id` | Propagated end to end for tracing. | yes |
| `causation_id` | Links a reaction to its trigger. | no |
| `aggregate_type`, `aggregate_id` | Routing and ordering key. | yes |
| `producer` | Owning service, set from the registry. | yes |
| `traceparent` | W3C trace context, populated by the producer. | no |

## Catalogue

| Event | Producer | Key payload | Consumers |
| --- | --- | --- | --- |
| `order.created.v1` | order | order, items, total, currency, optional faults | inventory |
| `inventory.reserved.v1` | inventory | reservation ID, items, currency | payment, order |
| `inventory.rejected.v1` | inventory | reason | order |
| `inventory.released.v1` | inventory | reservation ID, reason | order |
| `payment.authorized.v1` | payment | payment ID, amount, currency | fulfillment, order |
| `payment.failed.v1` | payment | reason, class | inventory, order |
| `payment.refunded.v1` | payment | payment ID, reason | order |
| `fulfillment.started.v1` | fulfillment | fulfillment ID | order |
| `fulfillment.completed.v1` | fulfillment | fulfillment ID | order |
| `fulfillment.failed.v1` | fulfillment | reason | payment, inventory, order |
| `order.completed.v1` | order | total, currency | (terminal) |
| `order.cancelled.v1` | order | reason, stage | (terminal) |
| `notification.scheduled.v1` | order | channel, topic | notification |
| `notification.sent.v1` | notification | notification ID, channel | (terminal) |

Payload definitions live in `internal/events/payloads.go`.

## Versioning policy

The version is encoded twice: in the `event_type` suffix (`...v1`) and in the
`event_version` field. The suffix and the field must agree; the contract test
enforces this.

1. **vN is a compatibility boundary.** Within `v1`, changes must be backward
   compatible.
2. **Allowed within a version:** adding an optional field; relaxing a validation
   rule. Consumers must ignore unknown fields (Go's `encoding/json` does by
   default) and tolerate missing optional fields.
3. **Not allowed within a version:** renaming or removing a field, changing a
   field's type or units, changing a field's meaning, or making an optional field
   required.
4. **Breaking changes ship as a new version.** A new event type such as
   `order.created.v2` is added alongside `v1`; producers migrate, and old
   consumers keep working until they are upgraded. Deprecated specs remain
   decodable and are marked `Deprecated` in the registry.

### Example: adding a field

Adding `loyalty_points` to `order.created.v1`:

```json
{ "order_id": "...", ..., "loyalty_points": 30 }   // v1 still valid
```

Old consumers ignore the field. This is backward compatible and requires no new
version.

### Example: changing a field type

Changing `total_cents` from integer to a string is breaking. It ships as
`order.created.v2`; v1 remains registered and decodable during the migration
window.

## Enforcement

- `internal/events/registry.go` rejects unknown types and version mismatches at
  decode time.
- `internal/events/contract_test.go` freezes a golden JSON document for every
  registered contract. Renaming a field, removing a field, or changing a type
  fails the test, forcing a conscious versioning decision.
- The same test proves that unknown fields are ignored (additive compatibility)
  and that malformed or incomplete payloads are rejected.

To add an event: add the payload type, register it, add a golden fixture, and
subscribe the relevant consumers.
