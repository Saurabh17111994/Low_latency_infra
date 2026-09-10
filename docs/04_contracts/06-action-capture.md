# Segment Build Contract — Action Capture

> **RE-SCOPED 2026-08-18 (CHG-028, DEC-041):** this contract describes the capture path as part of
> the integrated Execution Core (**Nautilus** engine + **go-arrow bridge** + custom projection
> glue). Postback intake now runs through the go-arrow bridge's order-updates WebSocket consumer
> (wrapping the vendored `go-arrow` SDK `OrderStream`); Nautilus provides the OMS, position engine,
> reconciliation, fill dedup, and event store; custom projection sinks materialize the Fluss
> tables below. The postback evidence, identity, correlation, quarantine, and ledger requirements
> are unchanged.

## Boundary

The go-arrow bridge consumes Arrow postbacks from the order-updates WebSocket
(`wss://order-updates.arrow.trade`), preserves original payload evidence, and re-publishes
normalized events to the Nautilus adapter, which feeds them into the OMS as external order events.
Nautilus applies its order state machine and position engine; custom projection sinks materialize
immutable audit, order and position state, and quarantine in Fluss. The position projector is the
Nautilus position engine (projection materialized through the sinks — not a separate deployment).

No broker `postback_seq` or overloaded `order_id` is assumed.

## Writes

1. `Fills` immutable postback/fill audit.
2. `Order_Lifecycle` KV keyed by `broker_order_id`.
3. `Positions` KV keyed by `position_id` and linked by `trade_context_id`.
4. `Postback_Quarantine` for unknown schema/status or missing/ambiguous correlation.
5. `Postback_Projection_Ledger` KV keyed by `postback_event_id` (durable projection status — DDL `17_postback_projection_ledger.sql`, live in dev 2026-08-13).

## Ordering and identity

State updates use source event/version/timestamp plus explicit status precedence. Older events
cannot regress terminal state. Conflict moves state to `UNKNOWN`, halts affected order flow, and
alerts.

Correlation accepts verified broker ID mapping, verified echoed client reference, or
evidence-approved reconciliation query. Symbol/quantity/time proximity is never sufficient.

## Position arithmetic and precedence

The position projector SHALL define account/instrument/side uniqueness, position minting,
scale-in, scale-out, re-entry (new `position_id` after closure), correction/bust handling,
quantity underflow, price precision, rounding, and fee treatment. An impossible-fill event
quarantines and halts affected flows. (The Nautilus position engine provides the arithmetic; the
custom layer pins the `UNKNOWN`/quarantine policy and the Fluss projection.)

When broker sequence/version evidence is unavailable, lifecycle projection SHALL use an explicit
tested combination of cumulative quantities, terminal-state precedence, verified broker event time
when available, platform receive time as non-authoritative evidence, and conflict handling.
Synthetic ordering SHALL not be described as broker ordering. Conflicting evidence moves the order
to `UNKNOWN`, quarantines the event, and halts affected order flow.

## Acceptance

Duplicate/out-of-order/no-sequence events, independent-write crash windows, missing mapping,
quarantine, terminal-state regression, projection replay, first/partial/multiple fills, position
creation/closure, rejected/cancelled/unknown status, and approved-policy audit reconstruction tests
pass. Bridge order-updates decode and external-order adoption into the Nautilus OMS require
dedicated adapter tests (canonical IDs in `11-testing-and-release.md` §Action Capture).

## Pinned canonical postback fingerprint form (bridge parity)

The platform postback fingerprint is SHA-256 (lowercase hex) over the canonical identity map
serialized exactly as Go's `encoding/json` serializes `map[string]string`: keys sorted, `"k":"v"`
with no whitespace, and Go default HTML escaping (`<` `>` `&` as `\u003c` `\u003e` `\u0026`;
quote/backslash/control characters escaped the same way). Any independent implementation that
must reproduce a bridge-computed digest has to match this byte for byte — two SHA-256s over
differently serialized inputs never agree.

Exactly seven identity keys participate (`id`, `remarks`, `status`, `report_type`, `fill_shares`,
`average_price`, `exchange_update_time`); extra keys are ignored, values are trimmed, and absent
values serialize as `""`. An all-empty identity is a contract violation: fail closed rather than
collapsing every bad postback onto `sha256("")`.

Known-good vectors (Go-computed, pinned 2026-09-10):

| Input (7-key map) | SHA-256 |
| --- | --- |
| `id=BRK-1, remarks=REF-1, status=COMPLETE, report_type=Fill, fill_shares=2, average_price=15050, exchange_update_time=2026-08-19T10:00:00Z` | `01fc1cd7d1ffd97cc763a067669077e82415d947981a4c9f5beb1ef5069ca8ac` |
| `id=BRK<2>, remarks=A&B "Q" \Z, status="", report_type="", fill_shares="", average_price="", exchange_update_time=""` (pins `<>&` + quote/backslash escaping) | `3934d20619eddb30f04849a1bf1e4ebd953d3a9eda951f8f973814d692623510` |

## Requirement traceability

- Functional: `REQ-AC-001` through `REQ-AC-013`
- Cross-cutting: `03-non-functional.md` §§3.3–3.8; `04-data.md` §§4.2–4.4, 4.6–4.7; `05-interfaces.md` §§5.5, 5.10–5.11; `06-operational.md` §§6.3–6.8, 6.10
- Implementation: `../08_implementation/05-execution-core.md` (integrated Execution Core dossier)

See `../02_requirements/02-functional/06-action-capture.md`.
