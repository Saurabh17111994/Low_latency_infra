# Segment Build Contract — Executor

> **RE-SCOPED 2026-08-18 (CHG-028, DEC-041):** this contract describes the Executor as part of the
> integrated Execution Core (**Nautilus** engine + **go-arrow bridge** + custom safety glue). The
> former standalone-service wording — Executor calling Arrow's REST API directly — is superseded:
> the go-arrow bridge is the **only** component that can reach Arrow; Nautilus commands the bridge
> over localhost. The no-third-party-OpenAlgo policy (DEC-006) stands unchanged.

## Boundary

The Execution Core is the only platform domain allowed to initiate money-moving calls, and within
it the **go-arrow bridge** is the only component that can physically reach Arrow. Nautilus (the
engine) commands the bridge over localhost HTTP/WebSocket; the bridge performs the Arrow REST
calls (`https://edge.arrow.trade`) and streams order-updates back. There is no intermediate
OpenAlgo layer (DEC-006 — no third-party layer remains policy; the bridge is first-party, pinned,
tested protocol code wrapping the vendored `go-arrow` SDK).

The single-operator (Saurabh, DEC-044) gate, fencing, attempt/correlation mapping, and immutable audit remain custom glue
on Nautilus: Nautilus provides the OMS, position engine, risk engine, reconciliation, fill dedup,
and event store; the custom gate layer enforces the HALTED start, single-operator (Saurabh, DEC-044) resume, and
single-writer boundary.

## Inputs and owned state

Inputs: durable immutable execution-intent rows; durable `Safety_Halt_Requests`; future immutable
`Position_Actions`; lifecycle/position/changelog health for validation. The execution-intent stream
is distinct from the retired `Trade_Decisions` feed.

Owned Fluss state: `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, and immutable
`Execution_Audit`. The Execution Core never mutates strategy fields.

## Gate

Default/restart-uncertain state is `HALTED`. States are `HALTED → RECONCILING → APPROVAL_PENDING →
ENABLED`, with `ENABLED → HALTED` on uncertainty. Every broker-facing command validates current
gate epoch. Halt blocks calls within five seconds. An unresolved `UNKNOWN` escalates to an
operator-review halt after the 15 s reconcile window and pushes the durable `HALT` report as soon as
it fires (M1-2) — the row's 30 s lease expiry is a backstop, not the first notice.

Every accepted intent is re-validated at the send instant (M1-1), under the same lock the bridge
send takes and before any durable attempt is claimed, so a refused request leaves no attempt
behind: a halt between acceptance and send answers 503 (`GATE_HALTED`); once the durable boot row
is adopted, an envelope whose epoch, fence token, or lease is not the current generation answers
409 (`STALE_EPOCH` / `FENCE_MISMATCH`) and halts — it is never sent; an envelope naming another
partition answers 403 (`WRONG_PARTITION`) and is counted, and a foreign sender cannot halt the gate
(no denial of service).

Resume requires broker/order, position/fill, offsets/continuity, Signal checkpoint, and
unknown-attempt reconciliation, followed by a single-operator (Saurabh, DEC-044) authorized
approval of the same evidence hash/epoch.

## Attempt protocol

Persist `PREPARED` attempt, immutable request hash, `client_order_ref` (deterministic, ≤16 chars
for Arrow `remarks` field), and gate epoch before commanding the bridge. The durable identity is
action-scoped (M1-5): `attempt_id` is the primary key, `(instruction_id, action, request_hash)` is
unique, and a different request hash under the same `(instruction_id, action)` is a contract
violation. The action is the bridge command (`place` / `modify` / `cancel`); a place, its later
modify, and its cancel share the `instruction_id` but are distinct money movements, so each is its
own claim stream and never a contract violation. Timeout, disconnect,
malformed response, crash window, or ambiguous response produces `UNKNOWN`, halts, and forbids
automatic retry until broker non-acceptance or verified idempotency is proven.

## Order API (Arrow REST via go-arrow bridge, confirmed)

The bridge exposes place/modify/cancel/query endpoints on localhost that map one-to-one onto the
confirmed Arrow REST contract below; Nautilus never holds broker credentials or Arrow endpoints.

- Place: `POST /order/regular` — `{exchange, symbol, quantity, transactionType: "B"/"S", order: "LMT"/"MKT", product: "I"/"C"/"M", price, validity: "DAY"/"IOC", remarks (max 16 chars), mpp (bool)}`. Response: `{status:"success", data:{orderNo, requestTime}}`
- Modify: `PATCH /order/regular/{id}`
- Cancel: `DELETE /order/regular/{id}`
- Detail: `GET /order/{id}` — full lifecycle history with `orderStatus`, `reportType`, `exchangeOrderID`, `fillShares`, `averagePrice`
- Order book: `GET /user/orders`
- Trade book: `GET /user/trades` — all fills with `fillPrice`, `fillQuantity`, `fillTime`, `fillID`
- Positions: `GET /user/positions`
- Auth: `appID` + `token` headers; token from `/auth/app/authenticate-token`, 24hr TTL, refreshable — handled inside the bridge only
- Rate limit: 10 req/sec per endpoint
- MKT orders disabled by default; use `mpp:true` for upper-limit routing
- Order lifecycle: PENDING → OPEN → COMPLETE (filled) / CANCELLED / REJECTED. TRIGGER_PENDING for stop orders
- Product codes: `I`=MIS (intraday, auto-squared 3:15 PM), `C`=CNC (delivery, T+1), `M`=NRML (F&O)
- Exchanges in scope: NSE, NFO, MCX. INDEX is market data only — the Execution Core must reject INDEX instructions

### Private Go bridge protocol (version 1)

The first-party Go bridge exposes the following private, bearer-authenticated boundary. The
endpoint is not a public API and its readiness does not authorize trading.

| Method | Path | Purpose | Authentication |
| --- | --- | --- | --- |
| `POST` | `/v1/commands` | Place, modify, cancel, query, or reconcile through the bridge | `Authorization: Bearer <bridge-token>` |
| `GET` | `/v1/events` | Receive normalized Arrow order-update reports over WebSocket | `Authorization: Bearer <bridge-token>` |
| `GET` | `/healthz` | Process health | local/private probe |
| `GET` | `/readyz` | Bridge mode/readiness; disabled mode is intentionally not ready | local/private probe |

Every command has `record_type=execution_command`, `contract_version=1`, a unique `request_id`,
and a command name from `place`, `modify`, `cancel`, `query-order`, `reconcile-orders`,
`reconcile-trades`, or `reconcile-positions`. Place and modify require separate
`instruction_id`, `execution_attempt_id`, and `client_order_ref` values. `client_order_ref` is
validated to 1–16 safe ASCII characters before it can become Arrow `remarks`.

Every accepted command returns `record_type=execution_report` and exactly one explicit outcome:

| Outcome | Meaning | Retry behavior |
| --- | --- | --- |
| `SUCCESS` | Verified broker response; place requires nonblank `data.orderNo` | Nautilus may advance its own state after durable recording |
| `REJECTED` | Local validation rejection or documented broker rejection | No place retry without a new approved attempt |
| `UNKNOWN` | Dispatch-time transport failure (timeout, reset/EOF, malformed/ambiguous response, or any status not proven pre-dispatch) or unknown postback state | No automatic retry; reconcile first and halt as required |

A command that provably never reached the bridge's dispatch point is not an outcome: DNS/TCP
connect failure, serialization failure, and the bridge's pre-dispatch rejections (`400`, `401`,
`403`, `404`, `409`, `500 fingerprint_failed`) are classified `NotSent` by the executor and may be
bounded-retried; only an exhausted budget surfaces as an unresolved attempt (H1-2 in the
2026-09-28 remediation plan). Timeout, write/read reset or EOF after the request bytes were sent,
a malformed response, and any other status are `Unknown`: the command may have reached the venue,
so it is never auto-retried, the safety gate halts process-wide, and reconciliation resolves it.

Repeated `request_id` with identical content returns the cached report without a second broker call.
Reusing a `request_id` with different content returns `request_id_reuse_violation` and never reaches
Arrow. The cache is process-local; restart recovery remains the responsibility of the durable
Execution Attempt and reconciliation protocol.

#### Postback vocabulary (executor dispatch key)

Arrow postbacks arrive on `/v1/events` with `command=postback`. The bridge normalizes the raw Arrow
pair (`reportType`, `orderStatus`) into exactly one canonical `event_type`; the raw pair stays
carried for audit and for the fingerprint (contract 06, raw `report_type` only):

| Canonical `event_type` | Arrow input | Executor action |
| --- | --- | --- |
| `order_filled` | `reportType=Fill` (any status), or `COMPLETE` with fill fields | book the fill from the normalized `fill_quantity`/`fill_price` |
| `order_canceled` | `reportType=Canceled` (or `Cancelled`) | emit `order_canceled` |
| `order_rejected` | `reportType=Rejected` or `orderStatus=REJECTED` | emit `order_rejected`, carrying `reject_reason` |
| `order_accepted` | `reportType=NewAck` / `PendingNew` | counted; no state change |
| `order_unknown` | anything else | safety-halt + warning + counter (fail closed) |

Dispatch is fail-closed: a postback whose `event_type` is missing (an older bridge, or the
documented rollback window) or is none of the five values above safety-halts the gate instead of
being dropped. Non-postback stream reports (reconcile echoes) are not lifecycle events and are
ignored. The same vocabulary is pinned machine-readably in
`code/testdata/postback-report-types.json`, consumed by the Go bridge tests and the executor
dispatch tests; contracts 06/07 and that fixture are checked against each other by a
gate-discovered Python test.

The bridge's `disabled` mode is the default and carries no Arrow credentials or route. `fake` mode
is test-only. `live` mode is an explicit process configuration in which only the Go process loads
Arrow credentials and opens the Arrow REST/order-update connections. Neither mode changes the
custom `HALTED → RECONCILING → APPROVAL_PENDING → ENABLED` gate, and no bridge health response may
be interpreted as gate enablement.

## Concurrency and fencing

One fenced active owner holds each `execution_partition_id` (Nautilus instance + custom fencing
token). Every attempt stores the gate epoch and fencing token. Immediately before a bridge command
that leads to an Arrow REST call, the core SHALL verify current gate state, gate epoch, fencing
ownership/token, durable attempt phase, and required health evidence. Lease loss, token mismatch,
storage uncertainty, network partition, or stale ownership prevents the call and moves the
affected gate to `HALTED`.

## Safety-halt control

The Execution Core SHALL consume durable, authenticated `Safety_Halt_Requests` from Signal, the
capture path, platform health, and authorized operators. Each request SHALL include
`halt_request_id`, account/portfolio/execution scope, source component/instance, reason code,
detection time, source epoch/version, evidence hash, and schema version. Requests SHALL be
idempotent. The core SHALL apply or reject each request with immutable audit evidence,
incrementing the gate epoch on an applied halt. Stale, malformed, or cross-scope requests are
rejected and audited. The core SHALL independently detect stale mandatory health even if the
halt-request stream is unavailable.

The Gateway SHALL consume the same durable table, because its in-process halt flag is lost on
restart: a synchronous boot replay (a store failure refuses startup) followed by a 1 s default poll
(`SAFETY_HALT_POLL_MS`). Each pass rebuilds the greatest-seen `source_epoch` per
`(source_component, source_instance)` from rows already `APPLIED` — restart-safe with no offset
state — and rejects a lower epoch as stale. A halt is applied only when the request's
`execution_partition_id` is the gateway's own and the account scope matches the gate row; only
`UNSAFE` rows halt (`RECOVERED` is audited with no gate effect and never auto-enables). The gate
halt is applied first and the outcome is audited second on the row itself
(`application_result`/`applied_ts`, KV upsert), so a crash between the two re-applies idempotently
on restart; a row whose gate row is temporarily unreadable stays `OPEN` and is retried. A consumer
that dies fails readiness and exits the process non-zero.

## Reconciliation capability

Reconciliation uses Arrow REST endpoints (DEC-023) through the go-arrow bridge: `GET /user/orders`,
`GET /user/trades`, `GET /user/positions`, and `GET /order/{id}`. These provide near-real-time data
for the single-operator (Saurabh, DEC-044) resume protocol. Consistency delay and rate limits (10 req/sec) must be
measured.

## Acceptance

Crash-window, duplicate, timeout, rejection, malformed response, missing mapping, changelog gap,
restart/corrupt state, fencing, safety-halt idempotency/scope, unauthorized/mismatched approval,
single-operator (Saurabh, DEC-044) resume, reconciliation capability, and approved-policy reconstruction tests pass. Bridge
`PlaceOrder`/modify/cancel endpoints require Arrow-sandbox smoke tests before trust (the go-arrow
SDK order path is currently untested). Live money stays blocked until the evidence package
approves enablement.

## Requirement traceability

- Functional: `REQ-EXE-001` through `REQ-EXE-013`
- Cross-cutting: `03-non-functional.md` §§3.1–3.8; `04-data.md` §§4.2–4.4, 4.6–4.7; `05-interfaces.md` §§5.7–5.9, 5.11; `06-operational.md` §§6.2–6.10
- Implementation: `../08_implementation/05-execution-core.md` (integrated Execution Core dossier)

See `../02_requirements/02-functional/07-executor.md`.
