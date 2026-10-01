# M1-3 design note — one shared report flow (`BridgeSession`)

- **Status:** implemented — design settled 2026-09-29 (post-recon); landed as CHG-389/CHG-390/CHG-391 (2026-09-29; one `BridgeSession`, route+node wiring, drop accounting). Source:
  `docs/plans/2026-09-28-medium-findings-remediation.md` M1-3 (E3); audit row "second dial /
  lost route fill"; recon report `ses_f165a3e71ffeoDy05aK3eJDDVf` (line anchors updated below).
- **Non-negotiables:** one `take_reports` dispatcher owns the single WS subscription; route-owned
  refs are booked (not halted as uncorrelated); node-owned refs reach the node; unknown refs halt
  in one place; no invented replay buffer; a socket drop is counted (`missed_window`) and the
  Tier-11 reconciler runs on reconnect.

## Verified facts (current tree, 2026-09-29)

- `main.rs:136-137` builds a route `HttpBridgeClient` (send-only; `take_reports` is never called
  on it — it has no WS at all). `engine.rs:231-237` builds the node's client lazily per factory
  call; the node's `connect` takes the stream (`execution/client.rs:1050-1055`).
- The node client is deliberately non-`Send` (`execution/client.rs:206-214`) and is the only code
  that can book node fills. **A `Send` session handle does not conflict with this**: the node
  holds the handle, calls `send_command`, and drains its dispatcher-fed channel; no state crosses
  threads.
- **No production pump:** `process_pending` is called only by tests and `shutdown.rs:286`;
  `flush_reports` only by `shutdown.rs:119`. During the day, node reports sit in the channel.
  `drain_reports` is `&self` (`client.rs:657`), and `record_tick` (`client.rs:349-352`) is the
  nautilus-driven periodic hook — the pump belongs there (or in whichever nautilus callback
  calls it; confirm the caller at implementation).
- Unknown refs halt today (`client.rs:679-688`); unrecognized `event_type` halts (`:730-748`).
- `transport.rs` intake loop (current `:573-596`) swallows drops (`connected` stays true, no
  metric); `reconcile_execution_mass_status` (`engine.rs:492-541`) has **no production trigger**.
- Route sync settlement: `http.rs` claim `:984-1105`, phase/broker id `:1231-1258`, response
  `:1260-1359`; the gateway event for the sync ack goes through `events::lifecycle_event_value`
  + `events::emit_event` (events.rs), called `http.rs:1272-1298`.

## Design (intent-preserving, literal plan)

### S1 — one session + dispatcher + registry + route booking

1. **New `src/bridge/session.rs`**
   - `BridgeSession { inner: tokio::sync::Mutex<SessionInner>, connected: AtomicBool, registry:
     Arc<Mutex<Registry>>, node_tx: mpsc::Sender<ReportEnvelope>, node_rx: Mutex<Option<...>>,
     halt: HaltNotifier, missed_window: AtomicU64 }`
   - `SessionHandle(Arc<BridgeSession>)` implements `BridgeClient`:
     `connect`/`send_command` → `ensure_open()` then forward; `is_connected` → the `AtomicBool`
     mirror (the trait method is sync; the transport is behind the async mutex);
     `take_reports` → hands out `node_rx` once (the node's existing consumer contract);
     `disconnect` → close.
   - `ensure_open()` (first call only): underlying `client.connect()`, `take_reports()`, spawn
     the dispatcher. Registered before connect so the route may open it first.
   - **Dispatcher loop:** recv from the real stream → registry lookup by `client_order_ref`:
     * `Route` → route booking (below); * `Node` → `node_tx.send` (bounded; full/closed = halt:
       a report the node cannot receive is ambiguity); * unknown → one-place halt
       (`HaltNotifier`).
2. **Registry:** `Arc<Mutex<HashMap<String, ReportOwner>>>`, created in `main.rs`, cloned into
   the session, the node client, and `ServerState`.
   - Node writes on `build_order_envelope` (`client.rs:908-916`, next to the `client_refs` write).
   - Route writes after `claim_for_send` succeeds (`http.rs:1052-1060` region) with the relation
     data the dispatcher needs (at minimum `Route`; side/symbol can be read from the
     `CommandEnvelope` payload when booking).
3. **Route booking:** the dispatcher normalizes the postback (C1 `event_type` dispatch) and emits
   it to the gateway via `events::` on a **spawned task** (never blocking the dispatcher on a
   gateway HTTP round-trip). The gateway/Fluss projection is the route leg's parity authority —
   the node's in-memory parity map is explicitly node-only (`client.rs:230-234`). Unrecognized
   `event_type` for a route ref uses the same fail-closed halt as node refs.
4. **Wiring:** `main.rs` builds the session once from `BridgeSelection`; the route forwarder
   becomes `Arc<tokio::sync::Mutex<Box<dyn BridgeClient + Send>>>` holding a `SessionHandle`;
   `BridgeExecutionClientFactory` receives the handle and passes it as the node client's bridge
   (instead of constructing one). Both decks unchanged (no config).
5. **Pump:** call `self.drain_reports()` from the nautilus-driven tick (`record_tick` caller) so
   node reports are booked intraday, with tests proving a report arriving between ticks is booked
   without shutdown. Keep the existing `process_pending` for enqueued jobs.

### S2 — drop accounting + reconcile on reconnect

- `transport.rs` intake loop: on any drop/reconnect, increment a `missed_window` counter exposed
  on the session (`telemetry::METRICS.missed_window`) and notify the session; on reconnect the
  session runs `reconcile_execution_mass_status` from a tokio task using a **Send** bridge handle
  (the session handle; the reconcile call needs `&mut B` — clone the handle and lock it), with
  the unknown-ref list from the registry (bounded; include node refs without a venue order id).
- Test: drop the fake WS server, assert `missed_window` increments, reconnect delivers the next
  postback, and reconcile runs once with the current refs (no invented replay buffer).

## Test plan (failing-first + mutation)

| Slice | Failing-first | Mutation |
|---|---|---|
| S1 one dial | count dials on the fake transport: today 2 clients / 1 WS; fixed: 1 WS and the route uses it | construct a second client in the factory |
| S1 route fill | route ref registered; fill arrives → gateway event emitted, no halt; today uncorrelated halt | drop the route registration |
| S1 unknown halt | unknown ref still halts | route *all* refs as node-owned |
| S1 pump | report between ticks booked without shutdown; today booked only at `flush_reports` | remove the drain call |
| S2 missed_window | drop increments the counter; today invisible | swallow the drop |

## Risks / notes

- Keep the dispatcher non-blocking: node channel is bounded (4096, same as today); a full channel
  is ambiguity (halt), never a silent drop.
- `send_command` through the session serializes node and route sends on one mutex — intended
  (one transport); the route's 2 s `RequestBudget` is unaffected.
- The route's old send-only client object disappears with S1 (the audit's "second dial").
- Do not move node booking into the dispatcher: the node client remains the only owner of
  `Rc` booking state (documented non-`Send` contract).
