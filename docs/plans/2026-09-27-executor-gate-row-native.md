# Executor gate row — native wiring (scope doc, H2-5)

**Created:** 2026-09-27 · **Status:** landed 2026-09-27 — **D2** (gateway-written),
fence/lease TTL 30 s / renew 10 s / halt-on-loss accepted. S1–S4 done; the S4 acceptance (the
H2-2 paper drill attempt 4) **PASSED**; follow-up CHG-337 (locked-posture retry quieting)
landed.

**Source:** plan `docs/plans/2026-09-27-execution-mode-hop-paper-sandbox.md` H2-5, finding
from CHG-331 attempt 3. Operator decision 2026-09-27: native path (no gateway semantic
exception, no data cleanup); realization **D2** (the executor stays isolated; the gateway is
the execution core's Fluss writer, as it already is for every other execution table).

**Goal:** make `Execution_Gate` a live, single-source durable row again — written on every
gate transition by the execution core — so the gateway's intent path and readiness work by
construction (row always exists, boot HALTED, transitions mirrored), with no seeding, no
special cases, and no dual-store drift.

**Not in scope:** the other three durable clients (attempts/journal/audit stay file/in-memory);
live-money enablement; any gateway readiness/semantics change; the t9 direct route.

## 1. Problem (verified 2026-09-27)

| Fact | Evidence |
|---|---|
| The gateway's forward leg reads `Execution_Gate` from Fluss and flips `flussReady=false` on NOT_FOUND | `NautilusIntentClient.forward` (`controls.lookup`, `key not found`), CHG-331 attempt 3 `/readyz` |
| No deployed component writes the row | nautilus: in-memory gate (compose sets no `DURABLE_*`); gateway: placeholder store, `/control/approve` → 501 (`gateStoreAuthoritative=false`) |
| The contract says the executor owns the table | `docs/04_contracts/07-executor.md` L29; `ExecutionGateColumnOwnership` (SCH-15) |
| The durable clients are designed but unbuilt on the Rust side | `durable.rs` L1-37 ("the remaining swap plugs Fluss-backed … behind the same traits") |
| The B4 halted E2E pins "missing row → DEFERRED + not ready" | `B4HaltedIntentConsumeDeferE2ETest` |

## 2. Constraint found while scoping: the executor cannot reach Fluss

| Service | Networks (compose **and** `docker-stack.yml`) |
|---|---|
| `execution-gateway` | `trading-net` + `execution-net` — the execution core's Fluss-facing process (writes `Fills`, `Order_Lifecycle`, `Positions`, ledger) |
| `nautilus` (executor) | `execution-net` only — deliberately isolated from `trading-net`, where Fluss lives |
| `fluss-coordinator` / `fluss-tablet` | `trading-net` |

So "executor publishes `Execution_Gate` to Fluss" (the chosen native path) has two
realizations, and the difference is architectural. Both end with the same property: the row
always exists, mirrors the executor's gate, and is the only authorization the gateway reads.

### D1 — Executor writes Fluss directly (`fluss-rs` 1.0.0, official client)

- Nautilus gets a Fluss connection; a `FlussGateStore` implements the existing
  `executiongate::GateStateStore` trait; boot creates/hydrates the row; approve/halt write it;
  a lease-renewal loop keeps the fence live.
- **Requires a network-boundary decision**: attach nautilus to Fluss (new internal control
  network with the Fluss services, or `trading-net`). This reverses the deployed rule
  "executor stays isolated from market-data networks" (AGENTS hazard; `make
  execution-network-check` guards the related invariants).
- Literal contract target (`07-executor.md` L29; dossier startup sequence L376-398:
  "connect owned Fluss state → acquire/fence the `execution_partition_id` lease").
- New Rust dependency + store + fencing/lease + boot reconcile + tests. Self-contained
  afterwards: no cross-service protocol, no gateway coupling.

### D2 — Executor publishes transitions; the gateway writes the row (projection pattern)

- The executor reports its gate transitions (state/epoch/reason/evidence + owner/fence/lease)
  to the gateway over the existing private signed channel (`GATEWAY_ENDPOINT`,
  `GATEWAY_SHARED_SECRET`, `GATEWAY_PROTOCOL_VERSION` — already used for projections/events);
  the gateway writes `Execution_Gate` with the **existing, tested Java
  `FlussGateStateStore`** ("production writer", `common/.../FlussGateStateStore.java`) and
  returns the persisted row, which the executor adopts (durable-first: the executor's gate is
  a cache of the row; enablement fails closed if the gateway is unreachable).
- No network change; no new dependency; reuses the built+tested writer and the established
  "gateway writes Fluss from nautilus events" pattern (the same pattern as
  `Fills`/`Order_Lifecycle`/`Positions`).
- Cost: a new executor→gateway report protocol + adoption semantics; enablement depends on
  the gateway being up (it already is the intent-path dependency).

| | D1 (executor-direct) | D2 (gateway-written) |
|---|---|---|
| Contract fit | Literal (executor owns the row) | Logical owner = executor; gateway is the transport writer (same as other execution tables) |
| Network boundary | **Changes** (executor → Fluss) | Unchanged (executor stays isolated) |
| New dependency | `fluss-rs` 1.0.0 + image rebuild | None |
| New protocol | None | Executor→gateway gate-report + ACK adoption |
| Reuses existing code | No (new Rust store) | Yes (`FlussGateStateStore`, gateway channel) |
| Enablement coupling | Self-contained | Gateway must be reachable (fail-closed) |
| Long-term maintenance | Crate bumps; one writer, one reader | One more projection on an existing writer; no new dependency |

**Recommendation: D2**, unless the operator wants the literal production topology (then D1
with an explicit network-boundary change). D2 keeps the security boundary that both the dev
compose and the production deck already enforce, reuses the only built+tested gate writer in
the repo, and adds no dependency; its cost is a small internal protocol whose failure mode is
already fail-closed (no gateway → no enablement).

**Decision (2026-09-27, operator): D2.** D1 is recorded as the rejected alternative (it
needs a network-boundary change and a new Rust dependency for no functional gain). The
protocol shape below is the approved one.

### D2 protocol (pinned)

1. **Gateway boot (execution-enabled path only):** open `FlussGateStateStore` and `init` the
   row (create HALTED/unfenced if absent; never clobber an existing row). The row therefore
   exists **before** the pending-intent replay runs, which removes the `key not found`
   readiness race entirely. The store is held authoritative for the process lifetime.
2. **New endpoint `POST /v1/gate`** on the gateway (same bearer + canonical-signature
   machinery as `/v1/events`): the executor reports one of `BOOT_HALT`, `APPROVE`, `HALT`,
   `RENEW` with `partition_id`, `account_scope_id`, `owner_instance_id`, `epoch`,
   `fence_token` (RENEW), `reason`, `evidence_hash`, `now_ms`. The gateway validates
   monotonicity/ownership and writes through the store, then returns the persisted row
   (state, epoch, fence_token, owner_instance_id, lease_expires_ts, transition_ts).
3. **Executor (durable-first):** after the signed envelope is verified locally (DEC-044
   checks unchanged), the transition is *requested* from the gateway; the executor adopts the
   returned epoch/fence/lease and only then completes the local transition. Any store/network
   failure refuses the transition (approve) or keeps/forces HALTED (halt) — fail-closed.
4. **Boot:** the executor reports `BOOT_HALT` (retried with backoff until the gateway answers)
   and hydrates its `control_epoch` from the response (never resets to 1).
5. **Lease:** while ENABLED, `RENEW` every 10 s with TTL 30 s; the first failed renewal
   safety-halts the executor (and reports `HALT`).

## 3. Semantics (either design; mirrors the Java reference writer)

1. **Single source of truth:** the `Execution_Gate` row (v3, 17 columns,
   `code/01_platform/02_sql/ddl/11_execution_gate.sql`; VERSIONED merge on `fence_token`).
   The executor's gate state is authoritative for *decisions*; the row is authoritative for
   *authorization* (the gateway reads it before every forward).
2. **Boot:** always `HALTED` (DEC-044, no auto-resume). The row is created if absent
   (`init`, never clobbers — P3-151) and, if it was `ENABLED`, transitioned to `HALTED`
   (fence retained, owner/lease cleared, epoch +1) so the row never authorizes a dead process.
3. **Transitions:** approve/halt write the full row (identity + state + epoch + reason +
   evidence + transition_ts + approval group + fence group), mirroring
   `FlussGateStateStore` semantics: epoch +1 per accepted transition; halt mints a strictly
   greater `fence_token` (VERSIONED write must land) and retains the token; `owner_instance_id`
   NULL + `lease_expires_ts` NULL = unfenced.
4. **Fencing/lease:** `ENABLED` carries `owner_instance_id` (executor instance), a monotonic
   `fence_token`, and `lease_expires_ts`; the lease is renewed while ENABLED; lease loss or
   renewal failure → safety halt (fail-closed). This is what the gateway's forward path
   requires (`NautilusIntentClient` defers on non-ENABLED, null owner, null/zero fence,
   absent/expired lease).
5. **Epoch coherence:** the epoch the gateway forwards is the row's epoch, and the executor
   accepts only envelopes naming that epoch (`http.rs verify_control`); therefore the
   executor's epoch must hydrate from the durable row at boot (never reset to 1) — in D2 the
   gateway returns it in the ACK.
6. **Identity columns:** `account_scope_id` + `schema_version='3'` written at creation; the
   executor config gains `ACCOUNT_SCOPE_ID` (compose has it for the gateway; nautilus lacks it).

## 4. Code map (current lines, 2026-09-27)

**Executor (Rust)**
- `code/02_services/04_executor/src/durable.rs` — `DurableFlags` (:54-70), gate selection +
  boot init (:309-336), `DurableClients.gate_store` (:210), wiring-status note (:24-37).
- `code/02_services/04_executor/src/durable_file.rs` — `FileGateStore` (:474-543): the
  semantics to mirror (init never-clobber, latest-row-wins).
- `code/02_services/04_executor/src/executiongate.rs` — `GateStateStore` trait
  `read/write/init` (:160-192), `GateRow`/`GateState` (:145-158), `BOOT_HALTED_OWNER` (:157).
- `code/02_services/04_executor/src/http.rs` — in-memory gate: `approve` (:260-298),
  `safety_halt` (:300-312), `bump_control_epoch` (:388), `verify_control` epoch check
  (:504-540), intent gate check (:755).
- `code/02_services/04_executor/src/main.rs` — boot + durable store opening (:97-160).
- `code/02_services/04_executor/src/config.rs` — `DURABLE_GATE_ENABLED` (:179),
  `EXECUTION_PARTITION_ID` (:186-188); add `ACCOUNT_SCOPE_ID`/`FLUSS_BOOTSTRAP` as needed.
- `code/02_services/04_executor/Dockerfile` — `cargo fetch --locked` (:40-49): a new crate
  enters the image through the lock file.

**Gateway (Java)**
- `code/02_services/06_execution_gateway/src/main/java/.../NautilusIntentClient.java`
  — forward path: lookup (:83), state/epoch/owner/fence/lease checks (:95-181), `sendWithFence`.
- `code/02_services/06_execution_gateway/src/main/java/.../FlussControlStateStore.java`
  — `lookup` (:59-87; NOT_FOUND "key not found").
- `code/02_services/06_execution_gateway/src/main/java/.../GatewayHttpServer.java`
  — `/control/approve` (:143-230; `gateStoreAuthoritative` 501 at :157).
- `code/02_services/06_execution_gateway/src/main/java/.../GatewayStartup.java`
  — `Stores` deliberately excludes gate/attempt stores (:29-35); probes the authority tables.
- `code/common/src/main/java/com/trading/common/schema/execution/FlussGateStateStore.java`
  — production writer: `read`/`init`/`acquire`/`renew`/approve/halt + `verifyPersisted`;
  `GateRow.fenceValidFor` (`GateRow.java` :75-103).

**Contracts/DDL**
- `code/01_platform/02_sql/ddl/11_execution_gate.sql` (v3 row, v4 options: VERSIONED on
  `fence_token`; writer obligations (a)-(c)).
- `docs/04_contracts/07-executor.md` L29; `docs/08_implementation/05-execution-core.md`
  §Gate state machine / startup sequence (L376-398).
- Compose: nautilus block (`docker-compose.yml` :1019-1046), gateway networks (:987+),
  `docker-stack.yml` mirrors both.

## 5. Commit slices (region-sliced, one CHG each)

| Slice | Region | Content |
|---|---|---|
| S1 | Gateway gate writer + endpoint (Java) | Wire `FlussGateStateStore` into the enabled startup path (init HALTED, authoritative) + `POST /v1/gate` report endpoint with monotonicity validation + failing-first tests. |
| S2 | Executor reporter + adoption (Rust) | Gate-report client over the existing signed channel; durable-first `approve`/`safety_halt`; boot `BOOT_HALT` + epoch hydrate; RENEW loop (10 s / 30 s TTL) with halt-on-loss + unit tests. |
| S3 | Compose/env + runbook | Enable the store in the execution-t3 profile; document the operator flow (no config change at hop time). |
| S4 | Live proof | H2-2 drill re-run on the new wiring (the paper drill is the acceptance test); mark H2-2/H2-3 `[x]`. |

## 6. Test plan

- **Unit (Rust):** trait conformance, init-never-clobbers, transition monotonicity
  (epoch +1, halt mints greater token), boot-halts-a-previously-ENABLED row, lease
  expiry → halt, durable-first failure (store error → transition refused).
- **Unit (Java):** the gateway writer path (existing `FlussGateStateStore` tests) + the new
  report endpoint validation (reject non-monotonic/stale reports).
- **Integration (live Fluss, env-gated):** write→read round-trip on the real table with the
  VERSIONED engine (dropped lower-token write is detected by re-read).
- **Acceptance:** the H2-2 paper drill — gateway enabled with the 9 pending intents present
  reaches `/readyz` 200, the lifecycle projection lands, and (with the gate ENABLED) the
  intent forward path forwards with the row's epoch/fence.
- Smoke-before-run: the unit + env-gated integration legs before the drill; no >5 min runs.

## 7. Risks / open questions

1. **Network boundary (D1 only):** rejected with D1 — no network change in this workstream.
2. **Enablement coupling (D2, accepted):** enabling requires the gateway reachable; failure is
   fail-closed (executor stays HALTED). The gateway is already the intent-path dependency.
3. **Fence/lease parameters (decided):** TTL 30 s, renew every 10 s, safety halt on the first
   failed renewal.
4. **fluss-rs (D1 only):** not applicable — D2 adds no dependency.
5. **Two-store reconciliation:** none — the row is the only durable gate state; the gateway is
   its only writer, the executor's gate is a cache of it.
6. **B4 test pin:** unchanged (a missing row stays fail-closed, and with the boot `init` it can
   no longer occur in the enabled path); the report endpoint gets its own failing-first tests.
7. **Boot-race residual:** the executor's `BOOT_HALT` retry can lag the gateway's boot by a few
   seconds; until it lands the row may be `ENABLED` from a previous life while the executor is
   HALTED. The forward leg then gets a nautilus 503 → DEFERRED (not a violation), and the next
   report converges the row to HALTED — benign, bounded, fail-closed.

## 8. Approval (2026-09-27)

- [x] Design: **D2** chosen (D1 rejected: network-boundary change + new dependency for no
  functional gain).
- [x] If D1: network decision — not applicable.
- [x] Fence/lease parameters: TTL 30 s / renew 10 s / halt-on-loss accepted.
- [x] Slice order S1→S4 accepted; S4 re-runs the H2-2 drill as the acceptance test.

## 9. S4 acceptance (2026-09-27)

The H2-2 paper drill attempt 4 passed end-to-end on the rebuilt images (gateway
`6d4218273f8f`, nautilus `e77b10ee8ef8`); full narrative in
`logs/exec-hop/paper-drill-20260927/notes.md`:

| Step | Observed |
|---|---|
| Enabled gateway boot | `/readyz` 200 (row created before the replay; no `key not found`) |
| Executor boot report | `BOOT_HALT` applied, row adopted, `/healthz durable_gate:true`, epoch 1 |
| Signed approve (epoch 1) | `ENABLED` epoch 3 / fence 1 — the sanctioned path HALTED(1)→RECONCILING(2)→APPROVAL_PENDING(3)→ENABLED(3); executor adopted 3 |
| Lease | `RENEW` applied every ~10 s, TTL 30 s |
| `t9_order_sandbox.py --live` | **PASS** — place 202 + `event_emission:accepted`, `Order_Lifecycle` 0→1, cancel 202 |
| Signed halt (epoch 3) | `HALTED` epoch 4 / fence 2; halt ACK epoch adopted locally |
| Revert | gateway `/readyz` 503, bridge `disabled`, nautilus `HALTED` epoch 4 |

Follow-up CHG-337: the locked-posture boot retry now logs the first failure at WARN and the
rest at DEBUG with a 30 s backoff cap (measured in the drill: a WARN every 5 s, ~17k lines/day
for an expected posture), covered by `gate_keeper_retries_a_refused_boot_report_then_hydrates`
and a live boot smoke (one WARN in 25 s; enable → hydration of the existing row).
