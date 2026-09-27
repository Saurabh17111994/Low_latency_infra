# Executor gate row — native wiring (scope doc, H2-5)

**Created:** 2026-09-27 · **Status:** draft — awaiting operator approval (design choice D1/D2
+ network decision). No code written from this doc yet.

**Source:** plan `docs/plans/2026-09-27-execution-mode-hop-paper-sandbox.md` H2-5, finding
from CHG-331 attempt 3. Operator decision 2026-09-27: native path (no gateway semantic
exception, no data cleanup).

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
| S1 | DDL/semantics pin (test-only) | Pin the v3 column set + writer obligations the store must satisfy (agreement test vs the DDL; no DDL edit). |
| S2 | Executor gate model | Fence/lease fields + durable-first transitions + boot hydrate/halt (unit-tested with a fake store). |
| S3a (D1) | Rust Fluss store | `fluss-rs` dependency + `FlussGateStore` implementing the trait + image/compose wiring + live env-gated test. |
| S3b (D2) | Gateway gate writer | Wire `FlussGateStateStore` into the gateway (authoritative) + the report endpoint + the executor's reporter; ACK adoption. |
| S4 | Compose/env + runbook | Enable the store in the execution-t3 profile; document the operator flow (no config change at hop time). |
| S5 | Live proof | H2-2 drill re-run on the new wiring (the paper drill is the acceptance test). |

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

1. **Network boundary (D1 only):** attaching the executor to Fluss reverses a deployed
   isolation rule — needs an explicit operator decision and an `execution-network-check`
   update if chosen.
2. **Enablement coupling (D2 only):** enabling requires the gateway reachable; failure is
   fail-closed (executor stays HALTED). Accepted?
3. **Fence/lease parameters:** lease TTL, renewal interval, and halt-on-loss behavior need
   pinned values (proposal: TTL 30 s, renew every 10 s, halt on first failed renewal).
4. **fluss-rs (D1 only):** async runtime/threading compatibility with the Nautilus kernel;
   image build fetches the crate via `cargo fetch --locked` (needs network at build).
5. **Two-store reconciliation:** none in either design — the row is the only durable gate
   state (D1: written by the executor; D2: written by the gateway from the executor's report).
6. **B4 test pin:** unchanged in both designs (a missing row stays fail-closed); D2 adds the
   report path, which needs its own failing-first tests.

## 8. Approval request

- [ ] Design: **D2 (recommended)** / D1 / other.
- [ ] If D1: network decision (dedicated Fluss control network / `trading-net`).
- [ ] Fence/lease parameters (proposal above) accepted.
- [ ] Slice order S1→S5 accepted; S5 re-runs the H2-2 drill as the acceptance test.
