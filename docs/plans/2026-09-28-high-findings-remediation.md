# High findings remediation — audit 2026-09-28 (scope doc)

**Created:** 2026-09-28 · **Status:** proposed — awaiting operator approval and the two `[?]`
decisions (§9); no code changed by this doc.
**Source:** `logs/full-project-audit-20260928/REPORT.md` — every High row (P0-2…P0-5, P0-7, P0-8,
P1-2…P1-14) plus the live token-set-hash defect found during the 2026-09-28 diagnosis session.
**Related:** `docs/plans/2026-09-28-critical-findings-remediation.md` (C1–C3) lands first and
separately; this doc does not touch it.
**Execution protocol:** `docs/plans/2026-09-24-plan-execution-protocol.md` (verify-first, marker
discipline, failing-first tests, one CHG per commit, smoke-before-run, tracker hygiene).
**Scope:** exactly the 20 High findings mapped in §1.1. Medium/Low findings stay in the backlog.
**Operator goal:** each defect fixed once at its root boundary, with a permanent machine guard and
a test that fails if the defect returns. No mode the operator must remember, no firefighting.

## 0. Live tracker

This section is the single source of truth for this wave; the body sections below are the evidence.
Where a body section and this list disagree, this list wins.

**Marker legend**

| Marker | Meaning |
|---|---|
| `[x]` | landed and verified (evidence named) |
| `[~]` | in progress right now |
| `[ ]` | action owed — doable offline / on the local stack |
| `[L]` | action owed that needs the live VM / a real rehearsal |
| `[?]` | needs an operator decision before it can start |
| `[-]` | no action — trigger-gated or superseded (trigger recorded) |

**Roll-up**

| Stage | Tasks | done | wip | todo | live | decide | skip |
|---|---|---|---|---|---|---|---|
| H1 — execution money path: halts, retry ambiguity, duplicates, terminal truth | 5 | 5 | 0 | 0 | 0 | 0 | 0 |
| H2 — ingestion: bad-time ticks, invisible drops, silent exits, token cross-check | 4 | 4 | 0 | 0 | 0 | 0 | 0 |
| H3 — production deck parity with the dev deck | 3 | 3 | 0 | 0 | 0 | 0 | 0 |
| H4 — evidence and release gates must not lie | 4 | 4 | 0 | 0 | 0 | 0 | 0 |
| H5 — compute/mock pinned bounds: dedup state, rate cap, client delivery | 3 | 0 | 0 | 3 | 0 | 0 | 0 |
| H6 — machine facts and their drift guard | 3 | 3 | 0 | 0 | 0 | 0 | 0 |
| **Total** | **22** | **19** | **0** | **3** | **0** | **0** | **0** |

#### H1 — execution money path: halts, retry ambiguity, duplicates, terminal truth

- [x] **H1-1** — Wire the halt consumer into the gateway: new `SafetyHaltTailConsumer` built in
  `ExecutionGatewayMain` after `openStores` (:105) and **before** `applyStartupReadiness(:129, true)`;
  synchronous boot replay (a throw refuses startup); 1 s daemon poll (`SAFETY_HALT_POLL_MS`);
  `gates.halt(expected=row)` first, durable audit second; greatest per-source epoch rebuilt from
  APPLIED rows (stale epoch → `REJECTED` + warn); scope check includes `execution_partition_id`;
  `RECOVERED` is audit-only (never auto-enables); consumer death = readiness fail + non-zero exit.
  (P0-2; contract 07 §114-123.) — CHG-370
- [x] **H1-2** — Transport taxonomy at the executor→bridge boundary: `SendFailure::{NotSent,
  Unknown}`; only pre-dispatch failures may retry (connect/DNS, serialization, HTTP
  400/401/403/404/409/500-fingerprint); timeout/EOF/reset/malformed/other-5xx are `Unknown` →
  **no retry** + durable UNKNOWN + safety halt propagated process-wide via `with_halt_notifier`
  (same pattern as `shutdown_watch`). (P0-3; contract 07 §44-47.) — CHG-369
- [x] **H1-3** — Durable attempt guard ON in both decks: `DURABLE_ATTEMPTS_ENABLED: "true"`,
  `DURABLE_DIR=/data/durable`, named volume `nautilus-durable` in compose **and** stack; code
  default stays OFF for bare runs; fail-closed already holds (unopenable dir = boot error;
  unrecordable claim = 503, zero bridge calls); flip only after H1-2/H1-4, during a halted window.
  (P0-4; CHG-136 deferral.) **Precondition (Medium plan M1-5):** the action-scoped claim key must
  land before this flip — with the guard ON, the current instruction-only key halts legitimate
  cancel/amend. — CHG-374
- [x] **H1-4** — `UNKNOWN` is never a terminal rejection: on `BridgeOutcome::Unknown` keep the
  halt + metrics (`order_unknown`, `unresolved_attempt`), emit **no** order event — the order stays
  pre-send (`Initialized`), no capacity release; reconciliation (contract §Reconciliation) resolves
  it, and only the operator may emit a rejection with evidence. (P0-5.) — CHG-369
- [x] **H1-5** — Fill identity version: `sourceVersion = receiveTime*1_000_000 +
  floorMod(fnv1a64(postbackEventId), 1_000_000)` in `FillEventMapper`, bounds-checked; same-ms
  distinct fills get distinct versions, replay yields the identical version, a hash collision
  degrades to today's loud `CONFLICT` (never a silent drop). (P1-13.) — CHG-373

#### H2 — ingestion: bad-time ticks, invisible drops, silent exits, token cross-check

- [x] **H2-1** — Freshness grace becomes quarantine-only: any non-FRESH decision quarantines,
  counts, and **returns** — always; grace only suppresses the UNSAFE halt. One branch replaces the
  two fall-through blocks; `armFreshnessGrace()` test seam; dossier sentence updated. (P1-6.) — CHG-378
- [x] **H2-2** — One drop path for backpressure: `WriterWorker` gets a drop sink; the service
  counts `acknowledged_loss` + `APPEND_DROPPED`, writes one durable uncertainty journal record on
  tracker halt, then requests a fatal stop **from a daemon thread** (inline = self-deadlock);
  `SKIPPED` during shutdown is counted, not a stop; queue-full branch routes into the same handler.
  (P1-11.) — CHG-379
- [x] **H2-3** — Go terminal failures stop pretending to be a clean shutdown: exit
  `exitTerminalRuntime=3` after the normal drain (SIGTERM stays 0); Java trusts only
  `shutdownStarted` as "requested", restarts once then goes fatal with `fatalStopReason` +
  `BRIDGE_CRASH`; freshness clock splits control vs data frames — ACTIVE with no market data goes
  stale in 15 s. (P1-12.) — CHG-380
- [x] **H2-4** — Per-slot token-hash comparison: replicate the Go carve Java-side (sorted tokens,
  1024/slot) and compare `TokenSetHash.of(slice)` against `assigned_token_set_hash`;
  verdicts `MATCH` / `MISMATCH` (real drift, existing metric) / `UNKNOWN_SLOT` / `EMPTY_SLICE`
  (layout drift, new decode reason); warn-only per contract; shared fixture
  `code/testdata/slot-token-hashes.json` — Go-generated, pins the real 2433-instrument/3-socket
  datastream (1024+1024+385), consumed by the Go fixture test, `TokenSetHashTest`, and the Python
  validator. (live token-hash finding.) — CHG-381

#### H3 — production deck parity with the dev deck

- [x] **H3-1** — One execution identity in the stack: top-level `x-execution-identity` anchor
  merged into gateway + nautilus environments; no literal `dev-*` fallback in Rust (partition set
  without scope = boot refusal); executor treats `409 SCOPE_MISMATCH` as non-retryable and stops
  the retry loop with a named error + readiness `durable_gate=false`. (P0-7.) — CHG-394, CHG-395
- [x] **H3-2** — Mirror the five `fs.s3a.*` keys (region, endpoint, path-style, access key, secret
  key) into all four stack `FLUSS_PROPERTIES` blocks; parity test pins compose == stack per role.
  (P0-8; CHG-306.) — CHG-393
- [x] **H3-3** — Retire the three seeded Position_State alert rules + corpus file and mark
  RB-POS-001 retired (DECIDED 2026-09-28 — retire; their producers were deleted in `0f3e5952`,
  and a substitute `gate_safety_halt` rule would be dead on arrival while executor OTLP is a
  NullSink); guard: every seeded alert must name a live producer. (P1-8.) — CHG-392

#### H4 — evidence and release gates must not lie

- [x] **H4-1** — `loadtest-20k-regression`: the evidence file and an `appended=` line are
  mandatory (missing = FAIL); count with `grep -c … || true` so zero hits is a pass; `UNSAFE>0`
  fails. (P1-2.) — CHG-396
- [x] **H4-2** — Daily VM mode is explicit and recorded: **DECIDED 2026-09-28 — the daily CloudPe
  VM intentionally runs the dev multi-socket universe** (`DEPLOYMENT_ENV=dev`, `UNIVERSE=full`:
  2433 tokens, 3 sockets, multi-connection approval). `.env.vm.example` and the VM guide state
  this operating mode explicitly (a decision, not an accidental default); the planned
  production-posture refusals and golden pins are dropped. No behavior change. (P1-3.) — CHG-397
- [x] **H4-3** — WARN-skips are counted: shared `warn_skip()` calls `note_skip` then prints;
  steps 2 and 7 use it; the verdict can no longer print a certificate with unrecorded skips;
  static invariant + behavioral tests. (P1-4.) — CHG-398
- [x] **H4-4** — `audit_r2.py` exits non-zero on its own FAIL: `validate` returns
  `0 if result=="PASS" else 1` (evidence still written); `provision` returns 1 when a bucket_lock
  READ/SET actually failed (`NOT_SET`/`UNSUPPORTED` stay non-fatal). (P1-5.) — CHG-399

#### H5 — compute/mock pinned bounds: dedup state, rate cap, client delivery

- [ ] **H5-1** — N7 dedup state is bounded: `strategy-host-emitted-ids` stays as read-only legacy;
  new ids go to one `MapState` per rule (`strategy-host-emitted-ids-v2/<ruleId>`); dedup reads both;
  prune only the triggering rule's map after 16 new ids (oldest `detTs`, null never evicted); no
  full scan per emission; old checkpoints restore unchanged. (P1-7.)
- [ ] **H5-2** — Mock PEAK honors the DEC-045 20/s/instrument cap: local `PER_INSTRUMENT_CAP=20`;
  constructor rejects a higher rate; PEAK no longer re-derives its profile from the rate; the
  workload keeps the 51–54 ms arrival shape. (P1-9.)
- [ ] **H5-3** — Per-session single-writer mock delivery: bounded outbox (64 batches) + one daemon
  writer per client; `offer` drops on overflow (marked); a 5 s no-progress watchdog evicts stalled
  sessions; the shared delivery pool is deleted. (P1-10.)

#### H6 — machine facts and their drift guard

- [x] **H6-1** — Supersede FACT-009/FACT-014 via `env_facts.py replace`: new rows claim the pushed
  GHCR digests recorded in `runtime.lock:25-26` (CHG-306 + T9.2 proof); append the AGENTS.md
  annotation; LIVE rows are never edited in place. (P1-14.) — CHG-400
- [x] **H6-2** — Supersede FACT-001/FACT-012: one-node Swarm with this host as Leader; two local
  `docker stack deploy` runs on 2026-09-21; a real-VM deploy remains unproven; host-state checks
  re-measured at implementation. (P1-14.) — CHG-400
- [x] **H6-3** — Drift guard `tests/test_env_fact_claims.py`: LIVE `Claim: VAR=value` must match
  `runtime.lock` (unknown claim variable = fail-closed); stale-phrase rule catches "unpushed"-style
  prose against a pushed digest; plan-anchor rule keeps FACT-001/012 from going stale again.
  (P1-14.) — CHG-401

## Overview

This is a six-region wave. Every region fixes one boundary and ships one permanent guard:

| Group | Boundary that failed | Fix boundary | Permanent guard |
|---|---|---|---|
| H1 | executor↔broker ambiguity + gateway never consumed halt rows | typed send outcomes + a wired, durable halt consumer | Rust taxonomy tests, gateway consumer tests, deck pins for the durable flag |
| H2 | ingestion admitted bad-time ticks, hid drops, and exited clean | terminal quarantine, one drop sink, real exit codes, per-slot hash | ingestion failure-matrix tests + shared slot-hash fixture |
| H3 | the production stack drifted from the dev deck | one identity anchor, mirrored `fs.s3a.*`, retired dead alerts | stack parity tests + producer-existence test |
| H4 | missing/skipped/FAILed evidence still printed green | evidence required, skips counted, FAIL = non-zero exit | Makefile target tests, gate-wiring tests, `audit_r2` main tests |
| H5 | compute state grew unbounded; mock exceeded a pinned cap and could stall | bound per-rule state, enforce the cap, per-session writer | compute/mock unit tests with measured assertions |
| H6 | the facts ledger kept claims the tree had outgrown | append-only supersession + a claim-vs-lock guard | `test_env_fact_claims.py` (gate-discovered) |

## 1. Verify-first (reproduced on `5b4725d2`)

All 22 tracker items (the 20 findings plus the split and guard items) were re-verified against the
working tree by seven read-only design agents; every verdict below is REPRODUCES (evidence
abbreviated; full file:line map in §5).

| Item | Verdict | Evidence |
|---|---|---|
| H1-1 | REPRODUCES | `SafetyHaltTailProcessor` constructed only in tests; `ExecutionGatewayMain` never replays halts; `appliedIds` in RAM; no epoch comparison. |
| H1-2 | REPRODUCES | `client.rs:444-447` maps every send error to `AttemptError::Transient`; `resilience.rs:362-368` retries; exhaustion returns `Err` without halting. |
| H1-3 | REPRODUCES | No `DURABLE_*` in `docker-compose.yml`; both decks rely on the gateway dedup + the bridge's restart-lost cache. |
| H1-4 | REPRODUCES | `client.rs:541-551` halts locally then `emit_order_rejected` — a terminal OMS event for a maybe-live order. |
| H1-5 | REPRODUCES | `FillEventMapper.java:73-76` uses ms `receive_time` as `sourceVersion`; two same-ms fills → equal version + different id → CONFLICT → scope halt. |
| H2-1 | REPRODUCES | `IngestionService.java:1127-1152` quarantines, then falls through to the append path while `inFreshnessGracePeriod()`. |
| H2-2 | REPRODUCES | `RawTickWriter.java:177-181` REJECTED → private `syncDropCount` only; `incrementAcknowledgedLoss()` fires only on the unreachable queue-full branch. |
| H2-3 | REPRODUCES | Terminal Go outcomes fall through to a clean exit 0 (`main.go:254,316-339`); Java treats code 0 as requested shutdown (`IngestionService.java:724`); `bridge_metrics` every 10 s refreshes the 15 s data watchdog. |
| H2-4 | REPRODUCES | Java compares the full-set digest to Go's per-slot `assigned_token_set_hash` (`IngestionService.java:1556-1560` vs `subscription_plan.go` carve) → every multi-socket ack raises `TOKEN_HASH_MISMATCH`. |
| H3-1 | REPRODUCES | `docker-stack.yml:1018-1019` hardcodes `dev-*` while the gateway uses `${…:-prod-*}`; stack env sets neither key; the durable gate can never arm. |
| H3-2 | REPRODUCES | Five `fs.s3a.*` keys exist in compose (`:448-452,588-592`) and in none of the four stack Fluss blocks. |
| H3-3 | REPRODUCES | All three rules reference producers deleted in `0f3e5952`; all seed `enabled:true`; no producer exists in compute. |
| H4-1 | REPRODUCES | `Makefile:604` `grep -c … \|\| echo 0` yields `"0\n0"` on a clean log (gate FAILs) and passes vacuously when the log is missing. |
| H4-2 | REPRODUCES | `.env.vm.example` sets neither `DEPLOYMENT_ENV` nor `UNIVERSE`; `day_run.py:929-930` defaults `dev` + `full` (2433 tokens, 3 sockets, multi-connection approved). |
| H4-3 | REPRODUCES | `run-monday-gates.sh:613-615,781-787` WARN branches never call `note_skip`; the verdict subtracts only `GATE_SKIPS`. |
| H4-4 | REPRODUCES | `audit_r2.py:712` returns 0 with `result="FAIL"`; `provision` returns 0 after a bucket_lock READ/SET FAILED (`:486`). |
| H5-1 | REPRODUCES | `maybeCompactEmittedIds` matches `ruleId + "\|"` but real ids are plain UUIDs (`N7RangeBreakoutStrategy.java:416-421`) → never pruned, full scan per emission. |
| H5-2 | REPRODUCES | `MockArrowServer.java:270` `rate = PEAK ? 30 : 20`; the 10 ms loop emits 30/s/instrument, above DEC-045's 20/s cap. |
| H5-3 | REPRODUCES | One pooled delivery task per client per batch, unbounded queue, no write deadline; a stalled client is retained forever. |
| H6-1 | REPRODUCES | FACT-009/FACT-014 are LIVE and claim unpushed digests while `runtime.lock:25-26` pins pushed GHCR digests (CHG-306). |
| H6-2 | REPRODUCES | FACT-001/FACT-012 are LIVE and claim worker/no-manager/no-deploy while the 2026-09-21 measurement (AGENTS.md annotation, CHG-279/283) contradicts them. |
| H6-3 | REPRODUCES | docs-audit C17 checks ledger shape only; nothing binds a claim to `runtime.lock`. |

## 2. Root cause (one sentence each)

- **H1-1** — the halt processor was written as an offline KV tail with no production caller, so no
  halt ever reached the durable gate.
- **H1-2** — the transport handed raw strings to a boolean classifier, so "not delivered" and
  "outcome unknown" were indistinguishable and everything looked retryable.
- **H1-3** — the Workstream-D guard landed deliberately OFF (CHG-136) and no deck ever turned it on.
- **H1-4** — an ambiguous outcome was reported to the OMS as a decided rejection instead of an
  unresolved attempt.
- **H1-5** — the KV version was a millisecond clock that two distinct fills can share, while the
  unique replay-stable identity (`postback_event_id`) was left out of it.
- **H2-1** — a grace-window edit moved the freshness `return` inside the halt-suppression branch,
  making bad-time ticks fall through to the append path.
- **H2-2** — the REJECTED contract was treated as "already accounted" at the writer while the
  account lived on the almost-unreachable queue path.
- **H2-3** — the process had no exit status for runtime terminal failure, and Java used a control
  frame — not the exit code — as the source of truth for "requested stop"; the freshness clock was
  shared by data and control frames.
- **H2-4** — a previous fix corrected the full-set check but left the per-slot field compared
  against a full-set digest.
- **H3-1** — CHG-336 wrote the executor's identity as literals instead of referencing the
  gateway's interpolation, and no check compares values.
- **H3-2** — the CHG-306 mirror was applied to compose only.
- **H3-3** — the 2026-09-05 cutover deleted the active-signal subsystem but not its alert corpus,
  and nothing joins seeded alerts to live producers.
- **H4-1** — grep's "no match" status was treated as an error needing a fallback instead of a
  count, and the recipe never asserted its evidence file exists.
- **H4-2** — the VM profile was written for lifecycle/EOD only and never recorded which universe
  the daily host is meant to run, leaving the dev defaults to read as accidental.
- **H4-3** — later SKIP branches adopted `note_skip`; the two earlier WARN branches did not, and the
  verdict trusts one counter.
- **H4-4** — the exit status is hardcoded success while the verdict lives in the evidence dict.
- **H5-1** — compaction groups by a key prefix the id generator stopped producing, and the trigger
  scan runs on the hot path instead of on write-count.
- **H5-2** — DEC-045 re-profiled the workload pacing but not the server's rate mapping; two sources
  of "PEAK" with no single cap.
- **H5-3** — one session's stream has many possible writers plus no queue bound or write deadline.
- **H6-1/H6-2** — the claims were true when written and were never superseded after the tree
  changed; the checks asserted only that a variable exists, not what it says.
- **H6-3** — no repo-level guard binds a ledger claim to the file that is its source of truth.

## 3. Recommended design (native; no new dependency)

### H1 — execution money path

1. **Halt consumer (P0-2).** New `SafetyHaltTailConsumer` (gateway package) owns the existing
   processor: constructed on `stores.gates()` after `openStores` and **before** the readiness
   flip; synchronous replay at boot (a throw aborts startup — never serve ready with unapplied
   halts); then a daemon thread (`execution-halt-consumer`, `SAFETY_HALT_POLL_MS` = 1000 ms) calls
   `replaySafetyHalts`. Apply order: `gates.halt(expected=row, …)` (durable epoch +1) **then**
   write the audit result via a new `ControlStateStore.recordApplication(haltId, result,
   appliedTs)` (Fluss impl sets `application_result`/`applied_ts`, upserts; rows not `OPEN` are
   skipped — this also stops self-echo). A crash between the two steps re-applies on restart
   (idempotent, safe direction); a write failure fails readiness and exits non-zero. Greatest
   per-source epoch is rebuilt on every replay from APPLIED rows — restart-safe without offsets;
   `sourceEpoch < max` is `REJECTED` + warn. Scope check = account **and** `execution_partition_id`.
   Only `UNSAFE` halts; `RECOVERED` is audited with no gate effect.
2. **Send taxonomy (P0-3).** `SendFailure::{NotSent, Unknown}` from `send_command`.
   *NotSent (bounded retry):* DNS/TCP connect before any byte, serialization failure, and bridge
   HTTP statuses returned before dispatch — 400 malformed, 401/403, 404, 409 request-id reuse,
   500 fingerprint failure. *Unknown (never retry):* read timeout, write/reset/EOF after send,
   malformed status/headers/body, any other 5xx or unrecognized status. The `Unknown` arm bypasses
   the retry loop and goes straight to the durable-UNKNOWN + halt handling; exhausted `NotSent`
   stays unresolved without halting. The halt propagates process-wide through
   `BridgeExecutionClient::with_halt_notifier` → `ServerState::safety_halt` → durable HALT via the
   existing `GateReporter`.
3. **Durable attempts (P0-4).** Deck-only: flag + `DURABLE_DIR=/data/durable` + named volume in
   both decks; keep the code default off. Fail-closed paths already exist (unopenable store = boot
   error; unrecordable claim = HTTP 503 and zero bridge calls). Rollout order: H1-2 and H1-4 first
   (ambiguity halts instead of retrying), then the flag flip during a halted window; smoke test =
   read-only mount must exit non-zero, restart must preserve `attempts.jsonl`. Document in the
   compose comment that the file store is the deployed guard and `FlussAttemptStore` is a gateway
   probe only.
4. **UNKNOWN outcome (P0-5).** Keep `safety_halt()`, add `order_unknown` + `unresolved_attempt`
   metrics and a reason log; emit no order event. The order stays `Initialized` (no capacity
   release, no false REJECTED). Reconciliation matches the broker's orders by the deterministic
   `client_order_ref`; the true async postback later emits the real terminal event; only a proven
   non-acceptance is turned into `emit_order_rejected`, with evidence, on the operator path.
5. **Fill version (P1-13).** `sourceVersion = receiveTime*1_000_000 +
   floorMod(fnv1a64(postbackEventId), 1_000_000)` with a fail-closed bounds check
   (`receiveTime > 0`, no overflow). Replay of the same fill yields the identical version →
   `DUPLICATE` no-op; distinct fills in one millisecond get distinct versions; the ~1e-6
   same-ms hash collision degrades to today's loud `CONFLICT`, never a silent drop. No store
   migration (positions/fills tables are not applied); any scratch store written with the old
   encoding must be rebuilt, never mixed. When a Fills v3 unblocks `source_sequence` for other
   reasons, that becomes primary and this composite becomes the documented fallback.

### H2 — ingestion

1. **Freshness (P1-6).** One terminal branch: any non-FRESH decision writes quarantine evidence,
   bumps the decode-error metric (reason FUTURE/STALE), and returns; the UNSAFE emission is the
   only thing the grace window suppresses. No new config; a package-private `armFreshnessGrace()`
   seam replaces the two ad-hoc arms so the grace path is testable.
2. **Drop accounting (P1-11).** `WriterWorker` gains `SyncDropSink`, called for every non-ACCEPTED
   outcome (keep `syncDropCount` for existing tests). The service handler increments
   `acknowledged_loss` + a new `APPEND_DROPPED` decode reason; on a halted tracker it writes one
   durable uncertainty journal record (`reason: backpressure_halt`, tracker totals) through a
   one-shot CAS, then a daemon thread calls `requestFatalStop` (inline would self-deadlock in the
   worker's own shutdown wait). `SKIPPED` during an orderly shutdown is counted, never a new stop.
   The queue-full branch and the queue 100% listener route into the same handler.
3. **Real exits + honest freshness (P1-12).** Go: `runHFT` returns a status; terminal runtime
   outcomes (policy violation, auth exhausted, subscription rejected, panic) drain then
   `os.Exit(3)`; SIGTERM/SIGINT remains 0. Java: `requested = shutdownStarted.get()` (no
   `exitCode==0`, no `!running`); a spontaneous exit is unexpected — restart once, then
   `requestFatalStop` with `fatalStopReason` + `BRIDGE_CRASH` evidence; the container's
   `on-failure:3` policy does the revival. Watchdog: `handleControlArrival()` no longer touches
   `lastFrameNanos`/`setLastFrameReceived`; connection-up time is a separate floor, so an ACTIVE
   connection with no data goes stale in 15 s.
4. **Token cross-check (live finding).** Java replicates the Go carve over the exact token list
   handed to the bridge (sorted, 1024 per slot, `hft-N`), compares `TokenSetHash.of(slice)` with
   the per-slot field, and reports `MISMATCH` (real drift) or `UNKNOWN_SLOT`/`EMPTY_SLICE` (layout
   drift, new decode reason) — warn-only per contract. The log prints the expected per-slot digest
   instead of the misleading full-set one. The full-set check stays on `manifest_fingerprint`. A
   shared fixture `code/testdata/slot-token-hashes.json` is consumed by both the Go and Java tests;
   a gate-discovered Python test validates it.

### H3 — production deck

1. **Identity (P0-7).** A top-level `x-execution-identity` mapping carries both keys with the
   gateway's expressions; both services merge it (`<<: *execution_identity`), so divergence is
   structurally impossible while operator overrides still apply to both. Remove the hardcoded
   `dev-scope` fallback in `config.rs` — partition without scope is a boot refusal; identity is
   never invented in code. Treat `409 SCOPE_MISMATCH` at boot as a non-retryable, named failure:
   ERROR log naming both keys, readiness `durable_gate=false`, stop the retry loop (today it is
   indistinguishable from a locked gateway).
2. **R2 keys (P0-8).** Insert the five `fs.s3a.*` keys after `s3.connection.ssl.enabled` in all
   four stack Fluss blocks with values identical to compose. The parity guard compares the
   `fs.s3a.*` key sets per role between compose and stack (stack tablets as a set of three);
   the three stack-absent non-S3 tablet keys are recorded as a same-class follow-up, not silently
   widened.
3. **Dead alerts (P1-8).** Retire the three rules and the corpus file; mark the runbook section
   HISTORICAL/RETIRED in the same change. Do not substitute `gate_safety_halt` while the executor's
   OTLP sink is a NullSink — that produces a second dead rule. Record the executor-OTLP dependency
   as the trigger for a future position-state alert.

### H4 — evidence and release gates

1. **Loadtest gate (P1-2).** Require the evidence file, require an `appended=` value, count UNSAFE
   with `grep -c … || true`; zero is a pass, missing evidence is a FAIL.
2. **VM profile (P1-3) — DECIDED 2026-09-28: keep dev + full.** `.env.vm.example` carries
   `DEPLOYMENT_ENV=dev` and `UNIVERSE=full` (2433 tokens, three sockets, multi-connection
   approval) with a comment naming this as the daily VM's operating mode, repeated in the VM
   guide. No production-posture refusal and no golden-image key pins are added (the operator chose
   the existing dev universe for this host); the pre-existing `full`+`production` refusal stays.
3. **Skip accounting (P1-4).** `warn_skip()` records via `note_skip` and prints the WARN in one
   call; steps 2 and 7 use it; the verdict then cannot claim a skipped step was verified. The
   shellcheck WARN stays a plain echo (step 1 still runs `bash -n`) and is explicitly allow-listed
   as a partial check.
4. **Audit exit codes (P1-4).** `validate` returns 1 when its evidence says FAIL (evidence is
   still written — a FAIL is a result, not an abort); `provision` returns 1 when an attempted
   bucket_lock read/set actually failed; `NOT_SET`/`UNSUPPORTED` remain recorded limitations.

### H5 — compute/mock

1. **N7 state (P1-7).** Keep the candidate id format exactly (downstream/PK contract). The legacy
   `strategy-host-emitted-ids` map becomes read-only (every pre-upgrade id still suppresses);
   new ids go to one `MapState` per rule (`…-v2/<ruleId>`), dedup reads both O(1). A transient
   per-rule counter triggers pruning of that rule's map only, after 16 new ids, evicting oldest
   `detTs` entries (null never evicted). No per-emission full scan. Old checkpoints restore
   exactly; rollback to the old binary loses only the post-upgrade window (documented).
2. **PEAK cap (P1-9).** A local `PER_INSTRUMENT_CAP = 20` (with the DEC-045 citation), the
   constructor rejects `tickRatePerSec > cap`, and `main` passes the parsed profile explicitly so
   PEAK keeps its workload shape without re-deriving the rate. After the fix PEAK and BASELINE
   share the 20/s wire pacing; the profile selects the arrival shape.
3. **Mock delivery (P1-10).** `ClientSession` owns a bounded `ArrayBlockingQueue` (64 batches) and
   a single daemon writer; the scheduler `offer`s and drops marked on overflow; a 1 s watchdog
   evicts sessions with no progress for 5 s (closing the socket unblocks a wedged write); `drop`
   is idempotent; the shared delivery pool is deleted.

### H6 — facts ledger

1. **H6-1/H6-2.** Use the ledger's own `env_facts.py replace` (retire + append, atomic): new rows
   claim the pushed digests with `runtime.lock` proof and supersede FACT-009/014; new rows record
   the one-node Leader state and the two local stack deploys while keeping "real-VM deploy
   unproven" explicit; append (never edit) the AGENTS.md annotation for the shipped pins.
2. **H6-3.** New `tests/test_env_fact_claims.py` parses the ledger, ignores non-LIVE/claimless
   rows, and checks every LIVE `Claim: VAR=value` against a small fail-closed registry
   (`*_IMAGE`/`*_VERSION` → `runtime.lock`; opt-in `PORT_<service>` → both decks). Two prose rules
   cover rows without claims (stale-pin phrases against a pushed digest; the 2026-09-21 anchors
   keeping FACT-001/012 from being LIVE). Unknown claim variables fail with a "register the
   source" message.

## 4. Permanent guards (the "will not come back" list)

| Item | Guard (test file + rule) |
|---|---|
| H1-1 | gateway tests: published halt applies (epoch+1); stale epoch rejected; restart does not double-apply; malformed row → no halt; replay throw refuses startup |
| H1-2 | Rust table test: one case per taxonomy row; timeout/EOF/malformed → exactly 1 bridge call + halt; connect-refused → retry >1, no halt |
| H1-3 | gate-discovered `test_execution_deck_durable_attempts.py`: flag true, dir set, volume mounted, in both decks |
| H1-4 | Rust: scripted Unknown → zero terminal order events, gate HALTED, metrics bumped, one bridge call |
| H1-5 | `FillEventMapperTest`: same-ms distinct ids → distinct versions; replay → identical; formula + bounds pins. Driver test: both APPLIED, re-feed DUPLICATE |
| H2-1 | `FreshnessGraceNeverAppendsTest`: grace → quarantine only, zero append calls, no UNSAFE; after grace → no append + UNSAFE |
| H2-2 | `WriterWorkerTest` + `IngestionNoSilentDropTest`: REJECTED calls the sink; halt writes the uncertainty record, sets fatal reason; ledger balances |
| H2-3 | Go status pins + Java `BridgeAuthFailureE2ETest`/`BridgeCrashLoopE2ETest` + control-frame-only watchdog test |
| H2-4 | `SlotTokenHashTest` + shared fixture consumed by Go and Java; Python fixture validator |
| H3-1 | `test_09_stack.py::TestExecutionIdentityParity`: both services carry equal, non-literal identity values |
| H3-2 | `test_09_stack.py::test_stack_fs_s3a_mirror_matches_compose` per role |
| H3-3 | `test_alert_threshold_parity.py::test_every_seeded_alert_has_a_live_producer` |
| H4-1 | `test_makefile_wave25.py` loadtest cases: clean pass, UNSAFE fail, missing evidence fail, low count fail |
| H4-2 | `test_vm_golden_recipe.py` + guide pins asserting the example documents the dev+full mode (no refusal matrix) |
| H4-3 | `test_14_gate_wiring.py` behavioral step-2/7 skip count + static "no uncounted skip WARN" invariant |
| H4-4 | `test_audit_r2.py` main-exit-code tests (validate FAIL, provision bucket_lock FAILED, NOT_SET non-fatal) |
| H5-1 | `StrategyHostFunctionTest`: real UUID ids → bounded size; prune scan counter; restore test for old/new checkpoints |
| H5-2 | `MockArrowServerTest`: measured aggregate ≤ 20/s + `configuredRate(PEAK)==20` + constructor rejects 30 |
| H5-3 | `MockArrowServerTest`: stalled client evicted ≤ 5 s + healthy peer keeps receiving; per-session single writer + bounded outbox |
| H6-1/2 | `test_env_fact_claims.py` claim rule + stale-phrase rule + 2026-09-21 anchor rule |
| H6-3 | (same file — it is itself the guard) |

## 5. Code map (current lines, for the commit slices)

| Item | Files : lines |
|---|---|
| H1-1 | `06_execution_gateway/.../SafetyHaltTailProcessor.java:36-56,92`; `ExecutionGatewayMain.java:105-108,117-142`; `FlussControlStateStore.java:95-118`; `ControlStateStore.java:8-46`; DDL `18_safety_halt_requests.sql` |
| H1-2 | `04_executor/src/execution/client.rs:444-478,541-551`; `resilience.rs:334-374`; `transport.rs:65-67,102,108-155,639-651`; `bridge/client.rs:37`; `http.rs:390` |
| H1-3 | `docker-compose.yml:295,1052-1090`; `docker-stack.yml:136,982-1035`; `config.rs:70,221`; `main.rs:120-123,157-200`; `http.rs:827-960,1048-1060`; `IntentReader.java:141-150` |
| H1-4 | `client.rs:78-86,492-500,534-551`; emitter vendored `:146,152-169`; contract 07 §125-131 |
| H1-5 | `FillEventMapper.java:15-21,66-76,88-100`; `PositionProjector.java:80-113`; `KvStateUpdateProtocol.java:23-36`; `PositionProjectorDriver.java:142-154`; `08_fills.sql:39-42` |
| H2-1 | `IngestionService.java:564,1095-1152,1300-1310,1593,2202`; dossier `03-ingestion.md:92,129,341` |
| H2-2 | `RawTickWriter.java:160-181`; `WriterWorker.java:46-51,66,84-103`; `IngestionService.java:293-306,340-355,1318-1320,1476-1492` |
| H2-3 | `main.go:85-89,254,316-339,546-558,604-609`; `supervisor.go:140-156,227-233`; `metrics.go:60-71`; `IngestionService.java:510-514,570-584,695-772,830-839,1596-1605`; `HealthProbe.java:230-249`; dossier `03-ingestion.md:279` |
| H2-4 | `IngestionService.java:194,251-258,329-331,1521-1560`; `TokenSetHash.java:31-63`; `go-bridge/main.go:174-190`; `subscription_plan.go:29-124`; `transport.go:47,238-249`; contract `ingestion-ndjson-schema.md:194` |
| H3-1 | `docker-stack.yml:977-978,1006-1019`; `docker-compose.yml:1041-1042,1077-1081`; `executor/src/main.rs:43-55`; `http.rs:293-326,452-500`; `gate_report.rs:180-198`; `config.rs:227-230`; `GatewayHttpServer.java:284-287` |
| H3-2 | `docker-compose.yml:448-452,588-592`; `docker-stack.yml:330-351,415-439,490-514,565-589`; CHG-306 |
| H3-3 | `position-state-alerts.json:1-99`; `seed_alerts.py:37-44`; runbook `01-runbooks.md:890-925`; `test_alert_threshold_parity.py:59,280-313` |
| H4-1 | `Makefile:596-608` |
| H4-2 | `.env.vm.example:1-30`; `day_run.py:51,65,191-251,928-936,1207`; `vm-golden-build.sh:91` |
| H4-3 | `run-monday-gates.sh:204,583-615,764-787,1136-1142` |
| H4-4 | `audit_r2.py:433-491,494-601,689-712` |
| H5-1 | `StrategyHostFunction.java:63-78,99,143-182,317-366,374-412`; `N7RangeBreakoutStrategy.java:349,416-421`; `StrategyHostFunctionTest.java:359-384` |
| H5-2 | `MockArrowServer.java:28-31,54-85,139-141,260-285`; `SyntheticWorkload.java:68-82`; `FixedScope.java:29-38`; `04-decisions.md:48` |
| H5-3 | `MockArrowServer.java:47-48,79-84,114-137,139-215,245-257` |
| H6-1/2 | `docs/ENVIRONMENT.md:64-75,159-166,191-211`; `runtime.lock:25-26`; `AGENTS.md:137-149`; `env_facts.py`; `plan 2026-09-22-fluss-1.0-upgrade.md:1747-1774` |
| H6-3 | `docs/ENVIRONMENT.md` + `runtime.lock`; new `tests/test_env_fact_claims.py` |

## 6. Test plan (failing-first + mutation)

Each item lands test-first: the new test must fail on the unmodified tree, then the fix turns it
green, then one mutation proves the test can catch the defect's return. Mutations are run locally
and reverted; only the fixed tree is committed.

| Item | Failing-first (today) | Mutation (must go red) |
|---|---|---|
| H1-1 | published halt applies in a gateway test — no consumer exists | delete the `gates.halt` call |
| H1-2 | timeout → exactly 1 bridge call + halt — it retries today | map Unknown back to Transient |
| H1-3 | deck test finds no flag/volume — fails today | strip one env line |
| H1-4 | Unknown → zero terminal order events — emits `OrderRejected` today | restore `emit_order_rejected` |
| H1-5 | same-ms distinct ids → distinct versions — equal today | drop the identity hash / change the seed |
| H2-1 | grace → zero append calls — appends today | move `return` back inside the halt branch |
| H2-2 | halted tracker → uncertainty record + stop — private counter today | remove the sink call; make the stop inline (deadlock) |
| H2-3 | Go status 3 + Java fatal decision — exit 0 today | revert `exitTerminalRuntime`; restore `exitCode==0 ⇒ NO_RESTART` |
| H2-4 | pinned slot vectors match — full-set comparison today | swap expectation back to the full-set digest; change the carve limit |
| H3-1 | parity test — values differ today | restore a literal `dev-scope` |
| H3-2 | mirror test — stack set is empty today | delete `fs.s3a.region` from one tablet |
| H3-3 | producer-existence test — three dead rules today | re-add a rule with a retired leaf |
| H4-1 | clean log passes, missing log fails — inverted today | restore `\|\| echo 0`; delete the file guard |
| H4-2 | example/guide lack the explicit dev+full statement | delete the mode line from the example; drop it from the guide |
| H4-3 | WARN-skip counts as a skip — counts verified today | delete one `warn_skip` call |
| H4-4 | validate FAIL → rc≠0 — rc 0 today | restore hardcoded `return 0` |
| H5-1 | real-UUID churn stays bounded — never pruned today | no-op prune; reintroduce the full scan |
| H5-2 | measured ≤ 20/s + rate pin — 30/s today | restore `PEAK ? 30 : 20`; drop the constructor cap |
| H5-3 | slow client evicted, peer healthy — retained today | route delivery through a shared pool again |
| H6-1/2 | claim test red against the current ledger | flip a claim digest; flip a row back to LIVE |
| H6-3 | (the guard itself, demonstrated red first) | — |

## 7. Rollout and evidence plan

- **Order:** H1 → H2 → H3 → H4 → H5 → H6. H1 is the money path and needs no deployment; H3's deck
  changes are offline (production deploy stays unproven per FACT-001 — the stack is not deployed
  to prove this fix here).
- **Commits:** one region-sliced commit per finding (shared hunks ship together: H1-2+H1-4 are one
  commit; H1-1, H1-3, H1-5 and each H2/H3/H4/H5/H6 item their own), one CHG record per commit
  under `docs/05_deployment/change-records/`, citing the failing-first test and the mutation.
- **Gate absorption:** every new Python test lives in `code/01_platform/04_scripts/tests/test_*.py`
  and joins gate step 3 automatically; no numbered step is added (`GATE_TOTAL=19`). Deck tests are
  YAML-only (no docker needed).
- **Per-area verification:** executor `cargo test --offline` (+ `--features paper`) and the gateway
  module suite (per-dossier — `make test` covers only common + ingestion); ingestion `make test`
  plus the new failure-matrix tests; compute/mock module suites; scripts via
  `python3 -m pytest code/01_platform/04_scripts/tests -q` from the repo root. Finish with
  `make gate` + `make static-check`.
- **Smoke-before-run:** no item needs a run longer than 5 minutes; the H1-3 durable restart smoke
  is a short container start with a read-only mount (must exit non-zero) and a restart (JSONL
  preserved). If any check grows past 5 minutes, the smoke rule applies before it.
- **Evidence:** CHG files carry command + result; H5-1 adds a checkpoint-size evidence record under
  `logs/`; H2-3's E2E runs against the fake bridge locally.
- **Never edit code or touch the cluster while a gate run is in flight.**

## 8. Risks and rollback

- **H1:** Unknown now halts loudly — operators must reconcile rather than assume; that is the
  required posture. Enabling durable attempts can make a previously silent re-forward visible as
  a 503 while halted — intended. Rollback per region is a revert; durable JSONL needs no
  migration.
- **H2:** the startup burst stays quarantined (its documented intent); quarantine volume rises
  briefly. A sustained overload now exits and is revived by `on-failure` — intended fail-closed.
  The token check stays warn-only, so availability is untouched.
- **H3:** deck changes are inert until a deploy; rollback restores the current (broken) parity
  only if deliberately reverted. The retired alerts were already unable to fire, so the safety
  loss is nominal; the replacement position alert is explicitly deferred on the executor OTLP
  dependency, not silently assumed.
- **H4:** an offline host now honestly reports skips instead of a false certificate; `audit_r2`
  rc 1 on FAIL is the intended contract. The VM refusal on an old golden image is visible at
  morning start with the exact fix (add the two lines).
- **H5:** old checkpoints restore unchanged; a rollback to the old binary loses only the
  post-upgrade dedup window (duplicates then collide on the candidates KV PK — documented, not
  hidden). Timing assertions use aggregate measurements with slack.
- **H6:** docs-only; `env_facts.py` keeps `.bak`; the new test is hermetic (no docker/network).

## 9. Operator decisions — recorded 2026-09-28

All noted defaults follow the operator standard (2026-09-28): **low latency · high throughput ·
correctness · low memory · native · no future maintenance or firefighting**. Where the criteria
conflict, correctness wins and the measured steady-state cost decides.

| # | Decision | Ruling | Effect on this plan |
|---|---|---|---|
| D1 | **H4-2** — daily VM universe | **Keep dev + full** (operator: the daily VM intentionally runs the dev multi-socket universe) | H4-2 revised: the mode is recorded explicitly in `.env.vm.example` + the VM guide; the planned production-posture refusals and golden pins are dropped; no behavior change |
| D2 | **H3-3** — dead Position_State alerts | **Retire** (as recommended) | unchanged: delete the 3 rules + corpus, mark RB-POS-001 retired; producer-existence guard added |
| D3 | H1-2/H1-4 — Unknown handling | immediate process-wide halt + durable HALT (default noted) | unchanged |
| D4 | H1-3 — durable flag window | flip after H1-2/H1-4, during a halted window (default noted) | unchanged |
| D5 | H1-5 — fill version encoding | composite `receiveTime·1e6 + hash(postbackEventId)` now (default noted) | unchanged |
| D6 | H1-1 — halt consumer cadence | 1 s full-scan replay (default noted) | unchanged |

## 10. Out of scope

- The three Critical findings (separate plan, landing first).
- Every Medium/Low row of `logs/full-project-audit-20260928/REPORT.md` (later waves).
- The 66 upstream `go-arrow` issues, production-VM/Dokploy work, 4-VM Swarm capacity items, and
  live-money enablement.
- The dead position-state **dashboard** (tracked in `2026-09-17-compute-identifier-parity.md`).
- Applying or changing any DDL; the Fills v3 `source_sequence` decision.
- Proving a production deploy from this host (FACT-001 boundary) — this wave changes decks and
  tests them statically; the real-VM proof belongs to the H2-5 S4 rehearsal.
