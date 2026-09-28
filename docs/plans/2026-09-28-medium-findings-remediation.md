# Medium findings remediation — audit 2026-09-28 (scope doc)

**Created:** 2026-09-28 · **Status:** proposed — awaiting operator approval and the two `[?]`
decisions (§9); no code changed by this doc.
**Source:** `logs/full-project-audit-20260928/REPORT.md` — every Medium row (the 26 `- M:` bullets,
the `/v1/intents` money-path note, and the five docs-vs-reality contradictions). 32 report rows →
**31 distinct items** (the Common `proto control_version` row duplicates the ingestion row and is
merged).
**Related:** Critical `docs/plans/2026-09-28-critical-findings-remediation.md` (C1–C3) and High
`docs/plans/2026-09-28-high-findings-remediation.md` (H1–H6) land first and separately; this doc
composes with them and never duplicates them.
**Execution protocol:** `docs/plans/2026-09-24-plan-execution-protocol.md` (verify-first, marker
discipline, failing-first tests, one CHG per commit, smoke-before-run, tracker hygiene).
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
| M1 — execution follow-ups (the layer under H1) | 6 | 5 | 0 | 1 | 0 | 0 | 0 |
| M2 — ops tooling truth | 5 | 0 | 0 | 5 | 0 | 0 | 0 |
| M3 — compute correctness and configuration | 4 | 0 | 0 | 4 | 0 | 0 | 0 |
| M4 — ingestion gates and accounting | 6 | 0 | 0 | 6 | 0 | 0 | 0 |
| M5 — DDL/mock parity pins | 2 | 0 | 0 | 2 | 0 | 0 | 0 |
| M6 — infra/DDL/rehearsal parity | 3 | 0 | 0 | 3 | 0 | 0 | 0 |
| M7 — docs currency and its guard | 5 | 0 | 0 | 5 | 0 | 0 | 0 |
| **Total** | **31** | **5** | **0** | **26** | **0** | **0** | **0** |

#### M1 — execution follow-ups (the layer under H1)

- [x] **M1-1** — `/v1/intents` re-verifies the adopted gate identity **inside the forwarder lock,
  before `claim_for_send`**: `recheck_send(&Envelope)` re-reads the in-memory snapshot; HALTED →
  503; when hydrated, epoch/fence/lease mismatch → 409 + `safety_halt`; foreign partition → 403
  `WRONG_PARTITION` (metric, no halt — a foreign sender must not DoS the gate). (E1; contract
  07:105-112.) — CHG-375
- [x] **M1-2** — UNKNOWN escalation reports the durable halt: extract `report_durable_halt` from
  `safety_halt` and call it from `escalate_unknown_review` after the 15 s reconcile window (keep
  `operator_review_required` and the epoch bump; add `unknown_escalated` metric). (E2.) — CHG-375
- [ ] **M1-3** — One shared report flow: a process-wide `BridgeSession` built once in `main.rs`;
  the single `take_reports` dispatcher routes route-owned refs to the route handler (booked once +
  gateway `/v1/events`), node-owned refs to the node channel, and unknown refs to one-place halt.
  On socket drop: `missed_window` counter + Tier-11 reconciler on reconnect — never a fake replay
  buffer. (E3.)
- [x] **M1-4** — Fill idempotence: per-order `seen_fills` (cap 64 FIFO) keyed by
  `postback_event_id`; a duplicate is counted (`fill_duplicate`) and returned before booking;
  empty id keeps today's path plus a `fill_without_postback_id` counter. (E4.) — CHG-376
- [x] **M1-5** — Action-scoped durable claim: key becomes `(instruction_id, action)` with
  `action = cmd_env.command`; same action + same hash → `Duplicate` (409, no send); same action +
  different hash → `ContractViolation` (halt); a different action claims fresh. Journal gains an
  additive `action` field. **Precondition for H1-3's flag flip.** (E5.) — CHG-371
- [x] **M1-6** — `Order_Lifecycle` goes through the versioned projector: pooled lookup +
  `OrderLifecycleProjector.apply` (APPLIED → upsert; DUPLICATE/STALE → no-op; CONFLICT/REGRESSION/
  UNKNOWN → quarantine + partition halt), per-key serialization; the producer's `sourceVersion`
  becomes the H1-5-shaped `eventTs·1e6 + hash(postback_event_id) mod 1e6`. (E6.) — CHG-377

#### M2 — ops tooling truth

- [ ] **M2-1** — One provisioning entry point: new `provision-observability.sh` refuses without
  `secrets.env`, sources `.env` + `secrets.env`, derives `O2_AUTH_BASIC` from `O2_PASSWORD` when
  absent, then runs `o2-provision.py` and `seed_alerts.py` under `set -e`; the daily guide's step 4
  becomes that script and the guide joins the docs-audit runbook set. (O1.)
- [ ] **M2-2** — EOD catch-up and honest heartbeat: durable state records the passed slot and
  outcome; a missed/failed 15:45 slot is retried (catch-up at start, retry every
  `EOD_RETRY_DELAY_SEC` within the trading day); `--check-heartbeat --last-run` fails when the
  latest passed slot has no success stamp (scheduler-dead vs not-archived distinguished).
  (O2; DECIDED 2026-09-28 — catch-up run + same-day retries.)
- [ ] **M2-3** — `o2-provision.py` stops treating unreadable as empty: a non-200/undecodable list
  read is fatal **before any POST**; stream-not-found POSTs are counted `deferred` (exit 0);
  every other POST, destination, or retention failure is fatal; `main(argv) -> int`. (O3.)
- [ ] **M2-4** — `make up` fails closed when the catalog guard fails
  (`|| { echo …; exit 1; }`), no longer swallowing the verdict with `|| echo "!!!"`. (O4.)
- [ ] **M2-5** — `day_run` probes fail closed: `EnvProbe(values, failures)`, `log_errors`/
  `effective_tokens` raise `ProbeUnavailable`, facts record probe failures, and I6/I7/I8 require
  “not failed” (amber, not green). Lands with/after C2-1 (same function). (O5.)

#### M3 — compute correctness and configuration

- [ ] **M3-1** — `ALLOWED_LATENESS_MS` wired into the aggregator: a `allowedLatenessMs` field +
  canonical 5-arg constructor + `SignalJobConfig.DEFAULT_ALLOWED_LATENESS_MS = 5_000L`; the three
  hardcoded `5_000L` sites (eviction, lateness gate, emitted-map bound) use the field;
  `SignalJob` passes `config.allowedLatenessMs()`. (C1.)
- [ ] **M3-2** — Windows close only on event time: the boundary branch in `onTimer` is gated on
  `domain == TimeDomain.EVENT_TIME`, so a processing-time fire can never truncate a candle
  (~1/15 000 per slot start today); finalization always waits for the watermark. (C2.)
- [ ] **M3-3** — Flag coherence fail-closed: `STRATEGY_HOST_ENABLED=true` requires
  `MULTITF_ENABLED=true` (throw at config load, mirroring `FEATURE_LAYER_ENABLED`); the
  strategies-set-but-host-off warning is hoisted out of the multiTf branch with both flags named.
  (C3.)
- [ ] **M3-4** — One watermark truth: keep the code default **500 ms**, update the six doc
  locations that say 5 s, append DEC-058 (default, rationale, real-feed revisit trigger), record
  the conflict resolution in `01-foundation.md`; a doc-parity test pins the config defaults to the
  dossier/contract. (C4; DECIDED 2026-09-28 — keep 500 ms, fix the docs.)

#### M4 — ingestion gates and accounting

- [ ] **M4-1** — SCH-22 becomes a real, requirement-backed gate: delete the uncalled
  `isManifestApproved`; add `INSTRUMENT_MANIFEST_MIN_COUNT` (default 1 = no dev regression; pin
  1024 in the production profiles and the daily VM profile — the VM reads the real feed)
  enforced at startup (below minimum = FATAL); document
  subscription as unconditional on a parsable, non-empty CSV meeting the minimum. (I1.)
- [ ] **M4-2** — Control-record contract version validated: one `CONTROL_CONTRACT_VERSION = 2`;
  unknown/missing/0 on the wire → quarantine `INVALID_SCHEMA` +
  `CONTROL_VERSION_MISMATCH` + return — never processed as v2. (I2.)
- [ ] **M4-3** — Queue listener wired: `queues[i].setListener` for every queue; a new `RESUMED`
  level with episode hysteresis; 80% → readiness false (`HealthProbe` queue dimension); 100% →
  readiness false + the H2-2 shared handler exactly once. (I3.)
- [ ] **M4-4** — Quarantine no longer stalls the reader: `AsyncQuarantineSink` decorator
  (bounded queue, default 4096, one daemon writer); `write()` is a non-blocking offer; overflow →
  the H2-2 shared handler (`QUARANTINE_OVERFLOW`); `close()` drains and counts. (I4.)
- [ ] **M4-5** — Startup grace retries the whole window: drop `lastAppendSuccessEpochMs == 0L`;
  metadata-not-ready retries inside the bounded grace regardless of an intervening ack; expiry
  FATAL, other classes immediate; dossier updated, CHG-326 clause marked superseded. (I5.)
- [ ] **M4-6** — Payload hashes verified always-on: a byte[] `PayloadHashValidator` path at the
  earliest tick admission point (missing → `MISSING_PAYLOAD_HASH` quarantine; mismatch →
  `HASH_MISMATCH` quarantine + metric; never append); delete the
  `INGEST_VALIDATE_PAYLOAD_HASH` flag (integrity is not optional). (I6.)

#### M5 — DDL/mock parity pins

- [ ] **M5-1** — `Signal_Candidates` row version is `"2"` everywhere: fix the DDL headers/prose
  (05 → 2 with the table-kind-v3 note; 23 → 2), regenerate the manifest offline (one regeneration
  shared with M6-1), and pin Java constant == both DDL headers == both domain lines == both
  manifest entries. (CM1.)
- [ ] **M5-2** — Mock broker speaks the canonical decoded-tick dialect: rebind the emitted keys to
  proto `TickEvent`/Go `Tick` names (`token`, `ts_ms`, `ltp_paise`, 5-element ladders, …; drop
  `change_pct`, nested `depth_*`, `ohlc_*`), keep framing/pacing; add a committed
  `code/testdata/mock-tick-sample.json` consumed by a strict Go decode test and a Python key-set
  pin. Lands after H5-2/H5-3. (CM2.)

#### M6 — infra/DDL/rehearsal parity

- [ ] **M6-1** — WITH-block parser stops truncating: strip `--` comments (outside quotes) and scan
  to the balancing close paren; fail-closed emission refuses a manifest when
  `'table.datalake.enabled'` is present but parsed away; regenerate (only `raw_table_1.lake_policy`
  changes to enabled) and add a DDL↔manifest parity test for every entry. (N1.)
- [ ] **M6-2** — O2 digest enforced end to end: `image-publish.sh --merge-env` overlays every
  `${X_IMAGE:?}` the stack demands from `runtime.lock` (missing/bare in lock = error);
  `deploy_preflight` requires deploy-env `*_IMAGE` to equal the lock ref; pin-check extends to
  every demanded `*_IMAGE`. One certifying `docker pull` at landing; digest failure = STOP, never
  invent one. (N2.)
- [ ] **M6-3** — p10 rehearsal remaps all base ports: otel `14317/14318`, OpenObserve
  `15080/15081`, MinIO `19000/19001` via `!override`; a guard proves no base-published port is
  left unremapped and the rehearsal stack cannot collide with base or prod ports (supersede
  FACT-017 append-only). (N3.)

#### M7 — docs currency and its guard

- [ ] **M7-1** — `00-start-here.md`: retitle the 2026-08-21 readiness snapshot as historical,
  26 → 27 tables and 26/26 → 27/27, 13/13 → 19/19, remove the retired 15 s-candle era and Action
  Capture rows, fix the dead `ddl/03_feature_candles_15s.sql` reference to `32_/33_`. (D1.)
- [ ] **M7-2** — `live-readiness-unified-plan.md`: additive supersession banner (Fluss 1.0.0 per
  the 2026-09-22 upgrade plan, gate 19 steps, U-row checkboxes stay dated evidence). (D2.)
- [ ] **M7-3** — `02-environments.md`: remove the three Action Capture mentions, note capture runs
  in the Execution Core (`03_action_capture` retired 2026-09-10). (D3.)
- [ ] **M7-4** — `version_matrix.yaml`: header says `versions.pin` pins Flink 2.2.1 / Fluss
  1.0.0 (no `TO_BE_PINNED`); no row edits. (D4.)
- [ ] **M7-5** — `2026-09-23-design-vocabulary-cleanup.md`: status → EXECUTED with the four
  commits and the green guard; plus the cross-cutting currency guard: docs-audit C1 leg (readiness
  table vs manifest), C5 leg (retired-live phrases with banner exemption), C7 leg (matrix claim vs
  `versions.pin`), and gate-discovered `tests/test_doc_currency_claims.py` (gate count, dead DDL
  refs, snapshot-sovereignty). (D5 + cross-cutting.)

## Overview

This is a seven-region wave. Every region fixes one boundary and ships one permanent guard:

| Group | Boundary that failed | Fix boundary | Permanent guard |
|---|---|---|---|
| M1 | the executor's send, halt-report and fill-book boundaries each had a blind spot | re-check before send, durable halt report, one report flow, one identity per fill/action/lifecycle event | Rust + gateway tests per boundary; one version formula family |
| M2 | ops evidence/tooling could report success while lying or failing open | evidence required, skips/errors counted, fail-closed probes, catch-up | Makefile/day_run/o2/EOD tests |
| M3 | compute config and timers drifted from their contract | one config path, event-time-only closes, coherent flags, one watermark truth | aggregator behavior tests + config/doc parity tests |
| M4 | ingestion gates existed but were unwired or dead | gates wired, drops counted, hashes verified, versions validated | failure-matrix + queue/quarantine tests |
| M5 | DDL and mock dialects drifted from their writers/parsers | one row-version truth, mock speaks the canonical dialect | Java + Python + Go pinned tests |
| M6 | DDL parser truncation and image/port pins were unenforced | parser fixed fail-closed, one render path for images, all ports remapped | manifest parity + pin-check + p10 port guard |
| M7 | docs tables/headers kept claims later changes invalidated | correct the five docs, add currency legs to docs-audit | docs-audit legs + `test_doc_currency_claims.py` |

## 1. Verify-first (reproduced on `5b4725d2`)

All 31 items were re-verified against the working tree by seven read-only design agents (the
load-bearing rows were re-checked again by hand). Every verdict is REPRODUCES.

| Report row | Plan item | Verdict | Evidence (abbreviated) |
|---|---|---|---|
| money-path note | M1-1 | REPRODUCES | `/v1/intents` verifies MAC and one gate snapshot (`http.rs:971-993`); envelope `gate_epoch`/`fence_token`/`execution_partition_id` are MAC inputs only, never compared. |
| Exec M | M1-2 | REPRODUCES | `escalate_unknown_review` halts locally + bumps epoch (`http.rs:582-605`) but never calls the reporter; the lease stops only via the local gate. |
| Exec M | M1-3 | REPRODUCES | `main.rs:74` builds a second `HttpBridgeClient`; only `client.rs:905` calls `take_reports`; route refs never registered for route envelopes → uncorrelated halt. |
| Exec M | M1-4 | REPRODUCES | `handle_fill` books `+= signed` unconditionally; `postback_event_id` is only a TradeId fallback; no seen-set. |
| Exec M | M1-5 | REPRODUCES | Claim key is `instruction_id` only; cancel/amend reuse it with different payloads → `ContractViolation` halt when the guard is ON (tests place+cancel without `.with_attempts`). |
| Exec M | M1-6 | REPRODUCES | `FlussProjectionWriter` blind-upserts `Order_Lifecycle`; `events.rs:99` hardcodes `"sourceVersion": 1`; the versioned projector is unused. |
| Ops M | M2-1 | REPRODUCES | Guide step 4 runs both scripts bare; `o2-provision.py:39-42` exits 2 without `O2_AUTH_BASIC`; the guide is outside the runbook audit set. |
| Ops M | M2-2 | REPRODUCES | `write_heartbeat` runs unconditionally after a failed controller; `next_fire(now)` is recomputed every loop → missed slot silently skipped. |
| Ops M | M2-3 | REPRODUCES | Alert/destination list reads discard status → empty list → re-POST everything; retention failures printed, exit aggregates only dashboard failures. |
| Ops M | M2-4 | REPRODUCES | `Makefile:168-169` `catalog-guard.sh || echo "!!!"` — `echo`'s status wins. |
| Ops M | M2-5 | REPRODUCES | `container_env` swallows probe exceptions → `{}`; `log_errors` returns `[]` on error; I6/I7/I8 pass unverified. |
| Compute M | M3-1 | REPRODUCES | Three hardcoded `5_000L` sites (`:287,458,686`); the 4-arg constructor never receives `config.allowedLatenessMs()`. |
| Compute M | M3-2 | REPRODUCES | `onTimer` computes `TimeDomain` at `:710` but the boundary loop (`:787-859`) is not gated on it; a processing-time fire closes a window. |
| Compute M | M3-3 | REPRODUCES | Only host→strategies and feature→host are validated; host-on/multitf-off wires nothing; the warning is nested inside the disabled branch. |
| Compute M | M3-4 | REPRODUCES | Code default 500 ms pinned by test; six docs say 5 s; no DEC records the change. |
| Ingestion M | M4-1 | REPRODUCES | `isManifestApproved` has zero call sites; any non-empty parse is `approved=true`. |
| Ingestion M | M4-2 | REPRODUCES | `handleControlRecord` passes `BridgeEvent.CONTRACT_VERSION` and never reads `cr.getContractVersion()`. |
| Ingestion M | M4-3 | REPRODUCES | `BoundedQueue.setListener` exists; only `tracker.setListener` is wired; the queue budget never touches readiness. |
| Ingestion M | M4-4 | REPRODUCES | `QuarantineWriter.write` calls `observe(writer.append(row))` on the reader thread with a 30 s pool wait. |
| Ingestion M | M4-5 | REPRODUCES | `RawTickWriter.java:295` gates metadata retry on `lastAppendSuccessEpochMs == 0L`; docs/CHG disagree. |
| Ingestion M | M4-6 | REPRODUCES | `INGEST_VALIDATE_PAYLOAD_HASH` is parsed/exported only; `PayloadHashValidator` has no production caller. |
| Common M | M5-1 | REPRODUCES | DDL 05 header/domain say 3; writer constant is `"2"`; DDL 23 says 1; manifest entries say 3 / 1. |
| Common M | M5-2 | REPRODUCES | Mock emits `instrument_token`/`exchange_ts`/`last_price_paise`/`ohlc_*`/nested `depth_*`; no in-repo consumer; tests pin the bad keys. |
| Infra M | M6-1 | REPRODUCES | Non-greedy WITH regex stops at `);` in a comment (`02_raw_table_1.sql:158`); manifest `raw_table_1.lake_policy = "off"`. |
| Infra M | M6-2 | REPRODUCES | `.env.example:26` bare O2 tag while `runtime.lock:32` pins the digest; pin-check checks lock shape only; preflight exempts upstreams. |
| Infra M | M6-3 | REPRODUCES | p10 overlay remaps only ZK/Fluss/Flink; base otel/O2/MinIO ports (`4317/4318`, `5080/5081`, `9000/9001`) still collide. |
| Docs | M7-1 | REPRODUCES | `00-start-here.md:77` "Current readiness — 2026-08-21"; 26 tables; 13/13; `feature_candles_15s`; Action Capture; dead DDL ref. |
| Docs | M7-2 | REPRODUCES | Plan still titled "Execution contract"; Fluss 0.9.1 at `:74,98,170,382`; no banner. |
| Docs | M7-3 | REPRODUCES | `02-environments.md:32,68,84` list Action Capture; compose has no such service. |
| Docs | M7-4 | REPRODUCES | `version_matrix.yaml:11-12` claims `TO_BE_PINNED`; `versions.pin:18-19` = 2.2.1 / 1.0.0. |
| Docs | M7-5 | REPRODUCES | Plan status says "Nothing executed yet"; the guard file + four commits exist on main. |

## 2. Root cause (one sentence each, grouped)

- **M1** — the executor's money-path boundaries were each authorized once at entry and never
  re-checked at the moment of action: send identity from a stale snapshot, halt escalation without
  the durable report, report intake split from the routed orders, fill identity unused for
  idempotence, claim identity scoped to the instruction instead of the action, and lifecycle
  writes bypassing the version gate.
- **M2** — ops tools encode failure as a benign default (empty list, `[]`, `{}`, echo status) or
  forget the passed slot, so evidence and exit codes can be green while the day is lost.
- **M3** — compute configuration was added at the edges while the aggregator kept its own
  literals; the timer domain was not considered; flag coherence was checked one way; and the
  documented watermark default was never updated when the code changed.
- **M4** — the ingestion safety API existed but had no consumer (manifest approval, queue
  listener, payload hash), the control-version gate was dropped in the proto migration, and
  quarantine was left synchronous on the reader thread.
- **M5** — the DDL header conflated the table-kind version with the row version and no guard
  pinned the row version; the mock's dialect was never rebound after its only consumer was
  removed.
- **M6** — the WITH parser matches text without stripping comments and emits without checking the
  DDL literal; digest delivery was split so locked-but-unpublished images were never carried; the
  p10 overlay grew service-by-service with no "all ports remapped" test.
- **M7** — dated snapshots stayed titled "Current", successors never banner-marked their
  predecessors, and no check binds a doc table/header claim to the file that is its truth.

## 3. Recommended design (native; no new dependency)

### M1 — execution follow-ups

1. **M1-1 re-check before send (E1).** `ServerState::recheck_send(&Envelope)` called inside the
   forwarder lock **before** `claim_for_send` (so a refused request leaves no durable attempt):
   re-read the in-memory snapshot; gate ≠ Enabled → 503 `GATE_HALTED`; when the reporter is armed
   (`gate_hydrated`), require `envelope.gate_epoch == control_epoch`, `envelope.fence_token ==
   fence_token`, and a live lease — else 409 `STALE_EPOCH`/`FENCE_MISMATCH` + `safety_halt`;
   partition mismatch → 403 `WRONG_PARTITION` + metric, no halt. Flag-off/paper keeps the gate-only
   path when no partition is configured. (The halt cannot take the async forwarder lock — that
   residual race is closed by H1-2's notifier and H1-4 at the transport boundary.)
2. **M1-2 durable UNKNOWN halt (E2).** Extract the report spawn from `safety_halt` into
   `report_durable_halt(reason)`; call it from `escalate_unknown_review` after the 15 s window
   (keep `operator_review_required` and the epoch bump). The durable gate then reads HALTED within
   the report cadence instead of waiting for the ≤ 30 s lease.
3. **M1-3 one report flow (E3).** Extract report intake into a process-wide `BridgeSession` built
   once in `main.rs`; exactly one `take_reports` dispatcher owns the single WS subscription. Route
   relations live in a shared registry: route-owned ref → parity update + normalized gateway event
   (C1's `event_type` dispatch); node-owned → the node's channel; unknown → one-place halt. No
   replay buffer is invented (the bridge has no cursor); on socket drop the dispatcher records
   `missed_window` and the existing Tier-11 reconciler runs on reconnect.
4. **M1-4 fill idempotence (E4).** `seen_fills: <order, capped 64 FIFO>` keyed by
   `postback_event_id`; duplicate → `fill_duplicate` metric + warn + return (no parity change, no
   event, no halt). Empty id keeps today's path + `fill_without_postback_id` (C1 makes the bridge
   id mandatory). Never synthesize a key from qty/price — that would suppress legitimate partials.
5. **M1-5 action-scoped claim (E5).** `try_claim(attempt_id, instruction_id, action, hash,
   client_order_ref)` with `action = cmd_env.command`; index `(instruction_id, action)`: same
   action + same hash → `Duplicate` (409, no send); same action + different hash →
   `ContractViolation` (halt); different action → fresh claim. Journal rows gain an additive
   `action` field. **This must land before H1-3's durable flag flip.**
6. **M1-6 versioned lifecycle (E6).** `writeLifecycle` reads the stored row (pooled Lookuper,
   same pattern as the control-state store), applies `OrderLifecycleProjector`: APPLIED → upsert;
   DUPLICATE/STALE → no-op (STALE audited); CONFLICT/REGRESSION/UNKNOWN → quarantine + halt the
   partition (epoch+1). Serialize per composite key so read-evaluate-write is not lost-update; a
   lookup failure is 503, never a blind upsert. Producer version uses the same formula family as
   H1-5 with `received_ts_ms`.

### M2 — ops tooling

1. **M2-1 one provisioning path (O1).** New `code/01_platform/04_scripts/provision-observability.sh`:
   resolve paths; exit 1 (naming guide step 2) when `secrets.env` is missing; source `.env` and
   `secrets.env`; require `O2_PASSWORD`; derive `O2_AUTH_BASIC` with base64 when absent; run
   `o2-provision.py` then `seed_alerts.py` under `set -e`. Guide step 4 calls exactly this script;
   the guide joins `runbook_docs()` and its shell blocks lose `<>` placeholders.
2. **M2-2 EOD catch-up (O2; decided 2026-09-28 — catch-up).** Durable `EOD_STATE_FILE`
   (`{slot_date, last_attempt_at, rc, missed, catch_up}`). At loop start and after recomputing
   `next_fire`: if the latest slot ≤ now has no success and the trading-day boundary has not
   passed → run the existing `fire()` once, mark `missed=true`, log loudly. After a failed fire,
   retry every `EOD_RETRY_DELAY_SEC` (default 900) inside the day. `--check-heartbeat` with
   `--last-run` fails when the latest passed slot has no success stamp, distinguishing
   "scheduler dead" from "EOD not archived"; without `--last-run` it stays pure liveness (stack
   healthcheck unchanged). Retries are idempotent (controller lease/state).
3. **M2-3 o2-provision fail-closed (O3).** Each provisioner returns failure counts; a non-200 or
   undecodable list read is fatal before any POST (zero writes); `400/404 stream-not-found` POSTs
   count `deferred` and keep exit 0; any other POST failure, destination failure, or retention PUT
   failure is fatal; `main(argv) -> int`, 1 on any fatal, all evidence printed.
4. **M2-4 make up fail-closed (O4).** Replace `|| echo "!!!"` with
   `|| { echo "!!! catalog-guard: …"; exit 1; }`; the guard's own exits propagate.
5. **M2-5 day_run probes fail closed (O5).** `EnvProbe(values, failures)` (a failed inspect of a
   running service = failure; empty `ps -q` = not running, not a failure); `log_errors` and
   `effective_tokens` raise `ProbeUnavailable`; facts carry `env_probe_failures`,
   `log_scan_failed`, `manifest_probe_failed`; I6/I7/I8 require “not failed” and print the probe
   detail. Lands with/after C2-1 (same function).

### M3 — compute

1. **M3-1 lateness plumbing (C1).** Field + 5-arg canonical constructor + one public
   `DEFAULT_ALLOWED_LATENESS_MS = 5_000L`; the three literals become the field; `SignalJob` passes
   `config.allowedLatenessMs()`. Default behavior unchanged; env overrides now actually work.
2. **M3-2 event-time-only closes (C2).** Wrap the boundary handling (from the pending/forming loop
   to the end) in `if (domain == TimeDomain.EVENT_TIME)`. The session-close path is already
   event-time; live snapshot behavior unchanged. Honest behavior change: on the rare aligned
   processing-time fire the window no longer closes early; validation now always waits for the
   watermark (≤ configured out-of-orderness).
3. **M3-3 flag coherence (C3).** Throw when `STRATEGY_HOST_ENABLED=true && !MULTITF_ENABLED`
   (message names both flags and why no branch would be wired); hoist the
   strategies-set-but-host-off warning out of the multiTf branch. A previously useless deployment
   now fails at boot with the fix in the message.
4. **M3-4 watermark truth (C4; decided 2026-09-28 — keep 500 ms).** Keep 500 ms (freshest deliberate change + code/test
   pin); update dossier/contract/architecture/requirements/configuration-audit to 500 ms with the
   measured rationale; append DEC-058 and the foundation conflict note; add a doc-parity test.

### M4 — ingestion

1. **M4-1 manifest minimum (I1).** Delete `isManifestApproved`; enforce
   `INSTRUMENT_MANIFEST_MIN_COUNT` (default 1; pin 1024 in the production profiles and the daily
   VM profile, which reads the real feed) immediately
   after the empty check — below minimum is FATAL with the effective numbers. Subscription is
   documented as unconditional on a parsable, non-empty, minimum-meeting CSV; identity is bound by
   DEC-050 config + `SubscriptionPreflight` + H2-4's warn-only token check.
2. **M4-2 control version (I2).** `CONTROL_CONTRACT_VERSION = 2`; at the top of
   `handleControlRecord`: unknown/missing/0 → quarantine `INVALID_SCHEMA` +
   `CONTROL_VERSION_MISMATCH` + return. Nothing downstream (including slot health) sees a wrong
   version, so readiness cannot pass on it (fail-closed backstop).
3. **M4-3 queue listener (I3).** Wire every `queues[i].setListener`; add `Level.RESUMED` fired
   once when the queue crosses back below 80% (episode flags, fire after unlock). WARNING →
   per-queue blocked dimension in `HealthProbe.isReady()` + readiness file update; RESUMED →
   clear; CRITICAL → blocked + H2-2's shared handler exactly once per episode. Per-tick
   `offer()==false` routes into the same handler, so no drop is uncounted.
4. **M4-4 async quarantine (I4).** `AsyncQuarantineSink` wraps the existing writer behind a bounded
   `ArrayBlockingQueue` (4096) + one daemon thread; `write()` offers without blocking; overflow →
   `onQuarantineDrop` into H2-2's handler (`QUARANTINE_OVERFLOW`); `close()` drains under the
   existing budget and counts leftovers. Durability evidence stays the writer's ERROR rows +
   flush-on-close + the overflow journal record.
5. **M4-5 grace retry (I5).** Remove the `lastAppendSuccessEpochMs == 0L` conjunct; within the
   bounded grace the metadata-not-ready class retries regardless of an intervening ack, then FATAL
   at expiry; all other fatal classes stay immediate. Rationale: CHG-356 proved an ack does not
   mean the tablet finished replaying. Dossier updated; CHG-326's "only while no append was ever
   acked" clause marked superseded.
6. **M4-6 payload hash (I6).** Add a byte[] path to `PayloadHashValidator` (ThreadLocal digest,
   540 B bound) and verify at the earliest admission point: missing → `MISSING_PAYLOAD_HASH`,
   mismatch → `HASH_MISMATCH` quarantine + metric, never append. Delete the flag (integrity is not
   optional; a switch is a mode to remember). Cost ≈ 12 MB/s at 60k tps — inside budget.

### M5 — DDL/mock

1. **M5-1 row version 2 (CM1).** Fix DDL 05 (header + writer-set line → 2, table-kind v3 note)
   and DDL 23 (:31 → 2); regenerate the manifest offline (`--force`, never apply DDL) — share the
   single regeneration with M6-1; extend `SignalCurrentDdlContractTest` and add the Python pin
   (Java constant == both headers == both domain lines == both manifest entries). Do not touch
   `18_safety_halt_requests.sql` (already consistent at 3).
2. **M5-2 canonical mock dialect (CM2).** Rebind `tick.put` keys to proto `TickEvent` / Go `Tick`
   names (`feed`, `mode`, `token`, `ts_ms`, `received_ms`, `ltp_paise`, `close_paise`,
   `open_paise`, `high_paise`, `low_paise`, `ltq`, `volume`, `total_buy_qty`, `total_sell_qty`,
   five-element `bid_px/ask_px/bid_qty/ask_qty/bid_orders/ask_orders`); drop `change_pct`, nested
   `depth_*`, `ohlc_*`, legacy names. Framing/pacing untouched. Committed fixture
   `code/testdata/mock-tick-sample.json`; strict Go decode (`DisallowUnknownFields`) + Python
   key-set pin. Lands after H5-2/H5-3 (same file regions).

### M6 — infra

1. **M6-1 WITH parser (N1).** Strip `--` comments outside single quotes, scan to the balancing
   close paren before the final `;` (quote-aware) in one helper shared by the other parsers;
   fail-closed emission: if the stripped DDL contains `'table.datalake.enabled'` but the options
   lack it → raise (refuse to write the manifest). Regenerate; expect exactly
   `raw_table_1.lake_policy` to change (enabled/iceberg/5 min/auto-compaction); any wider diff =
   stop (a second truncation class).
2. **M6-2 image digest enforcement (N2).** `image-publish.sh --merge-env` overlays every
   `${X_IMAGE:?}` demanded by the stack from `runtime.lock` when stdin does not cover it; a
   demanded var missing/bare in the lock is exit 3. `deploy_preflight` requires each deploy-env
   `*_IMAGE` the lock pins to equal the lock ref. Pin-check `[5/6]` extends to every demanded
   `*_IMAGE`. One certifying `docker pull` of the recorded O2 digest at landing; failure = STOP —
   never invent or substitute.
3. **M6-3 p10 ports (N3).** Add `!override` remaps: otel `14317:4317`/`14318:4318`, OpenObserve
   `15080:5080`/`15081:5081`, MinIO `19000:9000`/`19001:9001`. Guard: every base port-bearing
   service appears in the overlay; effective p10 host ports are unique, `^1\d{4}$`, and disjoint
   from base **and** the production stack's published port; supersede FACT-017 append-only.

### M7 — docs

1. **M7-1** retitle `:77` as a historical snapshot; 26→27 tables, 26/26→27/27; 13/13→19/19 at
   `:86,:89`; replace the 15 s/KV and Action Capture cells; fix the dead DDL ref to
   `32_candle_live.sql`/`33_candle_closed.sql`. Keep dated evidence text untouched.
2. **M7-2** additive banner after `:6` of the live-readiness plan: superseded as active tracker;
   Fluss 1.0.0; gate 19 steps; U-row checkboxes remain dated 2026-08-25 evidence, never re-tick.
3. **M7-3** remove Action Capture from `:32,:68`; rewrite `:84`; add one clause that the capture
   path now runs in the Execution Core.
4. **M7-4** replace the `version_matrix.yaml` header claim with the true pins; no row edits.
5. **M7-5** mark the vocabulary plan EXECUTED with the four commits + green guard; extend
   `docs_audit.py` with the three currency legs (C1 readiness-table vs manifest, C5 retired-live
   phrases with banner/history exemptions, C7 matrix claim vs `versions.pin`) and add
   `tests/test_doc_currency_claims.py` (gate count vs `GATE_TOTAL`, dead DDL basenames, snapshot
   sovereignty). H6-3 binds the facts ledger to `runtime.lock`; these legs bind prose/tables to
   the tree — no overlap.

## 4. Permanent guards (the "will not come back" list)

| Item | Guard (test file + rule) |
|---|---|
| M1-1 | `http.rs` tests: stale epoch → 409/0 bridge calls; foreign partition → 403/0 calls; halt-before-send hook → 409 |
| M1-2 | `http.rs` test with a stub reporter: escalation emits HALT (allow still refused) |
| M1-3 | Rust test: exactly one WS dial; route postback booked once + one gateway event; unknown ref halts |
| M1-4 | `client.rs` test: same id twice → 10 + `fill_duplicate=1`; distinct ids → 20; empty id → counter |
| M1-5 | guarded tests: place→cancel→202, place→amend→202, same-payload retry → Duplicate, same action different payload → halt |
| M1-6 | `OrderLifecycleWriterGateTest`: stale ack after CANCELED → no write; conflict → quarantine + halt; Rust version-formula pin |
| M2-1 | `test_vm_golden_recipe.py` wrapper pin; new `test_o2_daily_provision.py`; `test_docs_audit_c18.py` guide-in-scope |
| M2-2 | `test_15_eod_schedule.py` `catch_up_due()` matrix + Popen catch-up; new `test_eod_catchup.py` (failure→retry→success; heartbeat red) |
| M2-3 | `test_o2_tooling_wave42.py`: GET 500 → rc≠0 + zero POSTs; 404 stream → rc 0 `deferred=1`; retention 500 → rc≠0 |
| M2-4 | `test_makefile_wave25.py` `UpTargetGuardTest`: static no-`|| echo` + behavioral guard exit 3 → rc≠0 |
| M2-5 | `test_day_run.py` failure-flag matrix (I6/I7/I8 amber on probe failure); optional `test_day_run_probe_failclosed.py` |
| M3-1 | `MultiTimeframeAggregateFunctionTest` lateness=0 eviction; static `test_compute_config_wiring.py` (no bare `5_000L`) |
| M3-2 | `processingTimeTimerNeverClosesWindow` (zero closed rows, then one candle after watermark) |
| M3-3 | `SignalJobConfigTest.hostRequiresMultiTf`; static `test_compute_flag_composition.py` (warn outside the branch) |
| M3-4 | Existing Java pin stays; new `test_compute_config_doc_parity.py` (defaults vs dossier/contract) |
| M4-1 | `ManifestLoadTest` min-count cases; startup-refusal test; `test_sch22_manifest_contract.py` (no uncalled exact check) |
| M4-2 | `ControlContractVersionTest` (v2 ok; v3/0/missing quarantined + metric + zero health mutation); `FailureMatrixTest` row |
| M4-3 | `ReadinessGatingMatrixTest` queue dimension; `QueueBackpressureReadinessTest`; `BoundedQueueTest` RESUMED |
| M4-4 | `AsyncQuarantineSinkTest` (write never blocks; overflow counted/journaled; close drains); ledger extended |
| M4-5 | `RawTickWriterStartupGraceTest` early-ack-then-failure retries; existing expiry/no-grace pins stay |
| M4-6 | `GoldenCorpusPayloadHashTest` proto path; `IngestionPayloadHashGateTest` (tampered → quarantine); flag-removal pin |
| M5-1 | `SignalCurrentDdlContractTest` header/domain == 2; `test_signal_schema_version_pin.py` (Java == DDL == manifest) |
| M5-2 | `MockArrowServerTest` key set == fixture; Go strict-decode test; `test_mock_tick_contract.py` (Java ⊆ proto) |
| M6-1 | `test_schema_manifest_parity.py` (every entry recomputed); parser unit cases; truncation fixture; refusal case |
| M6-2 | `test_14_image_publish.py` merge case; `test_13_deploy_preflight.py` bare-vs-lock; `test_pin_check.py` coverage |
| M6-3 | `test_p10_rehearsal_ports.py` (coverage, uniqueness, no collision with base/prod) |
| M7-1..M7-4 | docs-audit C1/C5/C7 legs (below) |
| M7-5 | `tests/test_doc_currency_claims.py` (gate count, dead DDL refs, snapshot sovereignty) |

## 5. Code map (current lines, for the commit slices)

| Item | Files : lines |
|---|---|
| M1-1 | `04_executor/src/http.rs:390-446,971-1060`; `gateway_protocol.rs:27-40,95-104,257-326`; `config.rs:79,229`; `main.rs:127,187-200`; contract 07:105-112 |
| M1-2 | `http.rs:107-112,319-326,390-446,511-533,558-605`; `gate_report.rs:283` precedent |
| M1-3 | `main.rs:64-76,133-148,187-200`; `engine.rs:318-348`; `execution/client.rs:556-613,903-910`; `transport.rs:534-557,654-675`; `server.go:402-514`; `intent.rs:33-50,60-106` |
| M1-4 | `execution/client.rs:179-188,226-228,632-686`; `fake.rs:466-486`; `postback.go:255-283` |
| M1-5 | `http.rs:825-836,890-918,1013-1027,1048-1056`; `executiongate.rs:99-140`; `durable_file.rs:401-463`; `intent.rs:52-106`; contract 07:42-47,83-90 |
| M1-6 | `gateway/FlussProjectionWriter.java:74-87,207-213`; `ProjectionApplier.java:43-107`; `events.rs:48-53,80-116`; `OrderLifecycleProjector.java:74-160`; `09_order_lifecycle.sql:26-33` |
| M2-1 | `CLOUDPE_DAILY_VM.md:100-109,133`; `docs_audit.py:1225-1230,1337-1403`; `o2-provision.py:39-42`; `seed_alerts.py:80-86` |
| M2-2 | `eod_schedule.py:78-85,97-151,168-238`; `vm-golden-build.sh:168-191`; `docker-stack.yml:1360-1371` |
| M2-3 | `o2-provision.py:39-42,1347-1379,1636-1662,1802-1926` |
| M2-4 | `Makefile:149-169` |
| M2-5 | `day_run.py:142-155,267-281,452-516,776-791,830-863,872-899` |
| M3-1 | `MultiTimeframeAggregateFunction.java:85-111,156-176,287,458,686`; `SignalJobConfig.java:66,1277-1282`; `SignalJob.java:293-296` |
| M3-2 | `MultiTimeframeAggregateFunction.java:710-713,730-736,787-859` |
| M3-3 | `SignalJobConfig.java:157,208-223`; `SignalJob.java:285-297,326,392-396` |
| M3-4 | `SignalJobConfig.java:65,251,1277-1282`; `SignalJobConfigTest.java:52-58`; dossier `04-signal-job.md:370,795`; contract `03-compute.md:25`; `02-data-pipeline.md:50`; requirements `03-compute.md:29,90`; configuration-audit `:21,170` |
| M4-1 | `InstrumentManifestLoader.java:54-111,127-247,292-333`; `IngestionService.java:427-443`; `IngestionConfig.java:257`; `day_run.py:963-970` |
| M4-2 | `proto/market_data.proto:137-160`; `IngestionService.java:1023-1086,1368-1379`; `BridgeEvent.java:21,30`; `ingestion-ndjson-schema.md:14-19,216-222` |
| M4-3 | `BoundedQueue.java:29-50,82-84,135-189`; `IngestionService.java:292-303,1318-1320`; `HealthProbe.java:206-240` |
| M4-4 | `QuarantineWriter.java:98-165,205-220`; `QuarantineSink.java`; `IngestionService.java:319-325,1054-1067` |
| M4-5 | `RawTickWriter.java:73,240,287-315,387-411`; `IngestionService.java:599-634,2148-2185`; dossier `:92`; `CHG-326.md:36` |
| M4-6 | `IngestionConfig.java:71,133,257,390,692`; `ConfigKeys.java:92`; `PayloadHashValidator.java:18-140`; `IngestionService.java:1244-1253`; `proto/market_data.proto:96-97` |
| M5-1 | `05_signal_candidates.sql:12,18`; `23_signal_candidates_current.sql:31`; `SignalCandidatesTableColumns.java:70`; `N7RangeBreakoutStrategy.java:385-386`; `CanonicalSignalFilterFunction.java:85-101`; `SignalCurrentDdlContractTest.java:44-120`; `schema_manifest.json` |
| M5-2 | `MockArrowServer.java:19-31,139-165,227-237`; `MockArrowServerTest.java:73-75`; `proto/market_data.proto:56-133`; `go-bridge/main.go:21-63`; `golden_corpus_test.go:29-41`; `run-monday-gates.sh:1099-1121` |
| M6-1 | `ddl_apply.py:165-179,207-219,293-329,547-610,669-729`; `schema_manifest.json:5-16`; `02_raw_table_1.sql:155-172` |
| M6-2 | `runtime.lock:32`; `.env.example:26`; `image-publish.sh:47-48,72-124`; `pin-check.sh:57-92`; `deploy_preflight.py:40-42,170-201`; `docker-compose.yml:1308`; `docker-stack.yml:1234` |
| M6-3 | `docker-compose.p10.yml:19-35`; base `docker-compose.yml:1262-1264,1313-1315,1365-1367`; `docker-stack.yml:1251`; `ENVIRONMENT.md:226-234` (FACT-017) |
| M7-1 | `00-start-here.md:77-124,197,207`; `docs_audit.py:143-183`; `run-monday-gates.sh:201` |
| M7-2 | `2026-08-25-live-readiness-unified-plan.md:3,74,98,170,382`; successor `2026-09-22-fluss-1.0-upgrade.md:3` |
| M7-3 | `02-environments.md:32,68,84`; `AGENTS.md:119`; `docker-compose.yml:977-1052` |
| M7-4 | `version_matrix.yaml:11-12`; `versions.pin:18-19`; `docs_audit.py:355-370` |
| M7-5 | `2026-09-23-design-vocabulary-cleanup.md:3,102-163`; `docs_audit.py`; new `tests/test_doc_currency_claims.py` |

## 6. Test plan (failing-first + mutation)

Each item lands test-first (the test must fail on the unmodified tree), the fix turns it green,
one mutation proves it catches the defect's return; mutations are run locally and reverted.

| Item | Failing-first (today) | Mutation (must go red) |
|---|---|---|
| M1-1 | stale epoch still reaches the bridge | delete `recheck_send` |
| M1-2 | escalation leaves the durable gate ENABLED | drop the report call |
| M1-3 | second dial / lost route fill | remove the single `take_reports` |
| M1-4 | duplicate fill → 20 | remove the seen-check |
| M1-5 | guarded place→cancel halts | revert key to instruction-only |
| M1-6 | replay regresses CANCELED; version formula pin | restore blind upsert / drop the hash |
| M2-1 | wrapper absent; guide out of audit | drop `secrets.env` sourcing; remove guide from runbook set |
| M2-2 | catch-up never fires | skip startup catch-up; revert heartbeat verdict |
| M2-3 | GET 500 → rc 0 + re-POSTs | restore discarded status; drop retention counter |
| M2-4 | `make up` rc 0 with guard exit 3 | restore `\|\| echo "!!!"` |
| M2-5 | probe failure → I6/I7/I8 green | ignore failure flags |
| M3-1 | lateness=0 does not evict | restore `5_000L` at the eviction site |
| M3-2 | processing-time fire closes a truncated candle | remove the domain guard |
| M3-3 | host+no-multitf parses | delete the throw |
| M3-4 | doc-parity red (500 vs 5000) | flip the default → Java pin red |
| M4-1 | sub-minimum manifest starts | restore `>0` |
| M4-2 | v3 record processed as v2 | delete the version gate |
| M4-3 | readiness true at 100% queue | delete `setListener`; drop RESUMED |
| M4-4 | blocking backend stalls `write` | call `append` inline |
| M4-5 | early ack then metadata failure → FATAL | restore `== 0L` |
| M4-6 | tampered frame appends | accept always |
| M5-1 | header/manifest 3 vs writer 2 | revert header to 3; set writer constant to 3 |
| M5-2 | Java key-set red vs canonical names | rename `ltp_paise` back; corrupt fixture key |
| M6-1 | parity red on `raw_table_1` (off vs enabled) | restore the non-greedy regex; delete refusal |
| M6-2 | merge leaves O2 bare; preflight passes bare upstream | drop lock overlay; drop lock-match rule |
| M6-3 | coverage red on otel/O2/MinIO | delete one overlay block; remap to a base port |
| M7-1 | readiness 26 vs manifest 27 | manifest pin back to 26 |
| M7-2 | banner missing | remove banner, re-add "Fluss 0.9.1" to a live doc |
| M7-3 | three unmarked Action Capture hits | re-add the phrase to a live doc |
| M7-4 | header claims `TO_BE_PINNED` while the pin holds versions | stale header again |
| M7-5 | guard scans are absent/red | remove a dead-DDL exemption; stale status text |

## 7. Rollout and evidence plan

- **Order (independent first, integrations last):** M7 (docs) and M6-1 / M5-1 / M4-2 / M4-6 /
  M4-5 / M4-1 can land immediately; M3 next; M4-3 + M4-4 land with or after H2-2 (shared drop
  handler); M2-5 after/with C2-1; M1-5 **must precede H1-3's durable flag flip**; M1-1/M1-2 land
  with the H1 wave (same boundary); M1-3/M1-4/M1-6 after H1-5 (shared version identity); M5-2 and
  M6-2 after the H5-2/H5-3 and H3-2 regions respectively; **M5-1 and M6-1 share one manifest
  regeneration**.
- **Commits:** one region-sliced commit per item (M4-3+M4-4 share one with H2-2's hunks; M2-1's
  guide edit sequences with C3-6; M2-2 with C3-3), one CHG per commit citing the failing-first
  test and the mutation.
- **Gate absorption:** all new Python tests live in `code/01_platform/04_scripts/tests/test_*.py`
  (gate step 3 auto-discovery); docs-audit legs run in the existing docs-audit step; no numbered
  step is added (`GATE_TOTAL=19`).
- **Per-area verification:** ingestion `make test`; executor `cargo test --offline` (+ gateway
  module suite); compute/mock module suites; scripts `python3 -m pytest
  code/01_platform/04_scripts/tests -q` from the repo root; docs `make docs-audit`; finish with
  `make gate` + `make static-check`.
- **Smoke-before-run:** no item needs a run > 5 minutes. M6-2's single `docker pull` and M2-2's
  catch-up exercise are short; if anything grows past the threshold, the smoke rule applies first.
- **Evidence:** CHG files carry command + result; M3-4's DEC-058 and M7's banner/status edits are
  docs evidence; M6-2 records the pull digest check; M6-3 supersedes FACT-017 append-only.
- **Never edit code or touch the cluster while a gate run is in flight.**

## 8. Risks and rollback

- **M1:** strict re-check applies only when the reporter is armed; the halt cannot take the async
  forwarder lock (documented; closed at the transport boundary by H1-2/H1-4). The report
  dispatcher is a new chokepoint — an unexpected ref must halt, never drop. E5's action scope
  permits concurrent different actions on one instruction (correct per action semantics).
- **M2:** catch-up retries are idempotent but run the controller again — visible in logs and the
  state file. A genuinely unhealthy catalog now aborts `make up` after the stack is up (intended;
  repair per the guard message). Probe failures turn I6/I7/I8 amber instead of green.
- **M3:** M3-2 changes finalization timing on the rare aligned processing-time fire (final only at
  watermark; ≤ the configured out-of-orderness); session close still force-rolls. M3-3 fails a
  previously useless deployment at boot with the fix in the message.
- **M4:** quarantine rows can lag during a burst (bounded; overflow is fatal and counted); a
  stale handle may retry until grace expiry (bounded); always-on hashing adds ≈12 MB/s at 60k tps
  (inside budget); a manifest below the minimum blocks startup loudly.
- **M5:** DDL/manifest changes are proposal-only text + regeneration (no apply); the row contract
  stays v2, so no stored row is invalidated. The mock has no in-repo consumer; external scripts on
  old keys are unsupported and documented.
- **M6:** the parser change touches all DDLs — regenerate and diff; any change beyond
  `raw_table_1.lake_policy` means stop. Digest enforcement is dev/local only until a deploy
  exists; if the recorded O2 digest is unresolvable, STOP (never invent). p10 remaps are inert
  unless the overlay is started.
- **M7:** the currency legs are curated to live docs with banner/history exemptions; historical
  plans and change records are never edited to satisfy a check.

## 9. Operator decisions — recorded 2026-09-28

All noted defaults follow the operator standard (2026-09-28): **low latency · high throughput ·
correctness · low memory · native · no future maintenance or firefighting**. Where the criteria
conflict, correctness wins and the measured steady-state cost decides.

| # | Decision | Ruling | Effect on this plan |
|---|---|---|---|
| D1 | **M2-2** — missed 15:45 EOD slot | **Catch-up run + same-day retries** (as recommended) | unchanged: durable state, startup catch-up, 900 s retries, heartbeat distinguishes scheduler-dead vs not-archived |
| D2 | **M3-4** — watermark truth | **Keep 500 ms, fix the docs, append DEC-058** (as recommended) | unchanged: code/test stay the authority; six doc sites updated; 5 s remains an explicit override |
| D3 | M4-1 min-manifest count | default 1; pin 1024 in production profiles **and the daily VM** (default noted) | unchanged |
| D4 | M4-6 payload hash | always-on, delete the flag (default noted) | unchanged |
| D5 | M6-2 certification | one `docker pull` at landing; failure = STOP (default noted) | unchanged |
| D6 | M1-5 vs H1-3 | M1-5 lands before the durable flag flip (precondition) | unchanged |

## 10. Out of scope

- The three Critical findings and the six High groups (separate plans, landing first).
- Every Low row of `logs/full-project-audit-20260928/REPORT.md` (a later wave).
- Applying or migrating DDL; production deploy proof; live-money enablement; 4-VM capacity work.
- The dead position-state dashboard (tracked in `2026-09-17-compute-identifier-parity.md`).
- Any redesign beyond the listed boundaries (no new services, no new dependencies, no new
  numbered gate steps).
