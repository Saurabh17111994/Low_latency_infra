# Four options — implementation plan (checkpoint interval, checkpoint phase, buffer timeout, checkpoint block)

**Date:** 2026-10-03
**Status:** operator direction recorded 2026-10-03 (see Chosen path). Planning only — implementation
starts per item with the operator's go-ahead; each adopted item lands with its own failing-first
test, CHG record, full `make gate`, and evidence under `logs/`.
**Withdrawn (operator, 2026-10-03):** option 5 (TTL on `Signal_Candidates` / `Signal_Candidates_current`)
— removed from this plan; no work planned on it.
**Decoded:** option 2 (checkpoint phase) is decoded — Flink has **no native setting** to choose or
pin the phase; the phase is the job start second and every restart re-rolls it. The fix is not a
phase setting: it is removing the checkpoint block (option 4). A start-time choice remains a fragile
emergency stopgap only.
**Standing goal (operator):** low latency + high throughput + less state growth + data correctness —
every item below is judged on all four.
**Baseline evidence:** `logs/stage-profile-20261003-chg526-on/` and `...-off/` (900 s,
2 433 stocks @ 2 Hz, certified tree `2e961a05`), `logs/tracker-14/2026-10-03-chg526-rerun-latency-state-growth.md`,
gate `logs/soak/monday-gates-20261003-001358` (19/19, 0 skipped).
**Related plans (they stay the authority for their own items):**

- `docs/plans/2026-09-29-checkpoint-tail-remediation.md` — CT-1..CT-7; CT-4A changelog backend.
- `docs/plans/2026-09-30-p99-50ms-normal-path.md` — W1..W6 live tracker; W2 buffer timeout, W3 materialization.
- `docs/plans/2026-09-30-w3d-state-churn-design.md` — W3-d design; CHG-460.
- Dossier: `docs/08_implementation/04-signal-job.md` (config table + changelog entry).

## Chosen path (operator direction, 2026-10-03)

| Option | Decision |
|---|---|
| **L4 - Checkpoint block (changelog backend)** | **Apply** — the main fix; removes the checkpoint freeze |
| L3 - Buffer timeout (2 ms) | **Optional** — decide together with the W6 certification run; fallback 5 ms |
| L1 - Checkpoint interval | **Keep 10 s** — no change |
| L2 - Checkpoint phase | **Closed** — no native control exists; the fix is L4 |
| Option 5 - Signal-table TTL | **Withdrawn** (operator, 2026-10-03) |

## Next actions (in order)

1. **L4.3 — production deck config (no job code): DONE 2026-10-03 (CHG-540).** The plugin ships as a
   Swarm config + the two `state.changelog.*` keys sit in the TM properties; the stack render and
   `make pin-check` are green. Next: L4.4 smoke.
2. **L4.4 — 200 s smoke (dev stack): DONE 2026-10-03 — PASS** (`logs/stage-profile-20261003-cpb-smoke3`):
   25/25 checkpoints COMPLETED, all-subtask sync p99 20 ms / max 34 ms, e2e p50 53 ms, throughput
   4 866 rows/s, presence gate PASS, changelog live (+0.77 MB/min). Next: L4.5.
3. **L4.5 — production restore: config landed 2026-10-03 (CHG-541), adoption pending the next declared
   start window.** `CHANGELOG_STATE_BACKEND=true` + automatic `claim-mode=CLAIM` on restores; verify
   with the state meter + checkpoint stats; rollback = flag off. The 900 s certification (2026-10-03,
   `logs/stage-profile-20261003-l4-cert`) verified the flip's own criteria (sync p99 8 ms, 94/94) but
   surfaced the residual W3 materialization tail (45/50 > 100 ms at 10 s) — see the p99-50ms plan W3.
4. **L3 — optional buffer default:** decide the 2 ms default together with the p99-50ms W6
   certification run (test + CHG + gate).
5. **L1/L2 — no action:** keep the 10 s interval; the phase stays closed.

## 0. Live tracker

#### L1 - Checkpoint interval

- [x] **L1.1** Current state verified — production pin `PlatformConfig.CHECKPOINT_INTERVAL_MS = 10_000`
  (production path requires the pinned value, `SignalJobConfig.java:629-640`); dev submit path default
  60 000 ms (`pipeline-lib.sh:1158`), dev tunable 1 000–600 000 ms. Measured effect: 15 checkpoints /
  900 s at 60 s; checkpoint-window p99 class 189–388 ms (ON arm) vs 110–122 ms (OFF arm). With the
  changelog backend (L4) the sync cost is 20 ms p99 and the interval is no longer the p99 driver.
- [x] **L1.2** Decision taken (operator direction 2026-10-03): keep the 10 s production pin — no
  latency need after L4; a longer interval only increases crash replay.
- [ ] **L1.3** Fallback only, if the W6 certification still misses p99 ≤ 50 ms — CT-6 cadence round
  (10 s → 30/60 s): contract/pin re-check (CT-6 cites REQ-FC-006, which is the final-on-emission
  policy — verify the real cadence anchor first), `PlatformConfig` + `pipeline-lib.sh` + docs + CHG +
  full gate + confirming 900 s run. Cost: up to one interval of raw replay after a crash (recovery
  size, not correctness).

#### L2 - Checkpoint phase

- [x] **L2.1** Decoded 2026-10-03 — no native Flink option sets or pins the phase (no
  initial-delay/phase setting); the phase equals the coordinator start second (job start) and every
  restart re-rolls it (the 30 s auto-restart delay can recreate a :00 start). Natural experiment:
  ON arm phase :00.3 → checkpoint windows 189–388 ms; OFF arm phase :21.1 → 110–122 ms; the :00
  six-TF close alone is harmless (88–99 ms), and the collision amplifies the checkpoint block ~3×.
  Evidence: `logs/stage-profile-20261003-chg526-on|off/`.
- [-] **L2.2** Phase start-time workaround (start at :07/:22) — downgraded: fragile by construction
  (restarts re-roll it), no config control, and unnecessary once L4 lands. Reopen only if L4 is
  rejected and the phase collision is the largest remaining driver.

#### L3 - Buffer timeout

- [x] **L3.1** Trial measured (W2, CHG-450, 2026-09-30) — `BUFFER_TIMEOUT_MS` 10 → 2 (combined with the
  fetch-wait trial; marginal vs the fetch-wait arm alone): ingest p50 −6 / p95 −11 / p99 med −13 ms;
  tick p50 −14 / p95 −17 / p99 med −18 ms; throughput 4 866 rows/s; 95/95 checkpoints. Cost watch
  item: checkpoint e2e med 75 ms (fetch-wait alone 53 / control 40) — sub-0.75 % duty, all COMPLETED;
  fallback 5 ms if it grows. Evidence: W1+W2 round result in the p99-50ms plan + `logs/stage-profile-20260930-*`.
- [?] **L3.2** Decision: adopt 2 ms as the code default (`SignalJobConfig.java:1377`, currently 10) or
  keep it env-only. Recommended: adopt with the W6 certification if no checkpoint-e2e regression;
  fallback 5 ms.
- [ ] **L3.3** If adopted — failing-first test + CHG + full gate + the W6 confirming run. The
  fetch-wait key stays out of scope (operator decision 2026-10-03).

#### L4 - Checkpoint block itself

- [x] **L4.1** CT-4A changelog state backend — implemented behind `CHANGELOG_STATE_BACKEND` (default
  false, `SignalJobConfig.java:699`); certified at the 10 s cadence: host sync p99 20 ms / max 51 ms /
  0 records > 100 ms (RocksDB: p99 663 / max 1 157 / 1 819 records); e2e p50 50 ms; windows > 100 ms
  6/60 vs 52/60; throughput 4 865 rows/s; gate `monday-gates-20260929-235738` 19/19. Restore drills
  both directions green (CHG-443).
- [x] **L4.2** W3-d state-churn reduction (CHG-460, in HEAD `72d4d547`) — per-key live-mirror timers
  removed; 900 s verdict PASS: changelog +0.93 MB/min (≥ 95 % cut vs ~20), state 0.58 → 11.54 MB
  (baseline ~94 med / 183 max); covered by the 2026-10-03 full gate `monday-gates-20261003-001358`
  (19/19) on the certified tree.
- [x] **L4.3** Production deck prerequisites — DONE 2026-10-03 (CHG-540): `docker-stack.yml` now
  carries the plugin as a **Swarm config** (`flink-dstl-dfs-jar` →
  `/opt/flink/plugins/dstl-dfs/flink-dstl-dfs-2.2.1.jar`; chosen over a host bind — the repo path
  differs per VM and a missing bind source is a recorded production failure) + the two TM keys
  (`state.changelog.storage: filesystem`, `state.changelog.dstl.dfs.base-path: file:///checkpoints/changelog`).
  Verified: 92 offline stack tests, `make stack-selfcheck` (render OK), `make pin-check` PASS.
  Limits recorded: base path is node-local (single-node deck; the 4-VM rig needs shared storage);
  jar is gitignored (the VM rsync route carries it, CHG-493).
- [x] **L4.4** Smoke 200 s on the current tree with `CHANGELOG_STATE_BACKEND=true` — PASS 2026-10-03
  (`logs/stage-profile-20261003-cpb-smoke3`, 10 s cadence, full universe 2 Hz): 25/25 checkpoints
  COMPLETED; per-checkpoint sync p50 7 / p90 22 / p99 32 / max 34 ms; all-subtask sync p99 20 ms /
  max 34 ms (certified class, vs RocksDB 663 ms); e2e p50 53 / p99 90 / max 94 ms; throughput
  4 866 rows/s (baseline 4 865); presence gate PASS; changelog confirmed live (`flink.changelog.bytes`
  +0.77 MB/min, job dir 3.8 MB on the TM volume). Attempts 1–2 were stopped by harness staleness
  guards before any job ran (loadgen image, compute jar — both rebuilt); no fix failure observed.
- [ ] **L4.5** Production restore with the flag — `CHANGELOG_STATE_BACKEND=true` +
  `execution.state-recovery.claim-mode=CLAIM` (mandatory on an adoption restore: NO_CLAIM schedules a
  FULL_CHECKPOINT the backend refuses); anchor from the current table epoch; rollback = flag off
  (drilled both ways). Verify with the state meter + checkpoint stats. **Config landed 2026-10-03
  (CHG-541)** — compose job-env anchor + VM template + local env; the flag reaches both submit paths.
  **Certification 2026-10-03** (`logs/stage-profile-20261003-l4-cert`, 900 s @ 10 s): 94/94
  COMPLETED, sync p99 8 ms / max 66 ms, throughput 4,865 rows/s, presence PASS, changelog
  +0.45 MB/min, RocksDB flat. Residual recorded: W3 materialization tail (45/50 > 100 ms at 10 s on
  the current tree vs 6/60 on the 2026-09-29 tree) — not fixed by this flip. Adoption = next declared
  start window; rollback = flag off.
- [x] **L4.6** Decision (operator, 2026-10-03): **A — flip the flag now + open W3 materialization as
  the next item.** The full W6 bundle is deferred until W3 closes the > 100 ms tail; the flip is a
  strict win vs the sync block in the meantime.

**Roll-up**

| Stage | Tasks | done | wip | todo | live | decide | skip |
|---|---|---|---|---|---|---|---|
| L1 - Checkpoint interval | 3 | 2 | 0 | 1 | 0 | 0 | 0 |
| L2 - Checkpoint phase | 2 | 1 | 0 | 0 | 0 | 0 | 1 |
| L3 - Buffer timeout | 3 | 1 | 0 | 1 | 0 | 1 | 0 |
| L4 - Checkpoint block itself | 6 | 5 | 0 | 1 | 0 | 0 | 0 |
| **Total** | **14** | **9** | **0** | **3** | **0** | **1** | **1** |

## Overview

**What this plan is.** One place for the four native options discussed on 2026-10-03, each with its
current status, the work it needs, its cost, and its decision gate. It does not replace the related
plans; it routes work into them where they already own an item (W2/W3/W6, CT-4A/CT-6).

**Success criteria**

| # | Criterion | Proof |
|---|---|---|
| S1 | Every adopted item has its own test, CHG record, and a green gate | `make gate` certificate + CHG files |
| S2 | The checkpoint-window p99 class is removed by the changelog backend (L4), not by phase tricks | 900 s run: host sync p99 ≈ 20 ms, windows > 100 ms ≤ 6/60, throughput parity |
| S3 | No regression: all checkpoints COMPLETED, presence PASS, throughput within ±2 %, restore drills where state changes | same runs + drill evidence |

**Working rules** (same as the CT-4A and p99-50ms rounds)

1. One lever per measurement round; combined rounds only when the operator approves them.
2. Smoke first: 200 s must show data flow (table growth, source counters, presence) before any 900 s run.
3. No blocking waits > 30 s; readiness/process signals only.
4. Code changes: failing-first test → change → full `make gate` → only then measure. No code edits
   while a gate is running.
5. Env-only trial rounds touch no tracked file. A winning value becomes a default only with a code
   change + CHG + full gate.
6. Evidence lands under `logs/` and is never edited afterwards.
7. Never resolve a code/DDL/contract conflict silently — record it in `01-foundation.md` and keep the
   affected item blocked.

**Non-goals:** executor/downstream latency; feed timestamp-fidelity fix; topology/hop reduction (W5);
any change to dedup, candle, or strategy semantics; signal-table TTL (withdrawn); production deploy
proof (this host cannot prove a real-VM deploy — see the ENVIRONMENT.md FACT rows).

## Items — what work each option needs

| Option | Status | Work if approved | Cost / risk | Stop rule |
|---|---|---|---|---|
| **L1 Checkpoint interval** | Verified; decision pending | Keep 10 s (no work). If a cadence round is ever chosen: contract/pin re-check + config + docs + CHG + gate + 900 s run | Replay window grows to the interval; recovery size only | Throughput or recovery regression |
| **L2 Checkpoint phase** | **Decoded**; workaround downgraded | None. The fix lives in L4 | Phase choice is fragile by construction (restarts re-roll it) | n/a — reopen only if L4 is rejected |
| **L3 Buffer timeout** | Trial measured; decision pending | Adopt 2 ms as the code default (`SignalJobConfig`), test + CHG + gate + W6 run | More flush events; checkpoint e2e watch (75 vs 40–53 ms med, sub-0.75 % duty); fallback 5 ms | Checkpoint e2e grows or throughput drops |
| **L4 Checkpoint block** | Flag implemented + certified; production deck pending | Deck plugin/keys → 200 s smoke → production restore with the flag + `claim-mode=CLAIM` | More TM memory/files/IO; recovery time can vary; claim-mode rule on adoption | Checkpoint failures, memory growth, or no p99 improvement → flag off |

## Cross-goal impact (expected, from measured evidence)

| Option | Low latency | High throughput | Less state growth | Data correctness |
|---|---|---|---|---|
| L1 interval | Neutral after L4; fewer checkpoint events at 30/60 s | Neutral | Neutral (fewer metadata files) | Replay window grows; correctness unchanged |
| L2 phase | Only the fragile stopgap; no native control | Neutral | Neutral | None |
| L3 buffer 2 ms | −13…−18 ms at p95/p99 medians (W2) | Neutral (4 866 rows/s) | Neutral | None |
| L4 changelog backend | Removes the 189–388 ms checkpoint-window class; sync p99 20 ms | Neutral (4 865 rows/s) | Changelog files +494 KB metadata; W3-d cut churn ≥ 95 % | Exactly-once unchanged; restore drills green |

## Evidence map

| Item | Evidence |
|---|---|
| L1 | `SignalJobConfig.java:629-640`, `pipeline-lib.sh:1158`, `logs/stage-profile-20261003-chg526-on|off/` |
| L2 | `logs/stage-profile-20261003-chg526-on|off/` (phases + per-window p99), this plan |
| L3 | W2 round result (`2026-09-30-p99-50ms-normal-path.md`), CHG-450, `logs/stage-profile-20260930-*` |
| L4 | CT-4A: CHG-443/CHG-444, `monday-gates-20260929-235738`; W3-d: CHG-460, `72d4d547`; deck: CHG-540 (`docker-stack.yml` Swarm config + TM keys); smoke/restore evidence to be added |

## Rollback

- L1: revert the pin/config to 10 s; no state change.
- L2: no implementation — nothing to roll back.
- L3: revert the default (or the env value); no state/format change.
- L4: flip `CHANGELOG_STATE_BACKEND=false` (both directions drilled restore-safe); deck keys can stay
  (inert without the flag).

## Operator approval points

1. Approve this plan (scope + item order).
2. Per item before implementation: L1 (cadence change), L3 (default adoption), L4 (deck prerequisites,
   flag flip, restore window).
3. Production deploy only in a declared window; no silent default flips.
4. Smoke before any long run; full gate after any code change; evidence recorded before the item is
   marked done.
