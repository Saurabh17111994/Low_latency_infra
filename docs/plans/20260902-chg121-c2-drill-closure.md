# Plan: Close CHG-120 / CHG-121 — C2 Main Drill Validation

**Date:** 2026-09-02
**Status:** ✅ COMPLETE (2026-09-02) — drill chain ran, main drill PASS, CHG-120/CHG-121 closed
*(file recreated 2026-09-02 after a host power cut lost the original; content identical to the executed plan + completion record)*

## Context

Repo: `/home/saurabh/Jupyter_notebook/Flink_Fluss_Infrastructure/streaming_project_New`

Tracker-14 Block 1 / CHG-120 (throughput) + CHG-121 (tentative-signal settlement
guarantee) close on a PASSING main-mode C2 TM-kill drill:
G7c = 0 mismatches AND F4 = 0 orphaned TENTATIVEs.

Five root causes were found and fixed across the debugging session (drills 23:44 → 03:48).
The last one (fifth fix) was a CHECKER artifact, not a pipeline bug — the failed
20260902-034813 drill's data was proven clean by replaying the fixed analyzer
over the saved evidence (G7c 33,792 compared / 0 mismatches, F4 0 orphans, exit 0).

## Fix History (all landed, each with its guard)

| # | Drill | Root cause | Fix | Guard |
|---|---|---|---|---|
| 1 | 20260902-014318 (34 orphans) | `markerHook` field was `transient` → null after job-graph serialization → markers NEVER ran on cluster | field non-transient; `TentativeMarkerHook extends Serializable` | `markerHookSurvivesJobGraphSerialization` |
| 2 | 20260902-022843 (4 orphans) | transient lookup failure gave up permanently | drain retries failed lookups up to MAX_SETTLE_ATTEMPTS=3; one reused Lookuper | `transientLookupFailureRetriesAndStillSettles` |
| 3 | 20260902-030521 (31 orphans) | end-of-run BACKLOG: pipeline ~9,950/s vs 10,000/s feed → kill backlog never drains → run cancelled trailing ~50s | drill drain phase G-TMK-10c (`drill_drain_backlog` in tm-kill-full-load.sh): stop feed, poll source counter until stable 3×5s, DRAIN_TIMEOUT_S=180; drill fails loudly if drain doesn't complete | drain-completion gate in drill script (both modes) |
| 4 | (found while probing #3) | normal settle path never cleared markers (only reconcile did) → stale markers (2d TTL) + replayed final = duplicate settle | `markerHook.clear(tentativeId)` on early-confirm, normal settle, late-drop cancel | `everySettlePathClearsItsMarker` |
| 5 | 20260902-034813 (1024 G7c mismatches) | CHECKER artifact: drain stops feed mid-window → watermark stalls → last window never closes; G7c cutoff used wall-clock run_end (analysis runs minutes later) demanding a candle for an unclosable window | event-horizon bound: G7c comparable windows need `wend ≤ min(run_end−10s, horizon−500)`; F4 extracted to `f4_orphan_check` — orphan only if horizon past window end + 30s grace; trailing never-closed tentatives = unverifiable (excluded) | 8 new tests in `tests/test_holistic_g7_parity.py` (22→30) |

Also landed en route: `tolerable-failed-checkpoints=3` in pipeline-lib.sh submit
(drill-only, TOLERABLE_FAILED_CHECKPOINTS env) — a catch-up checkpoint expiry no
longer escalates to a global failure restart; G-TMK-10b single-restart gate
retained; smoke verdict moved before cleanup; docker-logs greps use `2>&1`
(Flink logs to stderr).

## Files Touched

- `code/01_platform/04_scripts/holistic-analyze.py` — event_horizon tracking in raw scan; `g7c_compare(..., event_horizon=)`; new `f4_orphan_check()`; F4 check deferred to after raw scan
- `code/01_platform/04_scripts/tests/test_holistic_g7_parity.py` — 8 new tests (G7c horizon semantics ×3, F4 semantics ×5)
- `code/01_platform/04_scripts/tm-kill-full-load.sh` — `drill_drain_backlog` (G-TMK-10c), wired after restart gates, both modes
- `code/02_services/02_compute/.../EarlySignalFunction.java` — marker clear on all settle paths
- `code/02_services/02_compute/.../EarlySignalFunctionTest.java` — `everySettlePathClearsItsMarker` (18 tests now)
- `code/01_platform/04_scripts/pipeline-lib.sh` — tolerable-failed-checkpoints=3
- `docs/05_deployment/change-records/CHG-121.md` — fourth + fifth fix sections + CLOSURE
- `docs/05_deployment/change-records/CHG-120.md` — validation closure
- `docs/08_implementation/01-foundation.md` — truth line 431→432
- Plus post-closure doc updates: throughput investigation report (RESOLVED), 04-signal-job dossier (early-signal path section), 01-runbooks (env keys + drill section), 22-failure-chaos-suite (C2 supplement), 09-acceptance-matrix (AC-SS-008 → PASSED)

## Completion Record (2026-09-02)

Executed per plan. Smoke PASS → main drill tm-kill-full-load-20260902-121511:
G7c 33,792/0 mismatches, F4 0 orphans, 1 kill-induced restart only,
recovery 52s, drain completed. RESULT.txt written; CHG-121 + CHG-120
closed in change records; docs-audit all-pass.

Operational incident during execution: host power cut at 12:35 killed the
drill script mid-analysis (all live phases already passed). Analysis
completed standalone over intact evidence + Fluss volumes; identical
results. No data loss.

Latency notes from the final run (for the backlog, not blockers):
preview e2e p50 611ms / p95 58s (catch-up burst after kill), window-close
→ committed p50 1.6s. The p95+ tail is the post-kill replay burst —
expected under catch-up, latency guard intentionally off in drill mode.

## Constraints / Rules (observed)

- USER RULE: no smoke/main drill runs without explicit approval.
- USER RULE: every fix pinned by a unit test or drill gate — never solve the
  same issue twice.
- USER RULE: smoke = compressed main drill (kill included, ~5 min).
- Known residual risk (accepted, CHG-121): crash between marker-durable and
  tentative-row-emitted leaves a stale marker; 2d TTL bounds it.
- Out of scope here: 15k/s capacity ramp (feed-bound at ~10k/s), preview p50
  1.9s vs 1s target, Stage D broker→table e2e latency, multi-timeframe work.
