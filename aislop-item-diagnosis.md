# aislop item-level diagnosis (full independent cross-check, 2026-10-03)

Scope: **all 548 items** in `aislop-remediation-tracker.md`. Method: mechanical finding-presence
reconciliation against the latest scan (306 rule+file pairs, 0 mismatches: 548 − 38 fixed = 510 present),
plus a **100% blind per-item code re-audit of all 510 non-fixed items** (16 independent reviewers,
verdict-blind). Result: **10 real items found — all corrected in CHG-538/539** (AS-025/026/033/034
cleared by rescan; AS-450/AS-371/AS-078/AS-119/AS-120/AS-160/AS-186/AS-276 fixed with the finding
remaining accepted); **42/548 findings cleared**, 403 policy, 97 not-real, 5 deferred.

## A. Fixed items — verified implemented (38)

- **G2 ruff (22)**: independent `ruff check` on each file+rule = 22/22 clean.
- **G3 (8)**: AS-035, AS-037, AS-320…AS-325 — findings cleared; tests in the scripts suite.
- **G4 (3)**: AS-286, AS-287, AS-303 — fail-closed guards in place (CHG-531).
- **G7 (1)**: AS-329 — unused import gone (`ruff F401` clean).
- **G13 (4)**: AS-053 (quarantine LogScanner, gate test 14/14), AS-155/AS-270/AS-284 (comments corrected, findings cleared).

All 38 findings are absent from the current scan; no other finding cleared or was gained.

## B. Real items found by the full audit (10) — all fixed (CHG-538/539)

| ID | Group | Site | Diagnosis |
|---|---|---|---|
| **AS-078** | G13 | `code/01_platform/04_scripts/audit_r2.py:2` | REAL — audit_r2.py:14-15 header points to foundation.md L159 for the EvidenceRecord shape; L159 is now a sequence table row and "### Evidence record" is at L301. Fix the pointer. |
| **AS-119** | G13 | `code/01_platform/04_scripts/holistic-analyze.py:1778` | REAL — holistic-analyze.py:1799 says "byReason counters (8 reasons)"; RawValidationFunction.invalidReason() returns 10 distinct reasons (negative-volume-delta, event-time-overflow-window added). Fix the count. |
| **AS-120** | G13 | `code/01_platform/04_scripts/o2-provision.py:2` | REAL — o2-provision.py:14-16 header says "43 alert rules (18 ING- + 16 SIGNAL- + 9 INFRA-)"; the file contains 47 alert names (18 ING- / 20 SIGNAL- / 9 INFRA-). Fix the counts. |
| **AS-160** | G13 | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:346` | REAL — IngestionConfig.java:348-352 comment and fallback default pin go-arrow v0.0.0-20260622-7cce1630 / tree f622f8a9; versions.pin:26 now pins go-arrow-v0.2.0 / tree sha256:1ea24cd6.... Fix comment + fallback. |
| **AS-186** | G13 | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:118` | REAL — OtlpMetricsEmitter.java:118 "Histogram (approximate via linear buckets)" is false: percentiles use a 1024-sample ring + sort (R-065/R-179); no bucket bounds exist. Fix the comment. |
| **AS-276** | G13 | `code/common/src/main/java/com/trading/common/schema/projection/PostbackFingerprint.java:63` | REAL — PostbackFingerprint.java:63 Javadoc says canonicalFrom is "used by tests to mint valid fixtures"; production matches() (:95) and canonicalParts (:55) call it. Fix the comment. |

**Status after CHG-538/539:** AS-025, AS-026, AS-033, AS-034 cleared by rescan (fixed); AS-450, AS-371, AS-078, AS-119, AS-120, AS-160, AS-186, AS-276 corrected — the aislop findings remain (accepted patterns), the defects/claims are fixed. XC-32…XC-41.
| **AS-450** | G11 | `code/02_services/01_ingestion/go-bridge/main.go:404` | REAL — production Go: read-loop auth refresh off-by-one. After authTries++ the call passes the post-increment budget as hasRefresh; the 3rd allowed refresh runs and can succeed but is classified authTerminalExhausted (false authentication_refresh_exhausted), permanently stopping the slot — the dial path retries on the same success. Unit tests cover classifyAuthRefresh in isolation, never the call site. Fix: derive didRefresh from the pre-increment guard + integration test. |
| **AS-371** | G10 | `code/01_platform/04_scripts/holistic-analyze.py:714` | REAL — regression from CHG-534 (f776ba65 removed the only buckets_1s.setdefault producer): burst_secs is always empty, the burst print always reads 0, every burst-correlation leg is dead, and the G6b stall guard can never fire although its comment claims "fully armed". Fix: restore the producer or retire the guard with an UNAVAILABLE note. |
| **AS-025** | G3 | `code/01_platform/04_scripts/holistic-analyze.py:1027` | REAL — holistic-analyze.py:1034-1039 drops corrupt checkpoints.jsonl lines with per-line except ValueError: pass; the under-counted cps feeds the burst-correlation verdict with no warning. Fix: count/report unparseable lines. |
| **AS-026** | G3 | `code/01_platform/04_scripts/holistic-analyze.py:1029` | REAL — holistic-analyze.py:1040-1041 swallows a missing/unreadable checkpoints.jsonl to cps=[] and prints "slow checkpoints (>5s): 0" — an unmeasured leg reads as a clean zero (counter_leg_note discipline not applied here). Fix: UNAVAILABLE note with the read error. |

## C. Deferred — not implemented by design (5)

| ID | Site | Trigger/sketch |
|---|---|---|
| AS-404 | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java` | split on next touch (see tracker evidence) |
| AS-438 | `code/01_platform/04_scripts/holistic-analyze.py` | split on next touch (see tracker evidence) |
| AS-448 | `code/02_services/01_ingestion/go-bridge/faketool/main.go` | split on next touch (see tracker evidence) |
| AS-461 | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java` | split on next touch (see tracker evidence) |
| AS-472 | `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java` | split on next touch (see tracker evidence) |

## D. Confirmed no-action (495)

97 NOT-REAL (tool premise wrong in context) + 398 POLICY (deliberate/accepted) — each item individually
re-judged blind in this audit; notes per item below.

## E. Full per-item ledger (548)

| ID | Group | Rule | Site | Recorded | Finding | Blind verdict | Final status |
|---|---|---|---|---|---|---|---|
| AS-001 | G1 | security/hardcoded-secret | `code/01_platform/04_scripts/t9_order_sandbox.py:157` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-002 | G1 | security/hardcoded-secret | `code/01_platform/04_scripts/t9_order_sandbox.py:918` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-003 | G1 | security/hardcoded-secret | `code/02_services/04_executor/src/engine.rs:611` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-004 | G1 | security/hardcoded-secret | `code/common/src/main/java/com/trading/common/config/ConfigKeys.java:99` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-005 | G1 | security/python-exec | `code/01_platform/04_scripts/t9_order_sandbox.py:379` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-006 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/cluster_check.py:203` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-007 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/cp_phase_capture.py:181` | won't-fix | present | NOT-REAL | no action — not a defect in context |
| AS-008 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/ddl_apply.py:485` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-009 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/ddl_apply.py:607` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-010 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/ddl_apply.py:621` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-011 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/disaster_drills.py:65` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-012 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/disaster_drills.py:136` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-013 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/eod_schedule.py:90` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-014 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/eod_schedule.py:119` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-015 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/eod_schedule.py:168` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-016 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:197` | won't-fix | present | NOT-REAL | no action — not a defect in context |
| AS-017 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:576` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-018 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:585` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-019 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:592` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-020 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:616` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-021 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:686` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-022 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:693` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-023 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:961` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-024 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:983` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-025 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:1027` | false-positive | present | REAL | **fixed — cleared by CHG-538** |
| AS-026 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:1029` | false-positive | present | REAL | **fixed — cleared by CHG-538** |
| AS-027 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:1180` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-028 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:1282` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-029 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:1323` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-030 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:1374` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-031 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:1425` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-032 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:1465` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-033 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:1509` | false-positive | present | NOT-REAL | fixed — cleared by CHG-538 (G6b read retired) |
| AS-034 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:1511` | false-positive | present | NOT-REAL | fixed — cleared by CHG-538 (G6b read retired) |
| AS-035 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:1589` | fixed | cleared | — | **implemented (verified)** |
| AS-036 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:1642` | won't-fix | present | NOT-REAL | no action — not a defect in context |
| AS-037 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/holistic-analyze.py:1644` | fixed | cleared | — | **implemented (verified)** |
| AS-038 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/image_staleness_check.py:249` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-039 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/o2_ingest.py:55` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-040 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/perf_evidence_parse.py:469` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-041 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/pernode_attribution_check.py:68` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-042 | G3 | ai-slop/swallowed-exception | `code/01_platform/04_scripts/soak-o2-evidence.py:75` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-043 | G3 | ai-slop/swallowed-exception | `code/01_platform/05_instruments/split_manifest.py:53` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-044 | G3 | ai-slop/swallowed-exception | `code/common/src/main/java/com/trading/common/schema/eod/EodControllerTool.java:543` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-045 | G8 | ai-slop/hardcoded-id | `code/02_services/04_executor/src/config.rs:396` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-046 | G8 | ai-slop/hardcoded-id | `code/02_services/04_executor/src/engine.rs:611` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-047 | G8 | ai-slop/hardcoded-url | `code/02_services/04_executor/src/config.rs:431` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-048 | G8 | ai-slop/hardcoded-url | `code/02_services/04_executor/src/engine.rs:613` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-049 | G8 | ai-slop/hardcoded-url | `code/02_services/04_executor/src/engine.rs:631` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-050 | G8 | ai-slop/hardcoded-url | `code/02_services/04_executor/src/engine.rs:697` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-051 | G13 | ai-slop/meta-comment | `code/01_platform/04_scripts/holistic-analyze.py:1591` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-052 | G13 | ai-slop/meta-comment | `code/01_platform/04_scripts/ing-tcp001/TokenCountReconcile.java:20` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-053 | G13 | ai-slop/meta-comment | `code/01_platform/04_scripts/ing-tcp001/TokenCountReconcile.java:195` | fixed | cleared | — | **implemented (verified)** |
| AS-054 | G13 | ai-slop/meta-comment | `code/02_services/01_ingestion/go-bridge/supervisor.go:112` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-055 | G13 | ai-slop/meta-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:859` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-056 | G13 | ai-slop/meta-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:1671` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-057 | G13 | ai-slop/meta-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2452` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-058 | G13 | ai-slop/meta-comment | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SourceIdleWatchdogState.java:7` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-059 | G13 | ai-slop/meta-comment | `code/02_services/04_executor/src/execution/client.rs:2519` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-060 | G13 | ai-slop/meta-comment | `code/02_services/04_executor/src/projection/mod.rs:1660` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-061 | G13 | ai-slop/meta-comment | `code/02_services/04_executor/src/shutdown.rs:336` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-062 | G13 | ai-slop/meta-comment | `code/02_services/06_execution_bridge/go-bridge/reauth.go:52` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-063 | G13 | ai-slop/meta-comment | `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/ExecutionGatewayMain.java:110` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-064 | G13 | ai-slop/meta-comment | `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/IntentReader.java:190` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-065 | G13 | ai-slop/meta-comment | `code/common/src/main/java/com/trading/common/ownership/OwnershipMatrix.java:55` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-066 | G13 | ai-slop/meta-comment | `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:301` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-067 | G13 | ai-slop/meta-comment | `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:365` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-068 | G13 | ai-slop/meta-comment | `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:424` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-069 | G13 | ai-slop/meta-comment | `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:456` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-070 | G13 | ai-slop/meta-comment | `code/common/src/main/java/com/trading/common/schema/execution/FlussAttemptStore.java:213` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-071 | G13 | ai-slop/meta-comment | `code/common/src/main/java/com/trading/common/schema/execution/GateRow.java:106` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-072 | G13 | ai-slop/meta-comment | `code/common/src/main/java/com/trading/common/schema/execution/InMemoryGateStateStore.java:156` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-073 | G13 | ai-slop/meta-comment | `code/common/src/main/java/com/trading/common/schema/position/PositionProjectorDriver.java:42` | won't-fix | present | NOT-REAL | no action — not a defect in context |
| AS-074 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/alert-routing-selftest.py:113` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-075 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/alert-routing-selftest.py:119` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-076 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/alert-routing-selftest.py:128` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-077 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/alert-routing-selftest.py:179` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-078 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/audit_r2.py:2` | won't-fix | present | REAL | **fixed — CHG-538/539 (finding remains accepted)** |
| AS-079 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/audit_r2.py:112` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-080 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/audit_r2.py:165` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-081 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/audit_r2.py:277` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-082 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/audit_r2.py:430` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-083 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/day_run.py:39` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-084 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/day_run.py:119` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-085 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/day_run.py:184` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-086 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/day_run.py:603` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-087 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/day_run.py:641` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-088 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/day_run.py:1011` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-089 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/day_run.py:1198` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-090 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/day_run.py:1350` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-091 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/disaster_drills.py:104` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-092 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/disaster_drills.py:248` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-093 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:387` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-094 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:428` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-095 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:630` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-096 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:653` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-097 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:677` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-098 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:700` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-099 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:704` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-100 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:765` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-101 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:779` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-102 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:860` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-103 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:903` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-104 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:932` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-105 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:956` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-106 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:1028` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-107 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:1095` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-108 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:1146` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-109 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:1253` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-110 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/docs_audit.py:1468` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-111 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/holistic-analyze.py:729` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-112 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/holistic-analyze.py:790` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-113 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/holistic-analyze.py:834` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-114 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/holistic-analyze.py:920` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-115 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/holistic-analyze.py:1432` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-116 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/holistic-analyze.py:1437` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-117 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/holistic-analyze.py:1569` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-118 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/holistic-analyze.py:1632` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-119 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/holistic-analyze.py:1778` | won't-fix | present | REAL | **fixed — CHG-538/539 (finding remains accepted)** |
| AS-120 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/o2-provision.py:2` | won't-fix | present | REAL | **fixed — CHG-538/539 (finding remains accepted)** |
| AS-121 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/o2-provision.py:49` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-122 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/o2-provision.py:910` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-123 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/o2-provision.py:1048` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-124 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/o2-provision.py:1236` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-125 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/r2_legal_hold_check.py:80` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-126 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/r2_legal_hold_check.py:220` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-127 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/r2_legal_hold_check.py:278` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-128 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/r2_legal_hold_check.py:435` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-129 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/stale_table_kind_scan.py:112` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-130 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/stale_table_kind_scan.py:309` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-131 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/stale_table_kind_scan.py:376` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-132 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/t9_order_sandbox.py:161` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-133 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/t9_order_sandbox.py:186` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-134 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/t9_order_sandbox.py:340` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-135 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/t9_order_sandbox.py:417` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-136 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/t9_order_sandbox.py:580` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-137 | G13 | ai-slop/narrative-comment | `code/01_platform/04_scripts/t9_order_sandbox.py:1091` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-138 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java:273` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-139 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:406` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-140 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:577` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-141 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:603` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-142 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:1757` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-143 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:1760` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-144 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2046` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-145 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2066` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-146 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2159` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-147 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2321` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-148 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2410` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-149 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TypedFlussRowConverter.java:74` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-150 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:30` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-151 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:41` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-152 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:101` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-153 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:165` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-154 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:197` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-155 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:216` | fixed | cleared | — | **implemented (verified)** |
| AS-156 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:220` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-157 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:263` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-158 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:279` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-159 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:305` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-160 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:346` | won't-fix | present | REAL | **fixed — CHG-538/539 (finding remains accepted)** |
| AS-161 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:352` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-162 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:357` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-163 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:362` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-164 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:424` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-165 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:596` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-166 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:689` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-167 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/SequenceGapMonitor.java:7` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-168 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/fingerprint/FingerprintBuilder.java:115` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-169 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:75` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-170 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:79` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-171 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:82` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-172 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:247` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-173 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:276` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-174 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/NtpClockChecker.java:178` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-175 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/NtpClockChecker.java:288` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-176 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:12` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-177 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:17` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-178 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:22` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-179 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:27` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-180 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:39` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-181 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:76` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-182 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:80` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-183 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:85` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-184 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:248` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-185 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:103` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-186 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:118` | won't-fix | present | REAL | **fixed — CHG-538/539 (finding remains accepted)** |
| AS-187 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:156` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-188 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:165` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-189 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:168` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-190 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:188` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-191 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:290` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-192 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:369` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-193 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:437` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-194 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:443` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-195 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:514` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-196 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:571` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-197 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:596` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-198 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java:69` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-199 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java:83` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-200 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java:163` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-201 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java:220` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-202 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java:237` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-203 | G13 | ai-slop/narrative-comment | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java:522` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-204 | G13 | ai-slop/narrative-comment | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/CandleAccumulator.java:51` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-205 | G13 | ai-slop/narrative-comment | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/CandleFetcher.java:26` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-206 | G13 | ai-slop/narrative-comment | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/N7RangeBreakoutStrategy.java:15` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-207 | G13 | ai-slop/narrative-comment | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/RawTableColumns.java:48` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-208 | G13 | ai-slop/narrative-comment | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SourceIdleWatchdogGenerator.java:145` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-209 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/bridge/transport.rs:39` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-210 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/bridge/transport.rs:289` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-211 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/engine.rs:847` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-212 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/engine.rs:892` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-213 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/executiongate.rs:1096` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-214 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/executiongate.rs:1203` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-215 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/executiongate.rs:1297` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-216 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/executiongate.rs:1336` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-217 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/executiongate.rs:1394` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-218 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/executiongate.rs:1430` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-219 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/executiongate.rs:1466` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-220 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/executiongate.rs:1503` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-221 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/executiongate.rs:1540` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-222 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/gateway_protocol.rs:571` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-223 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/http.rs:2215` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-224 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/projection/mod.rs:1134` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-225 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/projection/mod.rs:1330` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-226 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/projection/mod.rs:1377` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-227 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/projection/mod.rs:1393` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-228 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/projection/mod.rs:1436` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-229 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/projection/mod.rs:1471` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-230 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/projection/mod.rs:1510` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-231 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/projection/mod.rs:1573` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-232 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/projection/mod.rs:1575` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-233 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/resilience.rs:396` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-234 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/resilience.rs:408` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-235 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/resilience.rs:420` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-236 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/resilience.rs:433` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-237 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/resilience.rs:456` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-238 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/resilience.rs:486` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-239 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/resilience.rs:512` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-240 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/resilience.rs:535` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-241 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/resilience.rs:564` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-242 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/resilience.rs:598` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-243 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/resilience.rs:619` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-244 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/resilience.rs:644` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-245 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/resilience.rs:663` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-246 | G13 | ai-slop/narrative-comment | `code/02_services/04_executor/src/resilience.rs:689` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-247 | G13 | ai-slop/narrative-comment | `code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java:378` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-248 | G13 | ai-slop/narrative-comment | `code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/SyntheticWorkload.java:9` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-249 | G13 | ai-slop/narrative-comment | `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:52` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-250 | G13 | ai-slop/narrative-comment | `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:90` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-251 | G13 | ai-slop/narrative-comment | `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:105` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-252 | G13 | ai-slop/narrative-comment | `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:123` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-253 | G13 | ai-slop/narrative-comment | `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:138` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-254 | G13 | ai-slop/narrative-comment | `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:152` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-255 | G13 | ai-slop/narrative-comment | `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/SafetyHaltTailProcessor.java:10` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-256 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/config/ContainerMemoryGuard.java:45` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-257 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:26` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-258 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:40` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-259 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:54` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-260 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:68` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-261 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:73` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-262 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:87` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-263 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:113` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-264 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/model/AttemptPhase.java:45` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-265 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/model/GateTransitionValidator.java:233` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-266 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/observability/Json.java:44` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-267 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/observability/Json.java:75` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-268 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/schema/RawTableSchema.java:98` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-269 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/schema/RawTableSchema.java:187` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-270 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/schema/eod/EncryptedExportEodOffloadExecutor.java:17` | fixed | cleared | — | **implemented (verified)** |
| AS-271 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/schema/execution/GateRow.java:39` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-272 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/schema/position/PositionProjector.java:37` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-273 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/schema/projection/OrderLifecycleProjector.java:79` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-274 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/schema/projection/OrderLifecycleProjector.java:124` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-275 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/schema/projection/OrderLifecycleProjector.java:141` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-276 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/schema/projection/PostbackFingerprint.java:63` | won't-fix | present | REAL | **fixed — CHG-538/539 (finding remains accepted)** |
| AS-277 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:83` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-278 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:89` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-279 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:105` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-280 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:114` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-281 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:122` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-282 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:142` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-283 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:192` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-284 | G13 | ai-slop/narrative-comment | `code/common/src/main/java/com/trading/common/version/VersionGate.java:55` | fixed | cleared | — | **implemented (verified)** |
| AS-285 | G3 | ai-slop/python-broad-except | `code/01_platform/04_scripts/cp_phase_capture.py:181` | won't-fix | present | NOT-REAL | no action — not a defect in context |
| AS-286 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/audit_r2.py:474` | fixed | cleared | — | **implemented (verified)** |
| AS-287 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/audit_r2.py:540` | fixed | cleared | — | **implemented (verified)** |
| AS-288 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/cluster_check.py:159` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-289 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/day_run.py:426` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-290 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/day_run.py:982` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-291 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/day_run.py:995` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-292 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/day_run.py:1001` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-293 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/day_run.py:1102` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-294 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/day_run.py:1138` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-295 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/holistic-analyze.py:1171` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-296 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/local_int_004_smoke.py:38` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-297 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/local_int_004_smoke.py:42` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-298 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/o2-provision.py:1353` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-299 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/perf_evidence_parse.py:374` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-300 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/perf_evidence_parse.py:375` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-301 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/perf_evidence_parse.py:376` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-302 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/seed_dashboards.py:191` | won't-fix | present | NOT-REAL | no action — not a defect in context |
| AS-303 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/soak-o2-evidence.py:137` | fixed | cleared | — | **implemented (verified)** |
| AS-304 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/t9_order_sandbox.py:385` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-305 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/04_scripts/t9_order_sandbox.py:474` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-306 | G4 | ai-slop/python-chained-dict-get | `code/01_platform/06_stage_profiler/stage_profiler.py:599` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-307 | G5 | ai-slop/python-repetitive-dispatch | `code/01_platform/04_scripts/day_run.py:1150` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-308 | G5 | ai-slop/python-repetitive-dispatch | `code/01_platform/04_scripts/implementation_gate.py:300` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-309 | G5 | ai-slop/python-repetitive-dispatch | `code/01_platform/04_scripts/r2_legal_hold_check.py:117` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-310 | G5 | ai-slop/python-repetitive-dispatch | `code/01_platform/04_scripts/r2_legal_hold_check.py:119` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-311 | G5 | ai-slop/python-repetitive-dispatch | `code/01_platform/04_scripts/stage_capture_parse.py:165` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-312 | G5 | ai-slop/python-repetitive-dispatch | `code/01_platform/04_scripts/stage_capture_parse.py:167` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-313 | G5 | ai-slop/python-repetitive-dispatch | `code/01_platform/06_stage_profiler/stage_profiler.py:1550` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-314 | G6 | ai-slop/rust-non-test-unwrap | `code/02_services/04_executor/src/bin/t9_paper_25.rs:118` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-315 | G6 | ai-slop/rust-non-test-unwrap | `code/02_services/04_executor/src/bin/t9_paper_25.rs:137` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-316 | G6 | ai-slop/rust-non-test-unwrap | `code/02_services/04_executor/src/bin/t9_paper_25_full.rs:109` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-317 | G6 | ai-slop/rust-non-test-unwrap | `code/02_services/04_executor/src/bin/t9_paper_25_full.rs:122` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-318 | G6 | ai-slop/rust-non-test-unwrap | `code/02_services/04_executor/src/durable.rs:519` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-319 | G6 | ai-slop/rust-non-test-unwrap | `code/02_services/04_executor/src/durable_file.rs:587` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-320 | G3 | ai-slop/silent-recovery | `code/01_platform/04_scripts/holistic-analyze.py:961` | fixed | cleared | — | **implemented (verified)** |
| AS-321 | G3 | ai-slop/silent-recovery | `code/01_platform/04_scripts/holistic-analyze.py:1282` | fixed | cleared | — | **implemented (verified)** |
| AS-322 | G3 | ai-slop/silent-recovery | `code/01_platform/04_scripts/holistic-analyze.py:1323` | fixed | cleared | — | **implemented (verified)** |
| AS-323 | G3 | ai-slop/silent-recovery | `code/01_platform/04_scripts/holistic-analyze.py:1374` | fixed | cleared | — | **implemented (verified)** |
| AS-324 | G3 | ai-slop/silent-recovery | `code/01_platform/04_scripts/holistic-analyze.py:1425` | fixed | cleared | — | **implemented (verified)** |
| AS-325 | G3 | ai-slop/silent-recovery | `code/01_platform/04_scripts/holistic-analyze.py:1465` | fixed | cleared | — | **implemented (verified)** |
| AS-326 | G3 | ai-slop/silent-recovery | `code/01_platform/04_scripts/t8_sandbox_contract_check.py:47` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-327 | G3 | ai-slop/silent-recovery | `code/01_platform/05_instruments/split_manifest.py:53` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-328 | G7 | ai-slop/thin-wrapper | `code/01_platform/04_scripts/t9_order_sandbox.py:243` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-329 | G7 | ai-slop/unused-import | `code/01_platform/04_scripts/deploy_preflight.py:24` | fixed | cleared | — | **implemented (verified)** |
| AS-330 | G7 | ai-slop/unused-import | `code/01_platform/04_scripts/holistic-analyze.py:38` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-331 | G7 | ai-slop/unused-import | `code/01_platform/04_scripts/holistic-analyze.py:39` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-332 | G7 | ai-slop/unused-import | `code/01_platform/04_scripts/holistic-analyze.py:40` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-333 | G7 | ai-slop/unused-import | `code/01_platform/04_scripts/holistic-analyze.py:41` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-334 | G7 | ai-slop/unused-import | `code/01_platform/04_scripts/holistic-analyze.py:42` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-335 | G7 | ai-slop/unused-import | `code/01_platform/04_scripts/holistic-analyze.py:43` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-336 | G7 | ai-slop/unused-import | `code/01_platform/04_scripts/holistic-analyze.py:44` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-337 | G7 | ai-slop/unused-import | `code/01_platform/04_scripts/holistic-analyze.py:45` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-338 | G9 | ai-slop/todo-stub | `code/common/src/main/java/com/trading/common/schema/fluss/BoundedRetry.java:14` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-339 | G9 | ai-slop/todo-stub | `code/common/src/main/java/com/trading/common/schema/fluss/FlussWriteProfiles.java:17` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-340 | G9 | ai-slop/todo-stub | `code/common/src/main/java/com/trading/common/schema/fluss/FlussWriteProfiles.java:41` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-341 | G9 | ai-slop/todo-stub | `code/common/src/main/java/com/trading/common/schema/fluss/FlussWriteProfiles.java:75` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-342 | G9 | ai-slop/todo-stub | `code/common/src/main/java/com/trading/common/schema/fluss/WriteAwait.java:7` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-343 | G2 | ruff/E401 | `code/01_platform/04_scripts/local_int_004_smoke.py:16` | fixed | cleared | — | **implemented (verified)** |
| AS-344 | G2 | ruff/E401 | `code/01_platform/05_instruments/split_manifest.py:13` | fixed | cleared | — | **implemented (verified)** |
| AS-345 | G2 | ruff/E701 | `code/01_platform/04_scripts/local_int_004_smoke.py:26` | fixed | cleared | — | **implemented (verified)** |
| AS-346 | G2 | ruff/E702 | `code/01_platform/05_instruments/split_manifest.py:33` | fixed | cleared | — | **implemented (verified)** |
| AS-347 | G2 | ruff/E702 | `code/01_platform/05_instruments/split_manifest.py:39` | fixed | cleared | — | **implemented (verified)** |
| AS-348 | G2 | ruff/E731 | `code/01_platform/04_scripts/o2-provision.py:1813` | fixed | cleared | — | **implemented (verified)** |
| AS-349 | G2 | ruff/E741 | `code/01_platform/04_scripts/check_flink_properties.py:87` | fixed | cleared | — | **implemented (verified)** |
| AS-350 | G2 | ruff/E741 | `code/01_platform/04_scripts/plan_tracker.py:35` | fixed | cleared | — | **implemented (verified)** |
| AS-351 | G2 | ruff/E741 | `code/01_platform/04_scripts/plan_tracker.py:58` | fixed | cleared | — | **implemented (verified)** |
| AS-352 | G2 | ruff/F401 | `code/01_platform/01_docker/alert-consumer.py:38` | fixed | cleared | — | **implemented (verified)** |
| AS-353 | G2 | ruff/F401 | `code/01_platform/04_scripts/deploy_preflight.py:24` | fixed | cleared | — | **implemented (verified)** |
| AS-354 | G2 | ruff/F401 | `code/01_platform/04_scripts/image_staleness_check.py:63` | fixed | cleared | — | **implemented (verified)** |
| AS-355 | G2 | ruff/F541 | `code/01_platform/04_scripts/alert-routing-selftest.py:93` | fixed | cleared | — | **implemented (verified)** |
| AS-356 | G2 | ruff/F541 | `code/01_platform/04_scripts/alert-routing-selftest.py:177` | fixed | cleared | — | **implemented (verified)** |
| AS-357 | G2 | ruff/F541 | `code/01_platform/04_scripts/fused_timeline.py:223` | fixed | cleared | — | **implemented (verified)** |
| AS-358 | G2 | ruff/F541 | `code/01_platform/04_scripts/stale_table_kind_scan.py:454` | fixed | cleared | — | **implemented (verified)** |
| AS-359 | G2 | ruff/F821 | `code/01_platform/04_scripts/alert-routing-selftest.py:50` | fixed | cleared | — | **implemented (verified)** |
| AS-360 | G2 | ruff/F841 | `code/01_platform/04_scripts/day_run.py:759` | fixed | cleared | — | **implemented (verified)** |
| AS-361 | G2 | ruff/F841 | `code/01_platform/04_scripts/env_facts.py:48` | fixed | cleared | — | **implemented (verified)** |
| AS-362 | G2 | ruff/F841 | `code/01_platform/04_scripts/holistic-analyze.py:866` | fixed | cleared | — | **implemented (verified)** |
| AS-363 | G2 | ruff/F841 | `code/01_platform/04_scripts/holistic-analyze.py:1311` | fixed | cleared | — | **implemented (verified)** |
| AS-364 | G2 | ruff/F841 | `code/01_platform/04_scripts/holistic-analyze.py:1652` | fixed | cleared | — | **implemented (verified)** |
| AS-365 | G10 | complexity/deep-nesting | `code/01_platform/04_scripts/cp_phase_capture.py:208` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-366 | G10 | complexity/deep-nesting | `code/01_platform/04_scripts/ddl_apply_smoke.py:352` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-367 | G10 | complexity/deep-nesting | `code/01_platform/04_scripts/fluss-probes/CandleFeaturesTableProbe.java:183` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-368 | G10 | complexity/deep-nesting | `code/01_platform/04_scripts/fluss-probes/CandleVerify.java:57` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-369 | G10 | complexity/deep-nesting | `code/01_platform/04_scripts/fluss-probes/FlussKvScanStrategy.java:151` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-370 | G10 | complexity/deep-nesting | `code/01_platform/04_scripts/fluss-probes/FlussPrefixReader.java:92` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-371 | G10 | complexity/deep-nesting | `code/01_platform/04_scripts/holistic-analyze.py:714` | won't-fix | present | REAL | **fixed — CHG-538/539 (finding remains accepted)** |
| AS-372 | G10 | complexity/deep-nesting | `code/02_services/01_ingestion/go-bridge/faketool/main.go:37` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-373 | G10 | complexity/deep-nesting | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:611` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-374 | G10 | complexity/deep-nesting | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java:699` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-375 | G10 | complexity/deep-nesting | `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/FlussControlStateStore.java:99` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-376 | G10 | complexity/deep-nesting | `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:212` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-377 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/audit_r2.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-378 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/cluster_check.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-379 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/day_run.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-380 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/ddl_apply.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-381 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/ddl_apply_smoke.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-382 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/disaster_drills.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-383 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/docs_audit.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-384 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/eod_schedule.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-385 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/fluss-probes/FeatureSpikeProbe.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-386 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/fluss-probes/FlussReadabilityProbe.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-387 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/fluss-probes/FlussSignalLatency.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-388 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/fluss-probes/SignalCandidatesViewer.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-389 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/gate_preflight.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-390 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/holistic-analyze.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-391 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/image_staleness_check.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-392 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/o2-provision.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-393 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/perf_evidence_parse.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-394 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/prod_node_check.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-395 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/r2_legal_hold_check.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-396 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/stage_capture_parse.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-397 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/stale_table_kind_scan.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-398 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/t9_order_sandbox.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-399 | G11 | complexity/file-too-large | `code/01_platform/04_scripts/values_at_rest_scan.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-400 | G11 | complexity/file-too-large | `code/01_platform/06_stage_profiler/stage_profiler.py:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-401 | G11 | complexity/file-too-large | `code/02_services/01_ingestion/go-bridge/main.go:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-402 | G11 | complexity/file-too-large | `code/02_services/01_ingestion/go-bridge/marketdata/market_data.pb.go:0` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-403 | G11 | complexity/file-too-large | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-404 | G11 | complexity/file-too-large | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:0` | deferred | present | POLICY | no action — deferred by design |
| AS-405 | G11 | complexity/file-too-large | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-406 | G11 | complexity/file-too-large | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-407 | G11 | complexity/file-too-large | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-408 | G11 | complexity/file-too-large | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-409 | G11 | complexity/file-too-large | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/N7RangeBreakoutStrategy.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-410 | G11 | complexity/file-too-large | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-411 | G11 | complexity/file-too-large | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-412 | G11 | complexity/file-too-large | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/StrategyHostFunction.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-413 | G11 | complexity/file-too-large | `code/02_services/04_executor/src/bridge/transport.rs:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-414 | G11 | complexity/file-too-large | `code/02_services/04_executor/src/execution/client.rs:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-415 | G11 | complexity/file-too-large | `code/02_services/04_executor/src/executiongate.rs:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-416 | G11 | complexity/file-too-large | `code/02_services/04_executor/src/http.rs:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-417 | G11 | complexity/file-too-large | `code/02_services/04_executor/src/projection/mod.rs:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-418 | G11 | complexity/file-too-large | `code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-419 | G11 | complexity/file-too-large | `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/FlussProjectionWriter.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-420 | G11 | complexity/file-too-large | `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/GatewayHttpServer.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-421 | G11 | complexity/file-too-large | `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-422 | G11 | complexity/file-too-large | `code/common/src/main/java/com/trading/common/schema/eod/EodControllerTool.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-423 | G11 | complexity/file-too-large | `code/common/src/main/java/com/trading/common/schema/execution/FlussGateStateStore.java:0` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-424 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/alert-routing-selftest.py:110` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-425 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/cluster_check.py:561` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-426 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/day_run.py:418` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-427 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/ddl_apply.py:721` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-428 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/ddl_apply_smoke.py:460` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-429 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/disaster_drills.py:453` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-430 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/docs_audit.py:652` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-431 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/eod_schedule.py:321` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-432 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/fluss-probes/CandleVerify.java:57` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-433 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/fluss-probes/EventDayProbe.java:53` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-434 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/fluss-probes/FlussPrefixReader.java:92` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-435 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/fluss-probes/RawCompressionProbe.java:153` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-436 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/fused_timeline.py:231` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-437 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/gate_preflight.py:357` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-438 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/holistic-analyze.py:714` | deferred | present | POLICY | no action — deferred by design |
| AS-439 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/o2-provision.py:1513` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-440 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/seed_alerts.py:70` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-441 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/stale_table_kind_scan.py:610` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-442 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/t8_sandbox_contract_check.py:77` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-443 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/t9_order_sandbox.py:452` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-444 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/t9_order_sandbox.py:918` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-445 | G11 | complexity/function-too-long | `code/01_platform/04_scripts/t9_order_sandbox.py:1176` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-446 | G11 | complexity/function-too-long | `code/01_platform/06_stage_profiler/stage_profiler.py:1317` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-447 | G11 | complexity/function-too-long | `code/02_services/01_ingestion/go-bridge/cmd/gen-corpus/main.go:269` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-448 | G11 | complexity/function-too-long | `code/02_services/01_ingestion/go-bridge/faketool/main.go:37` | deferred | present | POLICY | no action — deferred by design |
| AS-449 | G11 | complexity/function-too-long | `code/02_services/01_ingestion/go-bridge/main.go:103` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-450 | G11 | complexity/function-too-long | `code/02_services/01_ingestion/go-bridge/main.go:404` | won't-fix | present | REAL | **fixed — CHG-538/539 (finding remains accepted)** |
| AS-451 | G11 | complexity/function-too-long | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:408` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-452 | G11 | complexity/function-too-long | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:611` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-453 | G11 | complexity/function-too-long | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:1675` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-454 | G11 | complexity/function-too-long | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2161` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-455 | G11 | complexity/function-too-long | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java:127` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-456 | G11 | complexity/function-too-long | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:188` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-457 | G11 | complexity/function-too-long | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:103` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-458 | G11 | complexity/function-too-long | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:516` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-459 | G11 | complexity/function-too-long | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java:90` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-460 | G11 | complexity/function-too-long | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java:486` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-461 | G11 | complexity/function-too-long | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java:699` | deferred | present | POLICY | no action — deferred by design |
| AS-462 | G11 | complexity/function-too-long | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java:1226` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-463 | G11 | complexity/function-too-long | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java:147` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-464 | G11 | complexity/function-too-long | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java:504` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-465 | G11 | complexity/function-too-long | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java:189` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-466 | G11 | complexity/function-too-long | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/StrategyHostFunction.java:312` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-467 | G11 | complexity/function-too-long | `code/02_services/04_executor/src/bridge/protocol.rs:663` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-468 | G11 | complexity/function-too-long | `code/02_services/04_executor/src/durable_file.rs:728` | false-positive | present | NOT-REAL | no action — not a defect in context |
| AS-469 | G11 | complexity/function-too-long | `code/02_services/06_execution_bridge/go-bridge/postback.go:68` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-470 | G11 | complexity/function-too-long | `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/ExecutionGatewayMain.java:72` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-471 | G11 | complexity/function-too-long | `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/GatewayHttpServer.java:154` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-472 | G11 | complexity/function-too-long | `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:212` | deferred | present | POLICY | no action — deferred by design |
| AS-473 | G11 | complexity/function-too-long | `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:1394` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-474 | G11 | complexity/function-too-long | `code/common/src/main/java/com/trading/common/schema/ddl/DdlText.java:129` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-475 | G11 | complexity/function-too-long | `code/common/src/main/java/com/trading/common/schema/eod/EodControllerTool.java:621` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-476 | G11 | complexity/function-too-long | `code/common/src/main/java/com/trading/common/schema/execution/ExecutionCommandGate.java:138` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-477 | G11 | complexity/function-too-long | `code/common/src/main/java/com/trading/common/schema/position/PositionProjector.java:77` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-478 | G11 | complexity/function-too-long | `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:77` | won't-fix | present | POLICY | no action — deliberate/accepted |
| AS-479 | G10 | complexity/too-many-params | `code/01_platform/04_scripts/env_facts.py:116` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-480 | G10 | complexity/too-many-params | `code/01_platform/04_scripts/stale_table_kind_scan.py:541` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-481 | G10 | complexity/too-many-params | `code/01_platform/04_scripts/stale_table_kind_scan.py:554` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-482 | G10 | complexity/too-many-params | `code/01_platform/04_scripts/stale_table_kind_scan.py:581` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-483 | G10 | complexity/too-many-params | `code/01_platform/04_scripts/t9_order_sandbox.py:218` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-484 | G10 | complexity/too-many-params | `code/01_platform/04_scripts/t9_order_sandbox.py:247` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-485 | G10 | complexity/too-many-params | `code/02_services/01_ingestion/go-bridge/main.go:358` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-486 | G10 | complexity/too-many-params | `code/02_services/01_ingestion/go-bridge/main.go:404` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-487 | G10 | complexity/too-many-params | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:213` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-488 | G12 | python-formatting | `code/01_platform/01_docker/alert-consumer.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-489 | G12 | python-formatting | `code/01_platform/04_scripts/alert-routing-selftest.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-490 | G12 | python-formatting | `code/01_platform/04_scripts/audit_r2.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-491 | G12 | python-formatting | `code/01_platform/04_scripts/catalog_drift.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-492 | G12 | python-formatting | `code/01_platform/04_scripts/change_control_check.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-493 | G12 | python-formatting | `code/01_platform/04_scripts/check_flink_properties.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-494 | G12 | python-formatting | `code/01_platform/04_scripts/clean_break_drill.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-495 | G12 | python-formatting | `code/01_platform/04_scripts/cluster_check.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-496 | G12 | python-formatting | `code/01_platform/04_scripts/compose_config_redact.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-497 | G12 | python-formatting | `code/01_platform/04_scripts/cp_phase_capture.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-498 | G12 | python-formatting | `code/01_platform/04_scripts/day_run.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-499 | G12 | python-formatting | `code/01_platform/04_scripts/ddl_apply.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-500 | G12 | python-formatting | `code/01_platform/04_scripts/ddl_apply_smoke.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-501 | G12 | python-formatting | `code/01_platform/04_scripts/deploy_preflight.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-502 | G12 | python-formatting | `code/01_platform/04_scripts/deployed_artifact_verify.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-503 | G12 | python-formatting | `code/01_platform/04_scripts/disaster_drills.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-504 | G12 | python-formatting | `code/01_platform/04_scripts/docs_audit.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-505 | G12 | python-formatting | `code/01_platform/04_scripts/env_facts.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-506 | G12 | python-formatting | `code/01_platform/04_scripts/eod_controller.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-507 | G12 | python-formatting | `code/01_platform/04_scripts/eod_schedule.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-508 | G12 | python-formatting | `code/01_platform/04_scripts/evidence_ownership_check.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-509 | G12 | python-formatting | `code/01_platform/04_scripts/execution_network_check.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-510 | G12 | python-formatting | `code/01_platform/04_scripts/fluss-client-metrics.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-511 | G12 | python-formatting | `code/01_platform/04_scripts/fluss-repair/LogScan.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-512 | G12 | python-formatting | `code/01_platform/04_scripts/fluss-repair/verify-and-truncate.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-513 | G12 | python-formatting | `code/01_platform/04_scripts/fused_timeline.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-514 | G12 | python-formatting | `code/01_platform/04_scripts/gate_memo.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-515 | G12 | python-formatting | `code/01_platform/04_scripts/gate_preflight.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-516 | G12 | python-formatting | `code/01_platform/04_scripts/holistic-analyze.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-517 | G12 | python-formatting | `code/01_platform/04_scripts/image_staleness_check.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-518 | G12 | python-formatting | `code/01_platform/04_scripts/implementation_gate.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-519 | G12 | python-formatting | `code/01_platform/04_scripts/ing-tcp001/reconcile-compare.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-520 | G12 | python-formatting | `code/01_platform/04_scripts/jfr-analyze.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-521 | G12 | python-formatting | `code/01_platform/04_scripts/latency_probe.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-522 | G12 | python-formatting | `code/01_platform/04_scripts/local_int_004_smoke.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-523 | G12 | python-formatting | `code/01_platform/04_scripts/o2-provision.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-524 | G12 | python-formatting | `code/01_platform/04_scripts/o2_ingest.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-525 | G12 | python-formatting | `code/01_platform/04_scripts/perf_evidence_parse.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-526 | G12 | python-formatting | `code/01_platform/04_scripts/pernode_attribution_check.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-527 | G12 | python-formatting | `code/01_platform/04_scripts/placement_check.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-528 | G12 | python-formatting | `code/01_platform/04_scripts/plan_tracker.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-529 | G12 | python-formatting | `code/01_platform/04_scripts/pom-snapshot-scan.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-530 | G12 | python-formatting | `code/01_platform/04_scripts/prod_node_check.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-531 | G12 | python-formatting | `code/01_platform/04_scripts/r2_archive_selection.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-532 | G12 | python-formatting | `code/01_platform/04_scripts/r2_archive_sync.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-533 | G12 | python-formatting | `code/01_platform/04_scripts/r2_legal_hold_check.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-534 | G12 | python-formatting | `code/01_platform/04_scripts/seed_alerts.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-535 | G12 | python-formatting | `code/01_platform/04_scripts/seed_dashboards.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-536 | G12 | python-formatting | `code/01_platform/04_scripts/skip_inventory.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-537 | G12 | python-formatting | `code/01_platform/04_scripts/soak-o2-evidence.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-538 | G12 | python-formatting | `code/01_platform/04_scripts/stage_capture_parse.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-539 | G12 | python-formatting | `code/01_platform/04_scripts/stage_gc_summary.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-540 | G12 | python-formatting | `code/01_platform/04_scripts/stale_table_kind_scan.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-541 | G12 | python-formatting | `code/01_platform/04_scripts/strategy_live_board.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-542 | G12 | python-formatting | `code/01_platform/04_scripts/t8_sandbox_contract_check.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-543 | G12 | python-formatting | `code/01_platform/04_scripts/t9_order_sandbox.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-544 | G12 | python-formatting | `code/01_platform/04_scripts/tablet-orphan-sweep.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-545 | G12 | python-formatting | `code/01_platform/04_scripts/values_at_rest_scan.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-546 | G12 | python-formatting | `code/01_platform/04_scripts/version_matrix_verify.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-547 | G12 | python-formatting | `code/01_platform/05_instruments/split_manifest.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
| AS-548 | G12 | python-formatting | `code/01_platform/06_stage_profiler/stage_profiler.py:0` | false-positive | present | POLICY | no action — deliberate/accepted |
