# Audit findings remediation — 2026-10-01 full-project audit

**Created:** 2026-10-01 · **Status:** all batches landed (CHG-495…500) and certified — final
19-step gate **PASS 19/19** (`logs/soak/monday-gates-20261001-190238`, tree `7057e4b2`).
H1 (push/tag) and the gitbutler-branch decision await the operator.

**Source:** full-project audit 2026-10-01 (four domain auditors + a repo-hygiene pass); operator
approved fixing all HIGH + medium/low findings, with one gate at the end.

## HIGH findings

| # | Finding (short) | Batch | State |
|---|---|---|---|
| H1 | remotes stale; 312+ commits local-only; no tag since 2026-09-23 | 5 | **operator decision** |
| H2 | contracts said forming-row upserts (superseded by CHG-486 closed-only) | 1 | landed CHG-495 |
| H3 | 20+ dossier evidence paths missing from disk | 4 | reconciled CHG-498 (evidence unrecoverable; claims marked) |
| H4 | two `logs/tracker-14/` files are 68-byte C14 placeholders cited as proof | 4 | reconciled CHG-498 |
| H5 | CHG-055 absent; cited commits unreachable | 4 | reconciled CHG-498 (tombstone) |
| H6 | `run-ingestion-full.sh` ran the retired HFT feed | 2 | landed CHG-496 |
| H7 | soak overlay hardcoded `/home/saurabh/...` manifest source | 2 | landed CHG-496 |
| H8 | gate-forced `ManifestLoadTest` hardcoded `/home/saurabh/...` | 2 | landed CHG-496 |
| H9 | daily-VM guide lacked a firewall step; 9 ports on 0.0.0.0 | 3 | landed CHG-497 |
| H10 | requirements presented the retired single-TF/forming-bar design as current | 1 | landed CHG-495 |
| H11 | architecture said Executor → Arrow REST directly; showed retired action-capture | 1 | landed CHG-495 |
| H12 | four remediation plans said "awaiting approval; no code changed" | 1 | landed CHG-495 |
| H13 | KPI plan said "nothing implemented" the day it landed | 1 | landed CHG-495 |

## Medium/low findings (same audit)

**Docs sweep — CHG-499:** start-here Phase 3 (DDL files, TradeDecisions, backpressure);
execution-dossier status/test counts + T4 rows; quality-targets approver + capture signal;
runbook candle env keys; networking deploy posture; `raw_table_1` retention 3 d; service counts
21/13; gate duration ~23–25 min; FACT-019/020 provenance (run 35875331208 / `2e7993ec`); Action
Capture retirement markers (6 pages); 8 stale plan status lines; pin-carrier plan note.

**Evidence honesty — CHG-498:** signal-dossier "Evidence retention" section + citation markers;
CHG-055 tombstone; CHG-022 verification closed (DEC-040); 8 historical records annotated for
absent evidence.

**Code/config — CHG-500:** `run-ingestion-full` LOG_DIR; soak journal defaults; dead
`POSITION_STATE_TABLE` removed; `INSTRUMENT_MANIFEST_VERSION` documented; local `.env` dead EOD
keys removed; cwd-relative evidence defaults fixed (clean-break/disaster/stage-capture, plus
`JOBGRAPH_OUT_DIR`); `prod_vms.example.json` key placeholder; HTTPS collector support (P2-191);
`marketdata-capture.jsonl` untracked + ignored; `change_control_check` now requires
`change_record_id` (+ CHG-149…159 repaired).

**Accepted, no change (with reason):**

- Executor venue-wide stubs (`submit_order_list`, `cancel_all_orders`, `query_account`) — T4
  scope, fail-loud with comments; implementing them is a product decision, not a fix.
- CHG id gaps 034/057–061 — never referenced anywhere except 055 (tombstoned); gaps left as-is.
- `code/logs/` stray dir — gitignored local artifact; soak tool defaults fixed instead.
- CHG date inversions (CHG-231/244) — recorded history, left untouched.

## Pending operator decisions

1. **H1 push/tag** — nothing is pushed; `origin` and `local-backup` are both at `cffb148e`
   (2026-09-24). This is the only unrecoverable-failure-risk item.
2. **Resolved 2026-10-01:** the stale `streaming_project_p6` worktree was backed up (commit
   `c6e01135` → `local-backup` `archive/p6-instrument-import-20260919` + 28K bundle + 19K logs
   tarball) and then removed with branch `p6-independent-waves`; 3.8 GB freed.
3. **Stale `gitbutler/*` branches** (2026-08-23/26) — deletion needs explicit approval.

## Verification

- Guards added/updated this remediation: `test_audit_high_batch2.py` (3),
  `test_audit_medium_low.py` (11), `test_vm_golden_recipe.py` (12 incl. the H9 guard),
  `test_change_control_check.py` (updated) — failing-first for every new guard.
- `make docs-audit` all pass; `make static-check` 0 failures; `make test-09` 68 passed;
  `make test` green (batch 2).
- **Final 19-step gate: PASS 19/19** — `logs/soak/monday-gates-20261001-190238` (tree
  `7057e4b2`), 2026-10-01; gate result recorded in
  `logs/audit-remediation-20261001/checks.txt`. The earlier partial run
  (`monday-gates-20261001-182817`) is marked `ABORTED.txt`.
