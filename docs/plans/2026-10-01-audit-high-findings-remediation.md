# Audit HIGH-findings remediation — 2026-10-01 full-project audit

**Created:** 2026-10-01 · **Status:** in progress — Batches 1–2 landed (CHG-495/496); Batch 3–4
open; H1 awaits the operator push decision.
**Source:** full-project audit 2026-10-01 (read-only; four domain auditors + a repo-hygiene
pass). Findings H1–H13 as reported to the operator; operator approved the batch plan.

## Findings and batches

| # | Finding (short) | Batch | State |
|---|---|---|---|
| H1 | remotes stale; 312 commits local-only; no tag since 2026-09-23 | 5 (operator decision) | open |
| H2 | contracts said forming-row upserts (superseded by CHG-486 closed-only) | 1 | landed CHG-495 |
| H3 | 20+ dossier evidence paths missing from disk | 4 | open |
| H4 | two `logs/tracker-14/` files are 68-byte placeholders cited as proof | 4 | open |
| H5 | CHG-055 absent; its cited commits are unreachable objects | 4 | open |
| H6 | `run-ingestion-full.sh` runs the retired HFT feed (no `ARROW_FEED`) | 2 | landed CHG-496 |
| H7 | soak overlay hardcodes `/home/saurabh/...` manifest source | 2 | landed CHG-496 |
| H8 | gate-forced `ManifestLoadTest` hardcodes `/home/saurabh/...` | 2 | landed CHG-496 |
| H9 | daily-VM guide lacks a firewall/tunnel step; 9 ports on 0.0.0.0 | 3 | open |
| H10 | requirements presented the retired single-TF/forming-bar design as current | 1 | landed CHG-495 |
| H11 | architecture said Executor → Arrow REST directly; showed retired action-capture | 1 | landed CHG-495 |
| H12 | four remediation plans said "awaiting approval; no code changed" after landing | 1 | landed CHG-495 |
| H13 | KPI plan said "nothing implemented" the day it landed | 1 | landed CHG-495 |

## Batch plan

1. **Docs truth** (H2, H10, H11, H12, H13) — annotations/corrections only; no requirement ids
   removed. Verify: `make docs-audit` + `make static-check` + both plan trackers.
2. **Code/config portability** (H6, H7, H8) — small code changes with failing-first tests;
   verify with the full `make gate`.
3. **Daily-VM security** (H9) — guide section + guard test; the firewall rule itself is
   applied on VM day.
4. **Evidence honesty** (H3, H4, H5) — annotate the dossier/citing docs; the evidence itself
   is NOT recoverable (checked: git objects, `local-backup`, the `streaming_project_p6`
   worktree). No fabricated replacements.
5. **Push/tag** (H1) — operator decision only; nothing pushed without it.

## Risks

- Requirements/contracts are spec authority: edits correct or date-stamp stale current-state
  claims; normative SHALL/MUST structure and ids are preserved.
- The code batch touches the Monday-gate path (H8) — the full gate must follow before the
  batch is called done.
- H3–H5 only remove false confidence; they do not restore lost evidence.
