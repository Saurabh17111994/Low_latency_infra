# Plan execution protocol - Fluss 1.0 upgrade + native adoption

**Created:** 2026-09-24 · **Applies to:** `2026-09-22-fluss-1.0-upgrade.md` and
`2026-09-22-fluss-1.0-native-adoption.md` (and any future `docs/plans/*.md` carrying a
`## 0. Live tracker`).

**Standing order:** the root `AGENTS.md` points here. When the operator says to implement an item
(e.g. `A2`, `B16-L3`, `F8-4`) or a group ("Wave 0", "the offline ones") from either plan, apply this
protocol without being asked to restate it. The minimal instruction is an item ID; everything else
(rules, guards, marker mechanics) is read from here.

**Where the truth lives.** §0 of each plan is the status (single source of truth); the batch/stage
sections are the evidence. Each item's block carries `GIVES YOU` / `FIT` / `COST` / `ACTION` /
`WRONG IF` - `ACTION` is the next step, `WRONG IF` is the falsifier. Where §0 and the body disagree,
§0 wins.

## Handoff

The operator names an item or a group. The agent reads the tracker line + the item block, re-verifies
the premise, and reports a short plan (what changes, which test, what evidence, which approvals are
needed) before changing anything. No approval is needed for the report itself.

## Rules, in order

1. **Verify first, trust no marker.** Re-check the premise against the tree or live state (read-only)
   before implementing. No item moves from `[ ]`/`[L]` to `[x]` without a measurement or state check.
   If the premise is stale, correct the tracker and stop.
2. **Classify by marker.** `[ ]` = offline action, do it. `[L]` = window work: prepare the probe or
   change plus state-based post-conditions and the revert; **the operator approves the window before
   anything touches a live cluster**. `[?]` = decision: present options + recommendation, the
   operator decides, then it becomes `[ ]`/`[L]`. Recreate-class items (`A3`, `A4`, `table.kv.ttl`,
   any lake-table recreation) additionally need a signed-off refill/rebuild plan.
3. **One change per window, revert scripted first.** Post-conditions are asserted from state (ZK
   `standby_replicas`, `Admin.getTableInfo`, row counts) - never exit codes.
4. **Smoke first, then the real run.** The root `AGENTS.md` testing workflow applies (smoke test
   passes before the actual test; no artificial sleeps >30s). The certifying gate runs only on a
   clean, committed tree; a change after a certificate means the next claim needs a new certifying
   gate (scoped `--steps` first, full gate to certify).
5. **Land per the wave discipline.** Every code adoption ships with a test that auto-joins the gate
   (`code/01_platform/04_scripts/tests/test_*.py` - gate step 3 auto-discovers it), one CHG record
   per commit in `docs/05_deployment/change-records/`, the dossier/doc update in the same change,
   and the marker flip.
6. **Update the marker.** Flip the §0 tracker line (`- [x] **ID** ...`) and, where the plan mirrors
   markers in the body (the native-adoption plan does: heading prefix `### [x] ID - ...` or the
   `STATUS:` line inside its template block), flip the body marker too. Then regenerate and check:
   `python3 code/01_platform/04_scripts/plan_tracker.py --plan <plan> --write` and `--check`.
   `[~]` while in progress; `[x]` only with evidence.
7. **Operator approval required for:** live/prod/4-VM state changes; recreate-class changes and
   anything that could end rollback (`T7.4`); deleting or rewriting evidence artifacts; closing a
   `[?]` decision; commits.

## Guardrails

- **Recreate-class is the danger zone.** `A3`, `A4`, `table.kv.ttl` and the clean-vs-legacy lake
  question interact: adopting one can end rollback (`T7.4`) or need a refill path that does not
  exist. Resolve `B34-F4` before any lake-enabled table is recreated.
- **Never implement in batch order.** Batch order is survey order. The dependency chains are
  `A1` probe → `A2` → `B3-L1`; `T5.2` → `A3`; DR reconstruction → `A4`. Everything else is
  schedulable by cost and window availability.

## Reporting

Each item reports: what changed, evidence (file / commit / log), the new marker state, and what
remains. The §0 roll-up is the progress metric.
