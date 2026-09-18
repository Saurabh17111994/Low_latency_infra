# Wave 38 — Stage capture and baseline — implementation plan

- **Status:** approved 2026-09-17, queued behind the running gate sweep
- **Source:** audit `p6-ops-scripts-tests-chaos-audit.md` Wave 38 (23 findings, 2 ticked, 21 open)
- **Scope verification:** all 21 open findings reproduced byte-for-byte against
  main's tree 2026-09-17 (read-only); 0 stale, 0 unclear. Both ticks
  (P6-562 @ec2b6ff1, P6-863 @520b8488) carry evidence and stand.
- **Rule:** no code edits until the gate sweep finishes — its measurement steps
  execute these exact scripts. This file is inert to the gate (no step reads
  `docs/plans/`).

## Code map (current lines — the audit's L-numbers predate ~120 lines of drift)

- `code/01_platform/04_scripts/stage-a2-baseline.sh` (~185 lines,
  `set -uo pipefail`, NO `-e` @L23, zero test coverage): 9 findings in
  4 regions — header L31–76 (552 run-dir, 553 uptime, 554 compose), job
  section L99–146 (210 wait loop, 211 G24, 212 handoff), floor L150–169
  (555 inputs, 213 math), tail (556 success-by-`ls`).
- `code/01_platform/04_scripts/stage-capture.sh` (~880 lines): 12 findings in
  6 regions — header L37–39 (790 validation), scrape L390–396 (214+215 share
  one 7-line hunk), matcher L479–488 (557 heredoc python), offset L537–607
  (558+559 share one block), probes L614–640 (560+561 adjacent), tail L774 /
  L816 / L858 (563 age, 564 checkpoints, 565 freshness).
- Test precedent: `tests/test_stage_capture_teardown_wave38.py` (Sandbox =
  real script + stubbed deps, covers 562/863). Copy the pattern; do not extend
  that file (teardown-lifecycle scope).

## Commits (region-sliced; each: code + tests + CHG record, failing-first)

| # | Files / region | Findings | Tests |
|---|---|---|---|
| 1 | baseline L31–76 | 552 PID-suffixed run-dir, 553 uptime validation, 554 `$COMPOSE` reuse | new `test_stage_a2_baseline_wave38.py`: concurrent run-dirs differ; `MIN_UPTIME_S=abc` + missing /proc fail loudly; bring-up invokes lib `$COMPOSE` (stub records argv) |
| 2 | baseline L99–146 | 210 JOB_ID guard, 211 mount-verified G24, 212 env-forwarding handoff | same file: empty/malformed JOB_ID fatals before curl; G24 passes only on mount (stub `compose exec` with/without mount line); handoff argv carries REST/rate vars |
| 3 | baseline L150–end | 555 floor-input checks, 213 per-vertex floor math, 556 evidence gate | same file: bad pct/duration fatal; multi-vertex + empty capture → INSUFFICIENT DATA not 0; header-only TSV refuses success |
| 4 | capture L390–396 + L700–704 | 214 curl/grep split, 215 `return` not `exit`, 216 debounce | new `test_stage_capture_wave38.py`: alive-but-empty metrics warns-not-dies; tick failure resumes loop; single REST blip survives, 3 consecutive fail loud with diagnostics |
| 5 | capture L537–607 | 558 separate sentinel, 559 numeric-only offset advance | same file: missing java.out → sentinel file (not offset); corrupt offset → 0; failed parse leaves offset untouched |
| 6 | capture L614–640 | 560 `timeout`+stderr probes, 561 `PROBE_DB`/`PROBE_RAW_TABLE` params | same file: hung probe bounded at 20s with per-tick err log; custom table reaches probe argv |
| 7 | capture L479–488, L774, L816, L858, L37–39 | 557 prefix matcher, 563 full-date age, 564 new-ids-only checkpoints, 565 scaled freshness, 790 duration validation | same file: trailing-dot want matches; midnight rollover age correct; rerun appends no dup ids; 25s floor with 5x scaling; `DURATION_S=abc` fails fast |

## Assessed risks

1. **P6-211 stub fidelity** — fake `mount` output inside stubbed `compose exec`
   must mirror real format; keep the old path-check as an inner fallback so a
   stub-shaped test can't bless a broken prod check.
2. **P6-212 forwarding list** — must equal what capture reads; re-grep capture's
   env reads at implementation time (`RATE_HZ` read still to confirm).
3. **P6-216 posture** — debounce softens fail-fast; final failure must still
   fire `capture_stall_diagnostics` loudly (assert in test).
4. **P6-556 strictness** — `<2 data rows` now fails; legitimate short/debug runs
   must pass an explicit short-duration flag, not rely on leniency (document).

## Verification per commit

- `bash -n` + `shellcheck -S warning` on both scripts; new tests green;
  failing-first shown (test fails on pre-fix script, passes after); 2–3
  mutations per commit caught (kill-0 style precedents); neighbours green
  (`test_stage_capture_parse.py`, `teardown_wave38`, pipeline-lib guards
  untouched); `change_control_check.py` all pass.

## Closeout

- Tick all 23 in the audit file with `> **Fixed:**` + commit per finding (the
  2 pre-ticked keep their notes); run `p6_map_refresh.py --write`, expect
  wave-38 count +21 (2 already counted).
- Then: Wave-41 port (queued), live-stack bench redesign, external closeout.
