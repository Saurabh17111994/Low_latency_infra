# DDL-apply timing and the concurrency question

**Date:** 2026-09-18 · **Status:** measured — no gate change made from it yet · **Source:** todo #31, and
section 6 of the wave-36/38 evidence report.

The premise this work started from was that gate step 11 (17 m 16 s in run 6) is serial *work* — "216
live create/write/verify/drop cycles … work whose entire purpose is to prove the DDL surface against a
real cluster" — and that running applies concurrently might cut it. **Measured answer: no.** The time
is a *wait*, and concurrency makes it worse.

## What was measured

### Phase 1 — one full apply, quiet cluster

`--apply-verified` with `DDL_APPLY_SKIP_SMOKE=1` and `DDL_APPLY_TABLE_PREFIX=bench1_` (scratch tables
only), every output line timestamped:

- **wall 19.6 s**, of which:
  - ~5 s of actual create/verify/drop work;
  - **14 s waiting**: `27 scratch table(s) dropped — waiting until the cluster can place a fresh
    replica again (probe …, budget 25m; teardown runs ~1.5 s per bucket)` → `scratch teardown drained
    after 14s — fresh replicas place again`.
- `DDL-APPLY-RESULT: PASS exit=0`, evidence record `status PASS`.

### Phase 2 — N concurrent applies on disjoint prefixes

| level | wall | applies | failures | cluster storm/NotLeader/ERROR | catalog after | min available MB |
|---|---|---|---|---|---|---|
| 1 | **500 s** | 1 | 0 | 0/0/0 | 33 | 7366 |
| 2 | 63 s | 2 | **1** | 0/0/0 | 33 | 7242 |
| 4 / 8 / 12 | not run — deliberately abandoned, see below | | | | | |

Two results, both decisive:

1. **Level 1 was the same apply as Phase 1, 32× slower** — `still draining after 430s` → `drained
   after 455s`. Nothing about the work changed; it inherited a backlog (Phase 1's own 27-table drop
   plus a scratch-table cleanup). The wait is *inherited churn*, so parallelising the work cannot
   shorten it.
2. **Concurrency 2 already fails an apply**: `FATAL — null / java.util.concurrent.TimeoutException`
   at `DdlApplyTool.java:412`, which is `admin.getTableInfo(…).get(TIMEOUT = 30 s)` — a **metadata
   read**, not a write. The cluster logged **no** `TableNotExistException`, **no** `NotLeader`, **no**
   `ERROR` lines in that window, so the load shows up as latency that the tool's own 30 s per-operation
   timeout converts into a hard failure.

Levels 4/8/12 were stopped on purpose, not by a timeout: with a hard failure already at 2, further
levels could only add "worse", at up to 25 minutes each. Recorded as a deliberate truncation.

### The mechanism, from the tool's own documented measurements

- `DRAIN_BUDGET = 25 min`; `TIMEOUT = 30 s` per operation.
- A dropped table's buckets are torn down ~1.5 s apart (~0.65 buckets/s, the remote-log cleanup step,
  whose remote store here is R2), and *while that queue drains the coordinator does not bring up new
  replicas*: the code records a table created right after an apply having no replica for 8 minutes.
- A full apply drops ~54 scratch tables (~540 buckets) ⇒ "a healthy drain is on the order of 10-15
  min".
- On budget expiry the drain canary is **kept**, deliberately: dropping a table under a pending batch
  spins the client Sender — the run-4b storm family.
- Step 11 runs **three** such applies (S1 full PASS, S2 no-ack, S4 container bad-ownership) against
  scratch-prefixed catalogs; that is the 17 m 16 s, and the last scenarios inherit the earlier ones'
  backlog on top of step 9's drill.

## Verdicts on the levers

| lever | claim it came with | what this measurement says | verdict |
|---|---|---|---|
| A — R2 remote-log interval on the drill stack | drain 454 s → 227 s, churn −46 % | already applied in the certificate (CHG-214 family) | keep |
| B — share the apply between the smoke and the drill's `DdlSmokeTwinSweepTest` | ~15 min; needs a redundancy proof | the twin smoke is why each scenario drops ~54 tables instead of 27 | **open — the largest single block** |
| C — run module halves / applies concurrently | ~7 min | parallel applies are unsafe at 2: a metadata read timed out; the cluster reported nothing | **rejected, as measured** |
| D — shard step 3 (1476 tests) | ~3 min | not measured here | open |
| E — cache the image builds | ~1 min | already stamped/skipped when nothing changed | keep |
| F — drop the drain wait on the *last* scenario | — (found here) | after step 11 nothing creates fresh tables (12/13 target existing tables, 14/16/19 are module suites), so that wait protects nothing | **proposed, ~5 min** |
| G — run the smoke *before* the drill | — (suggested earlier, withdrawn) | total churn is unchanged; the wait would just move to the drill | **withdrawn** |

## Also found while measuring

- **The catalog-guard's `33/27 … extra tables are drift, not health` is six leaked scratch tables:**
  four `chg100_sweep_*` created 2026-09-17 by the `DdlSmokeTwinSweepTest` drill class, `probe_tbl_1`,
  and a lowercase `signal_candidates`. No DDL file and no code path references any of them.
- **The tool's safety rule is real and was re-demonstrated:** when the benchmark was killed mid-drain,
  its drain canary was kept while a write could still be pending; its drop timed out once
  (`DdlApplyTool.java:213`) and succeeded about a minute later, after the coordinator's queue drained.
  Scratch cleanup verified afterwards: no `bench*` tables, catalog back to 33.
- **Step 11's per-apply evidence is self-erasing:** `logs/ddl-apply` is container-owned, the host cannot
  write it, so the smoke falls back to `/tmp/ddl-apply-smoke-evidence-*` and the certificate keeps only
  the 1 KB summary. That is why run 6 could not be profiled after the fact.

## Not measured / open

- Levels 4, 8 and 12 (deliberately abandoned).
- The redundancy proof for lever B, and lever D.
- Whether the ~1.5 s per bucket teardown pacing can be shortened at the source (it is the remote-log
  cleanup step against R2), which would compress every drain wait rather than move it.

## Evidence

- harness (kept, shellcheck-clean):
  `code/01_platform/04_scripts/ddl-apply-concurrency-bench.sh`
- `logs/ddl-bench/phase1/` — timestamped apply log, wall clock, evidence record
- `logs/ddl-bench/phase2/20260918T160123Z/` — `summary.tsv`, per-apply logs (each with its own drain
  line), per-level memory samples
