# Wave 42 — O2 provisioning and ingest smoke (15 findings) — DETAILED plan

- Date: 2026-09-19
- Findings: P6-428, P6-748 (reconcile-compare.py) · P6-457, P6-458 (local_int_004_smoke.py) ·
  P6-459, P6-460, P6-461, P6-757, P6-758, P6-759 (o2-provision.py) · P6-462, P6-760 (o2_ingest.py) ·
  P6-572, P6-791, P6-792 (stage_capture_parse.py)
- Status: verification complete (read-only, 2026-09-19); implementation staged while the certifying
  gate for `f2faf565` runs, applied after its verdict.

## 0. Verdict summary

All 15 were checked against the code; **15/15 real**, all MEDIUM/LOW as filed. Two need a record
correction:

- **P6-428** — the defect is real (no `--pre` guard → a truncated pre-probe makes every delta
  `post − 0`, so a real loss still PASSes in the default `>=` mode), but the *missing-file* half of
  the claim is wrong: `--pre` is `required=True` and `parse_probe` opens the file, so a missing file
  crashes. Only a truncated/empty one yields `{}`.
- **P6-758** — real, but the line ref is off by ~4: the rule is L1336–1342, not L1332–1335.

Everything else was confirmed exactly as filed, including the mechanisms: `api()` returns a **str**
body on HTTPError (L1419–1420) so `full.get("hash")` raises; 7 bounds with 7 counts; only
`HTTPError` is caught around `urlopen`; `except (URLError, OSError)` cannot catch
`JSONDecodeError` (a `ValueError`); `parse_stages_tsv` coerces a blank epoch to `None` and three
consumers assume a number; `parse_stages_tsv` has four call sites (L310, L394, L621, L651);
`METRIC_TYPES`, `make_panel`, `stream` appear only at their definitions.

### In-repo precedent for two fixes

- **P6-460** — the real emitter already implements the OTLP rule this seed violates:
  `OtlpMetricsEmitter` L709–719, *"R-036: OTLP histograms require explicitBounds to be one element
  shorter than bucketCounts"*, emitting `bucketCounts:[count,0,0,0]` against
  `explicitBounds:[p50,p90,p99]`. The seed should mirror that shape.
- **P6-791** — this file already has a documented policy for the same situation: operator quantiles
  are aggregated "MAX quantile value across subtasks" (L185–186). Using MAX for the latency
  histograms too keeps the `(task, sub, op)` key shape that `test_stage_capture_parse.py` pins.

## 1. Decisions taken (override any of these and the plan changes)

1. **P6-758 → keep the byte-rate rule, fix the description and threshold.** "TX >80% capacity" is
   not expressible as `rate(node_network_transmit_bytes_total[5m])` (bytes/s). Chosen: threshold
   `80e6` with a description naming MB/s and the implied share of a 1 GbE link. Alternative rejected:
   rewriting the rule as a genuine percentage — changes what the alert means and needs a
   capacity denominator the rule does not have.
2. **P6-460 → add the missing `+Inf` bucket** (7 bounds → 8 counts, the single `1` preserved so the
   bucket total still reconciles with `count`). It is seeded dev/test telemetry, but it contradicts
   the emitter's own shape, which is exactly what makes the seed misleading.
3. **P6-458 → stop pretending, do not implement.** The module's offline mode is documented as
   contract-only ("--live requires an execution-t3 fake stack"), so the fix is to delete the dead
   `if not seq` branch and the fake-lifecycle loop and say plainly that the lifecycle drive is not
   exercised offline (the Go/Rust/Java tests named in the comment are the proof). Alternative
   rejected: driving the fake broker from an offline smoke test — that is a live test, and it would
   be a new capability, not a repair.
4. **P6-791 → aggregate with MAX** rather than re-keying by source id, which would change the
   `latency_report` key shape that the existing test pins.
5. **P6-792 → parse once in `main` and pass the rows down** as an optional parameter (defaulting to
   the current internal parse), rather than caching. A cache keyed on mtime is wrong for tests that
   rewrite a fixture quickly; optional params keep every existing caller and test working.

## 2. Fixes

### 2.1 `ing-tcp001/reconcile-compare.py` (P6-428, P6-748)

- **P6-428** — add the missing third guard beside the other two: an empty/truncated `--pre` exits 1
  with the same "no evidence is not evidence of no loss" reasoning already written in the comment.
- **P6-748** — `raw_nonzero` must test the window delta (`post_raw - pre_raw`) like every other
  comparison in the file, not the absolute post count; otherwise historical RAW rows in a reused
  cluster fail a run that lost nothing.

### 2.2 `local_int_004_smoke.py` (P6-457, P6-458)

- **P6-457** — replace the no-op loop *and* the two duplicated glob checks with one loop that
  appends to `errs` for each of the three required DDL files, so `13_order_correlation` is actually
  checked (it exists on disk and the docstring claims it is verified).
- **P6-458** — delete the dead `if not seq` branch and the loop that "simulates" a lifecycle without
  driving anything; the comment states what is and is not exercised offline.

### 2.3 `o2-provision.py` (P6-459, P6-460, P6-461, P6-757, P6-758, P6-759)

- **P6-459** — guard the GET: `status != 200` or a non-dict body skips the dashboard with a printed
  reason instead of raising `AttributeError` (matching every other caller).
- **P6-460** — the seed histogram gets the `+Inf` bucket count; the comment cites the emitter's R-036
  rule so the next editor sees why 8 counts accompany 7 bounds.
- **P6-461** — catch `urllib.error.URLError`/`OSError` around the OTLP POST as well, and fail loudly
  with the same exit code as an HTTP error instead of a traceback that skips the rest of the run.
- **P6-757** — delete `METRIC_TYPES`, `stream()` and `make_panel()` (definitions only, no callers).
- **P6-758** — threshold + description per decision 1.
- **P6-759** — a non-200 PUT must not be followed by "dashboard converged": report the failure and
  count it, so the run's exit status reflects a dashboard that is still stale in O2.

### 2.4 `o2_ingest.py` (P6-462, P6-760)

- **P6-462** — the response path keeps its documented exit-code contract: a non-JSON/empty 200 body
  (or a non-dict payload) returns 4 ("O2 refused") instead of raising `JSONDecodeError`/
  `AttributeError`.
- **P6-760** — the refusal message names the secrets file that was actually consulted (the custom
  `O2_SECRETS_FILE` when set), not `_DEFAULT_SECRETS`.

### 2.5 `stage_capture_parse.py` (P6-572, P6-791, P6-792)

- **P6-572** — rows whose `epoch` is blank/non-numeric are skipped at parse (they cannot be ordered,
  differenced or windowed), with a comment saying why; `parse_stages_tsv` already drops malformed
  lines, so this is the same policy applied to the one field three consumers require.
- **P6-791** — latency histograms aggregate with MAX across source series, per decision 4.
- **P6-792** — `main` parses `stages.tsv` once and passes the rows to `divergence_report` and
  `b2_read_lag_report` (and to the `--json` branch) via an optional parameter.

## 3. Verification plan

- Wave test `code/01_platform/04_scripts/tests/test_o2_tooling_wave42.py`, offline and hermetic:
  a truncated `--pre` probe exits 1; a pre-existing RAW row does not fail a clean window;
  a missing `13_order_correlation.sql` is reported; the seeded histogram satisfies
  `len(bucketCounts) == len(explicitBounds) + 1` (and the same for the emitter's own shape);
  a non-200 GET/PUT path is reported and not called "converged"; a non-JSON 200 from O2 returns 4;
  a blank epoch is skipped without a `TypeError`; two latency source series for one operator yield
  the MAX; the secrets message names a custom `O2_SECRETS_FILE`.
- The two existing suites that the gate runs must stay green **unchanged**:
  `test_reconcile_compare.py`, `test_stage_capture_parse.py` (both run in gate step 3).
- `python3 -m py_compile` on all five; whole `tests/` suite; `make docs-audit`.
- Gate impact: the fixes are covered by step 3 through those two existing suites. None of the five
  files is an image input (not in `_platform_sources()`, no Dockerfile reference) → no image rebuild.

## 4. Out of scope

- The `--live` path of `local_int_004_smoke.py` (decision 3) and any new live coverage.
- `o2-provision.py`'s dead-code removal is limited to the three symbols the audit named; a full
  dead-code sweep of a 1921-line operator tool is not this wave.

---

## Staged implementation (2026-09-19, staged in `/tmp/w42/`, applied post-gate)

**Method**: wave test written first against the *unmodified* tools in a mirrored tree
(`/tmp/w42/mirror`, real file copies — no writable path behind a symlink).

**Red first**: `Ran 15 tests … FAILED (failures=10, errors=3)` — the test caught two fixes the plan
had listed but the first edit pass had not written (P6-460, P6-461).

**Fixes applied**: `apply_fixes.py` — 37 assert-before-edit operations (every old text must be
present exactly once, so a drifted tree fails loudly instead of half-applying).

| File | Findings | Fix |
| --- | --- | --- |
| `ing-tcp001/reconcile-compare.py` | P6-428, P6-748 | `--pre` guard; quarantine RAW check = window delta |
| `local_int_004_smoke.py` | P6-457, P6-458 | real check for all three DDL files; no-op loop + its sample deleted |
| `o2-provision.py` | P6-459, P6-460, P6-461, P6-757, P6-758, P6-759 | failure counting + caller exit 1; +Inf bucket; URLError → "not ready"; 3 dead defs deleted; net rule = 80 MB/s with honest description |
| `o2_ingest.py` | P6-462, P6-760 | non-JSON 200 → exit 4; `_secrets_path()` used by read and message |
| `stage_capture_parse.py` | P6-572, P6-791, P6-792 | drop unusable epochs; latency max across sources; parse once, pass `rows` down |
| docs (3) | P6-748, P6-758 | `10-observability.md` net row, `03-alert-routing.md` dev false-fire note, `ing-tcp001/README.md` reconcile row |

**Green**: `test_o2_tooling_wave42.py` → `Ran 17 tests … OK`; the two gate-step-3 suites unchanged →
`Ran 20 tests … OK`; `py_compile` clean on all five scripts.

**Full-suite catch (2026-09-19)**: `tests/test_alert_threshold_parity.py` pins the INFRA setpoints against
`10-observability.md`; P6-758 moved the net row from `80` (called "80%") to an absolute 80 MB/s rate, so
the parity record moves to `80_000_000` in the same commit. Two further full-suite failures
(`test_change_control_check.RepoIndexTests`, `test_gate_preflight.TestComposeFormInvariant`) were an
artifact of running discovery from `code/01_platform/04_scripts` instead of the repo root — both pass
under the gate's own invocation.

**Deliberate non-change**: `--seed`/`--instruments` stay on the `local_int_004_smoke.py` CLI (the
interface is referenced by the Makefile) even though the offline path no longer samples; the flag
was already inert for the offline contract.
