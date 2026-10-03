# aislop remediation tracker — live document

**Baseline:** aislop 0.17.0 scan of `main` @ `2e961a05`, 2026-10-03 10:20 · **548 findings** (66 errors) · score **72/100** · full detail in [`aislop-detailed-report.md`](./aislop-detailed-report.md) · raw JSON `/tmp/opencode/aislop-full.json`.

**This is a hand-updated tracker.** The frozen scan detail lives in `aislop-detailed-report.md`; this file is where status changes. Edit markers in place after each fix; update the group status line and the index row for that group. IDs `AS-###` are identical in both files and in `aislop-findings.csv`.

## Marker legend

| Marker | Meaning | Evidence you should add |
|---|---|---|
| `[ ]` | open — not verified yet | — |
| `[~]` | in progress | who / what is being changed |
| `[x]` | fixed and verified | commit / CHG id / test evidence |
| `[f]` | false positive / not a defect | one-line reason |
| `[-]` | accepted, won't fix | one-line reason |
| `[d]` | deferred | trigger that revives it |

**Update recipe:** (1) take the next group in the index order; (2) for each item, read the code at the recorded line and decide; (3) set the item marker and fill the `evidence:` line; (4) update the group's status line and its index row. Re-scan at milestones (command at the bottom) and add a row to the score history instead of regenerating this file wholesale.

---

## Group index (fix order top to bottom)

| Group | Phase | Items | Count | Status | Fixable |
|---|---|---|---:|---|---:|
| **G1 — Security verification** | 1 of 13 | AS-001–AS-005 | 5 | `[f]` 5 fp | 0 |
| **G2 — Python lint defects (ruff)** | 2 of 13 | AS-343–AS-364 | 22 | `[x]` 22/22 | 11 |
| **G3 — Failure visibility (error handling)** | 3 of 13 | AS-006–AS-044, AS-285, AS-320–AS-327 | 48 | `[x]` 12/48 | 0 |
| **G4 — Defensive dict access** | 4 of 13 | AS-286–AS-306 | 21 | `[x]` 3/21 | 0 |
| **G5 — Dispatch ladders** | 5 of 13 | AS-307–AS-313 | 7 | `[f]` 7/7 | 0 |
| **G6 — Rust panic safety** | 6 of 13 | AS-314–AS-319 | 6 | `[f]` 6/6 | 0 |
| **G7 — Mechanical Python cleanup** | 7 of 13 | AS-328–AS-337 | 10 | `[x]` 1/10 | 0 |
| **G8 — Hardcoded config values** | 8 of 13 | AS-045–AS-050 | 6 | `[f]` 6/6 | 0 |
| **G9 — TODO stubs** | 9 of 13 | AS-338–AS-342 | 5 | `[f]` 5/5 | 0 |
| **G10 — Structural maintainability (nesting & params)** | 10 of 13 | AS-365–AS-376, AS-479–AS-487 | 21 | `[-]` 5/21 | 0 |
| **G11 — Size pressure (long functions & large files)** | 11 of 13 | AS-377–AS-478 | 102 | `[-]` 91/102 | 0 |
| **G12 — Formatting** | 12 of 13 | AS-488–AS-548 | 61 | `[f]` 61/61 | 61 |
| **G13 — Comment policy (default WONTFIX)** | 13 of 13 | AS-051–AS-284 | 234 | `[x]` 4/234 · `[-]` 230/234 | 211 |

**Progress:** 42/548 fixed · 170 false-positive · 331 won't-fix · 5 deferred · **score: 75/100** · **cross-check:** 41 found, 41 fixed (XC-1…XC-41, CHG-532/533/534/535/536/537/538/539)

---

## Groups in detail

### G1 — Security verification (5 items)

**Status:** `[f]` verified false-positive — 0/5 fixed · 5 false-positive · 0 won't-fix · 0 deferred

**Why:** Possible credentials/tokens in source; highest impact if any are real.

**How:** Read each site by hand. Confirm test fixture / config key name / real value. If real: rotate out-of-band first, then remove. Record the verdict in evidence.

**Rules in this group:** `security/hardcoded-secret` (4) · `security/python-exec` (1)

- [f] **AS-001** `security/hardcoded-secret` — `code/01_platform/04_scripts/t9_order_sandbox.py:157` — Possible Hardcoded password/secret detected in source code
  > L157: `CONTROL_DEFAULT_SECRET = "local-dev-only"`
  - evidence: Intentional dev default, not a credential: mirrors the compose default `GATEWAY_SHARED_SECRET: ${GATEWAY_SHARED_SECRET:-local-dev-only}` (docker-compose.yml:1086,1119); overridable via `--secret` (t9_order_sandbox.py:1120); production uses `GATEWAY_SHARED_SECRET`/`GATEWAY_SHARED_SECRET_FILE` (config.rs:24-43). Added by commit 453c1111 (DEC-044 operator signer).
- [f] **AS-002** `security/hardcoded-secret` — `code/01_platform/04_scripts/t9_order_sandbox.py:918` — Possible Hardcoded password/secret detected in source code
  > L918: `def run_live(transport=None, probe=None, secret="local-dev-only", now=None,`
  - evidence: Same constant as AS-001: default parameter of the harness `run_live()`; the CLI always passes `args.secret` (t9_order_sandbox.py:1142). Not a secret.
- [f] **AS-003** `security/hardcoded-secret` — `code/02_services/04_executor/src/engine.rs:611` — Possible Authentication token detected in source code
  > L611: `let token = "tok_live_9f4c2a7e88b1";`
  - evidence: Test-only fixture in `#[cfg(test)] mod tests` (engine.rs:595), in `bridge_selection_debug_never_prints_the_auth_token`; deliberately distinctive so the test can assert the token never appears in `Debug` output (engine.rs:625-627). Added by commit 4d16c23f (P3-193 redaction).
- [f] **AS-004** `security/hardcoded-secret` — `code/common/src/main/java/com/trading/common/config/ConfigKeys.java:99` — Possible Hardcoded password/secret detected in source code
  > L99: `public static final String O2_PASSWORD = "O2_PASSWORD";`
  - evidence: Env-var key name, not a value: `ConfigKeys` is a registry of `public static final String X = "X"` constants (ConfigKeys.java:75-119); the key is also listed in `SecretGuard.SECRET_KEYS` (SecretGuard.java:53) as a secret that must NOT be present in the main env map.
- [f] **AS-005** `security/python-exec` — `code/01_platform/04_scripts/t9_order_sandbox.py:379` — Use of exec() can execute arbitrary code
  > L379: `def exec(self, service, shell):`
  - evidence: Rule matched a method named `exec` (`DockerExecTransport.exec`), not Python's builtin `exec()`; it runs a fixed `docker compose ... exec -T <service> sh -lc <shell>` template (t9_order_sandbox.py:380-381) in a local dev harness. No builtin `exec(`/`eval(` usage in the file.

### G2 — Python lint defects (ruff) (22 items)

**Status:** `[x]` complete — 22/22 fixed · 0 false-positive · 0 won't-fix · 0 deferred

**Why:** Objective, tool-verified defects (unused imports/vars, undefined name, one-liners).

**How:** Run ruff --fix for the mechanical ones (F401/F541/F841/E401), fix the rest by hand. Run the touched module's tests after each batch.

**Rules in this group:** `ruff/E401` (2) · `ruff/E701` (1) · `ruff/E702` (2) · `ruff/E731` (1) · `ruff/E741` (3) · `ruff/F401` (3) · `ruff/F541` (4) · `ruff/F821` (1) · `ruff/F841` (5)

- [x] **AS-343** `ruff/E401` — `code/01_platform/04_scripts/local_int_004_smoke.py:16` — Multiple imports on one line
  > L16: `import argparse, json, subprocess, sys`
  - evidence: Fixed in `68fb9823` (CHG-529): split `import argparse, json, subprocess, sys` into four lines.
- [x] **AS-344** `ruff/E401` — `code/01_platform/05_instruments/split_manifest.py:13` — Multiple imports on one line
  > L13: `import argparse, csv, pathlib, sys`
  - evidence: Fixed in `68fb9823` (CHG-529): split `import argparse, csv, pathlib, sys` into four lines.
- [x] **AS-345** `ruff/E701` — `code/01_platform/04_scripts/local_int_004_smoke.py:26` — Multiple statements on one line (colon)
  > L26: `if profile: cmd += ["--profile", profile]`
  - evidence: Fixed in `68fb9823` (CHG-529): split the one-line `if profile:` statement.
- [x] **AS-346** `ruff/E702` — `code/01_platform/05_instruments/split_manifest.py:33` — Multiple statements on one line (semicolon)
  > L33: `print(f"input not found: {inp}", file=sys.stderr); sys.exit(2)`
  - evidence: Fixed in `68fb9823` (CHG-529): split `print(...); sys.exit(2)` into two lines.
- [x] **AS-347** `ruff/E702` — `code/01_platform/05_instruments/split_manifest.py:39` — Multiple statements on one line (semicolon)
  > L39: `print("empty CSV", file=sys.stderr); sys.exit(2)`
  - evidence: Fixed in `68fb9823` (CHG-529): split `print(...); sys.exit(2)` into two lines.
- [x] **AS-348** `ruff/E731` — `code/01_platform/04_scripts/o2-provision.py:1813` — Do not assign a `lambda` expression, use a `def`
  > L1813: `v2api = lambda method, body=None: api_raw(`
  - evidence: Fixed in `68fb9823` (CHG-529): `v2api` lambda converted to a `def`.
- [x] **AS-349** `ruff/E741` — `code/01_platform/04_scripts/check_flink_properties.py:87` — Ambiguous variable name: `l`
  > L87: `return [l.strip() for l in m.group(1).split("\n") if l.strip()]`
  - evidence: Fixed in `68fb9823` (CHG-529): renamed comprehension variable `l` -> `line`.
- [x] **AS-350** `ruff/E741` — `code/01_platform/04_scripts/plan_tracker.py:35` — Ambiguous variable name: `l`
  > L35: `return [m.group(1) for m in (MARKER_RE.match(l) for l in lines) if m]`
  - evidence: Fixed in `68fb9823` (CHG-529): renamed comprehension variable `l` -> `line`.
- [x] **AS-351** `ruff/E741` — `code/01_platform/04_scripts/plan_tracker.py:58` — Ambiguous variable name: `l`
  > L58: `head = next((n for n, l in enumerate(lines) if l.startswith("**Roll-up**")), None)`
  - evidence: Fixed in `68fb9823` (CHG-529): renamed comprehension variable `l` -> `line`.
- [x] **AS-352** `ruff/F401` — `code/01_platform/01_docker/alert-consumer.py:38` — `time` imported but unused
  > L38: `import time`
  - evidence: Fixed in `68fb9823` (CHG-529): removed unused `import time`.
- [x] **AS-353** `ruff/F401` — `code/01_platform/04_scripts/deploy_preflight.py:24` — `shlex` imported but unused
  > L24: `import shlex`
  - evidence: Fixed in `68fb9823` (CHG-529): removed unused `import shlex`.
- [x] **AS-354** `ruff/F401` — `code/01_platform/04_scripts/image_staleness_check.py:63` — `os` imported but unused
  > L63: `import os`
  - evidence: Fixed in `68fb9823` (CHG-529): removed unused `import os`.
- [x] **AS-355** `ruff/F541` — `code/01_platform/04_scripts/alert-routing-selftest.py:93` — f-string without any placeholders
  > L93: `f",data=sys.argv[1].encode(),headers={{'Content-Type':'application/json'}}"`
  - evidence: Fixed in `68fb9823` (CHG-529): dropped the redundant `f` prefix and unescaped braces (generated request text unchanged).
- [x] **AS-356** `ruff/F541` — `code/01_platform/04_scripts/alert-routing-selftest.py:177` — f-string without any placeholders
  > L177: `print(f"3. temp alert created (always-firing, silence=0)")`
  - evidence: Fixed in `68fb9823` (CHG-529): dropped the redundant `f` prefix.
- [x] **AS-357** `ruff/F541` — `code/01_platform/04_scripts/fused_timeline.py:223` — f-string without any placeholders
  > L223: `f'avg(rate(node_cpu_seconds_total{{mode="iowait"}}[30s])) * 100', "sum"),`
  - evidence: Fixed in `68fb9823` (CHG-529): dropped the redundant `f` prefix and unescaped braces (PromQL value unchanged).
- [x] **AS-358** `ruff/F541` — `code/01_platform/04_scripts/stale_table_kind_scan.py:454` — f-string without any placeholders
  > L454: `f"no {RETIRED_TABLE} manifest entry (" + (f"PRESENT" if RETIRED_TABLE in entries else "absent") + ")"))`
  - evidence: Fixed in `68fb9823` (CHG-529): removed the redundant `f` prefix from the inner literal.
- [x] **AS-359** `ruff/F821` — `code/01_platform/04_scripts/alert-routing-selftest.py:50` — Undefined name `NoReturn`
  > L50: `def fail(msg: str) -> "NoReturn":  # type: ignore[valid-type]`
  - evidence: Fixed in `68fb9823` (CHG-529): imported `NoReturn`, unquoted the return annotation, removed the now-unneeded type-ignore.
- [x] **AS-360** `ruff/F841` — `code/01_platform/04_scripts/day_run.py:759` — Local variable `exc` is assigned to but never used
  > L759: `except subprocess.CalledProcessError as exc:`
  - evidence: Fixed in `68fb9823` (CHG-529): removed the unused `as exc` binding.
- [x] **AS-361** `ruff/F841` — `code/01_platform/04_scripts/env_facts.py:48` — Local variable `exc` is assigned to but never used
  > L48: `except OSError as exc:`
  - evidence: Fixed in `68fb9823` (CHG-529): removed the unused `as exc` binding.
- [x] **AS-362** `ruff/F841` — `code/01_platform/04_scripts/holistic-analyze.py:866` — Local variable `candle_re` is assigned to but never used
  > L866: `candle_re = re.compile(r"candle:(\d{13}):(\d{13})")`
  - evidence: Fixed in `68fb9823` (CHG-529): deleted the dead `candle_re` assignment.
- [x] **AS-363** `ruff/F841` — `code/01_platform/04_scripts/holistic-analyze.py:1311` — Local variable `thi` is assigned to but never used
  > L1311: `thi = [(t, (snaps[t].get("throttled_usec", 0))) for t in keys]`
  - evidence: Fixed in `68fb9823` (CHG-529): deleted the dead `thi` assignment.
- [x] **AS-364** `ruff/F841` — `code/01_platform/04_scripts/holistic-analyze.py:1652` — Local variable `dup_ing_max` is assigned to but never used
  > L1652: `dup_ing_max = 0`
  - evidence: Fixed in `68fb9823` (CHG-529): deleted the dead `dup_ing_max` initializer.

### G3 — Failure visibility (error handling) (48 items)

**Status:** `[x]` complete — 12/48 fixed · 32 false-positive · 4 won't-fix · 0 deferred · AS-035 reclassified from won't-fix by the 2026-10-03 blind audit, fixed in CHG-537 (`9321b489`) · AS-025/026 (checkpoint read discipline) and AS-033/034 (G6b read retired) cleared by CHG-538 (`81dbd763`)

**Why:** The highest-value behavioral class: failures are discarded or logged without the cause, so debugging live incidents becomes guesswork.

**How:** Per site choose one: (a) narrow the exception and handle it; (b) keep continuing but log the caught error (exc / exc_info=True) and state the specific failure mode in a comment; (c) re-raise or return an error. Never a bare except: pass.

**Rules in this group:** `ai-slop/python-broad-except` (1) · `ai-slop/silent-recovery` (8) · `ai-slop/swallowed-exception` (39)

- [f] **AS-006** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/cluster_check.py:203` — Bare except with pass swallows errors silently
  > L203: `except ValueError:`
  - evidence: False positive (G3 verified 2026-10-03): a swarm-JSON parse failure leaves `swarm={}` and `check_swarm_active` reports FAIL 'LocalNodeState=unknown' — the failure is visible.
- [-] **AS-007** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/cp_phase_capture.py:181` — Bare except with pass swallows errors silently
  > L181: `except Exception:`
  - evidence: Won't-fix (G3 verified 2026-10-03): documented best-effort operator-name map; the broad catch is deliberate (urllib can raise URLError/HTTPError/ValueError/KeyError) and names are display-only.
- [f] **AS-008** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/ddl_apply.py:485` — Catch block only prints error without proper handling
  > L485: `except OSError as exc:`
  - evidence: False positive (G3 verified 2026-10-03): best-effort evidence enrichment; the WARNING already includes the exception and the apply result must still be reported.
- [f] **AS-009** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/ddl_apply.py:607` — Catch block only prints error without proper handling
  > L607: `except (OSError, json.JSONDecodeError) as exc:`
  - evidence: False positive (G3 verified 2026-10-03): best-effort read-back in the PASS_WITH_LIMITATION branch; the WARNING includes the exception.
- [f] **AS-010** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/ddl_apply.py:621` — Catch block only prints error without proper handling
  > L621: `except (OSError, json.JSONDecodeError) as exc:`
  - evidence: False positive (G3 verified 2026-10-03): best-effort read-back in the PASS branch; the WARNING includes the exception.
- [f] **AS-011** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/disaster_drills.py:65` — Bare except with pass swallows errors silently
  > L65: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): cleanup kill of a timed-out process group; an OSError means the process is already gone.
- [f] **AS-012** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/disaster_drills.py:136` — Bare except with pass swallows errors silently
  > L136: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): optional .env read; env vars/defaults take over and the O2 probe reports auth failures.
- [f] **AS-013** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/eod_schedule.py:90` — Catch block only prints error without proper handling
  > L90: `except OSError as exc:`
  - evidence: False positive (G3 verified 2026-10-03): documented best-effort heartbeat write ('never a reason to stop the schedule'); the WARN includes the exception.
- [f] **AS-014** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/eod_schedule.py:119` — Catch block only prints error without proper handling
  > L119: `except OSError as exc:`
  - evidence: False positive (G3 verified 2026-10-03): documented best-effort last-run stamp; the WARN includes the exception.
- [f] **AS-015** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/eod_schedule.py:168` — Catch block only prints error without proper handling
  > L168: `except OSError as exc:`
  - evidence: False positive (G3 verified 2026-10-03): documented best-effort state write; the WARN includes the exception.
- [-] **AS-016** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:197` — Bare except with pass swallows errors silently
  > L197: `except OSError:`
  - evidence: Won't-fix (G3 verified 2026-10-03): missing file -> zero deltas is pinned by test_missing_file_is_all_zero (test_holistic_g7_parity.py:89); the harness smoke gate (holistic-measure.sh:600) fails missing injection first.
- [f] **AS-017** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:576` — Bare except with pass swallows errors silently
  > L576: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): temp-file unlink cleanup; an OSError means the file is already gone.
- [f] **AS-018** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:585` — Bare except with pass swallows errors silently
  > L585: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): best-effort read of the reader's stderr for message detail; the failure itself is printed with the path.
- [f] **AS-019** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:592` — Bare except with pass swallows errors silently
  > L592: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): temp-file unlink cleanup; an OSError means the file is already gone.
- [f] **AS-020** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:616` — Bare except with pass swallows errors silently
  > L616: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): temp-file unlink cleanup; an OSError means the file is already gone.
- [f] **AS-021** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:686` — Bare except with pass swallows errors silently
  > L686: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): a rows-read failure surfaces as ok=False and the caller's g7c_measurement_guard fails the audit.
- [f] **AS-022** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:693` — Bare except with pass swallows errors silently
  > L693: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): best-effort stderr detail read; the missing-__END__ failure is printed with the path.
- [f] **AS-023** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:961` — Catch block only prints error without proper handling
  > L961: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): handler now includes the caught error (CHG-530, same site as AS-320), but the swallowed-exception heuristic still fires because the handler prints and continues — the intended handling for an optional evidence file.
- [f] **AS-024** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:983` — Bare except with pass swallows errors silently
  > L983: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): optional GC log; absence is printed ('no pause lines found') and only correlation uses it.
- [x] **AS-025** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:1027` — Bare except with pass swallows errors silently
  > L1027: `except ValueError:`
  - evidence: Fixed (CHG-538, `81dbd763`): the full audit re-opened it — a corrupt checkpoints.jsonl line silently under-counted the correlation input. `read_checkpoint_windows()` now counts unparseable lines and surfaces them as UNAVAILABLE; cleared by rescan (XC-34).
- [x] **AS-026** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:1029` — Bare except with pass swallows errors silently
  > L1029: `except OSError:`
  - evidence: Fixed (CHG-538, `81dbd763`): a missing/unreadable checkpoints.jsonl printed "slow checkpoints: 0" (unmeasured leg as clean zero). The read error now surfaces as UNAVAILABLE; cleared by rescan (XC-35).
- [f] **AS-027** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:1180` — Bare except with pass swallows errors silently
  > L1180: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): optional java.out; absence is reported ('no otlp-metrics-payload lines found').
- [f] **AS-028** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:1282` — Catch block only prints error without proper handling
  > L1282: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): handler now includes the caught error (CHG-530, same site as AS-321); printing and continuing is the intended handling for an optional evidence file.
- [f] **AS-029** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:1323` — Catch block only prints error without proper handling
  > L1323: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): handler now includes the caught error (CHG-530, same site as AS-322); printing and continuing is the intended handling for an optional evidence file.
- [f] **AS-030** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:1374` — Catch block only prints error without proper handling
  > L1374: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): handler now includes the caught error (CHG-530, same site as AS-323); printing and continuing is the intended handling for an optional evidence file.
- [f] **AS-031** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:1425` — Catch block only prints error without proper handling
  > L1425: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): handler now includes the caught error (CHG-530, same site as AS-324); printing and continuing is the intended handling for an optional evidence file.
- [f] **AS-032** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:1465` — Catch block only prints error without proper handling
  > L1465: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): handler now includes the caught error (CHG-530, same site as AS-325); printing and continuing is the intended handling for an optional evidence file.
- [x] **AS-033** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:1509` — Bare except with pass swallows errors silently
  > L1509: `except ValueError:`
  - evidence: Fixed (CHG-538, `81dbd763`): the G6b block this handler belonged to was retired with the dead burst machinery (the whole block was unreachable); finding cleared by rescan.
- [x] **AS-034** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:1511` — Bare except with pass swallows errors silently
  > L1511: `except OSError:`
  - evidence: Fixed (CHG-538, `81dbd763`): same retired G6b read as AS-033; cleared by rescan.
- [x] **AS-035** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:1589` — Bare except with pass swallows errors silently
  > L1589: `except OSError:`
  - evidence: Fixed in CHG-537 (`9321b489`): the won't-fix rationale covered only a MISSING faketool.log (injection-less parity-only, CHG-198) — the broad `except OSError: pass` also swallowed permission/I-O errors, silently degrading a possibly-injecting run to parity-only and skipping G7a/G7b. `read_inject_counts()` now treats only FileNotFoundError as expected and surfaces other OSErrors as UNAVAILABLE with the error (XC-5 discipline); 4 tests, all red pre-fix. Found by the 2026-10-03 blind audit.
- [-] **AS-036** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:1642` — Bare except with pass swallows errors silently
  > L1642: `except ValueError:`
  - evidence: Won't-fix (G3 verified 2026-10-03): same family as AS-016 — a missing counter TSV yields last_sample_ms=0; Batch B hardening was offered and declined.
- [x] **AS-037** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/holistic-analyze.py:1644` — Bare except with pass swallows errors silently
  > L1644: `except OSError:`
  - evidence: Fixed in `43da9ffb` (CHG-533): the verification sweep showed the missing/empty counter file read as a clean zero — G7a/G7b passed vacuously while the run claimed "dedup exact, late-drop covered". G7 now appends UNAVAILABLE and prints "counter-exactness NOT MEASURED"; F6 prints NOT SAMPLED; the outer handler binds the OSError (clears this finding). Guard: CounterLegNoteTests + the G7/F6 wiring.
- [f] **AS-038** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/image_staleness_check.py:249` — Bare except with pass swallows errors silently
  > L249: `except ValueError:`
  - evidence: False positive (G3 verified 2026-10-03): documented fallback — comment 'context outside the repo: timestamp fallback covers it'.
- [f] **AS-039** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/o2_ingest.py:55` — Bare except with pass swallows errors silently
  > L55: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): missing secrets file falls back to env; o2_ingest then REFUSES with exit 3 (L88).
- [f] **AS-040** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/perf_evidence_parse.py:469` — Bare except with pass swallows errors silently
  > L469: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): optional secrets file; an empty result flows to the existing auth-failure reporting.
- [f] **AS-041** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/pernode_attribution_check.py:68` — Bare except with pass swallows errors silently
  > L68: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): optional secrets file; the caller REFUSES — no O2_AUTH_BASIC (pernode L176).
- [f] **AS-042** `ai-slop/swallowed-exception` — `code/01_platform/04_scripts/soak-o2-evidence.py:75` — Bare except with pass swallows errors silently
  > L75: `except OSError:`
  - evidence: False positive (G3 verified 2026-10-03): optional secrets file; the caller REFUSES — no O2_AUTH_BASIC (soak L108).
- [f] **AS-043** `ai-slop/swallowed-exception` — `code/01_platform/05_instruments/split_manifest.py:53` — Catch block only prints error without proper handling
  > L53: `except (ValueError, TypeError):`
  - evidence: False positive (G3 verified 2026-10-03): tool anchored the comment '(was except: pass)'; the real handler prints a warning with the bad token (P4-244).
- [f] **AS-044** `ai-slop/swallowed-exception` — `code/common/src/main/java/com/trading/common/schema/eod/EodControllerTool.java:543` — Empty catch block swallows errors silently
  > L543: `} catch (Exception e) {`
  - evidence: False positive (G3 verified 2026-10-03): the Java fallback is announced on stderr ('no live table.log.ttl for X — using fallback Y'); not silent.
- [-] **AS-285** `ai-slop/python-broad-except` — `code/01_platform/04_scripts/cp_phase_capture.py:181` — `except Exception: pass` silently drops every exception. Failures vanish without a trace.
  > L181: `except Exception:`
  - evidence: Won't-fix (G3 verified 2026-10-03): same site as AS-007 (python-broad-except); see that evidence.
- [x] **AS-320** `ai-slop/silent-recovery` — `code/01_platform/04_scripts/holistic-analyze.py:961` — except logs without the caught error then continues; the failure cause is lost
  > L961: `except OSError:`
  - evidence: Fixed in `966a8c8e` (CHG-530): signal-path-latency handler now binds the caught error and includes it in the absent message; post-fix scan confirms the silent-recovery finding cleared.
- [x] **AS-321** `ai-slop/silent-recovery` — `code/01_platform/04_scripts/holistic-analyze.py:1282` — except logs without the caught error then continues; the failure cause is lost
  > L1282: `except OSError:`
  - evidence: Fixed in `966a8c8e` (CHG-530): diskstats handler now binds the caught error and includes it in the absent message; post-fix scan confirms the silent-recovery finding cleared.
- [x] **AS-322** `ai-slop/silent-recovery` — `code/01_platform/04_scripts/holistic-analyze.py:1323` — except logs without the caught error then continues; the failure cause is lost
  > L1323: `except OSError:`
  - evidence: Fixed in `966a8c8e` (CHG-530): tm-throttle handler now binds the caught error and includes it in the absent message; post-fix scan confirms the silent-recovery finding cleared.
- [x] **AS-323** `ai-slop/silent-recovery` — `code/01_platform/04_scripts/holistic-analyze.py:1374` — except logs without the caught error then continues; the failure cause is lost
  > L1374: `except OSError:`
  - evidence: Fixed in `966a8c8e` (CHG-530): tm-prom-rocksdb handler now binds the caught error and includes it in the absent message; post-fix scan confirms the silent-recovery finding cleared.
- [x] **AS-324** `ai-slop/silent-recovery` — `code/01_platform/04_scripts/holistic-analyze.py:1425` — except logs without the caught error then continues; the failure cause is lost
  > L1425: `except OSError:`
  - evidence: Fixed in `966a8c8e` (CHG-530): proc-io handler now binds the caught error and includes it in the absent message; post-fix scan confirms the silent-recovery finding cleared.
- [x] **AS-325** `ai-slop/silent-recovery` — `code/01_platform/04_scripts/holistic-analyze.py:1465` — except logs without the caught error then continues; the failure cause is lost
  > L1465: `except OSError:`
  - evidence: Fixed in `966a8c8e` (CHG-530): gc.log handler now binds the caught error and includes it in the absent message; post-fix scan confirms the silent-recovery finding cleared.
- [f] **AS-326** `ai-slop/silent-recovery` — `code/01_platform/04_scripts/t8_sandbox_contract_check.py:47` — except logs without the caught error then continues; the failure cause is lost
  > L47: `except ImportError:  # pragma: no cover`
  - evidence: False positive (G3 verified 2026-10-03): the handler prints FATAL and sys.exit(2) — it does not continue.
- [f] **AS-327** `ai-slop/silent-recovery` — `code/01_platform/05_instruments/split_manifest.py:53` — except logs without the caught error then continues; the failure cause is lost
  > L53: `except (ValueError, TypeError):`
  - evidence: False positive (G3 verified 2026-10-03): comment-anchored duplicate of AS-043.

### G4 — Defensive dict access (21 items)

**Status:** `[x]` complete — 3/21 fixed · 17 false-positive · 1 won't-fix · 0 deferred

**Why:** `.get(..., {}).get(...)` chains turn missing data / schema drift into silent None.

**How:** Normalize at the boundary or split into explicit steps with a clear failure. Only worth changing where the input is external (config, API, JSON).

**Rules in this group:** `ai-slop/python-chained-dict-get` (21)

- [x] **AS-286** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/audit_r2.py:474` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L474: `rules = list(body.get("result", {}).get("rules", []) or [])`
  - evidence: Fixed in `c7877ad4` (CHG-531): `bucket_lock_rules()` validates `result.rules` and raises `UnsupportedFeature`; provision --set-lock no longer merges against an empty default (guard test: malformed GET -> no PUT, step records READ/SET FAILED).
- [x] **AS-287** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/audit_r2.py:540` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L540: `rules = body.get("result", {}).get("rules", [])`
  - evidence: Fixed in `c7877ad4` (CHG-531): the read-only check uses the same fail-closed helper; a malformed GET now records ERROR instead of a false NONE.
- [f] **AS-288** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/cluster_check.py:159` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L159: `return (spec.get("TaskTemplate") or {}).get("Placement", {}).get("MaxReplicas")`
  - evidence: False positive (G4 verified 2026-10-03): intended: `_max_per_node` returns None for absent Placement; callers handle it explicitly (`or 0`, `int(_max_per_node(spec) or 0)`).
- [f] **AS-289** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/day_run.py:426` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L426: `if s in running and facts.services.get(s, {}).get("health") == "unhealthy"]`
  - evidence: False positive (G4 verified 2026-10-03): intended: a missing service is already reported in the I1 `missing` list; the default only keeps it out of `unhealthy`.
- [f] **AS-290** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/day_run.py:982` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L982: `ingestion_running = facts.services.get("ingestion", {}).get("state") == "running"`
  - evidence: False positive (G4 verified 2026-10-03): intended: a missing `ingestion` reads as not-running (conservative); the readiness loop keeps waiting.
- [f] **AS-291** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/day_run.py:995` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L995: `if want_samples and facts.fluss.get("raw", {}).get("ok"):`
  - evidence: False positive (G4 verified 2026-10-03): intended: `facts.fluss['raw']` is set just above; a missing entry skips sampling conservatively.
- [f] **AS-292** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/day_run.py:1001` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L1001: `if second.get("ok") and facts.fluss.get(key, {}).get("ok"):`
  - evidence: False positive (G4 verified 2026-10-03): intended: guard default is falsy -> the sampled delta is not applied; the readiness loop keeps waiting.
- [f] **AS-293** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/day_run.py:1102` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L1102: `fluss_ok = facts.fluss.get("raw", {}).get("ok")`
  - evidence: False positive (G4 verified 2026-10-03): intended: missing raw stats read as not-ok -> wait_ready keeps waiting (fail-closed).
- [f] **AS-294** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/day_run.py:1138` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L1138: `if cps.get(signals[0]["id"], {}).get("latest_completed_ms"):`
  - evidence: False positive (G4 verified 2026-10-03): intended: a missing checkpoint entry keeps wait_signaljob waiting until the timeout (fail-closed).
- [f] **AS-295** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/holistic-analyze.py:1171` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L1171: `dp = (m.get("histogram", {}).get("dataPoints") or [{}])[0]`
  - evidence: False positive (G4 verified 2026-10-03): intended: `(... or [{}])[0]` is immediately guarded by `if not dp: continue`.
- [f] **AS-296** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/local_int_004_smoke.py:38` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L38: `net=cfg.get("networks",{}).get("execution-net",{})`
  - evidence: False positive (G4 verified 2026-10-03): intended: a missing network yields the explicit error 'execution-net must be internal:true'.
- [f] **AS-297** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/local_int_004_smoke.py:42` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L42: `svc=cfg.get("services",{}).get(name)`
  - evidence: False positive (G4 verified 2026-10-03): intended: a missing service yields the explicit error '<name> missing in execution-t3 profile'.
- [f] **AS-298** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/o2-provision.py:1353` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L1353: `cur = s.get("settings", {}).get("data_retention", 0)`
  - evidence: False positive (G4 verified 2026-10-03): intended: missing settings -> PUT the target retention (idempotent setter; worst case an extra PUT).
- [f] **AS-299** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/perf_evidence_parse.py:374` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L374: `("ckpt_completed", lambda d, n: d["checkpoints"].get("counts", {}).get("completed", "")),`
  - evidence: False positive (G4 verified 2026-10-03): intended: CSV column extraction; an empty string is the intended cell value for absent data.
- [f] **AS-300** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/perf_evidence_parse.py:375` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L375: `("ckpt_latest_size", lambda d, n: d["checkpoints"].get("latest_completed", {}).get("checkpointed_size", "")),`
  - evidence: False positive (G4 verified 2026-10-03): intended: CSV column extraction; an empty string is the intended cell value for absent data.
- [f] **AS-301** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/perf_evidence_parse.py:376` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L376: `("ckpt_duration_ms", lambda d, n: d["checkpoints"].get("latest_completed", {}).get("end_to_end_duration", "")),`
  - evidence: False positive (G4 verified 2026-10-03): intended: CSV column extraction; an empty string is the intended cell value for absent data.
- [-] **AS-302** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/seed_dashboards.py:191` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L191: `dash_id = existing.get(title, {}).get("dashboard_id")`
  - evidence: Won't-fix (G4 verified 2026-10-03): None dashboard_id is the intended 'create' signal; a malformed list entry would create a duplicate (recoverable). The adjacent `dashboards` default (L184) is the same family and was not flagged.
- [x] **AS-303** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/soak-o2-evidence.py:137` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L137: `result = payload.get("data", {}).get("result", [])`
  - evidence: Fixed in `c7877ad4` (CHG-531): a 200 body with status=error or no data object now fails the query (BAD PAYLOAD, rc=1) instead of being recorded as zero series.
- [f] **AS-304** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/t9_order_sandbox.py:385` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L385: `net = _compose_json().get("networks", {}).get("execution-net", {})`
  - evidence: False positive (G4 verified 2026-10-03): intended: fail-closed — `raise RuntimeError('execution-net network name not resolvable')`.
- [f] **AS-305** `ai-slop/python-chained-dict-get` — `code/01_platform/04_scripts/t9_order_sandbox.py:474` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L474: `net = cfg.get("networks", {}).get("execution-net", {})`
  - evidence: False positive (G4 verified 2026-10-03): intended: fail-closed — the T8 check fails when `internal` is absent.
- [f] **AS-306** `ai-slop/python-chained-dict-get` — `code/01_platform/06_stage_profiler/stage_profiler.py:599` — Chained `.get(..., {})` defaults hide missing-data cases.
  > L599: `table.get(task, {}).get(quantile, float("-inf")), s.value`
  - evidence: False positive (G4 verified 2026-10-03): intended: max-accumulate idiom; `float('-inf')` is the correct identity default.

### G5 — Dispatch ladders (7 items)

**Status:** `[f]` verified false-positive — 0/7 fixed · 7 false-positive · 0 won't-fix · 0 deferred

**Why:** Repeated equality ladders keep growing and hide missing cases.

**How:** Replace with a table / set membership / handler map. Keep behavior identical; add a test.

**Rules in this group:** `ai-slop/python-repetitive-dispatch` (7)

- [f] **AS-307** `ai-slop/python-repetitive-dispatch` — `code/01_platform/04_scripts/day_run.py:1150` — 4 repeated branches dispatch on `decision.action`.
  > L1150: `if decision.action == "keep":`
  - evidence: False positive (G5 verified 2026-10-03): intended dispatch: 4 actions with distinct side effects (emit/make_locked/make/raise), early returns, and an explicit unknown-action Refusal fallback; a callable table would add indirection.
- [f] **AS-308** `ai-slop/python-repetitive-dispatch` — `code/01_platform/04_scripts/implementation_gate.py:300` — 4 repeated branches dispatch on `t`.
  > L300: `if t == "run":`
  - evidence: False positive (G5 verified 2026-10-03): intended type-tag dispatch: each branch extracts different args (cmd/path/path+needle/dir+needle) and unknown types raise ValueError; no defect.
- [f] **AS-309** `ai-slop/python-repetitive-dispatch` — `code/01_platform/04_scripts/r2_legal_hold_check.py:117` — 5 repeated branches dispatch on `key`.
  > L117: `if key == "date":`
  - evidence: False positive (G5 verified 2026-10-03): intended: byte-exact port of the Java AuditHashChain.Manifest.canonical() parser (file comment: do NOT improve); each branch assigns a different local; the canonical round-trip is the integrity check.
- [f] **AS-310** `ai-slop/python-repetitive-dispatch` — `code/01_platform/04_scripts/r2_legal_hold_check.py:119` — 4 repeated branches dispatch on `key`.
  > L119: `elif key == "table":`
  - evidence: False positive (G5 verified 2026-10-03): same chain as AS-309 (overlapping window of the same parse_manifest if/elif); intended.
- [f] **AS-311** `ai-slop/python-repetitive-dispatch` — `code/01_platform/04_scripts/stage_capture_parse.py:165` — 5 repeated branches dispatch on `name`.
  > L165: `if name == "busyTimeMsPerSecond":`
  - evidence: False positive (G5 verified 2026-10-03): intended metric-name dispatch: only the first 4 branches are uniform accumulators; later branches are non-uniform (watermark uses a different key, latency nests a dict, _operator_ has extra guards).
- [f] **AS-312** `ai-slop/python-repetitive-dispatch` — `code/01_platform/04_scripts/stage_capture_parse.py:167` — 4 repeated branches dispatch on `name`.
  > L167: `elif name == "backPressuredTimeMsPerSecond":`
  - evidence: False positive (G5 verified 2026-10-03): same chain as AS-311 (overlapping window); intended.
- [f] **AS-313** `ai-slop/python-repetitive-dispatch` — `code/01_platform/06_stage_profiler/stage_profiler.py:1550` — 4 repeated branches dispatch on `args.cmd`.
  > L1550: `if args.cmd == "stages":`
  - evidence: False positive (G5 verified 2026-10-03): intended argparse subcommand dispatch: each command body differs substantially and required=True guarantees one of the four; no fallthrough to handle.

### G6 — Rust panic safety (6 items)

**Status:** `[f]` verified false-positive — 0/6 fixed · 6 false-positive · 0 won't-fix · 0 deferred

**Why:** `.unwrap()` in non-test Rust can panic the money-path-adjacent executor.

**How:** Use `?`, `.expect("invariant …")`, or pattern-match. First check whether the site is a test/probe binary under src/bin/ — if so a documented expect() or [f] is fine.

**Rules in this group:** `ai-slop/rust-non-test-unwrap` (6)

- [f] **AS-314** `ai-slop/rust-non-test-unwrap` — `code/02_services/04_executor/src/bin/t9_paper_25.rs:118` — `.unwrap()` in non-test code panics on None/Err. Surfaces as a hard crash for the caller.
  > L118: `.unwrap()`
  - evidence: False positive (G6 verified 2026-10-03): intended fail-loudly: `evidence['shadow_positions'].as_array().unwrap()` inside an assert — invariant on JSON built earlier in the same function; paper-only offline evidence binary (required-features = paper), no production caller (P3-182).
- [f] **AS-315** `ai-slop/rust-non-test-unwrap` — `code/02_services/04_executor/src/bin/t9_paper_25.rs:137` — `.unwrap()` in non-test code panics on None/Err. Surfaces as a hard crash for the caller.
  > L137: `evidence["evidence_hash"].as_str().unwrap(),`
  - evidence: False positive (G6 verified 2026-10-03): intended fail-loudly: `evidence_hash.as_str().unwrap()` in the final success println; the value is produced by finalize_evidence in the same function; paper-only offline binary.
- [f] **AS-316** `ai-slop/rust-non-test-unwrap` — `code/02_services/04_executor/src/bin/t9_paper_25_full.rs:109` — `.unwrap()` in non-test code panics on None/Err. Surfaces as a hard crash for the caller.
  > L109: `.unwrap()`
  - evidence: False positive (G6 verified 2026-10-03): intended fail-loudly: same as AS-314 in the full variant; invariants are asserted before anything is persisted (P3-184); paper-only offline binary.
- [f] **AS-317** `ai-slop/rust-non-test-unwrap` — `code/02_services/04_executor/src/bin/t9_paper_25_full.rs:122` — `.unwrap()` in non-test code panics on None/Err. Surfaces as a hard crash for the caller.
  > L122: `evidence["evidence_hash"].as_str().unwrap(),`
  - evidence: False positive (G6 verified 2026-10-03): intended fail-loudly: same as AS-315 in the full variant; paper-only offline binary.
- [f] **AS-318** `ai-slop/rust-non-test-unwrap` — `code/02_services/04_executor/src/durable.rs:519` — `.unwrap()` in non-test code panics on None/Err. Surfaces as a hard crash for the caller.
  > L519: `std::fs::create_dir_all(&dir).unwrap();`
  - evidence: False positive (G6 verified 2026-10-03): scanner misclassification: `std::fs::create_dir_all(...).unwrap()` in the `scratch_dir()` helper inside `#[cfg(test)] mod tests` (module at durable.rs:375); the rule excludes `#[test]` functions but not module-gated test code.
- [f] **AS-319** `ai-slop/rust-non-test-unwrap` — `code/02_services/04_executor/src/durable_file.rs:587` — `.unwrap()` in non-test code panics on None/Err. Surfaces as a hard crash for the caller.
  > L587: `std::fs::create_dir_all(&dir).unwrap();`
  - evidence: False positive (G6 verified 2026-10-03): scanner misclassification: same as AS-318 in durable_file.rs (test module at durable_file.rs:574).

### G7 — Mechanical Python cleanup (10 items)

**Status:** `[x]` complete — 1/10 fixed · 9 false-positive · 0 won't-fix · 0 deferred

**Why:** Dead imports and pass-through wrappers; zero behavior, small noise.

**How:** ruff --fix for imports; inline the thin wrapper's call. Run tests.

**Rules in this group:** `ai-slop/thin-wrapper` (1) · `ai-slop/unused-import` (9)

- [f] **AS-328** `ai-slop/thin-wrapper` — `code/01_platform/04_scripts/t9_order_sandbox.py:243` — Function 'payload_hash' is a thin wrapper that only calls another function
  > L243: `def payload_hash(payload_json):`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): intended: 2-line alias over `_sha256_hex` whose name is the protocol field (`payload_hash`) ported for Java/Rust parity; inlining would drop the semantic name at its 3 call sites.
- [x] **AS-329** `ai-slop/unused-import` — `code/01_platform/04_scripts/deploy_preflight.py:24` — Imported symbol 'shlex' is never used
  > L24: `import shlex`
  - evidence: Fixed in `68fb9823` (CHG-529): removed unused `import shlex` (same line as AS-353; side effect of the G2 fix).
- [f] **AS-330** `ai-slop/unused-import` — `code/01_platform/04_scripts/holistic-analyze.py:38` — Imported symbol 'org' is never used
  > L38: `import org.apache.fluss.client.Connection;`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): scanner string-blindness: Java import inside the `LOG_READ_SRC` triple-quoted string (lines 37-150) embedded in the Python file — not a Python import.
- [f] **AS-331** `ai-slop/unused-import` — `code/01_platform/04_scripts/holistic-analyze.py:39` — Imported symbol 'org' is never used
  > L39: `import org.apache.fluss.client.ConnectionFactory;`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): scanner string-blindness: Java import inside the `LOG_READ_SRC` triple-quoted string (lines 37-150) embedded in the Python file — not a Python import.
- [f] **AS-332** `ai-slop/unused-import` — `code/01_platform/04_scripts/holistic-analyze.py:40` — Imported symbol 'org' is never used
  > L40: `import org.apache.fluss.client.admin.Admin;`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): scanner string-blindness: Java import inside the `LOG_READ_SRC` triple-quoted string (lines 37-150) embedded in the Python file — not a Python import.
- [f] **AS-333** `ai-slop/unused-import` — `code/01_platform/04_scripts/holistic-analyze.py:41` — Imported symbol 'org' is never used
  > L41: `import org.apache.fluss.client.table.Table;`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): scanner string-blindness: Java import inside the `LOG_READ_SRC` triple-quoted string (lines 37-150) embedded in the Python file — not a Python import.
- [f] **AS-334** `ai-slop/unused-import` — `code/01_platform/04_scripts/holistic-analyze.py:42` — Imported symbol 'org' is never used
  > L42: `import org.apache.fluss.client.table.scanner.log.LogScanner;`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): scanner string-blindness: Java import inside the `LOG_READ_SRC` triple-quoted string (lines 37-150) embedded in the Python file — not a Python import.
- [f] **AS-335** `ai-slop/unused-import` — `code/01_platform/04_scripts/holistic-analyze.py:43` — Imported symbol 'org' is never used
  > L43: `import org.apache.fluss.client.table.scanner.log.ScanRecords;`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): scanner string-blindness: Java import inside the `LOG_READ_SRC` triple-quoted string (lines 37-150) embedded in the Python file — not a Python import.
- [f] **AS-336** `ai-slop/unused-import` — `code/01_platform/04_scripts/holistic-analyze.py:44` — Imported symbol 'org' is never used
  > L44: `import org.apache.fluss.config.Configuration;`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): scanner string-blindness: Java import inside the `LOG_READ_SRC` triple-quoted string (lines 37-150) embedded in the Python file — not a Python import.
- [f] **AS-337** `ai-slop/unused-import` — `code/01_platform/04_scripts/holistic-analyze.py:45` — Imported symbol 'org' is never used
  > L45: `import org.apache.fluss.metadata.TablePath;`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): scanner string-blindness: Java import inside the `LOG_READ_SRC` triple-quoted string (lines 37-150) embedded in the Python file — not a Python import.

### G8 — Hardcoded config values (6 items)

**Status:** `[f]` verified false-positive — 0/6 fixed · 6 false-positive · 0 won't-fix · 0 deferred

**Why:** Environment-specific URLs / provider IDs baked into source confuse environments.

**How:** Move to env/config or a typed config module. Canonical documentation links may stay; mark intentional ones [f] with a reason.

**Rules in this group:** `ai-slop/hardcoded-id` (2) · `ai-slop/hardcoded-url` (4)

- [f] **AS-045** `ai-slop/hardcoded-id` — `code/02_services/04_executor/src/config.rs:396` — Hardcoded provider/project ID in production code
  > L396: `("BRIDGE_AUTH_TOKEN", "tok_live_9f4c2a7e88b1"),`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): test-code misclassification: `#[cfg(test)] mod tests` (module at config.rs:269) — synthetic fixture for the Debug-redaction test (asserts prefix/suffix never leak).
- [f] **AS-046** `ai-slop/hardcoded-id` — `code/02_services/04_executor/src/engine.rs:611` — Hardcoded provider/project ID in production code
  > L611: `let token = "tok_live_9f4c2a7e88b1";`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): test-code misclassification: `#[cfg(test)] mod tests` (module at engine.rs:595) — synthetic fixture token for the BridgeSelection Debug-redaction test.
- [f] **AS-047** `ai-slop/hardcoded-url` — `code/02_services/04_executor/src/config.rs:431` — Hardcoded environment URL in production code
  > L431: `("ARROW_REST_URL", "https://api"),`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): test-code misclassification: dummy Arrow var in the `never_consumes_arrow_vars` test (config.rs:269 module).
- [f] **AS-048** `ai-slop/hardcoded-url` — `code/02_services/04_executor/src/engine.rs:613` — Hardcoded environment URL in production code
  > L613: `base_url: "http://bridge:8080".to_string(),`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): test-code misclassification: test fixture base_url in the BridgeSelection Debug test (engine.rs:595 module).
- [f] **AS-049** `ai-slop/hardcoded-url` — `code/02_services/04_executor/src/engine.rs:631` — Hardcoded environment URL in production code
  > L631: `base_url: "http://bridge:8080".to_string(),`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): test-code misclassification: same fixture in the factory Debug assertion (engine.rs:595 module).
- [f] **AS-050** `ai-slop/hardcoded-url` — `code/02_services/04_executor/src/engine.rs:697` — Hardcoded environment URL in production code
  > L697: `assert_eq!(base_url, "http://execution-bridge:8787");`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): test-code misclassification: assertion value in the endpoint-configured selection test (engine.rs:595 module).

### G9 — TODO stubs (5 items)

**Status:** `[f]` verified false-positive — 0/5 fixed · 5 false-positive · 0 won't-fix · 0 deferred

**Why:** Unresolved TODO/FIXME/HACK comments indicate unfinished or forgotten work.

**How:** Resolve, delete, or convert to a tracked item (CHG/issue) and reference it in the comment; then mark [x] (resolved) or [-] (accepted).

**Rules in this group:** `ai-slop/todo-stub` (5)

- [f] **AS-338** `ai-slop/todo-stub` — `code/common/src/main/java/com/trading/common/schema/fluss/BoundedRetry.java:14` — Unresolved TODO/FIXME/HACK comment indicates incomplete code
  > L14: `* {@code ServerConnection} TODO at the inflight-request send site). Consequences,`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): doc comment quoting the upstream fluss-rpc `ServerConnection` TODO as measured rationale for the bounded retry — not an unresolved marker in this codebase.
- [f] **AS-339** `ai-slop/todo-stub` — `code/common/src/main/java/com/trading/common/schema/fluss/FlussWriteProfiles.java:17` — Unresolved TODO/FIXME/HACK comment indicates incomplete code
  > L17: `* {@code // TODO add the wakeup logic refer to Kafka} (WriterClient.java:208-214).`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): doc comment quoting upstream `WriterClient.java:208-214` TODO (wakeup logic) to justify the D1 linger; not this codebase's incomplete code.
- [f] **AS-340** `ai-slop/todo-stub` — `code/common/src/main/java/com/trading/common/schema/fluss/FlussWriteProfiles.java:41` — Unresolved TODO/FIXME/HACK comment indicates incomplete code
  > L41: `* sender's own TODO documents the busy-loop hazard there ("The method sendWriteData is in a busy loop.`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): doc comment quoting the upstream Sender busy-loop TODO (Sender.java:234-238) as the reason for the non-zero linger floor.
- [f] **AS-341** `ai-slop/todo-stub` — `code/common/src/main/java/com/trading/common/schema/fluss/FlussWriteProfiles.java:75` — Unresolved TODO/FIXME/HACK comment indicates incomplete code
  > L75: `* (RecordAccumulator.java:114-116, "TODO add deliveryTimeoutMs to report success or failure on record`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): doc comment quoting upstream `RecordAccumulator.java:114-116` delivery-timeout TODO to justify the no-per-record-flush rule.
- [f] **AS-342** `ai-slop/todo-stub` — `code/common/src/main/java/com/trading/common/schema/fluss/WriteAwait.java:7` — Unresolved TODO/FIXME/HACK comment indicates incomplete code
  > L7: `* Sender.sendWriteData carries the authors' own TODO ("The method sendWriteData`
  - evidence: False positive (G7/G8/G9 verified 2026-10-03): doc comment quoting the upstream Sender busy-loop TODO as the measured hazard behind the drop-safety rule.

### G10 — Structural maintainability (nesting & params) (21 items)

**Status:** `[-]` accepted — 0/21 fixed · 16 false-positive · 5 won't-fix · 0 deferred · AS-371's hidden dead-guard defect fixed in CHG-538 (`81dbd763`, XC-33); finding remains accepted

**Why:** Deep nesting and long parameter lists make changes risky.

**How:** Early returns / extract functions; options object for >6 required params. Only where the refactor clearly pays; otherwise [d] with a trigger.

**Rules in this group:** `complexity/deep-nesting` (12) · `complexity/too-many-params` (9)

- [f] **AS-365** `complexity/deep-nesting` — `code/01_platform/04_scripts/cp_phase_capture.py:208` — Function nested too deeply (max: 5)
  > L208: `def run(rest_url: str, job_id: str, out_dir: Path, duration: float,`
  - evidence: False positive (G10 verified 2026-10-03): intended: 58-line capture loop; the while/try ladder is the operational flow (5 list-failure strikes, 3 fetch attempts).
- [f] **AS-366** `complexity/deep-nesting` — `code/01_platform/04_scripts/ddl_apply_smoke.py:352` — Function nested too deeply (max: 5)
  > L352: `def scenario(index, extra_env, expect_rc, expect_parts, expect_absent=(),`
  - evidence: False positive (G10 verified 2026-10-03): intended: 80-line test-harness scenario runner; the params are the pinned-contract knobs.
- [f] **AS-367** `complexity/deep-nesting` — `code/01_platform/04_scripts/fluss-probes/CandleFeaturesTableProbe.java:183` — Function nested too deeply (max: 5)
  > L183: `private static void tail(Connection conn, Admin admin, TablePath path, int rows)`
  - evidence: False positive (G10 verified 2026-10-03): intended: canonical Fluss drain loop (bucket -> batch -> pollBatch until null), the shared probe pattern; extracting would obscure it.
- [f] **AS-368** `complexity/deep-nesting` — `code/01_platform/04_scripts/fluss-probes/CandleVerify.java:57` — Function nested too deeply (max: 5)
  > L57: `public static void main(String[] args) throws Exception {`
  - evidence: False positive (G10 verified 2026-10-03): intended: probe main using the same canonical drain pattern; no defect.
- [f] **AS-369** `complexity/deep-nesting` — `code/01_platform/04_scripts/fluss-probes/FlussKvScanStrategy.java:151` — Function nested too deeply (max: 5)
  > L151: `private static void scan(Connection conn, Admin admin, String strategy, int rows) throws Exception {`
  - evidence: False positive (G10 verified 2026-10-03): intended: canonical drain — class doc: 'No .limit(), no empty-poll early exit'; deliberate.
- [f] **AS-370** `complexity/deep-nesting` — `code/01_platform/04_scripts/fluss-probes/FlussPrefixReader.java:92` — Function nested too deeply (max: 5)
  > L92: `static void run(String[] args) throws Exception {`
  - evidence: False positive (G10 verified 2026-10-03): intended: probe CLI; linear arg parsing plus the canonical scan; no defect.
- [-] **AS-371** `complexity/deep-nesting` — `code/01_platform/04_scripts/holistic-analyze.py:714` — Function nested too deeply (max: 5)
  > L714: `def main():`
  - evidence: Accepted complexity (G10 reclassified 2026-10-03): 1157-line linear report pipeline split by commented legs; no defect identified and a refactor is not warranted now (independent review concurred). · full audit 2026-10-03: hidden defect XC-33 fixed in CHG-538 (`81dbd763`) — dead burst machinery retired, checkpoint analysis un-nested; finding remains accepted.
- [-] **AS-372** `complexity/deep-nesting` — `code/02_services/01_ingestion/go-bridge/faketool/main.go:37` — Function nested too deeply (max: 5)
  > L37: `func main() {`
  - evidence: Accepted complexity (G10 reclassified 2026-10-03): 499-line linear faketool server main (flags, mux, handler closure); refactor not warranted now.
- [-] **AS-373** `complexity/deep-nesting` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:611` — Function nested too deeply (max: 5)
  > L611: `public void runWithBridge(String bridgeBinary) {`
  - evidence: Accepted complexity (G10 reclassified 2026-10-03): 245-line linear watchdog scheduling with nested lambdas; refactor not warranted now.
- [-] **AS-374** `complexity/deep-nesting` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java:699` — Function nested too deeply (max: 5)
  > L699: `public void processElement(RowData tick, Context ctx, Collector<RowData> out) throws Exception {`
  - evidence: Accepted complexity (G10 reclassified 2026-10-03): 426-line Flink hot-path processElement with deliberate inline eviction; a refactor only with a benchmark re-run.
- [f] **AS-375** `complexity/deep-nesting` — `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/FlussControlStateStore.java:99` — Function nested too deeply (max: 5)
  > L99: `public void replaySafetyHalts(Consumer<InternalRow> consumer) {`
  - evidence: False positive (G10 verified 2026-10-03): intended: canonical drain (P3-001: a single poll silently misses rows), shared with FlussIntentDedupStore/FlussProjectionLedgerStore.
- [-] **AS-376** `complexity/deep-nesting` — `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:212` — Function nested too deeply (max: 5)
  > L212: `static int run(String[] args) throws Exception {`
  - evidence: Accepted complexity (G10 reclassified 2026-10-03): 490-line linear CLI with sequential mode branches; refactor not warranted now.
- [f] **AS-479** `complexity/too-many-params` — `code/01_platform/04_scripts/env_facts.py:116` — Function has too many parameters (max: 6)
  - render_row() L116–L122 (7 lines)
  > L116: `def render_row(num, title, status, verified, check, recheck, body):`
  - evidence: False positive (G10 verified 2026-10-03): intended: the 7 params are the ledger fields this formatter writes; bundling adds indirection.
- [f] **AS-480** `complexity/too-many-params` — `code/01_platform/04_scripts/stale_table_kind_scan.py:541` — Function has too many parameters (max: 6)
  - classify_status() L541–L551 (11 lines)
  > L541: `def classify_status(lines: list[str], idx: int, claim_type: str, span: str,`
  - evidence: False positive (G10 verified 2026-10-03): intended: classification context (lines/idx/claim/span/heading/banner/doc-hist); pure 13-line function.
- [f] **AS-481** `complexity/too-many-params` — `code/01_platform/04_scripts/stale_table_kind_scan.py:554` — Function has too many parameters (max: 6)
  - classify_numeric() L554–L578 (25 lines)
  > L554: `def classify_numeric(lines: list[str], idx: int, claim_type: str, m: re.Match,`
  - evidence: False positive (G10 verified 2026-10-03): intended: same context as AS-480 for numeric-drift claims; pure function.
- [f] **AS-482** `complexity/too-many-params` — `code/01_platform/04_scripts/stale_table_kind_scan.py:581` — Function has too many parameters (max: 6)
  - classify() L581–L607 (27 lines)
  > L581: `def classify(lines: list[str], idx: int, claim_type: str, span: str,`
  - evidence: False positive (G10 verified 2026-10-03): intended: same context for the classifier dispatcher; pure function.
- [f] **AS-483** `complexity/too-many-params` — `code/01_platform/04_scripts/t9_order_sandbox.py:218` — Function has too many parameters (max: 6)
  - canonical() L218–L234 (17 lines)
  > L218: `def canonical(protocol_version, message_type, request_id, account_scope_id,`
  - evidence: False positive (G10 verified 2026-10-03): intended: the 10 params are the protocol fields in the byte-exact parity port (P3-079).
- [f] **AS-484** `complexity/too-many-params` — `code/01_platform/04_scripts/t9_order_sandbox.py:247` — Function has too many parameters (max: 6)
  - encode_envelope() L247–L269 (23 lines)
  > L247: `def encode_envelope(secret, protocol_version, message_type, request_id,`
  - evidence: False positive (G10 verified 2026-10-03): intended: protocol field list in the GatewayProtocol.encode() port; key order matches the Java writer.
- [f] **AS-485** `complexity/too-many-params` — `code/02_services/01_ingestion/go-bridge/main.go:358` — Function has too many parameters (max: 6)
  > L358: `func runHFT(ctx context.Context, cancel context.CancelFunc, client *arrow.Client, plan SubscriptionPlan, latencyMs int, responseTimeout time.Duration, refreshAuth func(co …`
  - evidence: False positive (G10 verified 2026-10-03): intended: dependency plumbing through a single call chain (ctx/cancel/client/plan/timeouts/refreshAuth/logf).
- [f] **AS-486** `complexity/too-many-params` — `code/02_services/01_ingestion/go-bridge/main.go:404` — Function has too many parameters (max: 6)
  > L404: `func runHFTEpoch(ctx context.Context, streamFactory hftStreamFactory, slot SlotAssignment, latencyMs int, responseTimeout time.Duration, epoch uint64, refreshAuth func(co …`
  - evidence: False positive (G10 verified 2026-10-03): intended: the same plumbing for the per-epoch supervisor.
- [f] **AS-487** `complexity/too-many-params` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:213` — Function has too many parameters (max: 6)
  > L213: `public void updateSlot(String slotId, String state, long epoch, int assigned, int acknowledged, int rejected, long frameNanos) {`
  - evidence: False positive (G10 verified 2026-10-03): intended: the 7 params are one compound slot-health write under the per-slot lock (P1-082).

### G11 — Size pressure (long functions & large files) (102 items)

**Status:** `[-]` accepted — 0/102 fixed · 6 false-positive · 91 won't-fix · 5 deferred · hidden defects XC-6…XC-10 fixed (CHG-534, `f776ba65`) · XC-32 fixed (CHG-538, `81dbd763`) · blind-verified 2026-10-03


**Why:** Functions >80 lines and files >400 lines slow every future change.

**How:** Split by responsibility, one file at a time. Large effort — treat as a backlog; [d] is an acceptable verdict with a trigger (e.g. next time the file is touched).

**Rules in this group:** `complexity/file-too-large` (47) · `complexity/function-too-long` (55)

- [-] **AS-377** `complexity/file-too-large` — `code/01_platform/04_scripts/audit_r2.py` — File too large (max: 400)
  - 728 lines on disk
  - evidence: single-purpose R2/S3 audit tool (config, SigV4, client, checks); 745 lines, no separable concern (G11 verification 2026-10-03)
- [-] **AS-378** `complexity/file-too-large` — `code/01_platform/04_scripts/cluster_check.py` — File too large (max: 400)
  - 693 lines on disk
  - evidence: single-purpose cluster validator (collect + checks + evidence + inline self-check); 735 lines (G11 verification 2026-10-03)
- [-] **AS-379** `complexity/file-too-large` — `code/01_platform/04_scripts/day_run.py` — File too large (max: 400)
  - 1372 lines on disk
  - evidence: single-purpose daily runner (pure decisions, Runner, Collector, verbs); 1372 lines (G11 verification 2026-10-03)
- [-] **AS-380** `complexity/file-too-large` — `code/01_platform/04_scripts/ddl_apply.py` — File too large (max: 400)
  - 858 lines on disk
  - evidence: single-purpose gated DDL-apply contract (parsers, manifest, apply, diff); 858 lines (G11 verification 2026-10-03)
- [-] **AS-381** `complexity/file-too-large` — `code/01_platform/04_scripts/ddl_apply_smoke.py` — File too large (max: 400)
  - 589 lines on disk
  - evidence: single-purpose exit-code smoke harness (S1/S2/S4 scenarios); 589 lines (G11 verification 2026-10-03)
- [-] **AS-382** `complexity/file-too-large` — `code/01_platform/04_scripts/disaster_drills.py` — File too large (max: 400)
  - 715 lines on disk
  - evidence: single-purpose drill runner (probes, plan/evidence, drive, suite); 715 lines (G11 verification 2026-10-03)
- [-] **AS-383** `complexity/file-too-large` — `code/01_platform/04_scripts/docs_audit.py` — File too large (max: 400)
  - 1755 lines on disk
  - evidence: single-purpose doc-vs-code gate; c1-c20 checks invoked linearly by main; 1755 lines (G11 verification 2026-10-03)
- [-] **AS-384** `complexity/file-too-large` — `code/01_platform/04_scripts/eod_schedule.py` — File too large (max: 400)
  - 442 lines on disk
  - evidence: single-purpose EOD scheduler; 442 lines (42 over threshold), one concern (G11 verification 2026-10-03)
- [-] **AS-385** `complexity/file-too-large` — `code/01_platform/04_scripts/fluss-probes/FeatureSpikeProbe.java` — File too large (max: 400)
  - 528 lines on disk
  - evidence: single-purpose Fluss probe; 528 lines (G11 verification 2026-10-03)
- [-] **AS-386** `complexity/file-too-large` — `code/01_platform/04_scripts/fluss-probes/FlussReadabilityProbe.java` — File too large (max: 400)
  - 761 lines on disk
  - evidence: single-purpose Fluss probe; 761 lines (G11 verification 2026-10-03)
- [-] **AS-387** `complexity/file-too-large` — `code/01_platform/04_scripts/fluss-probes/FlussSignalLatency.java` — File too large (max: 400)
  - 517 lines on disk
  - evidence: single-purpose Fluss probe; 517 lines (G11 verification 2026-10-03)
- [-] **AS-388** `complexity/file-too-large` — `code/01_platform/04_scripts/fluss-probes/SignalCandidatesViewer.java` — File too large (max: 400)
  - 463 lines on disk
  - evidence: single-purpose Fluss probe; 463 lines (G11 verification 2026-10-03)
- [-] **AS-389** `complexity/file-too-large` — `code/01_platform/04_scripts/gate_preflight.py` — File too large (max: 400)
  - 490 lines on disk
  - evidence: single-purpose read-only gate preflight (checks 1-8 + main); 490 lines (G11 verification 2026-10-03)
- [-] **AS-390** `complexity/file-too-large` — `code/01_platform/04_scripts/holistic-analyze.py` — File too large (max: 400)
  - 1874 lines on disk
  - evidence: single-purpose latency-analysis harness; 1914 lines; XC-10 deleted its unreachable retired-table parse loops (G11 verification 2026-10-03)
- [-] **AS-391** `complexity/file-too-large` — `code/01_platform/04_scripts/image_staleness_check.py` — File too large (max: 400)
  - 634 lines on disk
  - evidence: single-purpose image-staleness checker; 633 lines (G11 verification 2026-10-03)
- [-] **AS-392** `complexity/file-too-large` — `code/01_platform/04_scripts/o2-provision.py` — File too large (max: 400)
  - 1948 lines on disk
  - evidence: single-purpose provisioning tool; ~1200 of 1947 lines are embedded dashboard/alert specs; ~750 code lines remain over threshold (G11 verification 2026-10-03)
- [-] **AS-393** `complexity/file-too-large` — `code/01_platform/04_scripts/perf_evidence_parse.py` — File too large (max: 400)
  - 650 lines on disk
  - evidence: single-purpose perf snapshot builder; 650 lines (G11 verification 2026-10-03)
- [-] **AS-394** `complexity/file-too-large` — `code/01_platform/04_scripts/prod_node_check.py` — File too large (max: 400)
  - 489 lines on disk
  - evidence: single-purpose per-VM provisioning gate; 489 lines (G11 verification 2026-10-03)
- [-] **AS-395** `complexity/file-too-large` — `code/01_platform/04_scripts/r2_legal_hold_check.py` — File too large (max: 400)
  - 602 lines on disk
  - evidence: single-purpose legal-hold verifier; 602 lines (G11 verification 2026-10-03)
- [-] **AS-396** `complexity/file-too-large` — `code/01_platform/04_scripts/stage_capture_parse.py` — File too large (max: 400)
  - 691 lines on disk
  - evidence: single-purpose stage-capture report core; XC-8 extracted the duplicated window/percentile helper; 691 lines (G11 verification 2026-10-03)
- [-] **AS-397** `complexity/file-too-large` — `code/01_platform/04_scripts/stale_table_kind_scan.py` — File too large (max: 400)
  - 898 lines on disk
  - evidence: single-purpose stale-claim scanner; large declarative rule tables; 898 lines (G11 verification 2026-10-03)
- [-] **AS-398** `complexity/file-too-large` — `code/01_platform/04_scripts/t9_order_sandbox.py` — File too large (max: 400)
  - 1295 lines on disk
  - evidence: single-purpose T9 sandbox harness; XC-9 removed dead HostTransport + deduped the probe compile path; 1295 lines (G11 verification 2026-10-03)
- [-] **AS-399** `complexity/file-too-large` — `code/01_platform/04_scripts/values_at_rest_scan.py` — File too large (max: 400)
  - 476 lines on disk
  - evidence: single-purpose at-rest scanner; 476 lines (G11 verification 2026-10-03)
- [-] **AS-400** `complexity/file-too-large` — `code/01_platform/06_stage_profiler/stage_profiler.py` — File too large (max: 400)
  - 1581 lines on disk
  - evidence: single-purpose offline stage-profiler report core; 1581 lines (G11 verification 2026-10-03)
- [-] **AS-401** `complexity/file-too-large` — `code/02_services/01_ingestion/go-bridge/main.go` — File too large (max: 600)
  - 1085 lines on disk
  - evidence: bridge entry + run loop; package already split across 20+ files; 1085 lines (G11 verification 2026-10-03)
- [f] **AS-402** `complexity/file-too-large` — `code/02_services/01_ingestion/go-bridge/marketdata/market_data.pb.go` — File too large (max: 600)
  - 991 lines on disk
  - evidence: protoc-gen-go generated (line 19: 'Code generated ... DO NOT EDIT') — not hand-maintained (G11 verification 2026-10-03)
- [-] **AS-403** `complexity/file-too-large` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java` — File too large (max: 400)
  - 629 lines on disk
  - evidence: single-purpose table verify/ensure registry; 629 lines (G11 verification 2026-10-03)
- [d] **AS-404** `complexity/file-too-large` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java` — File too large (max: 400)
  - 2537 lines on disk
  - evidence: FOUR concerns (bridge supervision, tick admission, safety state machine, shutdown/monitors); near-duplicate journal blocks L1676/L2257 — split by region on next touch (G11 verification 2026-10-03)
- [-] **AS-405** `complexity/file-too-large` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java` — File too large (max: 400)
  - 737 lines on disk
  - evidence: single ingestion-config contract (validateFrom + validators); 737 lines (G11 verification 2026-10-03)
- [-] **AS-406** `complexity/file-too-large` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java` — File too large (max: 400)
  - 817 lines on disk
  - evidence: single OTLP emitter; 817 lines (G11 verification 2026-10-03)
- [-] **AS-407** `complexity/file-too-large` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java` — File too large (max: 400)
  - 623 lines on disk
  - evidence: single bounded Fluss append writer; 623 lines (G11 verification 2026-10-03)
- [-] **AS-408** `complexity/file-too-large` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java` — File too large (max: 400)
  - 1395 lines on disk
  - evidence: single keyed multi-timeframe operator; Slot state shared by all phases; 1407 lines (G11 verification 2026-10-03)
- [-] **AS-409** `complexity/file-too-large` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/N7RangeBreakoutStrategy.java` — File too large (max: 400)
  - 496 lines on disk
  - evidence: single N7 strategy; 496 lines (G11 verification 2026-10-03)
- [-] **AS-410** `complexity/file-too-large` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java` — File too large (max: 400)
  - 851 lines on disk
  - evidence: single SignalJob definition/assembly; 851 lines (G11 verification 2026-10-03)
- [-] **AS-411** `complexity/file-too-large` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java` — File too large (max: 400)
  - 1433 lines on disk
  - evidence: single config record + env validation; 1433 lines (G11 verification 2026-10-03)
- [-] **AS-412** `complexity/file-too-large` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/StrategyHostFunction.java` — File too large (max: 400)
  - 1130 lines on disk
  - evidence: single CoProcess strategy host; 1130 lines (G11 verification 2026-10-03)
- [f] **AS-413** `complexity/file-too-large` — `code/02_services/04_executor/src/bridge/transport.rs` — File too large (max: 1000)
  - 1521 lines on disk
  - evidence: production 754 lines < 1000 threshold; inline #[cfg(test)] module (766 lines) drives the count (G11 verification 2026-10-03)
- [-] **AS-414** `complexity/file-too-large` — `code/02_services/04_executor/src/execution/client.rs` — File too large (max: 1000)
  - 2595 lines on disk
  - evidence: production 1319 lines > 1000; single ExecutionClient impl; tests are 1276 lines (G11 verification 2026-10-03)
- [f] **AS-415** `complexity/file-too-large` — `code/02_services/04_executor/src/executiongate.rs` — File too large (max: 1000)
  - 1606 lines on disk
  - evidence: production 552 lines < 1000; inline #[cfg(test)] module drives the count (G11 verification 2026-10-03)
- [-] **AS-416** `complexity/file-too-large` — `code/02_services/04_executor/src/http.rs` — File too large (max: 1000)
  - 3653 lines on disk
  - evidence: production 1674 lines > 1000; single control-plane HTTP server; tests are 1978 lines (G11 verification 2026-10-03)
- [f] **AS-417** `complexity/file-too-large` — `code/02_services/04_executor/src/projection/mod.rs` — File too large (max: 1000)
  - 1696 lines on disk
  - evidence: production 830 lines < 1000; inline #[cfg(test)] module drives the count (G11 verification 2026-10-03)
- [-] **AS-418** `complexity/file-too-large` — `code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java` — File too large (max: 400)
  - 442 lines on disk
  - evidence: single mock TCP server; 442 lines (G11 verification 2026-10-03)
- [-] **AS-419** `complexity/file-too-large` — `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/FlussProjectionWriter.java` — File too large (max: 400)
  - 592 lines on disk
  - evidence: single gateway projection writer; 592 lines (G11 verification 2026-10-03)
- [-] **AS-420** `complexity/file-too-large` — `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/GatewayHttpServer.java` — File too large (max: 400)
  - 575 lines on disk
  - evidence: single gateway HTTP surface; 575 lines (G11 verification 2026-10-03)
- [-] **AS-421** `complexity/file-too-large` — `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java` — File too large (max: 400)
  - 1785 lines on disk
  - evidence: single offline DDL CLI implementing the documented 9-step contract; 1785 lines (G11 verification 2026-10-03)
- [-] **AS-422** `complexity/file-too-large` — `code/common/src/main/java/com/trading/common/schema/eod/EodControllerTool.java` — File too large (max: 400)
  - 745 lines on disk
  - evidence: single EOD controller CLI (status/run/extend/reconcile/reset/tiering); 745 lines (G11 verification 2026-10-03)
- [-] **AS-423** `complexity/file-too-large` — `code/common/src/main/java/com/trading/common/schema/execution/FlussGateStateStore.java` — File too large (max: 400)
  - 513 lines on disk
  - evidence: single Fluss GateStateStore impl; 513 lines (G11 verification 2026-10-03)
- [-] **AS-424** `complexity/function-too-long` — `code/01_platform/04_scripts/alert-routing-selftest.py:110` — Function too long (max: 80)
  - main() L110–L217 (108 lines)
  > L110: `def main() -> int:`
  - evidence: linear selftest flow (healthz -> alert create -> poll -> cleanup); 108 lines, one concern (G11 verification 2026-10-03)
- [-] **AS-425** `complexity/function-too-long` — `code/01_platform/04_scripts/cluster_check.py:561` — Function too long (max: 80)
  - self_check() L561–L689 (129 lines)
  > L561: `def self_check():`
  - evidence: offline scenario harness; 8 independent scenarios; 129 lines (G11 verification 2026-10-03)
- [-] **AS-426** `complexity/function-too-long` — `code/01_platform/04_scripts/day_run.py:418` — Function too long (max: 80)
  - evaluate() L418–L600 (183 lines)
  > L418: `def evaluate(facts: Facts) -> list:`
  - evidence: pure I1-I9 check evaluator; self-contained ordered blocks; 183 lines (G11 verification 2026-10-03)
- [-] **AS-427** `complexity/function-too-long` — `code/01_platform/04_scripts/ddl_apply.py:721` — Function too long (max: 80)
  - main() L721–L854 (134 lines)
  > L721: `def main():`
  - evidence: CLI apply-contract orchestration with early-return gates; 134 lines (G11 verification 2026-10-03)
- [-] **AS-428** `complexity/function-too-long` — `code/01_platform/04_scripts/ddl_apply_smoke.py:460` — Function too long (max: 80)
  - main() L460–L585 (126 lines)
  > L460: `def main():`
  - evidence: setup/scenarios/teardown smoke driver; 126 lines (G11 verification 2026-10-03)
- [-] **AS-429** `complexity/function-too-long` — `code/01_platform/04_scripts/disaster_drills.py:453` — Function too long (max: 80)
  - drive() L453–L646 (194 lines)
  > L453: `def drive(d, suite_id, approve, out_dir, verbose, deadline=None):`
  - evidence: single drill state machine (fault -> probes -> recovery -> evidence); 194 lines (G11 verification 2026-10-03)
- [-] **AS-430** `complexity/function-too-long` — `code/01_platform/04_scripts/docs_audit.py:652` — Function too long (max: 80)
  - c9_dec039_invariants() L652–L776 (125 lines)
  > L652: `def c9_dec039_invariants():`
  - evidence: one audit criterion's checklist (C9 DEC-039); 125 lines (G11 verification 2026-10-03)
- [-] **AS-431** `complexity/function-too-long` — `code/01_platform/04_scripts/eod_schedule.py:321` — Function too long (max: 80)
  - main() L321–L438 (118 lines)
  > L321: `def main(argv: list[str] | None = None) -> int:`
  - evidence: CLI entry with mutually exclusive modes; 118 lines (G11 verification 2026-10-03)
- [-] **AS-432** `complexity/function-too-long` — `code/01_platform/04_scripts/fluss-probes/CandleVerify.java:57` — Function too long (max: 80)
  > L57: `public static void main(String[] args) throws Exception {`
  - evidence: single probe main (parse + parity + report); 194 lines (G11 verification 2026-10-03)
- [-] **AS-433** `complexity/function-too-long` — `code/01_platform/04_scripts/fluss-probes/EventDayProbe.java:53` — Function too long (max: 80)
  > L53: `public static void main(String[] args) throws Exception {`
  - evidence: single probe main; 104 lines (G11 verification 2026-10-03)
- [-] **AS-434** `complexity/function-too-long` — `code/01_platform/04_scripts/fluss-probes/FlussPrefixReader.java:92` — Function too long (max: 80)
  > L92: `static void run(String[] args) throws Exception {`
  - evidence: single probe run; 180 lines (G11 verification 2026-10-03)
- [-] **AS-435** `complexity/function-too-long` — `code/01_platform/04_scripts/fluss-probes/RawCompressionProbe.java:153` — Function too long (max: 80)
  > L153: `static void write(Connection conn, Admin admin, long rows) throws Exception {`
  - evidence: single probe write routine; 102 lines (G11 verification 2026-10-03)
- [-] **AS-436** `complexity/function-too-long` — `code/01_platform/04_scripts/fused_timeline.py:231` — Function too long (max: 80)
  - main() L231–L422 (192 lines)
  > L231: `def main():`
  - evidence: linear fusion pipeline; XC-6 extracted the bounded forward_fill helper and fixed the stale fill; 192 lines (G11 verification 2026-10-03)
- [-] **AS-437** `complexity/function-too-long` — `code/01_platform/04_scripts/gate_preflight.py:357` — Function too long (max: 80)
  - main() L357–L485 (129 lines)
  > L357: `def main(certifying: bool = True) -> int:`
  - evidence: sequential preflight checks with two accumulators; 129 lines (G11 verification 2026-10-03)
- [d] **AS-438** `complexity/function-too-long` — `code/01_platform/04_scripts/holistic-analyze.py:714` — Function too long (max: 80)
  - main() L714–L1870 (1157 lines)
  > L714: `def main():`
  - evidence: ~12 analysis legs in one main(); nested defs L993/1027/1256 are the extractable seams — split by leg on next touch (G11 verification 2026-10-03)
- [-] **AS-439** `complexity/function-too-long` — `code/01_platform/04_scripts/o2-provision.py:1513` — Function too long (max: 80)
  - provision_dashboards() L1513–L1628 (116 lines)
  > L1513: `def provision_dashboards() -> int:`
  - evidence: single dashboard convergence loop; 116 lines (G11 verification 2026-10-03)
- [-] **AS-440** `complexity/function-too-long` — `code/01_platform/04_scripts/seed_alerts.py:70` — Function too long (max: 80)
  - main() L70–L185 (116 lines)
  > L70: `def main() -> int:`
  - evidence: linear seed CLI; 116 lines (G11 verification 2026-10-03)
- [-] **AS-441** `complexity/function-too-long` — `code/01_platform/04_scripts/stale_table_kind_scan.py:610` — Function too long (max: 80)
  - scan_file() L610–L758 (149 lines)
  > L610: `def scan_file(path: Path) -> list[tuple[int, int, str, str, str]]:`
  - evidence: single-pass scanner; repetitive but one responsibility; 149 lines (G11 verification 2026-10-03)
- [-] **AS-442** `complexity/function-too-long` — `code/01_platform/04_scripts/t8_sandbox_contract_check.py:77` — Function too long (max: 80)
  - main() L77–L285 (209 lines)
  > L77: `def main():`
  - evidence: 11 numbered self-contained contract groups; 209 lines (G11 verification 2026-10-03)
- [-] **AS-443** `complexity/function-too-long` — `code/01_platform/04_scripts/t9_order_sandbox.py:452` — Function too long (max: 80)
  - offline_contract() L452–L577 (126 lines)
  > L452: `def offline_contract():`
  - evidence: linear offline checklist; 126 lines (G11 verification 2026-10-03)
- [-] **AS-444** `complexity/function-too-long` — `code/01_platform/04_scripts/t9_order_sandbox.py:918` — Function too long (max: 80)
  - run_live() L918–L1088 (171 lines)
  > L918: `def run_live(transport=None, probe=None, secret="local-dev-only", now=None,`
  - evidence: live drill with early-return classifier per hop; 171 lines (G11 verification 2026-10-03)
- [-] **AS-445** `complexity/function-too-long` — `code/01_platform/04_scripts/t9_order_sandbox.py:1176` — Function too long (max: 80)
  - _self_check() L1176–L1291 (116 lines)
  > L1176: `def _self_check(out_dir, run_id):`
  - evidence: self-check harness; 116 lines (G11 verification 2026-10-03)
- [-] **AS-446** `complexity/function-too-long` — `code/01_platform/06_stage_profiler/stage_profiler.py:1317` — Function too long (max: 80)
  - build_report() L1317–L1527 (211 lines)
  > L1317: `def build_report(phase: Path) -> tuple[list[ProfileRow], str, dict[str, int]]:`
  - evidence: straight-line per-stage report builder; 211 lines (G11 verification 2026-10-03)
- [-] **AS-447** `complexity/function-too-long` — `code/02_services/01_ingestion/go-bridge/cmd/gen-corpus/main.go:269` — Function too long (max: 80)
  > L269: `func main() {`
  - evidence: single-purpose corpus generator CLI; 99 lines (G11 verification 2026-10-03)
- [d] **AS-448** `complexity/function-too-long` — `code/02_services/01_ingestion/go-bridge/faketool/main.go:37` — Function too long (max: 80)
  > L37: `func main() {`
  - evidence: test-only tool mixing subscription protocol, two tick modes, fault injection, snapshot pacing — extract per mode on next touch (G11 verification 2026-10-03)
- [-] **AS-449** `complexity/function-too-long` — `code/02_services/01_ingestion/go-bridge/main.go:103` — Function too long (max: 80)
  > L103: `func main() {`
  - evidence: linear CLI orchestration (env -> auth -> plan -> counters -> run); 202 lines (G11 verification 2026-10-03)
- [-] **AS-450** `complexity/function-too-long` — `code/02_services/01_ingestion/go-bridge/main.go:404` — Function too long (max: 80)
  > L404: `func runHFTEpoch(ctx context.Context, streamFactory hftStreamFactory, slot SlotAssignment, latencyMs int, responseTimeout time.Duration, epoch uint64, refreshAuth func(co …`
  - evidence: single epoch lifecycle; length from closures capturing ~12 epoch locals; 343 lines (G11 verification 2026-10-03) · full audit 2026-10-03: hidden defect XC-32 fixed in CHG-538 (`81dbd763`) — read-loop auth refresh off-by-one; finding remains accepted complexity.
- [-] **AS-451** `complexity/function-too-long` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:408` — Function too long (max: 80)
  > L408: `public static void main(String[] args) throws Exception {`
  - evidence: linear numbered startup phases; 168 lines (G11 verification 2026-10-03)
- [-] **AS-452** `complexity/function-too-long` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:611` — Function too long (max: 80)
  > L611: `public void runWithBridge(String bridgeBinary) {`
  - evidence: bridge supervision loop; XC-7 made unexpected loop failures fatal (BRIDGE_CRASH, non-zero exit); 241 lines (G11 verification 2026-10-03)
- [-] **AS-453** `complexity/function-too-long` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:1675` — Function too long (max: 80)
  > L1675: `private void processBridgeEvent(BridgeEvent event) {`
  - evidence: single-purpose bridge-event router; 131 lines (G11 verification 2026-10-03)
- [-] **AS-454** `complexity/function-too-long` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2161` — Function too long (max: 80)
  > L2161: `private void shutdown() {`
  - evidence: ordered teardown sequence; 159 lines (G11 verification 2026-10-03)
- [-] **AS-455** `complexity/function-too-long` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java:127` — Function too long (max: 80)
  > L127: `static ManifestResult loadFromPath(String path, int version) {`
  - evidence: single manifest loader; 121 lines (G11 verification 2026-10-03)
- [-] **AS-456** `complexity/function-too-long` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:188` — Function too long (max: 80)
  > L188: `static IngestionConfig validateFrom(Map<String, String> env, boolean guardSecrets) {`
  - evidence: centralized env->config validator with error accumulation; 186 lines (G11 verification 2026-10-03)
- [-] **AS-457** `complexity/function-too-long` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:103` — Function too long (max: 80)
  > L103: `private TickPacket(Builder b) {`
  - evidence: constructor = invariant validation + final-field copy; 120 lines (G11 verification 2026-10-03)
- [-] **AS-458** `complexity/function-too-long` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:516` — Function too long (max: 80)
  > L516: `public String buildMetricsJson() {`
  - evidence: single OTLP JSON serialization sequence; 98 lines (G11 verification 2026-10-03)
- [-] **AS-459** `complexity/function-too-long` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java:90` — Function too long (max: 80)
  > L90: `public boolean tryAccept(int recordBytes) {`
  - evidence: single accept-gate state machine; 96 lines (G11 verification 2026-10-03)
- [-] **AS-460** `complexity/function-too-long` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java:486` — Function too long (max: 80)
  > L486: `private static boolean updateMarketSnapshot(MultiTimeframeState state, RowData tick, long eventTime) {`
  - evidence: unrolled hot-path field updates in 3 labeled sections; 206 lines (G11 verification 2026-10-03)
- [d] **AS-461** `complexity/function-too-long` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java:699` — Function too long (max: 80)
  > L699: `public void processElement(RowData tick, Context ctx, Collector<RowData> out) throws Exception {`
  - evidence: admission/gap/timer/window/emission in one method; 5x duplicated pending-cap guard L939-L1043 — extract guard/accumulation helper on next touch (G11 verification 2026-10-03)
- [-] **AS-462** `complexity/function-too-long` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java:1226` — Function too long (max: 80)
  > L1226: `public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out) throws Exception {`
  - evidence: single timer demux (session close + event-time close); 149 lines (G11 verification 2026-10-03)
- [-] **AS-463** `complexity/function-too-long` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java:147` — Function too long (max: 80)
  > L147: `public static StreamExecutionEnvironment buildTopology(SignalJobConfig config) {`
  - evidence: linear Flink topology assembly; 321 lines (G11 verification 2026-10-03)
- [-] **AS-464** `complexity/function-too-long` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java:504` — Function too long (max: 80)
  > L504: `static void applyRuntimeOptions(SignalJobConfig config, Configuration flinkConfig) {`
  - evidence: single Configuration assembly; stale 0.4 comment fixed to 0.6 (L551); 218 lines (G11 verification 2026-10-03)
- [-] **AS-465** `complexity/function-too-long` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java:189` — Function too long (max: 80)
  > L189: `public static SignalJobConfig from(Map<String, String> env) {`
  - evidence: env parsing + fail-closed cross-field validation; 229 lines (G11 verification 2026-10-03)
- [-] **AS-466** `complexity/function-too-long` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/StrategyHostFunction.java:312` — Function too long (max: 80)
  > L312: `public void processElement1(RowData live, Context ctx, Collector<RowData> out)`
  - evidence: single strategy fan-out; 95 lines (G11 verification 2026-10-03)
- [f] **AS-467** `complexity/function-too-long` — `code/02_services/04_executor/src/bridge/protocol.rs:663` — Function too long (max: 120)
  > L663: `fn order_validation_matches_the_go_bridge() {`
  - evidence: #[cfg(test)] fixture (mod tests L615); tests are exempt (G11 verification 2026-10-03)
- [f] **AS-468** `complexity/function-too-long` — `code/02_services/04_executor/src/durable_file.rs:728` — Function too long (max: 120)
  > L728: `fn a_torn_final_line_is_dropped_but_a_corrupt_one_is_refused() {`
  - evidence: #[cfg(test)] fixture; 43 lines < 120 threshold (scanner brace miscount) — double false positive (G11 verification 2026-10-03)
- [-] **AS-469** `complexity/function-too-long` — `code/02_services/06_execution_bridge/go-bridge/postback.go:68` — Function too long (max: 80)
  > L68: `func runPostbackLoop(ctx context.Context, connect func() (OrderUpdateSource, error), publish func(ReportEnvelope) error, onError func(error), initialBackoff, maxBackoff t …`
  - evidence: single reconnect loop; ~45/139 lines are concurrency comments; 139 lines (G11 verification 2026-10-03)
- [-] **AS-470** `complexity/function-too-long` — `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/ExecutionGatewayMain.java:72` — Function too long (max: 80)
  > L72: `public static void main(String[] args) throws Exception {`
  - evidence: CLI boot lifecycle with ordered drain; 121 lines (G11 verification 2026-10-03)
- [-] **AS-471** `complexity/function-too-long` — `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/GatewayHttpServer.java:154` — Function too long (max: 80)
  > L154: `private void approve(HttpExchange x) throws IOException {`
  - evidence: single HTTP endpoint handler (approve); 91 lines (G11 verification 2026-10-03)
- [d] **AS-472** `complexity/function-too-long` — `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:212` — Function too long (max: 80)
  > L212: `static int run(String[] args) throws Exception {`
  - evidence: two alternate modes duplicate the connection lifecycle (L218-L269); live-apply twin loop L466-L557 — extract mode helpers on next touch (G11 verification 2026-10-03)
- [-] **AS-473** `complexity/function-too-long` — `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:1394` — Function too long (max: 80)
  > L1394: `static boolean isSmokeFixtureRow(InternalRow row, RowType rowType) {`
  - evidence: type-dispatched fixture comparator; 18 irreducible type arms; 124 lines (G11 verification 2026-10-03)
- [-] **AS-474** `complexity/function-too-long` — `code/common/src/main/java/com/trading/common/schema/ddl/DdlText.java:129` — Function too long (max: 80)
  > L129: `public static ParsedDdl parse(String text, String sourcePath) {`
  - evidence: one parse pipeline (strip -> columns -> PK -> options); 149 lines (G11 verification 2026-10-03)
- [-] **AS-475** `complexity/function-too-long` — `code/common/src/main/java/com/trading/common/schema/eod/EodControllerTool.java:621` — Function too long (max: 80)
  > L621: `static Options parse(String[] args, Function<String, String> env) {`
  - evidence: single CLI option parser; 98 lines (G11 verification 2026-10-03)
- [-] **AS-476** `complexity/function-too-long` — `code/common/src/main/java/com/trading/common/schema/execution/ExecutionCommandGate.java:138` — Function too long (max: 80)
  > L138: `public Result execute(Command cmd) {`
  - evidence: single command-gate state machine; 118 lines (G11 verification 2026-10-03)
- [-] **AS-477** `complexity/function-too-long` — `code/common/src/main/java/com/trading/common/schema/position/PositionProjector.java:77` — Function too long (max: 80)
  > L77: `public static ProjectionResult apply(PositionSnapshot current, FillEvent fill, long nowMs) {`
  - evidence: one fill projection (version gate -> arithmetic -> lifecycle -> snapshot); 122 lines (G11 verification 2026-10-03)
- [-] **AS-478** `complexity/function-too-long` — `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:77` — Function too long (max: 80)
  > L77: `public ProjectionResult project(NormalizedPostback postback, long nowMs) {`
  - evidence: one postback projection pipeline; 114 lines (G11 verification 2026-10-03)

### G12 — Formatting (61 items)

**Status:** `[f]` false-positive — 0/61 fixed · 61 false-positive · 0 won't-fix · 0 deferred · cross-check found 13 hidden defects in these files (XC-11…XC-23, CHG-535 (`e6f046ac`))


**Verification (2026-10-03):** aislop's `python-formatting` is `ruff format` (its own help text says "auto-format with ruff"); ruff 0.15.15 flags the same 61 files with defaults. The repo has **no** ruff/black config, no formatting Makefile target, no formatting test, and `git log -S "ruff format"` shows it was never adopted: 271 of 273 tracked `.py` files would be reformatted. Applying it would change ~16.7k diff lines (7,082 added / 3,048 removed) across the 61 — pure style churn against a deliberate house style. All 61 are hand-written (no generated/vendored files).

**Why:** Mechanical ruff-format drift.

**How:** `ruff format` per file. Do this AFTER functional groups to avoid churn in review diffs. Verify no semantic change (format-only).

**Rules in this group:** `python-formatting` (61)

- [f] **AS-488** `python-formatting` — `code/01_platform/01_docker/alert-consumer.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-489** `python-formatting` — `code/01_platform/04_scripts/alert-routing-selftest.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-490** `python-formatting` — `code/01_platform/04_scripts/audit_r2.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-491** `python-formatting` — `code/01_platform/04_scripts/catalog_drift.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-492** `python-formatting` — `code/01_platform/04_scripts/change_control_check.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-493** `python-formatting` — `code/01_platform/04_scripts/check_flink_properties.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-494** `python-formatting` — `code/01_platform/04_scripts/clean_break_drill.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-495** `python-formatting` — `code/01_platform/04_scripts/cluster_check.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-496** `python-formatting` — `code/01_platform/04_scripts/compose_config_redact.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-497** `python-formatting` — `code/01_platform/04_scripts/cp_phase_capture.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-498** `python-formatting` — `code/01_platform/04_scripts/day_run.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03); hidden defect XC-21 found here (fixed in CHG-535)
- [f] **AS-499** `python-formatting` — `code/01_platform/04_scripts/ddl_apply.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-500** `python-formatting` — `code/01_platform/04_scripts/ddl_apply_smoke.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-501** `python-formatting` — `code/01_platform/04_scripts/deploy_preflight.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-502** `python-formatting` — `code/01_platform/04_scripts/deployed_artifact_verify.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03); hidden defect XC-18 found here (fixed in CHG-535)
- [f] **AS-503** `python-formatting` — `code/01_platform/04_scripts/disaster_drills.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03); hidden defect XC-15 found here (fixed in CHG-535)
- [f] **AS-504** `python-formatting` — `code/01_platform/04_scripts/docs_audit.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-505** `python-formatting` — `code/01_platform/04_scripts/env_facts.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-506** `python-formatting` — `code/01_platform/04_scripts/eod_controller.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-507** `python-formatting` — `code/01_platform/04_scripts/eod_schedule.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-508** `python-formatting` — `code/01_platform/04_scripts/evidence_ownership_check.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-509** `python-formatting` — `code/01_platform/04_scripts/execution_network_check.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-510** `python-formatting` — `code/01_platform/04_scripts/fluss-client-metrics.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03); hidden defect XC-12 found here (fixed in CHG-535)
- [f] **AS-511** `python-formatting` — `code/01_platform/04_scripts/fluss-repair/LogScan.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-512** `python-formatting` — `code/01_platform/04_scripts/fluss-repair/verify-and-truncate.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-513** `python-formatting` — `code/01_platform/04_scripts/fused_timeline.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03); hidden defect XC-11 found here (fixed in CHG-535)
- [f] **AS-514** `python-formatting` — `code/01_platform/04_scripts/gate_memo.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-515** `python-formatting` — `code/01_platform/04_scripts/gate_preflight.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-516** `python-formatting` — `code/01_platform/04_scripts/holistic-analyze.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03); hidden defect XC-19 found here (fixed in CHG-535)
- [f] **AS-517** `python-formatting` — `code/01_platform/04_scripts/image_staleness_check.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-518** `python-formatting` — `code/01_platform/04_scripts/implementation_gate.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-519** `python-formatting` — `code/01_platform/04_scripts/ing-tcp001/reconcile-compare.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03); hidden defect XC-20 found here (fixed in CHG-535)
- [f] **AS-520** `python-formatting` — `code/01_platform/04_scripts/jfr-analyze.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-521** `python-formatting` — `code/01_platform/04_scripts/latency_probe.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-522** `python-formatting` — `code/01_platform/04_scripts/local_int_004_smoke.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-523** `python-formatting` — `code/01_platform/04_scripts/o2-provision.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-524** `python-formatting` — `code/01_platform/04_scripts/o2_ingest.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-525** `python-formatting` — `code/01_platform/04_scripts/perf_evidence_parse.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-526** `python-formatting` — `code/01_platform/04_scripts/pernode_attribution_check.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-527** `python-formatting` — `code/01_platform/04_scripts/placement_check.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-528** `python-formatting` — `code/01_platform/04_scripts/plan_tracker.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-529** `python-formatting` — `code/01_platform/04_scripts/pom-snapshot-scan.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-530** `python-formatting` — `code/01_platform/04_scripts/prod_node_check.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-531** `python-formatting` — `code/01_platform/04_scripts/r2_archive_selection.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-532** `python-formatting` — `code/01_platform/04_scripts/r2_archive_sync.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-533** `python-formatting` — `code/01_platform/04_scripts/r2_legal_hold_check.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03); hidden defect XC-13 found here (fixed in CHG-535)
- [f] **AS-534** `python-formatting` — `code/01_platform/04_scripts/seed_alerts.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03); hidden defect XC-14 found here (fixed in CHG-535)
- [f] **AS-535** `python-formatting` — `code/01_platform/04_scripts/seed_dashboards.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-536** `python-formatting` — `code/01_platform/04_scripts/skip_inventory.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-537** `python-formatting` — `code/01_platform/04_scripts/soak-o2-evidence.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-538** `python-formatting` — `code/01_platform/04_scripts/stage_capture_parse.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-539** `python-formatting` — `code/01_platform/04_scripts/stage_gc_summary.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-540** `python-formatting` — `code/01_platform/04_scripts/stale_table_kind_scan.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03); hidden defect XC-16 found here (fixed in CHG-535)
- [f] **AS-541** `python-formatting` — `code/01_platform/04_scripts/strategy_live_board.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-542** `python-formatting` — `code/01_platform/04_scripts/t8_sandbox_contract_check.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-543** `python-formatting` — `code/01_platform/04_scripts/t9_order_sandbox.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-544** `python-formatting` — `code/01_platform/04_scripts/tablet-orphan-sweep.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03); hidden defect XC-22 found here (fixed in CHG-535)
- [f] **AS-545** `python-formatting` — `code/01_platform/04_scripts/values_at_rest_scan.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)
- [f] **AS-546** `python-formatting` — `code/01_platform/04_scripts/version_matrix_verify.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03); hidden defect XC-23 found here (fixed in CHG-535)
- [f] **AS-547** `python-formatting` — `code/01_platform/05_instruments/split_manifest.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03); hidden defect XC-17 found here (fixed in CHG-535)
- [f] **AS-548** `python-formatting` — `code/01_platform/06_stage_profiler/stage_profiler.py` — Python file is not formatted correctly
  - evidence: ruff-format drift only; the repo has no formatter contract (271/273 tracked .py files flagged; never adopted; AST+comments identical if applied) — style-policy false positive (G12 cross-check 2026-10-03)

### G13 — Comment policy (default WONTFIX) (234 items)

**Status:** `[x]`/`[-]` closed — 4/234 fixed (findings cleared) · 0 false-positive · 230 won't-fix (deliberate convention; 9 corrected in place — 3 in CHG-536, 6 in CHG-539) · 0 deferred · cross-check XC-24…XC-30 fixed (CHG-536 (`1be7ec11`)) + XC-36…XC-41 fixed (CHG-539 (`d23926d4`), XC-37 in CHG-538 (`81dbd763`))


**Verification (2026-10-03):** rule split: 211 `narrative-comment` (193 decorative separators/section headers, 9 cross-reference prose, 7 JSDoc preambles) + 23 `meta-comment` (17 before/after narration, 6 plan/process step refs). The repo's annotation convention deliberately keeps dated/issue-referenced narration and section separators; the real defect class here is a comment whose factual claim is stale (the XC-6 / SignalJob "0.4" precedent) — under independent review now.

**Why:** The tool flags narrative and plan/evidence comments; this repo's AGENTS.md mandates exactly that style, so this is a policy mismatch by design.

**How:** Default verdict [f] (not a defect) or [-] (won't fix). Revisit only if you decide to change the comment convention — then optionally add a tuned .aislop/config.yml to silence them.

**Rules in this group:** `ai-slop/meta-comment` (23) · `ai-slop/narrative-comment` (211)

- [-] **AS-051** `ai-slop/meta-comment` — `code/01_platform/04_scripts/holistic-analyze.py:1591` — Meta/plan comment (before/after state narration)
  > L1591: `# Parity-only mode (TM-kill drill, 2026-08-31): without deliberate`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-052** `ai-slop/meta-comment` — `code/01_platform/04_scripts/ing-tcp001/TokenCountReconcile.java:20` — Meta/plan comment (before/after state narration)
  > L20: `/**`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [x] **AS-053** `ai-slop/meta-comment` — `code/01_platform/04_scripts/ing-tcp001/TokenCountReconcile.java:195` — Meta/plan comment (before/after state narration)
  > L195: `// P6-118: the scanner used to be capped at limit(1_000_000_000),`
  - evidence: the quarantine census used createBatchScanner on a LOG table with no limit; the pinned client rejects it and a limited scan reads only a bucket segment, so the count threw every run — fixed in CHG-536 (`1be7ec11`) (both tables via the offset-paged scanLog; gate guards red pre-fix); cleared by rescan
- [-] **AS-054** `ai-slop/meta-comment` — `code/02_services/01_ingestion/go-bridge/supervisor.go:112` — Meta/plan comment (before/after state narration)
  > L112: `// P1-157: a panic in the slot's main flow used to be swallowed by the`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-055** `ai-slop/meta-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:859` — Meta/plan comment (before/after state narration)
  > L859: `// Proto-only default (2026-08-29): the NDJSON pipe transport was`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-056** `ai-slop/meta-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:1671` — Meta/plan comment (plan/process reference)
  > L1671: `/**`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-057** `ai-slop/meta-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2452` — Meta/plan comment (plan/process reference)
  > L2452: `/**`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-058** `ai-slop/meta-comment` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SourceIdleWatchdogState.java:7` — Meta/plan comment (before/after state narration)
  > L7: `/**`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-059** `ai-slop/meta-comment` — `code/02_services/04_executor/src/execution/client.rs:2519` — Meta/plan comment (before/after state narration)
  > L2519: `// FOK has no bridge equivalent: previously it was silently rewritten to DAY.`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-060** `ai-slop/meta-comment` — `code/02_services/04_executor/src/projection/mod.rs:1660` — Meta/plan comment (before/after state narration)
  > L1660: ``// Only `< 0` used to be rejected, so a zero-price fill diluted the weighted average.``
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-061** `ai-slop/meta-comment` — `code/02_services/04_executor/src/shutdown.rs:336` — Meta/plan comment (before/after state narration)
  > L336: `// P3-460: the fail-closed invariant of shutdown step 1 used to be *assumed* - a gate that`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-062** `ai-slop/meta-comment` — `code/02_services/06_execution_bridge/go-bridge/reauth.go:52` — Meta/plan comment (before/after state narration)
  > L52: `// P3-048/P3-049: disabled is fail-closed for commands, not just for /healthz.`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-063** `ai-slop/meta-comment` — `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/ExecutionGatewayMain.java:110` — Meta/plan comment (before/after state narration)
  > L110: `// P3-277: the reader is owned by try-with-resources so a throw from`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-064** `ai-slop/meta-comment` — `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/IntentReader.java:190` — Meta/plan comment (before/after state narration)
  > L190: `/**`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-065** `ai-slop/meta-comment` — `code/common/src/main/java/com/trading/common/ownership/OwnershipMatrix.java:55` — Meta/plan comment (before/after state narration)
  > L55: `// Defensive immutable copies: the caller's set used to be stored by`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-066** `ai-slop/meta-comment` — `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:301` — Meta/plan comment (plan/process reference)
  > L301: `// Step 2 — manifest/DDL checksums.`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-067** `ai-slop/meta-comment` — `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:365` — Meta/plan comment (plan/process reference)
  > L365: `// Step 4 — empty-catalog precondition.`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-068** `ai-slop/meta-comment` — `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:424` — Meta/plan comment (plan/process reference)
  > L424: `// Step 4 — apply in deterministic order.`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-069** `ai-slop/meta-comment` — `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:456` — Meta/plan comment (plan/process reference)
  > L456: `// Step 7 — write/read smoke per table. A raw-client write to a`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-070** `ai-slop/meta-comment` — `code/common/src/main/java/com/trading/common/schema/execution/FlussAttemptStore.java:213` — Meta/plan comment (before/after state narration)
  > L213: `/**`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-071** `ai-slop/meta-comment` — `code/common/src/main/java/com/trading/common/schema/execution/GateRow.java:106` — Meta/plan comment (before/after state narration)
  > L106: `/**`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-072** `ai-slop/meta-comment` — `code/common/src/main/java/com/trading/common/schema/execution/InMemoryGateStateStore.java:156` — Meta/plan comment (before/after state narration)
  > L156: `// Not the holder — fail closed, no mutation (offline fencing constraint). This used`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-073** `ai-slop/meta-comment` — `code/common/src/main/java/com/trading/common/schema/position/PositionProjectorDriver.java:42` — Meta/plan comment (before/after state narration)
  > L42: `/** Projected — the new snapshot replaced the previous one. */`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-074** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/alert-routing-selftest.py:113` — Narrative comment block (decorative separator)
  > L113: `# --- 1. consumer alive --------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-075** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/alert-routing-selftest.py:119` — Narrative comment block (decorative separator)
  > L119: `# --- 2. negative: malformed body rejected, consumer survives ------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-076** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/alert-routing-selftest.py:128` — Narrative comment block (decorative separator)
  > L128: `# --- 3. temp always-firing alert ---------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-077** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/alert-routing-selftest.py:179` — Narrative comment block (decorative separator)
  > L179: `# --- 4-5. poll the durable record ---------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-078** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/audit_r2.py:2` — Narrative comment block (decorative separator)
  > L2: `# =============================================================================`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check) · full audit 2026-10-03: stale claim XC-36 corrected in CHG-539 (`d23926d4`) (L159→L301); style remains flagged.
- [-] **AS-079** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/audit_r2.py:112` — Narrative comment block (decorative separator)
  > L112: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-080** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/audit_r2.py:165` — Narrative comment block (decorative separator)
  > L165: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-081** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/audit_r2.py:277` — Narrative comment block (decorative separator)
  > L277: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-082** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/audit_r2.py:430` — Narrative comment block (decorative separator)
  > L430: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-083** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/day_run.py:39` — Narrative comment block (decorative separator)
  > L39: `# --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-084** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/day_run.py:119` — Narrative comment block (decorative separator)
  > L119: `# --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-085** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/day_run.py:184` — Narrative comment block (decorative separator)
  > L184: `# --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-086** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/day_run.py:603` — Narrative comment block (decorative separator)
  > L603: `# --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-087** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/day_run.py:641` — Narrative comment block (decorative separator)
  > L641: `# --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-088** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/day_run.py:1011` — Narrative comment block (decorative separator)
  > L1011: `# --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-089** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/day_run.py:1198` — Narrative comment block (phase/section header)
  > L1198: `# Phase 2: execution-t3 chain in its offline posture (profile-gated;`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-090** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/day_run.py:1350` — Narrative comment block (decorative separator)
  > L1350: `# --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-091** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/disaster_drills.py:104` — Narrative comment block (decorative separator)
  > L104: `# --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-092** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/disaster_drills.py:248` — Narrative comment block (decorative separator)
  > L248: `# --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-093** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:387` — Narrative comment block (cross-reference commentary)
  > L387: `# One measurement, used by both the check and its message: a second call`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-094** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:428` — Narrative comment block (decorative separator)
  > L428: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-095** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:630` — Narrative comment block (decorative separator)
  > L630: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-096** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:653` — Narrative comment block (decorative separator)
  > L653: `# --- doc scans (04-decisions.md is the dated record: DEC-012/018/039) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-097** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:677` — Narrative comment block (decorative separator)
  > L677: `# --- code: HFT modes ltpc (40 B) + full (196 B), nothing else accepted ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-098** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:700` — Narrative comment block (decorative separator)
  > L700: `# --- code: bridge converts ns -> ms ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-099** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:704` — Narrative comment block (decorative separator)
  > L704: `# --- manifest/DDL: ledger + halt kinds KV; ledger live in dev ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-100** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:765` — Narrative comment block (decorative separator)
  > L765: `# --- inventories include the candle table + ingestion_quarantine ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-101** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:779` — Narrative comment block (decorative separator)
  > L779: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-102** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:860` — Narrative comment block (decorative separator)
  > L860: `# --- coverage table in 11-testing-and-release.md ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-103** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:903` — Narrative comment block (decorative separator)
  > L903: `# --- dossier status rows (02-10) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-104** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:932` — Narrative comment block (decorative separator)
  > L932: `# --- REQ13-* ids are scoped to the master dossier (04) and defined there ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-105** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:956` — Narrative comment block (decorative separator)
  > L956: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-106** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:1028` — Narrative comment block (decorative separator)
  > L1028: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-107** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:1095` — Narrative comment block (decorative separator)
  > L1095: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-108** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:1146` — Narrative comment block (decorative separator)
  > L1146: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-109** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:1253` — Narrative comment block (decorative separator)
  > L1253: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-110** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/docs_audit.py:1468` — Narrative comment block (decorative separator)
  > L1468: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-111** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/holistic-analyze.py:729` — Narrative comment block (decorative separator)
  > L729: `# ---- previews: UNAVAILABLE after the 2026-09-05 multi-timeframe cutover ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-112** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/holistic-analyze.py:790` — Narrative comment block (decorative separator)
  > L790: `# ---- Final candle path: the LATENCY leg only ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-113** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/holistic-analyze.py:834` — Narrative comment block (decorative separator)
  > L834: `# ---- Signal_Candidates: volume by status + settlement balance ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-114** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/holistic-analyze.py:920` — Narrative comment block (decorative separator)
  > L920: `# ---- Signal-path latency (2026-08-31) -------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-115** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/holistic-analyze.py:1432` — Narrative comment block (decorative separator)
  > L1432: `# ---- G6 ASSERTIVE GUARDS (2026-08-30) --------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-116** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/holistic-analyze.py:1437` — Narrative comment block (decorative separator)
  > L1437: `# ---- D6 guard (2026-08-31): ingestion JVM live-set leak alarm ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-117** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/holistic-analyze.py:1569` — Narrative comment block (decorative separator)
  > L1569: `# ---- G7 DATA-QUALITY GUARDS (F2/F3 audit, 2026-08-30) ----------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-118** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/holistic-analyze.py:1632` — Narrative comment block (justification prose)
  > L1632: `# last counter sample timestamp: counter deltas only cover rounds`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-119** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/holistic-analyze.py:1778` — Narrative comment block (decorative separator)
  > L1778: `# ---- F6 (2026-08-31): raw-validation rejection counters ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check) · full audit 2026-10-03: stale claim XC-37 corrected in CHG-538 (`81dbd763`) (8→10 reasons); style remains flagged.
- [-] **AS-120** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/o2-provision.py:2` — Narrative comment block (decorative separator)
  > L2: `# =============================================================================`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check) · full audit 2026-10-03: stale claim XC-38 corrected in CHG-539 (`d23926d4`) (43→47 alerts); style remains flagged.
- [-] **AS-121** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/o2-provision.py:49` — Narrative comment block (decorative separator)
  > L49: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-122** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/o2-provision.py:910` — Narrative comment block (decorative separator)
  > L910: `# --- Ingestion phase (ING- prefix) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-123** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/o2-provision.py:1048` — Narrative comment block (decorative separator)
  > L1048: `# --- P8.3 SignalJob/Flink/collector rules (tracker 14, approved 2026-08-11) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-124** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/o2-provision.py:1236` — Narrative comment block (decorative separator)
  > L1236: `# --- 2026-08-22 single-pane: infra/JVM/host infra alerts (10-observability.md scale-up thresholds) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-125** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/r2_legal_hold_check.py:80` — Narrative comment block (decorative separator)
  > L80: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-126** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/r2_legal_hold_check.py:220` — Narrative comment block (decorative separator)
  > L220: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-127** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/r2_legal_hold_check.py:278` — Narrative comment block (decorative separator)
  > L278: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-128** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/r2_legal_hold_check.py:435` — Narrative comment block (decorative separator)
  > L435: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-129** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/stale_table_kind_scan.py:112` — Narrative comment block (decorative separator)
  > L112: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-130** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/stale_table_kind_scan.py:309` — Narrative comment block (decorative separator)
  > L309: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-131** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/stale_table_kind_scan.py:376` — Narrative comment block (decorative separator)
  > L376: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-132** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/t9_order_sandbox.py:161` — Narrative comment block (decorative separator)
  > L161: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-133** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/t9_order_sandbox.py:186` — Narrative comment block (decorative separator)
  > L186: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-134** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/t9_order_sandbox.py:340` — Narrative comment block (decorative separator)
  > L340: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-135** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/t9_order_sandbox.py:417` — Narrative comment block (decorative separator)
  > L417: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-136** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/t9_order_sandbox.py:580` — Narrative comment block (decorative separator)
  > L580: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-137** `ai-slop/narrative-comment` — `code/01_platform/04_scripts/t9_order_sandbox.py:1091` — Narrative comment block (decorative separator)
  > L1091: `# ---------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-138** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java:273` — Narrative comment block (decorative separator)
  > L273: `// --- v4: full-mode field capture, DDL order (indexes 21-71), all ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-139** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:406` — Narrative comment block (decorative separator)
  > L406: `// ---- main entry point ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-140** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:577` — Narrative comment block (decorative separator)
  > L577: `// ---- bridge subprocess loop ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-141** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:603` — Narrative comment block (JSDoc preamble with slop signal)
  > L603: `/**`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-142** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:1757` — Narrative comment block (decorative separator)
  > L1757: `// ---- Slot-scoped safety propagation (plan Amendment §Slot-scoped safety) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-143** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:1760` — Narrative comment block (decorative separator)
  > L1760: `// ---- Discontinuity evidence (plan §DiscontinuityWriter) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-144** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2046` — Narrative comment block (decorative separator)
  > L2046: `// ---- ING-FAIL-007: clock-jump monitoring ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-145** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2066` — Narrative comment block (decorative separator)
  > L2066: `// ---- SIGNAL-warn-jvm-heap-high: sustained container-memory readiness gate ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-146** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2159` — Narrative comment block (decorative separator)
  > L2159: `// ---- shutdown ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-147** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2321` — Narrative comment block (decorative separator)
  > L2321: `// ---- health accessor ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-148** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2410` — Narrative comment block (decorative separator)
  > L2410: `// ---- helpers ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-149** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TypedFlussRowConverter.java:74` — Narrative comment block (decorative separator)
  > L74: `// --- v4 (indexes 21-71): all BIGINT NULL. Boxed Long so "absent" is`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-150** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:30` — Narrative comment block (decorative separator)
  > L30: `// ---- Constants (matching dossier) -- T2 tunable backpressure (G2 Ingest) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-151** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:41` — Narrative comment block (decorative separator)
  > L41: `// ---- Validated values (populated by validate()) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-152** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:101` — Narrative comment block (decorative separator)
  > L101: `// ---- HFT connection policy (plan §IngestionConfig — exact values) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-153** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:165` — Narrative comment block (decorative separator)
  > L165: `// ---- Validation ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-154** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:197` — Narrative comment block (decorative separator)
  > L197: `// ---- Arrow auth (TOTP only — ARROW_TOKEN removed 2026-08-24) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [x] **AS-155** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:216` — Narrative comment block (decorative separator)
  > L216: `// ---- Arrow feed (HFT only — the Standard feed was removed 2026-08-14) ----`
  - evidence: stale 'Standard feed was removed 2026-08-14' — reinstated selectable 2026-09-24 (DEC-039); corrected in CHG-536 (`1be7ec11`); cleared by rescan
- [-] **AS-156** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:220` — Narrative comment block (decorative separator)
  > L220: `// ---- Fluss ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-157** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:263` — Narrative comment block (decorative separator)
  > L263: `// ---- Backpressure -- T2 tunable (G2 Ingest) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-158** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:279` — Narrative comment block (decorative separator)
  > L279: `// ---- Timing ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-159** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:305` — Narrative comment block (decorative separator)
  > L305: `// ---- HFT connection policy (plan §IngestionConfig — exact values) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-160** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:346` — Narrative comment block (decorative separator)
  > L346: `// ---- Fingerprint & SDK version ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check) · full audit 2026-10-03: stale claim XC-39 corrected in CHG-539 (`d23926d4`) (go-arrow v0.2.0 across code/docs/pin); style remains flagged.
- [-] **AS-161** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:352` — Narrative comment block (decorative separator)
  > L352: `// ---- DDL & clock strictness ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-162** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:357` — Narrative comment block (decorative separator)
  > L357: `// ---- Standard derived values ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-163** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:362` — Narrative comment block (decorative separator)
  > L362: `// ---- Fail if any errors ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-164** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:424` — Narrative comment block (decorative separator)
  > L424: `// ---- env helpers ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-165** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:596` — Narrative comment block (decorative separator)
  > L596: `// ---- T2 alias helpers — primary + PENDING_MAX_* alias ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-166** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:689` — Narrative comment block (decorative separator)
  > L689: `// ---- Builder ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-167** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/SequenceGapMonitor.java:7` — Narrative comment block (JSDoc preamble with slop signal)
  > L7: `/**`
  - evidence: 'NDJSON + proto paths may alternate' stale (NDJSON removed 2026-08-29); corrected in CHG-536 (`1be7ec11`); still style-flagged by design
- [-] **AS-168** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/fingerprint/FingerprintBuilder.java:115` — Narrative comment block (decorative separator)
  > L115: `// ---- internal helpers ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-169** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:75` — Narrative comment block (decorative separator)
  > L75: `// ---- liveness ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-170** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:79` — Narrative comment block (cross-reference commentary)
  > L79: `/** Called from a shutdown hook — marks the process as not-alive. */`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-171** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:82` — Narrative comment block (decorative separator)
  > L82: `// ---- readiness setters (called by IngestionService) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-172** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:247` — Narrative comment block (decorative separator)
  > L247: `// ---- readiness ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-173** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:276` — Narrative comment block (decorative separator)
  > L276: `// ---- diagnostics ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-174** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/NtpClockChecker.java:178` — Narrative comment block (decorative separator)
  > L178: `// ---- NTP query (RFC 5905 SNTP client) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-175** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/NtpClockChecker.java:288` — Narrative comment block (decorative separator)
  > L288: `// ---- Exception ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-176** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:12` — Narrative comment block (decorative separator)
  > L12: `// --- provenance ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-177** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:17` — Narrative comment block (decorative separator)
  > L17: `// --- routing ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-178** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:22` — Narrative comment block (decorative separator)
  > L22: `// --- event time ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-179** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:27` — Narrative comment block (decorative separator)
  > L27: `// --- trade data (verified/normalized; prices in paise) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-180** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:39` — Narrative comment block (decorative separator)
  > L39: `// --- v4 full-mode fields (raw_table_1 indexes 21-71) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-181** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:76` — Narrative comment block (decorative separator)
  > L76: `// --- fingerprint ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-182** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:80` — Narrative comment block (decorative separator)
  > L80: `// --- connection identity ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-183** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:85` — Narrative comment block (decorative separator)
  > L85: `// --- schema ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-184** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:248` — Narrative comment block (decorative separator)
  > L248: `// --- accessors ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-185** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:103` — Narrative comment block (decorative separator)
  > L103: `// ---- Counters ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-186** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:118` — Narrative comment block (decorative separator)
  > L118: `// ---- Histogram (approximate via linear buckets) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check) · full audit 2026-10-03: stale claim XC-40 corrected in CHG-539 (`d23926d4`) (ring+sort); style remains flagged.
- [-] **AS-187** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:156` — Narrative comment block (decorative separator)
  > L156: `// ---- Gauges ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-188** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:165` — Narrative comment block (decorative separator)
  > L165: `// ---- Reason counters ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-189** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:168` — Narrative comment block (decorative separator)
  > L168: `// ---- Slot metrics (plan §Monitoring — labeled by slot only) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-190** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:188` — Narrative comment block (decorative separator)
  > L188: `// ---- Resource metrics (plan Amendment §Resource) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-191** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:290` — Narrative comment block (decorative separator)
  > L290: `// ---- Recording API ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-192** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:369` — Narrative comment block (decorative separator)
  > L369: `// ---- Slot + resource recording (plan §Monitoring / Amendment §Resource) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-193** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:437` — Narrative comment block (decorative separator)
  > L437: `// ---- package-private test accessors ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-194** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:443` — Narrative comment block (decorative separator)
  > L443: `// ---- Flush ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-195** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:514` — Narrative comment block (decorative separator)
  > L514: `// ---- JSON builder (minimal OTLP metrics format) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-196** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:571` — Narrative comment block (decorative separator)
  > L571: `// ---- Slot gauges (labeled by slot only) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-197** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:596` — Narrative comment block (decorative separator)
  > L596: `// ---- Resource + capacity gauges (Amendment §Resource) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-198** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java:69` — Narrative comment block (decorative separator)
  > L69: `// ---- listener ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-199** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java:83` — Narrative comment block (decorative separator)
  > L83: `// ---- accept gate ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-200** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java:163` — Narrative comment block (decorative separator)
  > L163: `// ---- P1-262: fire outside the accept-gate lock ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-201** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java:220` — Narrative comment block (decorative separator)
  > L220: `// ---- health ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-202** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java:237` — Narrative comment block (decorative separator)
  > L237: `// ---- Success ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-203** `ai-slop/narrative-comment` — `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java:522` — Narrative comment block (decorative separator)
  > L522: `// ---- outcome type ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-204** `ai-slop/narrative-comment` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/CandleAccumulator.java:51` — Narrative comment block (cross-reference commentary)
  > L51: `/**`
  - evidence: 'NOT written to any output row' stale (emitted as CandleLiveColumns.INGEST_TS, CHG-491); corrected in CHG-536 (`1be7ec11`); still style-flagged by design
- [-] **AS-205** `ai-slop/narrative-comment` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/CandleFetcher.java:26` — Narrative comment block (cross-reference commentary)
  > L26: `/** Closes the underlying client. Called from operator close (mailbox thread). */`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-206** `ai-slop/narrative-comment` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/N7RangeBreakoutStrategy.java:15` — Narrative comment block (JSDoc preamble with slop signal)
  > L15: `/**`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-207** `ai-slop/narrative-comment` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/RawTableColumns.java:48` — Narrative comment block (decorative separator)
  > L48: `// --- v4: full-mode field capture (indexes 21-71, all BIGINT NULL) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-208** `ai-slop/narrative-comment` — `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SourceIdleWatchdogGenerator.java:145` — Narrative comment block (cross-reference commentary)
  > L145: `/**`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-209** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/bridge/transport.rs:39` — Narrative comment block (decorative separator)
  > L39: `// --- RFC 6455 opcodes (subset we speak: text, ping, pong, close). ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-210** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/bridge/transport.rs:289` — Narrative comment block (decorative separator)
  > L289: `// --- WebSocket report intake (minimal RFC 6455 client) ------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-211** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/engine.rs:847` — Narrative comment block (decorative separator)
  > L847: `// ---------- P3-439: the boot gate is observed from the client, not a constant ----------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-212** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/engine.rs:892` — Narrative comment block (decorative separator)
  > L892: `// ---------- P3-194 / P3-440: reconcile transport failures and the mass snapshot ----------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-213** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/executiongate.rs:1096` — Narrative comment block (decorative separator)
  > L1096: `// --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-214** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/executiongate.rs:1203` — Narrative comment block (decorative separator)
  > L1203: `// --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-215** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/executiongate.rs:1297` — Narrative comment block (decorative separator)
  > L1297: `// --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-216** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/executiongate.rs:1336` — Narrative comment block (decorative separator)
  > L1336: `// --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-217** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/executiongate.rs:1394` — Narrative comment block (decorative separator)
  > L1394: `// --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-218** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/executiongate.rs:1430` — Narrative comment block (decorative separator)
  > L1430: `// --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-219** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/executiongate.rs:1466` — Narrative comment block (decorative separator)
  > L1466: `// --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-220** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/executiongate.rs:1503` — Narrative comment block (decorative separator)
  > L1503: `// --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-221** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/executiongate.rs:1540` — Narrative comment block (decorative separator)
  > L1540: `// --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-222** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/gateway_protocol.rs:571` — Narrative comment block (decorative separator)
  > L571: `// -------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-223** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/http.rs:2215` — Narrative comment block (decorative separator)
  > L2215: `// ---- DEC-044 approval / safety-halt surface (A2.2/T9) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-224** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/projection/mod.rs:1134` — Narrative comment block (decorative separator)
  > L1134: `// --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-225** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/projection/mod.rs:1330` — Narrative comment block (decorative separator)
  > L1330: `// --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-226** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/projection/mod.rs:1377` — Narrative comment block (decorative separator)
  > L1377: `// --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-227** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/projection/mod.rs:1393` — Narrative comment block (decorative separator)
  > L1393: `// --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-228** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/projection/mod.rs:1436` — Narrative comment block (decorative separator)
  > L1436: `// --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-229** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/projection/mod.rs:1471` — Narrative comment block (decorative separator)
  > L1471: `// --------------------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-230** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/projection/mod.rs:1510` — Narrative comment block (decorative separator)
  > L1510: `// --- P3-166: re-entry mints a new cycle, but ONLY for a genuinely newer event ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-231** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/projection/mod.rs:1573` — Narrative comment block (decorative separator)
  > L1573: `// --- P3-390 / P3-392 / P3-394: numeric boundaries, Java-parity ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-232** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/projection/mod.rs:1575` — Narrative comment block (decorative separator)
  > L1575: `// --- P3-213: accumulation overflow fails closed (the Rust mirror of P3-394) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-233** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/resilience.rs:396` — Narrative comment block (decorative separator)
  > L396: `// --- RESILIENCE-002: exponential backoff correctness ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-234** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/resilience.rs:408` — Narrative comment block (decorative separator)
  > L408: `// --- RESILIENCE-003: retry budget exhaustion ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-235** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/resilience.rs:420` — Narrative comment block (decorative separator)
  > L420: `// --- RESILIENCE-004: circuit breaker opens after threshold ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-236** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/resilience.rs:433` — Narrative comment block (decorative separator)
  > L433: `// --- RESILIENCE-005: recovery after dependency returns (half-open probe closes) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-237** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/resilience.rs:456` — Narrative comment block (decorative separator)
  > L456: `// --- RESILIENCE-007: duplicate retry prevention (never re-invoke a done key) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-238** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/resilience.rs:486` — Narrative comment block (decorative separator)
  > L486: `// --- RESILIENCE-003 (orchestrator): transient failures retried until success ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-239** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/resilience.rs:512` — Narrative comment block (decorative separator)
  > L512: `// --- RESILIENCE-001/003: storm containment — bounded attempts, then exhausted ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-240** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/resilience.rs:535` — Narrative comment block (decorative separator)
  > L535: `// --- RESILIENCE-004 (orchestrator): breaker short-circuits further calls ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-241** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/resilience.rs:564` — Narrative comment block (decorative separator)
  > L564: `// --- async live-path retry: transient err -> retry until success, bounded ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-242** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/resilience.rs:598` — Narrative comment block (decorative separator)
  > L598: `// --- async live-path retry: budget exhaustion surfaces, never retries forever ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-243** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/resilience.rs:619` — Narrative comment block (decorative separator)
  > L619: `// --- P3-022: the budget bounds one call, not the orchestrator's lifetime ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-244** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/resilience.rs:644` — Narrative comment block (decorative separator)
  > L644: `// --- P3-022: a breaker short-circuit must not consume budget ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-245** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/resilience.rs:663` — Narrative comment block (decorative separator)
  > L663: `// --- P3-023: the backoff delay is actually applied between attempts ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-246** `ai-slop/narrative-comment` — `code/02_services/04_executor/src/resilience.rs:689` — Narrative comment block (decorative separator)
  > L689: `// --- P3-023: ...on the LIVE async bridge path too ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-247** `ai-slop/narrative-comment` — `code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java:378` — Narrative comment block (decorative separator)
  > L378: `// --- Main entry point ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-248** `ai-slop/narrative-comment` — `code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/SyntheticWorkload.java:9` — Narrative comment block (cross-reference commentary)
  > L9: `/** Deterministic, per-instrument variable-arrival workload used by benchmarks. */`
  - evidence: 'used by benchmarks' stale (mock arrow server + unit tests; bench uses faketool); corrected in CHG-536 (`1be7ec11`); still style-flagged by design
- [-] **AS-249** `ai-slop/narrative-comment` — `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:52` — Narrative comment block (decorative separator)
  > L52: `// --- 1) Direct key checks (attemptId as key) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-250** `ai-slop/narrative-comment` — `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:90` — Narrative comment block (decorative separator)
  > L90: `// --- 2) Direct key checks (brokerOrderId as key -> attemptId) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-251** `ai-slop/narrative-comment` — `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:105` — Narrative comment block (decorative separator)
  > L105: `// --- 3) Direct key checks (clientRef as key -> attemptId) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-252** `ai-slop/narrative-comment` — `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:123` — Narrative comment block (decorative separator)
  > L123: `// --- 4) Value scans: brokerOrderId as value (attempt -> broker) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-253** `ai-slop/narrative-comment` — `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:138` — Narrative comment block (decorative separator)
  > L138: `// --- 5) Value scans: attemptId as value (broker -> attempt) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-254** `ai-slop/narrative-comment` — `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:152` — Narrative comment block (decorative separator)
  > L152: `// --- 6) Value scans: clientRef as value ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-255** `ai-slop/narrative-comment` — `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/SafetyHaltTailProcessor.java:10` — Narrative comment block (JSDoc preamble with slop signal)
  > L10: `/**`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-256** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/config/ContainerMemoryGuard.java:45` — Narrative comment block (decorative separator)
  > L45: `// ---- env-tunable percentages (P3, 2026-08-29) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-257** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:26` — Narrative comment block (decorative separator)
  > L26: `// ---- ingestion / workload profile ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-258** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:40` — Narrative comment block (decorative separator)
  > L40: `// ---- raw_table_1 schema contract ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-259** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:54` — Narrative comment block (decorative separator)
  > L54: `// ---- dedup / candles (reject-startup values) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-260** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:68` — Narrative comment block (decorative separator)
  > L68: `// ---- checkpointing ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-261** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:73` — Narrative comment block (decorative separator)
  > L73: `// ---- fixed-delay restart strategy (CHECKPOINT_RESTART_STRATEGY) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-262** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:87` — Narrative comment block (decorative separator)
  > L87: `// ---- sink write-path (tracker 14 box 682/116, 2026-08-12; CHG-023 item 4, 2026-08-17) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-263** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:113` — Narrative comment block (decorative separator)
  > L113: `// ---- JVM / container memory ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-264** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/model/AttemptPhase.java:45` — Narrative comment block (JSDoc preamble with slop signal)
  > L45: `/**`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-265** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/model/GateTransitionValidator.java:233` — Narrative comment block (decorative separator)
  > L233: `// ---- result types ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-266** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/observability/Json.java:44` — Narrative comment block (decorative separator)
  > L44: `// ---- object members: every member carries a key ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-267** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/observability/Json.java:75` — Narrative comment block (decorative separator)
  > L75: `// ---- array elements: no key ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-268** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/schema/RawTableSchema.java:98` — Narrative comment block (decorative separator)
  > L98: `// --- v4: full-mode field capture (appended AFTER schema_version; 0-20 never move) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-269** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/schema/RawTableSchema.java:187` — Narrative comment block (decorative separator)
  > L187: `// --- v4: every added column is BIGINT NULL ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [x] **AS-270** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/schema/eod/EncryptedExportEodOffloadExecutor.java:17` — Narrative comment block (JSDoc preamble with slop signal)
  > L17: `/**`
  - evidence: stale 'replaces … when a master key is configured' — no encrypted --offload branch exists; annotated in CHG-536 (`1be7ec11`); cleared by rescan
- [-] **AS-271** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/schema/execution/GateRow.java:39` — Narrative comment block (JSDoc preamble with slop signal)
  > L39: `/**`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-272** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/schema/position/PositionProjector.java:37` — Narrative comment block (cross-reference commentary)
  > L37: `/** Applied — the returned snapshot replaces the current one. */`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-273** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/schema/projection/OrderLifecycleProjector.java:79` — Narrative comment block (decorative separator)
  > L79: `// --- Quantity sanity (impossible quantity) ---------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-274** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/schema/projection/OrderLifecycleProjector.java:124` — Narrative comment block (decorative separator)
  > L124: `// --- Source-version gate (SCH-09 KvStateUpdateProtocol semantics) ----`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-275** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/schema/projection/OrderLifecycleProjector.java:141` — Narrative comment block (decorative separator)
  > L141: `// --- Terminal regression check ---------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-276** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/schema/projection/PostbackFingerprint.java:63` — Narrative comment block (cross-reference commentary)
  > L63: `/** Field-level canonical part builder (used by tests to mint valid fixtures). */`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check) · full audit 2026-10-03: stale claim XC-41 corrected in CHG-539 (`d23926d4`) (production use); style remains flagged.
- [-] **AS-277** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:83` — Narrative comment block (decorative separator)
  > L83: `// --- Fingerprint integrity ------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-278** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:89` — Narrative comment block (decorative separator)
  > L89: `// --- Idempotent ledger resume / duplicate ----------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-279** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:105` — Narrative comment block (decorative separator)
  > L105: `// --- Correlation precedence -----------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-280** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:114` — Narrative comment block (decorative separator)
  > L114: `// --- Audit (immutable evidence) -------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-281** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:122` — Narrative comment block (decorative separator)
  > L122: `// --- Lifecycle monotonicity -----------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-282** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:142` — Narrative comment block (decorative separator)
  > L142: `// --- Position (serialize Nautilus-computed result, no arithmetic) ---`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [-] **AS-283** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:192` — Narrative comment block (decorative separator)
  > L192: `// --- helpers -------------------------------------------------------------`
  - evidence: deliberate comment convention (AGENTS.md mandates post-verification inline comments; separators/section labels are the house style in 132 files) — policy mismatch, not a defect; claim-checked 2026-10-03 (G13 cross-check)
- [x] **AS-284** `ai-slop/narrative-comment` — `code/common/src/main/java/com/trading/common/version/VersionGate.java:55` — Narrative comment block (cross-reference commentary)
  > L55: `/** Convenience: fail unless every matrix entry is pinned (used by CI before building images). */`
  - evidence: stale 'used by CI before building images' — tests only; corrected in CHG-536 (`1be7ec11`); cleared by rescan

---

## Appendix A — Per-file index (for batching same-file edits)

| File | Findings | Groups | IDs |
|---|---:|---|---|
| `code/01_platform/04_scripts/holistic-analyze.py` | 54 | G2/G3/G4/G7/G10/G11/G12/G13 | AS-016–AS-037, AS-051, AS-111–AS-119, AS-295, AS-320–AS-325, AS-330–AS-337, AS-362–AS-364, AS-371, AS-390, AS-438, AS-516 |
| `code/01_platform/04_scripts/docs_audit.py` | 21 | G11/G12/G13 | AS-093–AS-110, AS-383, AS-430, AS-504 |
| `code/01_platform/04_scripts/day_run.py` | 19 | G2/G4/G5/G11/G12/G13 | AS-083–AS-090, AS-289–AS-294, AS-307, AS-360, AS-379, AS-426, AS-498 |
| `code/01_platform/04_scripts/t9_order_sandbox.py` | 19 | G1/G4/G7/G10/G11/G12/G13 | AS-001–AS-002, AS-005, AS-132–AS-137, AS-304–AS-305, AS-328, AS-398, AS-443–AS-445, AS-483–AS-484, AS-543 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java` | 19 | G10/G11/G13 | AS-055–AS-057, AS-139–AS-148, AS-373, AS-404, AS-451–AS-454 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java` | 19 | G11/G13 | AS-150–AS-166, AS-405, AS-456 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java` | 15 | G11/G13 | AS-185–AS-197, AS-406, AS-458 |
| `code/02_services/04_executor/src/resilience.rs` | 14 | G13 | AS-233–AS-246 |
| `code/02_services/04_executor/src/projection/mod.rs` | 11 | G11/G13 | AS-060, AS-224–AS-232, AS-417 |
| `code/01_platform/04_scripts/o2-provision.py` | 10 | G2/G4/G11/G12/G13 | AS-120–AS-124, AS-298, AS-348, AS-392, AS-439, AS-523 |
| `code/01_platform/04_scripts/stale_table_kind_scan.py` | 10 | G2/G10/G11/G12/G13 | AS-129–AS-131, AS-358, AS-397, AS-441, AS-480–AS-482, AS-540 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java` | 10 | G11/G13 | AS-176–AS-184, AS-457 |
| `code/02_services/04_executor/src/executiongate.rs` | 10 | G11/G13 | AS-213–AS-221, AS-415 |
| `code/01_platform/04_scripts/alert-routing-selftest.py` | 9 | G2/G11/G12/G13 | AS-074–AS-077, AS-355–AS-356, AS-359, AS-424, AS-489 |
| `code/01_platform/04_scripts/audit_r2.py` | 9 | G4/G11/G12/G13 | AS-078–AS-082, AS-286–AS-287, AS-377, AS-490 |
| `code/01_platform/04_scripts/r2_legal_hold_check.py` | 8 | G5/G11/G12/G13 | AS-125–AS-128, AS-309–AS-310, AS-395, AS-533 |
| `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java` | 8 | G10/G11/G13 | AS-066–AS-069, AS-376, AS-421, AS-472–AS-473 |
| `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java` | 8 | G11/G13 | AS-277–AS-283, AS-478 |
| `code/01_platform/04_scripts/disaster_drills.py` | 7 | G3/G11/G12/G13 | AS-011–AS-012, AS-091–AS-092, AS-382, AS-429, AS-503 |
| `code/02_services/04_executor/src/engine.rs` | 7 | G1/G8/G13 | AS-003, AS-046, AS-048–AS-050, AS-211–AS-212 |
| `code/common/src/main/java/com/trading/common/config/PlatformConfig.java` | 7 | G13 | AS-257–AS-263 |
| `code/01_platform/04_scripts/ddl_apply.py` | 6 | G3/G11/G12 | AS-008–AS-010, AS-380, AS-427, AS-499 |
| `code/01_platform/04_scripts/eod_schedule.py` | 6 | G3/G11/G12 | AS-013–AS-015, AS-384, AS-431, AS-507 |
| `code/01_platform/04_scripts/perf_evidence_parse.py` | 6 | G3/G4/G11/G12 | AS-040, AS-299–AS-301, AS-393, AS-525 |
| `code/01_platform/05_instruments/split_manifest.py` | 6 | G2/G3/G12 | AS-043, AS-327, AS-344, AS-346–AS-347, AS-547 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java` | 6 | G10/G13 | AS-169–AS-173, AS-487 |
| `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java` | 6 | G13 | AS-249–AS-254 |
| `code/01_platform/04_scripts/cluster_check.py` | 5 | G3/G4/G11/G12 | AS-006, AS-288, AS-378, AS-425, AS-495 |
| `code/01_platform/04_scripts/local_int_004_smoke.py` | 5 | G2/G4/G12 | AS-296–AS-297, AS-343, AS-345, AS-522 |
| `code/01_platform/06_stage_profiler/stage_profiler.py` | 5 | G4/G5/G11/G12 | AS-306, AS-313, AS-400, AS-446, AS-548 |
| `code/02_services/01_ingestion/go-bridge/main.go` | 5 | G10/G11 | AS-401, AS-449–AS-450, AS-485–AS-486 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java` | 5 | G11/G13 | AS-198–AS-201, AS-459 |
| `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java` | 5 | G10/G11 | AS-374, AS-408, AS-460–AS-462 |
| `code/01_platform/04_scripts/cp_phase_capture.py` | 4 | G3/G10/G12 | AS-007, AS-285, AS-365, AS-497 |
| `code/01_platform/04_scripts/ddl_apply_smoke.py` | 4 | G10/G11/G12 | AS-366, AS-381, AS-428, AS-500 |
| `code/01_platform/04_scripts/image_staleness_check.py` | 4 | G2/G3/G11/G12 | AS-038, AS-354, AS-391, AS-517 |
| `code/01_platform/04_scripts/stage_capture_parse.py` | 4 | G5/G11/G12 | AS-311–AS-312, AS-396, AS-538 |
| `code/01_platform/04_scripts/deploy_preflight.py` | 3 | G2/G7/G12 | AS-329, AS-353, AS-501 |
| `code/01_platform/04_scripts/env_facts.py` | 3 | G2/G10/G12 | AS-361, AS-479, AS-505 |
| `code/01_platform/04_scripts/fused_timeline.py` | 3 | G2/G11/G12 | AS-357, AS-436, AS-513 |
| `code/01_platform/04_scripts/gate_preflight.py` | 3 | G11/G12 | AS-389, AS-437, AS-515 |
| `code/01_platform/04_scripts/plan_tracker.py` | 3 | G2/G12 | AS-350–AS-351, AS-528 |
| `code/01_platform/04_scripts/soak-o2-evidence.py` | 3 | G3/G4/G12 | AS-042, AS-303, AS-537 |
| `code/01_platform/04_scripts/t8_sandbox_contract_check.py` | 3 | G3/G11/G12 | AS-326, AS-442, AS-542 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java` | 3 | G11/G13 | AS-202–AS-203, AS-407 |
| `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java` | 3 | G11 | AS-410, AS-463–AS-464 |
| `code/02_services/04_executor/src/bridge/transport.rs` | 3 | G11/G13 | AS-209–AS-210, AS-413 |
| `code/common/src/main/java/com/trading/common/schema/eod/EodControllerTool.java` | 3 | G3/G11 | AS-044, AS-422, AS-475 |
| `code/common/src/main/java/com/trading/common/schema/fluss/FlussWriteProfiles.java` | 3 | G9 | AS-339–AS-341 |
| `code/common/src/main/java/com/trading/common/schema/projection/OrderLifecycleProjector.java` | 3 | G13 | AS-273–AS-275 |
| `code/01_platform/01_docker/alert-consumer.py` | 2 | G2/G12 | AS-352, AS-488 |
| `code/01_platform/04_scripts/check_flink_properties.py` | 2 | G2/G12 | AS-349, AS-493 |
| `code/01_platform/04_scripts/fluss-probes/CandleVerify.java` | 2 | G10/G11 | AS-368, AS-432 |
| `code/01_platform/04_scripts/fluss-probes/FlussPrefixReader.java` | 2 | G10/G11 | AS-370, AS-434 |
| `code/01_platform/04_scripts/implementation_gate.py` | 2 | G5/G12 | AS-308, AS-518 |
| `code/01_platform/04_scripts/ing-tcp001/TokenCountReconcile.java` | 2 | G13 | AS-052–AS-053 |
| `code/01_platform/04_scripts/o2_ingest.py` | 2 | G3/G12 | AS-039, AS-524 |
| `code/01_platform/04_scripts/pernode_attribution_check.py` | 2 | G3/G12 | AS-041, AS-526 |
| `code/01_platform/04_scripts/prod_node_check.py` | 2 | G11/G12 | AS-394, AS-530 |
| `code/01_platform/04_scripts/seed_alerts.py` | 2 | G11/G12 | AS-440, AS-534 |
| `code/01_platform/04_scripts/seed_dashboards.py` | 2 | G4/G12 | AS-302, AS-535 |
| `code/01_platform/04_scripts/values_at_rest_scan.py` | 2 | G11/G12 | AS-399, AS-545 |
| `code/02_services/01_ingestion/go-bridge/faketool/main.go` | 2 | G10/G11 | AS-372, AS-448 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/NtpClockChecker.java` | 2 | G13 | AS-174–AS-175 |
| `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/N7RangeBreakoutStrategy.java` | 2 | G11/G13 | AS-206, AS-409 |
| `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java` | 2 | G11 | AS-411, AS-465 |
| `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/StrategyHostFunction.java` | 2 | G11 | AS-412, AS-466 |
| `code/02_services/04_executor/src/bin/t9_paper_25.rs` | 2 | G6 | AS-314–AS-315 |
| `code/02_services/04_executor/src/bin/t9_paper_25_full.rs` | 2 | G6 | AS-316–AS-317 |
| `code/02_services/04_executor/src/config.rs` | 2 | G8 | AS-045, AS-047 |
| `code/02_services/04_executor/src/durable_file.rs` | 2 | G6/G11 | AS-319, AS-468 |
| `code/02_services/04_executor/src/execution/client.rs` | 2 | G11/G13 | AS-059, AS-414 |
| `code/02_services/04_executor/src/http.rs` | 2 | G11/G13 | AS-223, AS-416 |
| `code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java` | 2 | G11/G13 | AS-247, AS-418 |
| `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/ExecutionGatewayMain.java` | 2 | G11/G13 | AS-063, AS-470 |
| `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/GatewayHttpServer.java` | 2 | G11 | AS-420, AS-471 |
| `code/common/src/main/java/com/trading/common/observability/Json.java` | 2 | G13 | AS-266–AS-267 |
| `code/common/src/main/java/com/trading/common/schema/RawTableSchema.java` | 2 | G13 | AS-268–AS-269 |
| `code/common/src/main/java/com/trading/common/schema/execution/GateRow.java` | 2 | G13 | AS-071, AS-271 |
| `code/common/src/main/java/com/trading/common/schema/position/PositionProjector.java` | 2 | G11/G13 | AS-272, AS-477 |
| `code/01_platform/04_scripts/catalog_drift.py` | 1 | G12 | AS-491 |
| `code/01_platform/04_scripts/change_control_check.py` | 1 | G12 | AS-492 |
| `code/01_platform/04_scripts/clean_break_drill.py` | 1 | G12 | AS-494 |
| `code/01_platform/04_scripts/compose_config_redact.py` | 1 | G12 | AS-496 |
| `code/01_platform/04_scripts/deployed_artifact_verify.py` | 1 | G12 | AS-502 |
| `code/01_platform/04_scripts/eod_controller.py` | 1 | G12 | AS-506 |
| `code/01_platform/04_scripts/evidence_ownership_check.py` | 1 | G12 | AS-508 |
| `code/01_platform/04_scripts/execution_network_check.py` | 1 | G12 | AS-509 |
| `code/01_platform/04_scripts/fluss-client-metrics.py` | 1 | G12 | AS-510 |
| `code/01_platform/04_scripts/fluss-probes/CandleFeaturesTableProbe.java` | 1 | G10 | AS-367 |
| `code/01_platform/04_scripts/fluss-probes/EventDayProbe.java` | 1 | G11 | AS-433 |
| `code/01_platform/04_scripts/fluss-probes/FeatureSpikeProbe.java` | 1 | G11 | AS-385 |
| `code/01_platform/04_scripts/fluss-probes/FlussKvScanStrategy.java` | 1 | G10 | AS-369 |
| `code/01_platform/04_scripts/fluss-probes/FlussReadabilityProbe.java` | 1 | G11 | AS-386 |
| `code/01_platform/04_scripts/fluss-probes/FlussSignalLatency.java` | 1 | G11 | AS-387 |
| `code/01_platform/04_scripts/fluss-probes/RawCompressionProbe.java` | 1 | G11 | AS-435 |
| `code/01_platform/04_scripts/fluss-probes/SignalCandidatesViewer.java` | 1 | G11 | AS-388 |
| `code/01_platform/04_scripts/fluss-repair/LogScan.py` | 1 | G12 | AS-511 |
| `code/01_platform/04_scripts/fluss-repair/verify-and-truncate.py` | 1 | G12 | AS-512 |
| `code/01_platform/04_scripts/gate_memo.py` | 1 | G12 | AS-514 |
| `code/01_platform/04_scripts/ing-tcp001/reconcile-compare.py` | 1 | G12 | AS-519 |
| `code/01_platform/04_scripts/jfr-analyze.py` | 1 | G12 | AS-520 |
| `code/01_platform/04_scripts/latency_probe.py` | 1 | G12 | AS-521 |
| `code/01_platform/04_scripts/placement_check.py` | 1 | G12 | AS-527 |
| `code/01_platform/04_scripts/pom-snapshot-scan.py` | 1 | G12 | AS-529 |
| `code/01_platform/04_scripts/r2_archive_selection.py` | 1 | G12 | AS-531 |
| `code/01_platform/04_scripts/r2_archive_sync.py` | 1 | G12 | AS-532 |
| `code/01_platform/04_scripts/skip_inventory.py` | 1 | G12 | AS-536 |
| `code/01_platform/04_scripts/stage_gc_summary.py` | 1 | G12 | AS-539 |
| `code/01_platform/04_scripts/strategy_live_board.py` | 1 | G12 | AS-541 |
| `code/01_platform/04_scripts/tablet-orphan-sweep.py` | 1 | G12 | AS-544 |
| `code/01_platform/04_scripts/version_matrix_verify.py` | 1 | G12 | AS-546 |
| `code/02_services/01_ingestion/go-bridge/cmd/gen-corpus/main.go` | 1 | G11 | AS-447 |
| `code/02_services/01_ingestion/go-bridge/marketdata/market_data.pb.go` | 1 | G11 | AS-402 |
| `code/02_services/01_ingestion/go-bridge/supervisor.go` | 1 | G13 | AS-054 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java` | 1 | G11 | AS-403 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java` | 1 | G13 | AS-138 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java` | 1 | G11 | AS-455 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TypedFlussRowConverter.java` | 1 | G13 | AS-149 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/SequenceGapMonitor.java` | 1 | G13 | AS-167 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/fingerprint/FingerprintBuilder.java` | 1 | G13 | AS-168 |
| `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/CandleAccumulator.java` | 1 | G13 | AS-204 |
| `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/CandleFetcher.java` | 1 | G13 | AS-205 |
| `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/RawTableColumns.java` | 1 | G13 | AS-207 |
| `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SourceIdleWatchdogGenerator.java` | 1 | G13 | AS-208 |
| `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SourceIdleWatchdogState.java` | 1 | G13 | AS-058 |
| `code/02_services/04_executor/src/bridge/protocol.rs` | 1 | G11 | AS-467 |
| `code/02_services/04_executor/src/durable.rs` | 1 | G6 | AS-318 |
| `code/02_services/04_executor/src/gateway_protocol.rs` | 1 | G13 | AS-222 |
| `code/02_services/04_executor/src/shutdown.rs` | 1 | G13 | AS-061 |
| `code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/SyntheticWorkload.java` | 1 | G13 | AS-248 |
| `code/02_services/06_execution_bridge/go-bridge/postback.go` | 1 | G11 | AS-469 |
| `code/02_services/06_execution_bridge/go-bridge/reauth.go` | 1 | G13 | AS-062 |
| `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/FlussControlStateStore.java` | 1 | G10 | AS-375 |
| `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/FlussProjectionWriter.java` | 1 | G11 | AS-419 |
| `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/IntentReader.java` | 1 | G13 | AS-064 |
| `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/SafetyHaltTailProcessor.java` | 1 | G13 | AS-255 |
| `code/common/src/main/java/com/trading/common/config/ConfigKeys.java` | 1 | G1 | AS-004 |
| `code/common/src/main/java/com/trading/common/config/ContainerMemoryGuard.java` | 1 | G13 | AS-256 |
| `code/common/src/main/java/com/trading/common/model/AttemptPhase.java` | 1 | G13 | AS-264 |
| `code/common/src/main/java/com/trading/common/model/GateTransitionValidator.java` | 1 | G13 | AS-265 |
| `code/common/src/main/java/com/trading/common/ownership/OwnershipMatrix.java` | 1 | G13 | AS-065 |
| `code/common/src/main/java/com/trading/common/schema/ddl/DdlText.java` | 1 | G11 | AS-474 |
| `code/common/src/main/java/com/trading/common/schema/eod/EncryptedExportEodOffloadExecutor.java` | 1 | G13 | AS-270 |
| `code/common/src/main/java/com/trading/common/schema/execution/ExecutionCommandGate.java` | 1 | G11 | AS-476 |
| `code/common/src/main/java/com/trading/common/schema/execution/FlussAttemptStore.java` | 1 | G13 | AS-070 |
| `code/common/src/main/java/com/trading/common/schema/execution/FlussGateStateStore.java` | 1 | G11 | AS-423 |
| `code/common/src/main/java/com/trading/common/schema/execution/InMemoryGateStateStore.java` | 1 | G13 | AS-072 |
| `code/common/src/main/java/com/trading/common/schema/fluss/BoundedRetry.java` | 1 | G9 | AS-338 |
| `code/common/src/main/java/com/trading/common/schema/fluss/WriteAwait.java` | 1 | G9 | AS-342 |
| `code/common/src/main/java/com/trading/common/schema/position/PositionProjectorDriver.java` | 1 | G13 | AS-073 |
| `code/common/src/main/java/com/trading/common/schema/projection/PostbackFingerprint.java` | 1 | G13 | AS-276 |
| `code/common/src/main/java/com/trading/common/version/VersionGate.java` | 1 | G13 | AS-284 |

## Appendix B — Upkeep cheatsheet

```bash
# overall counts per marker
for m in ' ' '~' 'x' 'f' '-' 'd'; do printf '[%s] %s\n' "$m" "$(grep -c "^- \[$m\]" aislop-remediation-tracker.md)"; done

# per-group counts (marker char is column 4: ' ', '~', 'x', 'f', '-', 'd')
awk '/^### G[0-9]+/{g=$2} /^- \[/{m=substr($0,4,1); c[g" "m]++} END{for(k in c) print k, c[k]}' \
  aislop-remediation-tracker.md | sort

# re-scan at a milestone (detect only, nothing changed)
AISLOP_NO_TELEMETRY=1 AISLOP_NO_HISTORY=1 npx -y aislop@0.17.0 scan . --json \
  --exclude "**/target/**,**/__pycache__/**,**/.ruff_cache/**,logs/**" > /tmp/opencode/aislop-rescan.json
```

## Cross-check discoveries (2026-10-03)

Full blind re-verification of G2–G10 (2026-10-03): 7 independent reviewers classified all 114
non-fixed items; 113/114 verdicts confirmed, one correction (AS-037). Five defects the scan
missed or misjudged were found and fixed: XC-1/XC-2 in `CHG-532`, XC-3/XC-4/XC-5 in `CHG-533`.
The G11 size sweep (2026-10-03, 6 independent reviewers, all 102 items) confirmed the size
verdicts and found five more defects (XC-6…XC-10), fixed in `CHG-534`.
The G11 size sweep (2026-10-03, 6 independent reviewers, all 102 items) confirmed the size
verdicts and found five more defects (XC-6…XC-10), fixed in `CHG-534`.

| ID | Marker | Site | Evidence |
|---|---|---|---|
| XC-1 | `[x]` | `code/02_services/04_executor/src/bin/t9_paper_25.rs` | evidence.json was written (fsync) before `assert_no_secrets` and the shadow-invariant asserts — a secret-shaped key could reach disk before the panic (contradicts P3-184, which the full variant implements). Fixed in `6531d77d` (CHG-532): `verify_and_write()` runs every invariant first and writes last; guard test `failed_invariant_never_persists_evidence` proves a failing check leaves no file. |
| XC-2 | `[x]` | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java` | overnight expiry marked complete prior-session pendings as emitted and incremented `compute.candles.emitted` without emitting a row (missed session-close timer / stalled watermark). Fixed in `6531d77d` (CHG-532): expired pendings are marked never-re-emit and counted as `late.dropped`; the emitted counter moves only in `closeAndEmit`; harness test `overnightExpiryCountsDropsNotEmissions` pins it. |
| XC-3 | `[x]` | `code/01_platform/04_scripts/cluster_check.py` | a failed/unparseable `node inspect` / `service inspect` was silently defaulted to `{}`/empty labels — a real replica shortfall was downgraded to a topology WARN and placement-spread/global-coverage/published-ports skipped the service. Fixed in `43da9ffb` (CHG-533): probes record failures and `check_probes_readable` FAILs the run. Guards: 3 tests in `test_11_cluster_check.py`. |
| XC-4 | `[x]` | `code/01_platform/04_scripts/fluss-probes/CandleVerify.java` | a misplaced brace made `sumDeltaTrade`/`tradeRows` accumulate every row (quotes included), skewing the parity input and killing the zero-trade diagnostic. Fixed in `43da9ffb` (CHG-533): TRADE-branch brace + symmetric OHLC diagnostics. Guard: source-level brace test in `test_candle_verify_runner.py`. |
| XC-5 | `[x]` | `code/01_platform/04_scripts/holistic-analyze.py` | a missing/empty `tm-prom-dedup-late.tsv` made G7a/G7b vacuous while the run printed "G7: data-quality guards passed (dedup exact, late-drop covered)"; a missing `tm-prom-invalid.tsv` printed a measured-looking F6 `total=0`. Fixed in `43da9ffb` (CHG-533): unsampled counters append UNAVAILABLE, G7 prints "counter-exactness NOT MEASURED", F6 prints NOT SAMPLED. Guard: `CounterLegNoteTests`. (Also the AS-037 correction.) |
| XC-6 | `[x]` | `code/01_platform/04_scripts/fused_timeline.py` | P6-401's `(g - t) <= 120` bound never expired: `last_val` was carried past the bound, so a metric that stopped scraping was still forward-filled to the grid's end — the exact bug the P6-401 comment claims to have fixed. Fixed in `f776ba65` (CHG-534): `forward_fill()` leaves the grid point empty when no sample is in-bound. Guard: `test_fused_timeline_forward_fill.py` (3 tests; 2 were red before the fix). |
| XC-7 | `[x]` | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java` | `runWithBridge`'s `catch (Exception)` broke the loop without `fatalStopReason`; `main()` exited 0, so `restart: on-failure:3` never fired and the documented crash contract was bypassed. Reachable through every P1-269/P1-270 fail-loud throw in `ProtoFrameReader.readLoop` (truncated header/body, bad length/payload, over-limit batch). Fixed in `f776ba65` (CHG-534): the catch writes DROP via `recordBridgeExit` and records BRIDGE_CRASH through the shared `markBridgeFatal`. Guard: `truncatedFrameIsFatalNotCleanExit` (red leg: `fatalStopReason == null`). |
| XC-8 | `[x]` | `code/01_platform/04_scripts/stage_capture_parse.py` | `b2_consumer_read_report` and `b2_closed_read_report` carried verbatim copies of the window/percentile logic (only TSV name/headers differ). Fixed in `f776ba65` (CHG-534): shared `_window_percentile_report`; delegation test pins both legs. |
| XC-9 | `[x]` | `code/01_platform/04_scripts/t9_order_sandbox.py` | `HostTransport` was dead (zero references; the T8 profile publishes no host port) and both Fluss probes duplicated the classpath loader + javac prologue. Fixed in `f776ba65` (CHG-534): class deleted; `_probe_classpath`/`_ensure_probe_class` shared. Guard: `test_g11_probe_dedupe_and_dead_host_transport_removed`. |
| XC-10 | `[x]` | `code/01_platform/04_scripts/holistic-analyze.py` | the retired preview/final-candle reads were replaced by `prev_rows = []` / `final_rows = []` but their parse loops (and `prev_re`) stayed — unreachable code computing values that could never exist. Fixed in `f776ba65` (CHG-534): loops + empty assignments deleted (the assignments themselves became ruff F841-dead), `unavailable` notes kept. Guard: `TestNoDeadRetiredTableLoops`. |
| XC-11 | `[x]` | `code/01_platform/04_scripts/fused_timeline.py` | the G11 XC-6 fix bounded the Prometheus fill but left `_fetch_rdb_signals` carrying its last value to the grid's end with no age bound at all — a RocksDB gauge that stopped scraping was still reported as steady. Fixed in CHG-535 (`e6f046ac`): the RocksDB path uses the same bounded `forward_fill(grid, by_t, "max")`. Guard: `test_rdb_forward_fill_uses_the_bounded_helper` (red before). |
| XC-12 | `[x]` | `code/01_platform/04_scripts/fluss-client-metrics.py` | `_at_or_before()` returned the newest sample with no age bound (its docstring cited fused_timeline as the model, which now expires). A series that stopped mid-window was carried to the grid's end. Fixed in CHG-535 (`e6f046ac`): `max_age_s=120`. Guard: `test_expires_a_stopped_series` (red before). |
| XC-13 | `[x]` | `code/01_platform/04_scripts/r2_legal_hold_check.py` | comma-separated `--audit-prefix` was bucket-lock-checked for all prefixes but retrieval/hash-chain read only `prefixes[0]` — `audit/,orders/` could report PASS while orders/ manifests were never listed, fetched, or chain-checked. Fixed in CHG-535 (`e6f046ac`): every prefix retrieved; `retrieval_verdict()` aggregates (any broken chain fails, unbound roots named); `build_evidence` requires all prefixes valid. Guard: `test_multi_prefix_retrieval_requires_every_prefix` (red before). |
| XC-14 | `[x]` | `code/01_platform/04_scripts/seed_alerts.py` | a 200 whose body was unparseable (or of an unexpected shape) silently became `existing = {}` — dry-run printed "create" for every alert and a real run re-POSTed existing alerts as duplicates (contradicts P6-778). Fixed in CHG-535 (`e6f046ac`): exit 3 with an "unknown catalog" message. Guard: `test_unreadable_catalog_body_exits_three` (red before). |
| XC-15 | `[x]` | `code/01_platform/04_scripts/disaster_drills.py` | `sections[]` was positionally mismatched against `EVIDENCE_HEADINGS`: every rendered drill file showed the documented expectation under "## Scenario" and the fault commands under "## Documented expectation". Fixed in CHG-535 (`e6f046ac`): `build_sections()` emits title/expectation/env/fault in heading order. Guard: `test_sections_map_onto_the_headings_in_order` (red before). |
| XC-16 | `[x]` | `code/01_platform/04_scripts/stale_table_kind_scan.py` | truth constants were stale at 489/285/419 while the foundation C6 truth read 771/564/695 — the count-drift gate reported current counts as stale and treated rows matching the old truth as current (the live `00-start-here.md` row went unreported). Fixed in CHG-535 (`e6f046ac`): constants updated; `TruthFreshnessTests` parses the foundation line and fails on drift; both stale doc rows fixed. `--upstream` back to exit 0 with 0 live/unannotated. |
| XC-17 | `[x]` | `code/01_platform/05_instruments/split_manifest.py` | `rows < sum(chunks)` only warned, wrote fewer slot files than requested, and exited 0 (a header-only input wrote slot1.csv and stopped); over-supply failed only after the loop. Fixed in CHG-535 (`e6f046ac`): both refused up front; the unreachable trailing check removed. Tests: `test_split_manifest.py` (3, red before). |
| XC-18 | `[x]` | `code/01_platform/04_scripts/deployed_artifact_verify.py` | the `docker cp` failure return skipped the `shutil.rmtree` the docstring promises ("always cleanup"), leaking a temp dir per failed copy. Fixed in CHG-535 (`e6f046ac`). Guard: `test_cp_failure_leaves_no_temp_dir` (red before). |
| XC-19 | `[x]` | `code/01_platform/04_scripts/holistic-analyze.py` | `collect_rows(..., with_offset=True)` made the reader print `"@<offset> <row>"` lines but the validation kept only `"("` rows — the mode could never return a row. Fixed in CHG-535 (`e6f046ac`); test drives it through a mocked subprocess (red before). No live caller used the mode. |
| XC-20 | `[x]` | `code/01_platform/04_scripts/ing-tcp001/reconcile-compare.py` | `vanished` included the Fluss `-1` sentinel although `extra`/`raw_nonzero` already exclude it (P6-625) and its presence is mode-dependent — losing it between pre and post produced a false FAIL. Fixed in CHG-535 (`e6f046ac`). Guard: `test_fluss_minus_one_sentinel_vanishing_is_tolerated` (red before). |
| XC-21 | `[x]` | `code/01_platform/04_scripts/day_run.py` | `COMPANION_JOB_NAMES`, `EXIT_USAGE`, `EXIT_BUSY` had zero references anywhere. Removed in CHG-535 (`e6f046ac`); guard in `test_day_run.py` (red before). |
| XC-22 | `[x]` | `code/01_platform/04_scripts/tablet-orphan-sweep.py` | `zk_ls()` repeated the `returncode != 0` raise after the "Node does not exist" return — unreachable (`CompletedProcess.returncode` cannot change). Removed in CHG-535 (`e6f046ac`); source guard (red before). |
| XC-23 | `[x]` | `code/01_platform/04_scripts/version_matrix_verify.py` | `LATEST`/`LATEST_FORWARD` in `PIN_BLOCKERS` were unreachable (the `"latest" in version.lower()` branch always matches first); the verdict still failed, only the message differed. Removed in CHG-535 (`e6f046ac`); guard test (red before). |
| XC-24 | `[x]` | `code/01_platform/04_scripts/ing-tcp001/TokenCountReconcile.java` | the quarantine census read a LOG table with `createBatchScanner` and no limit — the pinned client rejects it ("BatchScanner over a Log Table requires limit to be set", verified in the 1.0.0 jar) and a limited scan reads only a bucket segment, so the count threw every run. Fixed in CHG-536 (`1be7ec11`): both tables go through the offset-paged `scanLog`; `scanBatch` deleted; 3 source guards in the gate shell test (red pre-fix). AS-053 cleared by rescan. |
| XC-25 | `[x]` | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java` | "Standard feed was removed 2026-08-14" stale — reinstated as a selectable channel 2026-09-24 (`ARROW_FEED=token|hft`, DEC-039). Corrected in CHG-536 (`1be7ec11`); AS-155 cleared by rescan. |
| XC-26 | `[x]` | `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/SequenceGapMonitor.java` | "NDJSON + proto paths may alternate" stale — the NDJSON pipe was removed 2026-08-29 (proto only). Corrected in CHG-536 (`1be7ec11`); AS-167 remains style-flagged by design. |
| XC-27 | `[x]` | `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/CandleAccumulator.java` | "NOT written to any output row, NOT part of any table schema" stale — emitted on the live row as `CandleLiveColumns.INGEST_TS` (CHG-491). Corrected in CHG-536 (`1be7ec11`); AS-204 remains style-flagged by design. |
| XC-28 | `[x]` | `code/common/src/main/java/com/trading/common/schema/eod/EncryptedExportEodOffloadExecutor.java` | "replaces … when a master key is configured" aspirational — `EodControllerTool --offload` is none\|mock\|lake, no encrypted branch. Annotated in CHG-536 (`1be7ec11`); AS-270 cleared by rescan. |
| XC-29 | `[x]` | `code/common/src/main/java/com/trading/common/version/VersionGate.java` | "used by CI before building images" false — only `VersionGateTest` calls `requireAllPinned`. Corrected in CHG-536 (`1be7ec11`); AS-284 cleared by rescan. |
| XC-30 | `[x]` | `code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/SyntheticWorkload.java` | "used by benchmarks" false — `bench-throughput.sh` uses faketool; the class serves `MockArrowServer` + unit tests. Corrected in CHG-536 (`1be7ec11`); AS-248 remains style-flagged by design. |
| XC-31 | `[x]` | `code/01_platform/04_scripts/holistic-analyze.py` | the AS-035 won't-fix rationale justified only a MISSING faketool.log (injection-less parity-only), but the broad `except OSError: pass` swallowed every read error — a permission/I-O failure silently turned G7a/G7b off for a run that may have injected. Found by the 2026-10-03 blind audit (47 items re-judged, 46 confirmed, this one disagreed). Fixed in CHG-537 (`9321b489`): `read_inject_counts()` carries the error; other OSErrors surface as UNAVAILABLE; 4 tests red pre-fix. |
| XC-32 | `[x]` | `code/02_services/01_ingestion/go-bridge/main.go` | read-loop auth refresh off-by-one: after `authTries++` the classifier received the post-increment budget, so the final allowed refresh was classified exhausted even when it succeeded — the slot was permanently stopped with a false `authentication_refresh_exhausted` (the dial path retried on the same success). Found by the 2026-10-03 full audit. Fixed in CHG-538 (`81dbd763`): `readLoopAuthRefresh()` records whether a refresh actually ran; 3 call-site tests, 2 red legs. AS-450's finding remains accepted complexity. |
| XC-33 | `[x]` | `code/01_platform/04_scripts/holistic-analyze.py` | dead burst machinery: CHG-534 removed the only `buckets_1s` producer, leaving `burst_secs` permanently empty, the G6b stall guard unable to fire (comment claimed "fully armed"), every burst correlation inert, and the whole checkpoint duration/B5 analysis nested inside `if burst_secs:` so it never ran. Found by the 2026-10-03 full audit. Fixed in CHG-538 (`81dbd763`): burst legs retired with an UNAVAILABLE entry, checkpoint analysis un-nested, G6b block removed. AS-371's finding remains accepted. |
| XC-34 | `[x]` | `code/01_platform/04_scripts/holistic-analyze.py` | corrupt `checkpoints.jsonl` lines were dropped by `except ValueError: pass`, silently under-counting the correlation input. Fixed in CHG-538 (`81dbd763`): `read_checkpoint_windows()` counts unparseable lines and surfaces them; AS-025 cleared by rescan. |
| XC-35 | `[x]` | `code/01_platform/04_scripts/holistic-analyze.py` | a missing/unreadable `checkpoints.jsonl` read as `cps=[]` and printed "slow checkpoints (>5s): 0" — an unmeasured leg as a clean zero. Fixed in CHG-538 (`81dbd763`): the read error surfaces as UNAVAILABLE; AS-026 cleared by rescan. |
| XC-36 | `[x]` | `code/01_platform/04_scripts/audit_r2.py` | header pointer to `01-foundation.md L159` for the EvidenceRecord shape was stale (the section is at L301). Fixed in CHG-539 (`d23926d4`). |
| XC-37 | `[x]` | `code/01_platform/04_scripts/holistic-analyze.py` | "byReason counters (8 reasons)" was stale — `RawValidationFunction.invalidReason()` returns 10 distinct reasons. Fixed in CHG-538 (`81dbd763`). |
| XC-38 | `[x]` | `code/01_platform/04_scripts/o2-provision.py` | header claimed 43 alert rules / 16 SIGNAL-; the file contains 47 names (18 ING- / 20 SIGNAL- / 9 INFRA-). Fixed in CHG-539 (`d23926d4`). |
| XC-39 | `[x]` | `code/02_services/01_ingestion/.../IngestionConfig.java` + `FullStackE2ETest.java` + `README.md` + `docs/08_implementation/03-ingestion.md` + parity pin | go-arrow pin moved to v0.2.0 (tree `1ea24cd6…`) but the comment, the dev fallback default, the builder default, the E2E env, the README, the dossier, and the doc-parity pin still said `v0.0.0-20260622-7cce1630`. All live references corrected in CHG-539 (`d23926d4`); dated plans left as historical records. |
| XC-40 | `[x]` | `code/02_services/01_ingestion/.../OtlpMetricsEmitter.java` | "Histogram (approximate via linear buckets)" was false — percentiles use a 1024-sample ring + sort (R-065/R-179). Fixed in CHG-539 (`d23926d4`). |
| XC-41 | `[x]` | `code/common/.../PostbackFingerprint.java` | `canonicalFrom` Javadoc said "used by tests to mint valid fixtures"; production `matches()` (and `canonicalParts`) call it. Fixed in CHG-539 (`d23926d4`). |


## Score history

| Date | Commit | Score | Fixed | False-pos | Won't-fix | Notes |
|---|---|---:|---:|---:|---:|---|
| 2026-10-03 | `2e961a05` | 72 | 0/548 | 0 | 0 | baseline scan (aislop 0.17.0) |
| 2026-10-03 | `68fb9823` | 74 | 23/548 | 5 | 0 | G1 verified false-positive; G2 fixed (CHG-529) + AS-329 side effect |
| 2026-10-03 | `966a8c8e` | 74 | 29/548 | 42 | 5 | G3 verified + Batch A (CHG-530) |
| 2026-10-03 | `c7877ad4` | 74 | 32/548 | 59 | 6 | G4 verified + fixes (CHG-531) |
| 2026-10-03 | `c7877ad4` | 74 | 32/548 | 66 | 6 | G5 verified — all 7 false-positive, no code change |
| 2026-10-03 | `c7877ad4` | 74 | 32/548 | 72 | 6 | G6 verified — all 6 false-positive, no code change |
| 2026-10-03 | `c7877ad4` | 74 | 32/548 | 81 | 6 | G7 verified — 1 fixed (AS-329, CHG-529) + 9 false-positive |
| 2026-10-03 | `c7877ad4` | 74 | 32/548 | 87 | 6 | G8 verified — all 6 false-positive, no code change |
| 2026-10-03 | `c7877ad4` | 74 | 32/548 | 92 | 6 | G9 verified — all 5 false-positive, no code change |
| 2026-10-03 | `c7877ad4` | 74 | 32/548 | 113 | 6 | G10 verified — all 21 false-positive, no code change |
| 2026-10-03 | `6531d77d` | 74 | 32/548 | 108 | 11 | G10 reclassified (5 accepted) + cross-check XC-1/XC-2 fixed (CHG-532) |
| 2026-10-03 | `43da9ffb` | 74 | 33/548 | 107 | 11 | G2–G10 verification sweep (7 blind reviewers, 114 items) + XC-3/XC-4/XC-5 fixed, AS-037 corrected (CHG-533) |
| 2026-10-03 | `f776ba65` | 74 | 33/548 | 113 | 102 | G11 verified (6 blind reviewers, all 102 size items: 91 accepted, 6 false-positive, 5 deferred) + XC-6…XC-10 fixed (CHG-534) |
| 2026-10-03 | `e6f046ac` | 74 | 33/548 | 174 | 102 | G12 verified — 61 formatting findings false-positive (no formatter contract) + cross-check XC-11…XC-23 fixed (CHG-535 (`e6f046ac`)) |
| 2026-10-03 | `1be7ec11` | 74 | 37/548 | 174 | 332 | G13 verified — 227 comment-policy won't-fix + 7 stale findings fixed (XC-24…XC-30, CHG-536 (`1be7ec11`); 4 cleared by rescan, scan 515→511) |
| 2026-10-03 | `d23926d4` | 75 | 42/548 | 170 | 331 | Full-audit corrections (CHG-538/539): 10 real items from the 100% blind re-audit — AS-025/026/033/034 cleared by rescan; AS-450/AS-371/AS-078/AS-119/AS-120/AS-160/AS-186/AS-276 corrected (findings remain; XC-32…XC-41); scan 510→506 |
| 2026-10-03 | `9321b489` | 74 | 38/548 | 174 | 331 | blind audit (47 items, 5 reviewers, verdict-blind): 46 confirmed, AS-035 reclassified won't-fix → fixed (XC-31, CHG-537 (`9321b489`); scan 511→510); G3 banner count corrected (7/36/5) |

*Generated from the frozen snapshot; keep this file hand-edited from here on. Baseline line numbers refer to `2e961a05`; fixes land per group and are recorded with the commit hash in each item's evidence.*

