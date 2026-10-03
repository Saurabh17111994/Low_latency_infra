# aislop — detailed findings report (detect-only)

**Project:** `streaming_project_New` · **Snapshot:** `main` @ `2e961a05` · **Generated:** 2026-10-03 10:11

**Tool:** aislop **0.17.0** (latest at scan time; deterministic, no LLM at runtime)

**Scan command (exactly as run):**

```bash
AISLOP_NO_TELEMETRY=1 AISLOP_NO_HISTORY=1 npx -y aislop@0.17.0 scan . --json \
  --exclude "**/target/**,**/__pycache__/**,**/.ruff_cache/**,logs/**"
```

**Scope:** 749 files. Excluded by design: `code/**/target/**` and other build output, `__pycache__/`, `.ruff_cache/`, and the evidence tree `logs/**` (which contains historical snapshots such as `logs/chg-399/audit_r2-pre-fix.py` that would double-count live files).

**Result:** score **72/100 — Needs Work** · 548 findings: **66 errors**, 477 warnings, 5 info · 292 mechanically fixable

**Fix status (live):** as of 2026-10-03 — G1: 5 false-positive · G2: 22 fixed in `68fb9823` (CHG-529) + AS-329 side effect · G3: 12 fixed, 32 false-positive, 4 won't-fix (`966a8c8e` CHG-530 + AS-037 in `43da9ffb` CHG-533 + AS-035 in `9321b489` CHG-537 + AS-025/026/033/034 in `81dbd763` CHG-538) · G4: 3 fixed, 17 false-positive, 1 won't-fix in `c7877ad4` (CHG-531) · G5: 7 false-positive (no code change, verified at `c7877ad4`) · G6: 6 false-positive (no code change, verified at `c7877ad4`) · G7: 1 fixed (AS-329, CHG-529) + 9 false-positive · G8: 6 false-positive · G9: 5 false-positive (verified at `c7877ad4`) · G10: 16 false-positive + 5 accepted complexity (reclassified 2026-10-03) · G11: 0 fixed, 6 false-positive, 91 accepted, 5 deferred (verified 2026-10-03) · G12: 61 false-positive (ruff-format drift, no formatter contract; 2026-10-03) — cross-check found 13 hidden defects XC-11…XC-23, fixed in CHG-535 (`e6f046ac`) · G13: 227 comment-policy won't-fix + 7 stale findings corrected (4 cleared) in CHG-536 (`1be7ec11`) · cross-check XC-1…XC-31 fixed in `6531d77d`/`43da9ffb`/`f776ba65`/`e6f046ac`/`1be7ec11`/`9321b489` (CHG-532/533/534/535/536/537) · blind audit 2026-10-03: 47 items re-judged verdict-blind — 46 confirmed, AS-035 corrected · full audit 2026-10-03: all 510 non-fixed items re-judged verdict-blind (16 reviewers) — 10 real (AS-450/AS-371/AS-025/AS-026/AS-078/AS-119/AS-120/AS-160/AS-186/AS-276), all corrected in CHG-538/539 (`81dbd763`/`d23926d4`), 500 confirmed no-action · post-sweep score **75/100** (scan 510→506). Checkbox markers below mirror `aislop-remediation-tracker.md` (canonical live doc); the scan content itself stays frozen at `2e961a05`.

**Raw data:** `/tmp/opencode/aislop-full.json` (machine-readable, 548 diagnostics with rule, severity, line, confidence, score impact) · **Checklist:** `aislop-findings.csv` (one row per finding, ready for spreadsheet triage).

---

## Read this first

- **Nothing was fixed during this scan.** This is the full, itemized output of a detect-only run. Every entry is a *candidate*; the tool is pattern-based and does not know intent. Cross-check each against the code at the stated line before deciding whether it is a real defect. The checkbox markers are the one live part of this report: they mirror `aislop-remediation-tracker.md`.
- **Line numbers are valid for `2e961a05`** (the scan revision; later commits shift them).
- **Java is not a first-class aislop target.** There is no Java formatter/linter/complexity pass; Java appears only through cross-language text rules (comment and exception patterns). The repo's 863 Java files are therefore only partially covered. Python, Go and Rust get full coverage.
- **This repo's comment convention clashes with two rules by design.** `AGENTS.md` mandates inline evidence/CHG/plan annotations and narrative documentation, so `ai-slop/narrative-comment` (211) and `ai-slop/meta-comment` (23) mostly encode deliberate convention. Treat them as policy mismatch, not defects.
- **Do not bulk-run `aislop fix` on this tree.** The 292 'fixable' findings are dominated by ruff formatting (61) and narrative comments (211); auto-fixing would generate a huge, convention-hostile diff.
- **Assessment vocabulary** (the tool's own triage): `confirmed defects` = 22 ruff lint errors; `conservative security` = 5 secret/exec hits; `AI-slop indicators` = 103 (39 swallowed exceptions, 59 warnings, 5 TODO info); `style/policy` = 418 (comments, formatting, complexity).

---

## 1. Totals by rule (triage table)

| Rule | Severity | Findings | Fixable | Engine | Assessment |
|---|---|---:|---:|---|---|
| `security/hardcoded-secret` | ERROR | 4 | 0 | Security | conservative security |
| `security/python-exec` | ERROR | 1 | 0 | Security | conservative security |
| `ai-slop/swallowed-exception` | ERROR | 39 | 0 | AI Slop | AI-slop indicators |
| `ai-slop/hardcoded-id` | WARNING | 2 | 0 | AI Slop | AI-slop indicators |
| `ai-slop/hardcoded-url` | WARNING | 4 | 0 | AI Slop | AI-slop indicators |
| `ai-slop/meta-comment` | WARNING | 23 | 0 | AI Slop | style/policy |
| `ai-slop/narrative-comment` | WARNING | 211 | 211 | AI Slop | style/policy |
| `ai-slop/python-broad-except` | WARNING | 1 | 0 | AI Slop | AI-slop indicators |
| `ai-slop/python-chained-dict-get` | WARNING | 21 | 0 | AI Slop | AI-slop indicators |
| `ai-slop/python-repetitive-dispatch` | WARNING | 7 | 0 | AI Slop | AI-slop indicators |
| `ai-slop/rust-non-test-unwrap` | WARNING | 6 | 0 | AI Slop | AI-slop indicators |
| `ai-slop/silent-recovery` | WARNING | 8 | 0 | AI Slop | AI-slop indicators |
| `ai-slop/thin-wrapper` | WARNING | 1 | 0 | AI Slop | AI-slop indicators |
| `ai-slop/unused-import` | WARNING | 9 | 9 | AI Slop | AI-slop indicators |
| `ai-slop/todo-stub` | INFO | 5 | 0 | AI Slop | AI-slop indicators |
| `ruff/E401` | ERROR | 2 | 2 | Linting (ruff) | confirmed defects |
| `ruff/E701` | ERROR | 1 | 0 | Linting (ruff) | confirmed defects |
| `ruff/E702` | ERROR | 2 | 0 | Linting (ruff) | confirmed defects |
| `ruff/E731` | ERROR | 1 | 0 | Linting (ruff) | confirmed defects |
| `ruff/E741` | ERROR | 3 | 0 | Linting (ruff) | confirmed defects |
| `ruff/F401` | ERROR | 3 | 3 | Linting (ruff) | confirmed defects |
| `ruff/F541` | ERROR | 4 | 4 | Linting (ruff) | confirmed defects |
| `ruff/F821` | ERROR | 1 | 0 | Linting (ruff) | confirmed defects |
| `ruff/F841` | ERROR | 5 | 2 | Linting (ruff) | confirmed defects |
| `complexity/deep-nesting` | WARNING | 12 | 0 | Code Quality (complexity) | style/policy |
| `complexity/file-too-large` | WARNING | 47 | 0 | Code Quality (complexity) | style/policy |
| `complexity/function-too-long` | WARNING | 55 | 0 | Code Quality (complexity) | style/policy |
| `complexity/too-many-params` | WARNING | 9 | 0 | Code Quality (complexity) | style/policy |
| `python-formatting` | WARNING | 61 | 61 | Formatting | style/policy |

## 2. Error index — all 66 error findings

Sorted security → AI slop → lint. Use the ID to jump to the full entry (section 3).

| ID | Rule | Location | Message |
|---|---|---|---|
| AS-001 | `security/hardcoded-secret` | `code/01_platform/04_scripts/t9_order_sandbox.py:157` | Possible Hardcoded password/secret detected in source code |
| AS-002 | `security/hardcoded-secret` | `code/01_platform/04_scripts/t9_order_sandbox.py:918` | Possible Hardcoded password/secret detected in source code |
| AS-003 | `security/hardcoded-secret` | `code/02_services/04_executor/src/engine.rs:611` | Possible Authentication token detected in source code |
| AS-004 | `security/hardcoded-secret` | `code/common/src/main/java/com/trading/common/config/ConfigKeys.java:99` | Possible Hardcoded password/secret detected in source code |
| AS-005 | `security/python-exec` | `code/01_platform/04_scripts/t9_order_sandbox.py:379` | Use of exec() can execute arbitrary code |
| AS-006 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/cluster_check.py:203` | Bare except with pass swallows errors silently |
| AS-007 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/cp_phase_capture.py:181` | Bare except with pass swallows errors silently |
| AS-008 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/ddl_apply.py:485` | Catch block only prints error without proper handling |
| AS-009 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/ddl_apply.py:607` | Catch block only prints error without proper handling |
| AS-010 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/ddl_apply.py:621` | Catch block only prints error without proper handling |
| AS-011 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/disaster_drills.py:65` | Bare except with pass swallows errors silently |
| AS-012 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/disaster_drills.py:136` | Bare except with pass swallows errors silently |
| AS-013 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/eod_schedule.py:90` | Catch block only prints error without proper handling |
| AS-014 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/eod_schedule.py:119` | Catch block only prints error without proper handling |
| AS-015 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/eod_schedule.py:168` | Catch block only prints error without proper handling |
| AS-016 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:197` | Bare except with pass swallows errors silently |
| AS-017 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:576` | Bare except with pass swallows errors silently |
| AS-018 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:585` | Bare except with pass swallows errors silently |
| AS-019 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:592` | Bare except with pass swallows errors silently |
| AS-020 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:616` | Bare except with pass swallows errors silently |
| AS-021 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:686` | Bare except with pass swallows errors silently |
| AS-022 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:693` | Bare except with pass swallows errors silently |
| AS-023 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:961` | Catch block only prints error without proper handling |
| AS-024 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:983` | Bare except with pass swallows errors silently |
| AS-025 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:1027` | Bare except with pass swallows errors silently |
| AS-026 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:1029` | Bare except with pass swallows errors silently |
| AS-027 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:1180` | Bare except with pass swallows errors silently |
| AS-028 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:1282` | Catch block only prints error without proper handling |
| AS-029 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:1323` | Catch block only prints error without proper handling |
| AS-030 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:1374` | Catch block only prints error without proper handling |
| AS-031 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:1425` | Catch block only prints error without proper handling |
| AS-032 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:1465` | Catch block only prints error without proper handling |
| AS-033 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:1509` | Bare except with pass swallows errors silently |
| AS-034 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:1511` | Bare except with pass swallows errors silently |
| AS-035 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:1589` | Bare except with pass swallows errors silently |
| AS-036 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:1642` | Bare except with pass swallows errors silently |
| AS-037 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/holistic-analyze.py:1644` | Bare except with pass swallows errors silently |
| AS-038 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/image_staleness_check.py:249` | Bare except with pass swallows errors silently |
| AS-039 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/o2_ingest.py:55` | Bare except with pass swallows errors silently |
| AS-040 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/perf_evidence_parse.py:469` | Bare except with pass swallows errors silently |
| AS-041 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/pernode_attribution_check.py:68` | Bare except with pass swallows errors silently |
| AS-042 | `ai-slop/swallowed-exception` | `code/01_platform/04_scripts/soak-o2-evidence.py:75` | Bare except with pass swallows errors silently |
| AS-043 | `ai-slop/swallowed-exception` | `code/01_platform/05_instruments/split_manifest.py:53` | Catch block only prints error without proper handling |
| AS-044 | `ai-slop/swallowed-exception` | `code/common/src/main/java/com/trading/common/schema/eod/EodControllerTool.java:543` | Empty catch block swallows errors silently |
| AS-343 | `ruff/E401` | `code/01_platform/04_scripts/local_int_004_smoke.py:16` | Multiple imports on one line |
| AS-344 | `ruff/E401` | `code/01_platform/05_instruments/split_manifest.py:13` | Multiple imports on one line |
| AS-345 | `ruff/E701` | `code/01_platform/04_scripts/local_int_004_smoke.py:26` | Multiple statements on one line (colon) |
| AS-346 | `ruff/E702` | `code/01_platform/05_instruments/split_manifest.py:33` | Multiple statements on one line (semicolon) |
| AS-347 | `ruff/E702` | `code/01_platform/05_instruments/split_manifest.py:39` | Multiple statements on one line (semicolon) |
| AS-348 | `ruff/E731` | `code/01_platform/04_scripts/o2-provision.py:1813` | Do not assign a `lambda` expression, use a `def` |
| AS-349 | `ruff/E741` | `code/01_platform/04_scripts/check_flink_properties.py:87` | Ambiguous variable name: `l` |
| AS-350 | `ruff/E741` | `code/01_platform/04_scripts/plan_tracker.py:35` | Ambiguous variable name: `l` |
| AS-351 | `ruff/E741` | `code/01_platform/04_scripts/plan_tracker.py:58` | Ambiguous variable name: `l` |
| AS-352 | `ruff/F401` | `code/01_platform/01_docker/alert-consumer.py:38` | `time` imported but unused |
| AS-353 | `ruff/F401` | `code/01_platform/04_scripts/deploy_preflight.py:24` | `shlex` imported but unused |
| AS-354 | `ruff/F401` | `code/01_platform/04_scripts/image_staleness_check.py:63` | `os` imported but unused |
| AS-355 | `ruff/F541` | `code/01_platform/04_scripts/alert-routing-selftest.py:93` | f-string without any placeholders |
| AS-356 | `ruff/F541` | `code/01_platform/04_scripts/alert-routing-selftest.py:177` | f-string without any placeholders |
| AS-357 | `ruff/F541` | `code/01_platform/04_scripts/fused_timeline.py:223` | f-string without any placeholders |
| AS-358 | `ruff/F541` | `code/01_platform/04_scripts/stale_table_kind_scan.py:454` | f-string without any placeholders |
| AS-359 | `ruff/F821` | `code/01_platform/04_scripts/alert-routing-selftest.py:50` | Undefined name `NoReturn` |
| AS-360 | `ruff/F841` | `code/01_platform/04_scripts/day_run.py:759` | Local variable `exc` is assigned to but never used |
| AS-361 | `ruff/F841` | `code/01_platform/04_scripts/env_facts.py:48` | Local variable `exc` is assigned to but never used |
| AS-362 | `ruff/F841` | `code/01_platform/04_scripts/holistic-analyze.py:866` | Local variable `candle_re` is assigned to but never used |
| AS-363 | `ruff/F841` | `code/01_platform/04_scripts/holistic-analyze.py:1311` | Local variable `thi` is assigned to but never used |
| AS-364 | `ruff/F841` | `code/01_platform/04_scripts/holistic-analyze.py:1652` | Local variable `dup_ing_max` is assigned to but never used |

---

## 3. Full findings, grouped by engine and rule

### Engine: Security

#### `security/hardcoded-secret` — ERROR × 4

- Engine `security` · category `Security` · assessment: **conservative security** (4 high) · fixable: no (0)
- Score impact tier: `strict` (multiplier 1)
- What it means: Possible hardcoded credential / token / password in source.
- Tool advice: Move secrets to environment variables or a secrets manager

- [f] **AS-001** `code/01_platform/04_scripts/t9_order_sandbox.py:157` — Possible Hardcoded password/secret detected in source code · **false positive** (G1 verified 2026-10-03 — see tracker)
  > L157: `CONTROL_DEFAULT_SECRET = "local-dev-only"`
- [f] **AS-002** `code/01_platform/04_scripts/t9_order_sandbox.py:918` — Possible Hardcoded password/secret detected in source code · **false positive** (G1 verified 2026-10-03 — see tracker)
  > L918: `def run_live(transport=None, probe=None, secret="local-dev-only", now=None,`
- [f] **AS-003** `code/02_services/04_executor/src/engine.rs:611` — Possible Authentication token detected in source code · **false positive** (G1 verified 2026-10-03 — see tracker)
  > L611: `let token = "tok_live_9f4c2a7e88b1";`
- [f] **AS-004** `code/common/src/main/java/com/trading/common/config/ConfigKeys.java:99` — Possible Hardcoded password/secret detected in source code · **false positive** (G1 verified 2026-10-03 — see tracker)
  > L99: `public static final String O2_PASSWORD = "O2_PASSWORD";`

#### `security/python-exec` — ERROR × 1

- Engine `security` · category `Security` · assessment: **conservative security** (1 high) · fixable: no (0)
- Score impact tier: `standard` (multiplier 1)
- What it means: exec() usage that can execute arbitrary code.
- Tool advice: Avoid exec — use safer alternatives

- [f] **AS-005** `code/01_platform/04_scripts/t9_order_sandbox.py:379` — Use of exec() can execute arbitrary code · **false positive** (G1 verified 2026-10-03 — see tracker)
  > L379: `def exec(self, service, shell):`

### Engine: AI Slop

#### `ai-slop/swallowed-exception` — ERROR × 39

- Engine `ai-slop` · category `AI Slop` · assessment: **AI-slop indicators** (39 high) · fixable: no (0)
- Score impact tier: `strict` (multiplier 1)
- What it means: Empty catch block or catch block that only logs — the failure is discarded.
- Tool advice: Handle errors explicitly: log with context, rethrow, or return an error value

- [f] **AS-006** `code/01_platform/04_scripts/cluster_check.py:203` — Bare except with pass swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L203: `except ValueError:`
- [-] **AS-007** `code/01_platform/04_scripts/cp_phase_capture.py:181` — Bare except with pass swallows errors silently · **won't-fix** (G3 verified 2026-10-03 — see tracker)
  > L181: `except Exception:`
- [f] **AS-008** `code/01_platform/04_scripts/ddl_apply.py:485` — Catch block only prints error without proper handling · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L485: `except OSError as exc:`
- [f] **AS-009** `code/01_platform/04_scripts/ddl_apply.py:607` — Catch block only prints error without proper handling · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L607: `except (OSError, json.JSONDecodeError) as exc:`
- [f] **AS-010** `code/01_platform/04_scripts/ddl_apply.py:621` — Catch block only prints error without proper handling · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L621: `except (OSError, json.JSONDecodeError) as exc:`
- [f] **AS-011** `code/01_platform/04_scripts/disaster_drills.py:65` — Bare except with pass swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L65: `except OSError:`
- [f] **AS-012** `code/01_platform/04_scripts/disaster_drills.py:136` — Bare except with pass swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L136: `except OSError:`
- [f] **AS-013** `code/01_platform/04_scripts/eod_schedule.py:90` — Catch block only prints error without proper handling · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L90: `except OSError as exc:`
- [f] **AS-014** `code/01_platform/04_scripts/eod_schedule.py:119` — Catch block only prints error without proper handling · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L119: `except OSError as exc:`
- [f] **AS-015** `code/01_platform/04_scripts/eod_schedule.py:168` — Catch block only prints error without proper handling · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L168: `except OSError as exc:`
- [-] **AS-016** `code/01_platform/04_scripts/holistic-analyze.py:197` — Bare except with pass swallows errors silently · **won't-fix** (G3 verified 2026-10-03 — see tracker)
  > L197: `except OSError:`
- [f] **AS-017** `code/01_platform/04_scripts/holistic-analyze.py:576` — Bare except with pass swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L576: `except OSError:`
- [f] **AS-018** `code/01_platform/04_scripts/holistic-analyze.py:585` — Bare except with pass swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L585: `except OSError:`
- [f] **AS-019** `code/01_platform/04_scripts/holistic-analyze.py:592` — Bare except with pass swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L592: `except OSError:`
- [f] **AS-020** `code/01_platform/04_scripts/holistic-analyze.py:616` — Bare except with pass swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L616: `except OSError:`
- [f] **AS-021** `code/01_platform/04_scripts/holistic-analyze.py:686` — Bare except with pass swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L686: `except OSError:`
- [f] **AS-022** `code/01_platform/04_scripts/holistic-analyze.py:693` — Bare except with pass swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L693: `except OSError:`
- [f] **AS-023** `code/01_platform/04_scripts/holistic-analyze.py:961` — Catch block only prints error without proper handling · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L961: `except OSError:`
- [f] **AS-024** `code/01_platform/04_scripts/holistic-analyze.py:983` — Bare except with pass swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L983: `except OSError:`
- [x] **AS-025** `code/01_platform/04_scripts/holistic-analyze.py:1027` — Bare except with pass swallows errors silently · **fixed** (CHG-538 — cleared by rescan; see tracker)
  > L1027: `except ValueError:`
- [x] **AS-026** `code/01_platform/04_scripts/holistic-analyze.py:1029` — Bare except with pass swallows errors silently · **fixed** (CHG-538 — cleared by rescan; see tracker)
  > L1029: `except OSError:`
- [f] **AS-027** `code/01_platform/04_scripts/holistic-analyze.py:1180` — Bare except with pass swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L1180: `except OSError:`
- [f] **AS-028** `code/01_platform/04_scripts/holistic-analyze.py:1282` — Catch block only prints error without proper handling · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L1282: `except OSError:`
- [f] **AS-029** `code/01_platform/04_scripts/holistic-analyze.py:1323` — Catch block only prints error without proper handling · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L1323: `except OSError:`
- [f] **AS-030** `code/01_platform/04_scripts/holistic-analyze.py:1374` — Catch block only prints error without proper handling · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L1374: `except OSError:`
- [f] **AS-031** `code/01_platform/04_scripts/holistic-analyze.py:1425` — Catch block only prints error without proper handling · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L1425: `except OSError:`
- [f] **AS-032** `code/01_platform/04_scripts/holistic-analyze.py:1465` — Catch block only prints error without proper handling · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L1465: `except OSError:`
- [x] **AS-033** `code/01_platform/04_scripts/holistic-analyze.py:1509` — Bare except with pass swallows errors silently · **fixed** (CHG-538 — G6b read retired, cleared by rescan; see tracker)
  > L1509: `except ValueError:`
- [x] **AS-034** `code/01_platform/04_scripts/holistic-analyze.py:1511` — Bare except with pass swallows errors silently · **fixed** (CHG-538 — G6b read retired, cleared by rescan; see tracker)
  > L1511: `except OSError:`
- [x] **AS-035** `code/01_platform/04_scripts/holistic-analyze.py:1589` — Bare except with pass swallows errors silently · **won't-fix** (G3 verified 2026-10-03 — see tracker)
  > L1589: `except OSError:`
- [-] **AS-036** `code/01_platform/04_scripts/holistic-analyze.py:1642` — Bare except with pass swallows errors silently · **won't-fix** (G3 verified 2026-10-03 — see tracker)
  > L1642: `except ValueError:`
- [x] **AS-037** `code/01_platform/04_scripts/holistic-analyze.py:1644` — Bare except with pass swallows errors silently · **fixed** (CHG-533 — the missing-counter case is surfaced as UNAVAILABLE; see tracker)
  > L1644: `except OSError:`
- [f] **AS-038** `code/01_platform/04_scripts/image_staleness_check.py:249` — Bare except with pass swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L249: `except ValueError:`
- [f] **AS-039** `code/01_platform/04_scripts/o2_ingest.py:55` — Bare except with pass swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L55: `except OSError:`
- [f] **AS-040** `code/01_platform/04_scripts/perf_evidence_parse.py:469` — Bare except with pass swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L469: `except OSError:`
- [f] **AS-041** `code/01_platform/04_scripts/pernode_attribution_check.py:68` — Bare except with pass swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L68: `except OSError:`
- [f] **AS-042** `code/01_platform/04_scripts/soak-o2-evidence.py:75` — Bare except with pass swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L75: `except OSError:`
- [f] **AS-043** `code/01_platform/05_instruments/split_manifest.py:53` — Catch block only prints error without proper handling · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L53: `except (ValueError, TypeError):`
- [f] **AS-044** `code/common/src/main/java/com/trading/common/schema/eod/EodControllerTool.java:543` — Empty catch block swallows errors silently · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L543: `} catch (Exception e) {`

#### `ai-slop/hardcoded-id` — WARNING × 2

- Engine `ai-slop` · category `AI Slop` · assessment: **AI-slop indicators** (2 medium) · fixable: no (0)
- Score impact tier: `advisory` (multiplier 0.25, rule cap 4)
- What it means: Provider/project/tenant identifier baked into source.
- Tool advice: Move provider IDs, tenant IDs, price IDs, and similar deployment-specific identifiers to env/config so agents do not bake one environment into source.

- [f] **AS-045** `code/02_services/04_executor/src/config.rs:396` — Hardcoded provider/project ID in production code · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L396: `("BRIDGE_AUTH_TOKEN", "tok_live_9f4c2a7e88b1"),`
- [f] **AS-046** `code/02_services/04_executor/src/engine.rs:611` — Hardcoded provider/project ID in production code · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L611: `let token = "tok_live_9f4c2a7e88b1";`

#### `ai-slop/hardcoded-url` — WARNING × 4

- Engine `ai-slop` · category `AI Slop` · assessment: **AI-slop indicators** (4 medium) · fixable: no (0)
- Score impact tier: `advisory` (multiplier 0.25, rule cap 4)
- What it means: Environment-specific URL baked into source instead of config.
- Tool advice: Move deployment-specific URLs to environment variables or a typed config module. Keep only stable documentation/public links inline.

- [f] **AS-047** `code/02_services/04_executor/src/config.rs:431` — Hardcoded environment URL in production code · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L431: `("ARROW_REST_URL", "https://api"),`
- [f] **AS-048** `code/02_services/04_executor/src/engine.rs:613` — Hardcoded environment URL in production code · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L613: `base_url: "http://bridge:8080".to_string(),`
- [f] **AS-049** `code/02_services/04_executor/src/engine.rs:631` — Hardcoded environment URL in production code · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L631: `base_url: "http://bridge:8080".to_string(),`
- [f] **AS-050** `code/02_services/04_executor/src/engine.rs:697` — Hardcoded environment URL in production code · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L697: `assert_eq!(base_url, "http://execution-bridge:8787");`

#### `ai-slop/meta-comment` — WARNING × 23

- Engine `ai-slop` · category `Comments` · assessment: **style/policy** (23 medium) · fixable: no (0)
- Score impact tier: `style` (multiplier 0.5, rule cap 8)
- What it means: Comments about plans/process/before-after state instead of the code itself.
- Tool advice: Remove — references to the build plan or before/after code state belong in PR descriptions and commit messages, not source.

- [-] **AS-051** `code/01_platform/04_scripts/holistic-analyze.py:1591` — Meta/plan comment (before/after state narration)
  > L1591: `# Parity-only mode (TM-kill drill, 2026-08-31): without deliberate`
- [-] **AS-052** `code/01_platform/04_scripts/ing-tcp001/TokenCountReconcile.java:20` — Meta/plan comment (before/after state narration)
  > L20: `/**`
- [x] **AS-053** `code/01_platform/04_scripts/ing-tcp001/TokenCountReconcile.java:195` — Meta/plan comment (before/after state narration)
  > L195: `// P6-118: the scanner used to be capped at limit(1_000_000_000),`
- [-] **AS-054** `code/02_services/01_ingestion/go-bridge/supervisor.go:112` — Meta/plan comment (before/after state narration)
  > L112: `// P1-157: a panic in the slot's main flow used to be swallowed by the`
- [-] **AS-055** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:859` — Meta/plan comment (before/after state narration)
  > L859: `// Proto-only default (2026-08-29): the NDJSON pipe transport was`
- [-] **AS-056** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:1671` — Meta/plan comment (plan/process reference)
  > L1671: `/**`
- [-] **AS-057** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2452` — Meta/plan comment (plan/process reference)
  > L2452: `/**`
- [-] **AS-058** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SourceIdleWatchdogState.java:7` — Meta/plan comment (before/after state narration)
  > L7: `/**`
- [-] **AS-059** `code/02_services/04_executor/src/execution/client.rs:2519` — Meta/plan comment (before/after state narration)
  > L2519: `// FOK has no bridge equivalent: previously it was silently rewritten to DAY.`
- [-] **AS-060** `code/02_services/04_executor/src/projection/mod.rs:1660` — Meta/plan comment (before/after state narration)
  > L1660: ``// Only `< 0` used to be rejected, so a zero-price fill diluted the weighted average.``
- [-] **AS-061** `code/02_services/04_executor/src/shutdown.rs:336` — Meta/plan comment (before/after state narration)
  > L336: `// P3-460: the fail-closed invariant of shutdown step 1 used to be *assumed* - a gate that`
- [-] **AS-062** `code/02_services/06_execution_bridge/go-bridge/reauth.go:52` — Meta/plan comment (before/after state narration)
  > L52: `// P3-048/P3-049: disabled is fail-closed for commands, not just for /healthz.`
- [-] **AS-063** `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/ExecutionGatewayMain.java:110` — Meta/plan comment (before/after state narration)
  > L110: `// P3-277: the reader is owned by try-with-resources so a throw from`
- [-] **AS-064** `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/IntentReader.java:190` — Meta/plan comment (before/after state narration)
  > L190: `/**`
- [-] **AS-065** `code/common/src/main/java/com/trading/common/ownership/OwnershipMatrix.java:55` — Meta/plan comment (before/after state narration)
  > L55: `// Defensive immutable copies: the caller's set used to be stored by`
- [-] **AS-066** `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:301` — Meta/plan comment (plan/process reference)
  > L301: `// Step 2 — manifest/DDL checksums.`
- [-] **AS-067** `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:365` — Meta/plan comment (plan/process reference)
  > L365: `// Step 4 — empty-catalog precondition.`
- [-] **AS-068** `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:424` — Meta/plan comment (plan/process reference)
  > L424: `// Step 4 — apply in deterministic order.`
- [-] **AS-069** `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:456` — Meta/plan comment (plan/process reference)
  > L456: `// Step 7 — write/read smoke per table. A raw-client write to a`
- [-] **AS-070** `code/common/src/main/java/com/trading/common/schema/execution/FlussAttemptStore.java:213` — Meta/plan comment (before/after state narration)
  > L213: `/**`
- [-] **AS-071** `code/common/src/main/java/com/trading/common/schema/execution/GateRow.java:106` — Meta/plan comment (before/after state narration)
  > L106: `/**`
- [-] **AS-072** `code/common/src/main/java/com/trading/common/schema/execution/InMemoryGateStateStore.java:156` — Meta/plan comment (before/after state narration)
  > L156: `// Not the holder — fail closed, no mutation (offline fencing constraint). This used`
- [-] **AS-073** `code/common/src/main/java/com/trading/common/schema/position/PositionProjectorDriver.java:42` — Meta/plan comment (before/after state narration)
  > L42: `/** Projected — the new snapshot replaced the previous one. */`

#### `ai-slop/narrative-comment` — WARNING × 211

- Engine `ai-slop` · category `Comments` · assessment: **style/policy** (211 medium) · fixable: yes (211)
- Score impact tier: `style` (multiplier 0.5, rule cap 8)
- What it means: Decorative separators, phase/section headers, JSDoc preambles, cross-reference prose.
- Tool advice: Remove — narrative/decorative comments belong in PR descriptions, not source. Code should be self-explanatory.

- [-] **AS-074** `code/01_platform/04_scripts/alert-routing-selftest.py:113` — Narrative comment block (decorative separator)
  > L113: `# --- 1. consumer alive --------------------------------------------------`
- [-] **AS-075** `code/01_platform/04_scripts/alert-routing-selftest.py:119` — Narrative comment block (decorative separator)
  > L119: `# --- 2. negative: malformed body rejected, consumer survives ------------`
- [-] **AS-076** `code/01_platform/04_scripts/alert-routing-selftest.py:128` — Narrative comment block (decorative separator)
  > L128: `# --- 3. temp always-firing alert ---------------------------------------`
- [-] **AS-077** `code/01_platform/04_scripts/alert-routing-selftest.py:179` — Narrative comment block (decorative separator)
  > L179: `# --- 4-5. poll the durable record ---------------------------------------`
- [-] **AS-078** `code/01_platform/04_scripts/audit_r2.py:2` — Narrative comment block (decorative separator) · full-audit correction: XC-36 fixed in CHG-539 (`d23926d4`); style remains flagged
  > L2: `# =============================================================================`
- [-] **AS-079** `code/01_platform/04_scripts/audit_r2.py:112` — Narrative comment block (decorative separator)
  > L112: `# ---------------------------------------------------------------------------`
- [-] **AS-080** `code/01_platform/04_scripts/audit_r2.py:165` — Narrative comment block (decorative separator)
  > L165: `# ---------------------------------------------------------------------------`
- [-] **AS-081** `code/01_platform/04_scripts/audit_r2.py:277` — Narrative comment block (decorative separator)
  > L277: `# ---------------------------------------------------------------------------`
- [-] **AS-082** `code/01_platform/04_scripts/audit_r2.py:430` — Narrative comment block (decorative separator)
  > L430: `# ---------------------------------------------------------------------------`
- [-] **AS-083** `code/01_platform/04_scripts/day_run.py:39` — Narrative comment block (decorative separator)
  > L39: `# --------------------------------------------------------------------------`
- [-] **AS-084** `code/01_platform/04_scripts/day_run.py:119` — Narrative comment block (decorative separator)
  > L119: `# --------------------------------------------------------------------------`
- [-] **AS-085** `code/01_platform/04_scripts/day_run.py:184` — Narrative comment block (decorative separator)
  > L184: `# --------------------------------------------------------------------------`
- [-] **AS-086** `code/01_platform/04_scripts/day_run.py:603` — Narrative comment block (decorative separator)
  > L603: `# --------------------------------------------------------------------------`
- [-] **AS-087** `code/01_platform/04_scripts/day_run.py:641` — Narrative comment block (decorative separator)
  > L641: `# --------------------------------------------------------------------------`
- [-] **AS-088** `code/01_platform/04_scripts/day_run.py:1011` — Narrative comment block (decorative separator)
  > L1011: `# --------------------------------------------------------------------------`
- [-] **AS-089** `code/01_platform/04_scripts/day_run.py:1198` — Narrative comment block (phase/section header)
  > L1198: `# Phase 2: execution-t3 chain in its offline posture (profile-gated;`
- [-] **AS-090** `code/01_platform/04_scripts/day_run.py:1350` — Narrative comment block (decorative separator)
  > L1350: `# --------------------------------------------------------------------------`
- [-] **AS-091** `code/01_platform/04_scripts/disaster_drills.py:104` — Narrative comment block (decorative separator)
  > L104: `# --------------------------------------------------------------------------`
- [-] **AS-092** `code/01_platform/04_scripts/disaster_drills.py:248` — Narrative comment block (decorative separator)
  > L248: `# --------------------------------------------------------------------------`
- [-] **AS-093** `code/01_platform/04_scripts/docs_audit.py:387` — Narrative comment block (cross-reference commentary)
  > L387: `# One measurement, used by both the check and its message: a second call`
- [-] **AS-094** `code/01_platform/04_scripts/docs_audit.py:428` — Narrative comment block (decorative separator)
  > L428: `# ---------------------------------------------------------------------------`
- [-] **AS-095** `code/01_platform/04_scripts/docs_audit.py:630` — Narrative comment block (decorative separator)
  > L630: `# ---------------------------------------------------------------------------`
- [-] **AS-096** `code/01_platform/04_scripts/docs_audit.py:653` — Narrative comment block (decorative separator)
  > L653: `# --- doc scans (04-decisions.md is the dated record: DEC-012/018/039) ---`
- [-] **AS-097** `code/01_platform/04_scripts/docs_audit.py:677` — Narrative comment block (decorative separator)
  > L677: `# --- code: HFT modes ltpc (40 B) + full (196 B), nothing else accepted ---`
- [-] **AS-098** `code/01_platform/04_scripts/docs_audit.py:700` — Narrative comment block (decorative separator)
  > L700: `# --- code: bridge converts ns -> ms ---`
- [-] **AS-099** `code/01_platform/04_scripts/docs_audit.py:704` — Narrative comment block (decorative separator)
  > L704: `# --- manifest/DDL: ledger + halt kinds KV; ledger live in dev ---`
- [-] **AS-100** `code/01_platform/04_scripts/docs_audit.py:765` — Narrative comment block (decorative separator)
  > L765: `# --- inventories include the candle table + ingestion_quarantine ---`
- [-] **AS-101** `code/01_platform/04_scripts/docs_audit.py:779` — Narrative comment block (decorative separator)
  > L779: `# ---------------------------------------------------------------------------`
- [-] **AS-102** `code/01_platform/04_scripts/docs_audit.py:860` — Narrative comment block (decorative separator)
  > L860: `# --- coverage table in 11-testing-and-release.md ---`
- [-] **AS-103** `code/01_platform/04_scripts/docs_audit.py:903` — Narrative comment block (decorative separator)
  > L903: `# --- dossier status rows (02-10) ---`
- [-] **AS-104** `code/01_platform/04_scripts/docs_audit.py:932` — Narrative comment block (decorative separator)
  > L932: `# --- REQ13-* ids are scoped to the master dossier (04) and defined there ---`
- [-] **AS-105** `code/01_platform/04_scripts/docs_audit.py:956` — Narrative comment block (decorative separator)
  > L956: `# ---------------------------------------------------------------------------`
- [-] **AS-106** `code/01_platform/04_scripts/docs_audit.py:1028` — Narrative comment block (decorative separator)
  > L1028: `# ---------------------------------------------------------------------------`
- [-] **AS-107** `code/01_platform/04_scripts/docs_audit.py:1095` — Narrative comment block (decorative separator)
  > L1095: `# ---------------------------------------------------------------------------`
- [-] **AS-108** `code/01_platform/04_scripts/docs_audit.py:1146` — Narrative comment block (decorative separator)
  > L1146: `# ---------------------------------------------------------------------------`
- [-] **AS-109** `code/01_platform/04_scripts/docs_audit.py:1253` — Narrative comment block (decorative separator)
  > L1253: `# ---------------------------------------------------------------------------`
- [-] **AS-110** `code/01_platform/04_scripts/docs_audit.py:1468` — Narrative comment block (decorative separator)
  > L1468: `# ---------------------------------------------------------------------------`
- [-] **AS-111** `code/01_platform/04_scripts/holistic-analyze.py:729` — Narrative comment block (decorative separator)
  > L729: `# ---- previews: UNAVAILABLE after the 2026-09-05 multi-timeframe cutover ----`
- [-] **AS-112** `code/01_platform/04_scripts/holistic-analyze.py:790` — Narrative comment block (decorative separator)
  > L790: `# ---- Final candle path: the LATENCY leg only ----`
- [-] **AS-113** `code/01_platform/04_scripts/holistic-analyze.py:834` — Narrative comment block (decorative separator)
  > L834: `# ---- Signal_Candidates: volume by status + settlement balance ----`
- [-] **AS-114** `code/01_platform/04_scripts/holistic-analyze.py:920` — Narrative comment block (decorative separator)
  > L920: `# ---- Signal-path latency (2026-08-31) -------------------------------`
- [-] **AS-115** `code/01_platform/04_scripts/holistic-analyze.py:1432` — Narrative comment block (decorative separator)
  > L1432: `# ---- G6 ASSERTIVE GUARDS (2026-08-30) --------------------------------`
- [-] **AS-116** `code/01_platform/04_scripts/holistic-analyze.py:1437` — Narrative comment block (decorative separator)
  > L1437: `# ---- D6 guard (2026-08-31): ingestion JVM live-set leak alarm ----`
- [-] **AS-117** `code/01_platform/04_scripts/holistic-analyze.py:1569` — Narrative comment block (decorative separator)
  > L1569: `# ---- G7 DATA-QUALITY GUARDS (F2/F3 audit, 2026-08-30) ----------------`
- [-] **AS-118** `code/01_platform/04_scripts/holistic-analyze.py:1632` — Narrative comment block (justification prose)
  > L1632: `# last counter sample timestamp: counter deltas only cover rounds`
- [-] **AS-119** `code/01_platform/04_scripts/holistic-analyze.py:1778` — Narrative comment block (decorative separator) · full-audit correction: XC-37 fixed in CHG-538 (`81dbd763`); style remains flagged
  > L1778: `# ---- F6 (2026-08-31): raw-validation rejection counters ----`
- [-] **AS-120** `code/01_platform/04_scripts/o2-provision.py:2` — Narrative comment block (decorative separator) · full-audit correction: XC-38 fixed in CHG-539 (`d23926d4`); style remains flagged
  > L2: `# =============================================================================`
- [-] **AS-121** `code/01_platform/04_scripts/o2-provision.py:49` — Narrative comment block (decorative separator)
  > L49: `# ---------------------------------------------------------------------------`
- [-] **AS-122** `code/01_platform/04_scripts/o2-provision.py:910` — Narrative comment block (decorative separator)
  > L910: `# --- Ingestion phase (ING- prefix) ---`
- [-] **AS-123** `code/01_platform/04_scripts/o2-provision.py:1048` — Narrative comment block (decorative separator)
  > L1048: `# --- P8.3 SignalJob/Flink/collector rules (tracker 14, approved 2026-08-11) ---`
- [-] **AS-124** `code/01_platform/04_scripts/o2-provision.py:1236` — Narrative comment block (decorative separator)
  > L1236: `# --- 2026-08-22 single-pane: infra/JVM/host infra alerts (10-observability.md scale-up thresholds) ---`
- [-] **AS-125** `code/01_platform/04_scripts/r2_legal_hold_check.py:80` — Narrative comment block (decorative separator)
  > L80: `# ---------------------------------------------------------------------------`
- [-] **AS-126** `code/01_platform/04_scripts/r2_legal_hold_check.py:220` — Narrative comment block (decorative separator)
  > L220: `# ---------------------------------------------------------------------------`
- [-] **AS-127** `code/01_platform/04_scripts/r2_legal_hold_check.py:278` — Narrative comment block (decorative separator)
  > L278: `# ---------------------------------------------------------------------------`
- [-] **AS-128** `code/01_platform/04_scripts/r2_legal_hold_check.py:435` — Narrative comment block (decorative separator)
  > L435: `# ---------------------------------------------------------------------------`
- [-] **AS-129** `code/01_platform/04_scripts/stale_table_kind_scan.py:112` — Narrative comment block (decorative separator)
  > L112: `# ---------------------------------------------------------------------------`
- [-] **AS-130** `code/01_platform/04_scripts/stale_table_kind_scan.py:309` — Narrative comment block (decorative separator)
  > L309: `# ---------------------------------------------------------------------------`
- [-] **AS-131** `code/01_platform/04_scripts/stale_table_kind_scan.py:376` — Narrative comment block (decorative separator)
  > L376: `# ---------------------------------------------------------------------------`
- [-] **AS-132** `code/01_platform/04_scripts/t9_order_sandbox.py:161` — Narrative comment block (decorative separator)
  > L161: `# ---------------------------------------------------------------------------`
- [-] **AS-133** `code/01_platform/04_scripts/t9_order_sandbox.py:186` — Narrative comment block (decorative separator)
  > L186: `# ---------------------------------------------------------------------------`
- [-] **AS-134** `code/01_platform/04_scripts/t9_order_sandbox.py:340` — Narrative comment block (decorative separator)
  > L340: `# ---------------------------------------------------------------------------`
- [-] **AS-135** `code/01_platform/04_scripts/t9_order_sandbox.py:417` — Narrative comment block (decorative separator)
  > L417: `# ---------------------------------------------------------------------------`
- [-] **AS-136** `code/01_platform/04_scripts/t9_order_sandbox.py:580` — Narrative comment block (decorative separator)
  > L580: `# ---------------------------------------------------------------------------`
- [-] **AS-137** `code/01_platform/04_scripts/t9_order_sandbox.py:1091` — Narrative comment block (decorative separator)
  > L1091: `# ---------------------------------------------------------------------------`
- [-] **AS-138** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/FlussClientAdapter.java:273` — Narrative comment block (decorative separator)
  > L273: `// --- v4: full-mode field capture, DDL order (indexes 21-71), all ---`
- [-] **AS-139** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:406` — Narrative comment block (decorative separator)
  > L406: `// ---- main entry point ----`
- [-] **AS-140** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:577` — Narrative comment block (decorative separator)
  > L577: `// ---- bridge subprocess loop ----`
- [-] **AS-141** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:603` — Narrative comment block (JSDoc preamble with slop signal)
  > L603: `/**`
- [-] **AS-142** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:1757` — Narrative comment block (decorative separator)
  > L1757: `// ---- Slot-scoped safety propagation (plan Amendment §Slot-scoped safety) ----`
- [-] **AS-143** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:1760` — Narrative comment block (decorative separator)
  > L1760: `// ---- Discontinuity evidence (plan §DiscontinuityWriter) ----`
- [-] **AS-144** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2046` — Narrative comment block (decorative separator)
  > L2046: `// ---- ING-FAIL-007: clock-jump monitoring ----`
- [-] **AS-145** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2066` — Narrative comment block (decorative separator)
  > L2066: `// ---- SIGNAL-warn-jvm-heap-high: sustained container-memory readiness gate ----`
- [-] **AS-146** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2159` — Narrative comment block (decorative separator)
  > L2159: `// ---- shutdown ----`
- [-] **AS-147** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2321` — Narrative comment block (decorative separator)
  > L2321: `// ---- health accessor ----`
- [-] **AS-148** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2410` — Narrative comment block (decorative separator)
  > L2410: `// ---- helpers ----`
- [-] **AS-149** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TypedFlussRowConverter.java:74` — Narrative comment block (decorative separator)
  > L74: `// --- v4 (indexes 21-71): all BIGINT NULL. Boxed Long so "absent" is`
- [-] **AS-150** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:30` — Narrative comment block (decorative separator)
  > L30: `// ---- Constants (matching dossier) -- T2 tunable backpressure (G2 Ingest) ----`
- [-] **AS-151** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:41` — Narrative comment block (decorative separator)
  > L41: `// ---- Validated values (populated by validate()) ----`
- [-] **AS-152** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:101` — Narrative comment block (decorative separator)
  > L101: `// ---- HFT connection policy (plan §IngestionConfig — exact values) ----`
- [-] **AS-153** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:165` — Narrative comment block (decorative separator)
  > L165: `// ---- Validation ----`
- [-] **AS-154** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:197` — Narrative comment block (decorative separator)
  > L197: `// ---- Arrow auth (TOTP only — ARROW_TOKEN removed 2026-08-24) ----`
- [x] **AS-155** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:216` — Narrative comment block (decorative separator)
  > L216: `// ---- Arrow feed (HFT only — the Standard feed was removed 2026-08-14) ----`
- [-] **AS-156** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:220` — Narrative comment block (decorative separator)
  > L220: `// ---- Fluss ----`
- [-] **AS-157** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:263` — Narrative comment block (decorative separator)
  > L263: `// ---- Backpressure -- T2 tunable (G2 Ingest) ----`
- [-] **AS-158** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:279` — Narrative comment block (decorative separator)
  > L279: `// ---- Timing ----`
- [-] **AS-159** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:305` — Narrative comment block (decorative separator)
  > L305: `// ---- HFT connection policy (plan §IngestionConfig — exact values) ----`
- [-] **AS-160** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:346` — Narrative comment block (decorative separator) · full-audit correction: XC-39 fixed in CHG-539 (`d23926d4`); style remains flagged
  > L346: `// ---- Fingerprint & SDK version ----`
- [-] **AS-161** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:352` — Narrative comment block (decorative separator)
  > L352: `// ---- DDL & clock strictness ----`
- [-] **AS-162** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:357` — Narrative comment block (decorative separator)
  > L357: `// ---- Standard derived values ----`
- [-] **AS-163** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:362` — Narrative comment block (decorative separator)
  > L362: `// ---- Fail if any errors ----`
- [-] **AS-164** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:424` — Narrative comment block (decorative separator)
  > L424: `// ---- env helpers ----`
- [-] **AS-165** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:596` — Narrative comment block (decorative separator)
  > L596: `// ---- T2 alias helpers — primary + PENDING_MAX_* alias ----`
- [-] **AS-166** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:689` — Narrative comment block (decorative separator)
  > L689: `// ---- Builder ----`
- [-] **AS-167** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/discontinuity/SequenceGapMonitor.java:7` — Narrative comment block (JSDoc preamble with slop signal)
  > L7: `/**`
- [-] **AS-168** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/fingerprint/FingerprintBuilder.java:115` — Narrative comment block (decorative separator)
  > L115: `// ---- internal helpers ----`
- [-] **AS-169** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:75` — Narrative comment block (decorative separator)
  > L75: `// ---- liveness ----`
- [-] **AS-170** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:79` — Narrative comment block (cross-reference commentary)
  > L79: `/** Called from a shutdown hook — marks the process as not-alive. */`
- [-] **AS-171** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:82` — Narrative comment block (decorative separator)
  > L82: `// ---- readiness setters (called by IngestionService) ----`
- [-] **AS-172** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:247` — Narrative comment block (decorative separator)
  > L247: `// ---- readiness ----`
- [-] **AS-173** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:276` — Narrative comment block (decorative separator)
  > L276: `// ---- diagnostics ----`
- [-] **AS-174** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/NtpClockChecker.java:178` — Narrative comment block (decorative separator)
  > L178: `// ---- NTP query (RFC 5905 SNTP client) ----`
- [-] **AS-175** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/NtpClockChecker.java:288` — Narrative comment block (decorative separator)
  > L288: `// ---- Exception ----`
- [-] **AS-176** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:12` — Narrative comment block (decorative separator)
  > L12: `// --- provenance ---`
- [-] **AS-177** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:17` — Narrative comment block (decorative separator)
  > L17: `// --- routing ---`
- [-] **AS-178** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:22` — Narrative comment block (decorative separator)
  > L22: `// --- event time ---`
- [-] **AS-179** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:27` — Narrative comment block (decorative separator)
  > L27: `// --- trade data (verified/normalized; prices in paise) ---`
- [-] **AS-180** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:39` — Narrative comment block (decorative separator)
  > L39: `// --- v4 full-mode fields (raw_table_1 indexes 21-71) ---`
- [-] **AS-181** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:76` — Narrative comment block (decorative separator)
  > L76: `// --- fingerprint ---`
- [-] **AS-182** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:80` — Narrative comment block (decorative separator)
  > L80: `// --- connection identity ---`
- [-] **AS-183** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:85` — Narrative comment block (decorative separator)
  > L85: `// --- schema ---`
- [-] **AS-184** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:248` — Narrative comment block (decorative separator)
  > L248: `// --- accessors ---`
- [-] **AS-185** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:103` — Narrative comment block (decorative separator)
  > L103: `// ---- Counters ----`
- [-] **AS-186** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:118` — Narrative comment block (decorative separator) · full-audit correction: XC-40 fixed in CHG-539 (`d23926d4`); style remains flagged
  > L118: `// ---- Histogram (approximate via linear buckets) ----`
- [-] **AS-187** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:156` — Narrative comment block (decorative separator)
  > L156: `// ---- Gauges ----`
- [-] **AS-188** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:165` — Narrative comment block (decorative separator)
  > L165: `// ---- Reason counters ----`
- [-] **AS-189** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:168` — Narrative comment block (decorative separator)
  > L168: `// ---- Slot metrics (plan §Monitoring — labeled by slot only) ----`
- [-] **AS-190** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:188` — Narrative comment block (decorative separator)
  > L188: `// ---- Resource metrics (plan Amendment §Resource) ----`
- [-] **AS-191** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:290` — Narrative comment block (decorative separator)
  > L290: `// ---- Recording API ----`
- [-] **AS-192** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:369` — Narrative comment block (decorative separator)
  > L369: `// ---- Slot + resource recording (plan §Monitoring / Amendment §Resource) ----`
- [-] **AS-193** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:437` — Narrative comment block (decorative separator)
  > L437: `// ---- package-private test accessors ----`
- [-] **AS-194** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:443` — Narrative comment block (decorative separator)
  > L443: `// ---- Flush ----`
- [-] **AS-195** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:514` — Narrative comment block (decorative separator)
  > L514: `// ---- JSON builder (minimal OTLP metrics format) ----`
- [-] **AS-196** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:571` — Narrative comment block (decorative separator)
  > L571: `// ---- Slot gauges (labeled by slot only) ----`
- [-] **AS-197** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:596` — Narrative comment block (decorative separator)
  > L596: `// ---- Resource + capacity gauges (Amendment §Resource) ----`
- [-] **AS-198** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java:69` — Narrative comment block (decorative separator)
  > L69: `// ---- listener ----`
- [-] **AS-199** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java:83` — Narrative comment block (decorative separator)
  > L83: `// ---- accept gate ----`
- [-] **AS-200** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java:163` — Narrative comment block (decorative separator)
  > L163: `// ---- P1-262: fire outside the accept-gate lock ----`
- [-] **AS-201** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java:220` — Narrative comment block (decorative separator)
  > L220: `// ---- health ----`
- [-] **AS-202** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java:237` — Narrative comment block (decorative separator)
  > L237: `// ---- Success ----`
- [-] **AS-203** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java:522` — Narrative comment block (decorative separator)
  > L522: `// ---- outcome type ----`
- [-] **AS-204** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/CandleAccumulator.java:51` — Narrative comment block (cross-reference commentary)
  > L51: `/**`
- [-] **AS-205** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/CandleFetcher.java:26` — Narrative comment block (cross-reference commentary)
  > L26: `/** Closes the underlying client. Called from operator close (mailbox thread). */`
- [-] **AS-206** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/N7RangeBreakoutStrategy.java:15` — Narrative comment block (JSDoc preamble with slop signal)
  > L15: `/**`
- [-] **AS-207** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/RawTableColumns.java:48` — Narrative comment block (decorative separator)
  > L48: `// --- v4: full-mode field capture (indexes 21-71, all BIGINT NULL) ---`
- [-] **AS-208** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SourceIdleWatchdogGenerator.java:145` — Narrative comment block (cross-reference commentary)
  > L145: `/**`
- [-] **AS-209** `code/02_services/04_executor/src/bridge/transport.rs:39` — Narrative comment block (decorative separator)
  > L39: `// --- RFC 6455 opcodes (subset we speak: text, ping, pong, close). ---`
- [-] **AS-210** `code/02_services/04_executor/src/bridge/transport.rs:289` — Narrative comment block (decorative separator)
  > L289: `// --- WebSocket report intake (minimal RFC 6455 client) ------------------------`
- [-] **AS-211** `code/02_services/04_executor/src/engine.rs:847` — Narrative comment block (decorative separator)
  > L847: `// ---------- P3-439: the boot gate is observed from the client, not a constant ----------`
- [-] **AS-212** `code/02_services/04_executor/src/engine.rs:892` — Narrative comment block (decorative separator)
  > L892: `// ---------- P3-194 / P3-440: reconcile transport failures and the mass snapshot ----------`
- [-] **AS-213** `code/02_services/04_executor/src/executiongate.rs:1096` — Narrative comment block (decorative separator)
  > L1096: `// --------------------------------------------------------------------------`
- [-] **AS-214** `code/02_services/04_executor/src/executiongate.rs:1203` — Narrative comment block (decorative separator)
  > L1203: `// --------------------------------------------------------------------------`
- [-] **AS-215** `code/02_services/04_executor/src/executiongate.rs:1297` — Narrative comment block (decorative separator)
  > L1297: `// --------------------------------------------------------------------------`
- [-] **AS-216** `code/02_services/04_executor/src/executiongate.rs:1336` — Narrative comment block (decorative separator)
  > L1336: `// --------------------------------------------------------------------------`
- [-] **AS-217** `code/02_services/04_executor/src/executiongate.rs:1394` — Narrative comment block (decorative separator)
  > L1394: `// --------------------------------------------------------------------------`
- [-] **AS-218** `code/02_services/04_executor/src/executiongate.rs:1430` — Narrative comment block (decorative separator)
  > L1430: `// --------------------------------------------------------------------------`
- [-] **AS-219** `code/02_services/04_executor/src/executiongate.rs:1466` — Narrative comment block (decorative separator)
  > L1466: `// --------------------------------------------------------------------------`
- [-] **AS-220** `code/02_services/04_executor/src/executiongate.rs:1503` — Narrative comment block (decorative separator)
  > L1503: `// --------------------------------------------------------------------------`
- [-] **AS-221** `code/02_services/04_executor/src/executiongate.rs:1540` — Narrative comment block (decorative separator)
  > L1540: `// --------------------------------------------------------------------------`
- [-] **AS-222** `code/02_services/04_executor/src/gateway_protocol.rs:571` — Narrative comment block (decorative separator)
  > L571: `// -------------------------------------------------------------------------`
- [-] **AS-223** `code/02_services/04_executor/src/http.rs:2215` — Narrative comment block (decorative separator)
  > L2215: `// ---- DEC-044 approval / safety-halt surface (A2.2/T9) ----`
- [-] **AS-224** `code/02_services/04_executor/src/projection/mod.rs:1134` — Narrative comment block (decorative separator)
  > L1134: `// --------------------------------------------------------------------------`
- [-] **AS-225** `code/02_services/04_executor/src/projection/mod.rs:1330` — Narrative comment block (decorative separator)
  > L1330: `// --------------------------------------------------------------------------`
- [-] **AS-226** `code/02_services/04_executor/src/projection/mod.rs:1377` — Narrative comment block (decorative separator)
  > L1377: `// --------------------------------------------------------------------------`
- [-] **AS-227** `code/02_services/04_executor/src/projection/mod.rs:1393` — Narrative comment block (decorative separator)
  > L1393: `// --------------------------------------------------------------------------`
- [-] **AS-228** `code/02_services/04_executor/src/projection/mod.rs:1436` — Narrative comment block (decorative separator)
  > L1436: `// --------------------------------------------------------------------------`
- [-] **AS-229** `code/02_services/04_executor/src/projection/mod.rs:1471` — Narrative comment block (decorative separator)
  > L1471: `// --------------------------------------------------------------------------`
- [-] **AS-230** `code/02_services/04_executor/src/projection/mod.rs:1510` — Narrative comment block (decorative separator)
  > L1510: `// --- P3-166: re-entry mints a new cycle, but ONLY for a genuinely newer event ---`
- [-] **AS-231** `code/02_services/04_executor/src/projection/mod.rs:1573` — Narrative comment block (decorative separator)
  > L1573: `// --- P3-390 / P3-392 / P3-394: numeric boundaries, Java-parity ---`
- [-] **AS-232** `code/02_services/04_executor/src/projection/mod.rs:1575` — Narrative comment block (decorative separator)
  > L1575: `// --- P3-213: accumulation overflow fails closed (the Rust mirror of P3-394) ---`
- [-] **AS-233** `code/02_services/04_executor/src/resilience.rs:396` — Narrative comment block (decorative separator)
  > L396: `// --- RESILIENCE-002: exponential backoff correctness ---`
- [-] **AS-234** `code/02_services/04_executor/src/resilience.rs:408` — Narrative comment block (decorative separator)
  > L408: `// --- RESILIENCE-003: retry budget exhaustion ---`
- [-] **AS-235** `code/02_services/04_executor/src/resilience.rs:420` — Narrative comment block (decorative separator)
  > L420: `// --- RESILIENCE-004: circuit breaker opens after threshold ---`
- [-] **AS-236** `code/02_services/04_executor/src/resilience.rs:433` — Narrative comment block (decorative separator)
  > L433: `// --- RESILIENCE-005: recovery after dependency returns (half-open probe closes) ---`
- [-] **AS-237** `code/02_services/04_executor/src/resilience.rs:456` — Narrative comment block (decorative separator)
  > L456: `// --- RESILIENCE-007: duplicate retry prevention (never re-invoke a done key) ---`
- [-] **AS-238** `code/02_services/04_executor/src/resilience.rs:486` — Narrative comment block (decorative separator)
  > L486: `// --- RESILIENCE-003 (orchestrator): transient failures retried until success ---`
- [-] **AS-239** `code/02_services/04_executor/src/resilience.rs:512` — Narrative comment block (decorative separator)
  > L512: `// --- RESILIENCE-001/003: storm containment — bounded attempts, then exhausted ---`
- [-] **AS-240** `code/02_services/04_executor/src/resilience.rs:535` — Narrative comment block (decorative separator)
  > L535: `// --- RESILIENCE-004 (orchestrator): breaker short-circuits further calls ---`
- [-] **AS-241** `code/02_services/04_executor/src/resilience.rs:564` — Narrative comment block (decorative separator)
  > L564: `// --- async live-path retry: transient err -> retry until success, bounded ---`
- [-] **AS-242** `code/02_services/04_executor/src/resilience.rs:598` — Narrative comment block (decorative separator)
  > L598: `// --- async live-path retry: budget exhaustion surfaces, never retries forever ---`
- [-] **AS-243** `code/02_services/04_executor/src/resilience.rs:619` — Narrative comment block (decorative separator)
  > L619: `// --- P3-022: the budget bounds one call, not the orchestrator's lifetime ---`
- [-] **AS-244** `code/02_services/04_executor/src/resilience.rs:644` — Narrative comment block (decorative separator)
  > L644: `// --- P3-022: a breaker short-circuit must not consume budget ---`
- [-] **AS-245** `code/02_services/04_executor/src/resilience.rs:663` — Narrative comment block (decorative separator)
  > L663: `// --- P3-023: the backoff delay is actually applied between attempts ---`
- [-] **AS-246** `code/02_services/04_executor/src/resilience.rs:689` — Narrative comment block (decorative separator)
  > L689: `// --- P3-023: ...on the LIVE async bridge path too ---`
- [-] **AS-247** `code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java:378` — Narrative comment block (decorative separator)
  > L378: `// --- Main entry point ---`
- [-] **AS-248** `code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/SyntheticWorkload.java:9` — Narrative comment block (cross-reference commentary)
  > L9: `/** Deterministic, per-instrument variable-arrival workload used by benchmarks. */`
- [-] **AS-249** `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:52` — Narrative comment block (decorative separator)
  > L52: `// --- 1) Direct key checks (attemptId as key) ---`
- [-] **AS-250** `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:90` — Narrative comment block (decorative separator)
  > L90: `// --- 2) Direct key checks (brokerOrderId as key -> attemptId) ---`
- [-] **AS-251** `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:105` — Narrative comment block (decorative separator)
  > L105: `// --- 3) Direct key checks (clientRef as key -> attemptId) ---`
- [-] **AS-252** `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:123` — Narrative comment block (decorative separator)
  > L123: `// --- 4) Value scans: brokerOrderId as value (attempt -> broker) ---`
- [-] **AS-253** `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:138` — Narrative comment block (decorative separator)
  > L138: `// --- 5) Value scans: attemptId as value (broker -> attempt) ---`
- [-] **AS-254** `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java:152` — Narrative comment block (decorative separator)
  > L152: `// --- 6) Value scans: clientRef as value ---`
- [-] **AS-255** `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/SafetyHaltTailProcessor.java:10` — Narrative comment block (JSDoc preamble with slop signal)
  > L10: `/**`
- [-] **AS-256** `code/common/src/main/java/com/trading/common/config/ContainerMemoryGuard.java:45` — Narrative comment block (decorative separator)
  > L45: `// ---- env-tunable percentages (P3, 2026-08-29) ----`
- [-] **AS-257** `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:26` — Narrative comment block (decorative separator)
  > L26: `// ---- ingestion / workload profile ----`
- [-] **AS-258** `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:40` — Narrative comment block (decorative separator)
  > L40: `// ---- raw_table_1 schema contract ----`
- [-] **AS-259** `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:54` — Narrative comment block (decorative separator)
  > L54: `// ---- dedup / candles (reject-startup values) ----`
- [-] **AS-260** `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:68` — Narrative comment block (decorative separator)
  > L68: `// ---- checkpointing ----`
- [-] **AS-261** `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:73` — Narrative comment block (decorative separator)
  > L73: `// ---- fixed-delay restart strategy (CHECKPOINT_RESTART_STRATEGY) ----`
- [-] **AS-262** `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:87` — Narrative comment block (decorative separator)
  > L87: `// ---- sink write-path (tracker 14 box 682/116, 2026-08-12; CHG-023 item 4, 2026-08-17) ----`
- [-] **AS-263** `code/common/src/main/java/com/trading/common/config/PlatformConfig.java:113` — Narrative comment block (decorative separator)
  > L113: `// ---- JVM / container memory ----`
- [-] **AS-264** `code/common/src/main/java/com/trading/common/model/AttemptPhase.java:45` — Narrative comment block (JSDoc preamble with slop signal)
  > L45: `/**`
- [-] **AS-265** `code/common/src/main/java/com/trading/common/model/GateTransitionValidator.java:233` — Narrative comment block (decorative separator)
  > L233: `// ---- result types ----`
- [-] **AS-266** `code/common/src/main/java/com/trading/common/observability/Json.java:44` — Narrative comment block (decorative separator)
  > L44: `// ---- object members: every member carries a key ----`
- [-] **AS-267** `code/common/src/main/java/com/trading/common/observability/Json.java:75` — Narrative comment block (decorative separator)
  > L75: `// ---- array elements: no key ----`
- [-] **AS-268** `code/common/src/main/java/com/trading/common/schema/RawTableSchema.java:98` — Narrative comment block (decorative separator)
  > L98: `// --- v4: full-mode field capture (appended AFTER schema_version; 0-20 never move) ---`
- [-] **AS-269** `code/common/src/main/java/com/trading/common/schema/RawTableSchema.java:187` — Narrative comment block (decorative separator)
  > L187: `// --- v4: every added column is BIGINT NULL ---`
- [x] **AS-270** `code/common/src/main/java/com/trading/common/schema/eod/EncryptedExportEodOffloadExecutor.java:17` — Narrative comment block (JSDoc preamble with slop signal)
  > L17: `/**`
- [-] **AS-271** `code/common/src/main/java/com/trading/common/schema/execution/GateRow.java:39` — Narrative comment block (JSDoc preamble with slop signal)
  > L39: `/**`
- [-] **AS-272** `code/common/src/main/java/com/trading/common/schema/position/PositionProjector.java:37` — Narrative comment block (cross-reference commentary)
  > L37: `/** Applied — the returned snapshot replaces the current one. */`
- [-] **AS-273** `code/common/src/main/java/com/trading/common/schema/projection/OrderLifecycleProjector.java:79` — Narrative comment block (decorative separator)
  > L79: `// --- Quantity sanity (impossible quantity) ---------------------------`
- [-] **AS-274** `code/common/src/main/java/com/trading/common/schema/projection/OrderLifecycleProjector.java:124` — Narrative comment block (decorative separator)
  > L124: `// --- Source-version gate (SCH-09 KvStateUpdateProtocol semantics) ----`
- [-] **AS-275** `code/common/src/main/java/com/trading/common/schema/projection/OrderLifecycleProjector.java:141` — Narrative comment block (decorative separator)
  > L141: `// --- Terminal regression check ---------------------------------------`
- [-] **AS-276** `code/common/src/main/java/com/trading/common/schema/projection/PostbackFingerprint.java:63` — Narrative comment block (cross-reference commentary) · full-audit correction: XC-41 fixed in CHG-539 (`d23926d4`); style remains flagged
  > L63: `/** Field-level canonical part builder (used by tests to mint valid fixtures). */`
- [-] **AS-277** `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:83` — Narrative comment block (decorative separator)
  > L83: `// --- Fingerprint integrity ------------------------------------------`
- [-] **AS-278** `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:89` — Narrative comment block (decorative separator)
  > L89: `// --- Idempotent ledger resume / duplicate ----------------------------`
- [-] **AS-279** `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:105` — Narrative comment block (decorative separator)
  > L105: `// --- Correlation precedence -----------------------------------------`
- [-] **AS-280** `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:114` — Narrative comment block (decorative separator)
  > L114: `// --- Audit (immutable evidence) -------------------------------------`
- [-] **AS-281** `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:122` — Narrative comment block (decorative separator)
  > L122: `// --- Lifecycle monotonicity -----------------------------------------`
- [-] **AS-282** `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:142` — Narrative comment block (decorative separator)
  > L142: `// --- Position (serialize Nautilus-computed result, no arithmetic) ---`
- [-] **AS-283** `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:192` — Narrative comment block (decorative separator)
  > L192: `// --- helpers -------------------------------------------------------------`
- [x] **AS-284** `code/common/src/main/java/com/trading/common/version/VersionGate.java:55` — Narrative comment block (cross-reference commentary)
  > L55: `/** Convenience: fail unless every matrix entry is pinned (used by CI before building images). */`

#### `ai-slop/python-broad-except` — WARNING × 1

- Engine `ai-slop` · category `AI Slop` · assessment: **AI-slop indicators** (1 medium) · fixable: no (0)
- Score impact tier: `standard` (multiplier 1)
- What it means: `except Exception: pass`-style handler that silently drops every exception.
- Tool advice: Either narrow the exception class (`except ValueError:`), log the error, or re-raise. If you genuinely intend to swallow, add a comment naming the specific failure mode you're handling — auditors will thank you.

- [-] **AS-285** `code/01_platform/04_scripts/cp_phase_capture.py:181` — `except Exception: pass` silently drops every exception. Failures vanish without a trace. · **won't-fix** (G3 verified 2026-10-03 — see tracker)
  > L181: `except Exception:`

#### `ai-slop/python-chained-dict-get` — WARNING × 21

- Engine `ai-slop` · category `AI Slop` · assessment: **AI-slop indicators** (21 medium) · fixable: no (0)
- Score impact tier: `maintainability` (multiplier 0.75, rule cap 24)
- What it means: `.get(..., {}).get(...)` fallback chain that hides missing-data cases.
- Tool advice: Normalize the input at the boundary, use a typed object, or split the lookup into explicit steps. Empty-dict fallback chains are a common agent shortcut that becomes brittle as schemas evolve.

- [x] **AS-286** `code/01_platform/04_scripts/audit_r2.py:474` — Chained `.get(..., {})` defaults hide missing-data cases. · **fixed** in `c7877ad4` (CHG-531)
  > L474: `rules = list(body.get("result", {}).get("rules", []) or [])`
- [x] **AS-287** `code/01_platform/04_scripts/audit_r2.py:540` — Chained `.get(..., {})` defaults hide missing-data cases. · **fixed** in `c7877ad4` (CHG-531)
  > L540: `rules = body.get("result", {}).get("rules", [])`
- [f] **AS-288** `code/01_platform/04_scripts/cluster_check.py:159` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L159: `return (spec.get("TaskTemplate") or {}).get("Placement", {}).get("MaxReplicas")`
- [f] **AS-289** `code/01_platform/04_scripts/day_run.py:426` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L426: `if s in running and facts.services.get(s, {}).get("health") == "unhealthy"]`
- [f] **AS-290** `code/01_platform/04_scripts/day_run.py:982` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L982: `ingestion_running = facts.services.get("ingestion", {}).get("state") == "running"`
- [f] **AS-291** `code/01_platform/04_scripts/day_run.py:995` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L995: `if want_samples and facts.fluss.get("raw", {}).get("ok"):`
- [f] **AS-292** `code/01_platform/04_scripts/day_run.py:1001` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L1001: `if second.get("ok") and facts.fluss.get(key, {}).get("ok"):`
- [f] **AS-293** `code/01_platform/04_scripts/day_run.py:1102` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L1102: `fluss_ok = facts.fluss.get("raw", {}).get("ok")`
- [f] **AS-294** `code/01_platform/04_scripts/day_run.py:1138` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L1138: `if cps.get(signals[0]["id"], {}).get("latest_completed_ms"):`
- [f] **AS-295** `code/01_platform/04_scripts/holistic-analyze.py:1171` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L1171: `dp = (m.get("histogram", {}).get("dataPoints") or [{}])[0]`
- [f] **AS-296** `code/01_platform/04_scripts/local_int_004_smoke.py:38` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L38: `net=cfg.get("networks",{}).get("execution-net",{})`
- [f] **AS-297** `code/01_platform/04_scripts/local_int_004_smoke.py:42` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L42: `svc=cfg.get("services",{}).get(name)`
- [f] **AS-298** `code/01_platform/04_scripts/o2-provision.py:1353` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L1353: `cur = s.get("settings", {}).get("data_retention", 0)`
- [f] **AS-299** `code/01_platform/04_scripts/perf_evidence_parse.py:374` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L374: `("ckpt_completed", lambda d, n: d["checkpoints"].get("counts", {}).get("completed", "")),`
- [f] **AS-300** `code/01_platform/04_scripts/perf_evidence_parse.py:375` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L375: `("ckpt_latest_size", lambda d, n: d["checkpoints"].get("latest_completed", {}).get("checkpointed_size", "")),`
- [f] **AS-301** `code/01_platform/04_scripts/perf_evidence_parse.py:376` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L376: `("ckpt_duration_ms", lambda d, n: d["checkpoints"].get("latest_completed", {}).get("end_to_end_duration", "")),`
- [-] **AS-302** `code/01_platform/04_scripts/seed_dashboards.py:191` — Chained `.get(..., {})` defaults hide missing-data cases. · **won't-fix** (G4 verified 2026-10-03 — see tracker)
  > L191: `dash_id = existing.get(title, {}).get("dashboard_id")`
- [x] **AS-303** `code/01_platform/04_scripts/soak-o2-evidence.py:137` — Chained `.get(..., {})` defaults hide missing-data cases. · **fixed** in `c7877ad4` (CHG-531)
  > L137: `result = payload.get("data", {}).get("result", [])`
- [f] **AS-304** `code/01_platform/04_scripts/t9_order_sandbox.py:385` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L385: `net = _compose_json().get("networks", {}).get("execution-net", {})`
- [f] **AS-305** `code/01_platform/04_scripts/t9_order_sandbox.py:474` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L474: `net = cfg.get("networks", {}).get("execution-net", {})`
- [f] **AS-306** `code/01_platform/06_stage_profiler/stage_profiler.py:599` — Chained `.get(..., {})` defaults hide missing-data cases. · **false positive** (G4 verified 2026-10-03 — see tracker)
  > L599: `table.get(task, {}).get(quantile, float("-inf")), s.value`

#### `ai-slop/python-repetitive-dispatch` — WARNING × 7

- Engine `ai-slop` · category `AI Slop` · assessment: **AI-slop indicators** (7 medium) · fixable: no (0)
- Score impact tier: `maintainability` (multiplier 0.75, rule cap 24)
- What it means: Repeated equality branch ladder that should be a table/handler map.
- Tool advice: Use a table, set membership, or handler map when branches share the same shape. SlopCodeBench highlights these selector ladders as code that keeps growing instead of absorbing new cases cleanly.

- [f] **AS-307** `code/01_platform/04_scripts/day_run.py:1150` — 4 repeated branches dispatch on `decision.action`. · **false positive** (G5 verified 2026-10-03 — see tracker)
  > L1150: `if decision.action == "keep":`
- [f] **AS-308** `code/01_platform/04_scripts/implementation_gate.py:300` — 4 repeated branches dispatch on `t`. · **false positive** (G5 verified 2026-10-03 — see tracker)
  > L300: `if t == "run":`
- [f] **AS-309** `code/01_platform/04_scripts/r2_legal_hold_check.py:117` — 5 repeated branches dispatch on `key`. · **false positive** (G5 verified 2026-10-03 — see tracker)
  > L117: `if key == "date":`
- [f] **AS-310** `code/01_platform/04_scripts/r2_legal_hold_check.py:119` — 4 repeated branches dispatch on `key`. · **false positive** (G5 verified 2026-10-03 — see tracker)
  > L119: `elif key == "table":`
- [f] **AS-311** `code/01_platform/04_scripts/stage_capture_parse.py:165` — 5 repeated branches dispatch on `name`. · **false positive** (G5 verified 2026-10-03 — see tracker)
  > L165: `if name == "busyTimeMsPerSecond":`
- [f] **AS-312** `code/01_platform/04_scripts/stage_capture_parse.py:167` — 4 repeated branches dispatch on `name`. · **false positive** (G5 verified 2026-10-03 — see tracker)
  > L167: `elif name == "backPressuredTimeMsPerSecond":`
- [f] **AS-313** `code/01_platform/06_stage_profiler/stage_profiler.py:1550` — 4 repeated branches dispatch on `args.cmd`. · **false positive** (G5 verified 2026-10-03 — see tracker)
  > L1550: `if args.cmd == "stages":`

#### `ai-slop/rust-non-test-unwrap` — WARNING × 6

- Engine `ai-slop` · category `AI Slop` · assessment: **AI-slop indicators** (6 medium) · fixable: no (0)
- Score impact tier: `strict` (multiplier 1)
- What it means: `.unwrap()` in non-test Rust code can panic on None/Err.
- Tool advice: Use `?` to propagate, `.expect("context")` if you really mean it (and the message names the invariant), or pattern-match the variant you care about. Reserve raw `.unwrap()` for tests and prototypes.

- [f] **AS-314** `code/02_services/04_executor/src/bin/t9_paper_25.rs:118` — `.unwrap()` in non-test code panics on None/Err. Surfaces as a hard crash for the caller. · **false positive** (G6 verified 2026-10-03 — see tracker)
  > L118: `.unwrap()`
- [f] **AS-315** `code/02_services/04_executor/src/bin/t9_paper_25.rs:137` — `.unwrap()` in non-test code panics on None/Err. Surfaces as a hard crash for the caller. · **false positive** (G6 verified 2026-10-03 — see tracker)
  > L137: `evidence["evidence_hash"].as_str().unwrap(),`
- [f] **AS-316** `code/02_services/04_executor/src/bin/t9_paper_25_full.rs:109` — `.unwrap()` in non-test code panics on None/Err. Surfaces as a hard crash for the caller. · **false positive** (G6 verified 2026-10-03 — see tracker)
  > L109: `.unwrap()`
- [f] **AS-317** `code/02_services/04_executor/src/bin/t9_paper_25_full.rs:122` — `.unwrap()` in non-test code panics on None/Err. Surfaces as a hard crash for the caller. · **false positive** (G6 verified 2026-10-03 — see tracker)
  > L122: `evidence["evidence_hash"].as_str().unwrap(),`
- [f] **AS-318** `code/02_services/04_executor/src/durable.rs:519` — `.unwrap()` in non-test code panics on None/Err. Surfaces as a hard crash for the caller. · **false positive** (G6 verified 2026-10-03 — see tracker)
  > L519: `std::fs::create_dir_all(&dir).unwrap();`
- [f] **AS-319** `code/02_services/04_executor/src/durable_file.rs:587` — `.unwrap()` in non-test code panics on None/Err. Surfaces as a hard crash for the caller. · **false positive** (G6 verified 2026-10-03 — see tracker)
  > L587: `std::fs::create_dir_all(&dir).unwrap();`

#### `ai-slop/silent-recovery` — WARNING × 8

- Engine `ai-slop` · category `AI Slop` · assessment: **AI-slop indicators** (8 medium) · fixable: no (0)
- Score impact tier: `strict` (multiplier 1)
- What it means: except/catch logs without the caught error and continues; the failure cause is lost.
- Tool advice: Include the caught error in the log, or re-raise / recover explicitly, so the failure stays diagnosable.

- [x] **AS-320** `code/01_platform/04_scripts/holistic-analyze.py:961` — except logs without the caught error then continues; the failure cause is lost · **fixed** in `966a8c8e` (CHG-530)
  > L961: `except OSError:`
- [x] **AS-321** `code/01_platform/04_scripts/holistic-analyze.py:1282` — except logs without the caught error then continues; the failure cause is lost · **fixed** in `966a8c8e` (CHG-530)
  > L1282: `except OSError:`
- [x] **AS-322** `code/01_platform/04_scripts/holistic-analyze.py:1323` — except logs without the caught error then continues; the failure cause is lost · **fixed** in `966a8c8e` (CHG-530)
  > L1323: `except OSError:`
- [x] **AS-323** `code/01_platform/04_scripts/holistic-analyze.py:1374` — except logs without the caught error then continues; the failure cause is lost · **fixed** in `966a8c8e` (CHG-530)
  > L1374: `except OSError:`
- [x] **AS-324** `code/01_platform/04_scripts/holistic-analyze.py:1425` — except logs without the caught error then continues; the failure cause is lost · **fixed** in `966a8c8e` (CHG-530)
  > L1425: `except OSError:`
- [x] **AS-325** `code/01_platform/04_scripts/holistic-analyze.py:1465` — except logs without the caught error then continues; the failure cause is lost · **fixed** in `966a8c8e` (CHG-530)
  > L1465: `except OSError:`
- [f] **AS-326** `code/01_platform/04_scripts/t8_sandbox_contract_check.py:47` — except logs without the caught error then continues; the failure cause is lost · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L47: `except ImportError:  # pragma: no cover`
- [f] **AS-327** `code/01_platform/05_instruments/split_manifest.py:53` — except logs without the caught error then continues; the failure cause is lost · **false positive** (G3 verified 2026-10-03 — see tracker)
  > L53: `except (ValueError, TypeError):`

#### `ai-slop/thin-wrapper` — WARNING × 1

- Engine `ai-slop` · category `AI Slop` · assessment: **AI-slop indicators** (1 medium) · fixable: no (0)
- Score impact tier: `maintainability` (multiplier 0.75, rule cap 24)
- What it means: Function that only forwards its parameters to another call.
- Tool advice: Consider calling the inner function directly instead of wrapping it

- [f] **AS-328** `code/01_platform/04_scripts/t9_order_sandbox.py:243` — Function 'payload_hash' is a thin wrapper that only calls another function · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L243: `def payload_hash(payload_json):`

#### `ai-slop/unused-import` — WARNING × 9

- Engine `ai-slop` · category `AI Slop` · assessment: **AI-slop indicators** (9 medium) · fixable: yes (9)
- Score impact tier: `mechanical` (multiplier 0.5, rule cap 16)
- What it means: Imported symbol is never used.
- Tool advice: Remove unused imports to keep the code clean

- [x] **AS-329** `code/01_platform/04_scripts/deploy_preflight.py:24` — Imported symbol 'shlex' is never used · **fixed** in `68fb9823` (CHG-529) (side effect of AS-353)
  > L24: `import shlex`
- [f] **AS-330** `code/01_platform/04_scripts/holistic-analyze.py:38` — Imported symbol 'org' is never used · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L38: `import org.apache.fluss.client.Connection;`
- [f] **AS-331** `code/01_platform/04_scripts/holistic-analyze.py:39` — Imported symbol 'org' is never used · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L39: `import org.apache.fluss.client.ConnectionFactory;`
- [f] **AS-332** `code/01_platform/04_scripts/holistic-analyze.py:40` — Imported symbol 'org' is never used · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L40: `import org.apache.fluss.client.admin.Admin;`
- [f] **AS-333** `code/01_platform/04_scripts/holistic-analyze.py:41` — Imported symbol 'org' is never used · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L41: `import org.apache.fluss.client.table.Table;`
- [f] **AS-334** `code/01_platform/04_scripts/holistic-analyze.py:42` — Imported symbol 'org' is never used · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L42: `import org.apache.fluss.client.table.scanner.log.LogScanner;`
- [f] **AS-335** `code/01_platform/04_scripts/holistic-analyze.py:43` — Imported symbol 'org' is never used · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L43: `import org.apache.fluss.client.table.scanner.log.ScanRecords;`
- [f] **AS-336** `code/01_platform/04_scripts/holistic-analyze.py:44` — Imported symbol 'org' is never used · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L44: `import org.apache.fluss.config.Configuration;`
- [f] **AS-337** `code/01_platform/04_scripts/holistic-analyze.py:45` — Imported symbol 'org' is never used · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L45: `import org.apache.fluss.metadata.TablePath;`

#### `ai-slop/todo-stub` — INFO × 5

- Engine `ai-slop` · category `AI Slop` · assessment: **AI-slop indicators** (5 medium) · fixable: no (0)
- Score impact tier: `standard` (multiplier 1)
- What it means: Unresolved TODO/FIXME/HACK comment.
- Tool advice: Resolve the TODO or create a tracked issue for it

- [f] **AS-338** `code/common/src/main/java/com/trading/common/schema/fluss/BoundedRetry.java:14` — Unresolved TODO/FIXME/HACK comment indicates incomplete code · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L14: `* {@code ServerConnection} TODO at the inflight-request send site). Consequences,`
- [f] **AS-339** `code/common/src/main/java/com/trading/common/schema/fluss/FlussWriteProfiles.java:17` — Unresolved TODO/FIXME/HACK comment indicates incomplete code · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L17: `* {@code // TODO add the wakeup logic refer to Kafka} (WriterClient.java:208-214).`
- [f] **AS-340** `code/common/src/main/java/com/trading/common/schema/fluss/FlussWriteProfiles.java:41` — Unresolved TODO/FIXME/HACK comment indicates incomplete code · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L41: `* sender's own TODO documents the busy-loop hazard there ("The method sendWriteData is in a busy loop.`
- [f] **AS-341** `code/common/src/main/java/com/trading/common/schema/fluss/FlussWriteProfiles.java:75` — Unresolved TODO/FIXME/HACK comment indicates incomplete code · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L75: `* (RecordAccumulator.java:114-116, "TODO add deliveryTimeoutMs to report success or failure on record`
- [f] **AS-342** `code/common/src/main/java/com/trading/common/schema/fluss/WriteAwait.java:7` — Unresolved TODO/FIXME/HACK comment indicates incomplete code · **false positive** (G7/G8/G9 verified 2026-10-03 — see tracker)
  > L7: `* Sender.sendWriteData carries the authors' own TODO ("The method sendWriteData`

### Engine: Linting (ruff)

#### `ruff/E401` — ERROR × 2

- Engine `lint` · category `Python Lint` · assessment: **confirmed defects** (2 high) · fixable: yes (2)
- Score impact tier: `standard` (multiplier 1)
- What it means: Multiple imports on one line (ruff E401).

- [x] **AS-343** `code/01_platform/04_scripts/local_int_004_smoke.py:16` — Multiple imports on one line · **fixed** in `68fb9823` (CHG-529)
  > L16: `import argparse, json, subprocess, sys`
- [x] **AS-344** `code/01_platform/05_instruments/split_manifest.py:13` — Multiple imports on one line · **fixed** in `68fb9823` (CHG-529)
  > L13: `import argparse, csv, pathlib, sys`

#### `ruff/E701` — ERROR × 1

- Engine `lint` · category `Python Lint` · assessment: **confirmed defects** (1 high) · fixable: no (0)
- Score impact tier: `standard` (multiplier 1)
- What it means: Multiple statements on one line (colon) (ruff E701).

- [x] **AS-345** `code/01_platform/04_scripts/local_int_004_smoke.py:26` — Multiple statements on one line (colon) · **fixed** in `68fb9823` (CHG-529)
  > L26: `if profile: cmd += ["--profile", profile]`

#### `ruff/E702` — ERROR × 2

- Engine `lint` · category `Python Lint` · assessment: **confirmed defects** (2 high) · fixable: no (0)
- Score impact tier: `standard` (multiplier 1)
- What it means: Multiple statements on one line (semicolon) (ruff E702).

- [x] **AS-346** `code/01_platform/05_instruments/split_manifest.py:33` — Multiple statements on one line (semicolon) · **fixed** in `68fb9823` (CHG-529)
  > L33: `print(f"input not found: {inp}", file=sys.stderr); sys.exit(2)`
- [x] **AS-347** `code/01_platform/05_instruments/split_manifest.py:39` — Multiple statements on one line (semicolon) · **fixed** in `68fb9823` (CHG-529)
  > L39: `print("empty CSV", file=sys.stderr); sys.exit(2)`

#### `ruff/E731` — ERROR × 1

- Engine `lint` · category `Python Lint` · assessment: **confirmed defects** (1 high) · fixable: no (0)
- Score impact tier: `standard` (multiplier 1)
- What it means: Lambda assigned to a name (ruff E731).

- [x] **AS-348** `code/01_platform/04_scripts/o2-provision.py:1813` — Do not assign a `lambda` expression, use a `def` · **fixed** in `68fb9823` (CHG-529)
  > L1813: `v2api = lambda method, body=None: api_raw(`

#### `ruff/E741` — ERROR × 3

- Engine `lint` · category `Python Lint` · assessment: **confirmed defects** (3 high) · fixable: no (0)
- Score impact tier: `standard` (multiplier 1)
- What it means: Ambiguous variable name (ruff E741).

- [x] **AS-349** `code/01_platform/04_scripts/check_flink_properties.py:87` — Ambiguous variable name: `l` · **fixed** in `68fb9823` (CHG-529)
  > L87: `return [l.strip() for l in m.group(1).split("\n") if l.strip()]`
- [x] **AS-350** `code/01_platform/04_scripts/plan_tracker.py:35` — Ambiguous variable name: `l` · **fixed** in `68fb9823` (CHG-529)
  > L35: `return [m.group(1) for m in (MARKER_RE.match(l) for l in lines) if m]`
- [x] **AS-351** `code/01_platform/04_scripts/plan_tracker.py:58` — Ambiguous variable name: `l` · **fixed** in `68fb9823` (CHG-529)
  > L58: `head = next((n for n, l in enumerate(lines) if l.startswith("**Roll-up**")), None)`

#### `ruff/F401` — ERROR × 3

- Engine `lint` · category `Python Lint` · assessment: **confirmed defects** (3 high) · fixable: yes (3)
- Score impact tier: `standard` (multiplier 1)
- What it means: Unused import (ruff F401).

- [x] **AS-352** `code/01_platform/01_docker/alert-consumer.py:38` — `time` imported but unused · **fixed** in `68fb9823` (CHG-529)
  > L38: `import time`
- [x] **AS-353** `code/01_platform/04_scripts/deploy_preflight.py:24` — `shlex` imported but unused · **fixed** in `68fb9823` (CHG-529)
  > L24: `import shlex`
- [x] **AS-354** `code/01_platform/04_scripts/image_staleness_check.py:63` — `os` imported but unused · **fixed** in `68fb9823` (CHG-529)
  > L63: `import os`

#### `ruff/F541` — ERROR × 4

- Engine `lint` · category `Python Lint` · assessment: **confirmed defects** (4 high) · fixable: yes (4)
- Score impact tier: `standard` (multiplier 1)
- What it means: f-string without any placeholders (ruff F541).

- [x] **AS-355** `code/01_platform/04_scripts/alert-routing-selftest.py:93` — f-string without any placeholders · **fixed** in `68fb9823` (CHG-529)
  > L93: `f",data=sys.argv[1].encode(),headers={{'Content-Type':'application/json'}}"`
- [x] **AS-356** `code/01_platform/04_scripts/alert-routing-selftest.py:177` — f-string without any placeholders · **fixed** in `68fb9823` (CHG-529)
  > L177: `print(f"3. temp alert created (always-firing, silence=0)")`
- [x] **AS-357** `code/01_platform/04_scripts/fused_timeline.py:223` — f-string without any placeholders · **fixed** in `68fb9823` (CHG-529)
  > L223: `f'avg(rate(node_cpu_seconds_total{{mode="iowait"}}[30s])) * 100', "sum"),`
- [x] **AS-358** `code/01_platform/04_scripts/stale_table_kind_scan.py:454` — f-string without any placeholders · **fixed** in `68fb9823` (CHG-529)
  > L454: `f"no {RETIRED_TABLE} manifest entry (" + (f"PRESENT" if RETIRED_TABLE in entries else "absent") + ")"))`

#### `ruff/F821` — ERROR × 1

- Engine `lint` · category `Python Lint` · assessment: **confirmed defects** (1 high) · fixable: no (0)
- Score impact tier: `standard` (multiplier 1)
- What it means: Undefined name (ruff F821) — NameError if that path executes.

- [x] **AS-359** `code/01_platform/04_scripts/alert-routing-selftest.py:50` — Undefined name `NoReturn` · **fixed** in `68fb9823` (CHG-529)
  > L50: `def fail(msg: str) -> "NoReturn":  # type: ignore[valid-type]`

#### `ruff/F841` — ERROR × 5

- Engine `lint` · category `Python Lint` · assessment: **confirmed defects** (5 high) · fixable: yes (2)
- Score impact tier: `standard` (multiplier 1)
- What it means: Local variable assigned but never used (ruff F841).

- [x] **AS-360** `code/01_platform/04_scripts/day_run.py:759` — Local variable `exc` is assigned to but never used · **fixed** in `68fb9823` (CHG-529)
  > L759: `except subprocess.CalledProcessError as exc:`
- [x] **AS-361** `code/01_platform/04_scripts/env_facts.py:48` — Local variable `exc` is assigned to but never used · **fixed** in `68fb9823` (CHG-529)
  > L48: `except OSError as exc:`
- [x] **AS-362** `code/01_platform/04_scripts/holistic-analyze.py:866` — Local variable `candle_re` is assigned to but never used · **fixed** in `68fb9823` (CHG-529)
  > L866: `candle_re = re.compile(r"candle:(\d{13}):(\d{13})")`
- [x] **AS-363** `code/01_platform/04_scripts/holistic-analyze.py:1311` — Local variable `thi` is assigned to but never used · **fixed** in `68fb9823` (CHG-529)
  > L1311: `thi = [(t, (snaps[t].get("throttled_usec", 0))) for t in keys]`
- [x] **AS-364** `code/01_platform/04_scripts/holistic-analyze.py:1652` — Local variable `dup_ing_max` is assigned to but never used · **fixed** in `68fb9823` (CHG-529)
  > L1652: `dup_ing_max = 0`

### Engine: Code Quality (complexity)

#### `complexity/deep-nesting` — WARNING × 12

- Engine `code-quality` · category `Complexity` · assessment: **style/policy** (12 medium) · fixable: no (0)
- Score impact tier: `maintainability` (multiplier 0.75, rule cap 24)
- What it means: Control-flow nesting deeper than the limit (default 5).
- Tool advice: Consider using early returns or extracting nested logic

- [f] **AS-365** `code/01_platform/04_scripts/cp_phase_capture.py:208` — Function nested too deeply (max: 5) · **false positive** (G10 verified 2026-10-03 — see tracker)
  > L208: `def run(rest_url: str, job_id: str, out_dir: Path, duration: float,`
- [f] **AS-366** `code/01_platform/04_scripts/ddl_apply_smoke.py:352` — Function nested too deeply (max: 5) · **false positive** (G10 verified 2026-10-03 — see tracker)
  > L352: `def scenario(index, extra_env, expect_rc, expect_parts, expect_absent=(),`
- [f] **AS-367** `code/01_platform/04_scripts/fluss-probes/CandleFeaturesTableProbe.java:183` — Function nested too deeply (max: 5) · **false positive** (G10 verified 2026-10-03 — see tracker)
  > L183: `private static void tail(Connection conn, Admin admin, TablePath path, int rows)`
- [f] **AS-368** `code/01_platform/04_scripts/fluss-probes/CandleVerify.java:57` — Function nested too deeply (max: 5) · **false positive** (G10 verified 2026-10-03 — see tracker)
  > L57: `public static void main(String[] args) throws Exception {`
- [f] **AS-369** `code/01_platform/04_scripts/fluss-probes/FlussKvScanStrategy.java:151` — Function nested too deeply (max: 5) · **false positive** (G10 verified 2026-10-03 — see tracker)
  > L151: `private static void scan(Connection conn, Admin admin, String strategy, int rows) throws Exception {`
- [f] **AS-370** `code/01_platform/04_scripts/fluss-probes/FlussPrefixReader.java:92` — Function nested too deeply (max: 5) · **false positive** (G10 verified 2026-10-03 — see tracker)
  > L92: `static void run(String[] args) throws Exception {`
- [-] **AS-371** `code/01_platform/04_scripts/holistic-analyze.py:714` — Function nested too deeply (max: 5) · **accepted complexity** (G10 reclassified 2026-10-03 — see tracker) · full-audit correction: XC-33 fixed in CHG-538 (`81dbd763`); finding remains accepted
  > L714: `def main():`
- [-] **AS-372** `code/02_services/01_ingestion/go-bridge/faketool/main.go:37` — Function nested too deeply (max: 5) · **accepted complexity** (G10 reclassified 2026-10-03 — see tracker)
  > L37: `func main() {`
- [-] **AS-373** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:611` — Function nested too deeply (max: 5) · **accepted complexity** (G10 reclassified 2026-10-03 — see tracker)
  > L611: `public void runWithBridge(String bridgeBinary) {`
- [-] **AS-374** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java:699` — Function nested too deeply (max: 5) · **accepted complexity** (G10 reclassified 2026-10-03 — see tracker)
  > L699: `public void processElement(RowData tick, Context ctx, Collector<RowData> out) throws Exception {`
- [f] **AS-375** `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/FlussControlStateStore.java:99` — Function nested too deeply (max: 5) · **false positive** (G10 verified 2026-10-03 — see tracker)
  > L99: `public void replaySafetyHalts(Consumer<InternalRow> consumer) {`
- [-] **AS-376** `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:212` — Function nested too deeply (max: 5) · **accepted complexity** (G10 reclassified 2026-10-03 — see tracker)
  > L212: `static int run(String[] args) throws Exception {`

#### `complexity/file-too-large` — WARNING × 47

- Engine `code-quality` · category `Complexity` · assessment: **style/policy** (47 medium) · fixable: no (0)
- Score impact tier: `style` (multiplier 0.5, rule cap 8)
- What it means: File exceeds the configured size limit (default 400 lines).
- Tool advice: Consider splitting this file into smaller modules

- [-] **AS-377** `code/01_platform/04_scripts/audit_r2.py` — File too large (max: 400)
  - 728 lines on disk
- [-] **AS-378** `code/01_platform/04_scripts/cluster_check.py` — File too large (max: 400)
  - 693 lines on disk
- [-] **AS-379** `code/01_platform/04_scripts/day_run.py` — File too large (max: 400)
  - 1372 lines on disk
- [-] **AS-380** `code/01_platform/04_scripts/ddl_apply.py` — File too large (max: 400)
  - 858 lines on disk
- [-] **AS-381** `code/01_platform/04_scripts/ddl_apply_smoke.py` — File too large (max: 400)
  - 589 lines on disk
- [-] **AS-382** `code/01_platform/04_scripts/disaster_drills.py` — File too large (max: 400)
  - 715 lines on disk
- [-] **AS-383** `code/01_platform/04_scripts/docs_audit.py` — File too large (max: 400)
  - 1755 lines on disk
- [-] **AS-384** `code/01_platform/04_scripts/eod_schedule.py` — File too large (max: 400)
  - 442 lines on disk
- [-] **AS-385** `code/01_platform/04_scripts/fluss-probes/FeatureSpikeProbe.java` — File too large (max: 400)
  - 528 lines on disk
- [-] **AS-386** `code/01_platform/04_scripts/fluss-probes/FlussReadabilityProbe.java` — File too large (max: 400)
  - 761 lines on disk
- [-] **AS-387** `code/01_platform/04_scripts/fluss-probes/FlussSignalLatency.java` — File too large (max: 400)
  - 517 lines on disk
- [-] **AS-388** `code/01_platform/04_scripts/fluss-probes/SignalCandidatesViewer.java` — File too large (max: 400)
  - 463 lines on disk
- [-] **AS-389** `code/01_platform/04_scripts/gate_preflight.py` — File too large (max: 400)
  - 490 lines on disk
- [-] **AS-390** `code/01_platform/04_scripts/holistic-analyze.py` — File too large (max: 400)
  - 1874 lines on disk
- [-] **AS-391** `code/01_platform/04_scripts/image_staleness_check.py` — File too large (max: 400)
  - 634 lines on disk
- [-] **AS-392** `code/01_platform/04_scripts/o2-provision.py` — File too large (max: 400)
  - 1948 lines on disk
- [-] **AS-393** `code/01_platform/04_scripts/perf_evidence_parse.py` — File too large (max: 400)
  - 650 lines on disk
- [-] **AS-394** `code/01_platform/04_scripts/prod_node_check.py` — File too large (max: 400)
  - 489 lines on disk
- [-] **AS-395** `code/01_platform/04_scripts/r2_legal_hold_check.py` — File too large (max: 400)
  - 602 lines on disk
- [-] **AS-396** `code/01_platform/04_scripts/stage_capture_parse.py` — File too large (max: 400)
  - 691 lines on disk
- [-] **AS-397** `code/01_platform/04_scripts/stale_table_kind_scan.py` — File too large (max: 400)
  - 898 lines on disk
- [-] **AS-398** `code/01_platform/04_scripts/t9_order_sandbox.py` — File too large (max: 400)
  - 1295 lines on disk
- [-] **AS-399** `code/01_platform/04_scripts/values_at_rest_scan.py` — File too large (max: 400)
  - 476 lines on disk
- [-] **AS-400** `code/01_platform/06_stage_profiler/stage_profiler.py` — File too large (max: 400)
  - 1581 lines on disk
- [-] **AS-401** `code/02_services/01_ingestion/go-bridge/main.go` — File too large (max: 600)
  - 1085 lines on disk
- [f] **AS-402** `code/02_services/01_ingestion/go-bridge/marketdata/market_data.pb.go` — File too large (max: 600)
  - 991 lines on disk
- [-] **AS-403** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java` — File too large (max: 400)
  - 629 lines on disk
- [d] **AS-404** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java` — File too large (max: 400)
  - 2537 lines on disk
- [-] **AS-405** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java` — File too large (max: 400)
  - 737 lines on disk
- [-] **AS-406** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java` — File too large (max: 400)
  - 817 lines on disk
- [-] **AS-407** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/RawTickWriter.java` — File too large (max: 400)
  - 623 lines on disk
- [-] **AS-408** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java` — File too large (max: 400)
  - 1395 lines on disk
- [-] **AS-409** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/N7RangeBreakoutStrategy.java` — File too large (max: 400)
  - 496 lines on disk
- [-] **AS-410** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java` — File too large (max: 400)
  - 851 lines on disk
- [-] **AS-411** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java` — File too large (max: 400)
  - 1433 lines on disk
- [-] **AS-412** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/StrategyHostFunction.java` — File too large (max: 400)
  - 1130 lines on disk
- [f] **AS-413** `code/02_services/04_executor/src/bridge/transport.rs` — File too large (max: 1000)
  - 1521 lines on disk
- [-] **AS-414** `code/02_services/04_executor/src/execution/client.rs` — File too large (max: 1000)
  - 2595 lines on disk
- [f] **AS-415** `code/02_services/04_executor/src/executiongate.rs` — File too large (max: 1000)
  - 1606 lines on disk
- [-] **AS-416** `code/02_services/04_executor/src/http.rs` — File too large (max: 1000)
  - 3653 lines on disk
- [f] **AS-417** `code/02_services/04_executor/src/projection/mod.rs` — File too large (max: 1000)
  - 1696 lines on disk
- [-] **AS-418** `code/02_services/05_mock_arrow/src/main/java/com/trading/mockarrow/MockArrowServer.java` — File too large (max: 400)
  - 442 lines on disk
- [-] **AS-419** `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/FlussProjectionWriter.java` — File too large (max: 400)
  - 592 lines on disk
- [-] **AS-420** `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/GatewayHttpServer.java` — File too large (max: 400)
  - 575 lines on disk
- [-] **AS-421** `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java` — File too large (max: 400)
  - 1785 lines on disk
- [-] **AS-422** `code/common/src/main/java/com/trading/common/schema/eod/EodControllerTool.java` — File too large (max: 400)
  - 745 lines on disk
- [-] **AS-423** `code/common/src/main/java/com/trading/common/schema/execution/FlussGateStateStore.java` — File too large (max: 400)
  - 513 lines on disk

#### `complexity/function-too-long` — WARNING × 55

- Engine `code-quality` · category `Complexity` · assessment: **style/policy** (55 medium) · fixable: no (0)
- Score impact tier: `style` (multiplier 0.5, rule cap 8)
- What it means: Function exceeds the configured length limit (default 80 lines).
- Tool advice: Consider breaking this function into smaller pieces

- [-] **AS-424** `code/01_platform/04_scripts/alert-routing-selftest.py:110` — Function too long (max: 80)
  - main() L110–L217 (108 lines)
  > L110: `def main() -> int:`
- [-] **AS-425** `code/01_platform/04_scripts/cluster_check.py:561` — Function too long (max: 80)
  - self_check() L561–L689 (129 lines)
  > L561: `def self_check():`
- [-] **AS-426** `code/01_platform/04_scripts/day_run.py:418` — Function too long (max: 80)
  - evaluate() L418–L600 (183 lines)
  > L418: `def evaluate(facts: Facts) -> list:`
- [-] **AS-427** `code/01_platform/04_scripts/ddl_apply.py:721` — Function too long (max: 80)
  - main() L721–L854 (134 lines)
  > L721: `def main():`
- [-] **AS-428** `code/01_platform/04_scripts/ddl_apply_smoke.py:460` — Function too long (max: 80)
  - main() L460–L585 (126 lines)
  > L460: `def main():`
- [-] **AS-429** `code/01_platform/04_scripts/disaster_drills.py:453` — Function too long (max: 80)
  - drive() L453–L646 (194 lines)
  > L453: `def drive(d, suite_id, approve, out_dir, verbose, deadline=None):`
- [-] **AS-430** `code/01_platform/04_scripts/docs_audit.py:652` — Function too long (max: 80)
  - c9_dec039_invariants() L652–L776 (125 lines)
  > L652: `def c9_dec039_invariants():`
- [-] **AS-431** `code/01_platform/04_scripts/eod_schedule.py:321` — Function too long (max: 80)
  - main() L321–L438 (118 lines)
  > L321: `def main(argv: list[str] | None = None) -> int:`
- [-] **AS-432** `code/01_platform/04_scripts/fluss-probes/CandleVerify.java:57` — Function too long (max: 80)
  > L57: `public static void main(String[] args) throws Exception {`
- [-] **AS-433** `code/01_platform/04_scripts/fluss-probes/EventDayProbe.java:53` — Function too long (max: 80)
  > L53: `public static void main(String[] args) throws Exception {`
- [-] **AS-434** `code/01_platform/04_scripts/fluss-probes/FlussPrefixReader.java:92` — Function too long (max: 80)
  > L92: `static void run(String[] args) throws Exception {`
- [-] **AS-435** `code/01_platform/04_scripts/fluss-probes/RawCompressionProbe.java:153` — Function too long (max: 80)
  > L153: `static void write(Connection conn, Admin admin, long rows) throws Exception {`
- [-] **AS-436** `code/01_platform/04_scripts/fused_timeline.py:231` — Function too long (max: 80)
  - main() L231–L422 (192 lines)
  > L231: `def main():`
- [-] **AS-437** `code/01_platform/04_scripts/gate_preflight.py:357` — Function too long (max: 80)
  - main() L357–L485 (129 lines)
  > L357: `def main(certifying: bool = True) -> int:`
- [d] **AS-438** `code/01_platform/04_scripts/holistic-analyze.py:714` — Function too long (max: 80)
  - main() L714–L1870 (1157 lines)
  > L714: `def main():`
- [-] **AS-439** `code/01_platform/04_scripts/o2-provision.py:1513` — Function too long (max: 80)
  - provision_dashboards() L1513–L1628 (116 lines)
  > L1513: `def provision_dashboards() -> int:`
- [-] **AS-440** `code/01_platform/04_scripts/seed_alerts.py:70` — Function too long (max: 80)
  - main() L70–L185 (116 lines)
  > L70: `def main() -> int:`
- [-] **AS-441** `code/01_platform/04_scripts/stale_table_kind_scan.py:610` — Function too long (max: 80)
  - scan_file() L610–L758 (149 lines)
  > L610: `def scan_file(path: Path) -> list[tuple[int, int, str, str, str]]:`
- [-] **AS-442** `code/01_platform/04_scripts/t8_sandbox_contract_check.py:77` — Function too long (max: 80)
  - main() L77–L285 (209 lines)
  > L77: `def main():`
- [-] **AS-443** `code/01_platform/04_scripts/t9_order_sandbox.py:452` — Function too long (max: 80)
  - offline_contract() L452–L577 (126 lines)
  > L452: `def offline_contract():`
- [-] **AS-444** `code/01_platform/04_scripts/t9_order_sandbox.py:918` — Function too long (max: 80)
  - run_live() L918–L1088 (171 lines)
  > L918: `def run_live(transport=None, probe=None, secret="local-dev-only", now=None,`
- [-] **AS-445** `code/01_platform/04_scripts/t9_order_sandbox.py:1176` — Function too long (max: 80)
  - _self_check() L1176–L1291 (116 lines)
  > L1176: `def _self_check(out_dir, run_id):`
- [-] **AS-446** `code/01_platform/06_stage_profiler/stage_profiler.py:1317` — Function too long (max: 80)
  - build_report() L1317–L1527 (211 lines)
  > L1317: `def build_report(phase: Path) -> tuple[list[ProfileRow], str, dict[str, int]]:`
- [-] **AS-447** `code/02_services/01_ingestion/go-bridge/cmd/gen-corpus/main.go:269` — Function too long (max: 80)
  > L269: `func main() {`
- [d] **AS-448** `code/02_services/01_ingestion/go-bridge/faketool/main.go:37` — Function too long (max: 80)
  > L37: `func main() {`
- [-] **AS-449** `code/02_services/01_ingestion/go-bridge/main.go:103` — Function too long (max: 80)
  > L103: `func main() {`
- [-] **AS-450** `code/02_services/01_ingestion/go-bridge/main.go:404` — Function too long (max: 80) · full-audit correction: XC-32 fixed in CHG-538 (`81dbd763`); finding remains accepted
  > L404: `func runHFTEpoch(ctx context.Context, streamFactory hftStreamFactory, slot SlotAssignment, latencyMs int, responseTimeout time.Duration, epoch uint64, refreshAuth func(context.Context) error, authRefr …`
- [-] **AS-451** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:408` — Function too long (max: 80)
  > L408: `public static void main(String[] args) throws Exception {`
- [-] **AS-452** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:611` — Function too long (max: 80)
  > L611: `public void runWithBridge(String bridgeBinary) {`
- [-] **AS-453** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:1675` — Function too long (max: 80)
  > L1675: `private void processBridgeEvent(BridgeEvent event) {`
- [-] **AS-454** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java:2161` — Function too long (max: 80)
  > L2161: `private void shutdown() {`
- [-] **AS-455** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java:127` — Function too long (max: 80)
  > L127: `static ManifestResult loadFromPath(String path, int version) {`
- [-] **AS-456** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java:188` — Function too long (max: 80)
  > L188: `static IngestionConfig validateFrom(Map<String, String> env, boolean guardSecrets) {`
- [-] **AS-457** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java:103` — Function too long (max: 80)
  > L103: `private TickPacket(Builder b) {`
- [-] **AS-458** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java:516` — Function too long (max: 80)
  > L516: `public String buildMetricsJson() {`
- [-] **AS-459** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java:90` — Function too long (max: 80)
  > L90: `public boolean tryAccept(int recordBytes) {`
- [-] **AS-460** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java:486` — Function too long (max: 80)
  > L486: `private static boolean updateMarketSnapshot(MultiTimeframeState state, RowData tick, long eventTime) {`
- [d] **AS-461** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java:699` — Function too long (max: 80)
  > L699: `public void processElement(RowData tick, Context ctx, Collector<RowData> out) throws Exception {`
- [-] **AS-462** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/MultiTimeframeAggregateFunction.java:1226` — Function too long (max: 80)
  > L1226: `public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out) throws Exception {`
- [-] **AS-463** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java:147` — Function too long (max: 80)
  > L147: `public static StreamExecutionEnvironment buildTopology(SignalJobConfig config) {`
- [-] **AS-464** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java:504` — Function too long (max: 80)
  > L504: `static void applyRuntimeOptions(SignalJobConfig config, Configuration flinkConfig) {`
- [-] **AS-465** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java:189` — Function too long (max: 80)
  > L189: `public static SignalJobConfig from(Map<String, String> env) {`
- [-] **AS-466** `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/StrategyHostFunction.java:312` — Function too long (max: 80)
  > L312: `public void processElement1(RowData live, Context ctx, Collector<RowData> out)`
- [f] **AS-467** `code/02_services/04_executor/src/bridge/protocol.rs:663` — Function too long (max: 120)
  > L663: `fn order_validation_matches_the_go_bridge() {`
- [f] **AS-468** `code/02_services/04_executor/src/durable_file.rs:728` — Function too long (max: 120)
  > L728: `fn a_torn_final_line_is_dropped_but_a_corrupt_one_is_refused() {`
- [-] **AS-469** `code/02_services/06_execution_bridge/go-bridge/postback.go:68` — Function too long (max: 80)
  > L68: `func runPostbackLoop(ctx context.Context, connect func() (OrderUpdateSource, error), publish func(ReportEnvelope) error, onError func(error), initialBackoff, maxBackoff time.Duration) {`
- [-] **AS-470** `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/ExecutionGatewayMain.java:72` — Function too long (max: 80)
  > L72: `public static void main(String[] args) throws Exception {`
- [-] **AS-471** `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/GatewayHttpServer.java:154` — Function too long (max: 80)
  > L154: `private void approve(HttpExchange x) throws IOException {`
- [d] **AS-472** `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:212` — Function too long (max: 80)
  > L212: `static int run(String[] args) throws Exception {`
- [-] **AS-473** `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java:1394` — Function too long (max: 80)
  > L1394: `static boolean isSmokeFixtureRow(InternalRow row, RowType rowType) {`
- [-] **AS-474** `code/common/src/main/java/com/trading/common/schema/ddl/DdlText.java:129` — Function too long (max: 80)
  > L129: `public static ParsedDdl parse(String text, String sourcePath) {`
- [-] **AS-475** `code/common/src/main/java/com/trading/common/schema/eod/EodControllerTool.java:621` — Function too long (max: 80)
  > L621: `static Options parse(String[] args, Function<String, String> env) {`
- [-] **AS-476** `code/common/src/main/java/com/trading/common/schema/execution/ExecutionCommandGate.java:138` — Function too long (max: 80)
  > L138: `public Result execute(Command cmd) {`
- [-] **AS-477** `code/common/src/main/java/com/trading/common/schema/position/PositionProjector.java:77` — Function too long (max: 80)
  > L77: `public static ProjectionResult apply(PositionSnapshot current, FillEvent fill, long nowMs) {`
- [-] **AS-478** `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java:77` — Function too long (max: 80)
  > L77: `public ProjectionResult project(NormalizedPostback postback, long nowMs) {`

#### `complexity/too-many-params` — WARNING × 9

- Engine `code-quality` · category `Complexity` · assessment: **style/policy** (9 medium) · fixable: no (0)
- Score impact tier: `maintainability` (multiplier 0.75, rule cap 24)
- What it means: Function takes more parameters than the limit (default 6).
- Tool advice: Consider using an options object parameter

- [f] **AS-479** `code/01_platform/04_scripts/env_facts.py:116` — Function has too many parameters (max: 6) · **false positive** (G10 verified 2026-10-03 — see tracker)
  - render_row() L116–L122 (7 lines)
  > L116: `def render_row(num, title, status, verified, check, recheck, body):`
- [f] **AS-480** `code/01_platform/04_scripts/stale_table_kind_scan.py:541` — Function has too many parameters (max: 6) · **false positive** (G10 verified 2026-10-03 — see tracker)
  - classify_status() L541–L551 (11 lines)
  > L541: `def classify_status(lines: list[str], idx: int, claim_type: str, span: str,`
- [f] **AS-481** `code/01_platform/04_scripts/stale_table_kind_scan.py:554` — Function has too many parameters (max: 6) · **false positive** (G10 verified 2026-10-03 — see tracker)
  - classify_numeric() L554–L578 (25 lines)
  > L554: `def classify_numeric(lines: list[str], idx: int, claim_type: str, m: re.Match,`
- [f] **AS-482** `code/01_platform/04_scripts/stale_table_kind_scan.py:581` — Function has too many parameters (max: 6) · **false positive** (G10 verified 2026-10-03 — see tracker)
  - classify() L581–L607 (27 lines)
  > L581: `def classify(lines: list[str], idx: int, claim_type: str, span: str,`
- [f] **AS-483** `code/01_platform/04_scripts/t9_order_sandbox.py:218` — Function has too many parameters (max: 6) · **false positive** (G10 verified 2026-10-03 — see tracker)
  - canonical() L218–L234 (17 lines)
  > L218: `def canonical(protocol_version, message_type, request_id, account_scope_id,`
- [f] **AS-484** `code/01_platform/04_scripts/t9_order_sandbox.py:247` — Function has too many parameters (max: 6) · **false positive** (G10 verified 2026-10-03 — see tracker)
  - encode_envelope() L247–L269 (23 lines)
  > L247: `def encode_envelope(secret, protocol_version, message_type, request_id,`
- [f] **AS-485** `code/02_services/01_ingestion/go-bridge/main.go:358` — Function has too many parameters (max: 6) · **false positive** (G10 verified 2026-10-03 — see tracker)
  > L358: `func runHFT(ctx context.Context, cancel context.CancelFunc, client *arrow.Client, plan SubscriptionPlan, latencyMs int, responseTimeout time.Duration, refreshAuth func(context.Context) error, logf fun …`
- [f] **AS-486** `code/02_services/01_ingestion/go-bridge/main.go:404` — Function has too many parameters (max: 6) · **false positive** (G10 verified 2026-10-03 — see tracker)
  > L404: `func runHFTEpoch(ctx context.Context, streamFactory hftStreamFactory, slot SlotAssignment, latencyMs int, responseTimeout time.Duration, epoch uint64, refreshAuth func(context.Context) error, authRefr …`
- [f] **AS-487** `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java:213` — Function has too many parameters (max: 6) · **false positive** (G10 verified 2026-10-03 — see tracker)
  > L213: `public void updateSlot(String slotId, String state, long epoch, int assigned, int acknowledged, int rejected, long frameNanos) {`

### Engine: Formatting

#### `python-formatting` — WARNING × 61

- Engine `format` · category `Format` · assessment: **style/policy** (61 medium) · fixable: yes (61)
- Score impact tier: `mechanical` (multiplier 0.5, rule cap 12)
- What it means: File is not ruff-format formatted (mechanical).
- Tool advice: Run `aislop fix` to auto-format with ruff

- [f] **AS-488** `code/01_platform/01_docker/alert-consumer.py` — Python file is not formatted correctly
- [f] **AS-489** `code/01_platform/04_scripts/alert-routing-selftest.py` — Python file is not formatted correctly
- [f] **AS-490** `code/01_platform/04_scripts/audit_r2.py` — Python file is not formatted correctly
- [f] **AS-491** `code/01_platform/04_scripts/catalog_drift.py` — Python file is not formatted correctly
- [f] **AS-492** `code/01_platform/04_scripts/change_control_check.py` — Python file is not formatted correctly
- [f] **AS-493** `code/01_platform/04_scripts/check_flink_properties.py` — Python file is not formatted correctly
- [f] **AS-494** `code/01_platform/04_scripts/clean_break_drill.py` — Python file is not formatted correctly
- [f] **AS-495** `code/01_platform/04_scripts/cluster_check.py` — Python file is not formatted correctly
- [f] **AS-496** `code/01_platform/04_scripts/compose_config_redact.py` — Python file is not formatted correctly
- [f] **AS-497** `code/01_platform/04_scripts/cp_phase_capture.py` — Python file is not formatted correctly
- [f] **AS-498** `code/01_platform/04_scripts/day_run.py` — Python file is not formatted correctly
- [f] **AS-499** `code/01_platform/04_scripts/ddl_apply.py` — Python file is not formatted correctly
- [f] **AS-500** `code/01_platform/04_scripts/ddl_apply_smoke.py` — Python file is not formatted correctly
- [f] **AS-501** `code/01_platform/04_scripts/deploy_preflight.py` — Python file is not formatted correctly
- [f] **AS-502** `code/01_platform/04_scripts/deployed_artifact_verify.py` — Python file is not formatted correctly
- [f] **AS-503** `code/01_platform/04_scripts/disaster_drills.py` — Python file is not formatted correctly
- [f] **AS-504** `code/01_platform/04_scripts/docs_audit.py` — Python file is not formatted correctly
- [f] **AS-505** `code/01_platform/04_scripts/env_facts.py` — Python file is not formatted correctly
- [f] **AS-506** `code/01_platform/04_scripts/eod_controller.py` — Python file is not formatted correctly
- [f] **AS-507** `code/01_platform/04_scripts/eod_schedule.py` — Python file is not formatted correctly
- [f] **AS-508** `code/01_platform/04_scripts/evidence_ownership_check.py` — Python file is not formatted correctly
- [f] **AS-509** `code/01_platform/04_scripts/execution_network_check.py` — Python file is not formatted correctly
- [f] **AS-510** `code/01_platform/04_scripts/fluss-client-metrics.py` — Python file is not formatted correctly
- [f] **AS-511** `code/01_platform/04_scripts/fluss-repair/LogScan.py` — Python file is not formatted correctly
- [f] **AS-512** `code/01_platform/04_scripts/fluss-repair/verify-and-truncate.py` — Python file is not formatted correctly
- [f] **AS-513** `code/01_platform/04_scripts/fused_timeline.py` — Python file is not formatted correctly
- [f] **AS-514** `code/01_platform/04_scripts/gate_memo.py` — Python file is not formatted correctly
- [f] **AS-515** `code/01_platform/04_scripts/gate_preflight.py` — Python file is not formatted correctly
- [f] **AS-516** `code/01_platform/04_scripts/holistic-analyze.py` — Python file is not formatted correctly
- [f] **AS-517** `code/01_platform/04_scripts/image_staleness_check.py` — Python file is not formatted correctly
- [f] **AS-518** `code/01_platform/04_scripts/implementation_gate.py` — Python file is not formatted correctly
- [f] **AS-519** `code/01_platform/04_scripts/ing-tcp001/reconcile-compare.py` — Python file is not formatted correctly
- [f] **AS-520** `code/01_platform/04_scripts/jfr-analyze.py` — Python file is not formatted correctly
- [f] **AS-521** `code/01_platform/04_scripts/latency_probe.py` — Python file is not formatted correctly
- [f] **AS-522** `code/01_platform/04_scripts/local_int_004_smoke.py` — Python file is not formatted correctly
- [f] **AS-523** `code/01_platform/04_scripts/o2-provision.py` — Python file is not formatted correctly
- [f] **AS-524** `code/01_platform/04_scripts/o2_ingest.py` — Python file is not formatted correctly
- [f] **AS-525** `code/01_platform/04_scripts/perf_evidence_parse.py` — Python file is not formatted correctly
- [f] **AS-526** `code/01_platform/04_scripts/pernode_attribution_check.py` — Python file is not formatted correctly
- [f] **AS-527** `code/01_platform/04_scripts/placement_check.py` — Python file is not formatted correctly
- [f] **AS-528** `code/01_platform/04_scripts/plan_tracker.py` — Python file is not formatted correctly
- [f] **AS-529** `code/01_platform/04_scripts/pom-snapshot-scan.py` — Python file is not formatted correctly
- [f] **AS-530** `code/01_platform/04_scripts/prod_node_check.py` — Python file is not formatted correctly
- [f] **AS-531** `code/01_platform/04_scripts/r2_archive_selection.py` — Python file is not formatted correctly
- [f] **AS-532** `code/01_platform/04_scripts/r2_archive_sync.py` — Python file is not formatted correctly
- [f] **AS-533** `code/01_platform/04_scripts/r2_legal_hold_check.py` — Python file is not formatted correctly
- [f] **AS-534** `code/01_platform/04_scripts/seed_alerts.py` — Python file is not formatted correctly
- [f] **AS-535** `code/01_platform/04_scripts/seed_dashboards.py` — Python file is not formatted correctly
- [f] **AS-536** `code/01_platform/04_scripts/skip_inventory.py` — Python file is not formatted correctly
- [f] **AS-537** `code/01_platform/04_scripts/soak-o2-evidence.py` — Python file is not formatted correctly
- [f] **AS-538** `code/01_platform/04_scripts/stage_capture_parse.py` — Python file is not formatted correctly
- [f] **AS-539** `code/01_platform/04_scripts/stage_gc_summary.py` — Python file is not formatted correctly
- [f] **AS-540** `code/01_platform/04_scripts/stale_table_kind_scan.py` — Python file is not formatted correctly
- [f] **AS-541** `code/01_platform/04_scripts/strategy_live_board.py` — Python file is not formatted correctly
- [f] **AS-542** `code/01_platform/04_scripts/t8_sandbox_contract_check.py` — Python file is not formatted correctly
- [f] **AS-543** `code/01_platform/04_scripts/t9_order_sandbox.py` — Python file is not formatted correctly
- [f] **AS-544** `code/01_platform/04_scripts/tablet-orphan-sweep.py` — Python file is not formatted correctly
- [f] **AS-545** `code/01_platform/04_scripts/values_at_rest_scan.py` — Python file is not formatted correctly
- [f] **AS-546** `code/01_platform/04_scripts/version_matrix_verify.py` — Python file is not formatted correctly
- [f] **AS-547** `code/01_platform/05_instruments/split_manifest.py` — Python file is not formatted correctly
- [f] **AS-548** `code/01_platform/06_stage_profiler/stage_profiler.py` — Python file is not formatted correctly

---

## 4. Appendices

### A. Findings by top-level area

| Area | Findings | Errors |
|---|---:|---:|
| `code/01_platform` | 298 | 63 |
| `code/02_services` | 194 | 1 |
| `code/common` | 56 | 2 |

### B. Top 30 files by finding count

| File | Findings | Errors |
|---|---:|---:|
| `code/01_platform/04_scripts/holistic-analyze.py` | 54 | 25 |
| `code/01_platform/04_scripts/docs_audit.py` | 21 | 0 |
| `code/01_platform/04_scripts/t9_order_sandbox.py` | 19 | 3 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/IngestionService.java` | 19 | 0 |
| `code/01_platform/04_scripts/day_run.py` | 19 | 1 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java` | 19 | 0 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/telemetry/OtlpMetricsEmitter.java` | 15 | 0 |
| `code/02_services/04_executor/src/resilience.rs` | 14 | 0 |
| `code/02_services/04_executor/src/projection/mod.rs` | 11 | 0 |
| `code/01_platform/04_scripts/o2-provision.py` | 10 | 1 |
| `code/01_platform/04_scripts/stale_table_kind_scan.py` | 10 | 1 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/model/TickPacket.java` | 10 | 0 |
| `code/02_services/04_executor/src/executiongate.rs` | 10 | 0 |
| `code/01_platform/04_scripts/alert-routing-selftest.py` | 9 | 3 |
| `code/01_platform/04_scripts/audit_r2.py` | 9 | 0 |
| `code/common/src/main/java/com/trading/common/schema/ddl/DdlApplyTool.java` | 8 | 0 |
| `code/01_platform/04_scripts/r2_legal_hold_check.py` | 8 | 0 |
| `code/common/src/main/java/com/trading/common/schema/projection/PostbackProjectionDriver.java` | 8 | 0 |
| `code/02_services/04_executor/src/engine.rs` | 7 | 1 |
| `code/01_platform/04_scripts/disaster_drills.py` | 7 | 2 |
| `code/common/src/main/java/com/trading/common/config/PlatformConfig.java` | 7 | 0 |
| `code/01_platform/04_scripts/ddl_apply.py` | 6 | 3 |
| `code/01_platform/04_scripts/eod_schedule.py` | 6 | 3 |
| `code/01_platform/04_scripts/perf_evidence_parse.py` | 6 | 1 |
| `code/01_platform/05_instruments/split_manifest.py` | 6 | 4 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/health/HealthProbe.java` | 6 | 0 |
| `code/02_services/06_execution_gateway/src/main/java/com/trading/execution/gateway/CorrelationResolver.java` | 6 | 0 |
| `code/01_platform/04_scripts/cluster_check.py` | 5 | 1 |
| `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/write/AppendTracker.java` | 5 | 0 |
| `code/01_platform/04_scripts/local_int_004_smoke.py` | 5 | 2 |

### C. Coverage caveats

- aislop's supported targets: TypeScript/JavaScript, Python, Go, Rust, Ruby, PHP, C#, C/C++. Shell and Markdown are not analyzed, so the 127 shell scripts and 855 docs are out of scope (the repo's own `make static-check` covers shell with shellcheck).
- Java is not in the supported matrix; only text-tier rules apply.
- `**/target/**` was excluded (10 000+ build/generated files); if you want a raw unfiltered count, re-run without the `--exclude` flag.
- Some `ruff/*` and `python-formatting` results depend on the ruff version the CLI resolves; re-run with the same aislop version for comparable results.

### D. Reproduce / cross-verify

```bash
# full detailed human output (every location, no truncation)
AISLOP_NO_TELEMETRY=1 AISLOP_NO_HISTORY=1 npx -y aislop@0.17.0 scan -d \
  --exclude "**/target/**,**/__pycache__/**,**/.ruff_cache/**,logs/**"

# verify one finding by hand, e.g. AS-001
sed -n '50p' code/01_platform/04_scripts/alert-routing-selftest.py
```

*Report generated from `/tmp/opencode/aislop-full.json` at 2026-10-03 10:11. No source file was modified to produce it.*

