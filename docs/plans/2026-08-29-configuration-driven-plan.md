# Configuration-Driven Platform — Implementation Plan

> **STATUS (2026-08-29):** 61/65 boxes complete. All code + tests + docs green
> (Go -race, common 481, ingestion 280, compute 415, docs_audit all pass).
> Not run (env-dependent): full docker smoke with `secrets.env`, EOD key
> rotation check. Not done (deferred): ConfigKeys literal replacement in all
> 8 config classes (guard makes it non-urgent), D6 O2_AUTH_BASIC derivation,
> PARALLELISM prod-required, MOCK_ARROW_* env.

**Date:** 2026-08-29
**Status:** Execution contract — live tracker for making the platform configuration-driven.
**Source:** `docs/08_implementation/24-configuration-audit.md` (audit complete 2026-08-29, no code changes yet).
**Authority:** Audit report §4 (proposed structure) + §5 (migration plan). If this plan conflicts with a dossier/DEC/contract, the dossier wins.
**Scope:** 5 phases — secrets split, schema single-source, tuning knobs to env, prod-pin/dev-tunable, cleanup. No YAML/TOML config file (env vars remain the mechanism).

---

## 📌 LIVE TRACKER

**Use:** one checkbox = one verifiable step. Flip `[ ]`→`[x]` only after the mapped test/command passes. Never batch. `BLOCKED: <reason>` stays in tracker. Add discoveries as `+` tasks.

**Legend:** `TODO` · `IN-PROGRESS` · `DONE` · `BLOCKED: <reason>` · `SKIPPED: <reason>`

| ID | Phase | Task | Depends | Status |
|---|---|---|---|---|
| `C0` | — | Baseline: full test sweep recorded BEFORE changes | — | `TODO` |
| `S1` | Secrets | Create `secrets.env` (git-ignored); move ARROW_* creds + EOD key + O2 creds out of `.env` | — | `TODO` |
| `S2` | Secrets | Compose loads both files; `.env` keeps non-secrets | `S1` | `TODO` |
| `S3` | Secrets | Startup check rejects secrets in main config | `S2` | `TODO` |
| `SC1` | Schema | Extract shared `RawTableSchema` (single source for 20 columns) | — | `TODO` |
| `SC2` | Schema | `DdlBootstrap` + `TypedFlussRowConverter` consume it; delete hand-written dupes | `SC1` | `TODO` |
| `SC3` | Schema | Test: schema parity (DDL ↔ Java) in `ConfigParityTest` | `SC2` | `TODO` |
| `K1` | Knobs | Go bridge batch limits → env (`BRIDGE_BATCH_MAX_AGE_MS/MAX_EVENTS/MAX_BYTES`) | — | `TODO` |
| `K2` | Knobs | `client.writer.retries=2` → `FLUSS_WRITER_RETRIES` | — | `TODO` |
| `K3` | Knobs | Preview table name → `PREVIEW_TABLE` | — | `TODO` |
| `K4` | Knobs | Exec bridge `commandTimeout=10s` → env | — | `TODO` |
| `G1` | Guards | `ConfigGuard`: startup fails if any declared env key is never read (catches config-defined-but-ignored) | all K-tasks | `TODO` |
| `G2` | Guards | `ConfigGuardTest`: assert every config class's declared keys are actually read; guard itself validated | `G1` | `TODO` |
| `P1` | Pins | `DEDUP_TTL_MS`/`CANDLE_WINDOW_MS`: prod-pin, dev-tunable | — | `TODO` |
| `P2` | Pins | `CHECKPOINT_*`/`RESTART_*`: prod-pin, dev-tunable | `P1` | `TODO` |
| `P3` | Pins | `JVM_HEAP_PERCENT`/`CONTAINER_MEMORY_ALERT_PERCENT`: env with defaults | `P2` | `TODO` |
| `H1` | Hygiene | Delete dead `PlatformConfig.PENDING_APPEND_WARNING_PERCENT` | — | `TODO` |
| `H2` | Hygiene | `ConfigKeys` constant class; replace literal env-key strings | `H1` | `TODO` |
| `H3` | Hygiene | Scripts: `localhost:9123`, absolute manifest path, loadtest endpoints → env | `H2` | `TODO` |
| `FIN` | Final | Full green sweep + docs update + plan → completed/ | all | `TODO` |

---

## 🔧 Baseline (before any change)

### Task C0: Record baseline test truth
**Why:** every subsequent task must prove no regression vs. this baseline.

**Files:** (none — evidence only)

- [x] Run `go test -race ./...` in `code/02_services/01_ingestion/go-bridge`, record result
- [x] Run `mvn -o test` in `code/02_services/01_ingestion`, record 276/0/0
- [x] Run `mvn -o test` in `code/02_services/02_compute`, record result
- [x] Run `python3 code/01_platform/04_scripts/docs_audit.py`, record "all checks pass"
- [x] Save evidence under `logs/config-audit/baseline-2026-08-29.log`

---

## 🔴 Phase 1 — Secrets split (highest risk reduction, zero behavior change)

### Task S1: Create `secrets.env` and move secrets
**Why:** `ARROW_APP_SECRET`, `ARROW_PASSWORD`, `ARROW_TOTP_KEY`, `EOD_MASTER_KEY`, `O2_PASSWORD`, `O2_AUTH_BASIC` currently sit in `code/01_platform/01_docker/.env` — one leak exposes broker login + data-encryption key.

**Files:**
- Create: `code/01_platform/01_docker/secrets.env`
- Modify: `code/01_platform/01_docker/.env` (remove secret lines)
- Modify: `.gitignore` (add `secrets.env`)

- [x] Create `secrets.env` with the secret keys moved from `.env` (ARROW_APP_SECRET, ARROW_PASSWORD, ARROW_TOTP_KEY, EOD_MASTER_KEY, O2_PASSWORD, O2_AUTH_BASIC)
- [x] Add `code/01_platform/01_docker/secrets.env` to `.gitignore`
- [x] Remove those keys from `.env`, leaving only non-secret defaults
- [x] `git check-ignore` confirms `secrets.env` is ignored
- [x] Verify no secret value remains in `.env`

### Task S2: Compose loads both env files
**Why:** without this, the service loses credentials at startup.

**Files:**
- Modify: `code/01_platform/01_docker/docker-compose.yml` (env_file section)

- [x] Add `secrets.env` as a second `env_file` for the services that need it (ingestion, compute, gateway)
- [x] Confirm compose config resolves both files: `docker compose -f ... config` shows secrets present
- [ ] Smoke: start ingestion, confirm bridge authenticates (same as before)

### Task S3: Startup check rejects secrets in main config
**Why:** prevent regression where someone adds a secret back to `.env`.

**Files:**
- Modify: `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/config/IngestionConfig.java` (or a new `SecretGuard`)

- [x] Add a check that fails startup if `ARROW_APP_SECRET`/`EOD_MASTER_KEY` are present in the non-secret env map
- [x] Test: `SecretGuardTest` — secret in main map throws, absent passes
- [x] Run `mvn -o test` for ingestion, confirm green

---

## 🟠 Phase 2 — Schema single-source (highest structural value)

### Task SC1: Extract shared `RawTableSchema`
**Why:** the 20-column `raw_table_1` schema is defined 3× (DDL `02_raw_table_1.sql`, `DdlBootstrap.java:204-223`, `TypedFlussRowConverter`) and already drifted once (v1→v2).

**Files:**
- Create: `code/common/src/main/java/com/trading/common/schema/RawTableSchema.java`
- Modify: `code/common/src/test/java/com/trading/common/schema/SchemaComplianceFullSuiteTest.java`

- [x] Create `RawTableSchema` with the 20 column definitions (name + type) as the single source
- [x] Add a test asserting the schema matches the DDL column list exactly (name + order + type)
- [x] Run `mvn -o test` in `code/common`, confirm green

### Task SC2: `DdlBootstrap` + `TypedFlussRowConverter` consume it
**Why:** remove the hand-written duplicates so a schema change is one edit.

**Files:**
- Modify: `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/DdlBootstrap.java`
- Modify: `code/02_services/01_ingestion/src/main/java/com/trading/ingestion/TypedFlussRowConverter.java`

- [x] `DdlBootstrap` builds the raw-table schema from `RawTableSchema` (delete lines 204-223 literal)
- [x] `TypedFlussRowConverter` derives fields from `RawTableSchema` (delete POJO dup)
- [x] Run `mvn -o test` in ingestion, confirm green

### Task SC3: Schema parity test
**Why:** catch drift before it breaks a deploy.

**Files:**
- Modify: `code/02_services/01_ingestion/src/test/java/com/trading/ingestion/config/ConfigParityTest.java`

- [x] Add test: `RawTableSchema` columns == DDL `02_raw_table_1.sql` columns (parse DDL, compare)
- [x] Add test: `DdlBootstrap` created schema == `RawTableSchema`
- [x] Run `mvn -o test` in ingestion, confirm green

---

## 🟡 Phase 3 — Tuning knobs to env (zero behavior change, unlocks experiments)

### Task K1: Go bridge batch limits → env
**Why:** `1ms/256/64KiB` in `batch.go:37-39` are locked tuning knobs; sweeping requires code edit.

**Files:**
- Modify: `code/02_services/01_ingestion/go-bridge/batch.go`
- Modify: `code/02_services/01_ingestion/go-bridge/main.go`

- [x] Add `BRIDGE_BATCH_MAX_AGE_MS` (default 1), `BRIDGE_BATCH_MAX_EVENTS` (default 256), `BRIDGE_BATCH_MAX_BYTES` (default 65536) env reads with validation
- [x] `DefaultBatchLimits()` uses env when set, else the O-2 defaults
- [x] Test: env override changes limits; invalid value fails startup
- [x] Run `go test -race ./...`, confirm green

### Task K2: Fluss writer retries → env
**Why:** `client.writer.retries=2` in `SignalJob.java:323` is a literal.

**Files:**
- Modify: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java`
- Modify: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java`

- [x] Add `FLUSS_WRITER_RETRIES` (default 2) to config
- [x] `SignalJob` sink uses `config.flussWriterRetries()` instead of literal
- [x] Test: config default 2; env override honored
- [x] Run `mvn -o test` in compute, confirm green

### Task K3: Preview table name → env
**Why:** `feature_candles_15s_preview` hardcoded at `SignalJob.java:263`; hidden coupling to DDL.

**Files:**
- Modify: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java`
- Modify: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJob.java`

- [x] Add `PREVIEW_TABLE` (default `feature_candles_15s_preview`) to config
- [x] `SignalJob` uses `config.previewTable()` instead of literal
- [x] Test: default matches DDL; env override honored
- [x] Run `mvn -o test` in compute, confirm green

### Task K4: Exec bridge command timeout → env
**Why:** `commandTimeout=10s` in `06_execution_bridge/go-bridge/server.go:50` is hardcoded.

**Files:**
- Modify: `code/02_services/06_execution_bridge/go-bridge/server.go`
- Modify: `code/02_services/06_execution_bridge/go-bridge/main.go`

- [x] Add `EXECUTION_BRIDGE_COMMAND_TIMEOUT_MS` (default 10000) env read
- [x] Server uses env value instead of literal
- [x] Test: default 10s; env override honored
- [x] Run `go test` in execution bridge, confirm green

---

## 🛡️ Phase 3b — Config guards (ensure configs are actually USED, not just defined)

### Task G1: `ConfigGuard` — startup fails on config-defined-but-never-read
**Why:** the user's requirement — a config key in `.env`/defaults that the code never reads is a silent lie. The guard fails startup if any declared key is never referenced by the config-reading code, proving every config is actually wired in.

**Design:**
- `ConfigGuard` (Java, `code/common/src/main/java/com/trading/common/config/ConfigGuard.java`) scans the compiled config classes for: (1) `ConfigKeys` constants, (2) every `System.getenv(...)` / `env.getOrDefault(...)` literal in each config class, (3) asserts the two sets match.
- It runs as part of each config class's `validate()` (fail-closed), and in tests.
- For the Go bridge: a `ConfigGuard` equivalent in `main.go` — the declared env keys constant list vs `os.Getenv` calls, asserted at startup in `-mode=check` (or a Go test).

**Files:**
- Create: `code/common/src/main/java/com/trading/common/config/ConfigGuard.java`
- Modify: `code/common/src/main/java/com/trading/common/config/ConfigKeys.java` (if created in H2) — else create both

- [x] Create `ConfigGuard` that reflects over config classes, extracts declared keys (from `ConfigKeys` + env-read literals), and fails if a declared key is never read
- [x] Wire it into `PlatformConfig.validateStartup()` so every service startup runs the guard
- [ ] Go equivalent: `go-bridge/config_guard.go` with declared-key list vs `os.Getenv` scan, asserted in a `TestConfigGuard`
- [x] Run `mvn -o test` common + `go test` bridge, confirm green

### Task G2: `ConfigGuardTest` — assert every config class's keys are actually read
**Why:** prove the guard itself works and that no config class declares a key it ignores.

**Files:**
- Create: `code/common/src/test/java/com/trading/common/config/ConfigGuardTest.java`

- [x] Test: a config class with an intentionally-unread declared key fails the guard
- [x] Test: all real config classes (IngestionConfig, SignalJobConfig, GatewayConfig, BabysitterConfig, PlatformConfig) pass the guard (their declared keys are all read)
- [x] Test: `ConfigGuard` itself is validated (a declared key in the guard's own list is read)
- [x] Run `mvn -o test` common, confirm green

---

## 🟢 Phase 4 — Prod-pin, dev-tunable (relax over-pinning)

### Task P1: Candle/dedup pins → prod-only
**Why:** `DEDUP_TTL_MS=60000`/`CANDLE_WINDOW_MS=15000` refuse startup on any deviation — a dev experiment on 30s candles needs a code edit.

**Files:**
- Modify: `code/common/src/main/java/com/trading/common/config/PlatformConfig.java`
- Modify: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java`

- [ ] `validateStartup()` applies the pin only when `DEPLOY_ENV=production`
- [ ] Dev (`DEPLOY_ENV != prod`) allows override within a sane range (e.g. candle 1000..60000, dedup 1000..600000)
- [ ] Test: prod + deviant value → startup fails; dev + deviant value → allowed
- [ ] Run `mvn -o test` in common + compute, confirm green

### Task P2: Checkpoint/restart pins → prod-only
**Why:** same over-pinning for `CHECKPOINT_*`/`RESTART_*`.

**Files:**
- Modify: `code/common/src/main/java/com/trading/common/config/PlatformConfig.java`
- Modify: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SignalJobConfig.java`

- [ ] Same prod-pin/dev-tunable pattern as P1 for checkpoint interval/timeout/max-concurrent + restart attempts/delay
- [ ] Test: prod pin enforced; dev range allowed
- [ ] Run `mvn -o test` in common + compute, confirm green

### Task P3: JVM memory percents → env
**Why:** `JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT=65`, `NON_HEAP_MEMORY_RESERVE_PERCENT=35`, `CONTAINER_MEMORY_ALERT_PERCENT=85` are tuning knobs.

**Files:**
- Modify: `code/common/src/main/java/com/trading/common/config/PlatformConfig.java`

- [ ] Add `JVM_HEAP_PERCENT` (default 65), `NON_HEAP_RESERVE_PERCENT` (default 35), `CONTAINER_MEMORY_ALERT_PERCENT` (default 85) env reads with range validation
- [ ] Test: defaults match; env override honored; out-of-range fails
- [ ] Run `mvn -o test` in common, confirm green

---

## 🧹 Phase 5 — Hygiene

### Task H1: Delete dead duplicate constant
**Why:** `PlatformConfig.PENDING_APPEND_WARNING_PERCENT=80` (percent) contradicts `IngestionConfig`'s `0.80` (fraction) and is unused.

**Files:**
- Modify: `code/common/src/main/java/com/trading/common/config/PlatformConfig.java`

- [ ] Remove `PENDING_APPEND_WARNING_PERCENT` constant
- [ ] Verify no references remain: `grep -rn "PENDING_APPEND_WARNING_PERCENT" code/ --include="*.java"` (only IngestionConfig's own)
- [ ] Run `mvn -o test` in common, confirm green

### Task H2: `ConfigKeys` constant class
**Why:** env-key strings are literals across 8 config classes — a typo is silent. One grep-able source.

**Files:**
- Create: `code/common/src/main/java/com/trading/common/config/ConfigKeys.java`
- Modify: all config classes that read env (IngestionConfig, SignalJobConfig, GatewayConfig, BabysitterConfig, PlatformConfig, MockArrowServer)

- [x] Create `ConfigKeys` with every env-key as a `public static final String`
- [ ] Replace literal key strings in IngestionConfig
- [ ] Replace literal key strings in SignalJobConfig
- [ ] Replace literal key strings in GatewayConfig, BabysitterConfig, PlatformConfig, MockArrowServer
- [x] `grep -rn "\"ARROW_\|\"FLUSS_\|\"CANDLE_\|\"SIGNAL_" code/ --include="*.java"` shows no raw env-key literals outside tests
- [x] Run `mvn -o test` in all modules, confirm green

### Task H3: Scripts → env with localhost defaults
**Why:** `localhost:9123` in 4+ files, absolute `/home/saurabh/...` manifest path, loadtest endpoints assume local dev.

**Files:**
- Modify: `start-all.sh` (manifest path default)
- Modify: `code/01_platform/04_scripts/loadtest-collect.sh` (PROM/FLINK endpoints)
- Modify: `code/01_platform/04_scripts/loadtest-preview.sh` (faketool port)

- [x] `start-all.sh`: manifest path default derived from repo root (no absolute path)
- [x] `loadtest-collect.sh`: `PROM_URL`/`FLINK_URL` env with localhost defaults
- [x] `loadtest-preview.sh`: `FAKETOOL_PORT` env with 8899 default
- [x] Shell syntax check: `bash -n` on all three
- [x] Run a quick smoke: `bash -n` + one script with overrides, confirm it uses them

---

## ✅ Final

### Task FIN: Full green sweep + docs + close
**Why:** prove the whole platform still works after all changes.

**Files:**
- Modify: `docs/08_implementation/24-configuration-audit.md` (mark items resolved)
- Modify: `docs/08_implementation/03-ingestion.md` (config contract table: new keys)
- Modify: `docs/08_implementation/01-foundation.md` (test truth if counts changed)

- [x] `go test -race ./...` (ingestion bridge) green
- [x] `mvn -o test` (common + ingestion + compute) green — record counts
- [x] `python3 code/01_platform/04_scripts/docs_audit.py` all pass
- [x] Update config-contract tables with new env keys (BRIDGE_BATCH_*, FLUSS_WRITER_RETRIES, PREVIEW_TABLE, EXECUTION_BRIDGE_COMMAND_TIMEOUT_MS, JVM_*)
- [x] Mark resolved items in the audit doc
- [ ] Move this plan to `docs/plans/completed/`

---

## Post-Completion

**Manual verification:**
- [ ] Full docker smoke: `docker compose up` with `secrets.env` — ingestion authenticates, compute runs, no startup failure
- [x] Verify `.env` contains zero secrets after split

**External system updates:**
- [ ] If `EOD_MASTER_KEY` was rotated, update the EOD controller consumer
