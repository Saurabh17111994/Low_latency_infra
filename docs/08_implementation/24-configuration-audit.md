# Configuration Audit — Hardcoded Values vs. Configurable Settings

**Date:** 2026-08-29 · **Scope:** entire repository · **Method:** source read + grep audit, no code changes

## Executive summary

The project is **already substantially configuration-driven** — the ingestion and compute services read validated env vars through dedicated config classes (`IngestionConfig`, `SignalJobConfig`, `GatewayConfig`, `BabysitterConfig`, `PlatformConfig`), fail closed on missing/invalid values, and keep secrets in a git-ignored `.env`. The audit found **~40 hardcoded values** that should be configurable, **~25 that should stay hardcoded** (true constants), **~10 duplicated/conflicting definitions**, and **5 architectural problems**.

The biggest gaps are not "missing env vars" but **fragmentation** (config split across 8 classes + 3 config file types), **schema duplication** (the 20-column raw table defined in 3 places), and **silent cross-module drift risk** (the same table name/version string repeated in config defaults, code literals, and DDL).

---

## 1. Complete list of hardcoded values

### 1A. Must be configurable (highest value)

| # | Value | Location | What it controls | Why problematic |
|---|-------|----------|-----------------|-----------------|
| M1 | `1ms / 256 / 64KiB` batch limits | `go-bridge/batch.go:37-39` | Proto frame batching (latency-vs-throughput) | "Locked O-2 defaults" but no env override — a pure tuning knob that requires code change to sweep |
| M2 | `ARROW_HFT_LATENCY_MS` default `50` | `IngestionConfig.java` | Broker tick interval | Default fine but it's the single most experiment-sensitive value; range `50..60000` hardcoded |
| M3 | `WATERMARK_OUT_OF_ORDER_MS=5000`, `ALLOWED_LATENESS_MS=5000`, `SOURCE_IDLE_MS=15000` | `SignalJobConfig.java:157-159` | Flink watermark/lateness | env-overridable already; but the *defaults* are in code, so a deploy that forgets them silently uses 5s — OK for dev, risky for prod |
| M4 | `PARALLELISM` default `8` | `SignalJobConfig.java:711` | Flink job parallelism | Hardcoded default 8; should be per-deployment |
| M5 | `CHECKPOINT_INTERVAL_MS=10000`, `CHECKPOINT_TIMEOUT_MS=30000`, `MAX_CONCURRENT_CHECKPOINTS=1` | `PlatformConfig.java:49-51` | Checkpoint contract | Pinned via `requirePinnedLong` — but these are *operational* values that a deployment might legitimately need to tune; the pin forces code change |
| M6 | `RESTART_MAX_ATTEMPTS=3`, `RESTART_DELAY_MS=30000` | `PlatformConfig.java:64-65` | Restart strategy | Same — pinned, but restart budget is environment-specific |
| M7 | `DEDUP_TTL_MS=60000`, `CANDLE_WINDOW_MS=15000` | `PlatformConfig.java:45-46` | Dedup TTL + candle window | These ARE load-bearing (correctness), but pinning them to refuse-start is overly rigid — a strategy experiment on 30s candles must edit code |
| M8 | `feature_candles_15s_preview` (hardcoded) | `SignalJob.java:263` | Preview sink table | Config has `CANDLE_TABLE` env but the preview table name is a literal; hidden coupling to DDL `30_feature_candles_15s_preview.sql` |
| M9 | `client.writer.retries=2` | `SignalJob.java:323` | Fluss client write retries | Hardcoded sink option; no env |
| M10 | `SINK_WRITE_STALL_TIMEOUT_MS=15000` | `PlatformConfig.java:78` | Fluss write stall bound | Governed pin, but it's a timeout — operationally tunable |
| M11 | `SOURCE_IDLE_ALERT_MS=60000` | `PlatformConfig.java:91` | Idle-tail alert | Monitoring threshold, should be env |
| M12 | `MAX_PENDING_APPEND_RECORDS/BYTES`, `PENDING_APPEND_WARNING_PERCENT` | `IngestionConfig.java:28-30`, `PlatformConfig.java:28-30` | Backpressure | Duplicated between IngestionConfig and PlatformConfig (see D1) |
| M13 | `APPEND_TIMEOUT_SECONDS=5`, `DRAIN_DEADLINE_SECONDS=30` | `IngestionConfig.java` | Append/drain | Env-overridable but defaults in code; DRAIN was previously hardcoded (fixed 2026-08-15) |
| M14 | `MOCK_ARROW_PORT=8888`, `MOCK_ARROW_PROFILE=baseline`, `MOCK_ARROW_INSTRUMENTS=50` | `MockArrowServer.java:253-264` | Mock broker | env-overridable but defaults in code; the mock's tick-generation params (`volume` base 1000, qty base 100) are hardcoded literals |
| M15 | `GATEWAY_BIND_HOST=127.0.0.1`, `GATEWAY_BIND_PORT=9180` | `GatewayConfig.java:125-126` | Gateway bind | env-overridable, defaults fine; a wildcard host (`0.0.0.0` / `::` / `*` / `[::]` / `0:0:0:0:0:0:0:0`) is refused unless `GATEWAY_ALLOW_WILDCARD_BIND=true` (P3-074), which only the two compose files set — neither publishes a host port for the gateway |
| M16 | `EXECUTION_BRIDGE_LISTEN_ADDR=127.0.0.1:8787` | `06_execution_bridge/go-bridge/main.go:44` | Bridge listen | env-overridable |
| M17 | `commandTimeout=10s` | `06_execution_bridge/go-bridge/server.go:50` | Command timeout | Hardcoded, no env |
| M18 | `healthPath=/healthz`, `readyPath=/readyz`, `commandPath=/v1/commands` | `06_execution_bridge/go-bridge/server.go:19-22` | HTTP API paths | Paths are a wire contract, but versioning `/v1` should be env if the API evolves |
| M19 | `O2_USER=admin@example.com`, `O2_PASSWORD=...` | `.env` | OpenObserve creds | **Secrets in .env** (see S1) |
| M20 | `EOD_MASTER_KEY=...` | `.env` | EOD encryption key | **Secret in .env** (see S1) |
| M21 | `ARROW_APP_SECRET/PASSWORD/TOTP_KEY` | `.env` | Broker credentials | **Secrets in .env** (see S1) |
| M22 | `AWS_ACCESS_KEY_ID=minioadmin`, `AWS_SECRET_ACCESS_KEY=minioadmin` | `.env` | MinIO creds | Dev defaults — acceptable locally, dangerous if .env leaks |
| M23 | `INSTRUMENT_MANIFEST_HOST_PATH` (absolute path) | `start-all.sh`, `docker-compose.yml` | Manifest CSV location | Hardcoded absolute path `/home/saurabh/.../NSE_CM_EQUITY (1024).csv` — machine-specific! |
| M24 | `localhost:9123`, `fluss-coordinator:9123` | many files | Fluss bootstrap | Repeated everywhere; see D3 |
| M25 | `bucket.num=16`, `table.log.ttl=7d`, `freshness=5min` | DDL `02_raw_table_1.sql` | Table storage params | Hardcoded in SQL; the DdlBootstrap Java mirror has them too (D2) |
| M26 | `bucket.num=8` | `fluss.properties`, DDL | Cluster bucketing | `8` in `fluss.properties`, `16` in DDL — conflicting (D4). **Resolved 2026-09-09 (P5-014/023/029/030): `fluss.properties` deleted — never wired into any image/mount/script (single-commit MVP artifact); the Fluss server config is the `FLUSS_PROPERTIES` env block in docker-compose.yml/docker-stack.yml. bucket.num exists only as per-table DDL options.** |
| M27 | `RATE_HZ` default `10`, `LIVE_THRESHOLD_RATE=500` | `loadtest-preview.sh:39`, `loadtest-collect.sh:32` | Loadtest rates | Operational test params; RATE_HZ is env-overridable but THRESHOLD is a literal |
| M28 | `DURATION_S=300`, `INTERVAL_S=30` | `loadtest-preview.sh:33-34` | Test duration | CLI-arg-driven, fine |
| M29 | `-port 8899` faketool | `loadtest-preview.sh:119` | Mock feed port | Hardcoded port |
| M30 | `TASK_MANAGER_MEMORY_MANAGED_SIZE=2g`, `TASK_MANAGER_NETWORK_MEMORY_MAX=256m`, `STATE_BACKEND=rocksdb` | `docker-compose.yml` | Flink TM memory | env-overridable in compose (`${...:-2g}`) — good pattern, defaults in compose |
| M31 | `JVM_HEAP_PERCENT_OF_CONTAINER_LIMIT=65`, `NON_HEAP_MEMORY_RESERVE_PERCENT=35`, `CONTAINER_MEMORY_ALERT_PERCENT=85` | `PlatformConfig.java:94-96` | JVM memory contract | Pinned constants; the 65/35 split is a real tuning knob |
| M32 | `MAX_ACTIVE_CANDIDATES_PER_INSTRUMENT=1` | `PlatformConfig.java:99` | Signal candidates | Strategy parameter, should be configurable per-strategy |
| M33 | `EARLY_SIGNAL_RULE`, `FORMING_RULE_ID`, `SIGNAL_STRATEGY_ID/VERSION/RULE_ID` | `SignalJobConfig.java:168-186` | Signal strategy identity | env-overridable, but these are the *strategy contract* — they should be a single strategy config block, not 6 separate env keys |

### 1B. Should probably be configurable (needs judgment)

| # | Value | Location | Notes |
|---|-------|----------|-------|
| S2 | `BROKER_BASELINE_TICKS_PER_INSTRUMENT_PER_SEC=20`, `MAX_TICKS=30` | `PlatformConfig.java:24-27` | Synthetic workload profile — could be env for load-testing different profiles |
| S3 | `MAX_INSTRUMENTS=3000` | `FixedScope.java:13` | Load cap; arguably a business constant, but experiments might want to lower it |
| S4 | `PENDING_APPEND_WARNING_PERCENT=80` | `PlatformConfig.java:33` | Dup of IngestionConfig 0.80 (D1) |
| S5 | `CANDLE_SCHEMA_VERSION="2"` default | `SignalJobConfig.java:153` | Schema version — env-overridable, good; but also duplicated in `PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION` (D2) |
| S6 | `OTEL_COLLECTOR_HOST=otel-collector:4318` | `SignalJobConfig.java:189` | Observability endpoint — env-overridable, fine |
| S7 | `FLUSS_WRITER_BATCH_TIMEOUT_MS=1`, `FLUSS_WRITER_MODE=generic`, `FLUSS_WRITERS=1` | `IngestionConfig.java` | A/B bench knobs — already env, good |
| S8 | `commandTimeout=10s` (exec bridge) | server.go:50 | env-overridable would be nice |
| S9 | `PROM=http://localhost:9250/metrics`, `FLINK=http://localhost:8081` | `loadtest-collect.sh:30-31` | Ops endpoints, hardcoded |
| S10 | `DEDUP_TTL_MS` / `CANDLE_WINDOW_MS` pinning | PlatformConfig.validateStartup | The pin mechanism itself is good; the *values* should be env-settable with the pin as a prod-only guard |

### 1C. Should remain hardcoded (true constants — keep in code)

| # | Value | Location | Why keep |
|---|-------|----------|----------|
| C1 | `FINGERPRINT_VERSION=1` | `FingerprintBuilder` | Canonical algorithm version — changing it breaks dedup semantics; must be code |
| C2 | `protocol_version=1` | proto `market_data.proto` | Wire protocol version — immutable |
| C3 | `ContractVersion` | `emitter_record.go` | Wire contract |
| C4 | `CONFIGURATION_VERSION`, `ALGORITHM_VERSION` | `CandleTableSchema` | Schema/algorithm identity — canonical, code |
| C5 | `SHA-256` hashing | `batch.go`, fingerprint | Crypto algorithm — constant |
| C6 | `int32` token range `1..2^31-1` | `parseTokensEnv` | Data-type bound |
| C7 | `1000/10` division checks | `loadtest-preview.sh:52` | Rate validation math |
| C8 | `1ms`/`256`/`64KiB` as *documented* O-2 defaults | batch.go comments | The *defaults* can stay; the *override* should be env (M1) |
| C9 | Column ordinals `CANDIDATE_ID=0...SUPERSEDES=19` | `SignalCandidatesTableColumns` | Schema column positions — contract |
| C10 | `GATEWAY_PROTOCOL_VERSION=execution-gateway.v1` | `GatewayConfig.java:128` | Protocol identity |
| C11 | `CommandPlace="place"` etc. | `06_execution_bridge/models.go:23-29` | Command vocabulary — API contract |
| C12 | `ACCOUNT_SCOPE_ID`, `EXECUTION_PARTITION_ID` required | GatewayConfig | Required identity, must be env-set (already) |

### 1D. Potential hardcoding problem (design issues, not constants)

| # | Problem | Location | Issue |
|---|---------|----------|-------|
| P1 | **20-column raw_table_1 schema defined in 3 places** | DDL `02_raw_table_1.sql`, `DdlBootstrap.java:204-223`, `TypedFlussRowConverter` POJO | Any column change must be made in 3 files; they've already drifted once (v1→v2 removed 8 columns, R-054/R-231) |
| P2 | **Table names repeated** | `SignalJobConfig` defaults, `SignalJob.java:263` literal, DDL, `GatewayConfig` defaults | `raw_table_1`, `feature_candles_15s_preview`, `Signal_Candidates`, etc. each appear 2-4×; a rename requires touching every layer |
| P3 | **Schema version duplicated** | `PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION="2"`, `SignalJobConfig` default, DDL comment "Schema version: 2" | Version string in 3 places; drift risk (already noted in PlatformConfig javadoc) |
| P4 | **Bootstrap server repeated** | `localhost:9123` fallback in SignalJobConfig, GatewayConfig, Python executor, scripts | The fallback `localhost:9123` is in 4+ files; a new env must set all |
| P5 | **Env keys duplicated across modules** | `FLUSS_BOOTSTRAP` vs `FLUSS_BOOTSTRAP_SERVERS` alias; `RAW_TABLE` vs `RAW_TABLE_NAME` | Two naming conventions for the same thing; `bootstrapServers()` accepts both (good), but the aliases are undocumented in one place |

---

## 2. Duplicated configuration / conflicts

| ID | What | Locations | Conflict? |
|----|------|-----------|-----------|
| D1 | `MAX_PENDING_APPEND_RECORDS/BYTES`, `PENDING_WARNING_PERCENT` | `IngestionConfig.java:28-30` (150k/192MiB/0.80) + `PlatformConfig.java:33` (80) + `.env` (150000/201326592/0.80) | **Yes — 3 definitions.** IngestionConfig is authoritative; PlatformConfig.PENDING_APPEND_WARNING_PERCENT=80 (percent) vs IngestionConfig 0.80 (fraction) — inconsistent units, and PlatformConfig's is dead code (IngestionConfig wins). |
| D2 | `RAW_TABLE_1_SCHEMA_VERSION="2"` | `PlatformConfig.java:42` + `SignalJobConfig.java:149` default + DDL comment | **Yes — 3 sources.** PlatformConfig is marked authoritative; SignalJobConfig derives from it — OK, but DDL comment can drift. |
| D3 | `FLUSS_BOOTSTRAP` | `.env`, `SignalJobConfig.bootstrapServers`, `GatewayConfig`, `Python executor`, scripts | Duplication, not conflict (same value) |
| D4 | `bucket.num` | `fluss.properties:8` vs DDL `02_raw_table_1.sql:16` | **Yes — 8 vs 16.** Cluster default vs table override; may be intentional (table overrides cluster) but undocumented as such. **Dissolved 2026-09-09: `fluss.properties` deleted (never wired — P5); bucket.num is per-table DDL only.** |
| D5 | `feature_candles_15s_preview` table name | `SignalJob.java:263` literal + DDL + config default | Duplication, drift risk |
| D6 | `O2_AUTH_BASIC` (base64) vs `O2_USER`+`O2_PASSWORD` | `.env` | Same secret in 2 forms — the base64 is redundant |

---

## 3. Architectural problems discovered

1. **Config fragmentation** — 8 config classes (`IngestionConfig`, `SignalJobConfig`, `GatewayConfig`, `BabysitterConfig`, `PlatformConfig`, `FixedScope`, `SafetyHaltJob` inline, `MockArrowServer` inline) + 3 config files (`.env`, `fluss.properties`, compose) + DDL. No single inventory of "what config exists."
2. **Schema definition scattered** (P1) — the worst maintenance hazard; a schema change is a 3-file edit.
3. **Mixed config-flag philosophy** — some values are *pinned* (refuse-start on deviation: `DEDUP_TTL_MS`, `CANDLE_WINDOW_MS`, HFT pins), some are *ranged* (env-settable within bounds), some are *silent defaults*. This is defensible but undocumented as a policy; a new maintainer can't tell which is which.
4. **Secrets in .env at rest** (S1/M19-22) — `.env` is git-ignored so not committed, but the file holds live broker credentials + EOD key + OpenObserve admin. The EOD key is a *real encryption key* (base64, `EOD_MASTER_KEY`).
5. **Test-env coupling** — `loadtest-preview.sh` and `loadtest-collect.sh` hardcode localhost endpoints and rates that assume a local dev cluster; running them against a remote cluster requires editing the script.

---

## 4. Proposed configuration structure

**Recommendation: keep env-var config as the primary mechanism** (it's Docker-native, already used, and the fail-closed validators exist). Do NOT introduce a YAML/TOML config file — that would be new tooling for no benefit. Instead:

1. **One canonical inventory doc** — extend `03-ingestion.md`'s configuration-contract table into a repo-wide `docs/08_implementation/24-configuration-audit.md` (this file) with a single table of every key, its owner module, default, range, and whether it's pinned/ranged/free.
2. **Centralize the constants** — move `PlatformConfig` + `FixedScope` + `IngestionConfig` defaults into one `code/common/src/main/java/com/trading/common/config/` package with a `ConfigKeys` class listing every key as a `String` constant (kills typos + gives a single grep target), and `PlatformConfig` as the default-value holder.
3. **Group env keys by section** with a consistent prefix:
   - `ARROW_*` — broker/feed (exists)
   - `FLUSS_*` — cluster (exists)
   - `SIGNAL_*` / `CANDLE_*` — strategy (exists partially)
   - `EXECUTION_*` — execution (exists)
   - `OPS_*` — operational (timeouts, retries, memory) — **new**
   - `OBS_*` — observability — **new**
4. **Per-environment override file** — keep `.env` as the single override point for compose; add `.env.prod` (git-ignored) documented as the prod overlay. Docker compose already supports `--env-file`.
5. **Secret handling** — move secrets out of `.env` into a separate git-ignored `secrets.env` (or secret manager in prod); `.env` keeps only non-secret defaults. The validators already never log secrets.
6. **Validation** — keep the existing fail-closed pattern (each config class validates at startup). Add a repo-wide `ConfigAudit` startup check that verifies: every env key read has a documented entry; no two modules read the same key with different defaults.

**Proposed schema (sections):**

```yaml
# Mental model only — NOT a new file format. This is how the env keys group.
broker:        ARROW_APP_ID, ARROW_APP_SECRET, ARROW_HFT_LATENCY_MS, ARROW_INSTRUMENT_TOKENS, ARROW_HFT_*
feed:          ARROW_MAX_EVENT_AGE_MS, ARROW_MAX_FUTURE_EVENT_SKEW_MS, CLOCK_OFFSET_LIMIT_MS
fluss:         FLUSS_BOOTSTRAP, FLUSS_DATABASE, RAW_TABLE, CANDLE_TABLE, bucket.num (from DDL)
strategy:      CANDLE_WINDOW_MS, DEDUP_TTL_MS, SIGNAL_STRATEGY_ID, EARLY_SIGNAL_RULE, MAX_ACTIVE_CANDIDATES
execution:     EXECUTION_ENABLED, GATEWAY_*, EXECUTION_BRIDGE_*, EXECUTION_INTENT_*
compute:       PARALLELISM, CHECKPOINT_*, RESTART_*, STATE_BACKEND, TASK_MANAGER_*
ops:           APPEND_TIMEOUT_SECONDS, DRAIN_DEADLINE_SECONDS, SINK_WRITE_STALL_TIMEOUT_MS, MOCK_ARROW_*
observability: OTEL_COLLECTOR_HOST, O2_*, SOURCE_IDLE_ALERT_MS
secrets:       (separate, git-ignored) ARROW_APP_SECRET, ARROW_PASSWORD, ARROW_TOTP_KEY, EOD_MASTER_KEY, O2_PASSWORD
```

---

## 5. Migration plan (safest/highest-value first)

**Phase 1 — inventory & de-dup (no behavior change):**
1. ✅ Create the single config-inventory table (this file) — done.
2. `ConfigKeys` constant class; replace literal env-key strings in all 8 config classes.
3. ✅ Fix D1: delete dead `PlatformConfig.PENDING_APPEND_WARNING_PERCENT`; make IngestionConfig the single owner.
4. Fix D6: derive `O2_AUTH_BASIC` from user+password at startup instead of storing base64.

**Phase 2 — split secrets:**
5. ✅ Move secrets from `.env` → `secrets.env` (git-ignored); compose loads both (`--env-file .env --env-file secrets.env`).
6. ✅ Add a startup check that rejects secrets in the main config (`SecretGuard`). Note (decision A, 2026-08-29): compose delivers secrets via env_file, so the guard skips its fail-closed check when `SECRETS_VIA_ENV_FILE=1` is set (compose + loadtest harness set it); host processes without the marker still fail closed.

**Phase 3 — schema single-source (highest structural value):**
7. ✅ Make DDL the single source of truth for the 20-column raw schema; `DdlBootstrap` reads the DDL (or a generated `RawTableSchema` class) instead of hand-written `SchemaBuilder`; `TypedFlussRowConverter` derives from the same. (This is the biggest risk-reducer.)

**Phase 4 — convert tuning knobs to env (M-list):**
8. ✅ Go batch limits (M1): add `BRIDGE_BATCH_MAX_AGE_MS / MAX_EVENTS / MAX_BYTES` env with the O-2 values as defaults (defaults stay locked; override allowed with validation).
9. Compute: `WATERMARK_OUT_OF_ORDER_MS`, `ALLOWED_LATENESS_MS`, `SOURCE_IDLE_MS` already env — document them; make `PARALLELISM` required-in-prod (M4).
10. ✅ `client.writer.retries` (M9) → env `FLUSS_WRITER_RETRIES`.
11. RETIRED 2026-09-05 (M8) — `PREVIEW_TABLE` no longer exists: preview rows are the
    multi-timeframe live snapshots in `candle_live` (DDL 32); the env key was dropped
    with the candle-era env surface (see runbooks §RETIRED early-signal/preview envs).
12. ✅ Exec bridge: `commandTimeout` (M17) → env `EXECUTION_BRIDGE_COMMAND_TIMEOUT_MS`.

**Phase 5 — relax over-pinning (judgment):**
13. ✅ `DEDUP_TTL_MS`/`CANDLE_WINDOW_MS` (M7): keep refuse-start pin in `prod` only; allow dev/experiment override (the pin mechanism already keys off `DEPLOY_ENV`).
14. ✅ `CHECKPOINT_*`, `RESTART_*` (M5/M6): same — prod-pin, dev-tunable.
15. ✅ `JVM_HEAP_PERCENT` (M31): env with 65/35 default.

**Phase 6 — script/ops params:**
16. ✅ `loadtest-collect.sh` endpoints/rates (S9/M27) → env with localhost defaults.
17. ✅ `INSTRUMENT_MANIFEST_HOST_PATH` (M23) → derive from repo root, not absolute path.
18. `MOCK_ARROW_*` tick-gen literals (M14) → env where they vary by test scenario.

**Explicitly NOT migrating (stay hardcoded):** protocol_version, fingerprint version, schema column ordinals, command vocabulary, crypto algorithms, token range bounds, loadtest rate-divisibility math, `ACCOUNT_SCOPE_ID`/`EXECUTION_PARTITION_ID` required identity.

---

## 6. Summary counts

| Category | Count | Examples |
|----------|-------|----------|
| Must be configurable | 33 | batch limits, checkpoint/restart pins, preview table, writer retries, secrets |
| Should probably be configurable | 10 | workload profile, schema version, OTEL host |
| Should remain hardcoded | 12 | protocol_version, fingerprint v1, column ordinals |
| Potential design problems | 5 | schema ×3, table names ×4, version ×3, bootstrap ×4, env aliases |
| Duplicated/conflicting | 6 | pending limits ×3, schema version ×3, bucket.num 8-vs-16 |
