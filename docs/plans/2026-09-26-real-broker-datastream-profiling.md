# Real-broker (Arrow DataStream) latency profiling — plan

**Status:** implementation landed 2026-09-26 (CHG-319 profiler mode +
CHG-320 native multi-socket; Go suite green, profiler tests 48/48); off-hours
bring-up dry run PASSED; **off-hours DataStream chain smoke PASSED 2026-09-26**
on the 14-token active subset (real ticks -> raw -> candles -> feature tables,
presence every stage, zero classifier errors after CHG-321); market-hours
full-universe measurement pending (next NSE session).
**User decisions locked:**
1. Add `FEED=real` to the stage profiler.
2. DataStream mode uses the full **2 433**-stock universe
   (`Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY.csv`).
3. **One** ingestion container; the multi-connection facility already exists
   (user, 2026-09-26 — confirmed in code, see §2.3), so the container holds
   3 connections/slots.
4. Off-hours dry run = **bring-up-only** (no capture, no presence claims).

**Related:** `docs/plans/2026-09-26-signal-source-latency-tuning.md` (fake-broker
ladder, CHG-315/316/317/318), `docs/08_implementation/03-ingestion.md`
(feed selection, 2026-09-24), `code/01_platform/06_stage_profiler/README.md`.

## 1. Goal

Run the exact same S1–S9 stage ladder + `compute.latency.tick_to_strategy` KPI
against the **real broker channel** (Arrow DataStream, `ARROW_FEED=token`,
`wss://ds.arrow.trade`, mode `full`) that the fake broker runs measure today,
so the pipeline is proven end-to-end with real ticks, real timestamps, and the
real trade/quote mix.

## 2. Audit findings (facts this plan is built on)

### 2.1 Channels
- Two broker channels exist: HFT (`wss://socket.arrow.trade`) and DataStream
  (`wss://ds.arrow.trade`). On this account **HFT is dead** — probe 2026-09-26:
  `E_ALL_INVALID / PLAN_NOT_SUBSCRIBED` for all 25 tokens tried.
- DataStream is already implemented (2026-09-24): `go-bridge/token_slot.go`
  adapts `DataStream` onto the existing slot supervisor; `ARROW_FEED=token`
  selects it; `main.go` subscribes mode `"full"`; the stack `.env` already has
  `ARROW_FEED=token`. Java ingestion, `raw_table_1`, compute are unchanged.
- Off-hours behaviour (probe 2026-09-26): DataStream delivers **last-close
  snapshots only** (984/1024 manifest tokens, one stale tick each, ~18.5 h old;
  the 22 NSE `NSETEST` test scrips re-send unchanged values every ~40 s).
  Not a live test feed.
- Off-hours DataStream is **not uniformly stale** (probes 2026-09-26, 75–110 s
  windows): most scrips send one last-close snapshot, but the liquid set (SBIN,
  RELIANCE, INFY, ITC, HDFCBANK, TCS, ICICIBANK, WIPRO, …) streams
  periodically with fresh LTT and advancing cumulative volume — 14/30 sampled
  tokens would pass the 5 s freshness gate, and 8/8 active names produced
  volume-advancing (TRADE-eligible) rows: 195 in 75 s, RELIANCE 81, SBIN 43.
  So an off-hours DataStream chain test is possible on the active subset; the
  in-session run remains the authoritative measurement. Evidence
  `logs/real-broker-probe-20260926/probe7-datastream-acceptable.txt`,
  `probe8-trades-vs-quotes.txt`, `datastream-active.csv`.
- No 24x7/demo/paper feed documented or reachable on this account.

### 2.2 Profiler flow and fake-only pieces (`stage-profile.sh`)
- Flow: `preflight → purge_tables → start_fleet → submit_job → compile_probes →
  warmup → run_capture → sample_raw → stop_fleet`, then the fail-closed
  presence gate (smoke) and `profile.md`.
- `start_fleet` (line ~174) is fake-only:
  `pipeline_start_faketool`, then N containers with
  `ARROW_FAKE_BROKER=1`, `ARROW_HFT_URL=ws://sp-faketool:8899`,
  `ARROW_HFT_LATENCY_MS=50`, `ARROW_HFT_CONNECTIONS=1`,
  `-e ARROW_APP_ID=${ARROW_APP_ID:-testd}`,
  `-e ARROW_USER_ID=${ARROW_USER_ID:-testd-user}`.
- Subscription confirmation greps `java.out` for `"HFT subscribed $per_slice"`
  (per container 811 in the 3×811 fake shape).
- `TOKENS` (S1/S8 probes) = first `PROBE_TOKEN_COUNT` (12) tokens of the CSV.
- `RATE_HZ` is a **label only** in capture (`rate_hz=`) and in the report title.
- Presence rules: minimum **1 sample per stage** (S1–S9) — presence, not volume.
- Default `NSE_PATH` is already the 2 433-row CSV; `INGESTION_CONTAINERS=3`.

### 2.3 Bridge capacity / token mode
- Hard caps (`subscription_plan.go`): `MaxHFTTokensPerConnection=1024`,
  `MaxHFTTokensPerRequest=512`, `MaxHFTConnections=3`.
- **The multi-connection facility already exists** (`main.go:169-175`):
  `ARROW_HFT_CONNECTIONS` is tunable 1..3 (default 1) and the code comment
  names the full 2 433-instrument manifest as exactly this use case;
  `c1_plug_and_play_test.go` (`TestPerSocketReconnectIsolation`) already plans
  **2 433 tokens × 3 slots**. 2 433 tokens cannot fit one connection (1024 hard
  cap), so the profiler real mode sets `ARROW_HFT_CONNECTIONS=3`
  (3×811 = 2 433, requests ≤512) — the same per-slot shape as the fake 3×811
  runs, inside one bridge process.
- **Runtime gate (found by the 2026-09-26 bring-up dry run, fixed natively in
  CHG-320):** the Go bridge itself refused `len(plan.Slots) != 1`
  unconditionally (`main.go runHFT`). It now allows extra sockets when
  `ARROW_HFT_MULTI_CONNECTION_APPROVED=true` and `DEPLOYMENT_ENV` is
  non-production (blank fails closed; production refuses) — mirroring the Java
  gate, with the profiler needing no workaround. Dry run confirmed:
  `multi-socket approved: slots=3`.
- **Java policy gate**: `ARROW_HFT_MULTI_CONNECTION_APPROVED` (default false)
  must be `true` for multi-connection; production rejects it “until broker
  evidence proves the account may hold the configured socket count”; dev
  accepts it (`IngestionConfig.java:266-305`). The step-0 probe below is that
  evidence for the DataStream plan.
- `token_slot.go` ignores `exchSeg`/`latencyMs`; it subscribes in ≤512-token
  requests and **synthesizes the ack** per request (fix measured 2026-09-24).
- The bridge logs `HFT subscribed %d tokens (latency=%dms)` **per slot**
  (`main.go:632`) with the slot's assignment (811), on the same code path for
  token mode — so with 1 container × 3 slots the line appears **3×**, not once
  with 2 433. The confirmation check must be adapted accordingly.

### 2.4 Java side
- `INSTRUMENT_MANIFEST_PATH` → `InstrumentManifestLoader` → the service hands
  the bridge child `ARROW_INSTRUMENT_TOKENS` derived from that manifest
  (`IngestionService.java:754-762`). So one manifest file drives both the
  expected instrument set and the actual subscription set.
- `READINESS_FILE_PATH` marker is written by the service (`:335`).
- Full-mode snapshots with `ltp_paise = 0` (a scrip that has not traded yet)
  were classified `VALID_TRADE` and then quarantined as `INTERNAL_ERROR` by
  `TickPacket.validate`; fixed in **CHG-321** — they are now
  `VALID_NON_TRADE` and stored as `QUOTE` (candles accumulate only
  `tick_type = 'TRADE'`, so nothing downstream changes). Found by the real
  bring-up dry run (`VALID_TRADE requires lastPricePaise > 0`).
- `ARROW_MAX_EVENT_AGE_MS` (profiler passes 5000) is the staleness/quarantine
  gate (`:1089`) — off-hours stale snapshots are dropped **by design**, so
  off-hours cannot produce S1–S9 samples.

### 2.5 Test/doc surface
- Profiler tests: `code/01_platform/06_stage_profiler/tests/test_stage_profiler.py`.
- README documents the env knobs and says “fake broker + 3 ingestion
  containers” — must gain the `FEED=real` description.

## 3. Design — `FEED=real`

New env knob: `FEED="${FEED:-faketool}"` with values `faketool | real`
(invalid value fails in `check_only` too).

`start_fleet` branches:

**faketool (unchanged):** faketool + N containers, current env block.

**real:**
- skip `pipeline_start_faketool`;
- require `INGESTION_CONTAINERS=1` (fail otherwise; capacity lives in slots,
  not processes);
- slots = `ceil(rows / 1024)` (2433 → 3); guard `slots ≤ MaxHFTConnections`;
- one container with:
  - `--env-file "$LIB_SECRETS_FILE"` for secrets; **non-secret identifiers
    read explicitly from the stack `.env`** (`stack_env_value ARROW_APP_ID` /
    `ARROW_USER_ID`) — `docker run --env-file` does not strip inline comments
    and that file has one on `ARROW_INSTRUMENT_TOKENS`;
  - `-e ARROW_FEED=token`, `-e ARROW_HFT_CONNECTIONS=<slots>` (3),
    `-e ARROW_HFT_MULTI_CONNECTION_APPROVED=true` (dev-only policy gate);
  - `-e INSTRUMENT_MANIFEST_PATH=/run/manifest-00.csv` (single 2 433-row slice);
  - **no** `ARROW_FAKE_BROKER`, **no** `ARROW_HFT_URL`, no `ARROW_HFT_LATENCY_MS`
    (HFT-only under token);
  - keep `FLUSS_*`, `RAW_TABLE_NAME`, `OTEL_COLLECTOR_HOST`,
    `METRICS_LOCAL_LOG`, `ARROW_TICK_COUNTS`, `ARROW_MAX_EVENT_AGE_MS`,
    `ARROW_MAX_FUTURE_EVENT_SKEW_MS`, `CLOCK_CHECK_REQUIRED=false`;
- credentials guard before start (present, not `testd`); never print secrets;
- subscription confirmation: wait for `slots` × `HFT subscribed <rows/slots>
  tokens` lines (sum == rows) instead of one exact `$per_slice` grep;
- market-hours guard: `FEED=real` refuses to start outside 09:15–15:30 IST
  unless `ALLOW_OFFHOURS_REAL=1` (explicit, clearly labeled override).

Report/labels:
- feed-aware report title (e.g. “Stage profile — real broker DataStream full,
  2 433 instruments, 900 s” vs the fake “@ N Hz”);
- `rate_hz=` label: for real runs write `datastream-full` (or `RATE_HZ=1`) —
  cosmetic only;
- S1–S9 semantics unchanged.

Pre-flight (step 0, read-only, ~2 min): subscribe **2 433 tokens** on
DataStream and count errors — >1024 acceptance is currently unverified (1024
verified 2026-09-26, 0 errors). Fallback if capped: keep the approved 1 024
manifest for the measurement and note the cap; the profiler mode is unaffected.

## 4. Implementation checklist

1. [x] Step 0 probe — done 2026-09-26 (`logs/real-broker-probe-20260926/`):
       2 433 tokens accepted on one connection (1 838 snapshots/60 s); 3
       concurrent connections all served (779/514/748, union 2 041) → policy
       evidence for `ARROW_HFT_MULTI_CONNECTION_APPROVED`.
2. [x] `stage-profile.sh`: `FEED` knob + real branch (env, slots, guards,
       slot-wise confirmation, feed-aware labels) — CHG-319.
3. [x] Profiler tests (`tests/test_stage_profiler.py`): 36 → 47, all green.
4. [x] README: `FEED=real` usage, capacity math, market-hours rule, dry run.
5. [x] This plan + `CHG-319` record.
6. [x] Off-hours bring-up dry run PASSED 2026-09-26 11:18 IST: plan `slots=3`,
       `HFT subscribed 385 + 1024 + 1024 = 2 433`, readiness marker + all-slot
       confirmation, clean teardown (`logs/stage-profile-20260926-111831/`,
       `logs/real-broker-probe-20260926/`).
7. [ ] Market-hours run (next session): smoke 200–300 s → if clean, main 900 s;
       same evidence format; report real p50/p95/p99 beside the fake numbers.
8. [x] Off-hours DataStream chain smoke 2026-09-26 11:45 IST, 14-token active
       subset (`datastream-active.csv`, 1 slot): presence PASS every stage —
       1 590 appends, 1 591 S1 samples, 1 323 post-dedup, 56 closed-candle
       reads; zero `VALID_TRADE` errors after CHG-321. Evidence
       `logs/stage-profile-20260926-114535/`. Caveat: off-hours broker stream
       + second-granular event time — S1/S5 are bounds, S2/S3/S4/S6/S7/S9 are
       the tight numbers; not comparable with the in-session run.

## 5. Risks / open issues (need decision or verification)

| # | issue | handling |
|---|---|---|
| 1 | 3 connections in one container (1024/slot hard cap) | **RESOLVED** — facility exists (user + code); set `ARROW_HFT_CONNECTIONS=3` + approval flag; dry run verifies |
| 2 | 2 433-token DataStream acceptance | **CLEARED** — 0 write errors, 1 838 snapshots/60 s; the 392-token gap is delivery pacing, not entitlement (probe5 pending) |
| 3 | Off-hours cannot measure (staleness gate drops snapshots) | **DECIDED** — bring-up-only dry run; live measurement in the next session |
| 3b | Concurrent DataStream sessions | **CLEARED** — 3 concurrent connections served (union 2 041) |
| 4 | S1/S8 probe tokens = first 12 CSV rows, possibly illiquid | raise `PROBE_TOKEN_COUNT` or pick liquid tokens if presence is thin |
| 5 | Real cadence ≈1 Hz/token full mode → throughput not comparable to the 2 Hz fake ladder | report as its own mode; do not rebaseline fake numbers |
| 6 | `ARROW_HFT_CONNECTIONS=3` under `ARROW_FEED=token` (3 WS from one process) is untested in the profiler shape | dry run + subscription confirmation check |
| 7 | Safety | market-data only; `EXECUTION_INTENT_ENABLED=false`; no order APIs used |
| 8 | Real DataStream event time has **1-second resolution** (`LTT` at second granularity, normalized to ms), so S1/S5 absolute latencies are quantized to ~1 s and not comparable with the fake ladder | report S2/S3/S4/S6/S7/S9 for tight latency; keep S1/S5 as bounds; measured off-hours: S2 0 ms, S3 1 ms, S4 4 ms, S6 23 ms, S7 16 ms medians |

## 6. Out of scope

- The “3 stocks 24x7” facility (not found on this account; user to confirm
  broker-side).
- HFT feed (plan dead on this account).
- Feature layer and the faketool trade-rate-semantics fix (real ticks will
  inform the latter).

## 7. Rollback

`FEED` defaults to `faketool`; real mode is opt-in per run and touches no
data-path code. Reverting = stop passing `FEED=real`.

## 8. Effort

Implementation + tests + docs ≈ 2–3 h. Dry run ≈ 15 min. Monday live run
30–60 min (smoke + main).

## 9. Monday runbook (in-session, full universe)

### Pre-checks (T-15 min)

1. **Trading day?** NSE holiday calendar. A holiday shows up as a smoke
   presence failure, not as a guard error — do not start on one.
2. **Stack up** (`make up` if not). The profiler preflight re-checks Fluss +
   taskmanager, but a cold stack costs ~2 min of the best window.
3. **Image current.** The loadgen image was rebuilt 2026-09-26
   (stamp `90bab706…`). If any `code/02_services/01_ingestion` or `go-bridge`
   source changes before Monday, rebuild it — the preflight fails closed on a
   stale stamp.
4. **Clock.** Host offset < ~10 ms (`timedatectl` / chrony). S1/S5 compare host
   receive time with broker time; a drifted host skews every number.
5. **Credentials** stay in `.env`/`secrets.env`; the run never prints them.

### T-0: the one command (inside 09:15–15:30 IST)

```
FEED=real INGESTION_CONTAINERS=1 PHASES=smoke,main SMOKE_S=300 MAIN_S=900 \
  bash code/01_platform/06_stage_profiler/stage-profile.sh
```

No `ALLOW_OFFHOURS_REAL` in-session — if the guard blocks, the clock is wrong.

### First 2 minutes — abort table

| minute | expected line | if it does not appear |
|---|---|---|
| 0:00–0:10 | `preflight (smoke): Fluss ready, fresh taskmanager, image stamp` | stack down or stale image — fix and rerun |
| 0:10–1:00 | four `purging …` / `… purged` pairs | a purge failure is fatal; read the failing line |
| 1:00–1:10 | `fleet (real): 2433 instruments -> 1 container x 3 slots` then `fleet up (real): subscriptions confirmed (3 slots, 2433 tokens)` | grab `…/smoke/capture/j1-0/java.out`; the CHG-320 gate message names the reason; the sum must be 2 433 |
| 1:10–1:30 | `SignalJob submitted: job_id=…` | check the jobmanager log |
| 1:30–2:00 | `warm-up: raw log advancing (a -> b)`, `b > a` | **no advance after ~3 min → Ctrl-C**; do not burn the main window on a dead feed |

Same-minute `java.out` spot checks:

```
grep -c "VALID_TRADE requires lastPricePaise > 0" …/smoke/capture/j1-0/java.out  # expect 0 (CHG-321)
grep "HFT subscribed" …/smoke/capture/j1-0/java.out                            # expect 3 lines, sum 2 433
```

### Then it runs itself

- smoke 300 s → presence gate. PASS → main 900 s starts automatically; FAIL →
  the run stops and prints the failing legs in `smoke-presence.json`. If only
  S1/S8 are thin, rerun the smoke with `PROBE_TOKEN_COUNT=25` (the first 12 CSV
  rows include illiquid names).
- main 900 s → `REPORT: logs/stage-profile-*/profile.md`.

### Read the report correctly

- S2/S3/S4/S6/S7/S9 are the tight numbers; S1/S5 are **bounds** (broker event
  time has 1-second resolution) and must not be compared with the fake ladder's
  ms figures.
- Throughput is live full-mode cadence (≈1 Hz/token) — its own mode, not the
  2 Hz fake pin.
- Keep as evidence: `profile.md`, `smoke-presence.json`, `j1-0/java.out`,
  `stages/`, and the launcher log; `session.txt` records the guard decision.
