# Dedup redesign plan — heap recent-window repeat filter (2026-09-03)

## 1. Objective

Remove `fingerprint-dedup` as the pipeline throughput ceiling (~1.7 ms/record/thread
→ ~5k/s at 8 subtasks; measured: source 96% backpressured, dedup 52% busy) by
replacing the RocksDB MapState + native TTL design with an operator-local
count-windowed repeat filter (~0.0001 ms/record). Target: pipeline sustains the
10,240/s feed with headroom; dedup busy <25% at feed rate.

Non-goals: no source/fetch changes (proven innocent), no candle/signal logic
changes, no feed or ingestion changes.

## 2. Background (one paragraph)

Ticks are not transactions: a missed repeat double-counts one tick in one 15 s
candle — harmless. Repeats arrive back-to-back (retries/resends), never minutes
apart. Therefore "seen in the last ~2,000 per stock" protects longer (≈100+ s at
target rates) than today's 60 s TTL, at ~10,000× lower per-record cost. Restores
resume from saved read positions (no replay), so an empty-at-start filter is
safe; atomic snapshots keep candles correct regardless.

## 3. Step 0 — discriminating speed test (no cluster, no project changes)

TRAP TO AVOID (found 2026-09-03 while inventorying tests):
`DedupRocksDbThroughputMemoryIT` already proves the current operator drains
≥20,480/s on RocksDB with FRESH state and finite waves. A naive fresh-state
race would show "no difference" and wrongly kill this redesign. Production's
1.7 ms is an ALL-IN cost under TTL-expiry churn + snapshots + shared hardware —
the speed test must include churn (time advanced past TTL between waves,
resyncs forced) to be honest.

Two measurements, both local:

- Test 1 (exists, run as-is): `DedupRocksDbThroughputMemoryIT` =
  fresh-state RocksDB ceiling on this machine. If it FAILS here, RocksDB
  per-op is slow in this environment → redesign strongly confirmed.
- Test 2 (new, heap harness — the harness default backend): current operator
  vs heap-window prototype, identical waves WITH churn (processing time
  advanced past TTL, `GAUGE_RESYNC_INTERVAL_ROWS` lowered to force resync
  scans). This splits Java-machinery cost (text building, key strings,
  gauge maps, resync scans) from backend cost. Pass bar for proceeding:
  prototype p99 < 50 µs/record AND ≥ 50× faster than current-on-heap.

Decision: Test-1-fail OR Test-2-prototype-much-faster → proceed with redesign
(backend cost sits ON TOP of whatever Test 2 shows). Test-1-pass AND small
Test-2 gap → STOP and pivot to in-place slimming (Option 3 of the analysis).
Test 2 doubles as the G-DEDUP-5 perf guardrail going forward.

## 4. Step 1 — rewrite the operator

File: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/FingerprintDedupFunction.java`

- Replace `MapState<String, DedupEntry>` + native TTL with operator-local
  `Map<Long token, LinkedHashSet<String>>` (access-order not needed; manual trim
  loop on a plain `HashSet` + `ArrayDeque` per token, or `LinkedHashMap`
  removeEldestEntry — pick whichever the speed test favors; bounded by
  `DEDUP_WINDOW_ENTRIES`, default 2000, range-guarded 100–100000).
- Per record: build key as today → `set.add(key)` → `false` = repeat (drop,
  `duplicates.inc()`) → `true` = new (pass, `firstEvents.inc()`, trim if over
  bound). Keep the two counters; they are cheap and finally give us the real
  repeat rate.
- DELETE: `DedupEntry`, `DedupExpiry` usage, native TTL config, `resyncTokenGauges`
  + `tokenStateCount` + `tokenRowsSinceResync` + `dedupCount`/`bytesEstimate`
  machinery (alerts already use checkpoint size), `PER_ENTRY_ESTIMATE_BYTES`.
- New operator `uid("fingerprint-dedup-v2")` so Flink never silently maps legacy
  checkpointed MapState onto the new operator (see G-DEDUP-3).
- Remove the now-unused `dedupTtlMs` from `SignalJobConfig` (+ `PlatformConfig`
  pin) and update constructor/tests that reference it. Downstream stream
  contract unchanged (first passes, repeat dropped) — no candle/preview/
  forming-bar/signal changes.

## 5. Fail-fast guards (built in, not bolted on)

| ID | Guard | Fails when | Why (future bug it catches) |
|---|---|---|---|
| G-DEDUP-1 | `checkState(set.size() <= MAX)` after every add+trim, per token | Bound violated | Any future edit that breaks trimming fails in minutes, not via a TM OOM hours later |
| G-DEDUP-2 | Global cap: total entries/subtask ≤ tokens×MAX; exceed → fail closed with message | Memory growth beyond design | Never silently evict (eviction silently changes repeat semantics); loud failure instead |
| G-DEDUP-3 | On open: reject restore if legacy `fingerprint-dedup` MapState present (explicit exception naming the old uid + required clean-start) | Mixed-version restore | Prevents silently running new logic on half-migrated legacy state |
| G-DEDUP-4 | `DEDUP_WINDOW_ENTRIES` required, range-validated in `fromEnv`; missing/out-of-range → startup failure | Bad config | No silent defaults in production for a correctness-adjacent knob |
| G-DEDUP-5 | Perf guardrail test: p99 per-record cost < 50 µs on the speed-test harness; part of the module suite | Any future edit reintroducing heavy per-record work | The 1.7 ms regression class can never silently return — the build fails first |
| G-DEDUP-6 | Contract tests: within-window repeat dropped; beyond-window repeat passes; empty-after-restore passes new; cold token (rescale) passes new; string identity (no hash-collision class by construction) | Any semantic break | Locks the relaxed-but-explicit contract |
| G-DEDUP-7 | Dup-rate observability: `first`/`duplicates` counters + alert on repeat-rate spike | Feed retry storm | Distinguishes "filter broken" from "feed suddenly resending" |
| G-DEDUP-8 | Downstream equivalence test: dup-free feed → candle output byte-identical before/after | Any accidental behavior change for clean feeds | Proves the redesign is invisible when there is nothing to dedup |

## 6. Cutover (operational, needs explicit approval at run time)

- New uid + G-DEDUP-3 make old checkpoints unrestorable by design → first deploy
  starts WITHOUT restore. Start-offset strategy must be chosen explicitly at
  deploy time (recommendation: latest offsets — skip the backlog — because ticks
  are point-in-time data; replaying a 7M backlog through a fresh pipeline
  floods it for no value). This is a deploy-time decision, recorded in the run
  log, not a code default.
- Rollback = previous image + restore from a pre-cutover checkpoint (retained
  until the new path is blessed by §7).

## 7. Proof criteria (same harness, same gates)

- One 720 s stage-a2 run at RATE_HZ=10 on the redesigned operator, pristine
  everything else. Success ALL required: source mean ≥ 8,000/s; dedup busy
  <25% with source backpressure <20%; checkpoints complete; zero loss on
  dup-free feed (candle equality per G-DEDUP-8); injected-repeat test (known
  dups in feed) shows them dropped; data age p50 < 5 s and backlog not growing.
- Failure (any criterion missed) → report evidence, restore prior image, no
  rollout. No commit without explicit approval (standing rule).

## 8. Risks

- Repeat rate is unmeasured (counter never scraped); if the live feed repeats
  heavily at wide spacing, the count window admits more than the 60 s TTL did.
  Mitigated by G-DEDUP-7 (we will finally see the rate) and by the harmlessness
  argument (§2); window size is one knob if evidence demands it.
- Step 0 may redirect to in-place slimming (accepted outcome, not failure).
- First deploy skips backlog by recommendation — a conscious data choice,
  not an accident (recorded per §6).
