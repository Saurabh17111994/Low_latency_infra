# Over-engineering audit — 2026-10-04 (read-only, nothing changed)

Scope: `code/**` (Java services, Go bridge, Rust executor, Python scripts/SQL DDL),
the root `Makefile` and `code/01_platform/04_scripts/*.sh`. Excluded: `logs/`,
`docs/`, `testdata/`, build output, generated artifacts.

Method: a discovery pass hunting for deletable complexity (reinvented stdlib,
single-implementation abstractions, dead flexibility/config keys, duplication,
unused dependencies, zero-caller code), then **manual verification of every
candidate against the repository** — grep for callers/config producers, and read
the change record and implementation doc that owns the code. A finding is only
listed here if the verification agreed with it.

Coverage honesty: this is **one audit pass**, not an exhaustive proof over every
package. Areas not listed below were either inspected and clean, or not reached.
Items already recorded by the 2026-10-02 audit sweep (`CHG-538`, `CHG-539`) were
skipped by instruction.

## Result in one line

Nearly nothing is accidental. Four candidates surfaced; **three are documented,
deliberate seams** (deleting them would break a declared interface, not remove
slop) and one is a 3-line manifest question. **The real optimisation upside in
this repository is runtime behaviour, not code deletion** — see the last section.

## Candidates and their verified disposition

### 1. Strategy-context live-fetch subsystem — **KEEP** (do not delete)

`signaljob/ContextProvider.java`, `ContextView.java`, `CandleFetcher.java`,
`FlussCandleFetcher.java`, `ContextProbeStrategy.java`, wired through
`SignalJobConfig.java:298-300` (`STRATEGY_CONTEXT_ENABLED`, default false),
forwarded by `code/01_platform/04_scripts/pipeline-lib.sh:1172`.

The first pass flagged it as unreachable (~930 lines). That is **wrong**: the
flag has been switched on for real. `docs/05_deployment/change-records/CHG-480.md:12-13`
records an operator smoke with `STRATEGY_CONTEXT_ENABLED=true` and
`STRATEGIES=n7-range-breakout-v1,ctx-probe-v1` (docker exec only, never
committed into `.env`), and the behaviour is a **declared** part of
`docs/08_implementation/04-signal-job.md:44,48,60-63` (including its metrics
`strategy/ctx-probe-v1.*`). The design is `docs/plans/2026-09-30-strategy-context-live-fetch.md`.

Disposition: keep. An off-by-default, documented capability is not dead code.
The only legitimate question is whether the *probe strategy* stays in the
shipped registry, and that is a product decision, not a cleanup.

### 2. Executor durable journal/audit flags — **KEEP the flags, optional trim of the in-memory stores**

`code/02_services/04_executor/src/durable.rs:277-295`, `config.rs:71-72,239-240`.

`DURABLE_JOURNAL_ENABLED` / `DURABLE_AUDIT_ENABLED` are **refused at boot** with
an explicit reason, and that refusal is the feature: `CHG-135.md:67-68` ("those
two clients have no durable implementation"), preceded by `CHG-065.md:22`
(default OFF). Failing fast on a configuration that cannot be honoured is a
guard at a trust boundary — the opposite of dead flexibility.

The genuinely questionable part is only the `InMemoryJournalStore` /
`InMemoryAuditSink` bodies plus their `DurableClients` fields — used by tests,
not by production paths. ~150 Rust lines, **low value, low confidence of being
worth the churn** (the tests that exercise them would need rewriting).

### 3. Three direct Nautilus dependencies — **QUESTION, do not silently drop**

`code/02_services/04_executor/Cargo.toml:28-30` declares
`nautilus-event-store`, `nautilus-portfolio`, `nautilus-risk` with the pinned
rev. Verified: **no import anywhere** in `src/` or `tests/`
(`rg -n "nautilus_event_store|nautilus_portfolio|nautilus_risk|nautilus::(event_store|portfolio|risk)"`
→ empty). But they are not only code: they appear in the version-compatibility
evidence (`code/01_platform/04_scripts/version_matrix.yaml:531`) and in the
recorded check `docs/08_implementation/05-execution-core.md:823`
(`cargo check --locked -p nautilus-execution -p nautilus-live -p nautilus-event-store  # PASS`).

Disposition: a 3-line manifest trim that **touches pinned evidence**, so it needs
the evidence owner's word before anyone removes it. Not a free win. Do not bundle
it into another change.

### 4. Executor telemetry export seam — **KEEP** (documented placeholder)

`code/02_services/04_executor/src/telemetry.rs:60-141` (`TelemetrySink`,
`NullSink`, `Metrics::snapshot`, `MetricsSnapshot`). No production caller exports
anything today — but this seam is named as a known remaining item in
`docs/08_implementation/09-production-swarm.md:931,953` ("replace `NullSink` with
a real sink"), in `10-observability.md:155`, and it is what the OpenObserve
dashboard panels are waiting on. The counters themselves (`METRICS`) are live.

Disposition: keep. It is a roadmap placeholder with a written reason.

## Inspected and clean

- **Fluss retry/pool helpers** (`BoundedRetry`, `WriteAwait`, `FlussHandlePool`):
  used by several durable stores — not a single-implementation abstraction.
- **`TokenSetHash`** exists in both `common` and the ingestion service but is not
  duplication: common hashes whole/slot token sets, ingestion does slot carving
  and verdicts.
- **Store and strategy interfaces**: multiple production implementations or
  actively wired test seams.
- **Dependency hygiene is already deliberate** — `Cargo.toml` carries
  rg-verified pruning comments (e.g. `tokio` `fs` dropped, P3-174), and
  `pipeline-lib.sh` forwards env with defaults rather than duplicating config.

## Where the real upside is (measured, not guessed)

The optimisation work is in runtime levers, not deletions; the full list with
file:line evidence is the `### Deferred levers — read-only sweep 2026-10-04`
block in `docs/plans/2026-10-03-p99-50ms-combined-plan.md`. Ranked by expected
effect:

1. **Latency** — in-DAG transit (3–5 hops × 10 ms buffer, F2/F4), source fetch
   window (F1), tablet append-ack p99 42 ms (F3), TM GC (101 pauses ≥ 15 ms per
   900 s against a 20 ms target), ingestion micro-batching off
   (`INGESTION_MAX_BATCH_RECORDS=1`), JFR recording inside the measured window.
2. **State growth** — actually storage growth, on the Fluss side: `raw_table_1`
   +179 MB/min (3 d TTL ⇒ ~770 GB at dev rate, ~7.7 TB at production rate)
   against 316 GB free; `Signal_Candidates_current` 1.9 GB is ~100 % `log-N`
   changelog segments with a 7 d TTL; idle `.index`/`.timeindex` preallocation
   holds ~460 MB on near-idle tables; `ingestion_quarantine` has no TTL. Flink's
   own state is small (RocksDB 4.3 MB/subtask) — the cost there is *churn*
   (`docs/plans/2026-09-30-w3d-state-churn-design.md`).

Conclusion: a "delete code" audit buys little here. Trimming retention/TTLs,
batching the ingest writes, and aligning the checkpoint profile with the
certified production one are the levers with measured headroom.
