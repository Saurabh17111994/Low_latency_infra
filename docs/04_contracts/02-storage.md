# Segment Build Contract — Storage

## Authority and status

Physical DDLs must implement the logical model in `../02_requirements/04-data.md`. Existing DDLs are stale if they use `seq_no` as required identity, ordering, deduplication, or gap evidence; use overloaded `order_id`; ~~assign ownership to a separate Ranking deployment~~ (**REMOVED 2026-08-15, CHG-005**); allow Executor/Arrow REST to mutate instructions; or retain raw data for only one day. A nullable broker sequence field may remain only as observational evidence and cannot support a guarantee.

Exact Fluss DDL properties are version-gated; unsupported syntax is not invented in this contract.

## Production topology

Three Fluss replicas/quorum are placed across the three workload VMs with anti-co-location (v1: the three Manager+Worker VMs; v2: the three+ Worker VMs — Fluss data placement is independent of the Swarm manager/worker split; see 09 v1→v2). One-VM-loss tests at the variable 50,000 ticks/s average baseline are mandatory. (The 90,000 ticks/s peak is retired, DEC-036; 60,000 ticks/s gate, DEC-045.)

## Required schemas

Market: `raw_table_1` (**v3 since 2026-08-31, CHG-117: 21 columns, `event_day STRING` (yyyyMMdd, Asia/Kolkata) first, `PARTITIONED BY (event_day)` with auto-partition DAY (precreate 2 / retention 7d), bucket key `instrument_token` (16 buckets), `table.datalake.enabled=true` — tiering to the R2 iceberg lake is live, day-partitioned layout; per-day restore and lake queries via `06_operations/07-lake-archive-ops.md`**), `feature_candles_15s` (KV upsert, PK `(instrument_token, window_start)` — sole candle output, 2026-08-13 conversion; authoritative durable candle state under DEC-038), `suspected_discontinuities`, `instruments`. (The pre-conversion `feature_candles_15s_current` KV projection is RETIRED 2026-08-13.)

Strategy: `Signal_Candidates` (immutable append-only LOG — RE-SCOPED 2026-08-13, **implemented 2026-08-13 — LOG v3 id 607**; one row per fired signal), `Signal_Candidates_current` (candidate current-state KV, PK `(instrument_token)`, supersession overwrites — NEW 2026-08-13, **implemented 2026-08-13 — KV companion id 608**). ~~`Ranking_Results`, immutable `Trade_Decisions`~~ — **REMOVED 2026-08-15 (CHG-005 — ranking/decisions out of scope, not deferred).**

Signal state ownership (DEC-038): `feature_candles_15s`, `Signal_Candidates`, and `Signal_Candidates_current` are **Fluss-owned authoritative state**; Flink checkpoints never carry a second full copy of them. A **fingerprint-dedup KV state table** (proposed name `fingerprint_dedup`) is the authoritative dedup set: key `(instrument_token, fingerprint_version, event_fingerprint)`, value `(first_seen_ms, expiry_ms)`, `bucket.key = instrument_token`, owner Signal job, rebuild source `raw_table_1` replay within the dedup TTL, bounded growth via a tested expiry/cleanup mechanism (native per-key TTL exists in Fluss 1.0.0 — `table.kv.ttl`, not adopted on this design), and restart behavior = verify Fluss availability, then rehydrate the Flink working cache after a compact checkpoint restore. Design/DDL/tests land in a later stage — **IMPLEMENTED 2026-08-15 (CHG-003: DDL 24 + manifest + expiry/cleanup + tests + live wiring; DEC-038 completed)**; the contract fixes ownership, keys, and semantics — the full contract is formalized in [Dedup state table contract](#dedup-state-table-contract-dec-038) below. **SUPERSEDED 2026-08-17 (CHG-022 / DEC-040): the dedup set is authoritative Flink keyed state (Design B) — see the section banner below.**

Intent: `Execution_Intent` (immutable LOG; Signal-owned; distinct from the retired `Trade_Decisions` feed).

Order/position: `Fills`, `Order_Lifecycle`, `Positions`, `Postback_Projection_Ledger`, `Postback_Quarantine`.

Execution: `Execution_Gate`, `Execution_Attempts`, `Order_Correlation`, `Execution_Audit`.

Control: `Safety_Halt_Requests`. (`Portfolio_Reservations` REMOVED 2026-08-15, CHG-005.)

Future: `Position_Actions`.

Every table requires an explicit owner, schema version, retention policy, writer/column ownership matrix, and applicable scope identity (`account_scope_id`, `portfolio_id`, or `execution_partition_id`) before DDL generation.

## Dedup state table contract (DEC-038)

> **SUPERSEDED (2026-08-17, CHG-022 / DEC-040):** the dedup arm of DEC-038 is
> superseded — the fingerprint dedup set is authoritative **Flink keyed state**
> (Design B: `MapState` keyed `version|token|fingerprint` + expiry index + one
> event-time timer per entry — **2026-08-17, CHG-023 item 2: the index +
> timers are REMOVED — expiry is native `StateTtlConfig` on the MapState**),
> not a Fluss KV table. The `fingerprint_dedup`
> table, bounded cache, query-on-miss store, and dedup write path are removed;
> the DDL (`24_fingerprint_dedup.sql`) is retained on file but unused. This
> contract section is retained as the historical DEC-038 record. See
> `docs/05_deployment/change-records/CHG-022.md`.

The fingerprint-dedup KV state table (proposed name `fingerprint_dedup`) is the **authoritative durable dedup set**: the Signal job is its single writer owner, and any Flink-side copy is derived working state only. This contract fixes ownership, keys, and semantics now; the physical DDL, the cleanup mechanism, and measured sizes land in the DEC-038 implementation stage and remain evidence-gated where noted below.

| Aspect | Contract |
| --- | --- |
| Authority | Fluss KV state table — authoritative for duplicate determination. The Flink cache is NEVER authoritative; on any cache-vs-Fluss disagreement, Fluss wins. |
| Key | `(instrument_token, fingerprint_version, event_fingerprint)`; `bucket.key = instrument_token` for per-instrument colocation. |
| Value | `(first_seen_ms, expiry_ms)` only. SHALL NOT contain raw bytes, decoded raw fields, candle values, candidate values, or event objects. |
| Update semantics | First-seen insert/upsert; a later event with the same key inside the logical TTL is a duplicate. A cache "unseen" SHALL NOT accept an event that authoritative Fluss state records as seen. Exact atomic/upsert connector semantics are evidence-gated, not assumed. |
| Logical TTL | `DEDUP_TTL_MS` exactly `60000` (1 minute); deployment rejects any other value. Expiry instant = `first_seen_ms + TTL`; entries are never expired early. |
| TTL ownership | Fluss TTL/expiry = logical correctness and authoritative-state lifecycle. Flink cache eviction = performance optimization only and SHALL NOT determine whether an event is logically a duplicate. |
| Cleanup | Native per-key TTL exists in Fluss 1.0.0 (`table.kv.ttl`) but is not adopted on this table, so cleanup keeps the explicit expiry-column + cleanup path that is designed and tested — the mechanism is evidence-gated, not assumed. Growth is bounded: entries = accepted rate × TTL horizon. |
| Bounded Flink cache | The working cache SHALL have explicit maximum-entry and/or maximum-byte bounds that are independent of total Fluss dedup cardinality, the 1-minute TTL (CHG-116), and instrument count; it SHALL NOT become a complete mirror of the dedup table. |
| Read/write behavior | Hot path: bounded cache lookup — no per-tick Fluss round trip. Fluss point lookups are permitted on cache miss, recovery, and explicit state verification. Durable dedup writes are batched/async; the hot path SHALL NOT become tick → Fluss read → Flink → Fluss write per tick. |
| Rebuild source | `raw_table_1` replay within the dedup TTL horizon — **exceptional controlled rebuild only** (Fluss state missing, corrupt, incompatible, or otherwise untrustworthy), never a normal restart path. |
| Versioning | Explicit schema/serialization version pinned to the connector; compatibility is preflighted at startup and changes are additive-only or fail closed before use. |
| Restart behavior | Restore the compact Flink checkpoint → verify Fluss dedup-table availability and compatibility (startup preflight) → rehydrate only the bounded working cache → resume from recovered source offsets. Fluss unavailability/incompatibility fails the job closed / degrades safely — never treated as an empty dedup set. |
| Failure semantics | Cache miss → consult Fluss (point lookup / authoritative state feed). Cache stale → Fluss wins. Fluss unavailable → fail closed/degraded. Fluss write uncertain → do not claim first-seen success until tested durable semantics are satisfied. Fluss state loss → exceptional controlled rebuild only. |
| Checkpoint invariant | The checkpoint carries only bounded working-cache metadata — never a second complete copy of the dedup set; checkpoint size scales with bounded working/recovery state, not Fluss dedup cardinality. |
| Anti-regression | No new Flink managed-state structure may be introduced for this Fluss-authoritative domain unless explicitly classified as bounded transient working state with an explicit entry/byte bound. |

Restart/recovery and checkpoint behavior that depends on exact connector semantics (upsert acknowledgment, changelog visibility, snapshot consistency) is evidence-gated by `SIG-STATE-001` to `SIG-STATE-003` and `STATE-COMPAT-001` until the pinned-version integration tests pass.

## Retention and lake

Eligible live event tables keep at least three complete trading days and extend retention while an EOD manifest is unverified, retryable, or under reconciliation. Execution, order, fill, gate, correlation, approval, reconciliation, and future position-action audit is immutable, encrypted, integrity-verifiable, access-controlled, and retained for at least one year or longer under the approved policy in Iceberg/S3. **WORM-equivalent control (2026-08-14): the object store is Cloudflare R2; immutability is enforced by an R2 'bucket lock' rule configured via the Cloudflare dashboard/Wrangler/API. The S3 Object Lock API is not implemented on R2. Provisioning and verification: `code/01_platform/04_scripts/audit_r2.py provision --set-lock` / `validate` (requires `CLOUDFLARE_API_TOKEN` + `CLOUDFLARE_ACCOUNT_ID`).**

Each manifest includes schema/table version, date/source range, rows/bytes, hashes/checksums, commit ID, verification state, and retries.

## Consistency

LOG append is at-least-once unless a pinned producer test proves a narrower retry case. KV projections use source event/version and state precedence. Independent writes are reconciled; cross-table atomicity is not assumed. Projections must persist or recover a durable pending/completion marker so audit, lifecycle, position, quarantine, and execution state can be replayed idempotently after partial failure.

## EOD controller

The EOD controller is a named service or scheduled job owning manifest creation, verification, retry/backoff, retention extension, expiry protection, storage-pressure alerts, and manual reconciliation. Its state is durable with restart/resume behavior. Source data for a trading day SHALL not expire while the manifest is unverified, retryable, or under reconciliation.

## DDL migration gate

Because the project is pre-production, replace stale tables through a clean-break migration. Before applying DDLs: pin Fluss version, validate types/properties, review destructive changes, define reset/replay, and pass schema parity tests.

## Requirement traceability

- Functional: `REQ-FLS-001` through `REQ-FLS-017`
- Cross-cutting: `03-non-functional.md` §§3.2–3.4, 3.6–3.8; `04-data.md` §§4.1–4.7; `05-interfaces.md` §§5.1, 5.3, 5.5, 5.11; `06-operational.md` §§6.5–6.6, 6.10

See `../02_requirements/02-functional/02-storage.md` and `../02_requirements/04-data.md`.
