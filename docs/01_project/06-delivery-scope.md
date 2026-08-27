# Delivery Scope and Evolution

## MVP Phase 4.2

The MVP proves the complete path with conservative safety boundaries.

### Must deliver

- Local Docker Compose environment
- Production Docker Swarm deployment definition
- Arrow market-data ingestion and normalized raw payload preservation
- Best-effort bounded fingerprint deduplication
- Event-time 15-second candle computation
- Forming-bar signal detection
- ~~In-operator ranking and portfolio constraints~~ — **REMOVED 2026-08-15 (CHG-005)**
- Immutable candidate audit records (**ranking audit REMOVED 2026-08-15, CHG-005**)
- ~~`Trade_Decisions` winner feed~~ — **REMOVED 2026-08-15 (CHG-005)**
- Durable Executor order gate and attempt ledger
- Arrow REST order submission adapter
- Three-ID order correlation and broker reconciliation
- Independent postback/fill capture
- Babysitter job wired as a checkpointed no-op
- Structured logs, metrics, traces, health checks, and operational alerts
- Minimum MVP safety alert thresholds, routing, ownership, acknowledgement targets, and response runbooks
- Evidence sufficient to reconstruct each live-money acceptance gate from observability and audit data
- Verified EOD Iceberg offload with a retention safety buffer

### MVP acceptance gates

1. A variable **50,000 ticks/s average baseline** (no instrument above 20 ticks/s) is sustained for its declared test window without acknowledged data loss. The **60,000 ticks/s sustained gate** (3,000 × 20, DEC-045) is the acceptance target; the **90,000 ticks/s peak** campaign is retired (DEC-036).
2. Trigger-tick-to-winner p99 meets the agreed target at the variable 50,000 ticks/s average baseline.
3. No duplicate broker order in crash-window fault-injection tests.
4. Order gate halts within five seconds of every defined uncertainty trigger.
5. One production workload VM can fail without violating the documented durability posture at the declared peak profile.
6. Failed EOD offload retries successfully before any source data expires.
7. Every broker postback can be correlated to an instruction and broker order, or is quarantined for reconciliation.

## Phase 4.3

- Advanced feature columns and pattern features
- Real babysitter trim, exit, trailing-stop, and re-entry logic
- ~~Market-context ranking inputs~~ — **REMOVED 2026-08-15 (CHG-005)**
- Sector and exposure constraints
- Current-price input for position management

## Phase 4.4

- Out-of-band suspected-discontinuity investigation and historical reconciliation
- Configuration hot reload where operationally justified
- ~~Ranking experiment support~~ — **REMOVED 2026-08-15 (CHG-005)**
- Mature order/position reconciliation tooling

## Phase 4.5

- Tune and finalize alert thresholds and routing from production-like evidence
- Mature SLO dashboards beyond the MVP safety views
- Expand operator runbooks beyond mandatory MVP safety procedures
- Expanded chaos and disaster-recovery exercises

## Phase 4.6

- Backfill existing Parquet history into Iceberg
- Validate counts/checksums and retire the old history source

## Phase 4.7

- Multi-broker adapters
- BSE and currency derivatives
- Charting and end-user alerts
- Backtesting
- P&L, win-rate, slippage, and strategy analytics
- Lake pruning and compaction policy
- ~~ML-based ranking~~ — **REMOVED 2026-08-15 (CHG-005)**

## Change-control rule

A deferred capability moves into an earlier phase only when its owner, contract, acceptance test, operational cost, and effect on the live-money safety boundary are documented. Updating this roadmap alone does not change authoritative schemas or interfaces.

## Related plans

- Requirements index: [`../02_requirements/00-index.md`](../02_requirements/00-index.md)
- Test strategy: [`../08_implementation/11-testing-and-release.md`](../08_implementation/11-testing-and-release.md)
- Deployment strategy: [`../05_deployment/00-release-strategy.md`](../05_deployment/00-release-strategy.md)
