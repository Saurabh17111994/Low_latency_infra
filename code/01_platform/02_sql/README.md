# Fluss DDL — reconciled proposals pending version validation

> **Status:** logical schemas are reconciled to current decisions, but the SQL is not approved for runtime application until the pinned Fluss/Flink capability suite passes.
>
> **Implementation contract:** [`../../../docs/08_implementation/01-foundation.md`](../../../docs/08_implementation/01-foundation.md)

## Current DDL set

| File | Logical object |
| --- | --- |
| `02_raw_table_1.sql` | `raw_table_1` — immutable raw packet/tick LOG |
| `05_signal_candidates.sql` | `Signal_Candidates` — candidate audit LOG v3 (2026-08-13, DEC-035): append-only, one row per fired signal, never updated, `bucket.key=instrument_token`; supersede columns retained for audit linkage |
| `06_ranking_results.sql` | `Ranking_Results` — immutable ranking audit LOG |
| `07_trade_decisions.sql` | `Trade_Decisions` — immutable instruction feed LOG |
| `08_fills.sql` | `Fills` — immutable broker postback/fill LOG |
| `09_order_lifecycle.sql` | `Order_Lifecycle` — broker-order lifecycle KV |
| `10_positions.sql` | `Positions` — fill-derived position KV |
| `11_execution_gate.sql` | `Execution_Gate` — executor gate KV |
| `12_execution_attempts.sql` | `Execution_Attempts` — executor attempt KV |
| `13_order_correlation.sql` | `Order_Correlation` — identity mapping KV |
| `14_execution_audit.sql` | `Execution_Audit` — immutable execution/safety audit LOG |
| `15_portfolio_reservations.sql` | `Portfolio_Reservations` — portfolio-capacity reservation KV |
| `16_postback_quarantine.sql` | `Postback_Quarantine` — immutable ambiguous/invalid postback LOG |
| `17_postback_projection_ledger.sql` | `Postback_Projection_Ledger` — projection progress and recovery KV |
| `18_safety_halt_requests.sql` | `Safety_Halt_Requests` — safety-halt KV |
| `19_suspected_discontinuities.sql` | `suspected_discontinuities` — feed discontinuity evidence LOG |
| `20_instruments.sql` | `instruments` — versioned instrument manifest KV |
| `21_ingestion_quarantine.sql` | `ingestion_quarantine` — immutable ingestion quarantine LOG |
| `23_signal_candidates_current.sql` | `Signal_Candidates_current` — signal KV current-state, PK `(instrument_token)`, latest/active per instrument, supersession overwrites; 22-column twin of `Signal_Candidates` (DEC-035) |
| `24_fingerprint_dedup.sql` | `fingerprint_dedup` — DEC-038 dedup state KV (retained unused since CHG-022/023; Flink MapState is authoritative) |
| `25_trade_instruction_state.sql` | `trade_instruction_state` — SCH-19 instruction-index KV |
| `26_eod_offload_state.sql` | `eod_offload_state` — SCH-23 EOD offload-state KV |
| `27_execution_intent.sql` | `Execution_Intent` — trade-intent feed LOG |
| `28_execution_intent_processed.sql` | `Execution_Intent_Processed` — processed-intent KV |
| `29_position_state.sql` | `Position_State` — active-position feedback KV |
| `32_candle_live.sql` | `candle_live` — live per-timeframe candle KV (6 TFs, upserted every 1s, 60s TTL) |
| `33_candle_closed.sql` | `candle_closed` — closed per-timeframe candle KV (immutable, 7d TTL) |

## Validation required before application

- Exact Fluss server/client and Flink connector versions.
- DDL dialect parse and clean apply.
- Effective schema/options inspection.
- `BYTES`, LOG, KV, bucket-key, and primary-key behavior.
- `partial_update` and FULL changelog behavior.
- Checkpoint, restore, and partial-visibility behavior.
- Non-null routing keys for all accepted events.
- Retention extension and EOD offload verification.
- Replication/quorum/placement.
- Encrypted long-term audit retention.

## Important rules

- The old `Trade_management_table`, overloaded `order_id`, `seq_no`, and exact `gaps` model are superseded.
- LOG-table comments do not enforce immutability; writer protocols and tests must.
- `partial_update` does not enforce stale-write or state-transition rules.
- A fixed TTL does not implement retention extension while offload is unverified.
- `make ddl` must not be treated as valid until it follows the schema lifecycle dossier and references the full current set.

## References

- Logical data contract: `../../../docs/02_requirements/04-data.md`
- Storage requirements: `../../../docs/02_requirements/02-functional/02-storage.md`
- Storage contract: `../../../docs/04_contracts/02-storage.md`
- Schema lifecycle: `../../../docs/08_implementation/01-foundation.md`
- Compatibility matrix: `../../../docs/08_implementation/01-foundation.md`
