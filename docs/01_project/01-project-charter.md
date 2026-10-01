# Project Charter

## Metadata

| Field | Value |
| --- | --- |
| Document ID | DOC-PROJ-CHARTER-001 |
| Status | Active project summary; MVP Phase 4.2 |
| Owner | Platform Team |
| Updated | 2026-07-23 |

## Purpose

The Streaming Trading Data Platform ingests live Arrow Trade market data from the selectable Arrow WebSocket channels — the live standard token stream (`wss://ds.arrow.trade`, `ARROW_FEED=token`) or HFT (`wss://socket.arrow.trade`, `ARROW_FEED=hft`; the 2026-08-14 Standard-feed removal note describes the HFT-only period, ended 2026-09-24) — computes features and signals with Apache Flink, stores live events in Apache Fluss, executes approved instructions through Arrow's native REST API (`https://edge.arrow.trade`), captures postbacks via Arrow's order-updates WebSocket (`wss://order-updates.arrow.trade`), and preserves eligible history in Apache Iceberg on S3.

The platform separates the **data path** from the **order path**:

- The data path may auto-recover and tolerate bounded data gaps.
- The order path must stop placing new orders when state is uncertain and require reconciliation before resuming.

## Goals

1. Ingest the supported NSE, NFO, MCX, and INDEX market-data scope via Arrow's binary WebSocket protocol.
2. Build event-time OHLCV candles in Flink.
3. Detect signals on forming bars and rank active setups in the same Flink job.
4. Deliver only selected winners to a push-driven executor.
5. Capture broker order lifecycle independently from execution submission.
6. Provide a separately deployable babysitter job; MVP behavior is a no-op stub.
7. Keep immutable event/audit history separate from mutable operational state.
8. Provide measurable latency, throughput, recovery, and safe-halt behavior.

## In scope for MVP

- Arrow market-data ingestion (selectable binary channels: standard token stream 13/17/93/249 B big-endian, live; HFT 40/196 B little-endian zstd, selectable — the 2026-08-14 Standard-feed removal note describes the HFT-only period, ended 2026-09-24)
- Single Arrow Trade account; NSE, NFO, MCX, and INDEX segments
- Fluss raw and feature LOG tables
- Flink 2.2.1 deduplication, event-time windows, candle computation
- Stateful signal detection (**in-operator ranking REMOVED 2026-08-15, CHG-005**)
- `Signal_Candidates` (+ `Signal_Candidates_current` current-state KV) outputs (**`Ranking_Results` and `Trade_Decisions` REMOVED 2026-08-15, CHG-005**)
- Independent postback capture into `Fills` via `wss://order-updates.arrow.trade`
- Executor-to-Arrow order handoff (`POST /order/regular`); no OpenAlgo layer
- Babysitter wiring with no emitted actions
- Operational logs, metrics, health checks, and alerts
- EOD offload of eligible history to Iceberg/S3

## Non-goals for MVP

- Multi-broker support
- BSE, BFO, NCD, and BCD derivatives and currency segments
- OpenAlgo; the platform uses Nautilus for execution/position management and reaches Arrow only through the localhost go-arrow bridge
- Charting and end-user notification features
- 250+ feature columns and pattern-feature libraries
- Real-time gap reconciliation
- Strategy authoring or backtesting
- ML ranking (**REMOVED 2026-08-15, CHG-005**)
- Business analytics such as P&L and win rate
- Kubernetes deployment

Operational alerts remain in scope; **end-user trading alerts** are not.

## Source links

- Detailed context: [`../02_requirements/01-context-and-scope.md`](../02_requirements/01-context-and-scope.md)
- Functional requirements: [`../02_requirements/02-functional/`](../02_requirements/02-functional/)
- Data requirements: [`../02_requirements/04-data.md`](../02_requirements/04-data.md)
- Interfaces: [`../02_requirements/05-interfaces.md`](../02_requirements/05-interfaces.md)
