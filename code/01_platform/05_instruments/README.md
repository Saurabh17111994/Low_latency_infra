# Instrument Manifest

The instrument universe is loaded from a **real CSV manifest** at ingestion
startup. The loader is fail-closed: a missing or unreadable file, a CSV without
a `Token` column or a `TradingSymbol`/`FullName` symbol column, a non-positive
token, or fewer rows than `INSTRUMENT_MANIFEST_MIN_COUNT` refuses the load
instead of starting on a partial universe. No fixture manifest lives in this
directory.

## Loader contract

| Key | Reader | Meaning |
|-----|--------|---------|
| `INSTRUMENT_MANIFEST_PATH` | Java `InstrumentManifestLoader` | CSV path inside the container (default `/instruments/NSE_CM_EQUITY.csv`) |
| `ARROW_INSTRUMENT_MANIFEST` | Go bridge `main.go` | The same CSV for the subscription plan |
| `ARROW_INSTRUMENT_TOKENS` | Go bridge `main.go` | Explicit token list; wins over the CSV when set |
| `INSTRUMENT_MANIFEST_MIN_COUNT` | Java loader | Fail-closed minimum rows (dev default 1; the production stack pins 1024 — M4-1/SCH-22) |

## How the file reaches the container

**Dev (`docker-compose.yml`)**

- `x-manifest` anchor — one host file, `INSTRUMENT_MANIFEST_HOST_PATH`
  (defaults to the approved 1024-row CSV), mounted read-only at
  `/instruments/NSE_CM_EQUITY.csv`.
- `x-manifest-dir` anchor — the host directory `INSTRUMENT_MANIFESTS_HOST_DIR`,
  mounted read-only at `/instruments/host` (all host manifests, for the N=3
  path).

**Production (`docker-stack.yml`)**

- The manifest is a **Swarm config** (`manifest-nse`, sourced from
  `MANIFEST_FILE` at deploy time), mounted at `/instruments/NSE_CM_EQUITY.csv`.
- Configs are immutable: a revised manifest must be committed under a **new**
  config name, or a redeploy keeps the old bytes and silently serves stale
  instruments.
- `INSTRUMENT_MANIFEST_MIN_COUNT` pins 1024, so a truncated CSV cannot silently
  subscribe a subset.

## Expected CSV format

Arrow Trade `GET /all` or `GET /nse` format (header line included):

```
Exchange,Segment,ExchSeg,Token,FullName,Symbol,TradingSymbol,Series,ISIN,LotSize,TickSize,PricePrecision,OptionType,Underlying,UnderlyingToken,StrikePrice,Expiry,FreezeQty,Lower Band,Upper Band,SurCodes,Events,ExchangeID
```

Or a simplified format with at minimum: `token`, `exchange`, `symbol`,
`trading_symbol`, `lot_size`, `tick_size`.

## 3-slot parameterization (streaming-3000)

- Default N=1 (1,024) via `ARROW_HFT_CONNECTIONS=1` — the live manifest at
  `Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv`.
- Premium N=3 shards 3000 as 1024+1024+952 without a code rewrite: set
  `ARROW_HFT_CONNECTIONS=3` and point `INSTRUMENT_MANIFEST_PATH` at a single
  3000-row CSV — `BuildSubscriptionPlan` auto-shards it
  (`subscription_plan.go`).
- A per-slot CSV-list key (the plural form of `ARROW_INSTRUMENT_MANIFEST`) was
  removed (P5-012): no reader existed, and the single-CSV auto-shard supersedes
  it. Use [`split_manifest.py`](split_manifest.py) only when per-slot files are
  needed on disk (`slot1.csv`, `slot2.csv`, …).

## References

- Ingestion requirement: [REQ-ING-004](/docs/02_requirements/02-functional/01-ingestion.md)
- Loader: [InstrumentManifestLoader.java](/code/02_services/01_ingestion/src/main/java/com/trading/ingestion/InstrumentManifestLoader.java)
  (rows are written to the live KV table by `InstrumentManifestWriter`; the
  manifest version is a loader parameter, default 1).
- The old [import_instruments.sh](/code/01_platform/04_scripts/import_instruments.sh)
  is **retired** (CHG-229) — it refuses with exit 2 and cannot write the live
  14-column KV table.
