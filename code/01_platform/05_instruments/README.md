# Instrument Manifest

Place instrument CSV files here. The Ingestion service loads instruments from this directory at startup.

## Expected CSV format

Arrow Trade `GET /all` or `GET /nse` format:

```
Exchange,Segment,ExchSeg,Token,FullName,Symbol,TradingSymbol,Series,ISIN,LotSize,TickSize,PricePrecision,OptionType,Underlying,UnderlyingToken,StrikePrice,Expiry,FreezeQty,Lower Band,Upper Band,SurCodes,Events,ExchangeID
```

Or a simplified format with at minimum: `token`, `exchange`, `symbol`, `trading_symbol`, `lot_size`, `tick_size`.

## Files

| File | Purpose |
|------|---------|
| `instruments.csv` | Active instrument manifest loaded at Ingestion startup |

## Status

- **Current:** `instruments.csv` is not yet present. Ingestion uses a **TEST-ONLY** stub (50 random tokens) for local development.
- **Production:** Drop the real instrument CSV here before live trading. See `docs/09_data_gaps.md` DATA-GAP-001.

## Safety guard

When `EXECUTION_ENABLED=true`, the Ingestion service SHALL fail startup if the active instrument manifest is the TEST-ONLY stub or is otherwise absent. The TEST-ONLY stub SHALL log a `WARN`-level message on every startup identifying itself as a development fixture. Production readiness SHALL remain false until a validated production manifest is loaded.

## References

- Ingestion requirement: [REQ-ING-004](/docs/02_requirements/02-functional/01-ingestion.md)
- Data gap: [DATA-GAP-001](/docs/09_data_gaps.md)
- Import: the ingestion service loads the approved manifest — `InstrumentManifestWriter`
  (`INSTRUMENT_MANIFEST_PATH`; the manifest version is a loader parameter, default 1).
  The old [import_instruments.sh](/code/01_platform/04_scripts/import_instruments.sh) is **retired**
  (CHG-229) — it refuses with exit 2 and cannot write the live 14-column KV table.

## T0 3-slot parameterization (streaming-3000)

- Default N=1 (1,024) via `ARROW_HFT_CONNECTIONS=1` — live manifest at `Arrow_broker/instruments/cash_stocks/NSE_CM_EQUITY (1024).csv`.
- Premium N=3 shards 3000 as 1024+1024+952 without code rewrite: set `ARROW_HFT_CONNECTIONS=3` and either
  - single 3000-row CSV at `INSTRUMENT_MANIFEST_PATH` (auto-sharded by `BuildSubscriptionPlan`), or
  - three per-slot CSVs `slot1.csv`/`slot2.csv`/`slot3.csv` and `ARROW_INSTRUMENT_MANIFESTS=/instruments/host/slot1.csv,...` (T1 will wire the list; until then use single-CSV sharding).
- See [`manifests/README.md`](manifests/README.md) and helper [`split_manifest.py`](split_manifest.py) for splitting.
- Compose mounts: `x-manifest` (single file, env `INSTRUMENT_MANIFEST_HOST_PATH`) + `x-manifest-dir` (directory at `/instruments/host`, env `INSTRUMENT_MANIFESTS_HOST_DIR`).
- Swarm: bake manifests under `/instruments` and set the same env in `docker-stack.yml`.

