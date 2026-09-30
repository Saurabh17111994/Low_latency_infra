-- raw_table_1: Immutable LOG — every accepted market tick
-- Owner: Ingestion
-- Type: LOG (no primary key)
-- Bucket key: instrument_token
-- Retention: 3 calendar days via table.log.ttl (2026-09-30, Wave A/A2 — was 9d;
-- operator Q36 re-scoped the live replay window to <= 24h with backfills from
-- the lake). The block-delete-unverified guard is the EodControllerTool extend
-- path (planTables/extend: one ALTER raising table.log.ttl AND
-- auto-partition.num-retention on partitioned tables; 1d runway default), not a
-- Fluss-side lock — see EodControllerTool "extend". Do not lower this or
-- num-retention without the controller guard in place).
-- Lake: EOD Iceberg offload (R-011: datalake options restored — they were
-- dropped in a rewrite while the header still claimed offload)
-- Scope: none (global market data, not per-account — P4-013: raw ticks carry
-- no account_scope_id column; no per-account projection is possible)
-- Schema version: 4
-- v4 (2026-09-24, full-mode field capture): the bridge already carried every
-- full-mode field and this table stored only 4 of them. The 51 columns at
-- indexes 21-71 now capture all of them, so ONE schema serves BOTH feeds (the
-- standard free-plan stream and HFT). Appended AFTER schema_version so indexes
-- 0-20 never move -- the positional contract spans common/RawTableSchema,
-- compute/RawTableColumns (boot-refusing static validator) and both converters.
-- All NULL: pre-v4 rows have no value, and the feeds populate different subsets.
-- NULL means "this feed does not provide it", never a silent 0.
-- Depth is flat (5 levels x px/qty/ord, no arrays) so sum(bid_qty_*) stays a
-- plain query. volume_delta is the per-tick traded quantity (difference of the
-- cumulative Volume; 0 = no trade, NULL = baseline unknown); candle volume must
-- sum THAT, never last_qty. Feed coverage: change_flag, oi_day_high,
-- oi_day_low, imbalance_qty, indicative_close_paise, ref_price_paise = standard
-- stream only; atv, btv = HFT only; the other 42 columns come from both.
--
-- v3 (2026-08-31, daily-partition migration — docs/plans/2026-08-31-r2-daily-
-- partitioning-and-archive-plan.md): event_day added as FIRST column and
-- partition key. Fluss auto-partition creates day partitions (yyyyMMdd,
-- Asia/Kolkata) and expires Fluss-side after num-retention; the tiering job
-- mirrors them as iceberg identity partitions -> R2 layout
-- lake/default/raw_table_1/data/event_day=YYYYMMDD/... Partition rules are
-- CREATE-time-frozen (verified: MetadataManager has no partition ALTER path)
-- — this table was RECREATED for v3; prior data archived in R2 under
-- lake/_stale-20260831-v1/. Ingestion fills event_day from event_time (IST).
-- Partition key must be STRING (IcebergLakeCatalog rejects other types);
-- value MUST be exactly yyyyMMdd to match auto-partition naming — a wrong
-- format silently lands rows in a second, non-auto partition (smoke GUARD E
-- asserts folder == today's IST date).
--
-- v2 (2026-08-03, review R-054/R-231): removed the 8 columns the ingestion
-- path never populates — quote fields (bid_price_paise/bid_qty/ask_price_paise/
-- ask_qty, hardcoded 0 by RealFlussRowConverter) and option metadata
-- (instrument_type/strike_paise/expiry/option_type, always empty/null).
-- The DDL must tell the truth: these columns return when the bridge
-- carries quote/derivative data. Column count drops 28 -> 20 (+1 in v3).
--
-- Late/out-of-order ticks (P4-170): event_day derives from event_time
-- (EventDay.ofValidated — format-checked, NOT recency-checked). A tick with
-- an old event_time writes to its old partition, NOT today's — by design
-- (audit fidelity: the tick belongs to its event day). There is NO late-tick
-- quarantine path and NO ingest_ts derivation — a partition older than the
-- 3d retention is already GC'd and the write fails visibly, never silently.
-- Backfill older than retention is unsupported (replay the day's lake data).
-- ack_ts (P4-171): NULL-allowed in DDL (frozen nullability), but BOTH live
-- converters write 0L for unknown (TypedFlussRowConverter + generic 0L path,
-- P1-061) — readers MUST treat NULL and 0 as the same unknown, never
-- WHERE ack_ts = 0 alone. Iceberg stats may show both; that is known.
-- raw_payload lake cost (P4-172): inline BYTES on every tick IS the design —
-- the lake holds full replayable blobs (r2-restore proves a day: 2.29M rows
-- → 313MB parquet). No hash-pointer refactor, no compaction thresholds
-- beyond auto-compaction=true — revisit only on measured R2 cost, not on theory.
-- validity invariant (P4-173): the converters write "" (empty, not NULL)
-- when no reason; NULL arises only from legacy/generic paths. Readers MUST
-- treat NULL and "" identically:
--   invalid <=> validity_state NOT IN ('VALID_TRADE','VALID_NON_TRADE')
--   (reason may be "" when unset — writer SHOULD set it, but its absence never
--   flips the verdict).
-- Corrected 2026-09-13: this said <> 'VALID', which no writer has ever emitted —
-- MarketTick.isValid() and ValidityClassification stamp only the two names
-- above, and compute's RawValidationFunction rejects every other value, so a
-- reader trusting the old text would have dropped all real rows.
-- schema_version column (P4-327): per-row BRIDGE payload version (converters
-- stamp packet.schemaVersion), NOT the DDL header version (table layout v3).
-- Same name, different domains by design — frozen name; rename needs a
-- recreate + converter + bootstrap + reader change for zero behavioral gain.

CREATE TABLE raw_table_1 (
    event_day               STRING      NOT NULL, -- yyyyMMdd IST from event_time; validated in bridge (EventDay.of+validate) + smoke GUARD E (Fluss has no CHECK)
    event_fingerprint       STRING      NOT NULL, -- not unique in LOG; dedup downstream via fingerprint_dedup + event_fingerprint (P4-169)
    fingerprint_version     STRING      NOT NULL,
    connection_id           STRING      NOT NULL,
    connection_epoch        BIGINT      NOT NULL,
    instrument_token        BIGINT      NOT NULL,
    exchange                STRING      NOT NULL,
    symbol                  STRING      NOT NULL,
    event_time              BIGINT      NOT NULL,
    ingest_ts               BIGINT      NOT NULL,
    ack_ts                  BIGINT      NULL, -- 0 = unknown (R-010)
    tick_type               STRING      NOT NULL,
    last_price_paise        BIGINT      NOT NULL,
    last_qty                BIGINT      NOT NULL,
    raw_payload             BYTES       NOT NULL,
    payload_hash            STRING      NOT NULL,
    decoder_version         STRING      NOT NULL,
    protocol_version        STRING      NOT NULL,
    validity_state          STRING      NOT NULL,
    validity_reason         STRING,
    schema_version          STRING      NOT NULL,
    open_paise               BIGINT      NULL, -- day open
    high_paise               BIGINT      NULL, -- day high
    low_paise                BIGINT      NULL, -- day low
    close_paise              BIGINT      NULL, -- PREVIOUS close, not today's
    vwap_paise               BIGINT      NULL, -- MarketTick.AvgPrice / HFT VWAP
    volume                   BIGINT      NULL, -- CUMULATIVE day volume
    volume_delta             BIGINT      NULL, -- traded qty since the previous tick for this token; 0 = no trade; NULL = unknown (first tick / after reconnect). Sum THIS for candle volume, never last_qty
    total_buy_qty            BIGINT      NULL, -- TBQ
    total_sell_qty           BIGINT      NULL, -- TSQ
    open_interest            BIGINT      NULL, -- OI
    bid_px_1                 BIGINT      NULL,
    bid_px_2                 BIGINT      NULL,
    bid_px_3                 BIGINT      NULL,
    bid_px_4                 BIGINT      NULL,
    bid_px_5                 BIGINT      NULL,
    bid_qty_1                BIGINT      NULL,
    bid_qty_2                BIGINT      NULL,
    bid_qty_3                BIGINT      NULL,
    bid_qty_4                BIGINT      NULL,
    bid_qty_5                BIGINT      NULL,
    bid_ord_1                BIGINT      NULL,
    bid_ord_2                BIGINT      NULL,
    bid_ord_3                BIGINT      NULL,
    bid_ord_4                BIGINT      NULL,
    bid_ord_5                BIGINT      NULL,
    ask_px_1                 BIGINT      NULL,
    ask_px_2                 BIGINT      NULL,
    ask_px_3                 BIGINT      NULL,
    ask_px_4                 BIGINT      NULL,
    ask_px_5                 BIGINT      NULL,
    ask_qty_1                BIGINT      NULL,
    ask_qty_2                BIGINT      NULL,
    ask_qty_3                BIGINT      NULL,
    ask_qty_4                BIGINT      NULL,
    ask_qty_5                BIGINT      NULL,
    ask_ord_1                BIGINT      NULL,
    ask_ord_2                BIGINT      NULL,
    ask_ord_3                BIGINT      NULL,
    ask_ord_4                BIGINT      NULL,
    ask_ord_5                BIGINT      NULL,
    change_flag              BIGINT      NULL, -- standard stream only; HFT has no equivalent
    oi_day_high              BIGINT      NULL, -- standard stream only
    oi_day_low               BIGINT      NULL, -- standard stream only
    lower_limit_paise        BIGINT      NULL, -- MarketTick.LowerLimit / HFT DprL
    upper_limit_paise        BIGINT      NULL, -- MarketTick.UpperLimit / HFT DprH
    imbalance_qty            BIGINT      NULL, -- CAS trailer, standard stream only, ~15:15 IST onward
    indicative_close_paise   BIGINT      NULL, -- CAS trailer, standard stream only
    ref_price_paise          BIGINT      NULL, -- CAS trailer, standard stream only
    last_traded_time         BIGINT      NULL, -- EPOCH MS: standard LTT is seconds (x1000), HFT LTT is microseconds (/1000)
    atv                      BIGINT      NULL, -- HFT only
    btv                      BIGINT      NULL -- HFT only
) PARTITIONED BY (event_day) WITH (
    'bucket.num' = '16',
    'bucket.key' = 'instrument_token',
    'table.log.ttl' = '3d', -- 2026-09-30 (A2): 3d live window; the EOD guard extends unverified days (1d runway)
    -- daily partitions: IST day boundary; retains last 3 closed IST days + 2
    -- precreated extra (P4-015: live + precreated = up to 5 physical; the EOD
    -- job plans against the unverified day's bound so the ±1-day IST-midnight
    -- skew between partition GC (day-granular) and log TTL (commit-time) is covered)
    'table.auto-partition.enabled' = 'true',
    'table.auto-partition.time-unit' = 'DAY',
    'table.auto-partition.num-precreate' = '2',
    'table.auto-partition.num-retention' = '3', -- 2026-09-30 (A2): matches log.ttl 3d (partition GC alone must not drop before VERIFIED)
    'table.auto-partition.time-zone' = 'Asia/Kolkata',
    'table.datalake.enabled' = 'true',
    'table.datalake.format' = 'iceberg',
    'table.datalake.freshness' = '5min',
    'table.datalake.auto-compaction' = 'true'
);
