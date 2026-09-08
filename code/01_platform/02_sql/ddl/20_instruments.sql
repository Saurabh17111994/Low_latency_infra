-- instruments: Manifest table — current and prior instrument manifest versions
-- Owner: Operators
-- Type: KV (primary key on instrument_token, manifest_version)
-- Bucket key: instrument_token (strict subset of the PK — per-instrument
--   colocation of all manifest versions; matches the DdlBootstrap fallback and
--   the feature_candles_15s pattern. With kv.format-version=2 the raw Fluss
--   client can upsert the composite PK (verified 2026-08-15).)
-- Schema version: 3 (v2 -> v3: bucket.key narrowed to instrument_token +
--   table.kv.format-version=2 so a raw-client operator loader can write)
--
-- v2 (2026-08-03, review R-090): composite PK. The header claimed "current AND
-- prior manifest versions" but the single-column key made it a one-row-per-
-- instrument upsert — a new manifest silently overwrote the prior version.
-- (instrument_token, manifest_version) retains every loaded manifest version.
-- Retention: current and prior manifest versions
-- Scope: GLOBAL (P4-065: exchange reference data — same manifest for all
--   accounts. The header previously invited per-account WHERE filters with no
--   column; there is no per-account manifest. If per-account manifests ever
--   appear, the PK/bucket key need account_scope_id + a recreate.)
--
-- Loader contract (P4-066 — enforced in InstrumentManifestWriter.ManifestEntry
-- compact-ctor + unit tests, never in DDL): lot_size > 0; tick_size_paise
-- NULL-or->0; option_type CE|PE iff instrument_type=OPT (required iff OPT,
--   forbidden otherwise); expiry NULL iff EQUITY. Single-is_active-TRUE per
--   instrument_token is LOADER discipline (deactivate-prior on load) — the
--   composite PK cannot prevent two active rows; enforced in load sequencing,
--   not storage.
-- Units (P4-218): expiry + loaded_ts epoch-millis UTC (frozen names — no
--   _ms rename; documented here, not in the column). expiry NULL =
--   non-expiring (EQUITY). manifest_version monotonic per loader.
-- Shape guard (P4-219): kv.format-version=2 + bucket.key=instrument_token
--   (single-field PK subset) is machine-pinned TWICE — writer preflight
--   (requireKvFormatVersion + PK/bucket asserts, fail-fast before any write)
--   and COMPAT-FLUSS-005 (CompatFlussCompositeKeyIntegrationTest). No header-
--   only claim: a wrong table fails at connect, not at first upsert.
-- Snapshot contract (P4-220): KV snapshot retains every loaded version
--   (unbounded by design — "current and prior" is the LOADER's retention,
--   not storage's); 90d log TTL bounds changelog replay only. Long-term
--   audit rides the manifest CSVs in version control, not a lake twin;
--   bootstrap older than 90d replays from the CSVs, not the log.

CREATE TABLE instruments (
    instrument_token        BIGINT      NOT NULL,
    trading_symbol          STRING      NOT NULL,
    exchange                STRING      NOT NULL,
    segment                 STRING      NOT NULL,
    instrument_type         STRING,
    lot_size                INT         NOT NULL,
    tick_size_paise         BIGINT,
    strike_paise            BIGINT,
    expiry                  BIGINT,
    option_type             STRING,
    manifest_version        INT         NOT NULL,
    is_active               BOOLEAN     NOT NULL,
    loaded_ts               BIGINT      NOT NULL,
    schema_version          STRING      NOT NULL,
    PRIMARY KEY (instrument_token, manifest_version) NOT ENFORCED
) WITH (
    'bucket.num' = '4',
    'bucket.key' = 'instrument_token',
    'table.kv.format-version' = '2',
    'table.log.ttl' = '90d'
);
