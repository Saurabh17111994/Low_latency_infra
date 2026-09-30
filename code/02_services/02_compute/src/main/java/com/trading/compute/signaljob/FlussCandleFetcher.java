package com.trading.compute.signaljob;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.BinaryString;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Native Fluss implementation of {@link CandleFetcher}: one exactly-keyed
 * lookup against the configured context table ({@code CANDLE_CONTEXT_TABLE} —
 * {@code candle_closed} until the Wave C cutover, {@code candle_features}
 * after) per request, decoded to a
 * {@link ContextCandle} (C1, docs/plans/2026-09-30-strategy-context-live-fetch.md).
 *
 * <p>With {@code CANDLE_CONTEXT_SEALED_ONLY=true} (the merged-source mode) a
 * {@code sealed=false} forming row is treated as absent — the finished-window
 * rule (DEC-059/W-B4); the provider's cooldown retries and serves the row once
 * it seals. A row without the sealed column fails closed (contract drift).
 *
 * <p>The lookup key is exactly the deployed primary key —
 * {@code (instrument_token, tf, window_start)} — in PK order; the decode reads
 * the DDL 33 column indexes. The connection is built the same way
 * {@code SignalJob.preflightTableContracts} builds its short-lived one
 * (bootstrap servers, configured database), and is owned by this fetcher:
 * {@link #close()} closes it. Fail-closed at open — a provider that cannot
 * reach its table is a configuration error, not a degraded mode.
 *
 * <p>Unused until {@code STRATEGY_CONTEXT_ENABLED=true}; no strategy sees it
 * before C2 lands the contract.
 */
final class FlussCandleFetcher implements CandleFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(FlussCandleFetcher.class);

    // candle_closed column indexes — must match DDL 33. DDL 35 (candle_features)
    // keeps the same 15-column prefix, so the same indexes decode both.
    static final int COL_INSTRUMENT_TOKEN = 0;
    static final int COL_TF = 3;
    static final int COL_WINDOW_START = 4;
    static final int COL_WINDOW_END = 5;
    static final int COL_OPEN_PAISE = 6;
    static final int COL_HIGH_PAISE = 7;
    static final int COL_LOW_PAISE = 8;
    static final int COL_CLOSE_PAISE = 9;
    static final int COL_VOLUME = 10;
    static final int COL_TICK_COUNT = 11;
    static final int COL_LAST_EVENT_TIME = 12;
    /** DDL 35 only: the sealed marker of a merged candle_features row. */
    static final int COL_SEALED = MergedCandleFeaturesColumns.SEALED;

    private final Lookuper lookuper;
    private final Connection connection;
    /** Wave C: accept only sealed (finished) rows — true when the source is candle_features. */
    private final boolean sealedOnly;

    /** Production path: connect, resolve the table, build the exact-PK lookuper. */
    static FlussCandleFetcher open(SignalJobConfig config) {
        org.apache.fluss.config.Configuration clientConf =
                new org.apache.fluss.config.Configuration();
        clientConf.setString("bootstrap.servers", config.bootstrapServers());
        Connection connection = ConnectionFactory.createConnection(clientConf);
        try {
            Table table = connection.getTable(
                    TablePath.of(config.database(), config.candleContextTable()));
            // Exact primary-key lookup: no lookupBy() — Fluss rejects a
            // lookupBy list that equals the full PK as an invalid prefix
            // lookup ("Please use primary key lookup (Lookuper without
            // lookupBy) instead", measured by the 2026-09-30 C1 smoke).
            Lookuper lookuper = table.newLookup().createLookuper();
            return new FlussCandleFetcher(lookuper, connection, config.candleContextSealedOnly());
        } catch (RuntimeException e) {
            try {
                connection.close();
            } catch (Exception closeError) {
                e.addSuppressed(closeError);
            }
            throw e;
        }
    }

    /**
     * @param connection the owning connection, or {@code null} when the
     *     lookuper is a test double (close() then has nothing to close)
     */
    FlussCandleFetcher(Lookuper lookuper, Connection connection) {
        this(lookuper, connection, false);
    }

    /**
     * Wave C: {@code sealedOnly=true} rejects {@code sealed=false} (forming)
     * rows as absent — the finished-window rule; the provider retries after
     * its cooldown, so a forming row is served once it seals.
     */
    FlussCandleFetcher(Lookuper lookuper, Connection connection, boolean sealedOnly) {
        this.lookuper = lookuper;
        this.connection = connection;
        this.sealedOnly = sealedOnly;
    }

    @Override
    public CompletableFuture<ContextCandle> fetch(ContextKey key) {
        GenericRow lookupKey = new GenericRow(3);
        lookupKey.setField(0, key.token());
        lookupKey.setField(1, BinaryString.fromString(key.tf().code()));
        lookupKey.setField(2, key.windowStart());
        return lookuper.lookup(lookupKey).thenApply(result -> decode(result, key, sealedOnly));
    }

    /**
     * Decodes the single row of an exact-PK lookup; {@code null} means the
     * window was never sealed (absent). A row that does not match the
     * requested key is contract drift and is rejected — it must never be
     * cached under the requested key.
     */
    static ContextCandle decode(LookupResult result, ContextKey key) {
        return decode(result, key, false);
    }

    /**
     * Wave C sealed-only variant: with {@code sealedOnly} a forming row is
     * absent (not yet a finished window) and a row lacking the sealed column
     * is contract drift — both return {@code null}, never throw.
     */
    static ContextCandle decode(LookupResult result, ContextKey key, boolean sealedOnly) {
        List<InternalRow> rows = result.getRowList();
        if (rows == null || rows.isEmpty()) {
            return null;
        }
        InternalRow row = rows.get(0);
        long token = row.getLong(COL_INSTRUMENT_TOKEN);
        String tfCode = row.getString(COL_TF).toString();
        long windowStart = row.getLong(COL_WINDOW_START);
        if (token != key.token()
                || windowStart != key.windowStart()
                || !key.tf().code().equals(tfCode)) {
            LOG.warn("strategy-context: candle lookup contract drift key={} row=({},{},{})",
                    key, token, tfCode, windowStart);
            return null;
        }
        if (sealedOnly) {
            if (row.getFieldCount() <= COL_SEALED) {
                LOG.warn("strategy-context: sealed-only source is missing the sealed column "
                        + "(row fields={}, need >{}) — is CANDLE_CONTEXT_TABLE a merged table?",
                        row.getFieldCount(), COL_SEALED);
                return null;
            }
            if (!row.getBoolean(COL_SEALED)) {
                return null; // forming window — absent until it seals
            }
        }
        return new ContextCandle(
                token,
                key.tf(),
                windowStart,
                row.getLong(COL_WINDOW_END),
                row.getLong(COL_OPEN_PAISE),
                row.getLong(COL_HIGH_PAISE),
                row.getLong(COL_LOW_PAISE),
                row.getLong(COL_CLOSE_PAISE),
                row.getLong(COL_VOLUME),
                row.getInt(COL_TICK_COUNT),
                row.getLong(COL_LAST_EVENT_TIME));
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                LOG.warn("strategy-context: closing the Fluss connection failed: {}",
                        e.toString());
            }
        }
    }
}
