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
 * lookup against {@code candle_closed} per request, decoded to a
 * {@link ContextCandle} (C1, docs/plans/2026-09-30-strategy-context-live-fetch.md).
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

    // candle_closed column indexes — must match DDL 33.
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

    private final Lookuper lookuper;
    private final Connection connection;

    /** Production path: connect, resolve the table, build the exact-PK lookuper. */
    static FlussCandleFetcher open(SignalJobConfig config) {
        org.apache.fluss.config.Configuration clientConf =
                new org.apache.fluss.config.Configuration();
        clientConf.setString("bootstrap.servers", config.bootstrapServers());
        Connection connection = ConnectionFactory.createConnection(clientConf);
        try {
            Table table = connection.getTable(
                    TablePath.of(config.database(), config.candleClosedTable()));
            // Exact primary-key lookup: no lookupBy() — Fluss rejects a
            // lookupBy list that equals the full PK as an invalid prefix
            // lookup ("Please use primary key lookup (Lookuper without
            // lookupBy) instead", measured by the 2026-09-30 C1 smoke).
            Lookuper lookuper = table.newLookup().createLookuper();
            return new FlussCandleFetcher(lookuper, connection);
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
        this.lookuper = lookuper;
        this.connection = connection;
    }

    @Override
    public CompletableFuture<ContextCandle> fetch(ContextKey key) {
        GenericRow lookupKey = new GenericRow(3);
        lookupKey.setField(0, key.token());
        lookupKey.setField(1, BinaryString.fromString(key.tf().code()));
        lookupKey.setField(2, key.windowStart());
        return lookuper.lookup(lookupKey).thenApply(result -> decode(result, key));
    }

    /**
     * Decodes the single row of an exact-PK lookup; {@code null} means the
     * window was never sealed (absent). A row that does not match the
     * requested key is contract drift and is rejected — it must never be
     * cached under the requested key.
     */
    static ContextCandle decode(LookupResult result, ContextKey key) {
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
            LOG.warn("strategy-context: candle_closed lookup contract drift key={} row=({},{},{})",
                    key, token, tfCode, windowStart);
            return null;
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
