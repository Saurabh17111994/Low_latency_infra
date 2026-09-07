package com.trading.ingestion.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.trading.common.config.PlatformConfig;
import java.time.Instant;
import org.junit.jupiter.api.Test;

/**
 * Producer-side {@code raw_table_1.schema_version} contract pin.
 *
 * <p>The persisted label is {@code String.valueOf(packet.schemaVersion())}
 * (FlussClientAdapter.append). It must equal the shared PlatformConfig constant
 * that SignalJob's {@code RAW_SCHEMA_VERSION} default derives from — otherwise
 * the consumer rejects every row (the 2026-08-10 stall: the builder default was
 * {@code 1} while the compute default was {@code "2"}).
 */
class TickPacketSchemaVersionTest {

    @Test
    void defaultBuilderLabelMatchesSharedContract() {
        TickPacket packet = new TickPacket.Builder()
                .raw(new RawTick.Builder()
                        .rawPayload(new byte[]{1, 2, 3})
                        .payloadHash("abc123")
                        .hashAlgorithm("SHA-256")
                        .protocolVersion("go-arrow-v0")
                        .decoderVersion("test")
                        .receiveTime(Instant.now())
                        .receiveTimeNanos(System.nanoTime())
                        .build())
                .validity(ValidityClassification.VALID_TRADE)
                .lastPricePaise(12345L) // P1-089: VALID_TRADE requires a positive price
                .instrumentToken(100000L)
                .tradingSymbol("SYM-EQ")
                .exchange("NSE")
                .eventTime(Instant.now().minusMillis(100))
                .ingestTs(Instant.now())
                .eventFingerprint("fp_1")
                .fingerprintVersion(1)
                .connectionId("test")
                .connectionEpoch(0L)
                .build();
        assertEquals(
                PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION,
                String.valueOf(packet.schemaVersion()));
    }
}
