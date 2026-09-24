package com.trading.ingestion;

import com.trading.common.config.PlatformConfig;
import com.trading.ingestion.model.RawTick;
import com.trading.ingestion.model.TickPacket;
import com.trading.ingestion.model.ValidityClassification;
import java.time.Instant;

/** Test fixture factory — creates valid TickPacket instances without real broker data. */
public final class TickPacketFixtures {

    private TickPacketFixtures() {}

    /** Create a valid trade tick with synthetic data. Token increments per call. */
    public static TickPacket validTrade(int index) {
        return validTradeWithQty(index, 25L, 100L);
    }

    /** Same as {@link #validTrade(int)} but with explicit LTQ and cumulative volume,
     *  so a test can prove last_qty is sourced from LTQ and not volume (P0). The tick
     *  contributes its own quantity, which under v4 is what makes it a real TRADE. */
    public static TickPacket validTradeWithQty(int index, long lastQty, long volume) {
        return validTradeWithQty(index, lastQty, volume, lastQty);
    }

    /** Same again with an explicit volume_delta, so a test can build the case that the
     *  old rule mislabelled: VALID_TRADE validity with nothing traded since the last tick. */
    public static TickPacket validTradeWithQty(int index, long lastQty, long volume, long volumeDelta) {
        long token = 100000L + (index % 50) * 100L + (index % 10);
        return new TickPacket.Builder()
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
                .instrumentToken(token)
                .tradingSymbol("SYM" + (index % 50 + 1) + "-EQ")
                .exchange("NSE")
                .eventTime(Instant.now().minusMillis(100))
                .ingestTs(Instant.now())
                .lastPricePaise(12345L + index)
                .lastQty(lastQty)
                .volume(volume)
                .volumeDelta(volumeDelta)
                .eventFingerprint("fp_" + token + "_" + index)
                .fingerprintVersion(1)
                .connectionId("test")
                .connectionEpoch(0L)
                .instanceId("test-instance")
                .schemaVersion(Integer.parseInt(PlatformConfig.RAW_TABLE_1_SCHEMA_VERSION))
                .build();
    }
}
