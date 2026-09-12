package com.trading.common.arrow;

import com.trading.common.identity.IdentityModel.BrokerOrderId;
import java.util.Map;
import java.util.Objects;

/**
 * Arrow {@code POST /order/regular} success response.
 *
 * The returned {@code orderNo} is the broker-authoritative order identity and is
 * mapped to {@link BrokerOrderId}. The platform {@code instruction_id} is never
 * echoed by Arrow; the round-trip is via {@code remarks} (= {@code client_order_ref}).
 */
public final class ArrowOrderResponse {

    private final BrokerOrderId brokerOrderId; // Arrow "orderNo"
    private final long requestTime; // epoch ms from Arrow

    public ArrowOrderResponse(BrokerOrderId brokerOrderId, long requestTime) {
        // P3-333: the constructor is public, so it enforces what fromJson enforces.
        // Otherwise a hand-built response carries exactly the corruption R-124 exists
        // to prevent — requestTime 0 is 1970-01-01, not a missing value.
        this.brokerOrderId = Objects.requireNonNull(brokerOrderId, "brokerOrderId");
        if (requestTime <= 0) {
            throw new IllegalArgumentException(
                    "requestTime must be a positive epoch-ms, got: " + requestTime);
        }
        this.requestTime = requestTime;
    }

    public BrokerOrderId brokerOrderId() { return brokerOrderId; }
    public long requestTime() { return requestTime; }

    /**
     * Parse a successful Arrow place response body (already JSON-decoded by caller).
     *
     * <p>R-198: the map itself may be null (a JSON body of {@code null}); guard it.
     * R-124: {@code requestTime} absent or non-numeric is a malformed response —
     * fail instead of silently defaulting to 0 (1970-01-01), which corrupts
     * latency/ordering analysis.
     */
    public static ArrowOrderResponse fromJson(Map<String, Object> data) {
        if (data == null) {
            throw new IllegalArgumentException(
                    "Arrow response body is null (expected {orderNo, requestTime})");
        }
        Object no = data.get("orderNo");
        Object rt = data.get("requestTime");
        if (no == null) {
            throw new IllegalArgumentException("orderNo missing in Arrow response");
        }
        if (!(rt instanceof Number)) {
            throw new IllegalArgumentException(
                    "requestTime missing or not numeric in Arrow response; got: " + rt);
        }
        long time = ((Number) rt).longValue();
        if (time <= 0) {
            throw new IllegalArgumentException(
                    "requestTime must be a positive epoch-ms, got: " + time);
        }
        return new ArrowOrderResponse(new BrokerOrderId(String.valueOf(no)), time);
    }
}
