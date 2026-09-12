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
        final long time;
        if (rt instanceof Number) {
            Number n = (Number) rt;
            // P3-489: a fractional epoch-ms is malformed, not something to truncate.
            // 1752539000.9 as a Double and 1752539000.5f as a Float were both being
            // silently cut to 1752539000, which reads as a plausible timestamp.
            // ponytail: Infinity still arrives as Long.MAX_VALUE via longValue() —
            // reported, not fixed here, because no finding covers it.
            if (n instanceof Double && n.doubleValue() != Math.rint(n.doubleValue())) {
                throw new IllegalArgumentException(
                        "requestTime must be an integral epoch-ms, got: " + rt);
            }
            if (n instanceof Float && n.doubleValue() != Math.rint(n.doubleValue())) {
                throw new IllegalArgumentException(
                        "requestTime must be an integral epoch-ms, got: " + rt);
            }
            time = n.longValue();
        } else if (rt instanceof String && !((String) rt).trim().isEmpty()) {
            // P3-334: Arrow has been observed to quote requestTime. A numeric string
            // is the same value, so it is parsed rather than rejected as non-numeric.
            try {
                time = Long.parseLong(((String) rt).trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "requestTime not numeric in Arrow response; got: " + rt, e);
            }
        } else {
            throw new IllegalArgumentException(
                    "requestTime missing or not numeric in Arrow response; got: " + rt);
        }
        if (time <= 0) {
            throw new IllegalArgumentException(
                    "requestTime must be a positive epoch-ms, got: " + time);
        }
        return new ArrowOrderResponse(new BrokerOrderId(String.valueOf(no)), time);
    }
}
