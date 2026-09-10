package com.trading.capture;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Map;

/**
 * Pure static decoder mirroring {@code go-bridge/postback.go#NormalizeOrderUpdate}.
 *
 * <p>Extracts the broker postback payload into a typed record without any
 * Fluss or broker dependency. All string extraction is null-safe and
 * trims whitespace, mirroring Go's {@code stringField} helper which returns
 * {@code ""} for missing/null, {@code TrimSpace} for strings, and
 * {@code fmt.Sprint} for numbers.
 */
public final class PostbackDecoder {

    private PostbackDecoder() {}

    /**
     * Decode a raw broker postback map (e.g. parsed JSON object) into a typed envelope.
     *
     * <p>Field mapping (pure logic, no Fluss):
     * <ul>
     *   <li>{@code brokerOrderId} ← {@code raw.get("id")}</li>
     *   <li>{@code exchangeOrderId} ← {@code raw.get("exchangeOrderID")}</li>
     *   <li>{@code clientOrderRef} ← {@code raw.get("remarks")}</li>
     *   <li>{@code orderStatus} ← {@code raw.get("orderStatus")}</li>
     *   <li>{@code reportType} ← {@code raw.get("reportType")}</li>
     *   <li>{@code fillShares} ← {@code raw.get("fillShares")}</li>
     *   <li>{@code averagePrice} ← {@code raw.get("averagePrice")}</li>
     *   <li>{@code fillPrice} ← {@code raw.get("fillPrice")}</li>
     *   <li>{@code fillQuantity} ← {@code raw.get("fillQuantity")}</li>
     *   <li>{@code fillTime} ← {@code raw.get("fillTime")}</li>
     *   <li>{@code instrumentToken} ← {@code raw.get("token")}</li>
     *   <li>{@code exchangeUpdateTime} ← {@code raw.get("exchangeUpdateTime")}</li>
     *   <li>{@code receivedTsMs} ← {@code System.currentTimeMillis()}</li>
     * </ul>
     *
     * @param raw raw postback payload (may be null)
     * @return decoded postback
     * @throws IllegalArgumentException if the payload carries no identity
     *         ({@code id} and {@code remarks} both empty after trim)
     */
    public static DecodedPostback decode(Map<String, Object> raw) {
        return decode(raw, System.currentTimeMillis());
    }

    /**
     * Deterministic decode overload for tests/backfill: the receive timestamp is
     * supplied by the caller instead of being read from the wall clock.
     *
     * @param raw raw postback payload (may be null)
     * @param receivedTsMs receive timestamp in epoch millis
     * @return decoded postback
     * @throws IllegalArgumentException if the payload carries no identity
     */
    static DecodedPostback decode(Map<String, Object> raw, long receivedTsMs) {
        if (raw == null) {
            raw = Map.of();
        }
        String brokerOrderId = stringField(raw, "id");
        String clientOrderRef = stringField(raw, "remarks");
        if (brokerOrderId.isEmpty() && clientOrderRef.isEmpty()) {
            throw new IllegalArgumentException("postback without identity (id and remarks both empty)");
        }
        String exchangeOrderId = stringField(raw, "exchangeOrderID");
        String orderStatus = stringField(raw, "orderStatus");
        String reportType = stringField(raw, "reportType");
        String fillShares = stringField(raw, "fillShares");
        String averagePrice = stringField(raw, "averagePrice");
        String fillPrice = stringField(raw, "fillPrice");
        String fillQuantity = stringField(raw, "fillQuantity");
        String fillTime = stringField(raw, "fillTime");
        String instrumentToken = stringField(raw, "token");
        String exchangeUpdateTime = stringField(raw, "exchangeUpdateTime");
        return new DecodedPostback(
                brokerOrderId,
                exchangeOrderId,
                clientOrderRef,
                orderStatus,
                reportType,
                fillShares,
                averagePrice,
                fillPrice,
                fillQuantity,
                fillTime,
                instrumentToken,
                exchangeUpdateTime,
                receivedTsMs);
    }

    private static String stringField(Map<String, Object> raw, String key) {
        Object value = raw.get(key);
        if (value == null) {
            return "";
        }
        if (value instanceof String s) {
            return s.trim();
        }
        if (value instanceof Double d) {
            return goFormatG(d).trim();
        }
        if (value instanceof Float f) {
            return goFormatG(f.doubleValue()).trim();
        }
        // Integer/Long and anything else: json.Number.String() / fmt.Sprint, trimmed.
        return String.valueOf(value).trim();
    }

    /**
     * Go's {@code fmt.Sprintf("%g", v)} for float64 — shortest round-trip digits,
     * exponent form when the decimal exponent is {@code < -4} or {@code >= 6}
     * (strconv.FormatFloat(v, 'g', -1, 64)), exponent always signed and padded to
     * at least two digits. Verified against a 16-value Go reference table.
     * NaN → {@code "NaN"}, ±Inf → {@code "+Inf"}/{@code "-Inf"} (Go spelling).
     */
    static String goFormatG(double value) {
        if (Double.isNaN(value)) return "NaN";
        if (Double.isInfinite(value)) return value > 0 ? "+Inf" : "-Inf";
        boolean negative = value < 0 || (value == 0.0 && Double.doubleToRawLongBits(value) < 0);
        BigDecimal shortest = shortestRoundTrip(Math.abs(value));
        String digits = shortest.unscaledValue().abs().toString();
        int dp = digits.length() - shortest.scale(); // decimal point position (Go digs.dp)
        int exp = dp - 1;
        String body;
        if (exp < -4 || exp >= 6) {
            StringBuilder sb = new StringBuilder();
            sb.append(digits.charAt(0));
            if (digits.length() > 1) {
                sb.append('.').append(digits.substring(1));
            }
            sb.append('e').append(exp >= 0 ? '+' : '-');
            int e = Math.abs(exp);
            if (e < 10) {
                sb.append('0');
            }
            sb.append(e);
            body = sb.toString();
        } else if (dp <= 0) {
            body = "0." + "0".repeat(-dp) + digits;
        } else if (dp >= digits.length()) {
            body = digits + "0".repeat(dp - digits.length());
        } else {
            body = digits.substring(0, dp) + "." + digits.substring(dp);
        }
        return negative ? "-" + body : body;
    }

    /**
     * Shortest decimal representation that round-trips to {@code d}, independent
     * of JDK {@code Double.toString} quirks (JDK 17 is not always shortest).
     */
    private static BigDecimal shortestRoundTrip(double d) {
        BigDecimal exact = new BigDecimal(d);
        for (int p = 1; p <= 17; p++) {
            BigDecimal r = exact.round(new MathContext(p, RoundingMode.HALF_EVEN)).stripTrailingZeros();
            if (r.doubleValue() == d) {
                return r;
            }
        }
        return exact.stripTrailingZeros();
    }

    /**
     * Immutable decoded postback capturing the raw broker fields in normalized string form.
     *
     * @param brokerOrderId      broker order id (raw {@code id})
     * @param exchangeOrderId    exchange-assigned order id (raw {@code exchangeOrderID})
     * @param clientOrderRef     client order ref (raw {@code remarks})
     * @param orderStatus        broker order status
     * @param reportType         broker report type
     * @param fillShares         fill shares (string, may be empty)
     * @param averagePrice       average fill price (string, may be empty)
     * @param fillPrice          fill price (string, may be empty)
     * @param fillQuantity       fill quantity (string, may be empty)
     * @param fillTime           fill time (string, may be empty)
     * @param instrumentToken    instrument token (raw {@code token})
     * @param exchangeUpdateTime exchange update time (raw {@code exchangeUpdateTime})
     * @param receivedTsMs       wall-clock receive time in epoch millis
     */
    public record DecodedPostback(
            String brokerOrderId,
            String exchangeOrderId,
            String clientOrderRef,
            String orderStatus,
            String reportType,
            String fillShares,
            String averagePrice,
            String fillPrice,
            String fillQuantity,
            String fillTime,
            String instrumentToken,
            String exchangeUpdateTime,
            long receivedTsMs) {}
}
