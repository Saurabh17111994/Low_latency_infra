package com.trading.capture;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/**
 * Deterministic fingerprint for a broker postback, mirroring
 * {@code go-bridge/postback.go#NormalizeOrderUpdate}:
 *
 * <pre>{@code
 * report.PostbackEventID = fingerprint(map[string]string{
 *   "id":                     brokerOrderID,
 *   "remarks":                clientOrderRef,
 *   "status":                 orderStatus,
 *   "report_type":            reportType,
 *   "fill_shares":            fillShares,
 *   "average_price":          averagePrice,
 *   "exchange_update_time":   exchangeUpdateTime,
 * })
 * }</pre>
 *
 * <p>Canonical form mirrors Go's {@code json.Marshal(map[string]string)}: the fixed
 * 7-key set, keys sorted lexicographically, each {@code "key":"value"} (Go JSON
 * escaping, no whitespace), then SHA-256 hex (lowercase) of the UTF-8 bytes.
 * Go hashes exactly this string (go-bridge/models.go:200), so digests are
 * byte-identical across the bridge.
 *
 * <p>Pure logic — no Fluss.
 */
public final class PostbackFingerprint {

    private PostbackFingerprint() {}

    /**
     * Fixed identity key set, in the same literal order Go passes to
     * {@code fingerprint(map[string]string{...})} (go-bridge/postback.go:209).
     * Extra input keys are ignored (Go's literal never includes them) and missing
     * keys hash as {@code ""} (Go's {@code stringField} returns {@code ""}).
     */
    private static final List<String> IDENTITY_KEYS = List.of(
            "id", "remarks", "status", "report_type",
            "fill_shares", "average_price", "exchange_update_time");

    /**
     * Compute the Go-bridge-compatible SHA-256 hex fingerprint.
     *
     * <p>Canonical: fixed 7 keys, missing/null values as {@code ""}, values
     * trimmed, keys sorted, each as {@code "key":"value"} with Go JSON escaping,
     * joined by {@code ,} inside {@code {…}}, hashed as UTF-8 bytes.
     *
     * @param fields values for the identity keys (extra keys ignored)
     * @return lowercase hex-encoded SHA-256 digest (64 chars)
     * @throws IllegalArgumentException if {@code fields} is null or empty
     */
    public static String fingerprint(Map<String, String> fields) {
        if (fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException(
                    "postback fingerprint requires at least one identity field");
        }
        List<String> keys = new ArrayList<>(IDENTITY_KEYS);
        Collections.sort(keys);
        StringBuilder canonical = new StringBuilder(256).append('{');
        for (int i = 0; i < keys.size(); i++) {
            String k = keys.get(i);
            String v = fields.get(k);
            if (v == null) {
                v = "";
            }
            v = v.trim();
            if (i > 0) {
                canonical.append(',');
            }
            canonical.append('"').append(goJsonEscape(k)).append("\":\"")
                    .append(goJsonEscape(v)).append('"');
        }
        canonical.append('}');
        return sha256Hex(canonical.toString());
    }

    /**
     * Go {@code json.Marshal} string escaping (default HTML escaping): quote and
     * backslash are backslash-escaped, LF/CR/TAB use the short escapes, control
     * chars &lt; 0x20 and U+2028/U+2029 use four-hex-digit backslash-u escapes,
     * and {@code < > &} are HTML-escaped the same way (EscapeHTML=true default).
     */
    private static String goJsonEscape(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                case '<' -> sb.append("\\u003c");
                case '>' -> sb.append("\\u003e");
                case '&' -> sb.append("\\u0026");
                default -> {
                    if (c < 0x20 || c == 0x2028 || c == 0x2029) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }

    /**
     * SHA-256 hex of the UTF-8 bytes of {@code input} (null → empty string).
     *
     * @param input string to hash
     * @return lowercase hex-encoded SHA-256 digest
     */
    public static String sha256Hex(String input) {
        if (input == null) {
            input = "";
        }
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
