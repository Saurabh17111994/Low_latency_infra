package com.trading.common.safety;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * SHA-256 digest of a token set, byte-identical to the ingestion contract:
 * tokens sorted ascending, each encoded as 8 big-endian bytes, digest
 * hex-encoded lowercase.
 *
 * <p>Parity targets (verified by pinned vectors in the test suite):
 * <ul>
 *   <li>Go {@code tokenSetHash} (go-bridge/subscription_plan.go) and</li>
 *   <li>Java {@code SafetyHaltWriter.computeAssignedTokenHash} /
 *       {@code InstrumentManifestLoader.computeFingerprint}.</li>
 * </ul>
 * Pinned vector: {@code [1000, 1001, 1]} &rarr;
 * {@code 8a65b772eeae7692de1f941da206dc6a5b6649568e999dc06fb16a7b0615744c}
 * (order-independent by construction).
 */
public final class TokenSetHash {

    private TokenSetHash() {}

    /** Lowercase SHA-256 hex over the sorted 8-byte-big-endian encoding. */
    public static String of(Collection<Long> tokens) {
        // P3-498: explicit null contract — never a bare ArrayList(null) NPE.
        java.util.Objects.requireNonNull(tokens, "tokens");
        List<Long> ordered = new ArrayList<>(tokens);
        // P3-354: domain validation before hashing — Go takes []int32 and
        // rejects duplicates/non-positive, so anything outside that domain
        // would produce a hash Go can never match. Fail here, not silently.
        for (Long t : ordered) {
            java.util.Objects.requireNonNull(t, "token element");
            if (t <= 0 || t > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("token out of positive-int32 domain: " + t);
            }
        }
        ordered.sort(Comparator.naturalOrder());
        for (int i = 1; i < ordered.size(); i++) {
            if (ordered.get(i).equals(ordered.get(i - 1))) {
                throw new IllegalArgumentException("duplicate token " + ordered.get(i));
            }
        }
        MessageDigest sha256;
        try {
            sha256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES);
        for (long token : ordered) {
            buf.clear();
            sha256.update(buf.putLong(token).array());
        }
        return toHex(sha256.digest());
    }

    /** Varargs convenience overload. */
    public static String of(long... tokens) {
        List<Long> list = new ArrayList<>(tokens.length);
        for (long t : tokens) {
            list.add(t);
        }
        return of(list);
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format(Locale.ROOT, "%02x", b));
        }
        return sb.toString();
    }
}
