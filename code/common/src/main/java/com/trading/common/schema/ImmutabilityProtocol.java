package com.trading.common.schema;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Immutability protocol (docs/08_implementation/01-foundation.md &rarr; "Immutability protocol", orig L465).
 *
 * <p>Same id + same hash &rarr; {@link Outcome#DUPLICATE} (drop).
 * Same id + different hash &rarr; {@link Outcome#VIOLATION} (quarantine + halt).
 */
public final class ImmutabilityProtocol {

    private ImmutabilityProtocol() {}

    public enum Outcome { ACCEPTED, DUPLICATE, VIOLATION }

    public static String canonicalHash(byte[] canonicalContent) {
        java.util.Objects.requireNonNull(canonicalContent, "canonicalContent is required");
        try {
            MessageDigest d = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(d.digest(canonicalContent));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    public static String canonicalHash(String canonicalContent) {
        java.util.Objects.requireNonNull(canonicalContent, "canonicalContent is required");
        return canonicalHash(canonicalContent.getBytes(StandardCharsets.UTF_8));
    }

    public static Outcome evaluate(String existingHash, String incomingHash) {
        java.util.Objects.requireNonNull(incomingHash, "incomingHash is required");
        if (existingHash == null) {
            return Outcome.ACCEPTED;
        }
        if (existingHash.equals(incomingHash)) {
            return Outcome.DUPLICATE;
        }
        return Outcome.VIOLATION;
    }
}
