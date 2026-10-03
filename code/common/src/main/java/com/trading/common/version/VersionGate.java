package com.trading.common.version;

import java.util.List;

/**
 * Runtime version gate
 * (docs/08_implementation/01-foundation.md &rarr; "Implementation rules", orig L347).
 *
 * <p>The platform must fail to start if a required version is absent or equals {@code "latest"}.
 * A placeholder value (see {@link PlaceholderVersions}) is not accepted for a live-money path.
 */
public final class VersionGate {

    private VersionGate() {}

    public static final String LATEST = "latest";

    /**
     * Throws if the resolved version is absent, blank, literally "latest",
     * or a placeholder sentinel (R-048: a placeholder is not accepted for a
     * live-money path). Returns the trimmed value (R-079) so whitespace never
     * leaks into downstream pin files.
     */
    public static String requirePinned(String what, String resolved) {
        if (resolved == null || resolved.isBlank()) {
            throw new IllegalStateException("Required version for '" + what + "' is absent; refusing to start.");
        }
        String trimmed = resolved.trim();
        if (LATEST.equalsIgnoreCase(trimmed)) {
            throw new IllegalStateException(
                "Required version for '" + what + "' is 'latest'; pin an exact version. Refusing to start.");
        }
        if (PlaceholderVersions.isPlaceholder(trimmed)) {
            throw new IllegalStateException(
                "Required version for '" + what + "' is a placeholder (" + trimmed
                + "); substitute a pinned, evidence-verified value before any "
                + "live-money path. Refusing to start.");
        }
        return trimmed;
    }

    /**
     * True only when the value is a real, pinned version (present, not latest, not a placeholder)
     * AND capability evidence has been recorded.
     */
    public static boolean isPinnedAndVerified(String what, String resolved, boolean capabilityVerified) {
        try {
            requirePinned(what, resolved);
        } catch (IllegalStateException e) {
            return false;
        }
        return capabilityVerified;
    }

    /** Convenience: fail unless every matrix entry is pinned (tests cover it; no CI caller wires it as of 2026-10-03). */
    public static void requireAllPinned(List<String> entries) {
        // R-182: a null list would throw a bare NPE in a safety-critical CI gate.
        if (entries == null) {
            throw new IllegalArgumentException(
                    "requireAllPinned: entries must not be null");
        }
        for (String e : entries) {
            requirePinned("matrix-entry", e);
        }
    }
}
