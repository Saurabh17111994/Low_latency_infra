package com.trading.common.schema.audit;

import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Production {@link MasterKeyStore} driven by environment variables:
 * {@code EOD_EXPORT_MASTER_KEY_B64} is the current (v1) 32-byte master key in
 * Base64; {@code EOD_EXPORT_MASTER_KEY_V<v>_B64} holds rotated versions
 * ({@code EOD_EXPORT_MASTER_KEY_V2_B64}, ...). Version 1 is required — an
 * export pipeline with no master key is a fail-closed configuration error.
 *
 * <p>Every key must decode to exactly {@link EnvelopeCrypto#AES_KEY_BYTES}
 * bytes; a short key would silently weaken the envelope to AES-128
 * (P6-281). {@link #keyFor} hands back a copy (P6-282). The rotation probe walks
 * versions 2..{@link #MAX_PROBED_VERSION} and does <em>not</em> stop at the
 * first gap: a store that retired v2 while holding v3 must load v3 as current
 * rather than quietly staying on v1 (P6-674).
 */
public final class EnvMasterKeyStore implements MasterKeyStore {

    /**
     * Highest rotation version the probe looks for. The env accessor can only be
     * asked for a name, never enumerated, so the probe is bounded: an unbounded
     * one would spin forever on an accessor that answers every name. A version
     * above this bound is invisible to the store.
     */
    static final int MAX_PROBED_VERSION = 64;

    private final Map<Integer, byte[]> keys;
    private final int current;

    private EnvMasterKeyStore(TreeMap<Integer, byte[]> keys) {
        this.keys = keys;
        // v1 is always present (checked in fromEnv), so the highest present
        // version is simply the last one — never the placeholder 0 (P6-678).
        this.current = keys.lastKey();
    }

    /** Build from an env accessor (the test twin passes a map; prod passes System::getenv). */
    public static EnvMasterKeyStore fromEnv(Function<String, String> env) {
        Objects.requireNonNull(env, "env");
        String v1 = env.apply("EOD_EXPORT_MASTER_KEY_B64");
        if (v1 == null || v1.isBlank()) {
            throw new IllegalArgumentException(
                    "EOD_EXPORT_MASTER_KEY_B64 is required for the encrypted export pipeline "
                            + "(fail-closed: no master key, no export)");
        }
        TreeMap<Integer, byte[]> keys = new TreeMap<>();
        keys.put(1, decode(v1));
        for (int v = 2; v <= MAX_PROBED_VERSION; v++) {
            String k = env.apply("EOD_EXPORT_MASTER_KEY_V" + v + "_B64");
            if (k != null && !k.isBlank()) {
                keys.put(v, decode(k));
            }
        }
        return new EnvMasterKeyStore(keys);
    }

    private static byte[] decode(String b64) {
        byte[] raw;
        try {
            raw = Base64.getDecoder().decode(b64.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("master key is not valid Base64", e);
        }
        if (raw.length != EnvelopeCrypto.AES_KEY_BYTES) {
            throw new IllegalArgumentException("master key must be "
                    + EnvelopeCrypto.AES_KEY_BYTES + " bytes (AES-256), got " + raw.length);
        }
        return raw;
    }

    @Override
    public int currentVersion() {
        return current;
    }

    @Override
    public byte[] keyFor(int version) {
        byte[] key = keys.get(version);
        if (key == null) {
            throw new IllegalArgumentException("master key version " + version
                    + " not present (rotation dropped it?)");
        }
        return key.clone();
    }
}
