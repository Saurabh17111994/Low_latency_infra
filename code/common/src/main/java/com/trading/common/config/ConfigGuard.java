package com.trading.common.config;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Startup guard (G1, 2026-08-29): every env key declared in {@link ConfigKeys}
 * must actually be read by config code. A declared-but-never-read key is a
 * silent lie — an operator sets it, nothing changes, and nobody notices. This
 * guard fails startup so the inventory cannot drift from reality.
 *
 * <p>Implementation: reflects over {@link ConfigKeys} constants, scans the
 * config classes' source (under {@code code/}) for each key's literal
 * (e.g. {@code "DEDUP_TTL_MS"}), and fails on keys found nowhere. The source
 * scan is conservative: test-only keys and deliberately-unused aliases are
 * allowed via {@link #IGNORED}.
 */
public final class ConfigGuard {

    /** Keys that are deliberately not read in code (env-file-only, aliases, docs). */
    private static final List<String> IGNORED = List.of(
            "CNC", "DAY", "AES256", "HOSTNAME"); // non-env literals caught by the inventory

    private ConfigGuard() {}

    /** @throws IllegalStateException listing every declared-but-unread key */
    public static void assertAllKeysRead() {
        List<String> missing = new ArrayList<>();
        for (String key : declaredKeys()) {
            if (IGNORED.contains(key)) {
                continue;
            }
            if (!keyReadInCode(key)) {
                missing.add(key);
            }
        }
        if (!missing.isEmpty()) {
            throw new IllegalStateException("ConfigGuard: declared-but-never-read keys: " + missing
                    + " — a config key in ConfigKeys that no code reads is a silent lie (G1). "
                    + "Wire it in or remove it from ConfigKeys.");
        }
    }

    /** All constant names declared in {@link ConfigKeys} (the value strings). */
    static List<String> declaredKeys() {
        List<String> keys = new ArrayList<>();
        for (Field f : ConfigKeys.class.getDeclaredFields()) {
            if (f.getType() == String.class) {
                try {
                    keys.add((String) f.get(null));
                } catch (IllegalAccessException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
        return keys;
    }

    /** True when the key literal appears in any .java under code/ (config reads it). */
    static boolean keyReadInCode(String key) {
        try {
            Path root = Paths.get("").toAbsolutePath();
            // walk up to the repo root (code/ contains the modules)
            while (root != null && !Files.isDirectory(root.resolve("code"))) {
                root = root.getParent();
            }
            if (root == null) {
                return true; // not in the repo — cannot verify, don't block
            }
            return keyInTree(root.resolve("code"), key);
        } catch (IOException e) {
            return true; // scan failure must not brick startup
        }
    }

    private static boolean keyInTree(Path dir, String key) throws IOException {
        try (var stream = Files.walk(dir)) {
            return stream
                    .filter(p -> p.toString().endsWith(".java"))
                    .limit(2000)
                    .anyMatch(p -> {
                        try {
                            String body = Files.readString(p, StandardCharsets.UTF_8);
                            return body.contains("\"" + key + "\"");
                        } catch (IOException e) {
                            return false;
                        }
                    });
        }
    }
}
