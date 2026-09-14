package com.trading.common.config;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

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
 *
 * <p>How the scan is anchored and what it can see (P6 W0, 2026-09-14):
 * <ul>
 *   <li>{@code ConfigKeys.java} itself is excluded — it declares every key as a
 *       literal, so including it made the guard vacuous (it could never fail);</li>
 *   <li>comments are stripped before matching, so a key named in javadoc is not
 *       a read;</li>
 *   <li>all sources are read once into a single corpus (no per-key rescan, no
 *       file cap), so the verdict does not depend on walk order;</li>
 *   <li>the source root is resolved from {@code -Dconfigguard.sources=<dir>},
 *       then from the code-source anchor of this class, then from the working
 *       directory. A packaged run (jar/container without sources) cannot be
 *       verified from source: that is reported loudly on stderr and skipped,
 *       because refusing to start there would break every deployment that
 *       ships without sources. A tree that <em>is</em> reachable but cannot be
 *       read is an environment fault and fails startup.</li>
 * </ul>
 */
public final class ConfigGuard {

    /** Keys that are deliberately not read in code (env-file-only, aliases, docs). */
    private static final List<String> IGNORED = List.of(
            "CNC", "DAY", "AES256", "HOSTNAME"); // non-env literals caught by the inventory

    /**
     * The declaration file: every key appears here as a literal, so it can never count as a
     * read (P6-025). Any file with this name is a key inventory, not a reader.
     */
    private static final String DECLARATIONS_FILE = "ConfigKeys.java";

    /** Explicit source root (a directory to scan); also the test seam for the scan. */
    static final String SOURCES_PROPERTY = "configguard.sources";

    private ConfigGuard() {}

    /** @throws IllegalStateException listing every declared-but-unread key */
    public static void assertAllKeysRead() {
        List<String> declared = declaredKeys();
        String corpus = sourceCorpusOrNull(); // one scan for every key (P6-266)
        List<String> missing = new ArrayList<>();
        for (String key : declared) {
            if (IGNORED.contains(key) || corpus == null) {
                continue;
            }
            if (!corpus.contains("\"" + key + "\"")) {
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
        return constantStrings(ConfigKeys.class);
    }

    /**
     * Every {@code static final String} constant on {@code owner} — the declared-key inventory.
     * Null and blank values are skipped: {@code "null"} is not a key.
     */
    static List<String> constantStrings(Class<?> owner) {
        List<String> keys = new ArrayList<>();
        for (Field f : owner.getDeclaredFields()) {
            int mods = f.getModifiers();
            // Only the env inventory: static final String constants. An instance or non-final
            // String field is not a key, and f.get(null) on an instance field would throw an
            // unwrapped IllegalArgumentException out of a startup guard (P6-656).
            if (f.getType() != String.class || !Modifier.isStatic(mods) || !Modifier.isFinal(mods)) {
                continue;
            }
            try {
                f.setAccessible(true);
                String value = (String) f.get(null);
                if (value != null && !value.isBlank()) {
                    keys.add(value);
                }
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("ConfigGuard: cannot read " + owner.getSimpleName()
                        + "." + f.getName(), e);
            }
        }
        return keys;
    }

    /**
     * True when the key literal appears in a source read (config reads it).
     *
     * <p>Conservative by construction: it matches the quoted literal anywhere outside comments
     * and outside the declarations file, so a key that is only <em>mentioned</em> in a log
     * message or a dead string still counts as read. Requiring a specific read expression would
     * have to know every helper (getenv, getProperty, envLong, …) and would start failing on
     * real reads — the false-positive direction that refuses startup.
     */
    static boolean keyReadInCode(String key) {
        String corpus = sourceCorpusOrNull();
        return corpus == null || corpus.contains("\"" + key + "\"");
    }

    /**
     * All readable {@code .java} sources as one comment-stripped corpus, or {@code null} when no
     * source tree is reachable (packaged run). One walk for every key, no file cap (P6-266).
     */
    static String sourceCorpusOrNull() {
        Path code = locateSources();
        if (code == null) {
            warnUnverifiable();
            return null;
        }
        try {
            return readCorpus(code);
        } catch (IOException e) {
            // Reachable but unreadable: an environment fault, not a licence to skip the check.
            throw new IllegalStateException(
                    "ConfigGuard: scanning " + code + " failed — cannot verify ConfigKeys", e);
        }
    }

    /** The source tree to scan, or {@code null} when none is reachable. */
    static Path locateSources() {
        String override = System.getProperty(SOURCES_PROPERTY);
        if (override != null && !override.isBlank()) {
            Path p = Paths.get(override).toAbsolutePath();
            return Files.exists(p) ? p : null;
        }
        // An anchor, not the working directory: the same code is started from an IDE, Maven,
        // a jar and a container, and only some of those have the repo as CWD (P6-658).
        Path fromClass = classAnchor();
        Path found = fromClass == null ? null : findCodeUnder(fromClass);
        return found != null ? found : findCodeUnder(Paths.get("").toAbsolutePath());
    }

    /** Directory holding this class' code source (the jar's directory for a packaged run). */
    private static Path classAnchor() {
        try {
            var source = ConfigGuard.class.getProtectionDomain().getCodeSource();
            if (source == null || source.getLocation() == null) {
                return null;
            }
            Path p = Paths.get(source.getLocation().toURI());
            return Files.isDirectory(p) ? p : p.getParent();
        } catch (Exception e) {
            return null; // no anchor — fall back to the working directory
        }
    }

    /** Walk up from {@code from} to the directory that contains {@code code/}. */
    private static Path findCodeUnder(Path from) {
        for (Path p = from; p != null; p = p.getParent()) {
            if (Files.isDirectory(p.resolve("code"))) {
                return p.resolve("code");
            }
        }
        return null;
    }

    /** One walk, comments stripped, declarations file excluded (P6-025, P6-657, P6-266). */
    private static String readCorpus(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            throw new IOException("not a source directory: " + dir);
        }
        StringBuilder corpus = new StringBuilder();
        try (Stream<Path> walk = Files.walk(dir)) {
            List<Path> files = walk
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".java"))
                    .filter(p -> !p.getFileName().toString().equals(DECLARATIONS_FILE))
                    .toList();
            for (Path p : files) {
                corpus.append(stripComments(Files.readString(p, StandardCharsets.UTF_8))).append('\n');
            }
        }
        return corpus.toString();
    }

    /**
     * Blanks block comments (javadoc included) and whole-line {@code //} comments, so a key
     * mentioned in prose is not mistaken for a read (P6-657). Trailing {@code //} comments stay:
     * a string literal can contain {@code //} (e.g. a URL) and removing text after it could hide
     * a real read — the false-positive direction. A literal inside a block-comment-looking string
     * is affected in that direction only, and is rare enough to accept.
     */
    static String stripComments(String src) {
        return src.replaceAll("(?s)/\\*.*?\\*/", " ").replaceAll("(?m)^[ \\t]*//.*$", " ");
    }

    /**
     * A packaged run (jar or container without sources) cannot verify the inventory from source.
     * That is reported loudly and skipped — see the class javadoc for why it is not fatal.
     */
    private static void warnUnverifiable() {
        System.err.println("ConfigGuard: no source tree to verify ConfigKeys against (looked for a"
                + " code/ directory from " + SOURCES_PROPERTY + ", the class-code anchor and the"
                + " working directory); declared-key verification SKIPPED for this run. Run from a"
                + " checkout or set -D" + SOURCES_PROPERTY + "=<dir> to verify.");
    }
}
