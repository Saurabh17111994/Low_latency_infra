package com.trading.common.config;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * G2 (2026-08-29): ConfigGuard itself validated — the real inventory passes
 * (every declared key is read somewhere in code), and a fabricated unread key
 * is detected.
 */
class ConfigGuardTest {

    /** Source-root override the guard accepts (a directory holding {@code code/}). */
    private static final String SOURCES_PROPERTY = "configguard.sources";

    @TempDir
    Path tmp;

    /** Writes a fake tree: pairs of (relative path, body) under {@code <tmp>/<name>/code/}. */
    private Path fixture(String name, String... pathAndBody) throws IOException {
        Path code = tmp.resolve(name).resolve("code");
        Files.createDirectories(code);
        for (int i = 0; i + 1 < pathAndBody.length; i += 2) {
            Path f = code.resolve(pathAndBody[i]);
            if (f.getParent() != null) {
                Files.createDirectories(f.getParent());
            }
            Files.writeString(f, pathAndBody[i + 1], StandardCharsets.UTF_8);
        }
        return code;
    }

    private static String keysFile(String constant, String value) {
        return "package keys;\npublic final class ConfigKeys {\n  public static final String "
                + constant + " = \"" + value + "\";\n}\n";
    }

    @Test
    void declaredKeysAreNonEmpty() {
        assertFalse(ConfigGuard.declaredKeys().isEmpty(),
                "ConfigKeys inventory must not be empty");
    }

    @Test
    void realInventoryPasses() {
        // Every key in ConfigKeys must be read by config code — if this fails,
        // a declared key is a silent lie and must be wired in or removed.
        assertDoesNotThrow(ConfigGuard::assertAllKeysRead);
    }

    @Test
    void keyReadInCodeDetectsRealAndMissingKeys() {
        // DEDUP_WINDOW_ENTRIES is read by SignalJobConfig — must be found.
        assertTrue(ConfigGuard.keyReadInCode("DEDUP_WINDOW_ENTRIES"),
                "DEDUP_WINDOW_ENTRIES is read by SignalJobConfig");
        // A fabricated key must NOT be found — proves the scan isn't vacuous.
        // (Split so this test file itself doesn't contain the literal.)
        String fake = "THIS_KEY_DOES_NOT_" + "EXIST_ANYWHERE";
        assertFalse(ConfigGuard.keyReadInCode(fake),
                "fabricated key must not be reported as read");
    }

    @Test
    void ignoredKeysDoNotBlockStartup() {
        // The IGNORED list is private; just assert the guard passes with them present.
        assertDoesNotThrow(ConfigGuard::assertAllKeysRead);
    }

    // ---- P6 W0 (2026-09-14): make the guard able to fail ----

    @Test
    void declarationSiteIsNotARead() throws IOException {
        // P6-025 (CRITICAL): ConfigKeys.java declares each key as a literal, so scanning it
        // made every key look "read" and the guard could never fail.
        Path code = fixture("declaration-only",
                "keys/ConfigKeys.java", keysFile("FIXTURE_UNREAD", "FIXTURE_UNREAD_key"));
        System.setProperty(SOURCES_PROPERTY, code.toString());
        try {
            assertFalse(ConfigGuard.keyReadInCode("FIXTURE_UNREAD_key"),
                    "the declaration itself must not count as a read");
        } finally {
            System.clearProperty(SOURCES_PROPERTY);
        }
    }

    @Test
    void declarationSiteIsNotARead_evenWhenOtherFilesExist() throws IOException {
        // Same as above with an unrelated reader present: the declaration must not be what
        // satisfies the scan (it would satisfy it for every key, including never-read ones).
        Path code = fixture("declaration-plus-other",
                "keys/ConfigKeys.java", keysFile("FIXTURE_UNREAD2", "FIXTURE_UNREAD2_key"),
                "svc/Other.java", "package svc;\nclass Other { String v() { return System.getenv(\"SOMETHING_ELSE\"); } }\n");
        System.setProperty(SOURCES_PROPERTY, code.toString());
        try {
            assertFalse(ConfigGuard.keyReadInCode("FIXTURE_UNREAD2_key"));
        } finally {
            System.clearProperty(SOURCES_PROPERTY);
        }
    }

    @Test
    void aRealReadElsewhereIsFound() throws IOException {
        Path code = fixture("real-read",
                "keys/ConfigKeys.java", keysFile("FIXTURE_READ", "FIXTURE_READ_key"),
                "svc/Reader.java", "package svc;\nclass Reader { String v() { return System.getenv(\"FIXTURE_READ_key\"); } }\n");
        System.setProperty(SOURCES_PROPERTY, code.toString());
        try {
            assertTrue(ConfigGuard.keyReadInCode("FIXTURE_READ_key"));
        } finally {
            System.clearProperty(SOURCES_PROPERTY);
        }
    }

    @Test
    void commentMentionsAreNotReads() throws IOException {
        // P6-657: a key named in javadoc / a comment is not a read.
        Path code = fixture("comment-only",
                "keys/ConfigKeys.java", keysFile("FIXTURE_COMMENT", "FIXTURE_COMMENT_key"),
                "svc/Doc.java", "package svc;\n"
                        + "/** Documents \"FIXTURE_COMMENT_key\" without reading it. */\n"
                        + "class Doc {\n  // \"FIXTURE_COMMENT_key\" is mentioned here too\n}\n");
        System.setProperty(SOURCES_PROPERTY, code.toString());
        try {
            assertFalse(ConfigGuard.keyReadInCode("FIXTURE_COMMENT_key"),
                    "comments must not satisfy the read check");
        } finally {
            System.clearProperty(SOURCES_PROPERTY);
        }
    }

    @Test
    void unreachableSourceTreeIsFatal() throws IOException {
        // P6-265: a tree that exists but cannot be scanned is an environment fault, not a
        // licence to skip verification.
        Path notADirectory = tmp.resolve("file-not-dir").resolve("code");
        Files.createDirectories(notADirectory.getParent());
        Files.writeString(notADirectory, "not a directory\n", StandardCharsets.UTF_8);
        System.setProperty(SOURCES_PROPERTY, notADirectory.toString());
        try {
            assertThrows(IllegalStateException.class, () -> ConfigGuard.keyReadInCode("ANY_KEY"));
        } finally {
            System.clearProperty(SOURCES_PROPERTY);
        }
    }

    @Test
    void absentSourceTreeWarnsButDoesNotBlock() {
        // P6-264: a packaged run (no sources) cannot verify — that stays non-fatal, but it is
        // reported loudly instead of passing silently.
        PrintStream original = System.err;
        ByteArrayOutputStream captured = new ByteArrayOutputStream();
        System.setProperty(SOURCES_PROPERTY, tmp.resolve("no-such-tree").toString());
        System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
        try {
            assertTrue(ConfigGuard.keyReadInCode("NO_SUCH_KEY_ANYWHERE"),
                    "unverifiable sources must not block startup (documented)");
        } finally {
            System.setErr(original);
            System.clearProperty(SOURCES_PROPERTY);
        }
        String err = captured.toString(StandardCharsets.UTF_8);
        assertTrue(err.contains("SKIPPED"), "the skip must be reported loudly: " + err);
    }

    @Test
    void declaredKeysAreOnlyStaticFinalStrings() {
        // P6-656: the inventory must be the env constants — an instance or non-final String
        // field is not a key (and f.get(null) on it would throw).
        assertEquals(List.of("FIX_CONST_value"), ConfigGuard.constantStrings(FixtureKeys.class));
    }

    @Test
    void sourcesAreResolvedFromAnAnchorNotTheWorkingDirectory() throws IOException {
        // P6-658: the guard must follow an explicit anchor, not the process CWD (which differs
        // between IDE, Maven, jar and container runs).
        Path code = fixture("anchor",
                "keys/ConfigKeys.java", keysFile("FIXTURE_ANCHOR", "FIXTURE_ANCHOR_key"),
                "reader/Reader.java", "class Reader { String k = \"FIXTURE_ANCHOR_key\"; }\n");
        System.setProperty(SOURCES_PROPERTY, code.toString());
        try {
            assertTrue(ConfigGuard.keyReadInCode("FIXTURE_ANCHOR_key"),
                    "the anchored tree must be the one scanned");
            assertFalse(ConfigGuard.keyReadInCode("DEDUP_WINDOW_ENTRIES"),
                    "the repo's own keys must not be found when the anchor points elsewhere");
        } finally {
            System.clearProperty(SOURCES_PROPERTY);
        }
    }

    @Test
    void sourceCorpusHasNoFileCap() throws IOException {
        // P6-266: the old scan stopped after 2000 files (walk order decided what was seen), so a
        // large repo could miss real reads. The corpus must cover every .java file.
        Path code = fixture("many-files");
        for (int i = 0; i < 2050; i++) {
            Files.writeString(code.resolve("F" + i + ".java"),
                    "package many;\nclass F" + i + " { String k = \"MARKER_" + i + "\"; }\n",
                    StandardCharsets.UTF_8);
        }
        System.setProperty(SOURCES_PROPERTY, code.toString());
        try {
            String corpus = ConfigGuard.sourceCorpusOrNull();
            assertTrue(corpus != null, "an anchored source tree must produce a corpus");
            assertTrue(corpus.contains("\"MARKER_0\""));
            assertTrue(corpus.contains("\"MARKER_2049\""),
                    "a file past the old 2000-file cap must still be scanned");
            assertEquals(2050, corpus.split("class F", -1).length - 1,
                    "every file must appear in the single corpus scan");
        } finally {
            System.clearProperty(SOURCES_PROPERTY);
        }
    }

    /** Fixture for the reflection filter (P6-656). */
    @SuppressWarnings("unused")
    private static final class FixtureKeys {
        static final String CONST = "FIX_CONST_value";
        static String NOT_FINAL = "FIX_NOT_FINAL_value";
        final String INSTANCE = "FIX_INSTANCE_value";
        static final String NULL_VALUE = null;
        static final Object NOT_A_STRING = new Object();
    }
}
