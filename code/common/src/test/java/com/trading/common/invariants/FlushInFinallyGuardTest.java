package com.trading.common.invariants;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Enforces the no-{@code .flush()}-inside-{@code finally} rule across every
 * main source tree, via the repo guard {@code 01_platform/04_scripts/flush_guard.sh}
 * (same shell-guard-plus-JUnit shape as {@code CepDependencyGuardTest}).
 *
 * <p>The rule exists because Fluss's {@code TableWriter.flush()} is not a
 * per-writer flush: it awaits every pending batch on the connection and in
 // Version note (2026-09-23): the 0.9.1 claims in this file were re-checked against Fluss 1.0.0 and still hold — flush()/close is still unbounded in 1.0.0 (`fluss-client/.../write/RecordAccumulator.java:149`, unchanged since 0.9.1) and `TableWriter`/`UpsertWriter` still expose no `close()`. Re-check on the next upgrade (DEC-052).
 * 0.9.1-incubating that await is unbounded. In a {@code finally} it is a no-op
 * on success (the ack was already awaited) and on failure it blocks forever,
 * so it masks the bounded-timeout failure it looks like it guards against —
 * exactly the bug removed from {@code FlussEodStateStore},
 * {@code EodControllerTool.copyBucket}, {@code DdlApplyTool} and
 * {@code CompositeKeyMatrixVerifier} on 2026-09-11. A live-cluster test cannot
 * catch a reintroduction (a healthy cluster never blocks, so the drill passes
 * either way); this static rule can.
 *
 * <p>Three legs: the real tree passes, the guard genuinely detects a violation
 * (so a broken guard cannot pass by scanning nothing or matching nothing), and
 * test sources are out of scope by design.
 */
@DisplayName("invariant: no .flush() inside finally in main sources (unbounded, masks bounded timeouts)")
class FlushInFinallyGuardTest {

    private static final String GUARD_REL = "01_platform/04_scripts/flush_guard.sh";

    /**
     * Floor for the scanned file count. The tree has ~290 main java files; a
     * guard whose include rules silently broke would report 0 and "pass", so
     * assert a real scan happened.
     */
    private static final int MIN_SCANNED_FILES = 200;

    private static final Pattern SCANNED = Pattern.compile("scanned (\\d+) files");

    @Test
    @DisplayName("the guard passes on the real main sources and actually scans them")
    void guardPassesOnRealMainSources() throws Exception {
        Path root = codeRoot();
        Result r = runGuard(root);

        assertEquals(0, r.exit, "flush_guard.sh failed on the real tree:\n" + r.output);
        assertTrue(r.output.contains("OK: no .flush() inside finally blocks"),
                "guard did not report the clean verdict:\n" + r.output);

        Matcher m = SCANNED.matcher(r.output);
        assertTrue(m.find(), "guard did not report a scanned-file count:\n" + r.output);
        int scanned = Integer.parseInt(m.group(1));
        assertTrue(scanned >= MIN_SCANNED_FILES,
                "guard scanned only " + scanned + " files (expected >= " + MIN_SCANNED_FILES
                        + ") — its include rules are broken and it would pass on an empty set");
    }

    @Test
    @DisplayName("the guard detects a flush-in-finally violation (its teeth)")
    void guardDetectsAViolation(@TempDir Path tmp) throws Exception {
        Path violator = tmp.resolve("src/main/java/Violator.java");
        Files.createDirectories(violator.getParent());
        Files.writeString(violator,
                "class Violator {\n"
                        + "    void write(Writer w) {\n"
                        + "        try {\n"
                        + "            w.upsert(row).get(1000, MILLISECONDS);\n"
                        + "        } finally {\n"
                        + "            w.flush();\n"
                        + "        }\n"
                        + "    }\n"
                        + "}\n",
                StandardCharsets.UTF_8);

        Result r = runGuard(tmp);

        assertNotEquals(0, r.exit,
                "the guard PASSED a file with .flush() in a finally block — it has no teeth:\n"
                        + r.output);
        assertTrue(r.output.contains("Violator.java"),
                "guard failed but did not name the offending file:\n" + r.output);
    }

    @Test
    @DisplayName("test sources are out of scope (live tests flush in finally by design)")
    void guardIgnoresTestSources(@TempDir Path tmp) throws Exception {
        Path testFile = tmp.resolve("src/test/java/LiveDrill.java");
        Files.createDirectories(testFile.getParent());
        Files.writeString(testFile,
                "class LiveDrill {\n"
                        + "    void write(Writer w) {\n"
                        + "        try {\n"
                        + "            w.upsert(row);\n"
                        + "        } finally {\n"
                        + "            w.flush();\n"
                        + "        }\n"
                        + "    }\n"
                        + "}\n",
                StandardCharsets.UTF_8);

        Result r = runGuard(tmp);

        assertEquals(0, r.exit,
                "the guard flagged a TEST source; the live drills flush in finally on purpose "
                        + "and are out of scope:\n" + r.output);
    }

    private record Result(int exit, String output) {}

    private static Result runGuard(Path root) throws IOException, InterruptedException {
        Path script = codeRoot().resolve(GUARD_REL);
        assertTrue(Files.isRegularFile(script), "missing guard script " + script);

        List<String> cmd = new ArrayList<>(List.of("bash", script.toString(), root.toString()));
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
        Process p = pb.start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        return new Result(p.waitFor(), out);
    }

    /**
     * The directory containing {@code 01_platform/}. Surefire runs with the
     * module basedir as cwd ({@code code/common}); an IDE may run from the code
     * root or the repo root. Fail loudly if none is found — never scan nothing.
     */
    private static Path codeRoot() {
        for (String candidate : List.of(".", "..", "../..")) {
            Path p = Path.of(candidate).toAbsolutePath().normalize();
            if (Files.isRegularFile(p.resolve(GUARD_REL))) {
                return p;
            }
        }
        throw new AssertionError("cannot locate the code root (no " + GUARD_REL
                + " in cwd, parent, or grandparent)");
    }
}
