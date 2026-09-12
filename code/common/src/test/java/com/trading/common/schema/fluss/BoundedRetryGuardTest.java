package com.trading.common.schema.fluss;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Structural guard for the C5 retry policy — the enforcement point that a hand-maintained
 * inventory could never be.
 *
 * <p>Why this exists: the retry policy was built once (CHG-119) and applied by hand to two
 * call sites; every other site silently opted out, and three successive inventories of those
 * sites were each wrong in a different way (missing whole stores, then false-positiving a
 * batch scan). A list cannot hold this boundary, so the boundary is asserted by a test.
 *
 * <p>Rule: every Fluss await in the runtime source must EITHER be an argument of
 * {@link BoundedRetry} {@code .await(...)} / {@code .run(...)} (the wrapper opens within the
 * preceding few lines), OR carry an explicit {@code retry-exempt:} comment giving the reason.
 * Anything else fails the build naming the exact file and line.
 *
 * <p>Exemptions are deliberate and visible in the code they apply to — currently only LOG
 * appends whose duplicates are tolerable, contrasted with {@code Fills}, which must stay
 * un-retried until its claimed downstream fingerprint dedup is actually verified.
 */
class BoundedRetryGuardTest {

    /**
     * A Fluss await: {@code .get(<timeout>, TimeUnit.X)}.
     *
     * <p>Deliberately greedy up to {@code TimeUnit.} — an earlier {@code [^)]*} form could not
     * match {@code timeout.toMillis()} (the argument contains a parenthesis), so it silently
     * skipped every site using the builder form. A guard with a false negative is worse than
     * no guard, because it reads as green.
     */
    private static final Pattern AWAIT = Pattern.compile("\\.get\\(.*TimeUnit\\.[A-Z]+");

    /**
     * One-shot operator tools, not the runtime path: they run on their own 30s budget
     * (DdlApplyTool) rather than the gateway's 2s request timeout, and are driven by an
     * operator rather than by live traffic. Excluded by name so the exclusion is visible.
     */
    private static final List<String> OPERATOR_TOOLS = List.of(
            "DdlApplyTool.java", "EodControllerTool.java", "TickTableViewer.java");

    /** How far above an await the wrapper/marker may open (the style used across the stores). */
    private static final int LOOKBACK = 4;

    /**
     * Burn-down list of bypasses found when this guard was first switched on. Now EMPTY:
     * the two runtime sites it listed were wrapped, and the four harness sites are covered
     * by a {@code retry-exempt-file:} marker instead. Do not add entries — wrap the site, or
     * justify a marker on the site itself.
     */
    private static final List<String> KNOWN_UNRETRIED = List.of();

    /**
     * File-level exemption for sources that must not be retried as a whole — currently only
     * the composite-key verification harness, whose entire purpose is to observe raw Fluss
     * behaviour and which would be defeated by retrying.
     */
    private static final String FILE_EXEMPT = "retry-exempt-file:";

    private static final String EXEMPT = "retry-exempt:";

    @Test
    @DisplayName("every Fluss await goes through BoundedRetry, or declares retry-exempt with a reason")
    void everyAwaitIsRetriedOrExempt() throws IOException {
        List<Path> roots = new ArrayList<>();
        Path here = Paths.get("").toAbsolutePath();
        roots.add(here.resolve("src/main/java"));
        // The gateway's stores live in a sibling module; include it when present so the guard
        // covers the runtime surface, not just this module.
        Path gateway = here.resolve("../02_services/06_execution_gateway/src/main/java").normalize();
        if (Files.isDirectory(gateway)) {
            roots.add(gateway);
        }

        List<String> bypasses = new ArrayList<>();
        int scanned = 0;
        for (Path root : roots) {
            if (!Files.isDirectory(root)) {
                continue;
            }
            try (Stream<Path> files = Files.walk(root)) {
                for (Path f : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                    if (OPERATOR_TOOLS.contains(f.getFileName().toString())) {
                        continue;
                    }
                    List<String> lines = Files.readAllLines(f);
                    if (lines.stream().anyMatch(l -> l.contains(FILE_EXEMPT))) {
                        continue;
                    }
                    for (int i = 0; i < lines.size(); i++) {
                        if (!AWAIT.matcher(lines.get(i)).find()) {
                            continue;
                        }
                        scanned++;
                        boolean covered = false;
                        for (int j = Math.max(0, i - LOOKBACK); j <= i && !covered; j++) {
                            String line = lines.get(j);
                            if (line.contains("BoundedRetry.") || line.contains(EXEMPT)) {
                                covered = true;
                            }
                        }
                        if (!covered) {
                            String key = root.relativize(f) + ":" + (i + 1);
                            if (!KNOWN_UNRETRIED.contains(key)) {
                                bypasses.add(key + "  " + lines.get(i).trim());
                            }
                        }
                    }
                }
            }
        }

        // If this fires, the guard itself is broken (e.g. the stores changed await form) and
        // is silently passing — a guard that scans nothing must not be mistaken for a green one.
        assertTrue(scanned > 0, "guard scanned no awaits at all — the AWAIT pattern is stale");

        assertTrue(bypasses.isEmpty(),
                "Fluss awaits bypassing the shared retry policy. Wrap them in "
                        + "BoundedRetry.await(...), or add a `" + EXEMPT + " <reason>` comment "
                        + "on the site:\n  " + String.join("\n  ", bypasses));
    }
}
