package com.trading.compute.signaljob;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * SIG-UNIT-007 dependency scan: the compute module must never declare or import
 * Apache Flink CEP (project rule in 01-foundation.md — no CEP in the MVP order
 * path). Three legs:
 *
 * <ol>
 *   <li>in-JVM scan: the module pom.xml, JVM build files and java/scala/kotlin sources contain none of
 *       the forbidden dependency/import literals (mirrors the repo's
 *       {@code cep_guard.sh} file patterns and exclusions);</li>
 *   <li>shell-guard agreement: running the repo's {@code cep_guard.sh} scoped
 *       to the compute module passes — the shell gate and this test must agree
 *       on the verdict;</li>
 *   <li>scan-scope parity: the file set this test scans is byte-for-byte the
 *       file set the shell guard scans — a future edit to either
 *       implementation's include/exclude rules cannot silently narrow the
 *       other's scope.</li>
 * </ol>
 *
 * <p>This test class deliberately never spells the forbidden literals in its
 * own source (it would trip itself): the patterns are assembled from parts.
 */
@DisplayName("SIG-UNIT-007 dependency scan — no Flink CEP dependency or import in the compute module")
class CepDependencyGuardTest {

    /** Assembled from parts so this file's own source never matches. */
    private static final Pattern FORBIDDEN = Pattern.compile(
            "flink-" + "cep|org\\.apache\\.flink\\." + "cep");

    private static final List<String> SCANNED_FILENAMES = List.of("pom.xml",
            "build.gradle", "build.gradle.kts", "settings.gradle", "settings.gradle.kts",
            "build.sbt", "libs.versions.toml", "extensions.xml");
    private static final List<String> SCANNED_SUFFIXES = List.of(".java", ".scala", ".kt", ".kts");
    private static final List<String> SKIPPED_DIRS = List.of(".git", "target", "node_modules");

    @Test
    @DisplayName("no CEP dependency declaration or CEP import exists in the module")
    void noFlinkCepDependencyOrImport() throws IOException {
        Path root = moduleRoot();
        List<Path> hits = scannedFiles(root).stream()
                .filter(this::containsForbidden)
                .map(root::relativize)
                .toList();
        assertTrue(hits.isEmpty(),
                "Flink CEP is forbidden in the compute module (01-foundation.md); found in: " + hits);
    }

    @Test
    @DisplayName("the repo cep_guard.sh shell gate agrees — running it on the module passes")
    void shellGuardAgreesOnModule() throws IOException, InterruptedException {
        Path root = moduleRoot();
        Path guard = repoRoot().resolve("code/01_platform/04_scripts/cep_guard.sh");
        assertTrue(Files.isRegularFile(guard), "missing guard script " + guard);

        Process p = new ProcessBuilder("bash", guard.toString(), root.toString())
                .redirectErrorStream(true)
                .start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exit = p.waitFor();

        assertEquals(0, exit,
                "cep_guard.sh failed on the compute module — the shell gate and this test disagree:\n" + out);
        assertTrue(out.contains("OK: no Flink CEP references found"),
                "cep_guard.sh did not report the clean verdict:\n" + out);
    }

    @Test
    @DisplayName("scan-scope parity — the shell guard and this test scan the identical file set")
    void shellGuardAndTestScanIdenticalFileSet() throws IOException, InterruptedException {
        Path root = moduleRoot();

        Set<Path> testFiles = scannedFiles(root).stream()
                .map(root::relativize)
                .collect(Collectors.toSet());
        assertFalse(testFiles.isEmpty(),
                "the test scans nothing — the module root resolution must be wrong");

        // Guard-side file list: grep -l with the exact flags cep_guard.sh uses
        // and a pattern that matches every line, so it lists every file it
        // would scan. If the guard's include/exclude rules ever drift from the
        // test's constants, this set diverges.
        Process p = new ProcessBuilder(
                        "grep", "-rEl",
                        "--include=pom.xml",
                        "--include=*.java", "--include=*.scala",
                        "--include=*.kt", "--include=*.kts",
                        "--include=build.gradle", "--include=build.gradle.kts",
                        "--include=settings.gradle", "--include=settings.gradle.kts",
                        "--include=build.sbt", "--include=libs.versions.toml",
                        "--include=extensions.xml",
                        "--exclude-dir=.git", "--exclude-dir=target", "--exclude-dir=node_modules",
                        "^", root.toString())
                .redirectErrorStream(true)
                .start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        assertEquals(0, p.waitFor(), "grep file enumeration failed:\n" + out);

        Set<Path> guardFiles = new HashSet<>();
        for (String line : out.split("\n")) {
            if (!line.isBlank()) {
                guardFiles.add(root.relativize(Path.of(line)));
            }
        }
        assertFalse(guardFiles.isEmpty(), "the shell guard scans nothing");

        assertEquals(guardFiles, testFiles,
                "scan-scope disagreement: guard and test enumerate different file sets "
                        + "(guard-only: " + onlyIn(guardFiles, testFiles)
                        + "; test-only: " + onlyIn(testFiles, guardFiles) + ")");
    }

    private static List<Path> scannedFiles(Path root) throws IOException {
        List<Path> out = new ArrayList<>();
        try (Stream<Path> walk = Files.walk(root)) {
            walk.filter(Files::isRegularFile)
                    .filter(CepDependencyGuardTest::isScannedFile)
                    .filter(p -> !isSkipped(p))
                    .forEach(out::add);
        }
        return out;
    }

    private static Set<Path> onlyIn(Set<Path> a, Set<Path> b) {
        return a.stream().filter(p -> !b.contains(p)).collect(Collectors.toSet());
    }

    private static boolean isScannedFile(Path p) {
        String name = p.getFileName().toString();
        if (SCANNED_FILENAMES.contains(name)) {
            return true;
        }
        for (String suffix : SCANNED_SUFFIXES) {
            if (name.endsWith(suffix)) {
                return true;
            }
        }
        return false;
    }

    private boolean containsForbidden(Path p) {
        try {
            String content = Files.readString(p, StandardCharsets.UTF_8);
            return FORBIDDEN.matcher(content).find();
        } catch (IOException e) {
            // A file that cannot be read must fail the scan loudly, like the
            // shell guard's set -euo pipefail — never silently pass.
            throw new IllegalStateException("cannot scan " + p + " for the CEP guard", e);
        }
    }

    private static boolean isSkipped(Path p) {
        for (Path part : p) {
            if (SKIPPED_DIRS.contains(part.toString())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Surefire runs with the module basedir as working directory; also accept
     * a repo-root working directory (IDE runs). Fail loudly if neither is
     * found — a scan of nothing must never pass (mirrors cep_guard.sh R-091).
     */
    private static Path moduleRoot() {
        for (String candidate : List.of("pom.xml", "code/02_services/02_compute/pom.xml")) {
            Path pom = Path.of(candidate);
            if (Files.isRegularFile(pom)) {
                return pom.toAbsolutePath().getParent();
            }
        }
        throw new AssertionError(
                "cannot locate the compute module root (no pom.xml in cwd or repo-relative path)");
    }

    /** Repo root = three levels above the module root (code/02_services/02_compute). */
    private static Path repoRoot() {
        Path root = moduleRoot().getParent().getParent().getParent();
        assertTrue(Files.isDirectory(root.resolve("code")),
                "resolved repo root " + root + " does not contain code/");
        return root;
    }
}
