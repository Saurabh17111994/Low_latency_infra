package com.trading.ingestion.shutdown;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * ING-FAIL-003: UncertaintyJournal — persists shutdown counters and can be read back.
 */
@DisplayName("ING-FAIL-003: UncertaintyJournal")
class UncertaintyJournalTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("Write entry → file exists with one line")
    void writeEntryCreatesFile() throws Exception {
        Path journalPath = tempDir.resolve("uncertainty-journal.jsonl");
        UncertaintyJournal journal = new UncertaintyJournal(journalPath);

        UncertaintyJournal.Entry entry = new UncertaintyJournal.Entry(
                "test-instance", Instant.now(), 1000, 950, 10, 40, 50000, 50, 2500, "shutdown"
        );

        journal.write(entry);

        assertTrue(Files.exists(journalPath), "Journal file must exist after write");
        List<String> lines = Files.readAllLines(journalPath);
        assertEquals(1, lines.size(), "First write produces exactly one line");
    }

    @Test
    @DisplayName("Multiple entries append correctly")
    void multipleEntriesAppend() throws Exception {
        Path journalPath = tempDir.resolve("uncertainty-journal.jsonl");
        UncertaintyJournal journal = new UncertaintyJournal(journalPath);

        for (int i = 0; i < 5; i++) {
            journal.write(new UncertaintyJournal.Entry(
                    "instance-" + i, Instant.now(),
                    100, 90, 5, 5, 5000, 10, 500, "test-" + i
            ));
        }

        List<String> lines = Files.readAllLines(journalPath);
        assertEquals(5, lines.size(), "Five writes produce five lines");
    }

    @Test
    @DisplayName("Entry JSON contains all fields")
    void entryContainsAllFields() {
        UncertaintyJournal.Entry entry = new UncertaintyJournal.Entry(
                "test-1", Instant.ofEpochMilli(1719000000000L),
                1000, 950, 10, 40, 50000, 50, 2500, "shutdown"
        );

        String json = entry.toJson();
        assertTrue(json.contains("\"instance\":\"test-1\""));
        assertTrue(json.contains("\"total_accepted\":1000"));
        assertTrue(json.contains("\"total_appended\":950"));
        assertTrue(json.contains("\"total_failed\":10"));
        assertTrue(json.contains("\"total_rejected\":40"));
        assertTrue(json.contains("\"total_bytes_accepted\":50000"));
        assertTrue(json.contains("\"pending_records\":50"));
        assertTrue(json.contains("\"pending_bytes\":2500"));
        assertTrue(json.contains("\"reason\":\"shutdown\""));
    }

    @Test
    @DisplayName("JSON escapes special characters in instance ID")
    void jsonEscapesSpecialCharacters() {
        UncertaintyJournal.Entry entry = new UncertaintyJournal.Entry(
                "test-\\\"quote", Instant.now(), 0, 0, 0, 0, 0, 0, 0, "reason"
        );

        String json = entry.toJson();
        assertTrue(json.contains("\\\\\\\""), "Quotes/backslashes must be escaped");
    }

    @Test
    @DisplayName("ensureWritable creates parent directory")
    void ensureWritableCreatesParent() throws Exception {        Path nested = tempDir.resolve("a/b/c/journal.jsonl");
        UncertaintyJournal journal = new UncertaintyJournal(nested);
        assertTrue(journal.ensureWritable(), "ensureWritable should succeed");
        assertTrue(java.nio.file.Files.isDirectory(tempDir.resolve("a/b/c")),
                "parent directory should be created");
    }

    @Test
    @DisplayName("P1-256: journal path being a directory is refused at readiness, not at shutdown")
    void directoryPathRefusedAtEnsureWritable() throws Exception {
        Path dir = tempDir.resolve("not-a-file");
        Files.createDirectory(dir);
        UncertaintyJournal journal = new UncertaintyJournal(dir);
        assertFalse(journal.ensureWritable(),
                "existing-directory path must fail ensureWritable instead of sailing to shutdown");
    }

    @Test
    @DisplayName("Control characters are escaped so JSONL stays one line (R-194)")
    void jsonEscapesControlCharacters() {
        UncertaintyJournal.Entry entry = new UncertaintyJournal.Entry(
                "instance", Instant.now(), 0, 0, 0, 0, 0, 0, 0, "crash reason\nsecond line\twith tab"
        );
        String json = entry.toJson();
        assertTrue(!json.contains("\n"), "newline must be escaped — JSONL invariant (R-194)");
        assertTrue(json.contains("\\t"), "tab escaped as \\t");
    }

    @Test
    @DisplayName("P1-095: null entry returns false instead of NPE-ing out of shutdown")
    void nullEntryReturnsFalse() {
        UncertaintyJournal journal =
                new UncertaintyJournal(tempDir.resolve("j.jsonl"));
        // Old code: entry.toJson() threw NullPointerException, which only
        // IOException was caught for — the NPE aborted shutdown pre-drain.
        assertFalse(assertDoesNotThrow(() -> journal.write(null)),
                "null entry must be a loud skip, never a throw");
    }

    @Test
    @DisplayName("P1-095: null shutdownTime returns false instead of throwing")
    void nullShutdownTimeReturnsFalse() {
        UncertaintyJournal journal =
                new UncertaintyJournal(tempDir.resolve("j.jsonl"));
        UncertaintyJournal.Entry entry = new UncertaintyJournal.Entry(
                "i", null, 1, 1, 0, 0, 10, 0, 0, "x");
        assertFalse(assertDoesNotThrow(() -> journal.write(entry)),
                "null shutdownTime must be a loud skip, never a throw");
    }

    @Test
    @DisplayName("P1-096: unwritable path returns false so the caller can escalate")
    void unwritablePathReturnsFalse() throws Exception {
        // A regular file where a directory is needed: createDirectories
        // throws FileAlreadyExistsException (an IOException) — deterministic,
        // no permission games, works as any user.
        Path blocker = tempDir.resolve("blocker");
        Files.write(blocker, new byte[]{1});
        UncertaintyJournal journal =
                new UncertaintyJournal(blocker.resolve("journal.jsonl"));
        UncertaintyJournal.Entry entry = new UncertaintyJournal.Entry(
                "i", Instant.now(), 1, 1, 0, 0, 10, 0, 0, "x");
        assertFalse(journal.write(entry), "lost durability must be reported");
    }

    @Test
    @DisplayName("P1-096: successful write returns true")
    void successfulWriteReturnsTrue() {
        UncertaintyJournal journal =
                new UncertaintyJournal(tempDir.resolve("j.jsonl"));
        assertTrue(journal.write(new UncertaintyJournal.Entry(
                "i", Instant.now(), 1, 1, 0, 0, 10, 0, 0, "x")));
    }

    @Test
    @DisplayName("P1-255: bare filename ensureWritable agrees with write (CWD)")
    void bareFilenameEnsureWritableMatchesWrite() {
        // UNCERTAINTY_JOURNAL_PATH=journal.jsonl has no parent: write()
        // targets CWD and succeeds, so the FATAL gate must judge CWD
        // writability — old code returned false while write() succeeded.
        // Compared against live CWD state so the test holds even where
        // CWD is not writable.
        java.nio.file.Path cwd = java.nio.file.Paths.get("").toAbsolutePath();
        UncertaintyJournal journal =
                new UncertaintyJournal(java.nio.file.Paths.get("journal.jsonl"));
        assertEquals(Files.isWritable(cwd), journal.ensureWritable(),
                "bare filename must gate on CWD, not refuse a writable path");
    }

    @Test
    @DisplayName("Bare-filename journal path does not NPE (R-117)")
    void bareFilenamePathDoesNotNpe() throws Exception {
        // Simulate UNCERTAINTY_JOURNAL_PATH=journal.jsonl (no parent).
        java.nio.file.Path cwd = java.nio.file.Paths.get("").toAbsolutePath();
        UncertaintyJournal journal = new UncertaintyJournal(cwd.resolve("journal.jsonl"));
        journal.write(new UncertaintyJournal.Entry("i", Instant.now(), 1, 1, 0, 0, 10, 0, 0, "x"));
        assertTrue(java.nio.file.Files.exists(cwd.resolve("journal.jsonl")),
                "write with bare filename must succeed (R-117)");
        java.nio.file.Files.deleteIfExists(cwd.resolve("journal.jsonl"));
    }
}
