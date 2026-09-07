package com.trading.ingestion.shutdown;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Persists ingestion uncertainty counters to a local journal file before
 * shutdown so they survive process restart.
 *
 * <p>Format: one JSON line per shutdown event (append-only, write-only —
 * no read-back API; implementing a resume path is out of scope. On restart
 * counter baselines are NOT restored — IngestionService logs the loss at
 * shutdown when the journal write itself fails).
 *
 * <p>Journal path: {@code /data/ingestion/uncertainty-journal.jsonl}
 * (configurable via {@code UNCERTAINTY_JOURNAL_PATH} env).
 *
 * <p>Dossier reference: {@code docs/08_implementation/03-ingestion.md} §J3, I10.
 */
public final class UncertaintyJournal {

    private static final Logger LOG = LoggerFactory.getLogger(UncertaintyJournal.class);
    private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_INSTANT;

    private final Path journalPath;
    /** In-memory entry count (R-217) — avoids re-scanning the whole file per write. */
    private final AtomicLong entryCount = new AtomicLong(0);

    public UncertaintyJournal() {
        String envPath = System.getenv("UNCERTAINTY_JOURNAL_PATH");
        String explicit = envPath != null && !envPath.isBlank()
                ? envPath
                : defaultJournalPath();
        this.journalPath = Paths.get(explicit);
    }

    /** Local development default; the container/compose override uses /data/ingestion. */
    static String defaultJournalPath() {
        String containerPath = System.getenv("UNCERTAINTY_JOURNAL_DIR");
        if (containerPath != null && !containerPath.isBlank()) {
            return containerPath + "/uncertainty-journal.jsonl";
        }
        String home = System.getenv().getOrDefault("HOME", "");
        if (home.isBlank() || System.getenv().containsKey("INGESTION_CONTAINER")) {
            return "/data/ingestion/uncertainty-journal.jsonl";
        }
        return home + "/.local/state/trading-platform/ingestion/uncertainty-journal.jsonl";
    }

    public UncertaintyJournal(Path path) {
        this.journalPath = path;
    }

    /**
     * Validate that the journal parent directory exists (creating it if
     * possible) and is writable. Returns true if the journal can be written.
     */
    public boolean ensureWritable() {
        Path parent = journalPath.getParent();
        if (parent == null) {
            // P1-255: a bare filename (no parent) writes to CWD — write()
            // allows it — so gate the FATAL check on CWD writability
            // instead of refusing a path that would succeed.
            parent = Paths.get("").toAbsolutePath();
        }
        try {
            // P1-256: the journal path itself being an existing DIRECTORY
            // would sail this check and fail loud only at shutdown-write
            // time — catch it here, at readiness.
            if (Files.exists(journalPath) && Files.isDirectory(journalPath)) {
                LOG.error("uncertainty-journal: path is a directory, not a file: {}", journalPath);
                return false;
            }
            // P1-256 follow-up: existing read-only regular file must fail readiness —
            // parent-writable alone would sail, then write() fails at shutdown.
            if (Files.exists(journalPath) && !Files.isDirectory(journalPath)
                    && !Files.isWritable(journalPath)) {
                LOG.error("uncertainty-journal: file not writable: {}", journalPath);
                return false;
            }
            Files.createDirectories(parent);
            if (!Files.isWritable(parent)) {
                LOG.error("uncertainty-journal: parent directory not writable: {}", parent);
                return false;
            }
            LOG.info("uncertainty-journal: path OK ({})", journalPath);
            return true;
        } catch (IOException e) {
            LOG.error("uncertainty-journal: cannot prepare directory {}: {}", parent, e.getMessage());
            return false;
        }
    }

    /**
     * Write a shutdown entry with cumulative counters.
     * Creates parent directories and the journal file if they don't exist.
     *
     * <p>P1-095: null entry/shutdownTime previously NPE'd out of write()
     * (only IOException was caught), aborting IngestionService.shutdown()
     * BEFORE the drain — the journal must never break shutdown.
     *
     * <p>P1-096: returns durability — false means the uncertainty state was
     * NOT persisted and the shutdown caller must escalate (disk-full, RO
     * remount, perm change after ensureWritable). Retry is pointless that
     * late, so the contract is report-loudly, not retry.
     *
     * @return true if the entry was appended, false if it was skipped or lost
     */
    public boolean write(Entry entry) {
        // P1-095: fail CLOSED without throwing — see javadoc.
        if (entry == null || entry.shutdownTime == null) {
            LOG.error("uncertainty-journal: skip null entry/time (never throw out of shutdown)");
            return false;
        }
        try {
            // R-117: a bare filename (e.g. UNCERTAINTY_JOURNAL_PATH=journal.jsonl)
            // has no parent — Files.createDirectories(null) would NPE. Guard it.
            Path parent = journalPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            String line = entry.toJson() + "\n";

            Files.write(journalPath,
                    line.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);

            long count = entryCount.incrementAndGet();
            // P1-257: sessionEntries — the counter resets per process and is
            // NOT the file's total entry count (prior runs' entries live in
            // the file too); the old label made incident readers misread it.
            LOG.info("uncertainty-journal: written (sessionEntries={}, path={})", count, journalPath);
            return true;

        } catch (IOException e) {
            LOG.error("uncertainty-journal: write failed (path={})", journalPath, e);
            return false;
        }
    }

    /** Immutable journal entry — one per shutdown. */
    public static final class Entry {
        public final String instanceId;
        public final Instant shutdownTime;
        public final long totalAccepted;
        public final long totalAppended;
        public final long totalFailed;
        public final long totalRejected;
        public final long totalBytesAccepted;
        public final long pendingRecords;
        public final long pendingBytes;
        public final String shutdownReason;

        public Entry(String instanceId,
                     Instant shutdownTime,
                     long totalAccepted,
                     long totalAppended,
                     long totalFailed,
                     long totalRejected,
                     long totalBytesAccepted,
                     long pendingRecords,
                     long pendingBytes,
                     String shutdownReason) {
            this.instanceId = instanceId;
            this.shutdownTime = shutdownTime;
            this.totalAccepted = totalAccepted;
            this.totalAppended = totalAppended;
            this.totalFailed = totalFailed;
            this.totalRejected = totalRejected;
            this.totalBytesAccepted = totalBytesAccepted;
            this.pendingRecords = pendingRecords;
            this.pendingBytes = pendingBytes;
            this.shutdownReason = shutdownReason;
        }

        String toJson() {
            return String.format(
                    "{\"instance\":\"%s\",\"shutdown_time\":\"%s\","
                            + "\"total_accepted\":%d,\"total_appended\":%d,"
                            + "\"total_failed\":%d,\"total_rejected\":%d,"
                            + "\"total_bytes_accepted\":%d,"
                            + "\"pending_records\":%d,\"pending_bytes\":%d,"
                            + "\"reason\":\"%s\"}",
                    escape(instanceId),
                    ISO.format(shutdownTime),
                    totalAccepted, totalAppended,
                    totalFailed, totalRejected,
                    totalBytesAccepted,
                    pendingRecords, pendingBytes,
                    escape(shutdownReason));
        }

        private static String escape(String s) {
            if (s == null) return "";
            // R-194: escape control characters too — an embedded \n/\t/\r in
            // instanceId or shutdownReason would break the JSONL invariant
            // (an extra physical line, malformed JSON).
            StringBuilder out = new StringBuilder(s.length() + 8);
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                switch (c) {
                    case '\\' -> out.append("\\\\");
                    case '"' -> out.append("\\\"");
                    case '\n' -> out.append("\\n");
                    case '\r' -> out.append("\\r");
                    case '\t' -> out.append("\\t");
                    case '\b' -> out.append("\\b");
                    case '\f' -> out.append("\\f");
                    default -> {
                        if (c < 0x20) {
                            out.append(String.format("\\u%04x", (int) c));
                        } else {
                            out.append(c);
                        }
                    }
                }
            }
            return out.toString();
        }
    }
}
