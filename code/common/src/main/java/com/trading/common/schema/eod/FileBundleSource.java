package com.trading.common.schema.eod;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * {@link EncryptedExportEodOffloadExecutor.BundleSource} that reads the
 * plaintext bundle for a record from {@code <recordId>.plain} on the source
 * dir — the drill/dev twin of the (documented, env-gated) Fluss reader: a
 * staging file produced by the export tooling or a prior dump.
 */
public final class FileBundleSource implements EncryptedExportEodOffloadExecutor.BundleSource {

    private final Path sourceDir;

    public FileBundleSource(Path sourceDir) {
        this.sourceDir = sourceDir;
    }

    @Override
    public byte[] read(EodOffloadRecord record) throws Exception {
        // P4-293: defense-in-depth — record fields become a path. Validate
        // against the allowlist and contain the resolution (Fluss-sourced or
        // future records must not escape sourceDir via '/' or '..').
        String tradingDate = record.tradingDate();
        String tableName = record.tableName();
        if (tradingDate == null || !tradingDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
            throw new IllegalArgumentException("invalid tradingDate: " + tradingDate);
        }
        if (tableName == null || !tableName.matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("invalid tableName: " + tableName);
        }
        Path base = sourceDir.toAbsolutePath().normalize();
        Path p = base.resolve(tradingDate + "__" + tableName + ".plain").normalize();
        if (!p.startsWith(base)) {
            throw new java.io.FileNotFoundException("bundle source escapes sourceDir: " + p);
        }
        if (!Files.exists(p)) {
            throw new java.io.FileNotFoundException("bundle source missing: " + p);
        }
        return Files.readAllBytes(p);
    }
}
