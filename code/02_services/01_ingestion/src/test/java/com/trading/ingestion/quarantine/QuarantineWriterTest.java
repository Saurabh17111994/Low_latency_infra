package com.trading.ingestion.quarantine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.fluss.client.table.writer.AppendResult;
import org.junit.jupiter.api.Test;

class QuarantineWriterTest {
    @Test
    void p1_090NullReasonDefaultsToInternalError() {
        // P1-090: a null reason previously NPE'd at reason.name() OUTSIDE the
        // try, breaking the never-throws contract.
        assertEquals(QuarantineWriter.Reason.INTERNAL_ERROR.name(),
                QuarantineWriter.safeReasonName(null));
        for (QuarantineWriter.Reason r : QuarantineWriter.Reason.values()) {
            assertEquals(r.name(), QuarantineWriter.safeReasonName(r));
        }
    }

    @Test
    void sanitizesCredentialBearingDetailsAndBoundsLength() {
        String detail = "ARROW_TOKEN=secret Authorization:Bearer-secret " + "x".repeat(800);
        String safe = QuarantineWriter.sanitizeDetail(detail);
        assertFalse(safe.contains("secret"));
        assertTrue(safe.contains("ARROW_TOKEN=[REDACTED]"));
        assertTrue(safe.length() <= 512);
    }

    // ING-SEC-RED-001 — inject every mandated secret class into a detail string
    // and assert none survive the Java boundary sanitizer.
    @Test
    void ingSecRed001SecretRedaction() {
        String detail = String.join(" | ",
                "ARROW_APP_SECRET=superSecretAppSecret123",
                "ARROW_PASSWORD=P@ssw0rd!secret",
                "ARROW_TOTP_KEY=JBSWY3DPEHPK3PXP",
                "ARROW_TOKEN=eyJhbGciOiJIUzI1NiJ9.secret",
                "access_token=ghp_secretToken456",
                "token=abcd1234secret",
                "appID=b3b40c832fcd",
                "Authorization=Bearer secretBearerToken",
                "https://socket.arrow.trade?appID=b3b40c832fcd&token=secretQueryToken");
        String safe = QuarantineWriter.sanitizeDetail(detail);
        // No raw secret values survive (the only acceptable occurrence is the
        // literal placeholder [REDACTED]).
        assertFalse(safe.contains("superSecretAppSecret123"));
        assertFalse(safe.contains("P@ssw0rd"));
        assertFalse(safe.contains("JBSWY3DPEHPK3PXP"));
        assertFalse(safe.contains("eyJhbGciOiJIUzI1NiJ9"));
        assertFalse(safe.contains("ghp_secretToken456"));
        assertFalse(safe.contains("abcd1234secret"));
        assertFalse(safe.contains("b3b40c832fcd"));
        assertFalse(safe.contains("secretBearerToken"));
        assertFalse(safe.contains("secretQueryToken"));
        assertTrue(safe.length() <= 512, "detail must be bounded to 512 chars");
    }

    @Test
    void p4_330SchemaVersionMatchesHeader() {
        // Header/manifest say 1 (bare numeric); the writer previously wrote "v1".
        assertEquals("1", QuarantineWriter.SCHEMA_VERSION);
    }

    @Test
    void p4_222EmptyPayloadHashesItsBytesNeverBlank() {
        // Missing payloads previously all shared payload_hash="" — collide.
        String nullHash = QuarantineWriter.computePayloadHash(null);
        String emptyHash = QuarantineWriter.computePayloadHash(new byte[0]);
        assertEquals(64, nullHash.length(), "SHA-256 hex, not blank");
        assertEquals(nullHash, emptyHash, "null coerces to empty bytes, same digest");
        String realHash = QuarantineWriter.computePayloadHash(new byte[] {1, 2, 3});
        assertEquals(64, realHash.length());
        assertFalse(realHash.equals(nullHash), "distinct bytes, distinct hash");
    }

    @Test
    void observePropagatesAsyncAppendFailures() {
        // R-033: a discarded append future silently loses quarantine evidence.
        CompletableFuture<AppendResult> failed =
                CompletableFuture.failedFuture(new RuntimeException("coordinator unreachable"));
        CompletableFuture<AppendResult> guarded =
                QuarantineWriter.observe(failed, "q-1", "MISSING_INSTRUMENT");
        assertTrue(failed.isCompletedExceptionally());
        assertThrows(CompletionException.class, guarded::join);
    }

    @Test
    void observeCompletesNormallyOnSuccessfulAppend() {
        CompletableFuture<AppendResult> ok = CompletableFuture.completedFuture(null);
        CompletableFuture<AppendResult> guarded =
                QuarantineWriter.observe(ok, "q-2", "MISSING_INSTRUMENT");
        assertEquals(null, guarded.join());
    }
}
