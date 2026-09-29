package com.trading.common.schema.ddl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.trading.common.schema.ddl.DdlApplyTool.StatusDecision;
import com.trading.common.schema.ddl.DdlApplyTool.TableRecord;
import com.trading.common.schema.ddl.DdlText.ParsedDdl;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The apply-status decision (DdlApplyTool step 8): an apply is PASS only when
 * EVERY table smoke passes; composite-PK raw-client limitations (the
 * COMPAT-FLUSS-005 matrix's failing cells) are never absorbed into PASS —
 * they yield PASS_WITH_LIMITATION with dedicated exit code 6 only when
 * --ack-limitations names exactly the limited tables, exit 1 otherwise.
 * Pure JVM, no cluster.
 */
@DisplayName("DdlApplyTool status decision — PASS only when the matrix fully passes")
class DdlApplyToolStatusTest {

    private static final String ICEBERG = "Key fields must have exactly one field for iceberg format";

    private static TableRecord rec(String logicalName, String smokeOutcome) {
        TableRecord r = new TableRecord(logicalName, "phys_" + logicalName, 1, "KV", 4, true);
        r.smoke(smokeOutcome);
        return r;
    }

    private static TableRecord pass(String name) {
        return rec(name, null);
    }

    private static TableRecord limited(String name) {
        return rec(name, ICEBERG);
    }

    private static TableRecord failed(String name) {
        return rec(name, "write failed: connection reset");
    }

    @Test
    @DisplayName("all smoke rows PASS -> status PASS, exit 0")
    void allPass() {
        StatusDecision d = DdlApplyTool.decideStatus(List.of(),
                List.of(pass("a"), pass("b")), List.of());
        assertEquals("PASS", d.status());
        assertEquals(0, d.exitCode());
        assertTrue(d.limitationTables().isEmpty(), "no limitation tables");
    }

    @Test
    @DisplayName("any parity/smoke failure dominates -> FAIL, exit 1 (even with limitations + ack)")
    void failureDominates() {
        StatusDecision d = DdlApplyTool.decideStatus(List.of("phys_b smoke failed"),
                List.of(pass("a"), limited("b")), List.of("b"));
        assertEquals("FAIL", d.status());
        assertEquals(1, d.exitCode());
        assertEquals(List.of("b"), d.limitationTables(),
                "limitations still recorded in the evidence even on FAIL");
    }

    @Test
    @DisplayName("composite-PK limitation without acknowledgment refuses -> exit 1, never PASS")
    void limitationWithoutAckRefuses() {
        StatusDecision d = DdlApplyTool.decideStatus(List.of(),
                List.of(pass("a"), limited("Order_Lifecycle"), limited("Order_Correlation")),
                List.of());
        assertEquals("PASS_WITH_LIMITATION", d.status(), "status must never be PASS");
        assertEquals(1, d.exitCode(), "unacknowledged limitation must fail the apply");
        assertTrue(d.messages().stream().anyMatch(m -> m.contains("REFUSED")),
                "refusal message present");
        assertTrue(d.messages().stream().anyMatch(m -> m.contains("Order_Lifecycle")),
                "refusal names the limited tables");
        assertTrue(d.messages().stream().anyMatch(m -> m.contains("--ack-limitations")),
                "refusal points at the acknowledgment flag");
        assertTrue(d.acknowledged().isEmpty());
    }

    @Test
    @DisplayName("exact acknowledgment of the limited tables -> PASS_WITH_LIMITATION, dedicated exit 6")
    void exactAckAcknowledges() {
        StatusDecision d = DdlApplyTool.decideStatus(List.of(),
                List.of(pass("a"), limited("Order_Lifecycle"), limited("Order_Correlation")),
                List.of("Order_Lifecycle", "Order_Correlation"));
        assertEquals("PASS_WITH_LIMITATION", d.status());
        assertEquals(6, d.exitCode(),
                "exact acknowledgment is the documented Flink-only design — dedicated "
                        + "exit 6 distinguishes it from full PASS (0) and failure (1)");
        assertEquals(List.of("Order_Lifecycle", "Order_Correlation"), d.limitationTables());
        assertEquals(List.of("Order_Lifecycle", "Order_Correlation"), d.acknowledged(),
                "acknowledgment recorded for the evidence");
        assertTrue(d.messages().stream().anyMatch(m -> m.contains("acknowledged")),
                "acknowledgment message present");
    }

    @Test
    @DisplayName("acknowledgment must name exactly the limited tables — partial or extra names refused")
    void ackMismatchRejected() {
        // Partial: misses Order_Correlation.
        StatusDecision partial = DdlApplyTool.decideStatus(List.of(),
                List.of(limited("Order_Lifecycle"), limited("Order_Correlation")),
                List.of("Order_Lifecycle"));
        assertEquals(1, partial.exitCode(), "partial acknowledgment must refuse");
        assertTrue(partial.messages().stream().anyMatch(m -> m.contains("unacknowledged")),
                "refusal names the unacknowledged table");

        // Extra: names a table that is not limited.
        StatusDecision extra = DdlApplyTool.decideStatus(List.of(),
                List.of(limited("Order_Lifecycle")), List.of("Order_Lifecycle", "instruments"));
        assertEquals(1, extra.exitCode(), "acknowledging a non-limited table must refuse");
        assertTrue(extra.messages().stream().anyMatch(m -> m.contains("acknowledged but not limited")),
                "refusal flags the extra acknowledgment");
    }

    @Test
    @DisplayName("acknowledgment is order-insensitive and trimmed")
    void ackIsOrderInsensitive() {
        StatusDecision d = DdlApplyTool.decideStatus(List.of(),
                List.of(limited("Order_Lifecycle"), limited("Order_Correlation")),
                List.of(" Order_Correlation ", "Order_Lifecycle"));
        assertEquals(6, d.exitCode(), "order/whitespace must not matter — the SET must match");
        assertFalse(d.status().equals("PASS"), "still PASS_WITH_LIMITATION, never PASS");
    }

    // ── limitation prediction from the manifest (composite PK + default bucket key) ──

    private static ParsedDdl ddl(List<String> pk, String bucketKey) {
        return new ParsedDdl("t", List.of(), pk, 4, bucketKey, Map.of(), "t.sql");
    }

    private static ParsedDdl ddl(List<String> pk, String bucketKey, String kvFormatVersion) {
        Map<String, String> options = kvFormatVersion == null
                ? Map.of() : Map.of("table.kv.format-version", kvFormatVersion);
        return new ParsedDdl("t", List.of(), pk, 4, bucketKey, options, "t.sql");
    }

    @Test
    @DisplayName("prediction: composite PK + default bucket key (= PK) is the limited cell")
    void compositeDefaultBucketKeyIsPredictedLimited() {
        assertTrue(DdlApplyTool.isPredictedLimited(ddl(
                List.of("account_scope_id", "broker_order_id"),
                "account_scope_id,broker_order_id")),
                "Order_Lifecycle shape must be predicted limited");
        assertTrue(DdlApplyTool.isPredictedLimited(ddl(
                List.of("instruction_id", "execution_attempt_id"),
                "instruction_id, execution_attempt_id")),
                "whitespace-tolerant, order-insensitive set comparison");
    }

    @Test
    @DisplayName("prediction: a subset bucket key without kv.format-version=2 is the limited cell")
    void compositeSubsetWithoutFormatVersionIsPredictedLimited() {
        // L6-2: the subset cell is writable only WITH the format version — the
        // matrix cell "v1 + single-field subset bucket key" fails with the same
        // IcebergKeyEncoder error as the default-bucket cells.
        assertTrue(DdlApplyTool.isPredictedLimited(ddl(
                List.of("account_scope_id", "instrument_token"), "account_scope_id")),
                "Position_State shape without kv.format-version=2 must be predicted limited");
        assertTrue(DdlApplyTool.isPredictedLimited(ddl(
                List.of("account_scope_id", "instrument_token"), "account_scope_id", "1")),
                "v1 + subset bucket key is the failing matrix cell");
        assertFalse(DdlApplyTool.isPredictedLimited(ddl(
                List.of("account_scope_id", "instrument_token"), "account_scope_id", "2")),
                "v2 + subset bucket key is the writable matrix cell");
    }

    @Test
    @DisplayName("prediction: composite PK + subset bucket key or single-field PK are writable")
    void compositeSubsetAndSinglePkNotPredictedLimited() {
        assertFalse(DdlApplyTool.isPredictedLimited(ddl(
                List.of("instrument_token", "window_start"), "instrument_token", "2")),
                "feature_candles_15s shape (subset bucket key + v2) must NOT be limited");
        assertFalse(DdlApplyTool.isPredictedLimited(ddl(
                List.of("instrument_token", "manifest_version"), "instrument_token", "2")),
                "instruments shape (subset bucket key + v2) must NOT be limited");
        assertFalse(DdlApplyTool.isPredictedLimited(ddl(List.of("halt_request_id"),
                "halt_request_id")),
                "single-field PK (Safety_Halt_Requests) must NOT be limited");
        assertFalse(DdlApplyTool.isPredictedLimited(ddl(List.of(), "id")),
                "LOG tables are never limited");
    }

    @Test
    @DisplayName("auto acknowledgment prefills the detected set; explicit names pass through")
    void resolveAckAutoFillsExpectedAndExplicitPassesThrough() {
        List<String> expected = List.of("Order_Lifecycle", "Order_Correlation");
        assertEquals(expected, DdlApplyTool.resolveAck(List.of("auto"), expected),
                "auto fills the manifest-detected tables — operator confirms, never guesses");
        assertEquals(List.of(), DdlApplyTool.resolveAck(List.of(), expected),
                "no ack stays empty");
        assertEquals(List.of("Order_Lifecycle"),
                DdlApplyTool.resolveAck(List.of("Order_Lifecycle"), expected),
                "explicit names pass through for the set-equality check");
        assertEquals(List.of(), DdlApplyTool.resolveAck(List.of("auto"), List.of()),
                "auto with no detected limitations acknowledges nothing");
    }

    @Test
    @DisplayName("auto flow: detected set -> PASS_WITH_LIMITATION exit 6 (confirm, not guess)")
    void autoAcknowledgmentFlowExitsSix() {
        List<String> expected = List.of("Order_Lifecycle", "Order_Correlation");
        List<String> ack = DdlApplyTool.resolveAck(List.of("auto"), expected);
        StatusDecision d = DdlApplyTool.decideStatus(List.of(),
                List.of(pass("a"), limited("Order_Lifecycle"), limited("Order_Correlation")),
                ack);
        assertEquals("PASS_WITH_LIMITATION", d.status());
        assertEquals(6, d.exitCode(), "confirmed auto-acknowledgment must exit 6, never 0");
        assertEquals(expected, d.acknowledged(), "evidence records the resolved names");
    }
}
