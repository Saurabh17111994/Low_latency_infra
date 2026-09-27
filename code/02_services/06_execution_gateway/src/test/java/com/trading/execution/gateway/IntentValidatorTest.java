package com.trading.execution.gateway;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;

class IntentValidatorTest {
    private static final String VALID_HASH = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    private static IntentRecord valid() {
        return new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "MARKET", null, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, VALID_HASH, null, "1", 0);
    }
    @Test void acceptsMatchingScopeAndRejectsExpiredOrWrongScope() {
        assertThatCode(() -> IntentValidator.validate(valid(), "acct", "part", 100)).doesNotThrowAnyException();
        assertThatThrownBy(() -> IntentValidator.validate(valid(), "other", "part", 100))
                .hasMessage("account scope mismatch");
        assertThatThrownBy(() -> IntentValidator.validate(valid(), "acct", "part", 10_001))
                .hasMessage("intent expired");
    }
    @Test void limitOrdersRequirePrice() {
        IntentRecord i = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "LIMIT", null, "MIS", "DAY", "s", "1", "cfg", 1, null, VALID_HASH, null, "1", 0);
        assertThatThrownBy(() -> IntentValidator.validate(i, "acct", "part", 100))
                .hasMessage("limit order requires positive limit_price_paise");
    }
    @Test void rejectsInvalidHash() {
        IntentRecord i = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "MARKET", null, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, "bad_hash", null, "1", 0);
        assertThatThrownBy(() -> IntentValidator.validate(i, "acct", "part", 100))
                .hasMessageContaining("request_hash");
    }
    @Test void rejectsSelfSupersede() {
        IntentRecord i = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "MARKET", null, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, VALID_HASH, "i", "1", 0);
        assertThatThrownBy(() -> IntentValidator.validate(i, "acct", "part", 100))
                .hasMessageContaining("self-supersede");
    }
    @Test void rejectsMarketWithLimitPrice() {
        IntentRecord i = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "MARKET", 100L, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, VALID_HASH, null, "1", 0);
        assertThatThrownBy(() -> IntentValidator.validate(i, "acct", "part", 100))
                .hasMessageContaining("market order must not have limit");
    }

    // --- Tier 11 offline expansion: edge-case matrix ---
    @Test void rejectsUnsupportedSchemaVersion() {
        IntentRecord i = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "MARKET", null, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, VALID_HASH, null, "2", 0);
        assertThatThrownBy(() -> IntentValidator.validate(i, "acct", "part", 100))
                .hasMessageContaining("unsupported schema_version");
    }
    @Test void rejectsPartitionMismatch() {
        assertThatThrownBy(() -> IntentValidator.validate(valid(), "acct", "other-part", 100))
                .hasMessage("partition mismatch");
    }
    @Test void rejectsZeroQuantityAndToken() {
        IntentRecord q0 = new IntentRecord("i", "c", "t", "acct", "part", 0, "NSE", "ABC", "BUY", 2,
                "MARKET", null, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, VALID_HASH, null, "1", 0);
        assertThatThrownBy(() -> IntentValidator.validate(q0, "acct", "part", 100))
                .hasMessageContaining("quantity/instrument must be positive");
        IntentRecord t0 = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 0,
                "MARKET", null, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, VALID_HASH, null, "1", 0);
        assertThatThrownBy(() -> IntentValidator.validate(t0, "acct", "part", 100))
                .hasMessageContaining("quantity/instrument must be positive");
    }
    @Test void rejectsUnsupportedSideAndOrderType() {
        IntentRecord side = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "HOLD", 2,
                "MARKET", null, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, VALID_HASH, null, "1", 0);
        assertThatThrownBy(() -> IntentValidator.validate(side, "acct", "part", 100))
                .hasMessageContaining("unsupported side");
        IntentRecord type = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "STOP", null, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, VALID_HASH, null, "1", 0);
        assertThatThrownBy(() -> IntentValidator.validate(type, "acct", "part", 100))
                .hasMessageContaining("unsupported order type");
    }
    @Test void rejectsBlankStrategyFields() {
        IntentRecord blank = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "MARKET", null, "MIS", "DAY", " ", "1", "cfg", 1, 10_000L, VALID_HASH, null, "1", 0);
        assertThatThrownBy(() -> IntentValidator.validate(blank, "acct", "part", 100))
                .hasMessageContaining("is required");
    }
    @Test void rejectsBlankRequiredFields() {
        IntentRecord blankSide = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "", "  ", 2,
                "MARKET", null, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, VALID_HASH, null, "1", 0);
        assertThatThrownBy(() -> IntentValidator.validate(blankSide, "acct", "part", 100))
                .hasMessageContaining("is required");
    }
    @Test void rejectsInvalidSupersedeFormat() {
        IntentRecord badSup = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "MARKET", null, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, VALID_HASH, "bad id!", "1", 0);
        assertThatThrownBy(() -> IntentValidator.validate(badSup, "acct", "part", 100))
                .hasMessageContaining("invalid supersedes_instruction_id");
    }
    @Test void acceptsNullSupersedeAndNullExpiry() {
        IntentRecord nullExp = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "MARKET", null, "MIS", "DAY", "s", "1", "cfg", 1, null, VALID_HASH, null, "1", 0);
        assertThatCode(() -> IntentValidator.validate(nullExp, "acct", "part", 100)).doesNotThrowAnyException();
        IntentRecord nullSup = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "MARKET", null, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, VALID_HASH, null, "1", 0);
        assertThatCode(() -> IntentValidator.validate(nullSup, "acct", "part", 100)).doesNotThrowAnyException();
    }
    @Test void rejectsUppercaseHashStillAcceptedLowercaseOnly() {
        String upper = VALID_HASH.toUpperCase();
        IntentRecord i = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "MARKET", null, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, upper, null, "1", 0);
        assertThatCode(() -> IntentValidator.validate(i, "acct", "part", 100)).doesNotThrowAnyException();
    }
    @Test void rejectsNegativeLimitPrice() {
        IntentRecord i = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "LIMIT", -5L, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, VALID_HASH, null, "1", 0);
        assertThatThrownBy(() -> IntentValidator.validate(i, "acct", "part", 100))
                .hasMessageContaining("limit order requires positive");
    }

    // --- CHG-333: canonical cross-service id grammar ---
    // The compute builder mints `ei-v1-` + 64 lowercase sha256 hex (70 chars). The
    // old {1,64} bound rejected every real intent and halted the gateway at offset 0
    // (CHG-331 attempt 2); the cross-service pin lives in
    // code/01_platform/04_scripts/tests/test_instruction_id_contract.py.
    private static final String CANONICAL_ID = "ei-v1-" + "0123456789abcdef".repeat(4);

    private static IntentRecord withId(String id) {
        return new IntentRecord(id, "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "MARKET", null, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, VALID_HASH, null, "1", 0);
    }

    @Test void acceptsCanonicalComputeInstructionId() {
        assertThatCode(() -> IntentValidator.validate(withId(CANONICAL_ID), "acct", "part", 100))
                .doesNotThrowAnyException();
    }
    @Test void acceptsCanonicalSupersedesInstructionId() {
        IntentRecord i = new IntentRecord("i", "c", "t", "acct", "part", 1, "NSE", "ABC", "BUY", 2,
                "MARKET", null, "MIS", "DAY", "s", "1", "cfg", 1, 10_000L, VALID_HASH,
                CANONICAL_ID, "1", 0);
        assertThatCode(() -> IntentValidator.validate(i, "acct", "part", 100)).doesNotThrowAnyException();
    }
    @Test void rejectsNonCanonicalInstructionIds() {
        // 65-char client id: past the client bound and not the canonical form.
        assertThatThrownBy(() -> IntentValidator.validate(withId("i".repeat(65)), "acct", "part", 100))
                .hasMessageContaining("invalid instruction_id");
        // Canonical prefix with the wrong digest length (63 hex, 69 chars).
        assertThatThrownBy(() -> IntentValidator.validate(withId("ei-v1-" + "a".repeat(63)),
                "acct", "part", 100))
                .hasMessageContaining("invalid instruction_id");
        // Uppercase hex is not the minted canonical form (HexFormat.of() emits lowercase).
        assertThatThrownBy(() -> IntentValidator.validate(withId("ei-v1-" + "A".repeat(64)),
                "acct", "part", 100))
                .hasMessageContaining("invalid instruction_id");
    }

}
