package com.trading.common.evidence;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The evidence record is a release-gate artifact: an incomplete or aliased record must be
 * rejected at construction, not discovered later by a reviewer (P6-272, P6-273).
 */
class EvidenceRecordTest {

    private static final String[] FIELDS = {
        "workItemId", "requirementIds", "artifact", "version", "environment", "workload",
        "clock", "result", "owner", "date", "limitations",
    };

    private static final String[] VALID = {
        "TASK-1", "REQ-FLS-001", "commit abc123", "abc123", "local", "50k eps / 15 min",
        "UTC offset 0; monotonic clock", "pass p99 42ms", "saurabh", "2026-09-14", "none",
    };

    /** Builds a valid record, overriding one field by index ("3=  "). */
    private static EvidenceRecord build(String... overrides) {
        String[] f = VALID.clone();
        for (String override : overrides) {
            int eq = override.indexOf('=');
            f[Integer.parseInt(override.substring(0, eq))] = override.substring(eq + 1);
        }
        return new EvidenceRecord(
                f[0], List.of(f[1]), f[2], f[3], f[4], f[5], f[6], f[7], f[8], f[9], f[10]);
    }

    @Test
    void validRecordKeepsEveryField() {
        EvidenceRecord record = build();
        assertThat(record.workItemId()).isEqualTo("TASK-1");
        assertThat(record.requirementIds()).containsExactly("REQ-FLS-001");
        assertThat(record.limitations()).isEqualTo("none");
    }

    @Test
    void everyStringFieldIsRequired() {
        for (int i = 0; i < FIELDS.length; i++) {
            if (i == 1) {
                continue; // requirementIds is the list — covered below
            }
            final int index = i;
            assertThatThrownBy(() -> build(index + "="))
                    .as("field '%s' blank", FIELDS[index])
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(FIELDS[index]);
        }
    }

    @Test
    void nullRequirementListIsRejected() {
        assertThatThrownBy(() -> new EvidenceRecord(
                "TASK-1", null, "a", "v", "local", "w", "c", "r", "o", "d", "none"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requirementIds");
    }

    @Test
    void blankRequirementIdElementIsRejected() {
        assertThatThrownBy(() -> new EvidenceRecord(
                "TASK-1", List.of("REQ-FLS-001", " "), "a", "v", "local", "w", "c", "r", "o",
                "d", "none"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requirementIds element");
    }

    @Test
    void requirementListIsCopiedSoLaterCallerMutationsDoNotLeakIn() {
        List<String> ids = new ArrayList<>(List.of("REQ-FLS-001"));
        EvidenceRecord record = new EvidenceRecord(
                "TASK-1", ids, "a", "v", "local", "w", "c", "r", "o", "d", "none");
        ids.add("REQ-FLS-999");
        assertThat(record.requirementIds()).containsExactly("REQ-FLS-001");
    }

    @Test
    void requirementListCannotBeMutatedThroughTheAccessor() {
        EvidenceRecord record = build();
        assertThatThrownBy(() -> record.requirementIds().add("REQ-FLS-999"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
