package com.trading.common.version;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * R-048/R-079/R-182/R-268 — version gate hardening: placeholders rejected at
 * the gate, whitespace trimmed, null list guarded, and placeholder detection
 * is sentinel-shape based so it survives the constants being replaced.
 */
@DisplayName("R-048: VersionGate rejects placeholders and trims")
class VersionGateTest {

    @Test
    @DisplayName("absent/latest/blank all rejected")
    void rejectsAbsentLatestBlank() {
        assertThrows(IllegalStateException.class, () -> VersionGate.requirePinned("x", null));
        assertThrows(IllegalStateException.class, () -> VersionGate.requirePinned("x", "  "));
        assertThrows(IllegalStateException.class, () -> VersionGate.requirePinned("x", "latest"));
        assertThrows(IllegalStateException.class, () -> VersionGate.requirePinned("x", " LATEST "));
    }

    @Test
    @DisplayName("placeholder sentinels rejected at the gate (R-048)")
    void rejectsPlaceholders() {
        assertThrows(IllegalStateException.class,
                () -> VersionGate.requirePinned("x", "FLINK_VERSION_TO_BE_PINNED"));
        assertThrows(IllegalStateException.class,
                () -> VersionGate.requirePinned("x", "ARROW_API_CONTRACT_TO_BE_VERIFIED"));
        // isPinnedAndVerified is false for placeholders regardless of evidence.
        assertFalse(VersionGate.isPinnedAndVerified(
                "x", "FLINK_VERSION_TO_BE_PINNED", true));
    }

    @Test
    @DisplayName("requirePinned returns the trimmed value (R-079)")
    void returnsTrimmed() {
        assertEquals("1.2.3", VersionGate.requirePinned("x", "  1.2.3  "));
    }

    @Test
    @DisplayName("requireAllPinned null-guards (R-182)")
    void requireAllPinnedNullGuards() {
        assertThrows(IllegalArgumentException.class,
                () -> VersionGate.requireAllPinned(null));
        // A null element fails with the descriptive IllegalStateException.
        // (java.util.Arrays.asList allows nulls; List.of does not.)
        assertThrows(IllegalStateException.class,
                () -> VersionGate.requireAllPinned(
                        java.util.Arrays.asList("1.0", null)));
    }

    @Test
    @DisplayName("placeholder detection is sentinel-shape based (R-268)")
    void placeholderShapeDetection() {
        assertTrue(PlaceholderVersions.isPlaceholder("FLINK_VERSION_TO_BE_PINNED"));
        assertTrue(PlaceholderVersions.isPlaceholder("anything_TO_BE_PINNED"));
        assertTrue(PlaceholderVersions.isPlaceholder("SCHEMA_LIFECYCLE_TO_BE_VERIFIED"));
        // Even after a constant is replaced by a real version, an un-pinned
        // sentinel still carries the marker and is detected.
        // P6-856: the marker shape is documented WITH the leading underscore
        // (R-268) and every constant in PlaceholderVersions carries it. The bare
        // word is prose (docs, status columns), not a pin marker — matching it
        // refused legitimate values.
        assertFalse(PlaceholderVersions.isPlaceholder("TO_BE_VERIFIED"));
        assertFalse(PlaceholderVersions.isPlaceholder("note: TO_BE_VERIFIED in matrix"));
        assertFalse(PlaceholderVersions.isPlaceholder("2.2.1"));
        assertFalse(PlaceholderVersions.isPlaceholder(null));
    }
}
