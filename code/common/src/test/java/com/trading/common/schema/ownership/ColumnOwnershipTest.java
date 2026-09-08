package com.trading.common.schema.ownership;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * SCH-15 unit tests for the {@link ColumnOwnership} matrix validator: the
 * fail-closed invariants (in-range + unique indexes, one declared writer per
 * column group, identity never partial-updated, no unowned column) and the
 * {@code checkWrite} enforcement guard.
 */
class ColumnOwnershipTest {

    private static ColumnOwnership valid() {
        return new ColumnOwnership(
                "t", "1", "svc",
                new String[] {"id", "a", "b", "c"},
                new int[] {0},
                new ColumnOwnership.Writer("svc:w1", 1, 2, 3));
    }

    @Test
    void validMatrixConstructsAndExposesFields() {
        ColumnOwnership m = valid();
        assertThat(m.tableName()).isEqualTo("t");
        assertThat(m.schemaVersion()).isEqualTo("1");
        assertThat(m.owner()).isEqualTo("svc");
        assertThat(m.isIdentity(0)).isTrue();
        assertThat(m.isIdentity(1)).isFalse();
        assertThat(m.writerFor(1)).isEqualTo("svc:w1");
        assertThat(m.writerFor(0)).isNull();
    }

    @Test
    void blankTableOrVersionOrOwnerRejected() {
        assertThatThrownBy(() -> new ColumnOwnership(" ", "1", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {0},
                new ColumnOwnership.Writer("svc:w1", 1, 2, 3)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ColumnOwnership("t", "", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {0},
                new ColumnOwnership.Writer("svc:w1", 1, 2, 3)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ColumnOwnership("t", "1", null,
                new String[] {"id", "a", "b", "c"}, new int[] {0},
                new ColumnOwnership.Writer("svc:w1", 1, 2, 3)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void emptyOrBlankColumnNamesRejected() {
        assertThatThrownBy(() -> new ColumnOwnership("t", "1", "svc",
                new String[] {}, new int[] {0},
                new ColumnOwnership.Writer("svc:w1", 1, 2, 3)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ColumnOwnership("t", "1", "svc",
                new String[] {"id", " ", "b", "c"}, new int[] {0},
                new ColumnOwnership.Writer("svc:w1", 1, 2, 3)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void emptyIdentityRejected() {
        assertThatThrownBy(() -> new ColumnOwnership("t", "1", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {},
                new ColumnOwnership.Writer("svc:w1", 1, 2, 3)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void outOfRangeIdentityRejected() {
        assertThatThrownBy(() -> new ColumnOwnership("t", "1", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {4},
                new ColumnOwnership.Writer("svc:w1", 1, 2, 3)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void duplicateIdentityRejected() {
        assertThatThrownBy(() -> new ColumnOwnership("t", "1", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {0, 0},
                new ColumnOwnership.Writer("svc:w1", 1, 2, 3)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void noWritersRejected() {
        assertThatThrownBy(() -> new ColumnOwnership("t", "1", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {0}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void emptyWriterRejected() {
        assertThatThrownBy(() -> new ColumnOwnership("t", "1", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {0},
                new ColumnOwnership.Writer("svc:w1")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new ColumnOwnership("t", "1", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {0},
                new ColumnOwnership.Writer(" ")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void writerOutOfRangeRejected() {
        assertThatThrownBy(() -> new ColumnOwnership("t", "1", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {0},
                new ColumnOwnership.Writer("svc:w1", 1, 2, 4)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void duplicateWriterNameRejected() {
        assertThatThrownBy(() -> new ColumnOwnership("t", "1", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {0},
                new ColumnOwnership.Writer("svc:w1", 1, 2),
                new ColumnOwnership.Writer("svc:w1", 3)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate writer name");
    }

    @Test
    void nullWriterEntryRejected() {
        assertThatThrownBy(() -> new ColumnOwnership("t", "1", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {0},
                new ColumnOwnership.Writer("svc:w1", 1, 2, 3), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("null entry");
    }

    @Test
    void duplicateWithinWriterRejected() {
        assertThatThrownBy(() -> new ColumnOwnership("t", "1", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {0},
                new ColumnOwnership.Writer("svc:w1", 1, 1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void overlappingWritersRejected() {
        assertThatThrownBy(() -> new ColumnOwnership("t", "1", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {0},
                new ColumnOwnership.Writer("svc:w1", 1, 2),
                new ColumnOwnership.Writer("svc:w2", 2, 3)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("one declared writer per column group");
    }

    @Test
    void identityClaimedByWriterRejected() {
        assertThatThrownBy(() -> new ColumnOwnership("t", "1", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {0},
                new ColumnOwnership.Writer("svc:w1", 0, 1, 2, 3)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("creation-only");
    }

    @Test
    void unownedColumnRejected() {
        assertThatThrownBy(() -> new ColumnOwnership("t", "1", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {0},
                new ColumnOwnership.Writer("svc:w1", 1, 2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("owned by no writer");
    }

    @Test
    void checkWriteAllowsOwnedColumns() {
        valid().checkWrite("svc:w1", 1, 2, 3);
        valid().checkWrite("svc:w1");
    }

    @Test
    void checkWriteRejectsForeignColumn() {
        ColumnOwnership m = new ColumnOwnership("t", "1", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {0},
                new ColumnOwnership.Writer("svc:w1", 1, 2),
                new ColumnOwnership.Writer("svc:w2", 3));
        assertThatThrownBy(() -> m.checkWrite("svc:w1", 1, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("svc:w2");
        assertThatThrownBy(() -> m.checkWrite("svc:nobody", 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void checkWriteRejectsIdentityColumn() {
        assertThatThrownBy(() -> valid().checkWrite("svc:w1", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("identity column");
    }

    @Test
    void checkWriteRejectsOutOfRange() {
        assertThatThrownBy(() -> valid().checkWrite("svc:w1", 5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void checkWriteRejectsBlankWriterName() {
        assertThatThrownBy(() -> valid().checkWrite(" ", 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void checkWritePartialUpdateRespectsOwnership_Sch15() {
        // SCH-15: partial_update must respect column ownership — a writer may only
        // touch its declared column group; identity columns are creation-only.
        ColumnOwnership m = new ColumnOwnership("t", "1", "svc",
                new String[] {"id", "a", "b", "c"}, new int[] {0},
                new ColumnOwnership.Writer("svc:w1", 1, 2),
                new ColumnOwnership.Writer("svc:w2", 3));
        // w1 can partial-update its own columns
        m.checkWrite("svc:w1", 1);
        m.checkWrite("svc:w1", 2);
        m.checkWrite("svc:w1", 1, 2);
        // w1 cannot touch w2's column
        assertThatThrownBy(() -> m.checkWrite("svc:w1", 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("svc:w2");
        // neither writer can partial-update identity
        assertThatThrownBy(() -> m.checkWrite("svc:w1", 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("identity column");
        assertThatThrownBy(() -> m.checkWrite("svc:w2", 0, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("identity column");
    }
}
