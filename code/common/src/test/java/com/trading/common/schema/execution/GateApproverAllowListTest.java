package com.trading.common.schema.execution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * P3-142/P3-143: an empty approver allow-list used to mean "any principal may approve", so a
 * missing or misconfigured approver set silently disabled the approval check on the money path.
 * It now fails at construction, and any-principal behaviour must be requested by name.
 */
class GateApproverAllowListTest {

    @Test
    @DisplayName("an empty allow-list is rejected instead of silently accepting any approver")
    void emptyAllowListIsRejected() {
        assertThatThrownBy(() -> new InMemoryGateStateStore(Set.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("authorizedApprovers must not be empty");
    }

    @Test
    @DisplayName("a null allow-list is rejected")
    void nullAllowListIsRejected() {
        assertThatThrownBy(() -> new InMemoryGateStateStore(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    @DisplayName("the any-approver behaviour is still available, but only by name")
    void anyApproverSeamStillWorks() {
        assertThatCode(() -> InMemoryGateStateStore.anyApprover()).doesNotThrowAnyException();
        assertThatCode(() -> new InMemoryGateStateStore(Set.of("ops-1"))).doesNotThrowAnyException();
    }
}
