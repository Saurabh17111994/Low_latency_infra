package com.trading.common.schema;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Unit tests for KV partial-update conflict / stale rules (COMPAT-FLUSS-004). */
class KvStateUpdateProtocolTest {

  @Test
  void newerVersionApplied() {
    assertThat(KvStateUpdateProtocol.evaluate(1, 2, false))
        .isEqualTo(KvStateUpdateProtocol.Outcome.APPLIED);
  }

  @Test
  void sameVersionSameContentIsDuplicate() {
    assertThat(KvStateUpdateProtocol.evaluate(2, 2, true))
        .isEqualTo(KvStateUpdateProtocol.Outcome.DUPLICATE);
  }

  @Test
  void sameVersionDifferentContentIsConflict() {
    assertThat(KvStateUpdateProtocol.evaluate(2, 2, false))
        .isEqualTo(KvStateUpdateProtocol.Outcome.CONFLICT);
  }

  @Test
  void olderVersionIsStale() {
    // When content matches, older version is STALE (idempotent re-delivery of old event)
    assertThat(KvStateUpdateProtocol.evaluate(2, 1, true))
        .isEqualTo(KvStateUpdateProtocol.Outcome.STALE);
    // When content differs, older version is REGRESSION (move backward unexpectedly)
    assertThat(KvStateUpdateProtocol.evaluate(2, 1, false))
        .isEqualTo(KvStateUpdateProtocol.Outcome.REGRESSION);
  }

  @Test
  void negativeExistingVersionIsUnknown() {
    assertThat(KvStateUpdateProtocol.evaluate(-1, 1, false))
        .isEqualTo(KvStateUpdateProtocol.Outcome.UNKNOWN);
  }

  @Test
  void haltTruthTableMatchesEveryProductionSwitch() {
    // Halting outcomes: divergence signals that must stop the key.
    assertThat(KvStateUpdateProtocol.requiresHalt(KvStateUpdateProtocol.Outcome.CONFLICT)).isTrue();
    assertThat(KvStateUpdateProtocol.requiresHalt(KvStateUpdateProtocol.Outcome.REGRESSION)).isTrue();
    assertThat(KvStateUpdateProtocol.requiresHalt(KvStateUpdateProtocol.Outcome.UNKNOWN)).isTrue();
    // STALE is a non-halting soft reject (L5-1): rejected + quarantined, but a
    // re-delivered old event must not halt the key — every production switch
    // (PositionProjector, PositionProjectionWriter, OrderLifecycleProjector,
    // PositionsObservationOperator) routes it to a soft "stale" result.
    assertThat(KvStateUpdateProtocol.requiresHalt(KvStateUpdateProtocol.Outcome.STALE)).isFalse();
    // Clean outcomes never halt.
    assertThat(KvStateUpdateProtocol.requiresHalt(KvStateUpdateProtocol.Outcome.APPLIED)).isFalse();
    assertThat(KvStateUpdateProtocol.requiresHalt(KvStateUpdateProtocol.Outcome.DUPLICATE)).isFalse();
  }
}
