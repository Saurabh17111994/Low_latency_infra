package com.trading.common.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** Unit tests for immutable-record duplicate / mutation detection. */
class ImmutabilityProtocolTest {

  @Test
  void identicalContentIsDuplicate() {
    String h = ImmutabilityProtocol.canonicalHash("abc");
    assertThat(ImmutabilityProtocol.evaluate(h, h)).isEqualTo(ImmutabilityProtocol.Outcome.DUPLICATE);
  }

  @Test
  void changedContentIsViolation() {
    assertThat(ImmutabilityProtocol.evaluate("h1", "h2"))
        .isEqualTo(ImmutabilityProtocol.Outcome.VIOLATION);
  }

  @Test
  void firstWriteIsAccepted() {
    assertThat(ImmutabilityProtocol.evaluate(null, "h")).isEqualTo(ImmutabilityProtocol.Outcome.ACCEPTED);
  }

  @Test
  void canonicalHashIsStableSha256() {
    String a = ImmutabilityProtocol.canonicalHash("x");
    String b = ImmutabilityProtocol.canonicalHash("x");
    assertThat(a).isEqualTo(b).hasSize(64); // SHA-256 hex
  }

  @Test
  void nullIncomingHashFailsFast() {
    assertThatThrownBy(() -> ImmutabilityProtocol.evaluate("h1", null))
        .isInstanceOf(NullPointerException.class).hasMessageContaining("incomingHash");
    assertThatThrownBy(() -> ImmutabilityProtocol.evaluate(null, null))
        .isInstanceOf(NullPointerException.class).hasMessageContaining("incomingHash");
  }

  @Test
  void nullContentFailsFast() {
    assertThatThrownBy(() -> ImmutabilityProtocol.canonicalHash((String) null))
        .isInstanceOf(NullPointerException.class).hasMessageContaining("canonicalContent");
    assertThatThrownBy(() -> ImmutabilityProtocol.canonicalHash((byte[]) null))
        .isInstanceOf(NullPointerException.class).hasMessageContaining("canonicalContent");
  }
}
