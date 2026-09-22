package com.trading.common.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

// Version note (2026-09-23): the 0.9.1 references in this file record the pre-1.0.0 baseline this code was written against, not a constraint of the running Fluss 1.0.0 — re-check them (DEC-052).
/**
 * COMPAT-FLUSS-004 — the "rejected, quarantined, and audited" half of the KV
 * stale/regressive/conflict contract, exercised at the projector layer where
 * the rejection actually lives. The live COMPAT-FLUSS-004 probe observes
 * last-write-wins at the Fluss storage layer (no CAS exists in the raw 0.9.1
 * client surface), so stale-write rejection is a projector responsibility
 * (KvStateUpdateProtocol; SCH-09 / SCH-20 boundary).
 *
 * <p>Drives a minimal in-test projection store through the protocol and
 * asserts the write-side contract:
 * <ul>
 *   <li>a newer write applies and advances the store;</li>
 *   <li>a stale write is REJECTED — the store is unchanged and a stale
 *       version can never resurrect or overwrite newer state;</li>
 *   <li>a regressive write is REJECTED;</li>
 *   <li>a conflict is REJECTED and raises the halt signal;</li>
 *   <li>an idempotent duplicate is a no-op, not a rejection;</li>
 *   <li>every rejection is quarantined and audited (audit-trail completeness).</li>
 * </ul>
 */
class KvStaleWriteRejectionTest {

  /**
   * Minimal projector-layer store: version + content + rejection side-effects.
   * An empty key is a first-write (no prior state to conflict with) — the
   * version gate (KvStateUpdateProtocol) only applies once the key exists,
   * exactly as a production projector decides between "first write" and
   * "update of known state" before consulting the protocol.
   */
  private static final class Store {
    boolean exists = false;
    long version = 0;
    String content = null;
    boolean halted = false;
    final List<String> quarantined = new ArrayList<>();
    final List<String> audit = new ArrayList<>();

    KvStateUpdateProtocol.Outcome apply(long incomingVersion, String incomingContent) {
      if (!exists) {
        // First write to an empty key: nothing to conflict with.
        exists = true;
        version = incomingVersion;
        content = incomingContent;
        audit.add(KvStateUpdateProtocol.Outcome.APPLIED + " v" + incomingVersion);
        return KvStateUpdateProtocol.Outcome.APPLIED;
      }
      boolean contentMatches =
          incomingContent == null ? content == null : incomingContent.equals(content);
      KvStateUpdateProtocol.Outcome outcome =
          KvStateUpdateProtocol.evaluate(version, incomingVersion, contentMatches);
      switch (outcome) {
        case APPLIED:
          version = incomingVersion;
          content = incomingContent;
          break;
        case DUPLICATE:
          break; // no-op — store unchanged, nothing quarantined
        default:
          // STALE / REGRESSION / CONFLICT / UNKNOWN → rejected: the store is
          // unchanged, the attempt is quarantined, and the key halts.
          quarantined.add("v" + incomingVersion + ":" + incomingContent);
          if (KvStateUpdateProtocol.requiresHalt(outcome)) {
            halted = true;
          }
      }
      audit.add(outcome + " v" + incomingVersion);
      return outcome;
    }
  }

  @Test
  void newerWriteAppliesAndAdvances() {
    Store s = new Store();
    assertThat(s.apply(1, "a")).isEqualTo(KvStateUpdateProtocol.Outcome.APPLIED);
    assertThat(s.apply(2, "b")).isEqualTo(KvStateUpdateProtocol.Outcome.APPLIED);
    assertThat(s.version).isEqualTo(2);
    assertThat(s.content).isEqualTo("b");
    assertThat(s.halted).isFalse();
    assertThat(s.quarantined).isEmpty();
  }

  @Test
  void staleWriteIsRejectedAndCannotResurrectNewerState() {
    Store s = new Store();
    s.apply(2, "b");
    // Same content as the stored state but an older version — an idempotent
    // re-delivery of an old event (STALE), not a content change (REGRESSION).
    assertThat(s.apply(1, "b")).isEqualTo(KvStateUpdateProtocol.Outcome.STALE);
    // Rejected: the store must not regress to the older version.
    assertThat(s.version).isEqualTo(2);
    assertThat(s.content).isEqualTo("b");
    assertThat(s.halted).as("stale write must halt the key").isTrue();
    assertThat(s.quarantined).containsExactly("v1:b");
  }

  @Test
  void regressiveWriteIsRejected() {
    Store s = new Store();
    s.apply(2, "b");
    assertThat(s.apply(1, "different"))
        .isEqualTo(KvStateUpdateProtocol.Outcome.REGRESSION);
    assertThat(s.version).isEqualTo(2);
    assertThat(s.content).isEqualTo("b");
    assertThat(s.halted).isTrue();
    assertThat(s.quarantined).containsExactly("v1:different");
  }

  @Test
  void conflictingWriteIsRejectedWithHalt() {
    Store s = new Store();
    s.apply(2, "b");
    assertThat(s.apply(2, "different"))
        .isEqualTo(KvStateUpdateProtocol.Outcome.CONFLICT);
    assertThat(s.version).isEqualTo(2);
    assertThat(s.content).isEqualTo("b");
    assertThat(s.halted).as("conflict must halt the key").isTrue();
    assertThat(s.quarantined).containsExactly("v2:different");
  }

  @Test
  void duplicateIsANoOpNotARejection() {
    Store s = new Store();
    s.apply(2, "b");
    assertThat(s.apply(2, "b")).isEqualTo(KvStateUpdateProtocol.Outcome.DUPLICATE);
    assertThat(s.version).isEqualTo(2);
    assertThat(s.content).isEqualTo("b");
    assertThat(s.halted).isFalse();
    assertThat(s.quarantined).isEmpty();
  }

  @Test
  void unknownVersionHaltsWithoutMutation() {
    Store s = new Store();
    s.apply(2, "b");
    assertThat(s.apply(-1, "x")).isEqualTo(KvStateUpdateProtocol.Outcome.UNKNOWN);
    assertThat(s.version).isEqualTo(2);
    assertThat(s.content).isEqualTo("b");
    assertThat(s.halted).isTrue();
    assertThat(s.quarantined).containsExactly("v-1:x");
  }

  @Test
  void mixedSequenceNeverRegressesAndAuditsEveryAttempt() {
    Store s = new Store();
    s.apply(1, "a");      // APPLIED
    s.apply(2, "b");      // APPLIED
    s.apply(1, "old");    // REGRESSION → rejected
    s.apply(2, "b");      // DUPLICATE → no-op
    s.apply(2, "other");  // CONFLICT → rejected + halt
    s.apply(3, "c");      // APPLIED (still writable after conflict halt)
    assertThat(s.version).isEqualTo(3);
    assertThat(s.content).isEqualTo("c");
    assertThat(s.quarantined).containsExactly("v1:old", "v2:other");
    // Audit-trail completeness: every attempt is recorded, in order.
    assertThat(s.audit).containsExactly(
        "APPLIED v1",
        "APPLIED v2",
        "REGRESSION v1",
        "DUPLICATE v2",
        "CONFLICT v2",
        "APPLIED v3");
  }
}
