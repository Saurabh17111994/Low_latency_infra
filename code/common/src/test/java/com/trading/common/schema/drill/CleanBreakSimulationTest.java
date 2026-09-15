package com.trading.common.schema.drill;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** SCH-25 clean-break drill, dry-run half: reset + replay reconvergence. */
class CleanBreakSimulationTest {

    private static List<CleanBreakSimulation.SourceEvent> log() {
        List<CleanBreakSimulation.SourceEvent> events = new ArrayList<>();
        long offset = 0;
        // Two tables, interleaved writes, last-write-wins per key.
        events.add(new CleanBreakSimulation.SourceEvent("Signal_Candidates", "k1", 1, "h1", offset++));
        events.add(new CleanBreakSimulation.SourceEvent("Positions", "p1", 1, "h1", offset++));
        events.add(new CleanBreakSimulation.SourceEvent("Signal_Candidates", "k1", 2, "h2", offset++));
        events.add(new CleanBreakSimulation.SourceEvent("Signal_Candidates", "k2", 1, "h1", offset++));
        events.add(new CleanBreakSimulation.SourceEvent("Positions", "p1", 2, "h2", offset++));
        events.add(new CleanBreakSimulation.SourceEvent("Positions", "p2", 1, "h1", offset++));
        events.add(new CleanBreakSimulation.SourceEvent("Signal_Candidates", "k1", 3, "h3", offset++));
        return List.copyOf(events);
    }

    @Test
    void fullReplayReconvergesEveryTable() {
        CleanBreakSimulation.RunResult r = CleanBreakSimulation.run(
                log(), List.of("Signal_Candidates", "Positions"), 0L);

        assertThat(r.converged()).isTrue();
        assertThat(r.tables()).hasSize(2);
        for (CleanBreakSimulation.TableResult t : r.tables()) {
            assertThat(t.converged()).as(t.table()).isTrue();
            assertThat(t.replayedRows()).isEqualTo(t.sourceRows());
        }
    }

    @Test
    void partialReplayDoesNotReconvergeStaleKeys() {
        // Replaying only the tail cannot rebuild keys whose last write precedes
        // the replay offset — the drill mandates FULL replay from offset 0.
        CleanBreakSimulation.RunResult r = CleanBreakSimulation.run(
                log(), List.of("Signal_Candidates", "Positions"), 5L);

        assertThat(r.converged()).isFalse();
        assertThat(r.tables().get(0).converged()).isFalse();
        assertThat(r.tables().get(1).converged()).isFalse();
    }

    @Test
    void mutatedSourceLogDivergesFromPreResetReference() {
        // The immutable-source guarantee is load-bearing: with a PRE-RESET
        // captured reference (the real drill), a tampered log cannot
        // reconverge and the drill fails closed.
        List<CleanBreakSimulation.SourceEvent> original = log();
        List<CleanBreakSimulation.SourceEvent> mutated = new ArrayList<>(original);
        // Tamper the FINAL write of k1 — an earlier overwrite would be
        // superseded by a later legitimate write and stay hidden.
        mutated.set(6, new CleanBreakSimulation.SourceEvent(
                "Signal_Candidates", "k1", 3, "TAMPERED", 6L));

        // Pre-reset reference captured from the genuine log.
        Map<String, Map<String, CleanBreakSimulation.ProjectedRow>> reference = capture(
                original, List.of("Signal_Candidates"));

        CleanBreakSimulation.RunResult r = CleanBreakSimulation.run(
                mutated, List.of("Signal_Candidates"), 0L, reference);

        assertThat(r.converged()).isFalse();
        assertThat(r.tables().get(0).converged()).isFalse();
    }

    @Test
    void resetThenReplayMatchesPreResetReference() {
        // The real drill semantics: capture the pre-reset projection as the
        // reference, reset, replay the full log, and compare — identical when
        // the log is immutable.
        List<CleanBreakSimulation.SourceEvent> log = log();
        Map<String, Map<String, CleanBreakSimulation.ProjectedRow>> reference =
                capture(log, List.of("Positions"));

        CleanBreakSimulation.RunResult afterReplay = CleanBreakSimulation.run(
                log, List.of("Positions"), 0L, reference);

        assertThat(afterReplay.converged()).isTrue();
        assertThat(afterReplay.tables().get(0).converged()).isTrue();
        assertThat(afterReplay.tables().get(0).sourceRows())
                .isEqualTo(afterReplay.tables().get(0).replayedRows());
    }

    // ---- wave 32: the convergence verdict must not depend on list order,
    // table-name shape, or a well-formed input ----

    @Test
    void outOfOrderLogKeepsTheHigherOffsetAsTheWinner() {
        // Append-only means offset order; the list order here is the reverse, so
        // list-position LWW would pick the offset-1 write and report divergence.
        List<CleanBreakSimulation.SourceEvent> log = List.of(
                new CleanBreakSimulation.SourceEvent("Signal_Candidates", "k1", 1, "late", 5L),
                new CleanBreakSimulation.SourceEvent("Signal_Candidates", "k1", 2, "early", 1L));
        Map<String, Map<String, CleanBreakSimulation.ProjectedRow>> reference =
                new java.util.LinkedHashMap<>();
        reference.put("Signal_Candidates", new java.util.LinkedHashMap<>(Map.of(
                "k1", new CleanBreakSimulation.ProjectedRow(1, "late"))));

        CleanBreakSimulation.RunResult r = CleanBreakSimulation.run(
                log, List.of("Signal_Candidates"), 0L, reference);

        assertThat(r.converged()).as("offset 5 is the last write, not list position 1").isTrue();
    }

    @Test
    void equalOffsetsAreBrokenBySourceVersion() {
        List<CleanBreakSimulation.SourceEvent> log = List.of(
                new CleanBreakSimulation.SourceEvent("Signal_Candidates", "k1", 7, "v7", 3L),
                new CleanBreakSimulation.SourceEvent("Signal_Candidates", "k1", 9, "v9", 3L));
        Map<String, Map<String, CleanBreakSimulation.ProjectedRow>> reference =
                new java.util.LinkedHashMap<>();
        reference.put("Signal_Candidates", new java.util.LinkedHashMap<>(Map.of(
                "k1", new CleanBreakSimulation.ProjectedRow(9, "v9"))));

        CleanBreakSimulation.RunResult r = CleanBreakSimulation.run(
                log, List.of("Signal_Candidates"), 0L, reference);

        assertThat(r.converged()).isTrue();
    }

    @Test
    void aTableWhoseNameContainsTheSeparatorDoesNotBreakAnotherTable() {
        // The reference legitimately holds a table literally named "a|b". A
        // prefix match on "a|" would pull its rows into table "a" and report a
        // false divergence for an immutable log.
        List<CleanBreakSimulation.SourceEvent> log = List.of(
                new CleanBreakSimulation.SourceEvent("a", "k1", 1, "h1", 0L),
                new CleanBreakSimulation.SourceEvent("a|b", "k2", 1, "h2", 1L));
        Map<String, Map<String, CleanBreakSimulation.ProjectedRow>> reference =
                new java.util.LinkedHashMap<>();
        reference.put("a", new java.util.LinkedHashMap<>(Map.of(
                "k1", new CleanBreakSimulation.ProjectedRow(1, "h1"))));
        reference.put("a|b", new java.util.LinkedHashMap<>(Map.of(
                "k2", new CleanBreakSimulation.ProjectedRow(1, "h2"))));

        CleanBreakSimulation.RunResult r = CleanBreakSimulation.run(
                log, List.of("a"), 0L, reference);

        assertThat(r.converged()).as("table a holds only table a's rows").isTrue();
        assertThat(r.tables().get(0).sourceRows()).isEqualTo(1L);
    }

    @Test
    void aNullEventFailsClosedInsteadOfThrowing() {
        List<CleanBreakSimulation.SourceEvent> log = new ArrayList<>();
        log.add(new CleanBreakSimulation.SourceEvent("Positions", "p1", 1, "h1", 0L));
        log.add(null);

        CleanBreakSimulation.RunResult r = CleanBreakSimulation.run(
                log, List.of("Positions"), 0L);

        assertThat(r.converged()).as("unusable evidence is a failure, not an exception").isFalse();
    }

    @Test
    void anEventWithANullTableFailsClosedInsteadOfThrowing() {
        List<CleanBreakSimulation.SourceEvent> log = new ArrayList<>();
        log.add(new CleanBreakSimulation.SourceEvent("Positions", "p1", 1, "h1", 0L));
        log.add(new CleanBreakSimulation.SourceEvent(null, "p2", 1, "h2", 1L));

        CleanBreakSimulation.RunResult r = CleanBreakSimulation.run(
                log, List.of("Positions"), 0L);

        assertThat(r.converged()).isFalse();
    }

    @Test
    void anEmptyTableListIsNotAVacuousPass() {
        CleanBreakSimulation.RunResult r = CleanBreakSimulation.run(log(), List.of(), 0L);

        assertThat(r.converged()).as("nothing verified is not everything verified").isFalse();
    }

    @Test
    void aTableAbsentFromTheLogReportsZeroRowsRatherThanThrowing() {
        CleanBreakSimulation.RunResult r = CleanBreakSimulation.run(
                log(), List.of("Signal_Candidates", "Positions", "Absent"), 0L);

        CleanBreakSimulation.TableResult absent = r.tables().get(2);
        assertThat(absent.table()).isEqualTo("Absent");
        assertThat(absent.sourceRows()).isZero();
        assertThat(absent.replayedRows()).isZero();
        assertThat(absent.converged()).isTrue();
        assertThat(r.tables().get(0).sourceRows()).isEqualTo(4L);
        assertThat(r.tables().get(1).sourceRows()).isEqualTo(3L);
    }

    private static Map<String, Map<String, CleanBreakSimulation.ProjectedRow>> capture(
            List<CleanBreakSimulation.SourceEvent> log, List<String> tables) {
        // Reference = from-scratch re-apply of the log, per table.
        Map<String, Map<String, CleanBreakSimulation.ProjectedRow>> out = new java.util.LinkedHashMap<>();
        for (String table : tables) {
            Map<String, CleanBreakSimulation.ProjectedRow> rows =
                    new java.util.LinkedHashMap<>();
            for (CleanBreakSimulation.SourceEvent e : log) {
                if (e.table().equals(table)) {
                    rows.put(e.key(), new CleanBreakSimulation.ProjectedRow(
                            e.sourceVersion(), e.contentHash()));
                }
            }
            out.put(table, rows);
        }
        return out;
    }
}
