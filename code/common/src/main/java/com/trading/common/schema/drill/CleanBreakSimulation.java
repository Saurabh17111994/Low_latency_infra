package com.trading.common.schema.drill;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * SCH-25 clean-break drill — the pure-JVM convergence core: proves that
 * reset + full replay of an immutable source log reconverges every target
 * projection to the reference state, and fails closed when the log it
 * replayed cannot be trusted to be that log.
 *
 * <p>Model: events are {@code (table, key, sourceVersion, contentHash)}
 * append-only entries. Each projection is last-write-wins on
 * {@code (table, key)}, decided by {@code (offset, sourceVersion)} rather than
 * by list order, so a reordered, duplicated or backfilled log cannot pick an
 * arbitrary winner and still compare equal to itself.
 *
 * <p>The reference is a from-scratch re-apply of the whole log; the replayed
 * projection is a re-apply from {@code replayFromOffset}. Convergence = every
 * projected row's (version, hash) equals the reference.
 *
 * <p><b>Fail-closed contract.</b> A run is a convergence proof only when the
 * log it replays is the log the reference was taken from, and that can only be
 * established with an independently captured reference. The
 * {@code run(log, tables, offset)} overload derives both sides from the same
 * {@code log} instance, so it cannot detect mutation: it returns
 * {@link RunResult#immutabilityVerified()} {@code == false} and the caller must
 * not read it as proof of immutability. Both paths reject a structurally
 * invalid input (a null log, a null or empty table list, a null event or event
 * field, a negative replay offset) with {@code converged() == false} and the
 * reasons in {@code violations()}.
 *
 * <p>This is the dry-run half of the drill — the operator half (dropping live
 * tables, restarting the Flink job) is the gated {@code clean_break_drill.py}
 * procedure; the semantics it relies on are pinned here.
 */
public final class CleanBreakSimulation {

    private CleanBreakSimulation() {}

    /** One append-only source event. */
    public record SourceEvent(String table, String key, long sourceVersion,
                              String contentHash, long offset) {}

    /** One projected row: last write wins on (table, key). */
    public record ProjectedRow(long sourceVersion, String contentHash) {}

    /**
     * Structured projection identity. A {@code "table|key"} string is not
     * usable here — a table name containing the separator would make one
     * table's rows selectable by another table's prefix.
     */
    private record TableKey(String table, String key) {}

    /** Per-table convergence result. */
    public record TableResult(String table, long sourceRows, long replayedRows,
                              boolean converged) {}

    /**
     * @param converged              every table's replayed projection equals the reference
     * @param immutabilityVerified   whether the reference came from outside the replayed log
     * @param violations             why the run is not a proof (empty when it is)
     */
    public record RunResult(List<TableResult> tables, boolean converged,
                            boolean immutabilityVerified, List<String> violations) {}

    /**
     * Run the simulation with the reference derived from the replayed log
     * itself — a shape check only, which cannot detect log mutation. Prefer
     * the four-argument overload for anything that must prove immutability.
     */
    public static RunResult run(List<SourceEvent> log, List<String> tables,
            long replayFromOffset) {
        return run(log, tables, replayFromOffset, null);
    }

    /**
     * Run the simulation against an externally captured pre-reset reference
     * (the real drill: capture pre-reset state, reset, replay the full log,
     * compare). When {@code preResetReference} is null the reference is a
     * from-scratch re-apply of the whole log, and
     * {@link RunResult#immutabilityVerified()} is false.
     *
     * @param log                the source log (append-only)
     * @param tables             the target tables to reset and replay
     * @param replayFromOffset   inclusive replay start (0 = full replay)
     * @param preResetReference  per-table captured pre-reset projections
     */
    public static RunResult run(List<SourceEvent> log, List<String> tables,
            long replayFromOffset,
            Map<String, Map<String, ProjectedRow>> preResetReference) {
        List<String> violations = validate(log, tables, replayFromOffset, preResetReference);
        if (!violations.isEmpty()) {
            // Fail closed: inputs the drill cannot trust cannot reconverge.
            List<TableResult> results = new ArrayList<>();
            for (String table : tables == null ? List.<String>of() : tables) {
                results.add(new TableResult(table, 0L, 0L, false));
            }
            return new RunResult(List.copyOf(results), false, false, List.copyOf(violations));
        }

        Map<TableKey, ProjectedRow> reference = preResetReference == null
                ? replay(log, 0L, null)
                : flatten(preResetReference);
        // Reset: drop the projections. Replay: re-apply from the replay offset.
        Map<TableKey, ProjectedRow> replayed =
                replay(log, replayFromOffset, new HashSet<>(tables));

        // One pass over the log for both row counts (the earlier version
        // re-streamed the whole log twice per table).
        Map<String, long[]> counts = new HashMap<>();
        for (SourceEvent e : log) {
            long[] c = counts.computeIfAbsent(e.table(), k -> new long[2]);
            c[0]++;
            if (e.offset() >= replayFromOffset) {
                c[1]++;
            }
        }

        List<TableResult> results = new ArrayList<>();
        boolean allConverged = true;
        for (String table : tables) {
            boolean converged = select(reference, table).equals(select(replayed, table));
            allConverged &= converged;
            long[] c = counts.getOrDefault(table, new long[2]);
            results.add(new TableResult(table, c[0], c[1], converged));
        }
        return new RunResult(List.copyOf(results), allConverged,
                preResetReference != null, List.of());
    }

    /**
     * Structural preconditions for a run to mean anything: a null or empty
     * input cannot be a convergence proof, and it must not surface as an
     * exception either — a throw out of a drill reads as "the infrastructure
     * broke", when the truth is "this evidence is not usable". Each problem is
     * reported as a violation and the run fails closed.
     */
    private static List<String> validate(List<SourceEvent> log, List<String> tables,
            long replayFromOffset, Map<String, Map<String, ProjectedRow>> preResetReference) {
        List<String> out = new ArrayList<>();
        if (log == null) {
            out.add("source log is null");
        }
        if (tables == null) {
            out.add("table list is null");
        } else if (tables.isEmpty()) {
            out.add("table list is empty — nothing would be verified");
        } else if (tables.stream().anyMatch(t -> t == null || t.isEmpty())) {
            out.add("table list contains a null or empty table name");
        }
        if (replayFromOffset < 0) {
            out.add("replay offset " + replayFromOffset + " is negative");
        }
        if (preResetReference != null && (preResetReference.containsKey(null)
                || preResetReference.values().stream().anyMatch(Map::isEmpty))) {
            out.add("pre-reset reference contains a null or empty table");
        }

        int index = -1;
        for (SourceEvent e : (log == null ? List.<SourceEvent>of() : log)) {
            index++;
            if (e == null) {
                out.add("event " + index + " is null");
            } else if (e.table() == null || e.key() == null || e.contentHash() == null) {
                out.add("event " + index + " has a null table, key or content hash");
            }
        }
        return out;
    }

    private static Map<TableKey, ProjectedRow> flatten(
            Map<String, Map<String, ProjectedRow>> perTable) {
        Map<TableKey, ProjectedRow> out = new LinkedHashMap<>();
        for (Map.Entry<String, Map<String, ProjectedRow>> t : perTable.entrySet()) {
            for (Map.Entry<String, ProjectedRow> row : t.getValue().entrySet()) {
                out.put(new TableKey(t.getKey(), row.getKey()), row.getValue());
            }
        }
        return out;
    }

    private static Map<TableKey, ProjectedRow> replay(List<SourceEvent> log, long fromOffset,
            Set<String> tables) {
        Map<TableKey, SourceEvent> winners = new LinkedHashMap<>();
        for (SourceEvent e : log) {
            if (e.offset() < fromOffset) {
                continue;
            }
            if (tables != null && !tables.contains(e.table())) {
                continue;
            }
            winners.merge(new TableKey(e.table(), e.key()), e, CleanBreakSimulation::later);
        }
        Map<TableKey, ProjectedRow> out = new LinkedHashMap<>();
        winners.forEach((key, e) -> out.put(key, new ProjectedRow(e.sourceVersion(), e.contentHash())));
        return out;
    }

    /**
     * Last write wins on (offset, sourceVersion) — list position never decides,
     * so an out-of-order or backfilled log cannot silently change the winner.
     * An exact (offset, version) tie keeps the event seen first; identical
     * offset and version with differing content is a contradiction the source
     * log cannot legitimately contain.
     */
    private static SourceEvent later(SourceEvent a, SourceEvent b) {
        if (a.offset() != b.offset()) {
            return b.offset() > a.offset() ? b : a;
        }
        return b.sourceVersion() > a.sourceVersion() ? b : a;
    }

    private static Map<String, ProjectedRow> select(Map<TableKey, ProjectedRow> all, String table) {
        Map<String, ProjectedRow> out = new LinkedHashMap<>();
        for (Map.Entry<TableKey, ProjectedRow> e : all.entrySet()) {
            if (e.getKey().table().equals(table)) {
                out.put(e.getKey().key(), e.getValue());
            }
        }
        return out;
    }
}
