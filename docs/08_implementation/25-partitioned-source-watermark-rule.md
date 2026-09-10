# Rule: Any Flink job reading a partitioned Fluss table must use the wall-clock idle wrapper

```text
rule_id: PART-SRC-IDLE-001
status: ACTIVE (2026-09-01, CHG-120)
applies_to: every Flink streaming job that reads a Fluss log table with daily (or any) partitions
evidence: CHG-120; live reproduction job e4b4c19f 2026-09-01; fix in SourceIdleWatchdogGenerator
```

Read this page before you write a new Flink source over a partitioned Fluss
table. It explains, in plain language, a bug that cost two days of debugging
on 2026-09-01 and will happen again to any new source that skips the wrapper.

## The problem, in plain language

**What we saw:** the SignalJob consumed 1.4 million ticks and produced zero
candles, zero previews, zero signals. The job looked healthy. The data was
there. Nothing came out.

**Why it happened, step by step:**

1. Fluss tables that are split into daily partitions always contain partitions
   with no data in them. Two are empty future days (tomorrow, the day after),
   and Fluss also re-creates one empty partition for yesterday. These
   partitions stay empty for hours or forever.

2. A Flink job reads each partition piece ("split") separately. An empty
   partition's split has no data, so it has no timestamp to work with. Flink
   treats "no data yet" as "the data might be old", and holds the whole job's
   clock at the very beginning of time.

3. Flink has a built-in escape hatch for this: if a split stays quiet for 15
   seconds, it is marked "idle" and removed from the clock calculation. This
   is the native solution (`withIdleness`). It works fine when the system is
   calm.

4. The bug: **Flink 2.2 pauses that 15-second timer whenever the job is busy
   (backpressured).** Under real load our job was busy ~99% of the time, so
   the timer advanced only ~1% of normal speed. Fifteen seconds of waiting
   became roughly 25 minutes. Our runs only last about 7 minutes. The empty
   splits were never marked idle, the clock never moved, and no window ever
   closed.

5. Result: every run under load produced nothing — silently. No error, no
   warning, no crash. This is the worst kind of failure: it looks like
   success.

**Why older runs passed:** before 2026-08-31 the raw table had no partitions,
so every split carried data and the broken timer never mattered.

## Why the native solution is not enough here

The native `withIdleness` timer is the right idea with one wrong assumption:
it measures quiet time on a clock that stops when the job is busy. That
assumption is reasonable for "this split is slow because the job is slow".
It is wrong for "this split is empty because the partition has no data yet"
— that split is quiet no matter how busy or idle the job is, and the paused
timer turns a 15-second decision into a 25-minute one. Under enough load the
decision simply never happens inside the job's lifetime. We cannot fix
Flink's timer from application code, so we cannot rely on it for partitioned
tables.

## The fix: mark quiet splits idle using the real wall clock

Inside the source operator, `SourceIdleWatchdogGenerator` now keeps its own
timer, measured in real elapsed seconds (wall clock). When a split has
received no records for `SOURCE_IDLE_MS` (15 seconds of true elapsed time),
the wrapper marks the split idle itself. The moment any record arrives, the
split becomes active again automatically.

- The real clock keeps running during backpressure, so the fix works exactly
  when the native timer freezes.
- It adds no new operators to the job graph, so old checkpoints still restore.
- If the fix ever marks a busy-but-slow split idle by mistake, the record that
  arrives late is counted in the existing late-drop counters, so the mistake
  is visible in monitoring instead of silent.

## The rule for new sources

If you add a new Flink streaming source over a partitioned Fluss table:

1. Do NOT use bare `withIdleness(SOURCE_IDLE_MS)` — that is the timer that
   freezes under load.
2. Wrap your watermark generator the way `CandleWatermarkStrategy.of` does:
   the generator goes inside `SourceIdleWatchdogGenerator`, which adds the
   wall-clock idle marking.
3. Keep `SOURCE_IDLE_MS` configurable per job; 15000 is the current default.
   Size it >> the max expected backpressure stall (P2-054): the wall-clock
   marking is unconditional by design (CHG-120), so too small a timeout
   converts backpressure into systematic lateness — watch the G7c late-drop
   counters for the cost.
4. Add a unit test with an injectable clock that proves: quiet split goes
  idle at the threshold, flowing split never goes idle, first record after
  idle reactivates. Copy the shape from `SourceIdleWatchdogGeneratorTest`.

If your source reads a table with NO partitions, the rule does not apply —
splits without data cannot exist there.

## Where the details live

- Fix and full reasoning: `code/02_services/02_compute/src/main/java/com/trading/compute/signaljob/SourceIdleWatchdogGenerator.java` (class javadoc)
- Wiring example: `CandleWatermarkStrategy.of`
- Tests to copy: `SourceIdleWatchdogGeneratorTest`
- Full incident record: `docs/05_deployment/change-records/CHG-120.md` (problem 14)
