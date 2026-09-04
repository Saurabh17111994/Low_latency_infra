# Fix Design — Bounded Data Drain for the Fluss Source (mailbox-gate chop)

Status: DESIGN (no implementation yet)
Date: 2026-09-03
Supersedes diagnosis: `ANALYSIS-diag7.md` (queue-full fetcher blocking = PRIMARY root cause, CONFIRMED)
Branches: Flink sources = `release-2.2.1` tag ONLY (`f4d62a2fb2e`); repo `…/flink`; job code in `…/streaming_project_New`

---

## 0. TL;DR

The diag7 root cause is a **fairness gate** in the Flink source task mailbox loop:

```java
// StreamTask.getCanEmitBatchOfRecords()  (release-2.2.1, unchanged on master)
return () -> !this.mailboxProcessor.hasMail() && taskIsAvailable();
```

`SourceOperator.emitNext()` drains records in a `do { pollNext() } while (MORE_AVAILABLE && canEmitBatchOfRecords.check())` loop. The `!hasMail()` term makes the drain **yield to the mailbox as soon as ANY mail is pending**. Under this workload the mailbox receives a **processing-time watermark-timer mail every ~200 ms** (the job's `withIdleness`/`SourceIdleWatchdog` periodic watermark emit, deferred to the mailbox) plus periodic checkpoint mails — so every drain is chopped to the 27–55 records that fit between two mail arrivals. The chopped drain starves the 2-slot Fluss `FutureCompletingBlockingQueue`, the fetcher blocks in `put()` a median ~2.3 s, and the fetch cadence becomes the throughput ceiling (measured: ~1.4–6.2 k ticks/s vs 10–20 k required).

The gate exists for a real reason: **without yielding, one `emitNext` could monopolize the mailbox thread indefinitely and starve control mails (checkpoints, cancellation, split changes)**. The fix must preserve that guarantee while allowing a **bounded** drain.

**Recommended fix (Option B, narrowly scoped, config-gated, off by default):** replace the *binary* `hasMail()` check in the *source-task drain path only* with a **bounded-drain allowance**: keep draining while `MORE_AVAILABLE && taskIsAvailable()` **and** the mailbox has no **urgent** mail **and** the drain has not exceeded a small **record/byte/time quantum**. Urgent mail (checkpoint barriers, cancellation, stop/savepoint) still interrupts immediately; deferrable mail (periodic watermark timer, async checkpoint completion) waits at most one quantum.

This preserves every Flink correctness property: checkpoint/barrier handling is urgent and preempts immediately; watermark timeliness is bounded by the quantum (≤ a few ms of extra delay on a 200 ms cadence — immaterial); SourceIdleWatchdog keeps ticking (its timer mail is only deferred ≤1 quantum); backpressure is unchanged (`taskIsAvailable()` still gates). The risk is bounded because the change is **per-source-operator, opt-in, and reverts to the stock gate when disabled or when urgent mail is present**.

---

## 1. Current behavior (exact, source-verified)

### 1.1 The mailbox loop (flink-runtime 2.2.1)

Every task (incl. a source task) runs one mailbox thread executing `MailboxProcessor.runMailboxLoop()`:

```java
while (isNextLoopPossible()) {
    processMail(localMailbox, false);          // drain all currently-available mail
    mailboxDefaultAction.runDefaultAction(...); // = StreamTask.processInput -> ONE emitNext
}
```

- `processMail` first consumes a **batch** of mails (`mailbox.createBatch()`), then runs them non-blocking.
- Then the **default action** runs once. For a source task the default action is `StreamTask.processInput` → `inputProcessor.processInput()` → `SourceOperator.emitNext()`.
- Mail that arrives *while* the default action runs is processed on the **next** loop iteration.

### 1.2 SourceOperator.emitNext (the drain)

```java
public DataInputStatus emitNext(DataOutput<OUT> output) throws Exception {
    ...
    if (operatingMode != OperatingMode.READING) return emitNextNotReading(output);
    InputStatus status;
    do {
        status = sourceReader.pollNext(currentMainOutput);
    } while (status == InputStatus.MORE_AVAILABLE
            && canEmitBatchOfRecords.check()
            && !shouldWaitForAlignment());
    return convertToInternalStatus(status);
}
```

`canEmitBatchOfRecords.check()` = `!hasMail() && taskIsAvailable()` (StreamTask:1161).

So one `emitNext` call = a **drain batch**: `pollNext()` repeatedly until either
- the reader says `NOTHING_AVAILABLE` (no more buffered records — fetcher has nothing ready), or
- `taskIsAvailable()` false (downstream backpressured — record writer full), or
- `hasMail()` true (ANY mail pending — **yield to mailbox**), or
- alignment wait.

### 1.3 What "hasMail() true" does to the drain

When the drain stops because `hasMail()` became true, `emitNext` returns `MORE_AVAILABLE` (there is still buffered data). `processInput` sees MORE_AVAILABLE + `taskIsAvailable()` → **returns immediately without suspending**. The mailbox loop then goes around again: `processMail` (runs the pending mail), then `runDefaultAction` → another `emitNext` → another drain attempt. Each drain attempt re-checks `hasMail()` **at the top of every loop iteration** (it's in the `while` condition), so a mail arriving mid-drain cuts the drain at the *next* `pollNext()` boundary — i.e. the drain emits only the records already pulled before the mail was observed.

**Net:** every mail that arrives during a drain chops that drain to a small quantum. The drain does NOT resume draining the backlog in one go; it drains one mail-tick's worth per mailbox round.

---

## 2. Exact reason the current gate exists (source-verified history)

The do-while drain + `!mailboxHasMail` was introduced by **FLINK-30533** (`SourceOperator#emitNext() should push records to DataOutput in a while loop`, Dong Lin, Jan 2023; then refactored to the `CanEmitBatchOfRecordsChecker` in FLINK-30623). The commit message:

> "This optimization is currently disabled for TwoInputStreamTask and MultipleInputStreamTask."

The **intent** (from the code + PR): emitNext should push a *batch* of records per default-action invocation to amortize the per-call overhead — but must **yield control back to the mailbox periodically** so that:
- **checkpoint trigger / barrier** mails are not delayed indefinitely behind a huge drain,
- **cancellation / stop-with-savepoint** mails are handled promptly,
- **split-added / split-removed / watermark-alignment** control events are applied promptly,
- **async checkpoint completion / other operator actions** run.

`hasMail()` is the cheapest possible "is there control work I should yield for?" signal. The gate was never designed to bound the *drain to a specific small size* — it is a **binary fairness switch**: "if any mail is waiting, stop draining and let the loop process mail."

The gate is **unchanged on Flink master** — upstream has not found this to be a problem for typical sources, because typical sources either (a) have a large internal buffer and the reader returns NOTHING_AVAILABLE between fetches (so the drain is naturally short and `hasMail()` rarely fires mid-drain), or (b) do not receive a high rate of periodic timer mail. The Fluss source under this workload is unusual: it **always has records buffered** (the fetcher fills a 2-slot queue as fast as it can) AND the task receives **periodic mailbox mail ~5×/s** (200 ms watermark timer) — so the drain is *always* chopped.

---

## 3. Exact mechanism causing queue-full blocking (the diag7 causal chain, restated precisely)

1. Fluss `SplitFetcher` thread polls the broker → gets up to a batch → calls `elementsQueue.put(fetcherIndex, lastRecords)` on the `FutureCompletingBlockingQueue` (capacity **2**).
2. The source task drains via `pollNext()` → `SourceReaderBase.getNextFetch` → takes from the queue. Under a healthy drain the queue stays near-empty and `put()` never blocks.
3. The mailbox fairness gate chops each drain to 27–55 records (diag7 measured: 331/331 gate-stop exits; median drain quantum ~27–55). Because the task spends much of each ~200 ms window processing watermark-timer mail + re-entering the drain, the **average consumption rate from the 2-slot queue drops** below the fetcher's fill rate.
4. The queue fills to 2/2. The fetcher's next `put()` **blocks** (FCBQ `put` → `waitOnPut`, full-queue wait).
5. While blocked, the fetcher cannot fetch more — the broker→queue pipeline stalls. When the drain finally consumes a slot, the put unblocks and the fetch cadence resumes — but the **block time (median ~2.3 s, max 7.2 s) becomes the effective fetch cadence**.
6. Result: the source emits in bursts (drain quantum → stall ~2.3 s → next fetch) instead of a steady stream → throughput collapses to ~1.4–6.2 k/s; per-tick latency and data age balloon (131–141 s age observed); backlog grows.
7. No records are lost (fetch supply == emitted count) — this is a **throughput/latency** pathology, not a data-loss one.

**The single controllable lever in this chain is step 3**: stop chopping the drain. If the drain runs in bounded batches long enough to keep the 2-slot queue from saturating, the fetcher never blocks and throughput/latency recover. (The 2-slot capacity itself is a secondary amplifier — enlarging it would help, but it is a Fluss-connector constant and does not remove the root chopping.)

---

## 4. Mail taxonomy: what trips `hasMail()` and what may/can wait

I enumerated every mailbox mail source in the source-task path (release-2.2.1) and classified each.

| Mail | Producer | Priority (`MailOptions`) | Frequency under this job | Must interrupt drain? | Can be deferred? |
|---|---|---|---|---|---|
| Checkpoint barrier / trigger | `CheckpointBarrierHandler` / `StreamTask.triggerCheckpointAsync` | **urgent** (FLINK-35051 made unaligned-barrier handling high-priority) | per checkpoint (job default **1 s**, EXACTLY_ONCE) | **YES — correctness** (barrier must be processed in order; unaligned barriers must not be overtaken by records) | NO (unaligned) / ≤1 quantum (aligned, see §6) |
| Checkpoint abort / subsumed / cleanup | coordinator RPC → mailbox | urgent-ish (RPC path) | rare | YES (state bookkeeping) | ≤1 quantum |
| Stop-with-savepoint / cancel | RPC / async | **urgent** | rare | YES | NO |
| Periodic watermark emit (`onPeriodicEmit`) | `ProgressiveTimestampsAndWatermarks` 200 ms timer → `deferCallbackToMailbox` | **default (non-urgent)** | **~5/s** — THE dominant mail | NO (can wait ≤1 quantum) | **YES — safe** |
| `SourceIdleWatchdog`/`WatermarksWithIdleness` progress | same 200 ms `onPeriodicEmit` path (decorator runs inside watermark generator) | default | ~5/s | NO | YES (bounded) |
| Async checkpoint completion / notify | async checkpoint thread → mailbox | default | per checkpoint | NO (result already durable) | YES |
| Split added/removed/paused | `AddSplitEvent`/operator event | default/urgent | rare (source start) | NO (only at source init) | YES |
| Latency marker emit | `LatencyMarkerEmitter` timer | default | if enabled | NO | YES |
| Async operator actions / user timers | user code | default | depends | NO | YES (Flink already treats them as deferrable) |

**Key correctness facts:**
- Flink **already models urgency**: `MailboxExecutor.MailOptions.urgent()` vs `deferrable()` vs default. `TaskMailbox` preserves urgent-mail FIFO and lets urgent mails jump the queue. The **checkpoint barrier path and cancellation use urgent mail**. The **200 ms watermark timer uses default (non-urgent)** mail.
- So a drain that yields to **urgent** mail only — while continuing past **non-urgent** mail for a bounded quantum — preserves every hard guarantee (checkpoint ordering, cancellation, stop/savepoint) and only delays soft work (watermark ticks, idle watchdog, async-completion) by ≤1 quantum.
- There is **no mailbox "batch of records being emitted" concept** in the drain — `pollNext` emits records directly to the `DataOutput`. Records emitted during a drain are not "mail". The only question is how long one `emitNext` may keep the mailbox thread before the loop services mail.

---

## 5. Candidate fix options

### Option A — Remove `!hasMail()` (the naive suggestion)
```java
return () -> taskIsAvailable();   // drain until backpressure or no data
```
- **Correctness risk: UNACCEPTABLE alone.** One `emitNext` would drain the entire buffered backlog (and, if the source reader keeps returning MORE_AVAILABLE, could spin forever) before the mailbox loop ever processes a checkpoint barrier/cancel. With EXACTLY_ONCE + 1 s checkpoints this can: (a) delay checkpoint barriers unboundedly → checkpoint timeouts / job failure under sustained load, (b) make cancellation/stop unresponsive, (c) for unaligned checkpoints, violate the ordering guarantee that barriers not be overtaken by unbounded record emission. This is exactly the "do not propose removing hasMail() unless you can show correctness" caveat. **Rejected as a standalone fix.**

### Option B — Bounded drain with urgent-mail preemption + quantum (RECOMMENDED)
Keep the stock gate but make it yield only to **urgent** mail, plus impose a small **quantum** so even without mail the drain cannot run forever:
```java
// conceptual (per-source-operator override, opt-in)
return () -> taskIsAvailable()
        && !mailboxProcessor.hasUrgentMail()      // urgent mail preempts immediately
        && drainQuantum.notExceeded();            // record-count / byte / time bound
```
- The quantum is the safety net that makes removing the *non-urgent* `hasMail()` safe: even if the source reader returns MORE_AVAILABLE forever (infinite buffered supply), the drain self-terminates after N records / B bytes / T ms and the loop services mail. Choose N ≈ a few thousand records or T ≈ a few ms (tunable) so that under this workload the 2-slot queue never saturates, while under pathological conditions the drain still yields every quantum.
- Preserves: urgent mail (checkpoint/cancel) preempts at the next record boundary (same as today — urgent mail is already ahead of records in the sense that the loop checks mail each iteration); watermark timers are delayed ≤1 quantum.
- **This is the fix that directly targets the diag7 chain**: the drain is no longer chopped by the 200 ms timer mail; it runs in bounded batches that keep the FCBQ from saturating.

### Option C — Increase the Fluss source queue capacity (2 → larger) as a complement
- `FutureCompletingBlockingQueue` capacity 2 is a separate amplifier. Raising it (e.g. 2 → 64–256) gives the drain more buffered slack so short choppy drains don't saturate it.
- **Not sufficient alone** (the root chopping still caps sustained throughput — diag7 showed the drain quantum IS the cadence), but it is **low-risk and complementary** to B. Could be shipped as a config (`source.reader.queue.capacity`-style) in the Fluss/Flink connector or via the source's `SourceReaderContext`/config plumbing. Note: this touches the **Fluss connector** (`flink-connector-fluss` or the vendored copy) — a different jar than the Flink core change in B.

### Option D — Mailbox-level "default action budget" (deeper, upstream-shaped)
- Change `MailboxProcessor.runMailboxLoop` to track how long the default action has run and preempt it after a budget (time-based), independent of mail. This is the "cleanest" general mechanism (a true fair scheduler) but touches the **core mailbox loop** used by every task type — the highest blast radius, needs upstream-level design/testing, and risks subtle regressions in non-source tasks. **Not recommended as a first fix**; could be a follow-up if B proves insufficient.

### Option E — Suppress/rate-limit the 200 ms watermark timer
- The *trigger* of the chop is the ~5/s watermark-timer mail. If the job did not emit periodic watermarks so aggressively (or the timer ran on a non-mailbox scheduler), the chop would vanish. But: (a) the timer cadence is `auto-watermark-interval`, which the job's `SourceIdleWatchdog`/`withIdleness` design depends on (it ticks on `onPeriodicEmit`); (b) reducing it would slow idle detection; (c) it does not fix the *general* case (checkpoints at 1 s also chop). **Not the primary fix**, but a **legit secondary lever** (raise auto-watermark-interval if the idle watchdog tolerates it).

---

## 6. Correctness analysis per option (deep dive on the recommended B)

### 6.1 Checkpoint/barrier correctness (item 6 of the brief)
- **Aligned EXACTLY_ONCE (this job's mode):** barriers are carried *in-band* as records? No — for a **source operator** (FLIP-27), the source task has **no network inputs**; checkpoint barriers do not arrive as a record stream. Checkpointing is driven by `triggerCheckpointAsync` (RPC from the coordinator → urgent mailbox mail) → the operator snapshots state. Records emitted by the source during the snapshot are fine (the source is the *origin*; its checkpoint is a state snapshot of read offsets + in-flight splits, and the barrier/alignment concern applies *downstream* of the source, not at the source). Delaying the *trigger* mail by ≤1 quantum delays the snapshot start by ≤ quantum — no correctness break, only a tiny checkpoint-trigger latency increase.
- **Unaligned barriers / source splits:** `shouldWaitForAlignment()` is a separate condition already in the loop; we do not touch it. If unaligned checkpoints were enabled with source splits (`ALLOW_UNALIGNED_SOURCE_SPLITS`), barrier handling is urgent mail — Option B still preempts on urgent mail. Preserved.
- **Conclusion:** B preserves checkpoint correctness. The only effect is that a checkpoint trigger may be delayed by ≤1 drain quantum (ms-scale), which is immaterial vs the 1 s checkpoint interval.

### 6.2 Watermark timeliness (item 7)
- The 200 ms periodic watermark emit is deferred ≤1 quantum (≈ a few ms of extra delay worst case) when a drain is mid-flight at the moment the timer fires. On a 200 ms cadence this is ≤ ~2% added watermark latency — does not move p50/p95/p99 event-time semantics materially. Idle detection (`withIdleness(15000)`) is unaffected (its threshold is 15 s).
- Actually watermark *freshness improves*: the current chop causes the source to emit in bursts with multi-second gaps (the 2.3 s block), so watermarks (which advance on emitted records) also stall in bursts. Fixing the drain makes watermark advance **steadier**, reducing event-time staleness — a net win.

### 6.3 SourceIdleWatchdog (item 8)
- The project's `SourceIdleWatchdogGenerator` **decorates the watermark generator** inside the source operator and relies on `onPeriodicEmit` firing every 200 ms regardless of data. That `onPeriodicEmit` runs on the **processing-time timer → mailbox mail**. Under Option B this mail is **non-urgent → deferred ≤1 quantum** — the watchdog tick is delayed by at most a few ms per 200 ms, and **never starved** (the quantum guarantees the loop returns to mail). Idle thresholds are 15 s / 60 s — many orders of magnitude above a few ms. **No behavioral change.**
- If the drain were unbounded (Option A), the watchdog timer mail could be starved during a long drain → idle detection would lag → **A would regress the watchdog**. B does not.

### 6.4 Backpressure / availability (item 9)
- `taskIsAvailable()` = `recordWriter.isAvailable() && changelogAvailable()` remains **in the gate** (both A and B keep it). So when downstream backpressures (record writer full), the drain stops at the next record boundary regardless of mail/quantum — backpressure propagates to the source exactly as today. The FCBQ fills, the fetcher blocks on `put()` — but now that is *correct* backpressure (downstream slow), not the artificial mailbox chop. No change to availability futures or the `ResumeWrapper`/suspend logic in `processInput` (that path is untouched: when NOTHING_AVAILABLE or !available, `processInput` still suspends the default action and resumes on the availability future).
- One subtlety: with a *larger* effective drain, the source pushes more records per emitNext into the record writer. If the writer is near-full, `taskIsAvailable()` flips false mid-drain and we stop — same as today. The record writer's own buffers absorb bursts. No new backpressure hazard.

### 6.5 Fairness / starvation (the reason the gate exists)
- The **quantum** is the guarantee that no single drain monopolizes the mailbox thread. Choose it so that even a source reader that returns `MORE_AVAILABLE` indefinitely (e.g. an infinitely buffered or CPU-spinning reader) yields every N records or T ms. This is strictly stronger than today's guarantee in the *no-mail* case: today, if NO mail ever arrives, a source that always returns MORE_AVAILABLE already drains forever (the gate's `!hasMail()` is vacuous when the mailbox is empty). So B does not weaken the no-mail case at all — it only changes the *with-non-urgent-mail* case, and adds a quantum that today does not exist.
- **Moral equivalence:** today the loop is "drain until mail or no-data-or-backpressure". B is "drain until **urgent** mail or quantum or no-data-or-backpressure". The only behavior removed is yielding to *non-urgent* mail mid-quantum — exactly the mail that Flink itself marks deferrable.

### 6.6 Two-input / multiple-input tasks
- The drain optimization is a **source (single-input) task** concern. We scope B to `SourceOperator`'s checker (the source task). `TwoInputStreamTask`/`MultipleInputStreamTask` already disable the batch loop (FLINK-30533) — untouched.

---

## 7. Recommended fix (detail)

**Scope:** a config-gated override of the *source-task* `CanEmitBatchOfRecordsChecker` only. Stock behavior when disabled (default), so no risk to other jobs/sources until opted in.

**Where:**
1. `StreamTask.getCanEmitBatchOfRecords()` — keep the stock impl for all tasks.
2. Add a **source-operator-level** checker that composes:
   - `taskIsAvailable()` (unchanged),
   - `!mailboxProcessor.hasUrgentMail()` (new — yields only to urgent mail; need a `hasUrgentMail()` accessor on `TaskMailbox`/`MailboxProcessor` — small, safe addition; urgent mail is already tracked via `hasNewUrgentMail`),
   - a **drain quantum** (records or bytes or time; reset each `emitNext` entry).
3. Gate by a config option, e.g. `pipeline.source.bounded-drain.enabled` + `pipeline.source.bounded-drain.max-records` / `max-ms` (defaults: disabled; max-records ≈ 4096, max-ms ≈ 5 ms — to be tuned by benchmark). Wire it through `SourceOperatorFactory`/`SourceOperator` (they already receive config).

**Why not a Flink core mailbox change first:** the mailbox loop is shared by all task types; changing it is high-blast-radius and upstream-shaped. The source-operator checker is the minimal, correct, reversible place — the drain policy is inherently a *source* policy (it is the source that buffers aggressively and drives the mailbox rate).

**Estimated change surface:** ~1 new accessor (`TaskMailbox.hasUrgentMail`), ~1 new config path (3 keys), ~30–50 lines in `SourceOperator`/`SourceOperatorFactory`/`StreamTask` to build the composed checker. All behind the opt-in flag.

**Complement (separate, optional):** raise the Fluss source queue capacity 2 → (config, default maybe 64) in the connector. Independent of B; helps absorb residual burstiness. Touches the Fluss connector jar, not Flink core.

---

## 8. Expected effect (quantitative targets)

Baseline (diag7, 2.2.1, stock gate): source ~1.4–6.2 k ticks/s (test feed 10.24 k/s demand; real peak 20.48 k/s); fetch-cadence = put-block time (median 2.3 s); age 131–141 s; backlog persistent; 331/331 gate-stop drains (27–55 rec).

With B (bounded drain, no chop):

| Metric | Baseline (stock) | Target (with B) | Mechanism |
|---|---|---|---|
| Sustained throughput | ~1.4–6.2 k/s | **10–20 k/s** (feed-limited) | drain no longer chopped by 200 ms timer mail; FCBQ never saturates; fetcher never blocks |
| Drain quantum | 27–55 rec (mail-chopped) | up to quantum (4096 rec / 5 ms) | quantum replaces mail-chop |
| Fetcher put-block | median 2.3 s, max 7.2 s | **~0** (queue stays < 2) | consumption ≥ fill rate |
| p50 tick latency | degraded (bursty) | **low & stable** (< feed interval × few) | steady stream, no multi-second gaps |
| p95/p99 tick latency | tail blown by block gaps | **tight** (no 2.3 s stalls) | no queue-full blocking |
| Data age | 131–141 s | **low** (seconds, bounded by drain latency) | steady advance |
| Backlog | persistent (queue-full drops) | **near-zero** | consumption ≥ arrival |
| Loss / dup | 0 / 0 (preserved) | **0 / 0** | no record-path change; barrier/checkpoint untouched |

Caveats: (a) these are targets to *measure*, not guarantees — the controlled benchmark in §9 is the arbiter; (b) if the true ceiling is elsewhere (e.g. Fluss client fetch batch size, broker feed, record-writer serialization), B will lift throughput only until that next bottleneck — the benchmark will reveal it; (c) watermark/idle behavior improves but is not the goal.

---

## 9. Controlled benchmark plan

**Precondition:** start from a known-good state — ingestion healthy on the live table (the fail-fast fix is deployed; restart ingestion after any table purge per the operating rule).

### 9.1 Environment (fixed across runs)
- Real broker feed at `RATE_HZ=10` (10.24 k ticks/s demand) — the diag7 profile. Optionally a second profile at `RATE_HZ=20` (20.48 k = real peak) to test headroom.
- 1 TM / 16 slots; the Signal job (or the feature-table job under test) reading the Fluss source; other containers idle.
- Fresh TM per run (B3 guard: `restarting flink-taskmanager`).
- Flink `release-2.2.1` build with the **only** change being the B checker behind the opt-in flag; two jars: stock and B-enabled.
- Metrics: source throughput (records/s), tick p50/p95/p99 latency (from the job's latency sink or event-time lag), data age (max event-time lag at the sink), backlog (source queue occupancy / pending), fetch put-block time (the diag7 FCBQ probe markers, if re-instrumented), loss/dup (end-to-end count reconciliation).

### 9.2 Run matrix (one lever per run; each 5–10 min steady-state after 2 min ramp)
| Run | Jar | bounded-drain flag | Queue cap | Purpose |
|---|---|---|---|---|
| R0 (baseline) | stock | off | 2 | reproduce diag7 numbers on the current stack (sanity; must match ~1.4–6.2 k/s) |
| R1 | B | on, quantum=4096/5ms | 2 | **primary**: does removing the non-urgent chop recover throughput? |
| R2 | B | on, quantum sweep (1k / 4k / 16k records; 2/5/10 ms) | 2 | find the quantum knee (throughput vs watermark/checkpoint delay) |
| R3 | stock | off | 64 (cap raised) | isolate the queue-cap amplifier alone |
| R4 | B + cap | on (best quantum) | 64 | combined effect |
| R5 | B | on, RATE_HZ=20 | best | headroom to real peak |
| R6 (correctness) | B (best) | on | best | EXACTLY_ONCE checkpoint at 1 s: assert no checkpoint timeout, no barrier reorder, loss=0 dup=0, clean cancel/stop |

### 9.3 Correctness gates (every run)
- **Loss/dup:** end-to-end count reconciliation == feed count (no silent drop, no duplicate).
- **Checkpoint health:** all checkpoints complete within interval (no `CheckpointException`/timeout); trigger latency (mailbox delay) added by the quantum measured (expect ≤ quantum).
- **Watermark progress:** watermarks advance monotonically; idle watchdog does NOT fire spuriously during a busy run (15 s idle threshold respected).
- **Backpressure:** with a deliberately slow sink (run R7, not in matrix: cap the record writer), the source must backpressure cleanly — FCBQ fills and the fetcher blocks (correct), no OOM, no unbounded buffering. Confirms B did not break the availability path.

### 9.4 Accept / reject
**Accept** if R1/R2 show ≥ 10 k/s sustained, p50/p95/p99 latency low & stable, age in seconds, backlog near-zero, loss=0 dup=0, checkpoints clean, watchdog quiet — across R2's best quantum and R6's correctness run.
**Reject / iterate** if: throughput does not move (→ next bottleneck is elsewhere; investigate fetcher batch size / client); OR correctness gates fail (→ B is unsound, fall back); OR watermark/idle regress.

---

## 10. Explicit non-goals / deferrals
- No change to the **mailbox loop** itself (Option D deferred).
- No change to **checkpoint/barrier handling** (urgent path preserved).
- No change to **record-path serialization** or the Fluss client beyond the optional queue-cap lever (Option C).
- No production rollout until §9 passes.
- The ingestion-side fail-fast fix (separate, already landed) is unrelated to this source-task design.

---

## Appendix: source refs (release-2.2.1)
- `StreamTask.java:1160-1162` — `getCanEmitBatchOfRecords()` = `() -> !hasMail() && taskIsAvailable()`
- `SourceOperator.java:505-511` — `emitNext` do-while with `canEmitBatchOfRecords.check()`
- `MailboxProcessor.java` — `runMailboxLoop`, `processMail`, `suspendDefaultAction`, `DefaultActionSuspension`, `MailboxController`
- `TaskMailbox(Impl).java` — priority/urgent batch model, `createBatch`, `hasNewUrgentMail`
- `MailOptionsImpl.java` — `URGENT`/`DEFERRABLE`/`DEFAULT`
- `ProgressiveTimestampsAndWatermarks.java` — 200 ms periodic `onPeriodicEmit` timer
- `StreamTask.getProcessingTimeServiceFactory` → `deferCallbackToMailbox` — timers run as mailbox mail
- Origin: FLINK-30533 (fb2722cdebf) + FLINK-30623 (2d1510a9d5) — batch drain + checker; unchanged on master
- Project: `SourceIdleWatchdogGenerator` (onPeriodicEmit decorator), `CandleWatermarkStrategy.withIdleness(SOURCE_IDLE_MS=15000)`, `SignalJob` EXACTLY_ONCE 1 s checkpoints
