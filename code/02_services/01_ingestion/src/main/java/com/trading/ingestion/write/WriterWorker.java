// WriterWorker — single writer thread that drains the BoundedQueue and
// submits each packet to RawTickWriter (T5, contract §3.3 Q17/O-1).
//
// The queue is the bounded buffer between the parser and the writer; this
// worker is the ONLY consumer. The Fluss AppendWriter does the actual
// transport batching (batch-timeout=1ms, O-2) — the worker keeps submission
// on one thread so the client's batching is effective (no cross-thread
// contention on the writer's internal buffer).
//
// Design:
//   - Single worker thread (O-1: 1 writer by default; N>1 only if Test D).
//   - Drains in a loop: take() → write() (async append, ack via listener).
//   - Shutdown: close() stops acceptance, drains remaining queue, waits for
//     pending acks via the tracker's drain (drainDeadline).
//   - No silent drop: every accepted (dequeued) packet goes to write();
//     write() returns ACCEPTED/REJECTED/SKIPPED synchronously, terminal
//     outcomes via the listener.

package com.trading.ingestion.write;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.trading.ingestion.model.TickPacket;

public final class WriterWorker implements AutoCloseable {

    private final BoundedQueue queue;
    private final RawTickWriter writer;
    private final Duration drainDeadline;
    private final Thread thread;
    private final CountDownLatch stopped = new CountDownLatch(1);
    private volatile boolean running = true;

    public WriterWorker(BoundedQueue queue, RawTickWriter writer, Duration drainDeadline) {
        this.queue = queue;
        this.writer = writer;
        this.drainDeadline = drainDeadline;
        this.thread = new Thread(this::run, "writer-worker");
        this.thread.setDaemon(false);
    }

    public void start() {
        thread.start();
    }

    private void run() {
        // Drain until closed AND empty (take() returns null). The `running`
        // flag is not checked here: close() must let the worker process
        // everything already queued (T5-W4 shutdown drain).
        while (true) {
            try {
                BoundedQueue.Entry entry = queue.take();
                if (entry == null) {
                    break; // closed + drained
                }
                TickPacket packet = entry.packet();
                int rowBytes = entry.rowBytes();
                // Async append; terminal outcome via writer's OutcomeListener.
                // write() reserves tracker budget and returns ACCEPTED; on
                // REJECTED/SKIPPED the tracker is already released by write().
                writer.write(packet);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable t) {
                // Never let a worker exception kill the drain loop silently.
                // The packet is lost ONLY if write() itself threw before
                // reserving — log loudly; the tracker accounting stays intact.
                java.util.logging.Logger.getLogger("WriterWorker")
                        .severe("writer-worker: uncaught drain error: " + t);
            }
        }
        stopped.countDown();
    }

    /**
     * Stop accepting new items, drain the queue, wait for pending appends
     * (tracker drain), then stop. Blocks up to drainDeadline.
     */
    @Override
    public void close() {
        running = false;
        queue.close();
        try {
            // Drain loop exits after queue.close() + empty
            if (!stopped.await(drainDeadline.toMillis(), TimeUnit.MILLISECONDS)) {
                java.util.logging.Logger.getLogger("WriterWorker")
                        .severe("writer-worker: drain exceeded " + drainDeadline);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        writer.close(); // waits for pending acks (drainDeadline)
    }
}
