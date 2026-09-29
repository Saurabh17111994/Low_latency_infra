package com.trading.mockarrow;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Mock Arrow market-data server — emits fake tick data over a raw TCP socket.
 *
 * All prices in integer paise (₹1 = 100 paise).
 *
 * <p>R-039: this is a plain {@code ServerSocket} server, NOT a WebSocket
 * server — the previous Javadoc advertised ws://8888 which the implementation
 * never provided. It exists to feed the ingestion's stdin bridge and local
 * harnesses, so raw TCP newline-delimited JSON is the correct wire format.
 *
 * <p>R-040: message format is strictly one tick object per line
 * (newline-delimited JSON) — a batch is written as N lines, never as a JSON
 * array, matching what the NDJSON parser consumes.
 *
 * <p>R-071: the scheduler ticks every 10 ms (100 batches/s); the effective
 * rate is {@code batchSize × 100}, where
 * {@code batchSize = ceil(instruments × tickRatePerSec / 100)}. Pass
 * {@code tickRatePerSec} as the TARGET ticks/s across the instrument set.
 */
public class MockArrowServer {
    private static final Logger log = LoggerFactory.getLogger(MockArrowServer.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final long DEFAULT_SEED = 20260801L;

    private final int port;
    private final int tickRatePerSec;

    /**
     * DEC-045 per-instrument wire cap: the mock must never exceed the rate the
     * production profile is validated at. PEAK changes the arrival shape, never
     * the rate.
     */
    static final int PER_INSTRUMENT_CAP = 20;

    /** The configured arrival shape (DEC-045) — PEAK/BASELINE pacing, not a rate. */
    private final SyntheticWorkload.Profile profile;
    private final List<Long> instruments;
    private final SyntheticWorkload workload;
    private final AtomicLong tickCounter = new AtomicLong(0);
    private ServerSocket serverSocket;
    private volatile boolean running = false;
    private ScheduledExecutorService tickScheduler;

    /** H5-3: per-session outbox bound (batches); overflow drops and counts. */
    static final int SESSION_OUTBOX_CAPACITY = 64;

    /** H5-3: a session with no write progress for this long is evicted. */
    static final long SESSION_STALLED_AFTER_MS = 5_000;

    /** H5-3: watchdog cadence — how often stalled sessions are swept. */
    static final long SESSION_SWEEP_INTERVAL_MS = 1_000;

    // Per-instrument price state for realistic walks (in paise)
    private final Map<Long, Long> prices = new ConcurrentHashMap<>();
    private final Map<Long, Long> basePrices = new HashMap<>();

    public MockArrowServer(int port, int tickRatePerSec, Collection<Long> instruments) {
        this(port, tickRatePerSec, instruments, DEFAULT_SEED, SyntheticWorkload.Profile.BASELINE);
    }

    public MockArrowServer(int port, int tickRatePerSec, Collection<Long> instruments, long seed) {
        this(port, tickRatePerSec, instruments, seed, SyntheticWorkload.Profile.BASELINE);
    }

    /**
     * @param profile the arrival shape (DEC-045); the RATE stays capped at
     *     {@link #PER_INSTRUMENT_CAP} — PEAK changes pacing, never the wire cap
     * @throws IllegalArgumentException when {@code tickRatePerSec} is outside
     *     {@code 1..PER_INSTRUMENT_CAP}
     */
    public MockArrowServer(int port, int tickRatePerSec, Collection<Long> instruments,
            long seed, SyntheticWorkload.Profile profile) {
        if (tickRatePerSec < 1 || tickRatePerSec > PER_INSTRUMENT_CAP) {
            throw new IllegalArgumentException("tickRatePerSec must be in 1.."
                    + PER_INSTRUMENT_CAP + " (DEC-045 per-instrument cap; got "
                    + tickRatePerSec + ")");
        }
        this.port = port;
        this.tickRatePerSec = tickRatePerSec;
        this.instruments = List.copyOf(instruments);
        this.profile = profile;
        this.workload = new SyntheticWorkload(new SyntheticWorkload.Config(
                this.instruments, seed, profile, System.currentTimeMillis()));
        SplittableRandom rng = new SplittableRandom(seed);
        for (long inst : instruments) {
            long base = 10000L + rng.nextLong(190001); // 100.00-2000.01 rupees
            basePrices.put(inst, base);
            prices.put(inst, base);
        }
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        log.info("Mock Arrow TCP server started on tcp://0.0.0.0:{} ({} instruments, {} ticks/s, NDJSON)",
                 port, instruments.size(), tickRatePerSec);

        // Accept connections in background
        Thread acceptThread = new Thread(() -> {
            while (running) {
                try {
                    Socket client = serverSocket.accept();
                    log.info("Client connected: {}", client.getRemoteSocketAddress());
                    handleClient(client);
                } catch (IOException e) {
                    if (running) log.error("Accept error", e);
                }
            }
        }, "mock-arrow-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();

        // Schedule tick generation
        long intervalMs = 10;
        tickScheduler = Executors.newSingleThreadScheduledExecutor();
        tickScheduler.scheduleAtFixedRate(this::generateTicks, 0, intervalMs, TimeUnit.MILLISECONDS);
        // H5-3: evict sessions that stopped making write progress; closing the socket
        // unblocks a wedged write so its writer thread can exit.
        tickScheduler.scheduleAtFixedRate(this::sweepStalledSessions,
                SESSION_SWEEP_INTERVAL_MS, SESSION_SWEEP_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    // Connected clients
    private final List<ClientSession> clients = new CopyOnWriteArrayList<>();

    /**
     * H5-3: one session owns a bounded outbox and ONE daemon writer. The retired
     * shared pool had an unbounded queue and no write deadline, so a stalled
     * client grew the heap and kept a pool thread forever.
     */
    private final class ClientSession {
        private final Socket socket;
        private final BufferedWriter writer;
        private final ArrayBlockingQueue<String> outbox =
                new ArrayBlockingQueue<>(SESSION_OUTBOX_CAPACITY);
        private final AtomicLong lastProgressMs = new AtomicLong(System.currentTimeMillis());
        private final AtomicLong droppedBatches = new AtomicLong();
        private final Thread writerThread;
        private volatile boolean writesPaused; // H5-3 test seam only

        ClientSession(Socket socket, BufferedWriter writer) {
            this.socket = socket;
            this.writer = writer;
            this.writerThread = new Thread(this::writeLoop, "mock-arrow-session-writer");
            this.writerThread.setDaemon(true);
            this.writerThread.start();
        }

        /** Queue one batch; false when the bounded outbox is full (dropped, counted). */
        boolean offer(String batch) {
            if (outbox.offer(batch)) {
                return true;
            }
            droppedBatches.incrementAndGet();
            return false;
        }

        long lastProgressMs() {
            return lastProgressMs.get();
        }

        long droppedBatches() {
            return droppedBatches.get();
        }

        void close() {
            try {
                writer.close();
            } catch (IOException ignored) {
            }
            try {
                socket.close();
            } catch (IOException ignored) {
            }
            writerThread.interrupt();
        }

        /** H5-3 test seam: stop this session's writer so the watchdog sees a real stall. */
        void pauseWritesForTest() {
            writesPaused = true;
        }

        private void writeLoop() {
            try {
                while (running) {
                    if (writesPaused) {
                        Thread.sleep(200);
                        continue;
                    }
                    String batch = outbox.poll(1, TimeUnit.SECONDS);
                    if (batch == null) {
                        continue; // no work; the sweep evicts a wedged socket
                    }
                    // R-040: the batch already ends every tick with '\n' — write it
                    // as-is; an extra separator would put a blank line between batches.
                    writer.write(batch);
                    writer.flush();
                    lastProgressMs.set(System.currentTimeMillis());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                log.debug("mock-arrow: session writer stopped ({}): {}",
                        socket.getRemoteSocketAddress(), e.toString());
                close();
                clients.remove(this);
            }
        }
    }

    private void handleClient(Socket client) {
        try {
            var writer = new BufferedWriter(
                new OutputStreamWriter(client.getOutputStream(), StandardCharsets.UTF_8));
            clients.add(new ClientSession(client, writer));
        } catch (IOException e) {
            // R-180: never leak the accepted socket on writer setup failure.
            log.error("Failed to setup client writer — closing socket", e);
            try {
                client.close();
            } catch (IOException ignored) {
            }
        }
    }

    private void generateTicks() {
        if (clients.isEmpty()) return;
        int batchSize = Math.max(1, (int) Math.ceil(instruments.size() * tickRatePerSec / 100.0));
        List<Map<String, Object>> batch = new ArrayList<>(batchSize);

        for (int i = 0; i < batchSize; i++) {
            long inst = workload.next().instrumentToken();
            long pricePaise = walkPrice(inst);
            long now = System.currentTimeMillis();

            Map<String, Object> tick = new LinkedHashMap<>();
            tick.put("instrument_token", inst);
            tick.put("exchange_ts", now);
            tick.put("last_price_paise", pricePaise);
            tick.put("last_qty", 1 + ThreadLocalRandom.current().nextInt(100));
            tick.put("change_pct", round2((pricePaise - basePrices.get(inst)) * 100.0 / basePrices.get(inst)));
            tick.put("volume", 1000L + ThreadLocalRandom.current().nextInt(50000));
            tick.put("buy_quantity", 100 + ThreadLocalRandom.current().nextInt(5000));
            tick.put("sell_quantity", 100 + ThreadLocalRandom.current().nextInt(5000));
            tick.put("ohlc_open_paise", pricePaise - ThreadLocalRandom.current().nextInt(201));
            tick.put("ohlc_high_paise", pricePaise + ThreadLocalRandom.current().nextInt(301));
            tick.put("ohlc_low_paise", pricePaise - ThreadLocalRandom.current().nextInt(301));
            tick.put("ohlc_close_paise", pricePaise);
            tick.put("depth_buy", generateDepth(pricePaise, -1, 5));
            tick.put("depth_sell", generateDepth(pricePaise + 5, 1, 5));
            batch.add(tick);
        }

        // R-040: serialize each tick as its OWN NDJSON line (one object per
        // line) — a JSON array line would not parse as a tick downstream.
        StringBuilder ndjson = new StringBuilder(batch.size() * 160);
        try {
            for (Map<String, Object> tick : batch) {
                ndjson.append(mapper.writeValueAsString(tick)).append('\n');
            }
        } catch (Exception e) {
            log.error("JSON serialization failed", e);
            return;
        }
        String json = ndjson.toString();
        tickCounter.addAndGet(batch.size());

        // H5-3: the scheduler only enqueues into each session's bounded outbox; the
        // session's own daemon writer does the write+flush, so a slow client can
        // never stall tick pacing and its queue can never grow without bound.
        int expected = clients.size();
        for (var session : clients) {
            if (!session.offer(json)) {
                log.debug("mock-arrow: outbox full for {} — batch dropped ({} total)",
                        session.socket.getRemoteSocketAddress(), session.droppedBatches());
            }
        }
        if (expected > 0) {
            log.debug("mock-arrow: enqueued {} deliveries for batch of {}",
                    expected, batch.size());
        }
    }

    /** H5-3: evict sessions with no write progress for {@link #SESSION_STALLED_AFTER_MS}. */
    private void sweepStalledSessions() {
        long now = System.currentTimeMillis();
        for (var session : clients) {
            long idleMs = now - session.lastProgressMs();
            if (idleMs > SESSION_STALLED_AFTER_MS) {
                log.warn("mock-arrow: evicting stalled client {} (no write progress for {} ms)",
                        session.socket.getRemoteSocketAddress(), idleMs);
                session.close();
                clients.remove(session);
            }
        }
    }

    /** Returns price in paise after a random walk step. */
    private long walkPrice(long inst) {
        return prices.compute(inst, (k, v) -> {
            long base = basePrices.get(inst);
            long step = (long) (ThreadLocalRandom.current().nextGaussian() * 50); // ~0.50 rupees std dev
            return Math.max(base / 2, Math.min(base * 2, v + step));
        });
    }

    /** Generate depth levels in paise. direction: -1 = buy (below mid), 1 = sell (above mid) */
    private List<Map<String, Object>> generateDepth(long midPricePaise, int direction, int levels) {
        var depth = new ArrayList<Map<String, Object>>(levels);
        for (int i = 0; i < levels; i++) {
            long offset = 5L * (i + 1); // 0.05 rupees = 5 paise per level
            Map<String, Object> level = new LinkedHashMap<>();
            level.put("price_paise", midPricePaise + direction * offset);
            level.put("qty", 100 + ThreadLocalRandom.current().nextInt(5000));
            depth.add(level);
        }
        return depth;
    }

    private double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }

    public long getTickCount() { return tickCounter.get(); }

    public void stop() {
        running = false;
        if (tickScheduler != null) tickScheduler.shutdownNow();
        // H5-3: close each session (its writer thread exits on the closed socket).
        for (var c : clients) {
            c.close();
        }
        clients.clear();
        try { serverSocket.close(); } catch (IOException ignored) {}
        log.info("Mock Arrow server stopped ({} ticks emitted)", tickCounter.get());
    }

    // --- Main entry point ---
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("MOCK_ARROW_PORT", "8888"));
        String profileName = System.getenv().getOrDefault("MOCK_ARROW_PROFILE", "baseline");
        SyntheticWorkload.Profile profile;
        try {
            profile = SyntheticWorkload.Profile.valueOf(profileName.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown MOCK_ARROW_PROFILE: " + profileName, e);
        }
        long seed = Long.parseLong(requireEnv("MOCK_ARROW_SEED"));
        // DEC-045: the wire rate is capped per instrument; the profile selects the
        // arrival shape only (the retired mapping gave PEAK 30/s, above the cap).
        int rate = PER_INSTRUMENT_CAP;
        int numInstruments = Integer.parseInt(
            System.getenv().getOrDefault("MOCK_ARROW_INSTRUMENTS", "50"));

        List<Long> instruments = new ArrayList<>();
        if (numInstruments <= 0) throw new IllegalArgumentException("MOCK_ARROW_INSTRUMENTS must be positive");
        for (int i = 0; i < numInstruments; i++) {
            instruments.add(100000L + i * 100L);
        }

        var server = new MockArrowServer(port, rate, instruments, seed, profile);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
        while (true) Thread.sleep(1000);
    }

    private static String requireEnv(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(key + " is required");
        }
        return value;
    }

    /** Test/observability seam: the configured per-instrument rate (≤ the cap). */
    int configuredRate() {
        return tickRatePerSec;
    }

    /** Test/observability seam: the configured arrival shape. */
    SyntheticWorkload.Profile configuredProfile() {
        return profile;
    }

    /** Test seam: live session count (H5-3 eviction tests). */
    int sessionCountForTest() {
        return clients.size();
    }

    /**
     * Test seam (H5-3): stop ONE session's writer so the real watchdog sees a
     * genuine stall — a kernel-buffer wedge needs timing that would flake the test.
     */
    void pauseSessionWritesForTest(Socket client) {
        for (var session : clients) {
            if (session.socket.getPort() == client.getLocalPort()) {
                session.pauseWritesForTest();
            }
        }
    }
}
