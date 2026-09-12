package com.trading.execution.gateway;

import com.trading.common.schema.fluss.BoundedRetry;

import java.time.Duration;
import java.util.Map;

/** Strict, private-only configuration for the Fluss execution gateway. */
public record GatewayConfig(
        String flussBootstrap,
        String flussDatabase,
        String intentTable,
        String gateTable,
        String attemptsTable,
        String correlationTable,
        String ledgerTable,
        String haltTable,
        String bindHost,
        int bindPort,
        String nautilusEndpoint,
        String protocolVersion,
        String sharedSecret,
        Duration requestTimeout,
        Duration pollTimeout,
        String accountScopeId,
        String executionPartitionId,
        boolean executionEnabled,
        int maxPendingProjectionRecords,
        Duration requestBudget) {

    public GatewayConfig {
        require(flussBootstrap, "FLUSS_BOOTSTRAP");
        require(flussDatabase, "FLUSS_DATABASE");
        require(intentTable, "EXECUTION_INTENT_TABLE");
        require(gateTable, "EXECUTION_GATE_TABLE");
        require(attemptsTable, "EXECUTION_ATTEMPTS_TABLE");
        require(correlationTable, "ORDER_CORRELATION_TABLE");
        require(ledgerTable, "PROJECTION_LEDGER_TABLE");
        require(haltTable, "SAFETY_HALT_TABLE");
        require(bindHost, "GATEWAY_BIND_HOST");
        require(nautilusEndpoint, "NAUTILUS_PRIVATE_ENDPOINT");
        require(protocolVersion, "GATEWAY_PROTOCOL_VERSION");
        require(sharedSecret, "GATEWAY_SHARED_SECRET");
        require(accountScopeId, "ACCOUNT_SCOPE_ID");
        require(executionPartitionId, "EXECUTION_PARTITION_ID");
        if (bindPort < 0 || bindPort > 65535) throw new IllegalArgumentException("invalid bind port");
        if (requestTimeout.isZero() || requestTimeout.isNegative()
                || pollTimeout.isZero() || pollTimeout.isNegative()) {
            throw new IllegalArgumentException("timeouts must be positive");
        }
        // executionEnabled is a fail-closed flag; no extra validation beyond boolean parsing
        if (maxPendingProjectionRecords <= 0) throw new IllegalArgumentException(
                "MAX_PENDING_PROJECTION_RECORDS must be positive");
        if (requestBudget.isZero() || requestBudget.isNegative()) {
            throw new IllegalArgumentException("GATEWAY_REQUEST_BUDGET_MS must be positive");
        }
        // C2: the request budget must fit at least one attempt plus the backoff that follows it.
        // Below that it cannot retry anything, and truncated to zero it would shed every call
        // after a single attempt with no backoff applied at all - the same "the wait never
        // actually happened" shape as P3-023. Refuse it at startup, naming both numbers.
        Duration minimumBudget = requestTimeout.plusMillis(BoundedRetry.BACKOFF_MILLIS);
        if (requestBudget.compareTo(minimumBudget) < 0) {
            throw new IllegalArgumentException(
                    "GATEWAY_REQUEST_BUDGET_MS (" + requestBudget.toMillis()
                            + "ms) must be at least GATEWAY_REQUEST_TIMEOUT_MS plus backoff ("
                            + minimumBudget.toMillis() + "ms)");
        }
    }

    /**
     * The default request budget: one site's full retry budget, granted to the whole request
     * instead of to each of the eleven Fluss calls inside it (C2). Derived from the configured
     * timeout rather than hardcoded so that changing {@code GATEWAY_REQUEST_TIMEOUT_MS} cannot
     * leave the request budget behind, and referenced from {@link BoundedRetry} so the two
     * arithmetic definitions cannot drift apart.
     */
    public static Duration defaultRequestBudget(Duration requestTimeout) {
        return Duration.ofMillis(BoundedRetry.ATTEMPTS * requestTimeout.toMillis()
                + (BoundedRetry.ATTEMPTS - 1) * BoundedRetry.BACKOFF_MILLIS);
    }

    /** Dossier staleness bound default (WP-2): flood beyond this flips readiness false. */
    public static final int DEFAULT_MAX_PENDING_PROJECTION_RECORDS = 1000;

    /**
     * Legacy 18-arg constructor: defaults MAX_PENDING_PROJECTION_RECORDS to the
     * dossier default. New code should pass it explicitly or use fromEnvironment.
     */
    public GatewayConfig(
            String flussBootstrap, String flussDatabase, String intentTable, String gateTable,
            String attemptsTable, String correlationTable, String ledgerTable, String haltTable,
            String bindHost, int bindPort, String nautilusEndpoint, String protocolVersion,
            String sharedSecret, Duration requestTimeout, Duration pollTimeout,
            String accountScopeId, String executionPartitionId, boolean executionEnabled) {
        this(flussBootstrap, flussDatabase, intentTable, gateTable, attemptsTable,
                correlationTable, ledgerTable, haltTable, bindHost, bindPort, nautilusEndpoint,
                protocolVersion, sharedSecret, requestTimeout, pollTimeout, accountScopeId,
                executionPartitionId, executionEnabled, DEFAULT_MAX_PENDING_PROJECTION_RECORDS,
                defaultRequestBudget(requestTimeout));
    }

    /**
     * Legacy 17-arg constructor for existing call sites (defaults executionEnabled to false).
     * New code should use the canonical 18-arg record constructor with explicit executionEnabled.
     */
    public GatewayConfig(
            String flussBootstrap,
            String flussDatabase,
            String intentTable,
            String gateTable,
            String attemptsTable,
            String correlationTable,
            String ledgerTable,
            String haltTable,
            String bindHost,
            int bindPort,
            String nautilusEndpoint,
            String protocolVersion,
            String sharedSecret,
            Duration requestTimeout,
            Duration pollTimeout,
            String accountScopeId,
            String executionPartitionId) {
        this(
                flussBootstrap,
                flussDatabase,
                intentTable,
                gateTable,
                attemptsTable,
                correlationTable,
                ledgerTable,
                haltTable,
                bindHost,
                bindPort,
                nautilusEndpoint,
                protocolVersion,
                sharedSecret,
                requestTimeout,
                pollTimeout,
                accountScopeId,
                executionPartitionId,
                false);
    }

    public static GatewayConfig fromEnvironment() {
        return from(Map.ofEntries(
                Map.entry("FLUSS_BOOTSTRAP", env("FLUSS_BOOTSTRAP", "localhost:9123")),
                Map.entry("FLUSS_DATABASE", env("FLUSS_DATABASE", "default")),
                Map.entry("EXECUTION_INTENT_TABLE", env("EXECUTION_INTENT_TABLE", "Execution_Intent")),
                Map.entry("EXECUTION_GATE_TABLE", env("EXECUTION_GATE_TABLE", "Execution_Gate")),
                Map.entry("EXECUTION_ATTEMPTS_TABLE", env("EXECUTION_ATTEMPTS_TABLE", "Execution_Attempts")),
                Map.entry("ORDER_CORRELATION_TABLE", env("ORDER_CORRELATION_TABLE", "Order_Correlation")),
                Map.entry("PROJECTION_LEDGER_TABLE", env("PROJECTION_LEDGER_TABLE", "Postback_Projection_Ledger")),
                Map.entry("SAFETY_HALT_TABLE", env("SAFETY_HALT_TABLE", "Safety_Halt_Requests")),
                Map.entry("GATEWAY_BIND_HOST", env("GATEWAY_BIND_HOST", "127.0.0.1")),
                Map.entry("GATEWAY_BIND_PORT", env("GATEWAY_BIND_PORT", "9180")),
                Map.entry("NAUTILUS_PRIVATE_ENDPOINT", env("NAUTILUS_PRIVATE_ENDPOINT", "http://127.0.0.1:9190/v1/intents")),
                Map.entry("GATEWAY_PROTOCOL_VERSION", env("GATEWAY_PROTOCOL_VERSION", "execution-gateway.v2")),
                Map.entry("GATEWAY_SHARED_SECRET", requiredEnv("GATEWAY_SHARED_SECRET")),
                Map.entry("GATEWAY_REQUEST_TIMEOUT_MS", env("GATEWAY_REQUEST_TIMEOUT_MS", "2000")),
                // Blank (the default) means "derive from GATEWAY_REQUEST_TIMEOUT_MS" - see
                // defaultRequestBudget. Listed here so the whole env surface stays in one place.
                Map.entry("GATEWAY_REQUEST_BUDGET_MS", env("GATEWAY_REQUEST_BUDGET_MS", "")),
                Map.entry("GATEWAY_POLL_TIMEOUT_MS", env("GATEWAY_POLL_TIMEOUT_MS", "250")),
                Map.entry("ACCOUNT_SCOPE_ID", requiredEnv("ACCOUNT_SCOPE_ID")),
                Map.entry("EXECUTION_PARTITION_ID", requiredEnv("EXECUTION_PARTITION_ID")),
                Map.entry("EXECUTION_ENABLED", env("EXECUTION_ENABLED", "false")),
                Map.entry("MAX_PENDING_PROJECTION_RECORDS",
                        env("MAX_PENDING_PROJECTION_RECORDS", "1000"))));
    }

    static GatewayConfig from(Map<String, String> e) {
        Duration requestTimeout = Duration.ofMillis(longValue(e, "GATEWAY_REQUEST_TIMEOUT_MS"));
        String budgetValue = e.get("GATEWAY_REQUEST_BUDGET_MS");
        Duration requestBudget = budgetValue == null || budgetValue.isBlank()
                ? defaultRequestBudget(requestTimeout)
                : Duration.ofMillis(longValue(e, "GATEWAY_REQUEST_BUDGET_MS"));
        return new GatewayConfig(
                e.get("FLUSS_BOOTSTRAP"), e.get("FLUSS_DATABASE"), e.get("EXECUTION_INTENT_TABLE"),
                e.get("EXECUTION_GATE_TABLE"), e.get("EXECUTION_ATTEMPTS_TABLE"),
                e.get("ORDER_CORRELATION_TABLE"), e.get("PROJECTION_LEDGER_TABLE"), e.get("SAFETY_HALT_TABLE"),
                e.get("GATEWAY_BIND_HOST"), integer(e, "GATEWAY_BIND_PORT"), e.get("NAUTILUS_PRIVATE_ENDPOINT"),
                e.get("GATEWAY_PROTOCOL_VERSION"),
                e.get("GATEWAY_SHARED_SECRET"), requestTimeout,
                Duration.ofMillis(longValue(e, "GATEWAY_POLL_TIMEOUT_MS")),
                e.get("ACCOUNT_SCOPE_ID"), e.get("EXECUTION_PARTITION_ID"),
                parseExecutionEnabled(e.get("EXECUTION_ENABLED")),
                e.get("MAX_PENDING_PROJECTION_RECORDS") == null
                        ? DEFAULT_MAX_PENDING_PROJECTION_RECORDS
                        : integer(e, "MAX_PENDING_PROJECTION_RECORDS"),
                requestBudget);
    }

    private static boolean parseExecutionEnabled(String value) {
        if (value == null || value.isBlank()) return false;
        return Boolean.parseBoolean(value.trim());
    }

    /**
     * The generation this gateway SIGNS with — the first of the accepted set.
     *
     * <p>P3-079: {@link #protocolVersion()} may name several accepted generations
     * ({@code "execution-gateway.v1,execution-gateway.v2"}) so a mixed fleet can migrate without a
     * flag day, but that list is a <b>verification</b> setting. Stamping it into an outbound
     * envelope would send a peer the literal {@code "execution-gateway.v1,execution-gateway.v2"}
     * as its version — a string no peer accepts and no canonical form matches. Emitting the first
     * entry instead keeps a multi-value config usable on both paths.
     */
    public String emittedProtocolVersion() {
        String configured = protocolVersion();
        if (configured == null) return null;
        int comma = configured.indexOf(',');
        return comma < 0 ? configured.trim() : configured.substring(0, comma).trim();
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static String requiredEnv(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException(key + " is required");
        return value;
    }

    private static int integer(Map<String, String> e, String key) {
        try { return Integer.parseInt(e.get(key)); }
        catch (Exception ex) { throw new IllegalArgumentException(key + " must be an integer", ex); }
    }

    private static long longValue(Map<String, String> e, String key) {
        try { return Long.parseLong(e.get(key)); }
        catch (Exception ex) { throw new IllegalArgumentException(key + " must be an integer", ex); }
    }

    private static void require(String value, String name) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(name + " is required");
    }

    /**
     * P3-073: the record's synthesized {@code toString()} includes every component, so a startup
     * dump or an error context would print the gateway's HMAC secret in plaintext.
     *
     * <p>Hand-written on purpose — a record cannot call the synthesized form — which also fails
     * closed for the future: a newly added component is absent from the dump until someone includes
     * it here, rather than leaking the moment it is declared.
     */
    @Override
    public String toString() {
        return "GatewayConfig[flussBootstrap=" + flussBootstrap + ", flussDatabase=" + flussDatabase
                + ", intentTable=" + intentTable + ", gateTable=" + gateTable
                + ", attemptsTable=" + attemptsTable + ", correlationTable=" + correlationTable
                + ", ledgerTable=" + ledgerTable + ", haltTable=" + haltTable
                + ", bindHost=" + bindHost + ", bindPort=" + bindPort
                + ", nautilusEndpoint=" + nautilusEndpoint + ", protocolVersion=" + protocolVersion
                + ", sharedSecret=***"
                + ", requestTimeout=" + requestTimeout + ", pollTimeout=" + pollTimeout
                + ", accountScopeId=" + accountScopeId
                + ", executionPartitionId=" + executionPartitionId
                + ", executionEnabled=" + executionEnabled
                + ", maxPendingProjectionRecords=" + maxPendingProjectionRecords
                + ", requestBudget=" + requestBudget + "]";
    }

}
