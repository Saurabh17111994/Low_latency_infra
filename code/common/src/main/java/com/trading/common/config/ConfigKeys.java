package com.trading.common.config;

/**
 * Central inventory of every environment/config key (H2, 2026-08-29).
 * One grep-able source so a typo in a key string fails loudly instead
 * of silently defaulting. Keys are grouped by owning service prefix.
 */
public final class ConfigKeys {

    private ConfigKeys() {}

    public static final String ACCOUNT_SCOPE_ID = "ACCOUNT_SCOPE_ID";
    public static final String AES256 = "AES256";
    public static final String ALGORITHM_VERSION = "ALGORITHM_VERSION";
    public static final String ALLOWED_LATENESS_MS = "ALLOWED_LATENESS_MS";
    public static final String ALLOW_FULL_REPLAY = "ALLOW_FULL_REPLAY";
    public static final String ALLOW_RUNTIME_DDL = "ALLOW_RUNTIME_DDL";
    public static final String APPEND_TIMEOUT_SECONDS = "APPEND_TIMEOUT_SECONDS";
    public static final String ARROW_APP_ID = "ARROW_APP_ID";
    public static final String ARROW_APP_SECRET = "ARROW_APP_SECRET";
    public static final String ARROW_AUTH_METHOD = "ARROW_AUTH_METHOD";
    public static final String ARROW_HFT_AUTH_REFRESH_ATTEMPTS = "ARROW_HFT_AUTH_REFRESH_ATTEMPTS";
    public static final String ARROW_HFT_CONNECTIONS = "ARROW_HFT_CONNECTIONS";
    public static final String ARROW_HFT_HEARTBEAT_SECONDS = "ARROW_HFT_HEARTBEAT_SECONDS";
    public static final String ARROW_HFT_LATENCY_MS = "ARROW_HFT_LATENCY_MS";
    public static final String ARROW_HFT_MAX_TOKENS_PER_CONNECTION = "ARROW_HFT_MAX_TOKENS_PER_CONNECTION";
    public static final String ARROW_HFT_MAX_TOKENS_PER_REQUEST = "ARROW_HFT_MAX_TOKENS_PER_REQUEST";
    public static final String ARROW_HFT_MIN_ACTIVE_SLOTS = "ARROW_HFT_MIN_ACTIVE_SLOTS";
    public static final String ARROW_HFT_MULTI_CONNECTION_APPROVED = "ARROW_HFT_MULTI_CONNECTION_APPROVED";
    public static final String ARROW_HFT_RECONNECT_BASE_SECONDS = "ARROW_HFT_RECONNECT_BASE_SECONDS";
    public static final String ARROW_HFT_RECONNECT_MAX_SECONDS = "ARROW_HFT_RECONNECT_MAX_SECONDS";
    public static final String ARROW_HFT_STALL_TIMEOUT_SECONDS = "ARROW_HFT_STALL_TIMEOUT_SECONDS";
    public static final String ARROW_HFT_SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS = "ARROW_HFT_SUBSCRIPTION_RESPONSE_TIMEOUT_SECONDS";
    public static final String ARROW_INSTRUMENT_TOKENS = "ARROW_INSTRUMENT_TOKENS";
    public static final String ARROW_MAX_EVENT_AGE_MS = "ARROW_MAX_EVENT_AGE_MS";
    public static final String ARROW_MAX_FUTURE_EVENT_SKEW_MS = "ARROW_MAX_FUTURE_EVENT_SKEW_MS";
    public static final String ARROW_PASSWORD = "ARROW_PASSWORD";
    public static final String ARROW_TOKEN = "ARROW_TOKEN";
    public static final String ARROW_TOTP_KEY = "ARROW_TOTP_KEY";
    public static final String ARROW_USER_ID = "ARROW_USER_ID";
    public static final String AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID";
    public static final String AWS_ACCESS_KEY_ID_FILE = "AWS_ACCESS_KEY_ID_FILE";
    public static final String AWS_REGION = "AWS_REGION";
    public static final String AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY";
    public static final String AWS_SECRET_ACCESS_KEY_FILE = "AWS_SECRET_ACCESS_KEY_FILE";
    public static final String CANDLE_WINDOW_MS = "CANDLE_WINDOW_MS";
    public static final String CHECKPOINT_DIR = "CHECKPOINT_DIR";
    public static final String CHECKPOINT_INTERVAL_MS = "CHECKPOINT_INTERVAL_MS";
    public static final String CHECKPOINT_TIMEOUT_MS = "CHECKPOINT_TIMEOUT_MS";
    public static final String CLOCK_CHECK_REQUIRED = "CLOCK_CHECK_REQUIRED";
    public static final String CLOCK_OFFSET_LIMIT_MS = "CLOCK_OFFSET_LIMIT_MS";
    public static final String CNC = "CNC";
    public static final String CONFIGURATION_VERSION = "CONFIGURATION_VERSION";
    public static final String DAY = "DAY";
    public static final String DEDUP_WINDOW_ENTRIES = "DEDUP_WINDOW_ENTRIES";
    /**
     * Preferred deployment key: ingestion reads this one and falls back to {@link #DEPLOY_ENV}
     * (P1-077), and log4j2 resolves {@code ${env:DEPLOYMENT_ENV:-${env:DEPLOY_ENV:-dev}}} in the
     * same order. Setting it can never be silently outranked.
     */
    public static final String DEPLOYMENT_ENV = "DEPLOYMENT_ENV";

    /** Legacy alias, read only when {@link #DEPLOYMENT_ENV} is absent. */
    public static final String DEPLOY_ENV = "DEPLOY_ENV";

    /** Compose env-file deployments set this marker; SecretGuard respects it. */
    public static final String SECRETS_VIA_ENV_FILE = "SECRETS_VIA_ENV_FILE";
    public static final String DRAIN_DEADLINE_SECONDS = "DRAIN_DEADLINE_SECONDS";
    public static final String EOD_MASTER_KEY = "EOD_MASTER_KEY";
    public static final String ERROR = "ERROR";
    public static final String EXECUTION_INTENT_ENABLED = "EXECUTION_INTENT_ENABLED";
    public static final String EXECUTION_INTENT_TABLE = "EXECUTION_INTENT_TABLE";
    public static final String EXECUTION_PARTITION_ID = "EXECUTION_PARTITION_ID";
    public static final String EXECUTION_PRODUCT_TYPE = "EXECUTION_PRODUCT_TYPE";
    public static final String EXECUTION_TIME_IN_FORCE = "EXECUTION_TIME_IN_FORCE";
    public static final String FLUSS_BOOTSTRAP = "FLUSS_BOOTSTRAP";
    public static final String FLUSS_BOOTSTRAP_SERVERS = "FLUSS_BOOTSTRAP_SERVERS";
    public static final String FLUSS_DATABASE = "FLUSS_DATABASE";
    public static final String FLUSS_WRITERS = "FLUSS_WRITERS";
    public static final String FLUSS_WRITER_BATCH_SIZE_BYTES = "FLUSS_WRITER_BATCH_SIZE_BYTES";
    public static final String FLUSS_WRITER_BATCH_TIMEOUT_MS = "FLUSS_WRITER_BATCH_TIMEOUT_MS";
    public static final String FLUSS_WRITER_MODE = "FLUSS_WRITER_MODE";
    public static final String FLUSS_WRITER_RETRIES = "FLUSS_WRITER_RETRIES";
    public static final String GO_ARROW_SDK_VERSION = "GO_ARROW_SDK_VERSION";
    public static final String HOSTNAME = "HOSTNAME";
    public static final String INFO = "INFO";
    public static final String INGESTION_ALLOW_DEGRADED = "INGESTION_ALLOW_DEGRADED";
    public static final String INGESTION_MAX_BATCH_RECORDS = "INGESTION_MAX_BATCH_RECORDS";
    public static final String INGESTION_MAX_BATCH_WAIT_MS = "INGESTION_MAX_BATCH_WAIT_MS";
    public static final String INGEST_VALIDATE_PAYLOAD_HASH = "INGEST_VALIDATE_PAYLOAD_HASH";
    public static final String JVM_HEAP_PERCENT = "JVM_HEAP_PERCENT";
    public static final String MAX_CONCURRENT_CHECKPOINTS = "MAX_CONCURRENT_CHECKPOINTS";
    public static final String MAX_PENDING_APPEND_BYTES = "MAX_PENDING_APPEND_BYTES";
    public static final String MAX_PENDING_APPEND_RECORDS = "MAX_PENDING_APPEND_RECORDS";
    public static final String MEMORY_ALERT_PERCENT = "MEMORY_ALERT_PERCENT";
    public static final String NON_HEAP_RESERVE_PERCENT = "NON_HEAP_RESERVE_PERCENT";
    public static final String O2_AUTH_BASIC = "O2_AUTH_BASIC";
    public static final String O2_PASSWORD = "O2_PASSWORD";
    public static final String OTEL_COLLECTOR_HOST = "OTEL_COLLECTOR_HOST";
    public static final String PARALLELISM = "PARALLELISM";
    public static final String PENDING_APPEND_WARNING_PERCENT = "PENDING_APPEND_WARNING_PERCENT";
    public static final String PENDING_MAX_BYTES = "PENDING_MAX_BYTES";
    public static final String PENDING_MAX_RECORDS = "PENDING_MAX_RECORDS";
    public static final String PENDING_WARNING_PERCENT = "PENDING_WARNING_PERCENT";
    public static final String PROFILE = "PROFILE";
    public static final String R2_ENDPOINT = "R2_ENDPOINT";
    public static final String RAW_SCHEMA_VERSION = "RAW_SCHEMA_VERSION";
    public static final String RAW_TABLE = "RAW_TABLE";
    public static final String RAW_TABLE_NAME = "RAW_TABLE_NAME";
    public static final String RESTART_DELAY_MS = "RESTART_DELAY_MS";
    public static final String RESTART_MAX_ATTEMPTS = "RESTART_MAX_ATTEMPTS";
    public static final String S3_ENDPOINT = "S3_ENDPOINT";
    public static final String S3_PATH_STYLE = "S3_PATH_STYLE";
    public static final String SAVEPOINT_DIR = "SAVEPOINT_DIR";
    public static final String SIGNAL_CANDIDATES_TABLE = "SIGNAL_CANDIDATES_TABLE";
    public static final String SIGNAL_CURRENT_TABLE = "SIGNAL_CURRENT_TABLE";
    public static final String SIGNAL_QUANTITY = "SIGNAL_QUANTITY";
    public static final String SIGNAL_STRATEGY_ID = "SIGNAL_STRATEGY_ID";
    public static final String SIGNAL_STRATEGY_VERSION = "SIGNAL_STRATEGY_VERSION";
    public static final String SINK_WRITE_STALL_TIMEOUT_MS = "SINK_WRITE_STALL_TIMEOUT_MS";
    public static final String SOURCE_IDLE_ALERT_MS = "SOURCE_IDLE_ALERT_MS";
    public static final String SOURCE_IDLE_MS = "SOURCE_IDLE_MS";
    public static final String STATE_BACKEND = "STATE_BACKEND";
    public static final String STATE_BACKEND_LOCAL_DIRS = "STATE_BACKEND_LOCAL_DIRS";
    public static final String STATE_BACKEND_MANAGED_MEMORY = "STATE_BACKEND_MANAGED_MEMORY";
    public static final String STATE_RECOVERY_PATH = "STATE_RECOVERY_PATH";
    public static final String TASK_MANAGER_MEMORY_MANAGED_SIZE = "TASK_MANAGER_MEMORY_MANAGED_SIZE";
    public static final String TASK_MANAGER_NETWORK_MEMORY_MAX = "TASK_MANAGER_NETWORK_MEMORY_MAX";
    public static final String TRADE_DECISIONS_ENABLED = "TRADE_DECISIONS_ENABLED";
    public static final String TRADE_DECISIONS_TABLE = "TRADE_DECISIONS_TABLE";
    public static final String TRADE_INSTRUCTION_STATE_TABLE = "TRADE_INSTRUCTION_STATE_TABLE";
    public static final String MULTITF_ENABLED = "MULTITF_ENABLED";
    public static final String MULTITF_LIVE_SNAPSHOT_INTERVAL_MS = "MULTITF_LIVE_SNAPSHOT_INTERVAL_MS";
    public static final String MULTITF_SESSION_BYPASS = "MULTITF_SESSION_BYPASS";
    public static final String MULTITF_SIGNAL_CONTEXT_ENABLED = "MULTITF_SIGNAL_CONTEXT_ENABLED";
    public static final String N7_RULE_ID = "N7_RULE_ID";
    public static final String STRATEGY_HOST_ENABLED = "STRATEGY_HOST_ENABLED";
    public static final String STRATEGIES = "STRATEGIES";
    public static final String CANDLE_LIVE_TABLE = "CANDLE_LIVE_TABLE";
    public static final String CANDLE_CLOSED_TABLE = "CANDLE_CLOSED_TABLE";
    public static final String UNCERTAINTY_JOURNAL_PATH = "UNCERTAINTY_JOURNAL_PATH";
    public static final String WARN = "WARN";
    public static final String WATERMARK_OUT_OF_ORDER_MS = "WATERMARK_OUT_OF_ORDER_MS";
}
