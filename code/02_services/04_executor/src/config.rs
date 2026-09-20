use std::net::SocketAddr;

use anyhow::{bail, Context, Result};

/// Pinned execution-core constants (dossier §Configuration contract — §Boundary contracts).
/// Fail-closed defaults: changing these requires dossier update + approval.
pub const FENCING_LEASE_PROFILE: &str = "30s";
pub const CORRELATION_POLICY_VERSION: &str = "corr.v1";
pub const GATE_FENCE_TOKEN_BITS: u32 = 64;

/// Strictly parses a boolean env value (P3-434): only `true`/`false` are accepted, case-insensitive
/// and with surrounding whitespace ignored. Any other spelling is a hard error instead of a silent
/// `false` — an operator's failed attempt to enable a durable flag must not pass unnoticed, and an
/// unparseable value must fail closed rather than be guessed at.
fn parse_bool_env(key: &str, raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => bail!("{key} must be \"true\" or \"false\", got {raw:?}"),
    }
}

/// CHG-249: Docker and Swarm secrets are mounted as FILES, so a secret-based deployment points
/// `GATEWAY_SHARED_SECRET_FILE` at the mount and leaves `GATEWAY_SHARED_SECRET` unset — the
/// idiomatic `_FILE` pattern, matching the Go execution bridge and the Java gateway. The file wins
/// when both forms are present. A named-but-unreadable or empty file is a hard error: the executor
/// must never fall back to an unset secret while appearing configured. Resolution happens before
/// parsing so the P3-435 emptiness check below keeps working unchanged.
fn apply_gateway_secret_file(vars: &mut std::collections::HashMap<String, String>) -> Result<()> {
    let path = vars
        .get("GATEWAY_SHARED_SECRET_FILE")
        .map(|p| p.trim().to_string())
        .unwrap_or_default();
    if path.is_empty() {
        return Ok(());
    }
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("GATEWAY_SHARED_SECRET_FILE={path} unreadable"))?;
    let secret = raw.trim();
    if secret.is_empty() {
        bail!("GATEWAY_SHARED_SECRET_FILE={path} is empty");
    }
    vars.insert("GATEWAY_SHARED_SECRET".to_string(), secret.to_string());
    Ok(())
}

/// Strict service configuration — HALTED default, fail-closed.
///
/// Endpoints (gateway/bridge) are optional at boot so the service can start health-only and
/// HALTED without a broker being reachable; they are consumed only when a connection is actually
/// needed (later work packages). The service **never** reads `ARROW_*` variables.
// NOTE: no `Debug` derive — this struct carries `BRIDGE_AUTH_TOKEN` and the gateway shared
// secret. The manual impl below redacts both (P3-193 sibling); the `bridge_auth_token` field
// comment's "never logged" promise depends on it.
#[derive(Clone)]
pub struct ServiceConfig {
    pub gateway_endpoint: String,
    pub bridge_endpoint: String,
    /// Bearer token for the Go execution bridge (`BRIDGE_AUTH_TOKEN`). Empty by default;
    /// only sent to the private execution-net bridge (never logged — see main.rs).
    pub bridge_auth_token: String,
    pub log_level: String,
    pub execution_enabled: bool,
    pub listen_addr: String,
    pub gateway_shared_secret: String,
    pub protocol_version: String,
    /// Durable write-path flags (B7) — each client behind a dedicated flag, default OFF.
    /// Enabling requires explicit user approval (recorded in the CHG per B7.5).
    pub durable_gate_enabled: bool,
    pub durable_attempts_enabled: bool,
    pub durable_journal_enabled: bool,
    pub durable_audit_enabled: bool,
    /// Directory the file-backed durable stores live in (D1/D2). Only read when a durable flag is
    /// ON; the default is inside the service's own working directory, so a deployment that wants the
    /// stores to outlive a container must point `DURABLE_DIR` at a mounted volume.
    pub durable_dir: String,
    /// The partition this executor owns, from `EXECUTION_PARTITION_ID`. Required exactly when the
    /// durable gate is enabled: without it there is no partition whose row the executor could own.
    pub execution_partition_id: Option<String>,
    /// Max |host-clock offset vs UTC| in ms before the drift monitor safety-halts (B8).
    /// Mirrors compose `CLOCK_OFFSET_LIMIT_MS` (ingestion default 200 — see CHG-064).
    pub clock_offset_limit_ms: i64,
}

impl std::fmt::Debug for ServiceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceConfig")
            .field("gateway_endpoint", &self.gateway_endpoint)
            .field("bridge_endpoint", &self.bridge_endpoint)
            .field("bridge_auth_token", &"<redacted>")
            .field("log_level", &self.log_level)
            .field("execution_enabled", &self.execution_enabled)
            .field("listen_addr", &self.listen_addr)
            .field("gateway_shared_secret", &"<redacted>")
            .field("protocol_version", &self.protocol_version)
            .field("durable_gate_enabled", &self.durable_gate_enabled)
            .field("durable_attempts_enabled", &self.durable_attempts_enabled)
            .field("durable_journal_enabled", &self.durable_journal_enabled)
            .field("durable_audit_enabled", &self.durable_audit_enabled)
            .field("durable_dir", &self.durable_dir)
            .field("execution_partition_id", &self.execution_partition_id)
            .field("clock_offset_limit_ms", &self.clock_offset_limit_ms)
            .finish()
    }
}

impl ServiceConfig {
    /// Parses from the process environment; fails closed on a forbidden `EXECUTION_ENABLED=true`.
    pub fn from_env() -> Result<Self> {
        let mut vars: std::collections::HashMap<String, String> = std::env::vars().collect();
        apply_gateway_secret_file(&mut vars)?;
        Self::from_iter(vars)
    }

    /// Parses from a key/value iterator (testable without mutating process env).
    pub(crate) fn from_iter<I>(kv: I) -> Result<Self>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let map: std::collections::HashMap<String, String> = kv.into_iter().collect();
        let get = |k: &str| map.get(k).map(String::as_str);

        // Single strict bool reader for every boolean env (P3-434); absent means `false`.
        let bool_env = |key: &str| -> Result<bool> {
            match get(key) {
                Some(raw) => parse_bool_env(key, raw),
                None => Ok(false),
            }
        };

        // Fail closed: execution may never be enabled at boot.
        let enabled = bool_env("EXECUTION_ENABLED")?;
        if enabled {
            bail!("EXECUTION_ENABLED must not be true at boot — service always starts HALTED");
        }

        // Optional endpoints — health-only boot is allowed without a broker.
        let gateway = get("GATEWAY_ENDPOINT").unwrap_or("").to_string();
        let bridge = get("BRIDGE_ENDPOINT").unwrap_or("").to_string();
        let bridge_auth_token = get("BRIDGE_AUTH_TOKEN").unwrap_or("").to_string();
        let gateway_shared_secret = get("GATEWAY_SHARED_SECRET").unwrap_or("").to_string();

        // Fail closed (P3-435): a configured gateway endpoint with no usable secret would boot
        // credential-less and then attempt authenticated communication with the gateway.
        if !gateway.trim().is_empty() && gateway_shared_secret.trim().is_empty() {
            bail!(
                "GATEWAY_SHARED_SECRET must be non-empty when GATEWAY_ENDPOINT is configured \
                 (blank secret would boot without a gateway credential)"
            );
        }

        // Fail closed (P3-191): the B8 drift monitor uses this as its safety-halt bound, so a
        // non-positive limit would trip the watchdog immediately or disable it outright.
        let clock_offset_limit_ms = get("CLOCK_OFFSET_LIMIT_MS")
            .map(|v| v.parse::<i64>())
            .transpose()?
            .unwrap_or(200);
        if clock_offset_limit_ms <= 0 {
            bail!(
                "CLOCK_OFFSET_LIMIT_MS must be > 0 (got {clock_offset_limit_ms}) — it bounds the B8 \
                 drift-monitor safety-halt"
            );
        }

        Ok(Self {
            gateway_endpoint: gateway,
            bridge_endpoint: bridge,
            bridge_auth_token,
            log_level: get("LOG_LEVEL").unwrap_or("info").to_string(),
            execution_enabled: false,
            listen_addr: get("EXECUTOR_LISTEN_ADDR")
                .unwrap_or("127.0.0.1:8787")
                .to_string(),
            gateway_shared_secret,
            protocol_version: get("GATEWAY_PROTOCOL_VERSION")
                .unwrap_or("execution-gateway.v2")
                .to_string(),
            clock_offset_limit_ms,
            durable_gate_enabled: bool_env("DURABLE_GATE_ENABLED")?,
            durable_attempts_enabled: bool_env("DURABLE_ATTEMPTS_ENABLED")?,
            durable_journal_enabled: bool_env("DURABLE_JOURNAL_ENABLED")?,
            durable_audit_enabled: bool_env("DURABLE_AUDIT_ENABLED")?,
            // Only consulted when a durable flag is ON; the flag path fails closed if the directory
            // cannot be opened or created, so a bad value is reported at the store, not swallowed.
            durable_dir: get("DURABLE_DIR").unwrap_or("data/durable").to_string(),
            // Sibling of compose `EXECUTION_PARTITION_ID`. Absent stays absent: the durable gate
            // path refuses to start rather than inventing a partition name.
            execution_partition_id: get("EXECUTION_PARTITION_ID").map(str::to_string),
        })
    }

    /// The address the health server binds to.
    pub fn listen_addr(&self) -> Result<SocketAddr> {
        self.listen_addr
            .parse()
            .with_context(|| format!("invalid EXECUTOR_LISTEN_ADDR: {}", self.listen_addr))
    }

    pub fn is_halted_default(&self) -> bool {
        !self.execution_enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kv(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn halted_default() {
        let c = ServiceConfig {
            clock_offset_limit_ms: 200,
            durable_gate_enabled: false,
            durable_attempts_enabled: false,
            durable_journal_enabled: false,
            durable_audit_enabled: false,
            durable_dir: "data/durable".into(),
            execution_partition_id: None,
            gateway_endpoint: "http://gw:8080".into(),
            bridge_endpoint: "http://bridge:8787".into(),
            bridge_auth_token: String::new(),
            log_level: "info".into(),
            execution_enabled: false,
            listen_addr: "127.0.0.1:8787".into(),
            gateway_shared_secret: String::new(),
            protocol_version: "execution-gateway.v1".into(),
        };
        assert!(c.is_halted_default());
        assert_eq!(c.listen_addr().unwrap().port(), 8787);
    }

    #[test]
    fn boots_health_only_without_endpoints() {
        let c = ServiceConfig::from_iter(kv(&[("LOG_LEVEL", "debug")])).unwrap();
        assert!(c.gateway_endpoint.is_empty());
        assert!(c.bridge_endpoint.is_empty());
        assert!(!c.execution_enabled);
        assert!(c.is_halted_default());
        assert_eq!(c.listen_addr, "127.0.0.1:8787");
        assert!(c.gateway_shared_secret.is_empty());
        // P3-079: v2 (length-prefixed canonical form) is the default — the fix is native, not
        // opt-in. The env-override test below still pins v1, proving the default is not hardcoded.
        assert_eq!(c.protocol_version, "execution-gateway.v2");
    }

    #[test]
    fn reads_bridge_and_listen_from_env() {
        let c = ServiceConfig::from_iter(kv(&[
            ("BRIDGE_ENDPOINT", "http://bridge:8787"),
            ("EXECUTOR_LISTEN_ADDR", "0.0.0.0:8787"),
        ]))
        .unwrap();
        assert_eq!(c.bridge_endpoint, "http://bridge:8787");
        assert_eq!(c.listen_addr, "0.0.0.0:8787");
    }

    #[test]
    fn reads_bridge_auth_token_from_env() {
        let c = ServiceConfig::from_iter(kv(&[
            ("BRIDGE_ENDPOINT", "http://bridge:8787"),
            ("BRIDGE_AUTH_TOKEN", "devtest"),
        ]))
        .unwrap();
        assert_eq!(c.bridge_endpoint, "http://bridge:8787");
        assert_eq!(c.bridge_auth_token, "devtest");
        // Default stays empty (fail-closed; no accidental credential).
        let c = ServiceConfig::from_iter(kv(&[])).unwrap();
        assert!(c.bridge_auth_token.is_empty());
    }

    #[test]
    fn reads_gateway_protocol_from_env() {
        let c = ServiceConfig::from_iter(kv(&[
            ("GATEWAY_SHARED_SECRET", "s3cr3t"),
            ("GATEWAY_PROTOCOL_VERSION", "execution-gateway.v1"),
        ]))
        .unwrap();
        assert_eq!(c.gateway_shared_secret, "s3cr3t");
        assert_eq!(c.protocol_version, "execution-gateway.v1");
    }

    /// P3-193 sibling (follow-on commit): `ServiceConfig` carries `BRIDGE_AUTH_TOKEN` and the
    /// gateway shared secret, so its `Debug` must never print them. The field comment already
    /// promises "never logged" — the derived `Debug` would break that promise.
    #[test]
    fn service_config_debug_never_prints_secrets() {
        let c = ServiceConfig::from_iter(kv(&[
            ("BRIDGE_ENDPOINT", "http://bridge:8787"),
            ("BRIDGE_AUTH_TOKEN", "tok_live_9f4c2a7e88b1"),
            ("GATEWAY_SHARED_SECRET", "gw_live_deadbeef01"),
        ]))
        .unwrap();
        let dbg = format!("{c:?}");
        assert!(
            dbg.contains("http://bridge:8787"),
            "endpoint should stay readable: {dbg}"
        );
        assert!(
            dbg.contains("<redacted>"),
            "redaction should be visible: {dbg}"
        );
        assert!(
            !dbg.contains("tok_live"),
            "bridge token prefix leaked: {dbg}"
        );
        assert!(
            !dbg.contains("9f4c2a7e88b1"),
            "bridge token suffix leaked: {dbg}"
        );
        assert!(
            !dbg.contains("gw_live"),
            "gateway secret prefix leaked: {dbg}"
        );
        assert!(
            !dbg.contains("deadbeef01"),
            "gateway secret suffix leaked: {dbg}"
        );
    }

    #[test]
    fn never_consumes_arrow_vars() {
        // Presence of Arrow credentials must not change any parsed field.
        let c = ServiceConfig::from_iter(kv(&[
            ("ARROW_REST_URL", "https://api"),
            ("ARROW_APP_ID", "app"),
            ("ARROW_TOKEN", "secret-token"),
        ]))
        .unwrap();
        assert!(!c.execution_enabled);
        assert!(c.gateway_endpoint.is_empty());
        assert!(c.bridge_endpoint.is_empty());
    }

    #[test]
    fn rejects_execution_enabled_true() {
        let err = ServiceConfig::from_iter(kv(&[("EXECUTION_ENABLED", "true")])).unwrap_err();
        assert!(
            err.to_string().contains("must not be true at boot"),
            "got: {err}"
        );
    }

    #[test]
    fn rejects_invalid_listen_addr() {
        let c = ServiceConfig::from_iter(kv(&[("EXECUTOR_LISTEN_ADDR", "not-an-addr")])).unwrap();
        assert!(c.listen_addr().is_err());
    }
    #[test]
    fn clock_offset_limit_defaults_and_overrides() {
        // Default mirrors compose CLOCK_OFFSET_LIMIT_MS=200 (B8 / CHG-064).
        let c = ServiceConfig::from_iter(kv(&[])).unwrap();
        assert_eq!(c.clock_offset_limit_ms, 200);
        let c = ServiceConfig::from_iter(kv(&[("CLOCK_OFFSET_LIMIT_MS", "350")])).unwrap();
        assert_eq!(c.clock_offset_limit_ms, 350);
    }

    /// P3-191: the limit gates the B8 drift-monitor safety-halt, so a non-positive value (which
    /// would permanently trip or silently disable the watchdog) must fail at boot, not parse `Ok`.
    #[test]
    fn rejects_non_positive_clock_offset_limit() {
        for bad in ["0", "-1", "-200"] {
            let err = ServiceConfig::from_iter(kv(&[("CLOCK_OFFSET_LIMIT_MS", bad)])).unwrap_err();
            assert!(
                err.to_string().contains("CLOCK_OFFSET_LIMIT_MS"),
                "value {bad:?} must be rejected with the key named, got: {err}"
            );
        }
    }

    /// P3-434: a boolean flag must be spelled `true`/`false` (case-insensitive, surrounding
    /// whitespace ignored). Any other spelling is a hard error, never a silent `false`.
    #[test]
    fn rejects_non_boolean_flag_spellings() {
        for (key, bad) in [
            ("EXECUTION_ENABLED", "1"),
            ("DURABLE_GATE_ENABLED", "yes"),
            ("DURABLE_ATTEMPTS_ENABLED", "on"),
            ("DURABLE_JOURNAL_ENABLED", "0"),
            ("DURABLE_AUDIT_ENABLED", "enabled"),
        ] {
            let err = ServiceConfig::from_iter(kv(&[(key, bad)])).unwrap_err();
            assert!(
                err.to_string().contains(key),
                "{key}={bad:?} must be rejected with the key named, got: {err}"
            );
        }
    }

    #[test]
    fn boolean_flags_accept_only_true_false_spellings() {
        for (raw, expected) in [
            ("true", true),
            ("TRUE", true),
            (" True ", true),
            ("false", false),
            ("FALSE", false),
            (" false ", false),
        ] {
            let c = ServiceConfig::from_iter(kv(&[("DURABLE_GATE_ENABLED", raw)])).unwrap();
            assert_eq!(c.durable_gate_enabled, expected, "raw value {raw:?}");
        }
        // The executor Dockerfile's only real assignment (`ENV EXECUTION_ENABLED=false`) still boots.
        let c = ServiceConfig::from_iter(kv(&[("EXECUTION_ENABLED", "false")])).unwrap();
        assert!(!c.execution_enabled);
    }

    /// P3-435: a configured gateway endpoint with no usable secret would boot without a
    /// credential — fail closed instead.
    #[test]
    fn rejects_gateway_endpoint_without_secret() {
        for pairs in [
            vec![("GATEWAY_ENDPOINT", "http://gw:8080")],
            vec![
                ("GATEWAY_ENDPOINT", "http://gw:8080"),
                ("GATEWAY_SHARED_SECRET", ""),
            ],
            vec![
                ("GATEWAY_ENDPOINT", "http://gw:8080"),
                ("GATEWAY_SHARED_SECRET", "   "),
            ],
        ] {
            let err = ServiceConfig::from_iter(kv(&pairs)).unwrap_err();
            assert!(
                err.to_string().contains("GATEWAY_SHARED_SECRET"),
                "missing/blank secret must name GATEWAY_SHARED_SECRET, got: {err}"
            );
        }
    }

    #[test]
    fn accepts_gateway_endpoint_with_non_blank_secret() {
        // compose/stack default is `local-dev-only` (docker-compose.yml) — never blank.
        let c = ServiceConfig::from_iter(kv(&[
            ("GATEWAY_ENDPOINT", "http://execution-gateway:9180"),
            ("GATEWAY_SHARED_SECRET", "local-dev-only"),
        ]))
        .unwrap();
        assert_eq!(c.gateway_shared_secret, "local-dev-only");
    }

    /// CHG-249: Docker/Swarm secrets arrive as FILES. Resolution happens before parsing so the
    /// downstream emptiness checks keep working unchanged, and the file wins when both forms
    /// are present — the convention already used by the Go execution bridge.
    #[test]
    fn gateway_secret_can_come_from_a_file() {
        let path = std::env::temp_dir().join(format!("executor-secret-{}", std::process::id()));
        std::fs::write(&path, "from-file-sentinel\n").unwrap();

        let mut vars = std::collections::HashMap::new();
        vars.insert("GATEWAY_ENDPOINT".to_string(), "http://gw:8080".to_string());
        vars.insert("GATEWAY_SHARED_SECRET".to_string(), "plain-loses".to_string());
        vars.insert("GATEWAY_SHARED_SECRET_FILE".to_string(), path.to_string_lossy().into_owned());
        apply_gateway_secret_file(&mut vars).unwrap();
        assert_eq!(vars.get("GATEWAY_SHARED_SECRET").map(String::as_str), Some("from-file-sentinel"));

        let c = ServiceConfig::from_iter(vars).unwrap();
        assert_eq!(c.gateway_shared_secret, "from-file-sentinel");
        std::fs::remove_file(&path).ok();
    }

    /// A named-but-unusable file must fail LOUD — never a silent fall-through to no credential.
    #[test]
    fn unusable_gateway_secret_file_fails_loud() {
        let dir = std::env::temp_dir();
        let mut missing = std::collections::HashMap::new();
        missing.insert(
            "GATEWAY_SHARED_SECRET_FILE".to_string(),
            dir.join(format!("executor-secret-absent-{}", std::process::id())).to_string_lossy().into_owned(),
        );
        let err = apply_gateway_secret_file(&mut missing).unwrap_err();
        assert!(err.to_string().contains("GATEWAY_SHARED_SECRET_FILE"), "got: {err}");

        let empty_path = dir.join(format!("executor-secret-empty-{}", std::process::id()));
        std::fs::write(&empty_path, "  \n").unwrap();
        let mut empty = std::collections::HashMap::new();
        empty.insert("GATEWAY_SHARED_SECRET_FILE".to_string(), empty_path.to_string_lossy().into_owned());
        let err = apply_gateway_secret_file(&mut empty).unwrap_err();
        assert!(err.to_string().contains("is empty"), "got: {err}");
        std::fs::remove_file(&empty_path).ok();
    }

    /// Nothing named: behaviour is exactly as before the file form existed.
    #[test]
    fn gateway_secret_file_absent_is_a_no_op() {
        let mut vars = std::collections::HashMap::new();
        vars.insert("GATEWAY_SHARED_SECRET".to_string(), "plain".to_string());
        apply_gateway_secret_file(&mut vars).unwrap();
        assert_eq!(vars.get("GATEWAY_SHARED_SECRET").map(String::as_str), Some("plain"));
    }

    #[test]
    fn durable_flags_default_off_and_selective_enable() {
        let c = ServiceConfig::from_iter(kv(&[])).unwrap();
        assert!(!c.durable_gate_enabled);
        assert!(!c.durable_attempts_enabled);
        assert!(!c.durable_journal_enabled);
        assert!(!c.durable_audit_enabled);
        let c = ServiceConfig::from_iter(kv(&[
            ("DURABLE_GATE_ENABLED", "true"),
            ("DURABLE_JOURNAL_ENABLED", "true"),
        ]))
        .unwrap();
        assert!(c.durable_gate_enabled);
        assert!(!c.durable_attempts_enabled);
        assert!(c.durable_journal_enabled);
        assert!(!c.durable_audit_enabled);
    }
}
