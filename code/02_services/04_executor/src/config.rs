use std::net::SocketAddr;

use anyhow::{bail, Context, Result};

/// Pinned execution-core constants (dossier §Configuration contract — §Boundary contracts).
/// Fail-closed defaults: changing these requires dossier update + approval.
pub const FENCING_LEASE_PROFILE: &str = "30s";
pub const CORRELATION_POLICY_VERSION: &str = "corr.v1";
pub const GATE_FENCE_TOKEN_BITS: u32 = 64;

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
            .field("clock_offset_limit_ms", &self.clock_offset_limit_ms)
            .finish()
    }
}

impl ServiceConfig {
    /// Parses from the process environment; fails closed on a forbidden `EXECUTION_ENABLED=true`.
    pub fn from_env() -> Result<Self> {
        Self::from_iter(std::env::vars())
    }

    /// Parses from a key/value iterator (testable without mutating process env).
    pub(crate) fn from_iter<I>(kv: I) -> Result<Self>
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let map: std::collections::HashMap<String, String> = kv.into_iter().collect();
        let get = |k: &str| map.get(k).map(String::as_str);

        // Fail closed: execution may never be enabled at boot.
        let enabled = get("EXECUTION_ENABLED")
            .map(|v| v.parse::<bool>().unwrap_or(false))
            .unwrap_or(false);
        if enabled {
            bail!("EXECUTION_ENABLED must not be true at boot — service always starts HALTED");
        }

        // Optional endpoints — health-only boot is allowed without a broker.
        let gateway = get("GATEWAY_ENDPOINT").unwrap_or("").to_string();
        let bridge = get("BRIDGE_ENDPOINT").unwrap_or("").to_string();
        let bridge_auth_token = get("BRIDGE_AUTH_TOKEN").unwrap_or("").to_string();
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
            gateway_shared_secret: get("GATEWAY_SHARED_SECRET").unwrap_or("").to_string(),
            protocol_version: get("GATEWAY_PROTOCOL_VERSION")
                .unwrap_or("execution-gateway.v2")
                .to_string(),
            clock_offset_limit_ms,
            durable_gate_enabled: get("DURABLE_GATE_ENABLED")
                .map(|v| v == "true")
                .unwrap_or(false),
            durable_attempts_enabled: get("DURABLE_ATTEMPTS_ENABLED")
                .map(|v| v == "true")
                .unwrap_or(false),
            durable_journal_enabled: get("DURABLE_JOURNAL_ENABLED")
                .map(|v| v == "true")
                .unwrap_or(false),
            durable_audit_enabled: get("DURABLE_AUDIT_ENABLED")
                .map(|v| v == "true")
                .unwrap_or(false),
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
