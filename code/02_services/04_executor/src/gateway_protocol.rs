//! Private gateway-to-Nautilus authenticated envelope (T4).
//!
//! Faithful Rust port of `com.trading.execution.gateway.GatewayProtocol`:
//! - canonical string is `protocol_version\nmessage_type\nrequest_id\naccount_scope_id\n`
//!   `execution_partition_id\npayload_hash\ngate_epoch\nfence_token\ndeadline_epoch_ms\n`
//!   `payload_json` (payload is the compact JSON encoding of the `payload` node);
//! - authentication is `hex(hmacSha256(canonical, secret))`;
//! - payload hash is `hex(sha256(payload_json_bytes))`;
//! - verification checks version, required identity fields, deadline, HMAC, and
//!   payload hash — same order and semantics as the Java `verify` method.
//!
//! The service **never** logs the shared secret.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// Result of envelope verification, mirroring `GatewayProtocol.Verification`.
#[derive(Debug, Clone)]
pub struct Verification {
    pub accepted: bool,
    pub reason: String,
    pub envelope: Option<Envelope>,
}

#[derive(Debug, Clone)]
pub struct Envelope {
    pub protocol_version: String,
    pub message_type: String,
    pub request_id: String,
    pub account_scope_id: String,
    pub execution_partition_id: String,
    pub payload_hash: String,
    pub gate_epoch: i64,
    pub fence_token: String,
    pub deadline_epoch_ms: i64,
    pub payload: serde_json::Value,
    pub authentication: String,
}

/// Compute `hex(sha256(bytes))` — same as Java `GatewayProtocol.sha256`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

fn hmac_hex(secret: &str, value: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(value.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Canonical-form generations (P3-079). v1 joins fields with a bare newline; v2 length-prefixes
/// them. Both are accepted — the version field selects the form, so an upgrade never has to be
/// atomic across the gateway, the executor and the sandbox client. Must match the Java
/// `GatewayProtocol.PROTOCOL_V1` / `PROTOCOL_V2`.
pub const PROTOCOL_V1: &str = "execution-gateway.v1";
pub const PROTOCOL_V2: &str = "execution-gateway.v2";

/// Picks the canonical form from the envelope's version — deterministic, never a guess.
///
/// v2 is strictly opt-in by name. Every other version (including a custom generation string) keeps
/// the legacy newline join byte for byte, so nothing that signed or verified before this change
/// signs or verifies differently now; the only requirement is that the version-to-form mapping is
/// total and stable, which it is.
fn canonical(e: &Envelope, payload_json: &str) -> String {
    if e.protocol_version == PROTOCOL_V2 {
        canonical_v2(e, payload_json)
    } else {
        canonical_v1(e, payload_json)
    }
}

/// v1: fields joined by a bare newline. Ambiguous across field boundaries — a `request_id` of
/// "a\nb" with scope "c" signs identically to "a" with "b\nc" — which is why the newline backstop
/// exists. Retained byte-for-byte so existing peers keep verifying.
fn canonical_v1(e: &Envelope, payload_json: &str) -> String {
    [
        e.protocol_version.as_str(),
        e.message_type.as_str(),
        e.request_id.as_str(),
        e.account_scope_id.as_str(),
        e.execution_partition_id.as_str(),
        e.payload_hash.as_str(),
        &e.gate_epoch.to_string(),
        e.fence_token.as_str(),
        &e.deadline_epoch_ms.to_string(),
        payload_json,
    ]
    .join("\n")
}

/// v2 (P3-079): length-prefixed, so the encoding is injective by construction — no field content
/// can forge a boundary. Must match Java `GatewayProtocol.lengthPrefixed` and Python
/// `canonical_v2` byte for byte: for each field, `decimal(utf8 byte length)` + ':' + field,
/// concatenated with no separator.
///
/// `str::len()` is the UTF-8 byte length in Rust, which is what the other two count.
fn canonical_v2(e: &Envelope, payload_json: &str) -> String {
    length_prefixed(&[
        e.protocol_version.as_str(),
        e.message_type.as_str(),
        e.request_id.as_str(),
        e.account_scope_id.as_str(),
        e.execution_partition_id.as_str(),
        e.payload_hash.as_str(),
        &e.gate_epoch.to_string(),
        e.fence_token.as_str(),
        &e.deadline_epoch_ms.to_string(),
        payload_json,
    ])
}

/// THE canonical v2 rule, as a standalone helper so it can be tested directly. `str::len()` is the
/// UTF-8 byte length in Rust, which is what Java's `getBytes(UTF_8).length` and Python's
/// `len(s.encode("utf-8"))` also produce.
fn length_prefixed(fields: &[&str]) -> String {
    let mut out = String::new();
    for field in fields {
        out.push_str(&field.len().to_string());
        out.push(':');
        out.push_str(field);
    }
    out
}

/// Identity fields that v1's newline join cannot keep unambiguous. v2 is injective, so this is
/// applied to v1 envelopes only.
fn v1_has_newline_boundary(e: &Envelope) -> bool {
    [
        e.protocol_version.as_str(),
        e.message_type.as_str(),
        e.request_id.as_str(),
        e.account_scope_id.as_str(),
        e.execution_partition_id.as_str(),
        e.payload_hash.as_str(),
        e.fence_token.as_str(),
    ]
    .iter()
    .any(|p| p.contains('\n') || p.contains('\r'))
}

/// P3-079 rollout, matching Java `acceptsVersion`: the configured version may name more than one
/// accepted generation ("execution-gateway.v1,execution-gateway.v2") so a mixed fleet migrates
/// without a flag day. A single value behaves exactly as before.
///
/// Deliberately does NOT whitelist the known generation literals — pinning them would reject an
/// operator's custom version string that verified perfectly well before v2 existed, and it would buy
/// no security, because `canonical` maps a version to its form deterministically (no form for a peer
/// to guess at) and the accepted set is the operator's config, which a peer cannot influence.
fn accepts_version(configured: &str, presented: &str) -> bool {
    configured.split(',').any(|c| c.trim() == presented)
}

fn text(v: &serde_json::Value, field: &str) -> String {
    v.get(field)
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string()
}

fn reject(reason: &str) -> Verification {
    Verification {
        accepted: false,
        reason: reason.to_string(),
        envelope: None,
    }
}

/// Encode an unsigned envelope: compute canonical + HMAC and return the full JSON string.
///
/// Mirrors `GatewayProtocol.encode` — the caller supplies all fields except `authentication`.
pub fn encode_envelope(secret: &str, e: &Envelope) -> Result<String, String> {
    if secret.is_empty() {
        return Err("secret required".to_string());
    }
    // P3-079 (Java parity): v1's canonical form joins fields with a bare
    // newline, so a request_id of "a\nb" plus scope "c" signs identically to
    // "a" plus "b\nc" (cross-field shift, valid HMAC, wrong binding). Reject
    // newline-bearing identity fields before signing — same rule as Java
    // requireNoNewlines. v2 is length-prefixed and needs no such guard, so this
    // is applied to v1 envelopes only.
    if e.protocol_version != PROTOCOL_V2 && v1_has_newline_boundary(e) {
        return Err("identity field must not contain newline".to_string());
    }
    // payload_json must be the compact encoding the Java ObjectMapper would produce.
    // We use serde_json::to_string which is compact; Java's default is also compact
    // for the payload node (no pretty). For cross-language fidelity the test vectors
    // use the same compact form.
    let payload_json = serde_json::to_string(&e.payload).map_err(|e| e.to_string())?;
    let canon = canonical(e, &payload_json);
    let auth = hmac_hex(secret, &canon);
    let mut node = serde_json::Map::new();
    node.insert(
        "protocol_version".into(),
        serde_json::Value::String(e.protocol_version.clone()),
    );
    node.insert(
        "message_type".into(),
        serde_json::Value::String(e.message_type.clone()),
    );
    node.insert(
        "request_id".into(),
        serde_json::Value::String(e.request_id.clone()),
    );
    node.insert(
        "account_scope_id".into(),
        serde_json::Value::String(e.account_scope_id.clone()),
    );
    node.insert(
        "execution_partition_id".into(),
        serde_json::Value::String(e.execution_partition_id.clone()),
    );
    node.insert(
        "payload_hash".into(),
        serde_json::Value::String(e.payload_hash.clone()),
    );
    node.insert(
        "gate_epoch".into(),
        serde_json::Value::Number(e.gate_epoch.into()),
    );
    node.insert(
        "fence_token".into(),
        serde_json::Value::String(e.fence_token.clone()),
    );
    node.insert(
        "deadline_epoch_ms".into(),
        serde_json::Value::Number(e.deadline_epoch_ms.into()),
    );
    node.insert("payload".into(), e.payload.clone());
    node.insert("authentication".into(), serde_json::Value::String(auth));
    serde_json::to_string(&serde_json::Value::Object(node)).map_err(|e| e.to_string())
}

/// Verify a JSON envelope string — mirrors `GatewayProtocol.verify`.
pub fn verify(json: &str, secret: &str, expected_version: &str, now_ms: i64) -> Verification {
    if secret.is_empty() {
        return reject("authentication failed");
    }
    let v: serde_json::Value = match serde_json::from_str(json) {
        Ok(x) => x,
        Err(_) => return reject("malformed envelope"),
    };
    let payload = v.get("payload").cloned().unwrap_or(serde_json::Value::Null);
    let e = Envelope {
        protocol_version: text(&v, "protocol_version"),
        message_type: text(&v, "message_type"),
        request_id: text(&v, "request_id"),
        account_scope_id: text(&v, "account_scope_id"),
        execution_partition_id: text(&v, "execution_partition_id"),
        payload_hash: text(&v, "payload_hash"),
        gate_epoch: v
            .get("gate_epoch")
            .and_then(|x| x.as_i64())
            .unwrap_or(i64::MIN),
        fence_token: text(&v, "fence_token"),
        deadline_epoch_ms: v
            .get("deadline_epoch_ms")
            .and_then(|x| x.as_i64())
            .unwrap_or(i64::MIN),
        payload: payload.clone(),
        authentication: text(&v, "authentication"),
    };
    if !accepts_version(&expected_version, &e.protocol_version) {
        return reject("unsupported version");
    }
    if e.request_id.is_empty()
        || e.account_scope_id.is_empty()
        || e.execution_partition_id.is_empty()
        || e.payload_hash.is_empty()
        || e.fence_token.is_empty()
    {
        return reject("missing identity");
    }
    if e.deadline_epoch_ms < now_ms {
        return reject("deadline expired");
    }
    // P3-079 (Java parity): verify what was signed — the signer refuses
    // newline-bearing identity fields, so a hand-crafted v1 envelope carrying one
    // (valid HMAC over a shifted canonical) must not verify. v2 is length-prefixed,
    // so a newline there is unambiguous and must NOT be rejected.
    if e.protocol_version != PROTOCOL_V2 && v1_has_newline_boundary(&e) {
        return reject("malformed envelope");
    }
    // Recompute canonical + HMAC using the same payload_json the sender used.
    // The sender's payload_json is the compact encoding of the payload node.
    let payload_json = match serde_json::to_string(&payload) {
        Ok(s) => s,
        Err(_) => return reject("malformed envelope"),
    };
    let canon = canonical(&e, &payload_json);
    let expected_auth = hmac_hex(secret, &canon);
    if expected_auth != e.authentication {
        return reject("authentication failed");
    }
    let payload_hash = sha256_hex(payload_json.as_bytes());
    if payload_hash != e.payload_hash {
        return reject("payload hash mismatch");
    }
    Verification {
        accepted: true,
        reason: "accepted".to_string(),
        envelope: Some(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn envelope(payload: serde_json::Value) -> Envelope {
        let payload_json = serde_json::to_string(&payload).unwrap();
        let hash = sha256_hex(payload_json.as_bytes());
        Envelope {
            protocol_version: "execution-gateway.v1".into(),
            message_type: "EXECUTION_INTENT".into(),
            request_id: "req-1".into(),
            account_scope_id: "acc-1".into(),
            execution_partition_id: "part-1".into(),
            payload_hash: hash,
            gate_epoch: 7,
            fence_token: "fence-abc".into(),
            deadline_epoch_ms: 9_999_999_999_999,
            payload,
            authentication: String::new(),
        }
    }

    #[test]
    fn round_trip_accepted() {
        let e = envelope(json!({"instruction_id":"i1"}));
        let encoded = encode_envelope("s3cr3t", &e).unwrap();
        let v = verify(&encoded, "s3cr3t", "execution-gateway.v1", 1_000_000);
        assert!(v.accepted, "reason: {}", v.reason);
    }

    #[test]
    fn rejects_wrong_secret() {
        let e = envelope(json!({"x":1}));
        let encoded = encode_envelope("s3cr3t", &e).unwrap();
        let v = verify(&encoded, "wrong", "execution-gateway.v1", 1_000_000);
        assert!(!v.accepted);
        assert_eq!(v.reason, "authentication failed");
    }

    #[test]
    fn rejects_expired_deadline() {
        let mut e = envelope(json!({"x":1}));
        e.deadline_epoch_ms = 1000;
        let encoded = encode_envelope("s3cr3t", &e).unwrap();
        let v = verify(&encoded, "s3cr3t", "execution-gateway.v1", 2_000);
        assert!(!v.accepted);
        assert_eq!(v.reason, "deadline expired");
    }

    #[test]
    fn rejects_wrong_version() {
        let e = envelope(json!({"x":1}));
        let encoded = encode_envelope("s3cr3t", &e).unwrap();
        let v = verify(&encoded, "s3cr3t", "other.v9", 1_000_000);
        assert!(!v.accepted);
        assert_eq!(v.reason, "unsupported version");
    }

    #[test]
    fn rejects_missing_identity() {
        let mut e = envelope(json!({"x":1}));
        e.request_id = String::new();
        let encoded = encode_envelope("s3cr3t", &e).unwrap();
        // Manually blank the field after encode to simulate missing.
        let mut v: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        v["request_id"] = serde_json::Value::String(String::new());
        let s = serde_json::to_string(&v).unwrap();
        let vr = verify(&s, "s3cr3t", "execution-gateway.v1", 1_000_000);
        assert!(!vr.accepted);
        assert_eq!(vr.reason, "missing identity");
    }

    #[test]
    fn payload_hash_mismatch() {
        let e = envelope(json!({"x":1}));
        let mut encoded = encode_envelope("s3cr3t", &e).unwrap();
        // Tamper payload after encode without updating hash.
        let mut v: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        v["payload"] = json!({"x":2});
        encoded = serde_json::to_string(&v).unwrap();
        let vr = verify(&encoded, "s3cr3t", "execution-gateway.v1", 1_000_000);
        assert!(
            !vr.accepted,
            "tampered payload should fail auth: {}",
            vr.reason
        );
        // Auth will fail first; but if we recompute auth for tampered payload, hash mismatches.
        // To test hash path, re-encode with correct auth for new payload but keep old hash.
        let mut e2 = envelope(json!({"x":2}));
        e2.payload_hash = e.payload_hash.clone(); // keep old hash
        let enc2 = encode_envelope("s3cr3t", &e2).unwrap();
        let vr2 = verify(&enc2, "s3cr3t", "execution-gateway.v1", 1_000_000);
        assert!(!vr2.accepted);
        assert_eq!(vr2.reason, "payload hash mismatch");
    }
    #[test]
    fn p3_079_newline_identity_rejected_on_encode_and_verify() {
        // P3-079: "a\nb"+"c" and "a"+"b\nc" sign identically under a bare-\n
        // join — both sides must refuse newline-bearing identity fields.
        let mut e = envelope(json!({"x":1}));
        e.request_id = "a\nb".to_string();
        assert!(encode_envelope("s3cr3t", &e).is_err());
        // Hand-crafted wire bytes (bypasses encode): verify must still refuse.
        let good = envelope(json!({"x":1}));
        let encoded = encode_envelope("s3cr3t", &good).unwrap();
        let mut v: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        v["request_id"] = serde_json::Value::String("a\nb".to_string());
        let s = serde_json::to_string(&v).unwrap();
        let vr = verify(&s, "s3cr3t", "execution-gateway.v1", 1_000_000);
        assert!(!vr.accepted);
    }
    #[test]
    fn deadline_boundary_at_now_is_valid_not_expired() {
        // TIME-004 / TIME-005 skew boundary: verify() expires an envelope only
        // when `deadline_epoch_ms < now_ms` (strict). A deadline exactly equal to
        // now — i.e. zero residual latency, no skew, no backward drift — must be
        // accepted; otherwise a healthy order would be phantom-expired. This
        // pins "latency is never negative / UTC epoch is the monotonic reference".
        let mut e = envelope(json!({"x": 1}));
        e.deadline_epoch_ms = 1_000_000;
        let encoded = encode_envelope("s3cr3t", &e).unwrap();

        // now_ms == deadline -> NOT expired (strict <).
        let v = verify(&encoded, "s3cr3t", "execution-gateway.v1", 1_000_000);
        assert!(
            v.accepted,
            "deadline == now must be accepted, got: {}",
            v.reason
        );

        // now_ms one past the deadline -> expired.
        let v2 = verify(&encoded, "s3cr3t", "execution-gateway.v1", 1_000_001);
        assert!(!v2.accepted);
        assert_eq!(v2.reason, "deadline expired");
    }
    #[test]
    fn time008_envelope_serialize_deserialize_roundtrips_exactly() {
        // TIME-008 ser/deser: encode -> verify reconstructs the SAME envelope — fields,
        // hash and payload survive byte-for-byte (stable UTC/version encoding).
        let e = envelope(json!({"instruction_id":"i1","symbol":"wti","qty":10}));
        let encoded = encode_envelope("s3cr3t", &e).unwrap();
        let v = verify(&encoded, "s3cr3t", "execution-gateway.v1", 1_000_000);
        assert!(v.accepted, "reason: {}", v.reason);
        let got = v.envelope.expect("accepted envelope decodes");
        assert_eq!(got.protocol_version, e.protocol_version);
        assert_eq!(got.message_type, e.message_type);
        assert_eq!(got.request_id, e.request_id);
        assert_eq!(got.account_scope_id, e.account_scope_id);
        assert_eq!(got.execution_partition_id, e.execution_partition_id);
        assert_eq!(got.payload_hash, e.payload_hash);
        assert_eq!(got.gate_epoch, e.gate_epoch);
        assert_eq!(got.fence_token, e.fence_token);
        assert_eq!(got.deadline_epoch_ms, e.deadline_epoch_ms);
        assert_eq!(
            serde_json::to_string(&got.payload).unwrap(),
            serde_json::to_string(&e.payload).unwrap(),
            "payload must round-trip byte-identically (TIME-008)"
        );
    }

    // -------------------------------------------------------------------------
    // Cross-language gateway-protocol HMAC parity (Java ↔ Rust)
    // Fixed fixture mirrors Java GatewayProtocolParityTest.FIXED_PAYLOAD_JSON etc.
    // Deterministic inputs — secret, deadline, request_id, gate_epoch, fence_token
    // and a DELIBERATELY non-alphabetical payload key order (zulu before alpha)
    // that diverges if either side sorts keys (BTreeMap / ORDER_MAP_ENTRIES_BY_KEYS).
    // If canonicalisation differed, the HMAC and outer envelope would mismatch.
    // -------------------------------------------------------------------------
    const PARITY_SECRET: &str = "parity-test-secret-v1-2026-08-24";
    const PARITY_PROTOCOL_VERSION: &str = "execution-gateway.v1";
    const PARITY_MESSAGE_TYPE: &str = "EXECUTION_INTENT";
    const PARITY_REQUEST_ID: &str = "parity-req-0001";
    const PARITY_ACCOUNT_SCOPE_ID: &str = "parity-acct-001";
    const PARITY_EXECUTION_PARTITION_ID: &str = "parity-partition-1";
    const PARITY_GATE_EPOCH: i64 = 42;
    const PARITY_FENCE_TOKEN: &str = "parity-fence-token-xyz";
    const PARITY_DEADLINE_EPOCH_MS: i64 = 2_000_000_000_000;
    // Canonical payload bytes — must be byte-identical on both sides. The key order
    // zulu,alpha,qty is intentionally non-alphabetical (sorted would be alpha,qty,zulu).
    const PARITY_PAYLOAD_JSON: &str = r#"{"zulu":"z","alpha":"a","qty":100}"#;
    const PARITY_PAYLOAD_HASH: &str =
        "bceb5c2c5139f412f53bf7d27178ea551f68d333566d52485fc5d705e3d06e71";
    const PARITY_AUTH: &str = "ae9003e44be67518aafd38be98d9cb6132120890925bc1dc713fc2708fa0e9ba";
    const PARITY_ENVELOPE_JSON: &str = r#"{"protocol_version":"execution-gateway.v1","message_type":"EXECUTION_INTENT","request_id":"parity-req-0001","account_scope_id":"parity-acct-001","execution_partition_id":"parity-partition-1","payload_hash":"bceb5c2c5139f412f53bf7d27178ea551f68d333566d52485fc5d705e3d06e71","gate_epoch":42,"fence_token":"parity-fence-token-xyz","deadline_epoch_ms":2000000000000,"payload":{"zulu":"z","alpha":"a","qty":100},"authentication":"ae9003e44be67518aafd38be98d9cb6132120890925bc1dc713fc2708fa0e9ba"}"#;
    const PARITY_NOW_MS: i64 = 1_000_000_000_000; // well before deadline

    // P3-079: cross-language v2 conformance vector. Same field values as the v1 fixture above, so
    // the only difference is the canonical form — a divergence between Java/Rust/Python surfaces
    // here as a wrong HMAC rather than as a subtle mis-binding.
    const PARITY_V2_CANONICAL: &str = "20:execution-gateway.v216:EXECUTION_INTENT15:parity-req-000115:parity-acct-00118:parity-partition-164:bceb5c2c5139f412f53bf7d27178ea551f68d333566d52485fc5d705e3d06e712:4222:parity-fence-token-xyz13:200000000000034:{\"zulu\":\"z\",\"alpha\":\"a\",\"qty\":100}";
    const PARITY_V2_AUTH: &str = "655eba65b51e57c79ce50e620511da371722f430266d32e6a7e44cb425b0e9f0";

    fn parity_envelope_v2() -> Envelope {
        let mut e = parity_envelope();
        e.protocol_version = PROTOCOL_V2.to_string();
        e
    }

    #[test]
    fn p3_079_v2_canonical_matches_cross_language_vector() {
        let e = parity_envelope_v2();
        let payload_json = serde_json::to_string(&e.payload).unwrap();
        let canon = canonical(&e, &payload_json);
        assert_eq!(
            canon, PARITY_V2_CANONICAL,
            "v2 canonical bytes must match the vector"
        );
        assert_eq!(
            hmac_hex(PARITY_SECRET, &canon),
            PARITY_V2_AUTH,
            "v2 HMAC must match the Java/Python vector"
        );
    }

    #[test]
    fn p3_079_length_prefixing_is_injective_where_v1_is_not() {
        // The exact ambiguity P3-079 describes: two DIFFERENT field splits, one v1 string.
        assert_eq!(
            ["a\nb", "c"].join("\n"),
            ["a", "b\nc"].join("\n"),
            "v1 is ambiguous across a field boundary"
        );
        assert_ne!(
            length_prefixed(&["a\nb", "c"]),
            length_prefixed(&["a", "b\nc"]),
            "v2 must keep the same two fields distinct"
        );
        // Byte length, not char length: 'e-acute' is 2 UTF-8 bytes but 1 char.
        assert_eq!(
            length_prefixed(&["\u{e9}"]),
            "2:\u{e9}",
            "v2 must count UTF-8 bytes or the three languages disagree off-ASCII"
        );
    }

    #[test]
    fn p3_079_v2_round_trips_and_v1_fixture_is_untouched() {
        let e = parity_envelope_v2();
        let encoded = encode_envelope(PARITY_SECRET, &e).expect("v2 encode");
        assert!(
            encoded.contains(PARITY_V2_AUTH),
            "encoded v2 envelope must carry the v2 HMAC"
        );
        // The v1 fixture still verifies byte-for-byte — the change is additive.
        let v1 = verify(
            PARITY_ENVELOPE_JSON,
            PARITY_SECRET,
            PARITY_PROTOCOL_VERSION,
            PARITY_NOW_MS,
        );
        assert!(v1.accepted, "v1 fixture must still verify: {}", v1.reason);
        // A v1-only config refuses v2; a dual-accept config takes both, each under its own form.
        let refused = verify(
            &encoded,
            PARITY_SECRET,
            PARITY_PROTOCOL_VERSION,
            PARITY_NOW_MS,
        );
        assert!(
            !refused.accepted,
            "a v1-only config must refuse a v2 envelope"
        );
        let dual = format!("{},{}", PROTOCOL_V1, PROTOCOL_V2);
        let v2_dual = verify(&encoded, PARITY_SECRET, &dual, PARITY_NOW_MS);
        assert!(
            v2_dual.accepted,
            "dual-accept must take v2: {}",
            v2_dual.reason
        );
        let v1_dual = verify(PARITY_ENVELOPE_JSON, PARITY_SECRET, &dual, PARITY_NOW_MS);
        assert!(
            v1_dual.accepted,
            "dual-accept must still take v1: {}",
            v1_dual.reason
        );
    }

    #[test]
    fn p3_079_v2_allows_newline_fields_that_v1_must_refuse() {
        let mut e = parity_envelope_v2();
        e.request_id = "req-\n-shift".to_string();
        // v2: the encoding is injective, so a newline is a non-issue.
        let encoded = encode_envelope(PARITY_SECRET, &e).expect("v2 must accept a newline field");
        let v = verify(&encoded, PARITY_SECRET, PROTOCOL_V2, PARITY_NOW_MS);
        assert!(
            v.accepted,
            "v2 must verify what v1 had to refuse: {}",
            v.reason
        );
        // v1: still refused — the backstop tracks the FORM, not the literal version.
        let mut legacy = e.clone();
        legacy.protocol_version = "v1".to_string();
        assert!(
            encode_envelope(PARITY_SECRET, &legacy).is_err(),
            "a custom version still gets the legacy join and still needs the backstop"
        );
    }

    fn parity_envelope() -> Envelope {
        // Use from_str to preserve the exact FIXED key order; json! with preserve_order
        // would also work but from_str is the most direct proof that deserializing the
        // Java-produced payload bytes round-trips identically.
        let payload: serde_json::Value =
            serde_json::from_str(PARITY_PAYLOAD_JSON).expect("fixture payload must parse");
        Envelope {
            protocol_version: PARITY_PROTOCOL_VERSION.to_string(),
            message_type: PARITY_MESSAGE_TYPE.to_string(),
            request_id: PARITY_REQUEST_ID.to_string(),
            account_scope_id: PARITY_ACCOUNT_SCOPE_ID.to_string(),
            execution_partition_id: PARITY_EXECUTION_PARTITION_ID.to_string(),
            payload_hash: PARITY_PAYLOAD_HASH.to_string(),
            gate_epoch: PARITY_GATE_EPOCH,
            fence_token: PARITY_FENCE_TOKEN.to_string(),
            deadline_epoch_ms: PARITY_DEADLINE_EPOCH_MS,
            payload,
            authentication: String::new(),
        }
    }

    #[test]
    fn parity_payload_serialization_is_byte_identical_to_java() {
        // This is the core preserve_order check: serde_json with preserve_order must
        // emit keys in insertion order (zulu first), matching Jackson ObjectNode.
        // Without preserve_order the output would be {"alpha":"a","qty":100,"zulu":"z"}.
        let payload: serde_json::Value = serde_json::from_str(PARITY_PAYLOAD_JSON).unwrap();
        let json = serde_json::to_string(&payload).unwrap();
        assert_eq!(
            json, PARITY_PAYLOAD_JSON,
            "payload JSON bytes must be byte-identical to Java fixture (preserve_order)"
        );
        // Also verify the json! macro preserves order (insertion order = source order)
        let mac_payload = json!({"zulu":"z","alpha":"a","qty":100});
        let mac_json = serde_json::to_string(&mac_payload).unwrap();
        assert_eq!(
            mac_json, PARITY_PAYLOAD_JSON,
            "json! macro payload must also preserve insertion order"
        );
        // Hash must match the fixture
        let hash = sha256_hex(json.as_bytes());
        assert_eq!(hash, PARITY_PAYLOAD_HASH);
    }

    #[test]
    fn parity_encode_produces_byte_identical_envelope_to_java_fixture() {
        let e = parity_envelope();
        let encoded = encode_envelope(PARITY_SECRET, &e).unwrap();
        assert_eq!(
            encoded, PARITY_ENVELOPE_JSON,
            "Rust encode must be byte-identical to Java fixture (canonical + HMAC match)"
        );
    }

    #[test]
    fn parity_verify_java_signed_token_accepted() {
        // PARITY_ENVELOPE_JSON is documented as the Java-produced token (see
        // GatewayProtocolParityTest.EXPECTED_ENVELOPE_JSON). Rust must accept it.
        let v = verify(
            PARITY_ENVELOPE_JSON,
            PARITY_SECRET,
            PARITY_PROTOCOL_VERSION,
            PARITY_NOW_MS,
        );
        assert!(
            v.accepted,
            "Rust must accept Java-signed parity fixture, got: {}",
            v.reason
        );
        let env = v.envelope.unwrap();
        assert_eq!(env.request_id, PARITY_REQUEST_ID);
        assert_eq!(env.payload_hash, PARITY_PAYLOAD_HASH);
        // Payload round-trip bytes must remain identical after verify decode/encode
        let payload_json = serde_json::to_string(&env.payload).unwrap();
        assert_eq!(payload_json, PARITY_PAYLOAD_JSON);
    }

    #[test]
    fn parity_tampered_payload_rejected() {
        // Tamper the payload inside the otherwise valid envelope — must be rejected
        // (authentication failed or payload hash mismatch, depending on tamper path).
        let mut v: serde_json::Value = serde_json::from_str(PARITY_ENVELOPE_JSON).unwrap();
        v["payload"] = json!({"zulu":"tampered","alpha":"a","qty":100});
        let tampered = serde_json::to_string(&v).unwrap();
        let vr = verify(
            &tampered,
            PARITY_SECRET,
            PARITY_PROTOCOL_VERSION,
            PARITY_NOW_MS,
        );
        assert!(
            !vr.accepted,
            "tampered payload must be rejected, got accepted"
        );
        assert!(
            vr.reason == "authentication failed" || vr.reason == "payload hash mismatch",
            "unexpected tamper reason: {}",
            vr.reason
        );
        // Also test the subtle hash-mismatch path: recompute a valid auth for the
        // new payload but keep the old hash — should hit payload hash mismatch.
        let mut e = parity_envelope();
        e.payload = json!({"zulu":"tampered","alpha":"a","qty":100});
        // Instead construct correctly: encode tampered with correct hash then overwrite hash.
        let correct_hash_payload = json!({"zulu":"tampered","alpha":"a","qty":100});
        let correct_payload_json = serde_json::to_string(&correct_hash_payload).unwrap();
        let correct_hash = sha256_hex(correct_payload_json.as_bytes());
        let mut e2 = e.clone();
        e2.payload_hash = correct_hash.clone();
        let enc2 = encode_envelope(PARITY_SECRET, &e2).unwrap();
        let mut v2: serde_json::Value = serde_json::from_str(&enc2).unwrap();
        v2["payload_hash"] = serde_json::Value::String(PARITY_PAYLOAD_HASH.to_string());
        // re-sign with the stale hash's canonical
        let stale_canonical = [
            PARITY_PROTOCOL_VERSION,
            PARITY_MESSAGE_TYPE,
            PARITY_REQUEST_ID,
            PARITY_ACCOUNT_SCOPE_ID,
            PARITY_EXECUTION_PARTITION_ID,
            PARITY_PAYLOAD_HASH,
            &PARITY_GATE_EPOCH.to_string(),
            PARITY_FENCE_TOKEN,
            &PARITY_DEADLINE_EPOCH_MS.to_string(),
            &correct_payload_json,
        ]
        .join("\n");
        let mut mac = HmacSha256::new_from_slice(PARITY_SECRET.as_bytes()).unwrap();
        mac.update(stale_canonical.as_bytes());
        let stale_auth = hex::encode(mac.finalize().into_bytes());
        v2["authentication"] = serde_json::Value::String(stale_auth);
        let tampered2_json = serde_json::to_string(&v2).unwrap();
        let vr2 = verify(
            &tampered2_json,
            PARITY_SECRET,
            PARITY_PROTOCOL_VERSION,
            PARITY_NOW_MS,
        );
        assert!(!vr2.accepted);
        assert_eq!(vr2.reason, "payload hash mismatch");
    }

    #[test]
    fn parity_rust_encode_verified_by_rust_and_hash_matches_fixture() {
        // Full round-trip: Rust encodes fixed inputs -> verify with same secret succeeds
        // and all decoded fields match the fixture constants.
        let e = parity_envelope();
        let encoded = encode_envelope(PARITY_SECRET, &e).unwrap();
        let v = verify(
            &encoded,
            PARITY_SECRET,
            PARITY_PROTOCOL_VERSION,
            PARITY_NOW_MS,
        );
        assert!(
            v.accepted,
            "self-encoded parity fixture must verify: {}",
            v.reason
        );
        let env = v.envelope.unwrap();
        assert_eq!(env.protocol_version, PARITY_PROTOCOL_VERSION);
        assert_eq!(env.message_type, PARITY_MESSAGE_TYPE);
        assert_eq!(env.account_scope_id, PARITY_ACCOUNT_SCOPE_ID);
        assert_eq!(env.execution_partition_id, PARITY_EXECUTION_PARTITION_ID);
        assert_eq!(env.gate_epoch, PARITY_GATE_EPOCH);
        assert_eq!(env.fence_token, PARITY_FENCE_TOKEN);
        assert_eq!(env.deadline_epoch_ms, PARITY_DEADLINE_EPOCH_MS);
        // Authentication must equal the pre-computed fixture auth
        let parsed: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(parsed["authentication"].as_str().unwrap(), PARITY_AUTH);
    }
}
