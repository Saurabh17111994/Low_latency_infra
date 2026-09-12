//! Normalized lifecycle-event emission (A2.4 leg: route ack → gateway `/v1/events`).
//!
//! Maps the synchronous bridge report of a routed place into a
//! `NormalizedExecutionEvent` (exact Java record shape —
//! `06_execution_gateway/.../NormalizedExecutionEvent.java`, Jackson camelCase property
//! names) and POSTs it to the gateway `/v1/events` intake, where
//! `FlussProjectionWriter` persists `Order_Lifecycle` + `Order_Correlation` rows.
//! The original payload blob is not carried (no fill bytes yet); `postbackEventId`
//! is deterministic per attempt (gateway ledger dedup key) so a future re-emission
//! is idempotent.
//!
//! Fail-closed contract:
//! - Missing `GATEWAY_ENDPOINT` → emission is disabled (offline/paper mode), and the
//!   caller surfaces `event_emission: disabled` — never a fake success.
//! - Emission failure does NOT change the 202 order acceptance — the order was already
//!   accepted by the bridge; the caller surfaces `event_emission: failed:<reason>`.
//! - No retry here: a retried emit is idempotent by postback id, but in-process retries
//!   are the reconciler's job (same rule as the sync forward).

use crate::bridge::protocol::{CommandEnvelope, ReportEnvelope};
use crate::bridge::transport::http_post;
use crate::gateway_protocol::{encode_envelope, sha256_hex};

/// Event type used for a routed place acknowledgement lifecycle image.
pub const EVENT_TYPE_LIFECYCLE: &str = "LIFECYCLE";

/// Actor id recorded with events emitted by the nautilus executor.
pub const ACTOR_NAUTILUS: &str = "nautilus";

/// Event envelope message type (canonicalized; the gateway verifies hash+auth, not the tag).
pub const EVENT_MESSAGE_TYPE: &str = "EXECUTION_EVENT";

/// Builds a normalized lifecycle+correlation event image for a routed place ack.
///
/// `trade_context_id` is read from the verified payload; quantities come from the
/// minted place envelope; `now_ms` stamps the event. Returns the exact
/// `NormalizedExecutionEvent` JSON (camelCase, nested records; `audit`/`fill`/`position`
/// are null — there is no fill or position change in a synchronous place ack).
pub fn lifecycle_event_value(
    report: &ReportEnvelope,
    place: &CommandEnvelope,
    account_scope_id: &str,
    execution_partition_id: &str,
    gate_epoch: i64,
    trade_context_id: &str,
    now_ms: i64,
) -> serde_json::Value {
    let postback_event_id = format!(
        "pb-{}",
        sha256_hex(
            format!("v1|{}|{}", place.instruction_id, place.execution_attempt_id).as_bytes()
        )
    );
    let is_cancel = place.command() == Some(crate::bridge::protocol::Command::Cancel);
    // P3-441: the evidence label names the leg that was actually routed, so a Modify ack is
    // not reported as a place ack. `None` means the command name matched no known leg
    // (Command::from_str), which keeps the pre-existing place-ack path.
    let verification_evidence = match place.command() {
        Some(command) => format!("sync-{}-ack", command.as_str()),
        None => "sync-place-ack".to_string(),
    };
    // Default state per leg: a cancel ack without an explicit status normalizes
    // to CANCELED (never ACCEPTED — that would misstate the lifecycle).
    let default_state = if is_cancel { "CANCELED" } else { "ACCEPTED" };
    let normalized_state = report
        .order_status
        .clone()
        .unwrap_or_else(|| default_state.to_string());
    // P3-442: keyed on the normalized state rather than the leg — a cancel the broker refused
    // leaves the order live, so its pending quantity is real and must not be zeroed.
    let pending_qty = if normalized_state == "CANCELED" {
        0
    } else {
        place
            .order
            .as_ref()
            .and_then(|o| o.quantity.parse::<i64>().ok())
            .unwrap_or(0)
    };
    serde_json::json!({
        "postbackEventId": postback_event_id,
        "accountScopeId": account_scope_id,
        "executionPartitionId": execution_partition_id,
        "gateEpoch": gate_epoch,
        "actorId": ACTOR_NAUTILUS,
        "eventType": EVENT_TYPE_LIFECYCLE,
        "eventTs": now_ms,
        "audit": serde_json::Value::Null,
        "fill": serde_json::Value::Null,
        "lifecycle": {
            "brokerOrderId": report.broker_order_id,
            "instructionId": place.instruction_id,
            "executionAttemptId": place.execution_attempt_id,
            "tradeContextId": trade_context_id,
            "normalizedState": normalized_state,
            "cumulativeQty": 0,
            "pendingQty": pending_qty,
            "averageFillPricePaise": serde_json::Value::Null,
            "sourceVersion": 1,
            "sourceEventTime": now_ms,
            "lastReceiveTime": now_ms,
            "correlationState": "VERIFIED",
        },
        "position": serde_json::Value::Null,
        "correlation": {
            "instructionId": place.instruction_id,
            "executionAttemptId": place.execution_attempt_id,
            "clientOrderRef": place.client_order_ref,
            "brokerOrderId": report.broker_order_id,
            "tradeContextId": trade_context_id,
            "positionId": serde_json::Value::Null,
            "verificationState": "VERIFIED",
            "verificationEvidence": verification_evidence,
            "correlatedTs": now_ms,
        },
    })
}

/// POSTs a normalized event image to `{gateway_endpoint}/v1/events`.
///
/// - Empty `gateway_endpoint` → `Err` "event emission disabled" (fail-closed; the caller
///   surfaces `disabled`, never a fake success).
/// - Any 2xx response is success (gateway answers 202); anything else is `Err`.
pub async fn emit_event(
    gateway_endpoint: &str,
    shared_secret: &str,
    protocol_version: &str,
    value: &serde_json::Value,
    now_ms: i64,
) -> Result<(), String> {
    if gateway_endpoint.is_empty() {
        return Err("event emission disabled (no GATEWAY_ENDPOINT)".to_string());
    }
    // P3-443: the function contract is Result, so the emit path must not unwrap. (Serializing a
    // serde_json::Value cannot actually fail — this removes the panic-inside-Result inconsistency.)
    let payload_json = serde_json::to_string(value)
        .map_err(|e| format!("event payload serialization failed: {e}"))?;
    let payload_hash = sha256_hex(payload_json.as_bytes());
    let envelope = crate::gateway_protocol::Envelope {
        protocol_version: protocol_version.to_string(),
        message_type: EVENT_MESSAGE_TYPE.to_string(),
        request_id: value["postbackEventId"]
            .as_str()
            .unwrap_or("evt")
            .to_string(),
        account_scope_id: value["accountScopeId"].as_str().unwrap_or("").to_string(),
        execution_partition_id: value["executionPartitionId"]
            .as_str()
            .unwrap_or("")
            .to_string(),
        payload_hash,
        gate_epoch: value["gateEpoch"].as_i64().unwrap_or(0),
        fence_token: "evt-fence".to_string(),
        deadline_epoch_ms: now_ms + 60_000,
        payload: value.clone(),
        authentication: String::new(),
    };
    let body = encode_envelope(shared_secret, &envelope).map_err(|e| e.to_string())?;
    let url = format!("{}/v1/events", gateway_endpoint.trim_end_matches('/'));
    let resp = http_post(&url, "", body.as_bytes())
        .await
        .map_err(|e| e.to_string())?;
    if (200..300).contains(&resp.status) {
        Ok(())
    } else {
        let body_text = String::from_utf8_lossy(&resp.body);
        Err(format!(
            "gateway /v1/events responded {}: {}",
            resp.status,
            body_text.trim().chars().take(200).collect::<String>()
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::protocol::{Command, OrderCommand};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn place_env() -> CommandEnvelope {
        let order = OrderCommand::new("NSE", "BI-EQ")
            .with_quantity("1")
            .with_side(crate::bridge::protocol::TransactionType::Buy)
            .with_order_type(crate::bridge::protocol::OrderType::Lmt)
            .with_product(crate::bridge::protocol::Product::Cash)
            .with_validity(crate::bridge::protocol::Validity::Day)
            .with_price("5050");
        let mut env = CommandEnvelope::new(Command::Place, "req-1");
        env.instruction_id = "T9-SB-0001".into();
        env.execution_attempt_id = "att-0001".into();
        env.client_order_ref = "a1b2c3d4e5f6a7b8".into();
        env.order = Some(order);
        env
    }

    fn report() -> ReportEnvelope {
        ReportEnvelope {
            broker_order_id: "BRK-0007".into(),
            client_order_ref: "a1b2c3d4e5f6a7b8".into(),
            order_status: Some("ACCEPTED".into()),
            request_id: "req-1".into(),
            ..Default::default()
        }
    }

    fn cancel_env_with_echoed_order(quantity: &str) -> CommandEnvelope {
        // P3-442: the real cancel leg echoes the original order node; the older cancel
        // fixture carries none, so its pending-quantity assertion passed vacuously.
        let order = OrderCommand::new("NSE", "BI-EQ")
            .with_quantity(quantity)
            .with_side(crate::bridge::protocol::TransactionType::Buy)
            .with_order_type(crate::bridge::protocol::OrderType::Lmt)
            .with_product(crate::bridge::protocol::Product::Cash)
            .with_validity(crate::bridge::protocol::Validity::Day)
            .with_price("5050");
        let mut env = CommandEnvelope::new(Command::Cancel, "req-2");
        env.instruction_id = "T9-SB-0001".into();
        env.execution_attempt_id = "att-0002".into();
        env.client_order_ref = "b1b2c3d4e5f6a7b8".into();
        env.broker_order_id = "BRK-0001".into();
        env.order = Some(order);
        env
    }

    #[test]
    fn cancel_ack_with_echoed_order_reports_zero_pending() {
        // P3-442: CANCELED beside a non-zero pending quantity is a self-contradicting record.
        let mut rep = report();
        rep.order_status = None; // a sync cancel ack carries no status -> normalized CANCELED
        let v = lifecycle_event_value(
            &rep,
            &cancel_env_with_echoed_order("100"),
            "s",
            "p",
            1,
            "tc",
            5,
        );
        assert_eq!(v["lifecycle"]["normalizedState"], "CANCELED");
        assert_eq!(v["lifecycle"]["pendingQty"], 0);
    }

    #[test]
    fn cancel_ack_that_did_not_take_effect_keeps_its_pending_quantity() {
        // The other half of P3-442: zeroing must follow the normalized state, not the leg.
        // A cancel the broker refused leaves the order live, so its pending quantity is real.
        let mut rep = report();
        rep.order_status = Some("REJECTED".into());
        let v = lifecycle_event_value(
            &rep,
            &cancel_env_with_echoed_order("100"),
            "s",
            "p",
            1,
            "tc",
            5,
        );
        assert_eq!(v["lifecycle"]["normalizedState"], "REJECTED");
        assert_eq!(v["lifecycle"]["pendingQty"], 100);
    }

    #[test]
    fn modify_ack_is_evidenced_as_modify_not_place() {
        // P3-441: the evidence label must name the leg that was actually routed.
        let mut env = place_env();
        env.command = Command::Modify.as_str().into();
        let v = lifecycle_event_value(&report(), &env, "s", "p", 1, "tc", 5);
        assert_eq!(v["correlation"]["verificationEvidence"], "sync-modify-ack");
    }

    #[test]
    fn builds_java_record_shaped_event() {
        let v = lifecycle_event_value(
            &report(),
            &place_env(),
            "dev-scope",
            "dev-partition",
            7,
            "tc-1",
            123,
        );
        // Top-level Newtonian field names (Jackson property names).
        assert_eq!(
            v["postbackEventId"].as_str().unwrap().len(),
            67,
            "pb- + 64 hex"
        );
        assert_eq!(v["accountScopeId"], "dev-scope");
        assert_eq!(v["executionPartitionId"], "dev-partition");
        assert_eq!(v["gateEpoch"], 7);
        assert_eq!(v["actorId"], "nautilus");
        assert_eq!(v["eventType"], "LIFECYCLE");
        assert_eq!(v["eventTs"], 123);
        assert!(v["audit"].is_null() && v["fill"].is_null() && v["position"].is_null());
        // Lifecycle image (Java Lifecycle record fields).
        let lc = &v["lifecycle"];
        assert_eq!(lc["brokerOrderId"], "BRK-0007");
        assert_eq!(lc["instructionId"], "T9-SB-0001");
        assert_eq!(lc["executionAttemptId"], "att-0001");
        assert_eq!(lc["tradeContextId"], "tc-1");
        assert_eq!(lc["normalizedState"], "ACCEPTED");
        assert_eq!(lc["cumulativeQty"], 0);
        assert_eq!(lc["pendingQty"], 1);
        assert!(lc["averageFillPricePaise"].is_null());
        assert_eq!(lc["correlationState"], "VERIFIED");
        // Correlation image (Java Correlation record fields).
        let c = &v["correlation"];
        assert_eq!(c["clientOrderRef"], "a1b2c3d4e5f6a7b8");
        assert_eq!(c["brokerOrderId"], "BRK-0007");
        assert_eq!(c["verificationState"], "VERIFIED");
        assert_eq!(c["correlatedTs"], 123);
        assert!(c["positionId"].is_null());
    }

    #[test]
    fn postback_id_is_deterministic_per_attempt() {
        let a = lifecycle_event_value(&report(), &place_env(), "s", "p", 1, "tc", 1);
        let b = lifecycle_event_value(&report(), &place_env(), "s", "p", 1, "tc", 2);
        assert_eq!(a["postbackEventId"], b["postbackEventId"]);
    }

    #[test]
    fn cancel_ack_normalizes_to_canceled_with_cancel_evidence() {
        let mut env = CommandEnvelope::new(Command::Cancel, "req-2");
        env.instruction_id = "T9-SB-0001".into();
        env.execution_attempt_id = "att-0002".into();
        env.client_order_ref = "b1b2c3d4e5f6a7b8".into();
        env.broker_order_id = "BRK-0001".into();
        // Sync cancel ack carries no explicit status (the fake pushes CANCELED on
        // the async report) — the lifecycle image must still normalize honestly.
        let mut rep = report();
        rep.order_status = None;
        let v = lifecycle_event_value(&rep, &env, "s", "p", 1, "tc", 5);
        assert_eq!(v["lifecycle"]["normalizedState"], "CANCELED");
        assert_eq!(v["lifecycle"]["pendingQty"], 0);
        assert_eq!(v["correlation"]["verificationEvidence"], "sync-cancel-ack");
    }

    #[tokio::test]
    async fn emit_disabled_without_endpoint() {
        let v = lifecycle_event_value(&report(), &place_env(), "s", "p", 1, "tc", 1);
        let err = emit_event("", "secret", "execution-gateway.v1", &v, 1)
            .await
            .unwrap_err();
        assert!(err.contains("disabled"), "err: {err}");
    }

    #[tokio::test]
    async fn emit_posts_to_gateway_events_endpoint() {
        // Spin a minimal listener that captures the request and replies 202.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let server = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            // P3-444: one read() may return a partial request, and the client keeps the socket
            // open for the reply, so there is no EOF to wait for. Read the headers, then the
            // body length they announce, under a timeout so a silent client fails fast.
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            let mut expected: Option<usize> = None;
            loop {
                let n = tokio::time::timeout(std::time::Duration::from_secs(5), s.read(&mut chunk))
                    .await
                    .expect("timed out reading the request")
                    .unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                if expected.is_none() {
                    if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..pos]).to_ascii_lowercase();
                        let len = head
                            .split("content-length:")
                            .nth(1)
                            .and_then(|v| v.trim().split(' ').next())
                            .and_then(|d| d.parse::<usize>().ok())
                            .unwrap_or(0);
                        expected = Some(pos + 4 + len);
                    }
                }
                if let Some(end) = expected {
                    if buf.len() >= end {
                        break;
                    }
                }
            }
            let text = String::from_utf8_lossy(&buf);
            tx.send(text.to_string()).unwrap();
            s.write_all(
                b"HTTP/1.1 202 Accepted\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
            )
            .await
            .unwrap();
            drop(tx);
        });

        let v = lifecycle_event_value(
            &report(),
            &place_env(),
            "dev-scope",
            "dev-partition",
            7,
            "tc-1",
            123,
        );
        emit_event(
            &format!("http://{addr}"),
            "secret",
            "execution-gateway.v1",
            &v,
            1,
        )
        .await
        .expect("emit succeeds");
        let request = rx.recv().await.expect("server got request");
        // The captured request proves the POST targets /v1/events and carries the event.
        assert!(
            request.starts_with("POST /v1/events HTTP/1.1"),
            "request: {request}"
        );
        assert!(
            request.contains("postbackEventId"),
            "request contains event body"
        );
        assert!(request.contains("LIFECYCLE"), "request contains event type");
        // P3-444: the capture must hold the whole body, not just the first TCP segment.
        assert!(
            request.trim_end().ends_with('}'),
            "request body was truncated: {request}"
        );
        server.await.unwrap();
    }
}
