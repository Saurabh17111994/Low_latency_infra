//! Gateway-intent → bridge command mapping (T4a sync forward leg).
//!
//! Maps a verified `POST /v1/intents` payload (BI-EQ sandbox order schema) into the private
//! bridge [`CommandEnvelope`] (place) using exactly the identity separation pinned by
//! [`BridgeExecutionClient::build_order_envelope`](crate::execution::BridgeExecutionClient):
//! `instruction_id` comes from the payload, `execution_attempt_id` is minted fresh per
//! attempt (UUID v4), and `client_order_ref` (Arrow `remarks`, ≤16 safe ASCII) is the
//! deterministic 14-hex hash of `v1|instruction_id|execution_attempt_id` (same rule as the
//! client, 05-execution-core §Reconciliation).
//!
//! The mapping is total: any unsupported value fails closed with `Err`, so the route returns
//! 422 and never forwards a half-mapped order. The payload fields mirror
//! `NautilusIntentClient.sendWithFence()` / the T9 sandbox `bieq_payload()` schema
//! (see `code/01_platform/04_scripts/t9_order_sandbox.py`).

use nautilus_core::UUID4;

use crate::bridge::{
    Command, CommandEnvelope, OrderCommand, OrderType, Product, TransactionType, Validity,
};
use crate::execution::client::deterministic_client_order_ref;

/// Builds a place [`CommandEnvelope`] from a verified gateway payload.
///
/// Supported fields (everything else fails closed):
/// - `instruction_id`, `symbol`, `exchange` — required strings;
/// - `side`: `BUY` | `SELL`;
/// - `order_type`: `LIMIT` | `MARKET` | `SL-LIMIT` | `SL-MARKET`;
/// - `quantity`: positive integer (number or digit string);
/// - `limit_price_paise` (integer, paise): required for `LIMIT`/`SL-LIMIT`, forbidden for
///   `MARKET`/`SL-MARKET` (a client-supplied price on a market order is a payload mistake,
///   e.g. a limit order typo'd as market, so it is rejected here rather than dropped);
/// - `product_type`: `CNC` | `MIS` | `MTM`;
/// - `time_in_force`: `DAY` | `IOC`.
pub fn place_envelope_from_payload(payload: &serde_json::Value) -> Result<CommandEnvelope, String> {
    let instruction_id = required_str(payload, "instruction_id")?;
    let order = order_command_from_payload(payload)?;

    // Fresh attempt id per attempt + deterministic broker-facing ref (identity separation).
    let execution_attempt_id = UUID4::new().to_string();
    let client_order_ref =
        deterministic_client_order_ref("v1", &instruction_id, &execution_attempt_id);
    let mut envelope = CommandEnvelope::new(Command::Place, &UUID4::new().to_string());
    envelope.instruction_id = instruction_id;
    envelope.execution_attempt_id = execution_attempt_id;
    envelope.client_order_ref = client_order_ref;
    envelope.order = Some(order);
    envelope.validate().map_err(|e| e.to_string())?;
    Ok(envelope)
}

/// Builds a cancel [`CommandEnvelope`] from a verified gateway payload
/// (`"action": "cancel"` on `/v1/intents`).
///
/// Required: `instruction_id` (correlation) and `broker_order_id` (the bridge's
/// cancel keys on the broker id — protocol validation rejects a cancel without
/// one). The identity rule mirrors the place leg exactly: fresh UUID v4
/// `execution_attempt_id`, deterministic 14-hex `client_order_ref` over
/// `v1|instruction_id|execution_attempt_id`. No `order` block is attached.
pub fn cancel_envelope_from_payload(
    payload: &serde_json::Value,
) -> Result<CommandEnvelope, String> {
    let instruction_id = required_str(payload, "instruction_id")?;
    let broker_order_id = required_str(payload, "broker_order_id")
        .map_err(|_| "broker_order_id required for cancel".to_string())?;

    let execution_attempt_id = UUID4::new().to_string();
    let client_order_ref =
        deterministic_client_order_ref("v1", &instruction_id, &execution_attempt_id);
    let mut envelope = CommandEnvelope::new(Command::Cancel, &UUID4::new().to_string());
    envelope.instruction_id = instruction_id;
    envelope.execution_attempt_id = execution_attempt_id;
    envelope.client_order_ref = client_order_ref;
    envelope.broker_order_id = broker_order_id;
    envelope.validate().map_err(|e| e.to_string())?;
    Ok(envelope)
}

/// Builds an amend (modify) [`CommandEnvelope`] from a verified gateway payload
/// (`"action": "amend"` on `/v1/intents`).
///
/// Same identity rule as place/cancel (fresh `execution_attempt_id`, deterministic
/// 14-hex `client_order_ref` over `v1|instruction_id|execution_attempt_id`), plus the
/// required `broker_order_id` (the bridge's modify keys on the broker id — protocol
/// validation rejects a modify without one, mirroring `validateCommand` in the bridge:
/// `broker_order_id is required for modify`). The `order` block is the AMENDED order
/// (symbol/exchange/side/order_type/quantity/limit_price_paise/product_type/
/// time_in_force), same schema as place.
pub fn amend_envelope_from_payload(payload: &serde_json::Value) -> Result<CommandEnvelope, String> {
    let instruction_id = required_str(payload, "instruction_id")?;
    let broker_order_id = required_str(payload, "broker_order_id")
        .map_err(|_| "broker_order_id required for amend".to_string())?;
    let order = order_command_from_payload(payload)?;

    let execution_attempt_id = UUID4::new().to_string();
    let client_order_ref =
        deterministic_client_order_ref("v1", &instruction_id, &execution_attempt_id);
    let mut envelope = CommandEnvelope::new(Command::Modify, &UUID4::new().to_string());
    envelope.instruction_id = instruction_id;
    envelope.execution_attempt_id = execution_attempt_id;
    envelope.client_order_ref = client_order_ref;
    envelope.broker_order_id = broker_order_id;
    envelope.order = Some(order);
    envelope.validate().map_err(|e| e.to_string())?;
    Ok(envelope)
}

/// Shared order-block mapping for place/amend (the amended order has the same
/// schema as a fresh order; only the command/`broker_order_id` differ).
fn order_command_from_payload(payload: &serde_json::Value) -> Result<OrderCommand, String> {
    let symbol = required_str(payload, "symbol")?;
    let exchange = required_str(payload, "exchange")?;
    let side = match required_str(payload, "side")?.as_str() {
        "BUY" => TransactionType::Buy,
        "SELL" => TransactionType::Sell,
        other => return Err(format!("unsupported side: {other}")),
    };
    let order_type = match required_str(payload, "order_type")?.as_str() {
        "LIMIT" => OrderType::Lmt,
        "MARKET" => OrderType::Mkt,
        "SL-LIMIT" => OrderType::SlLmt,
        "SL-MARKET" => OrderType::SlMkt,
        other => return Err(format!("unsupported order_type: {other}")),
    };
    let product = match required_str(payload, "product_type")?.as_str() {
        "CNC" => Product::Cash,
        "MIS" => Product::Intraday,
        "MTM" => Product::Monthly,
        other => return Err(format!("unsupported product_type: {other}")),
    };
    let validity = match required_str(payload, "time_in_force")?.as_str() {
        "DAY" => Validity::Day,
        "IOC" => Validity::Ioc,
        other => return Err(format!("unsupported time_in_force: {other}")),
    };
    let quantity = payload
        .get("quantity")
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .map(|q| q.to_string())
        .ok_or_else(|| "quantity required (positive integer)".to_string())?;

    let mut order = OrderCommand::new(&exchange, &symbol)
        .with_quantity(&quantity)
        .with_side(side)
        .with_order_type(order_type)
        .with_product(product)
        .with_validity(validity);
    // P3-210: a priced MARKET/SL-MARKET payload is a client mistake — reject it instead of
    // silently dropping the price (an explicit JSON null counts as absent).
    if matches!(order_type, OrderType::Mkt | OrderType::SlMkt)
        && payload
            .get("limit_price_paise")
            .is_some_and(|v| !v.is_null())
    {
        return Err("limit_price_paise not allowed for MARKET/SL-MARKET".to_string());
    }
    if matches!(order_type, OrderType::Lmt | OrderType::SlLmt) {
        let price_paise = match payload.get("limit_price_paise").filter(|v| !v.is_null()) {
            None => return Err("limit_price_paise required for LIMIT/SL-LIMIT".to_string()),
            // P3-455: a supplied price that is not an integer is a type error, not a missing one.
            Some(v) => v
                .as_i64()
                .ok_or_else(|| "limit_price_paise must be an integer".to_string())?,
        };
        order = order.with_price(&price_paise.to_string());
    }
    Ok(order)
}

fn required_str(payload: &serde_json::Value, key: &str) -> Result<String, String> {
    payload
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| format!("{key} required"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn bieq() -> serde_json::Value {
        json!({
            "instruction_id": "T9-SB-0001",
            "candidate_id": "cand-T9-0001",
            "trade_context_id": "tc-T9-0001",
            "instrument_token": 762583,
            "symbol": "BI-EQ",
            "exchange": "NSE",
            "side": "BUY",
            "quantity": 1,
            "order_type": "LIMIT",
            "limit_price_paise": 5050,
            "product_type": "CNC",
            "time_in_force": "DAY",
        })
    }

    #[test]
    fn maps_bieq_payload_to_place_envelope() {
        let env = place_envelope_from_payload(&bieq()).expect("BI-EQ payload maps");
        assert_eq!(env.command, "place");
        assert_eq!(env.instruction_id, "T9-SB-0001");
        assert!(!env.execution_attempt_id.is_empty());
        // Deterministic 14-hex ref fits the Arrow 16-char remarks limit.
        assert_eq!(env.client_order_ref.len(), 14);
        assert!(env.client_order_ref.chars().all(|c| c.is_ascii_hexdigit()));
        let order = env.order.expect("order mapped");
        assert_eq!(order.exchange, "NSE");
        assert_eq!(order.symbol, "BI-EQ");
        assert_eq!(order.quantity, "1");
        assert_eq!(order.price, "5050");
        let v = serde_json::to_value(&order).unwrap();
        assert_eq!(v["transaction_type"], "BUY");
        assert_eq!(v["order_type"], "LMT");
        assert_eq!(v["product"], "C");
        assert_eq!(v["validity"], "DAY");
    }

    #[test]
    fn sells_with_mis_and_ioc_map() {
        let payload = json!({
            "instruction_id": "T9-SB-0002",
            "symbol": "BI-EQ", "exchange": "NSE", "side": "SELL",
            "quantity": 2, "order_type": "MARKET",
            "product_type": "MIS", "time_in_force": "IOC",
        });
        let env = place_envelope_from_payload(&payload).expect("market sell maps");
        let order = env.order.expect("order mapped");
        let v = serde_json::to_value(&order).unwrap();
        assert_eq!(v["transaction_type"], "SELL");
        assert_eq!(v["order_type"], "MKT");
        assert_eq!(v["product"], "I");
        // Market orders must not carry a price.
        assert!(order.price.is_empty(), "market price must be empty");
    }

    #[test]
    fn rejects_missing_instruction() {
        let mut p = bieq();
        p.as_object_mut().unwrap().remove("instruction_id");
        let err = place_envelope_from_payload(&p).unwrap_err();
        assert!(err.contains("instruction_id"), "err: {err}");
    }

    #[test]
    fn rejects_unsupported_side_and_product() {
        let mut p = bieq();
        p["side"] = json!("HEDGE");
        assert!(place_envelope_from_payload(&p)
            .unwrap_err()
            .contains("side"));
        let mut p = bieq();
        p["product_type"] = json!("XX");
        assert!(place_envelope_from_payload(&p)
            .unwrap_err()
            .contains("product_type"));
    }

    #[test]
    fn limit_requires_price_and_positive_quantity() {
        let mut p = bieq();
        p.as_object_mut().unwrap().remove("limit_price_paise");
        assert!(place_envelope_from_payload(&p)
            .unwrap_err()
            .contains("price"));
        let mut p = bieq();
        p["quantity"] = json!(0);
        let err = place_envelope_from_payload(&p).unwrap_err();
        assert!(err.contains("quantity"), "err: {err}");
    }

    #[test]
    fn priced_market_payload_is_rejected() {
        let mut p = bieq();
        p["order_type"] = json!("MARKET");
        // A leftover price on a market payload is a payload mistake (e.g. a typo'd limit order):
        // the mapper rejects it instead of silently dropping the price.
        let err = place_envelope_from_payload(&p)
            .expect_err("priced MARKET must not map to an unpriced MKT order");
        assert!(err.contains("limit_price_paise"), "err: {err}");
        assert!(err.contains("not allowed"), "err: {err}");
    }

    #[test]
    fn deterministic_ref_matches_client_rule() {
        let env = place_envelope_from_payload(&bieq()).unwrap();
        assert_eq!(
            env.client_order_ref,
            deterministic_client_order_ref("v1", "T9-SB-0001", &env.execution_attempt_id)
        );
    }

    fn cancel_payload() -> serde_json::Value {
        json!({
            "action": "cancel",
            "instruction_id": "T9-SB-0001",
            "broker_order_id": "BRK-0001",
        })
    }

    #[test]
    fn maps_cancel_payload_to_cancel_envelope() {
        let env = cancel_envelope_from_payload(&cancel_payload()).expect("cancel payload maps");
        assert_eq!(env.command, "cancel");
        assert_eq!(env.instruction_id, "T9-SB-0001");
        assert_eq!(env.broker_order_id, "BRK-0001");
        // Same deterministic identity rule as the place leg.
        assert_eq!(env.client_order_ref.len(), 14);
        assert!(env.client_order_ref.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(
            env.client_order_ref,
            deterministic_client_order_ref("v1", "T9-SB-0001", &env.execution_attempt_id)
        );
        assert!(env.order.is_none(), "cancel carries no order block");
    }

    #[test]
    fn cancel_requires_broker_order_id() {
        let mut p = cancel_payload();
        p.as_object_mut().unwrap().remove("broker_order_id");
        let err = cancel_envelope_from_payload(&p).unwrap_err();
        assert!(err.contains("broker_order_id"), "err: {err}");
    }

    #[test]
    fn cancel_requires_instruction_for_correlation() {
        let mut p = cancel_payload();
        p.as_object_mut().unwrap().remove("instruction_id");
        let err = cancel_envelope_from_payload(&p).unwrap_err();
        assert!(err.contains("instruction_id"), "err: {err}");
    }

    fn amend_payload() -> serde_json::Value {
        let mut p = bieq();
        p["action"] = serde_json::json!("amend");
        p["broker_order_id"] = serde_json::json!("BRK-0001");
        p["limit_price_paise"] = serde_json::json!(5090); // amended price
        p
    }

    #[test]
    fn maps_amend_payload_to_modify_envelope() {
        let env = amend_envelope_from_payload(&amend_payload()).expect("amend payload maps");
        assert_eq!(env.command, "modify");
        assert_eq!(env.instruction_id, "T9-SB-0001");
        assert_eq!(env.broker_order_id, "BRK-0001");
        // Same deterministic identity rule as place/cancel.
        assert_eq!(env.client_order_ref.len(), 14);
        assert!(env.client_order_ref.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(
            env.client_order_ref,
            deterministic_client_order_ref("v1", "T9-SB-0001", &env.execution_attempt_id)
        );
        // The order block is the AMENDED order (same schema as place).
        let order = env.order.expect("amend carries the amended order block");
        assert_eq!(order.exchange, "NSE");
        assert_eq!(order.symbol, "BI-EQ");
        assert_eq!(order.quantity, "1");
        assert_eq!(order.price, "5090");
    }

    #[test]
    fn amend_requires_broker_order_id() {
        let mut p = amend_payload();
        p.as_object_mut().unwrap().remove("broker_order_id");
        let err = amend_envelope_from_payload(&p).unwrap_err();
        assert!(err.contains("broker_order_id"), "err: {err}");
    }

    #[test]
    fn amend_requires_instruction_for_correlation() {
        let mut p = amend_payload();
        p.as_object_mut().unwrap().remove("instruction_id");
        let err = amend_envelope_from_payload(&p).unwrap_err();
        assert!(err.contains("instruction_id"), "err: {err}");
    }
}

/// P3-210: a payload must not carry a price for a market order. The plain `MARKET` case is
/// covered by `tests::priced_market_payload_is_rejected` (the rewritten former drop-assert test);
/// this module pins the `SL-MARKET` case, where the rejection must be the price error and not the
/// downstream stop-trigger error.
#[cfg(test)]
mod p3_210_tests {
    use super::*;
    use serde_json::json;

    fn market_payload(order_type: &str, price: serde_json::Value) -> serde_json::Value {
        json!({
            "instruction_id": "T9-SB-0001",
            "symbol": "BI-EQ",
            "exchange": "NSE",
            "side": "BUY",
            "quantity": 1,
            "order_type": order_type,
            "limit_price_paise": price,
            "product_type": "CNC",
            "time_in_force": "DAY",
        })
    }

    #[test]
    fn rejects_priced_sl_market_payload() {
        let err = place_envelope_from_payload(&market_payload("SL-MARKET", json!(5050)))
            .expect_err("priced SL-MARKET must be rejected, not accepted with the price dropped");
        assert!(err.contains("limit_price_paise"), "err: {err}");
        assert!(err.contains("not allowed"), "err: {err}");
    }
}

#[cfg(test)]
mod p3_455_tests {
    use super::*;
    use serde_json::json;

    /// A minimal order payload; `price` is omitted when `None`.
    fn limit_payload(order_type: &str, price: Option<serde_json::Value>) -> serde_json::Value {
        let mut p = json!({
            "instruction_id": "T9-SB-0001",
            "symbol": "BI-EQ",
            "exchange": "NSE",
            "side": "BUY",
            "quantity": 1,
            "order_type": order_type,
            "product_type": "CNC",
            "time_in_force": "DAY",
        });
        if let Some(price) = price {
            p["limit_price_paise"] = price;
        }
        p
    }

    #[test]
    fn missing_price_error_names_both_limit_types() {
        let err = place_envelope_from_payload(&limit_payload("SL-LIMIT", None)).unwrap_err();
        assert!(
            err.contains("limit_price_paise required for LIMIT/SL-LIMIT"),
            "err: {err}"
        );
    }

    #[test]
    fn non_integer_price_reports_type_not_missing() {
        // Digit string and beyond-i64 number: both are supplied but not an integer.
        for price in [json!("5050"), json!(9_223_372_036_854_775_808u64)] {
            let err = place_envelope_from_payload(&limit_payload("LIMIT", Some(price.clone())))
                .unwrap_err();
            assert!(
                err.contains("limit_price_paise must be an integer"),
                "price {price}: err: {err}"
            );
        }
    }
}
