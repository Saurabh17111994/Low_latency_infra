// Package main — broker HTTP-code classification (offline, no Arrow, no Fluss).
//
// Dossier Reconciliation § (05-execution-core.md): verified shapes for bridge PlaceOrder
// response envelope:
//
//	HTTP 200 + status:"success" (or success:true) + nonblank data.orderNo/brokerOrderId → ACCEPTED
//	HTTP 400/409/422 + status:"error" (or success:false) + nonblank message/errorMessage → REJECTED
//	HTTP 401/403/408/429/5xx, transport failure, missing body, malformed JSON, or any other
//	combination → AMBIGUOUS/UNKNOWN → HALT, never retry
//
// This file provides a pure function ClassifyBrokerResponse that implements the table
// without I/O, without the Arrow SDK, and without Fluss. It is used by broker.go's
// classifySDKError and is directly testable via go test -run TestClassify / TestBridgeClassification.
//
// Mapping table implemented:
//
//	| HTTP code | body shape                                   | outcome   | halt/retry |
//	|-----------|----------------------------------------------|-----------|------------|
//	| 200       | status:"success" + data.orderNo nonblank      | ACCEPTED  | — (success)|
//	| 200       | missing/whitespace orderNo or not success     | UNKNOWN   | HALT       |
//	| 400,409,422 | status:"error" + message nonblank          | REJECTED  | — (terminal)|
//	| 400,409,422 | empty message or status:"success"          | UNKNOWN   | HALT       |
//	| 401,403,408,429,5xx, transport, empty, malformed, other| UNKNOWN   | HALT       |
//	| both signals present and disagreeing (e.g. status:"success" + success:false)| UNKNOWN | HALT |
//
// Never returns SUCCESS from the error path; UNKNOWN is always HALT without retry.
package main

import (
	"encoding/json"
	"strings"
)

// classificationEnvelope captures the minimal JSON fields needed for the dossier
// table. It intentionally covers both string status and boolean success shapes,
// and both message / errorMessage variants, plus top-level and data-wrapped
// order identifiers.
type classificationEnvelope struct {
	Status       string          `json:"status"`
	Success      *bool           `json:"success"`
	Message      string          `json:"message"`
	ErrorMessage string          `json:"errorMessage"`
	ErrorMsgAlt  string          `json:"error_message"`
	Data         json.RawMessage `json:"data"`
	// Some brokers echo orderNo at top level; accept either location.
	OrderNo       string `json:"orderNo"`
	BrokerOrderId string `json:"brokerOrderId"`
	BrokerOrderID string `json:"broker_order_id"`
}

type classificationData struct {
	OrderNo       string `json:"orderNo"`
	BrokerOrderId string `json:"brokerOrderId"`
	BrokerOrderID string `json:"broker_order_id"`
	OrderNoAlt    string `json:"order_no"`
	OrderId       string `json:"orderId"`
	OrderIDAlt    string `json:"order_id"`
}

// ClassifyBrokerResponse is a pure, offline classifier for Arrow broker HTTP responses.
// statusCode is the HTTP status (0 means transport failure / no response).
// body may be nil, []byte, string, map[string]interface{}, or any struct marshalable to JSON.
// Returns one of OutcomeSuccess (ACCEPTED), OutcomeRejected, or OutcomeUnknown.
// UNKNOWN means AMBIGUOUS → HALT, never retry.
func ClassifyBrokerResponse(statusCode int, body interface{}) string {
	if body == nil {
		return OutcomeUnknown
	}
	var raw []byte
	switch v := body.(type) {
	case []byte:
		raw = v
	case string:
		raw = []byte(v)
	case json.RawMessage:
		raw = []byte(v)
	default:
		// map, struct, etc. — marshal to JSON for uniform parsing.
		b, err := json.Marshal(v)
		if err != nil {
			return OutcomeUnknown
		}
		raw = b
	}
	// Trim whitespace; empty body is UNKNOWN (missing body case).
	if len(strings.TrimSpace(string(raw))) == 0 {
		return OutcomeUnknown
	}
	// Strict JSON: malformed JSON is UNKNOWN.
	var env classificationEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return OutcomeUnknown
	}

	// P3-040: a self-contradictory envelope — explicit status and explicit success
	// flag that disagree — is malformed per the table above, so it resolves to
	// UNKNOWN/HALT rather than to either terminal outcome. Trusting the success
	// side would let a rejection read as acceptance; trusting the error side would
	// let an accepted order read as rejected, which invites a duplicate placement.
	if signalsContradict(env) {
		return OutcomeUnknown
	}

	// 200 → ACCEPTED only when success + orderNo nonblank.
	if statusCode == 200 {
		if !isSuccessEnvelope(env) {
			return OutcomeUnknown
		}
		if strings.TrimSpace(extractOrderNo(env)) != "" {
			return OutcomeSuccess
		}
		return OutcomeUnknown
	}

	// 400 / 409 / 422 → REJECTED only when error + message nonblank.
	// All other codes (including 401/403/408/429/5xx) are UNKNOWN by definition
	// even if they carry an error payload — they are retry-ambiguous.
	if statusCode == 400 || statusCode == 409 || statusCode == 422 {
		if !isErrorEnvelope(env) {
			return OutcomeUnknown
		}
		if strings.TrimSpace(extractRejectionMessage(env)) != "" {
			return OutcomeRejected
		}
		return OutcomeUnknown
	}

	return OutcomeUnknown
}

func isSuccessEnvelope(env classificationEnvelope) bool {
	if strings.EqualFold(strings.TrimSpace(env.Status), "success") {
		return true
	}
	if env.Success != nil && *env.Success {
		return true
	}
	return false
}

// signalsContradict reports whether the envelope carries both an explicit status
// and an explicit success flag that disagree. A single signal, an unfamiliar
// status wording, or an absent field is not a contradiction.
func signalsContradict(env classificationEnvelope) bool {
	if env.Success == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(env.Status)) {
	case "success":
		return !*env.Success
	case "error":
		return *env.Success
	default:
		return false
	}
}

func isErrorEnvelope(env classificationEnvelope) bool {
	if strings.EqualFold(strings.TrimSpace(env.Status), "error") {
		return true
	}
	if env.Success != nil && !*env.Success {
		return true
	}
	return false
}

func extractOrderNo(env classificationEnvelope) string {
	// Top-level shortcuts (some SDKs echo orderNo at top level).
	if s := strings.TrimSpace(env.OrderNo); s != "" {
		return s
	}
	if s := strings.TrimSpace(env.BrokerOrderId); s != "" {
		return s
	}
	if s := strings.TrimSpace(env.BrokerOrderID); s != "" {
		return s
	}
	if len(env.Data) == 0 || string(env.Data) == "null" {
		return ""
	}
	// Data is typically an object; try typed unmarshal first.
	var d classificationData
	if err := json.Unmarshal(env.Data, &d); err == nil {
		if s := strings.TrimSpace(d.OrderNo); s != "" {
			return s
		}
		if s := strings.TrimSpace(d.BrokerOrderId); s != "" {
			return s
		}
		if s := strings.TrimSpace(d.BrokerOrderID); s != "" {
			return s
		}
		if s := strings.TrimSpace(d.OrderNoAlt); s != "" {
			return s
		}
		if s := strings.TrimSpace(d.OrderId); s != "" {
			return s
		}
		if s := strings.TrimSpace(d.OrderIDAlt); s != "" {
			return s
		}
	}
	// Fallback generic map for unknown key variants.
	var m map[string]interface{}
	if err := json.Unmarshal(env.Data, &m); err == nil {
		for _, k := range []string{"orderNo", "brokerOrderId", "broker_order_id", "order_no", "orderId", "order_id"} {
			if v, ok := m[k]; ok {
				if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
					return strings.TrimSpace(s)
				}
			}
		}
	}
	// Data may itself be a quoted string containing JSON (defensive).
	trimmed := strings.TrimSpace(string(env.Data))
	if len(trimmed) >= 2 && trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"' {
		var inner string
		if err := json.Unmarshal(env.Data, &inner); err == nil && strings.TrimSpace(inner) != "" {
			return strings.TrimSpace(inner)
		}
	}
	return ""
}

func extractRejectionMessage(env classificationEnvelope) string {
	if s := strings.TrimSpace(env.Message); s != "" {
		return s
	}
	if s := strings.TrimSpace(env.ErrorMessage); s != "" {
		return s
	}
	if s := strings.TrimSpace(env.ErrorMsgAlt); s != "" {
		return s
	}
	return ""
}
