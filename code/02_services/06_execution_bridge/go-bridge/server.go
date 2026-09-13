package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	commandPath = "/v1/commands"
	eventsPath  = "/v1/events"
	healthPath  = "/healthz"
	readyPath   = "/readyz"
)

type BridgeServer struct {
	broker         Broker
	authToken      string
	commandTimeout time.Duration
	mode           string
	hub            *EventHub
	requestMu      sync.Mutex
	requests       map[string]*requestState
}

type requestState struct {
	fingerprint string
	done        chan struct{}
	report      ReportEnvelope
}

func NewBridgeServer(broker Broker, authToken, mode string) (*BridgeServer, error) {
	if broker == nil {
		return nil, errors.New("broker is required")
	}
	if strings.TrimSpace(authToken) == "" {
		return nil, errors.New("private bridge auth token is required")
	}
	timeout, err := commandTimeoutFromEnv()
	if err != nil {
		return nil, err
	}
	return &BridgeServer{
		broker: broker, authToken: authToken, mode: mode,
		commandTimeout: timeout, hub: NewEventHub(),
		requests: make(map[string]*requestState),
	}, nil
}

// commandTimeoutFromEnv reads EXECUTION_BRIDGE_COMMAND_TIMEOUT_MS (K4,
// 2026-08-29). Unset defaults to 10s; a present value must parse as a
// positive integer in 100..600000 ms — a zero/negative timeout would hang
// command dispatch forever.
func commandTimeoutFromEnv() (time.Duration, error) {
	const defaultMs = 10_000
	v := os.Getenv("EXECUTION_BRIDGE_COMMAND_TIMEOUT_MS")
	if v == "" {
		return time.Duration(defaultMs) * time.Millisecond, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 100 || n > 600_000 {
		return 0, fmt.Errorf("EXECUTION_BRIDGE_COMMAND_TIMEOUT_MS=%q must be an integer in 100..600000", v)
	}
	return time.Duration(n) * time.Millisecond, nil
}

func (s *BridgeServer) Handler() http.Handler { return http.HandlerFunc(s.serveHTTP) }

func (s *BridgeServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == healthPath && r.Method == http.MethodGet:
		s.writeHealth(w, false)
	case r.URL.Path == readyPath && r.Method == http.MethodGet:
		s.writeHealth(w, true)
	case r.URL.Path == commandPath && r.Method == http.MethodPost:
		if !s.authorized(r) {
			s.writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		s.handleCommand(w, r)
	case r.URL.Path == eventsPath && r.Method == http.MethodGet:
		if !s.authorized(r) {
			s.writeError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		s.handleEvents(w, r)
	default:
		s.writeError(w, http.StatusNotFound, "not_found")
	}
}

func (s *BridgeServer) authorized(r *http.Request) bool {
	const prefix = "Bearer "
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	got := []byte(strings.TrimSpace(strings.TrimPrefix(header, prefix)))
	want := []byte(s.authToken)
	return len(got) == len(want) && subtle.ConstantTimeCompare(got, want) == 1
}

func (s *BridgeServer) handleCommand(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 128*1024)
	defer r.Body.Close()
	var command CommandEnvelope
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&command); err != nil {
		s.writeError(w, http.StatusBadRequest, "malformed_command")
		return
	}
	if err := ensureEOF(decoder); err != nil {
		s.writeError(w, http.StatusBadRequest, "malformed_command")
		return
	}
	if err := validateCommand(command); err != nil {
		s.writeError(w, http.StatusBadRequest, "invalid_command: "+err.Error())
		return
	}
	state, owner, conflict, err := s.beginRequest(command)
	if err != nil {
		// P3-474: a command that cannot be digested cannot be deduplicated safely,
		// so it never reaches the broker.
		s.writeError(w, http.StatusInternalServerError, "fingerprint_failed")
		return
	}
	if conflict {
		s.writeError(w, http.StatusConflict, "request_id_reuse_violation")
		return
	}
	if !owner {
		<-state.done
		s.writeJSON(w, http.StatusOK, state.report)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.commandTimeout)
	defer cancel()
	result := s.dispatch(ctx, command)
	report := resultToReport(command, result)
	s.finishRequest(state, report)
	s.writeJSON(w, http.StatusOK, report)
}

func (s *BridgeServer) beginRequest(command CommandEnvelope) (*requestState, bool, bool, error) {
	fp, err := fingerprint(command)
	if err != nil {
		return nil, false, false, err
	}
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	if existing, ok := s.requests[command.RequestID]; ok {
		if existing.fingerprint != fp {
			return nil, false, true, nil
		}
		return existing, false, false, nil
	}
	state := &requestState{fingerprint: fp, done: make(chan struct{})}
	s.requests[command.RequestID] = state
	return state, true, false, nil
}

func (s *BridgeServer) finishRequest(state *requestState, report ReportEnvelope) {
	s.requestMu.Lock()
	state.report = report
	close(state.done)
	// Retain completed request identities for this process lifetime. A restart
	// deliberately loses this cache; durable attempt reconciliation remains the
	// authority for crash recovery.
	s.requestMu.Unlock()
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func (s *BridgeServer) dispatch(ctx context.Context, c CommandEnvelope) BrokerResult {
	switch c.Command {
	case CommandPlace:
		return s.broker.Place(ctx, c)
	case CommandModify:
		return s.broker.Modify(ctx, c)
	case CommandCancel:
		return s.broker.Cancel(ctx, c)
	case CommandQueryOrder:
		return s.broker.QueryOrder(ctx, c)
	case CommandReconcileOrders:
		return s.broker.ReconcileOrders(ctx, c)
	case CommandReconcileTrades:
		return s.broker.ReconcileTrades(ctx, c)
	case CommandReconcilePosition:
		return s.broker.ReconcilePositions(ctx, c)
	default:
		return unknownResult(fmt.Errorf("unsupported command %q", c.Command))
	}
}

func resultToReport(c CommandEnvelope, result BrokerResult) ReportEnvelope {
	report := ReportEnvelope{
		RecordType: RecordReport, ContractVersion: ProtocolVersion,
		RequestID: c.RequestID, Command: c.Command, Outcome: result.Outcome,
		Reason: result.Reason, InstructionID: c.InstructionID,
		ExecutionAttemptID: c.ExecutionAttemptID, ClientOrderRef: c.ClientOrderRef,
		BrokerOrderID: result.BrokerOrderID, ExchangeOrderID: result.ExchangeOrderID,
		OrderStatus: result.OrderStatus, ReportType: result.ReportType,
		ReceivedTsMs: nowMs(), ResponseFingerprint: result.Fingerprint,
	}
	if result.Data != nil {
		data, err := json.Marshal(result.Data)
		if err != nil {
			// P3-264: a payload the bridge cannot serialize must not become a
			// SUCCESS with Data quietly missing — downstream reconcile would read
			// the gap as "nothing to reconcile". Fail closed and keep the
			// correlation fields, so the failure is still reconcilable.
			return ReportEnvelope{
				RecordType: RecordReport, ContractVersion: ProtocolVersion,
				RequestID: c.RequestID, Command: c.Command,
				Outcome: OutcomeUnknown, Reason: "ambiguous_broker_response",
				InstructionID: c.InstructionID, ExecutionAttemptID: c.ExecutionAttemptID,
				ClientOrderRef: c.ClientOrderRef, ReceivedTsMs: nowMs(),
			}
		}
		report.Data = data
	}
	return report
}

func (s *BridgeServer) writeHealth(w http.ResponseWriter, readiness bool) {
	brokerDisabled := false
	if rb, ok := s.broker.(*ReauthBroker); ok && rb.IsDisabled() {
		brokerDisabled = true
	}
	value := map[string]any{
		"record_type":            "execution_bridge_health",
		"contract_version":       ProtocolVersion,
		"status":                 "UP",
		"mode":                   s.mode,
		"credentials_in_process": s.mode == "live",
		"arrow_route_in_process": s.mode == "live",
	}
	if brokerDisabled {
		value["status"] = "UP disabled"
	}
	if readiness {
		// A disabled bridge is intentionally healthy but not ready for broker
		// commands. Live enablement is a separate gate owned by the execution
		// service, not implied by process readiness.
		value["ready"] = s.mode != "disabled" && !brokerDisabled
		if s.mode == "disabled" || brokerDisabled {
			value["reason"] = "broker_disabled"
		}
	}
	status := http.StatusOK
	// P3-054: only readiness may fail on a disabled broker. A disabled bridge is
	// alive and answers liveness — failing /healthz restarted it, and a restart
	// cannot restore a token that needs a TOTP re-auth, so it restarted forever.
	if readiness && (value["status"] == "UP disabled" || s.mode == "disabled") {
		status = http.StatusServiceUnavailable
	}
	s.writeJSON(w, status, value)
}

func (s *BridgeServer) writeError(w http.ResponseWriter, status int, reason string) {
	s.writeJSON(w, status, map[string]any{
		"record_type": RecordReport, "contract_version": ProtocolVersion,
		"outcome": OutcomeUnknown, "reason": reason, "received_ts_ms": nowMs(),
	})
}

func (s *BridgeServer) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

var websocketUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin: func(r *http.Request) bool {
		// This endpoint is private and bearer-authenticated. Reject browser
		// origins rather than turning the service into a cross-origin API.
		return r.Header.Get("Origin") == ""
	},
}

func (s *BridgeServer) handleEvents(w http.ResponseWriter, r *http.Request) {
	conn, err := websocketUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	sub := s.hub.Subscribe()
	defer sub.Close()
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	})
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-closed:
			return
		case payload, ok := <-sub.Events:
			if !ok {
				return
			}
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}
		case <-ping.C:
			_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

type EventHub struct {
	mu          sync.Mutex
	nextID      uint64
	subscribers map[uint64]*EventSubscription
}

type EventSubscription struct {
	Events chan []byte
	hub    *EventHub
	id     uint64
	once   sync.Once
}

func NewEventHub() *EventHub { return &EventHub{subscribers: map[uint64]*EventSubscription{}} }

func (h *EventHub) Subscribe() *EventSubscription {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextID++
	sub := &EventSubscription{Events: make(chan []byte, 32), hub: h, id: h.nextID}
	h.subscribers[sub.id] = sub
	return sub
}

func (s *EventSubscription) Close() {
	s.once.Do(func() {
		s.hub.mu.Lock()
		if _, ok := s.hub.subscribers[s.id]; ok {
			delete(s.hub.subscribers, s.id)
			close(s.Events)
		}
		s.hub.mu.Unlock()
	})
}

// Publish is fail-closed for slow consumers: a full queue is removed rather
// than silently dropping an execution report.
func (h *EventHub) Publish(report ReportEnvelope) error {
	payload, err := json.Marshal(report)
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, sub := range h.subscribers {
		select {
		case sub.Events <- append([]byte(nil), payload...):
		default:
			delete(h.subscribers, id)
			close(sub.Events)
		}
	}
	return nil
}
