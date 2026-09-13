package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const (
	ProtocolVersion = 1
	RecordCommand   = "execution_command"
	RecordReport    = "execution_report"
	OutcomeSuccess  = "SUCCESS"
	OutcomeRejected = "REJECTED"
	OutcomeUnknown  = "UNKNOWN"
)

const (
	CommandPlace             = "place"
	CommandModify            = "modify"
	CommandCancel            = "cancel"
	CommandQueryOrder        = "query-order"
	CommandReconcileOrders   = "reconcile-orders"
	CommandReconcileTrades   = "reconcile-trades"
	CommandReconcilePosition = "reconcile-positions"
)

// CommandEnvelope is the private protocol between Nautilus and this bridge.
// It deliberately carries platform identities separately; broker_order_id is
// only present after Arrow has assigned it.
//
// Identity requiredness is scoped to the command, not to the struct tag
// (P3-250): place/modify require instruction_id, execution_attempt_id and
// client_order_ref, cancel/query-order require only broker_order_id, and the
// reconcile commands require none of them. The empty=>absent wire shape is
// deliberate and symmetric with the executor's Rust peer, which declares the
// same fields with skip_serializing_if = "String::is_empty" and re-checks
// requiredness per command. omitempty therefore describes the wire, not
// optionality.
type CommandEnvelope struct {
	RecordType         string        `json:"record_type"`
	ContractVersion    int           `json:"contract_version"`
	RequestID          string        `json:"request_id"`
	Command            string        `json:"command"`
	InstructionID      string        `json:"instruction_id,omitempty"`
	ExecutionAttemptID string        `json:"execution_attempt_id,omitempty"`
	ClientOrderRef     string        `json:"client_order_ref,omitempty"`
	BrokerOrderID      string        `json:"broker_order_id,omitempty"`
	Order              *OrderCommand `json:"order,omitempty"`
}

// OrderCommand uses platform-neutral values. The adapter is the only place
// that converts them into Arrow's string-valued request fields.
type OrderCommand struct {
	Exchange         string `json:"exchange"`
	Symbol           string `json:"symbol"`
	Quantity         string `json:"quantity"`
	TransactionType  string `json:"transaction_type"`
	OrderType        string `json:"order_type"`
	Product          string `json:"product"`
	Price            string `json:"price,omitempty"`
	Validity         string `json:"validity"`
	MarketProtection bool   `json:"market_protection,omitempty"`
}

// ReportEnvelope is returned synchronously for commands and is also sent on
// the private event WebSocket for asynchronous Arrow order updates.
type ReportEnvelope struct {
	RecordType          string          `json:"record_type"`
	ContractVersion     int             `json:"contract_version"`
	RequestID           string          `json:"request_id,omitempty"`
	Command             string          `json:"command"`
	Outcome             string          `json:"outcome"`
	Reason              string          `json:"reason,omitempty"`
	InstructionID       string          `json:"instruction_id,omitempty"`
	ExecutionAttemptID  string          `json:"execution_attempt_id,omitempty"`
	ClientOrderRef      string          `json:"client_order_ref,omitempty"`
	BrokerOrderID       string          `json:"broker_order_id,omitempty"`
	ExchangeOrderID     string          `json:"exchange_order_id,omitempty"`
	PostbackEventID     string          `json:"postback_event_id,omitempty"`
	OrderStatus         string          `json:"order_status,omitempty"`
	ReportType          string          `json:"report_type,omitempty"`
	FillShares          string          `json:"fill_shares,omitempty"`
	AveragePrice        string          `json:"average_price,omitempty"`
	FillPrice           string          `json:"fill_price,omitempty"`
	FillQuantity        string          `json:"fill_quantity,omitempty"`
	FillTime            string          `json:"fill_time,omitempty"`
	InstrumentToken     string          `json:"instrument_token,omitempty"`
	ReceivedTsMs        int64           `json:"received_ts_ms"`
	ResponseFingerprint string          `json:"response_fingerprint,omitempty"`
	Data                json.RawMessage `json:"data,omitempty"`
}

var clientRefPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,16}$`)

func validateClientOrderRef(ref string) error {
	if !clientRefPattern.MatchString(ref) {
		return fmt.Errorf("client_order_ref must contain 1-16 ASCII letters, digits, '.', '_' or '-'")
	}
	return nil
}

func validateCommand(c CommandEnvelope) error {
	if c.RecordType != RecordCommand {
		return fmt.Errorf("record_type must be %q", RecordCommand)
	}
	if c.ContractVersion != ProtocolVersion {
		return fmt.Errorf("unsupported contract_version %d", c.ContractVersion)
	}
	if strings.TrimSpace(c.RequestID) == "" {
		return fmt.Errorf("request_id is required")
	}
	switch c.Command {
	case CommandPlace, CommandModify:
		if strings.TrimSpace(c.InstructionID) == "" {
			return fmt.Errorf("instruction_id is required for %s", c.Command)
		}
		if strings.TrimSpace(c.ExecutionAttemptID) == "" {
			return fmt.Errorf("execution_attempt_id is required for %s", c.Command)
		}
		if c.Order == nil {
			return fmt.Errorf("order is required for %s", c.Command)
		}
		if c.Command == CommandPlace && strings.TrimSpace(c.BrokerOrderID) != "" {
			return fmt.Errorf("broker_order_id is not allowed for place")
		}
		if c.Command == CommandModify {
			if err := validateBrokerOrderID(c.BrokerOrderID, c.Command); err != nil {
				return err
			}
		}
		if err := validateOrderCommand(*c.Order); err != nil {
			return err
		}
		if err := validateClientOrderRef(c.ClientOrderRef); err != nil {
			return err
		}
	case CommandCancel, CommandQueryOrder:
		if err := validateBrokerOrderID(c.BrokerOrderID, c.Command); err != nil {
			return err
		}
		// P3-473: an order body on a broker_order_id-keyed command is a caller
		// mistake (typically a payload meant for place). Reject it instead of
		// silently ignoring the field.
		if c.Order != nil {
			return fmt.Errorf("order is not allowed for %s", c.Command)
		}
	case CommandReconcileOrders, CommandReconcileTrades, CommandReconcilePosition:
		// P3-473: reconciles correlate by platform identity only; an order body or a
		// broker order id here means the caller mismatched the command.
		if c.Order != nil {
			return fmt.Errorf("order is not allowed for %s", c.Command)
		}
		if strings.TrimSpace(c.BrokerOrderID) != "" {
			return fmt.Errorf("broker_order_id is not allowed for %s", c.Command)
		}
	default:
		return fmt.Errorf("unsupported command %q", c.Command)
	}
	return nil
}

func validateOrderCommand(o OrderCommand) error {
	// P3-044: compare the trimmed value — comparing the raw string let " INDEX "
	// pass the emptiness check and then fail EqualFold, so an index slipped through
	// as executable.
	exchange := strings.TrimSpace(o.Exchange)
	if exchange == "" || strings.EqualFold(exchange, "INDEX") {
		return fmt.Errorf("execution exchange must be non-empty and not INDEX")
	}
	// P3-253: validation matched these fields case-insensitively after trimming,
	// but the adapter upper-cases them without trimming and forwards the exchange
	// verbatim — so a padded value passed validation and reached Arrow padded.
	for _, field := range []struct{ name, value string }{
		{"exchange", o.Exchange},
		{"order_type", o.OrderType},
		{"product", o.Product},
		{"validity", o.Validity},
	} {
		if containsSpace(field.value) {
			return fmt.Errorf("%s must not contain whitespace", field.name)
		}
	}
	// P3-252: the symbol is forwarded verbatim, so whitespace and unbounded
	// length used to reach Arrow as part of an otherwise well-formed order.
	if o.Symbol == "" || containsSpace(o.Symbol) {
		return fmt.Errorf("order symbol is required and must not contain whitespace")
	}
	if len(o.Symbol) > maxSymbolLength {
		return fmt.Errorf("order symbol is limited to %d characters", maxSymbolLength)
	}
	if strings.TrimSpace(o.Quantity) == "" {
		return fmt.Errorf("order quantity is required")
	}
	if !allDigits(o.Quantity) || strings.TrimLeft(o.Quantity, "0") == "" {
		return fmt.Errorf("quantity must be a positive integer string")
	}
	// P3-252: a quantity the bridge cannot represent must fail here rather than
	// after a venue round trip.
	if _, err := strconv.ParseInt(o.Quantity, 10, 64); err != nil {
		return fmt.Errorf("quantity is out of range")
	}
	switch strings.ToUpper(strings.TrimSpace(o.TransactionType)) {
	case "B", "S", "BUY", "SELL":
	default:
		return fmt.Errorf("transaction_type must be B, S, BUY, or SELL")
	}
	orderType := strings.ToUpper(strings.TrimSpace(o.OrderType))
	switch orderType {
	case "LMT", "MKT", "SL-LMT", "SL-MKT":
	default:
		return fmt.Errorf("unsupported order_type %q", o.OrderType)
	}
	// P1-191: LMT/SL prices must be positive numbers — empty, zero,
	// negative, or non-numeric prices fail fast as REJECTED, never reach
	// the broker. Canonical digits with one optional dot (exponent,
	// sign, and separators rejected); at least one non-zero digit.
	if (orderType == "LMT" || orderType == "SL-LMT" || orderType == "SL-MKT") && !priceIsPositive(o.Price) {
		return fmt.Errorf("price must be a positive number for %s", orderType)
	}
	// P3-045: Arrow documents price "0" for market orders, so every zero spelling
	// ("", "0", "00", "0.0") is the same valid value and is canonicalised by the
	// adapter; a non-zero market price stays a caller error.
	if orderType == "MKT" && !priceIsZero(o.Price) {
		return fmt.Errorf("MKT price must be empty or zero")
	}
	switch strings.ToUpper(strings.TrimSpace(o.Product)) {
	case "I", "C", "M":
	default:
		return fmt.Errorf("product must be I, C, or M")
	}
	switch strings.ToUpper(strings.TrimSpace(o.Validity)) {
	case "DAY", "IOC":
	default:
		return fmt.Errorf("validity must be DAY or IOC")
	}
	return nil
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func nowMs() int64 { return time.Now().UnixMilli() }

func fingerprint(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func priceIsPositive(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	dots, digits, positive := 0, 0, false
	for _, r := range s {
		switch {
		case r == '.':
			dots++
			if dots > 1 {
				return false
			}
		case r >= '0' && r <= '9':
			digits++
			if r != '0' {
				positive = true
			}
		default:
			return false
		}
	}
	return digits > 0 && positive
}

// containsSpace reports whether the value contains any whitespace character
// (Unicode-aware). Several order fields are upper-cased without trimming and
// exchange/symbol are forwarded verbatim, so whitespace that passes validation
// reaches Arrow unchanged (P3-252, P3-253).
func containsSpace(s string) bool {
	return strings.ContainsFunc(s, unicode.IsSpace)
}

// maxSymbolLength bounds the order symbol so an oversized value fails here
// instead of at the venue (P3-252).
const maxSymbolLength = 64

// priceIsZero reports whether the price is an absent or semantically-zero value
// (optional single dot, digits only, no non-zero digit). Arrow documents
// `price: "0"` for market orders, so every zero spelling a caller might send is
// accepted and canonicalised to "0" before the request leaves the bridge
// (P3-045).
func priceIsZero(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return true
	}
	dots, digits := 0, 0
	for _, r := range s {
		switch {
		case r == '.':
			dots++
			if dots > 1 {
				return false
			}
		case r >= '0' && r <= '9':
			digits++
			if r != '0' {
				return false
			}
		default:
			return false
		}
	}
	return digits > 0
}

// validateBrokerOrderID fails closed on a missing or whitespace-carrying broker
// order id (P3-251). The id is assigned by Arrow and this bridge does not own its
// grammar, so no charset is imposed beyond "no whitespace": the SDK already
// refuses path-hostile ids and escapes the segment
// (third_party/go-arrow/arrow/orders.go:337-357, :430-438), and a charset the
// bridge invented could reject a legitimate id the venue issued.
func validateBrokerOrderID(id, command string) error {
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("broker_order_id is required for %s", command)
	}
	if containsSpace(id) {
		return fmt.Errorf("broker_order_id must not contain whitespace")
	}
	return nil
}
