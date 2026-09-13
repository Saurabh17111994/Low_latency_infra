package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func validPlaceCommand() CommandEnvelope {
	return CommandEnvelope{
		RecordType: RecordCommand, ContractVersion: ProtocolVersion,
		RequestID: "req-1", Command: CommandPlace,
		InstructionID: "instruction-1", ExecutionAttemptID: "attempt-1",
		ClientOrderRef: "INS1234567890123",
		Order: &OrderCommand{
			Exchange: "NSE", Symbol: "SBIN-EQ", Quantity: "2",
			TransactionType: "BUY", OrderType: "LMT", Product: "I",
			Price: "15050", Validity: "DAY",
		},
	}
}

func TestValidateCommandRejectsUnsafeClientReference(t *testing.T) {
	command := validPlaceCommand()
	command.ClientOrderRef = "too-long-client-reference"
	if err := validateCommand(command); err == nil {
		t.Fatal("expected client reference validation error")
	}
}

func TestValidateCommandRejectsIndexAndMissingAttempt(t *testing.T) {
	command := validPlaceCommand()
	command.Order.Exchange = "INDEX"
	if err := validateCommand(command); err == nil {
		t.Fatal("INDEX must not be executable")
	}
	command = validPlaceCommand()
	command.ExecutionAttemptID = ""
	if err := validateCommand(command); err == nil {
		t.Fatal("execution attempt is required")
	}
}

// P1-036: ValidityGTC exists in the vendored enum but the broker path only
// accepts DAY|IOC — a GTC order must fail validation, never reach the broker.
func TestValidateCommandRejectsGTCValidity(t *testing.T) {
	for _, v := range []string{"GTC", "gtc", "FOK", ""} {
		command := validPlaceCommand()
		command.Order.Validity = v
		if err := validateCommand(command); err == nil {
			t.Fatalf("validity %q must be rejected (DAY|IOC only)", v)
		}
	}
}

func TestToArrowOrderMapsPlatformValues(t *testing.T) {
	order, err := toArrowOrder(*validPlaceCommand().Order, "INS1234567890123")
	if err != nil {
		t.Fatal(err)
	}
	if order.TransactionType != "B" || order.OrderType != "LMT" || order.Remarks != "INS1234567890123" {
		t.Fatalf("unexpected Arrow order: %+v", order)
	}
}

func TestToArrowMarketOrderDefaultsPriceToZero(t *testing.T) {
	command := validPlaceCommand()
	command.Order.OrderType = "MKT"
	command.Order.Price = ""
	order, err := toArrowOrder(*command.Order, command.ClientOrderRef)
	if err != nil {
		t.Fatal(err)
	}
	if order.Price != "0" {
		t.Fatalf("market price=%q, want 0", order.Price)
	}
}

// P1-191: LMT/SL prices must be positive numbers (empty/zero/negative/
// non-numeric never reach the broker).
func TestValidateCommandRejectsNonPositivePrice(t *testing.T) {
	for _, price := range []string{"", "0", "0.00", "00", "-5", "abc", "12.3.4", "1e4", "+5", "  "} {
		for _, orderType := range []string{"LMT", "SL-LMT", "SL-MKT"} {
			command := validPlaceCommand()
			command.Order.OrderType = orderType
			command.Order.Price = price
			if err := validateCommand(command); err == nil {
				t.Fatalf("order_type %s price %q must be rejected", orderType, price)
			}
		}
	}
	for _, price := range []string{"15050", "1", "0.01", "15050.25"} {
		command := validPlaceCommand()
		command.Order.Price = price
		if err := validateCommand(command); err != nil {
			t.Fatalf("LMT price %q must be accepted: %v", price, err)
		}
	}
}

// P3-250: identity requiredness is scoped to the command, and the wire shape is
// deliberately "empty identity => key absent" on both sides of the contract
// (the Rust peer declares these fields with skip_serializing_if =
// "String::is_empty"). This pins both halves so neither drifts.
func TestCommandEnvelopeIdentityRequirednessAndWireShape(t *testing.T) {
	for _, tc := range []struct {
		name string
		drop func(*CommandEnvelope)
	}{
		{"instruction_id", func(c *CommandEnvelope) { c.InstructionID = "" }},
		{"execution_attempt_id", func(c *CommandEnvelope) { c.ExecutionAttemptID = "" }},
		{"client_order_ref", func(c *CommandEnvelope) { c.ClientOrderRef = "" }},
	} {
		command := validPlaceCommand()
		tc.drop(&command)
		if err := validateCommand(command); err == nil {
			t.Fatalf("place without %s must fail validation", tc.name)
		}
	}

	cancel := CommandEnvelope{
		RecordType: RecordCommand, ContractVersion: ProtocolVersion,
		RequestID: "req-2", Command: CommandCancel, BrokerOrderID: "BRK-1",
	}
	if err := validateCommand(cancel); err != nil {
		t.Fatalf("cancel without platform identities must stay valid: %v", err)
	}

	emptyJSON, err := json.Marshal(cancel)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"instruction_id", "execution_attempt_id", "client_order_ref"} {
		if strings.Contains(string(emptyJSON), key) {
			t.Fatalf("empty %s must stay omitted on the wire: %s", key, emptyJSON)
		}
	}

	fullJSON, err := json.Marshal(validPlaceCommand())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"instruction_id", "execution_attempt_id", "client_order_ref"} {
		if !strings.Contains(string(fullJSON), key) {
			t.Fatalf("set %s must be present on the wire: %s", key, fullJSON)
		}
	}
}

// P3-473: commands that correlate by broker_order_id (or by nothing at all) must
// not silently accept payloads meant for place — a cancel carrying an order body
// is a caller bug, not a field to ignore. Every accepted-but-invalid shape is
// collected so one run reports all of them.
func TestValidateCommandRejectsSemanticExtras(t *testing.T) {
	order := validPlaceCommand().Order
	var accepted []string
	expectRejected := func(label string, c CommandEnvelope) {
		if err := validateCommand(c); err == nil {
			accepted = append(accepted, label)
		}
	}
	for _, command := range []string{CommandCancel, CommandQueryOrder} {
		expectRejected(command+" with order body", CommandEnvelope{
			RecordType: RecordCommand, ContractVersion: ProtocolVersion,
			RequestID: "req-1", Command: command, BrokerOrderID: "BRK-1", Order: order,
		})
	}
	for _, command := range []string{CommandReconcileOrders, CommandReconcileTrades, CommandReconcilePosition} {
		base := CommandEnvelope{RecordType: RecordCommand, ContractVersion: ProtocolVersion,
			RequestID: "req-1", Command: command}
		if err := validateCommand(base); err != nil {
			t.Fatalf("%s without extras must stay valid: %v", command, err)
		}
		withOrder := base
		withOrder.Order = order
		expectRejected(command+" with order body", withOrder)
		withBrokerID := base
		withBrokerID.BrokerOrderID = "BRK-1"
		expectRejected(command+" with broker_order_id", withBrokerID)
	}
	if len(accepted) > 0 {
		t.Fatalf("accepted command shapes that must be rejected: %v", accepted)
	}
	legit := CommandEnvelope{RecordType: RecordCommand, ContractVersion: ProtocolVersion,
		RequestID: "req-1", Command: CommandCancel, BrokerOrderID: "BRK-1"}
	if err := validateCommand(legit); err != nil {
		t.Fatalf("cancel with broker_order_id must stay valid: %v", err)
	}
}

// P3-044: the INDEX guard compared the raw value, so padding (" INDEX ",
// "index\t") satisfied the emptiness check and then failed the EqualFold
// comparison — an index/underlying slipped through as executable.
func TestValidateCommandRejectsPaddedIndexExchange(t *testing.T) {
	var accepted []string
	for _, exchange := range []string{"INDEX", "index", " INDEX ", "index\t", "\tInDeX\n", " index "} {
		command := validPlaceCommand()
		command.Order.Exchange = exchange
		if err := validateCommand(command); err == nil {
			accepted = append(accepted, exchange)
		}
	}
	if len(accepted) > 0 {
		t.Fatalf("index exchanges accepted as executable: %q", accepted)
	}
	command := validPlaceCommand()
	command.Order.Exchange = "NSE"
	if err := validateCommand(command); err != nil {
		t.Fatalf("NSE must stay valid: %v", err)
	}
}
