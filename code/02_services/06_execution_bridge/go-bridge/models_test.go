package main

import (
	"encoding/json"
	"fmt"
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

// P3-252: an unbounded symbol or quantity reached Arrow as a well-formed order,
// so a local caller bug became a venue round trip (and, for quantity, a value the
// bridge cannot represent). Every accepted-beyond-bound shape is collected so one
// run reports all of them.
func TestValidateCommandRejectsUnboundedSymbolAndQuantity(t *testing.T) {
	var accepted []string
	for _, tc := range []struct {
		label  string
		mutate func(*OrderCommand)
	}{
		{"padded symbol", func(o *OrderCommand) { o.Symbol = " SBIN " }},
		{"symbol with internal whitespace", func(o *OrderCommand) { o.Symbol = "SB IN" }},
		{"65-character symbol", func(o *OrderCommand) { o.Symbol = strings.Repeat("A", 65) }},
		{"20-digit quantity", func(o *OrderCommand) { o.Quantity = strings.Repeat("9", 20) }},
		{"padded quantity", func(o *OrderCommand) { o.Quantity = " 5 " }},
	} {
		command := validPlaceCommand()
		tc.mutate(command.Order)
		if err := validateCommand(command); err == nil {
			accepted = append(accepted, tc.label)
		}
	}
	if len(accepted) > 0 {
		t.Fatalf("order shapes accepted beyond their bounds: %v", accepted)
	}
	for _, tc := range []struct {
		label  string
		mutate func(*OrderCommand)
	}{
		{"64-character symbol", func(o *OrderCommand) { o.Symbol = strings.Repeat("A", 64) }},
		{"max int64 quantity", func(o *OrderCommand) { o.Quantity = "9223372036854775807" }},
		{"ordinary order", func(o *OrderCommand) {}},
	} {
		command := validPlaceCommand()
		tc.mutate(command.Order)
		if err := validateCommand(command); err != nil {
			t.Fatalf("%s must stay valid: %v", tc.label, err)
		}
	}
}

// P3-045: Arrow documents `price: "0"` for market orders, so every spelling of
// zero is the same valid MKT price. The guard accepted only "" and "0" exactly
// and rejected the rest, which the adapter would then have forwarded verbatim.
func TestValidateMarketOrderPriceForms(t *testing.T) {
	var rejected []string
	for _, price := range []string{"", "0", "00", "0.0", "0.00", "0.000", " 0 "} {
		command := validPlaceCommand()
		command.Order.OrderType = "MKT"
		command.Order.Price = price
		if err := validateCommand(command); err != nil {
			rejected = append(rejected, fmt.Sprintf("%q (%v)", price, err))
		}
	}
	if len(rejected) > 0 {
		t.Fatalf("zero-valued market prices must be valid: %v", rejected)
	}
	for _, price := range []string{"1", "0.5", "15050", ".", "0.0.0", "-0", "00.1"} {
		command := validPlaceCommand()
		command.Order.OrderType = "MKT"
		command.Order.Price = price
		if err := validateCommand(command); err == nil {
			t.Fatalf("non-zero MKT price %q must be rejected", price)
		}
	}
	// P1-191 decision, deliberately unchanged: a stop-loss market order carries
	// its trigger in `price` (the request has no separate trigger field), so an
	// empty or zero trigger stays rejected.
	for _, price := range []string{"", "0", "0.00"} {
		command := validPlaceCommand()
		command.Order.OrderType = "SL-MKT"
		command.Order.Price = price
		if err := validateCommand(command); err == nil {
			t.Fatalf("SL-MKT price %q must stay rejected", price)
		}
	}
}

// P3-045: the adapter canonicalises every accepted zero form so the request bytes
// do not depend on how the caller spelled zero.
func TestToArrowMarketOrderCanonicalisesZeroPriceForms(t *testing.T) {
	for _, price := range []string{"", "0", "00", "0.0", "0.000"} {
		command := validPlaceCommand()
		command.Order.OrderType = "MKT"
		command.Order.Price = price
		order, err := toArrowOrder(*command.Order, command.ClientOrderRef)
		if err != nil {
			t.Fatalf("MKT price %q must be accepted: %v", price, err)
		}
		if order.Price != "0" {
			t.Fatalf("MKT price %q forwarded as %q, want 0", price, order.Price)
		}
	}
	command := validPlaceCommand()
	command.Order.Price = "15050.25"
	order, err := toArrowOrder(*command.Order, command.ClientOrderRef)
	if err != nil {
		t.Fatal(err)
	}
	if order.Price != "15050.25" {
		t.Fatalf("LMT price forwarded as %q, want 15050.25", order.Price)
	}
}

// P3-253: validation trimmed (or ignored) whitespace on these fields while the
// adapter upper-cases them without trimming and forwards exchange verbatim, so a
// padded value passed validation and then reached Arrow padded.
func TestValidateCommandRejectsPaddedForwardedFields(t *testing.T) {
	var accepted []string
	for _, tc := range []struct {
		label  string
		mutate func(*OrderCommand)
	}{
		{"padded exchange", func(o *OrderCommand) { o.Exchange = " NSE " }},
		{"padded order_type", func(o *OrderCommand) { o.OrderType = " lmt " }},
		{"padded product", func(o *OrderCommand) { o.Product = " c " }},
		{"padded validity", func(o *OrderCommand) { o.Validity = " ioc " }},
		{"order_type with internal whitespace", func(o *OrderCommand) { o.OrderType = "L MT" }},
		{"validity with internal whitespace", func(o *OrderCommand) { o.Validity = "I OC" }},
	} {
		command := validPlaceCommand()
		tc.mutate(command.Order)
		if err := validateCommand(command); err == nil {
			accepted = append(accepted, tc.label)
		}
	}
	if len(accepted) > 0 {
		t.Fatalf("padded fields accepted and forwarded: %v", accepted)
	}

	// Case stays tolerated where the adapter upper-cases the field, and the
	// adapter's output is canonical for a valid order.
	for _, tc := range []struct {
		label  string
		mutate func(*OrderCommand)
	}{
		{"lowercase order_type", func(o *OrderCommand) { o.OrderType = "lmt" }},
		{"lowercase product", func(o *OrderCommand) { o.Product = "c" }},
		{"lowercase validity", func(o *OrderCommand) { o.Validity = "day" }},
		{"lowercase transaction_type", func(o *OrderCommand) { o.TransactionType = "buy" }},
	} {
		command := validPlaceCommand()
		tc.mutate(command.Order)
		if err := validateCommand(command); err != nil {
			t.Fatalf("%s must stay valid: %v", tc.label, err)
		}
		order, err := toArrowOrder(*command.Order, command.ClientOrderRef)
		if err != nil {
			t.Fatalf("%s must be convertible: %v", tc.label, err)
		}
		for _, field := range []struct{ name, value string }{
			{"exchange", order.Exchange},
			{"order_type", order.OrderType},
			{"product", order.Product},
			{"validity", order.Validity},
		} {
			if strings.TrimSpace(field.value) != field.value {
				t.Fatalf("%s forwarded padded as %q", field.name, field.value)
			}
		}
	}
}
