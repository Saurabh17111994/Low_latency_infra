package main

import "testing"

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
