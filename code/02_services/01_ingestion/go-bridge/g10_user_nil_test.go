package main

import (
	"encoding/json"
	"testing"

	"github.com/arrow-trade/go-arrow/arrow"
)

// G10 guard: all four User helpers are nil-receiver safe (P1-204) — a nil
// *User or data:null User must return zero values, never panic.
func TestG10UserHelpersNilSafe(t *testing.T) {
	var nilUser *arrow.User
	if nilUser.HasDefaultBankAccount() || nilUser.HasExchangeAccess("NSE") || nilUser.IsTotpEnabled() {
		t.Fatal("G10/P1-204: nil *User helpers must return false")
	}
	if nilUser.GetDefaultBankAccount() != nil {
		t.Fatal("G10/P1-204: nil *User GetDefaultBankAccount must return nil")
	}
	var nullData arrow.User
	if err := json.Unmarshal([]byte(`{"status":"success","data":null}`), &nullData); err != nil {
		t.Fatalf("G10/P1-204: data:null must decode: %v", err)
	}
	u := &nullData
	if u.HasDefaultBankAccount() || u.HasExchangeAccess("NSE") || u.IsTotpEnabled() {
		t.Fatal("G10/P1-204: data:null helpers must return false")
	}
	if u.GetDefaultBankAccount() != nil {
		t.Fatal("G10/P1-204: data:null GetDefaultBankAccount must return nil")
	}
}

// G10 guard: GetDefaultBankAccount returns a copy (P1-205) — caller
// mutation must not alias the User's interior slice element.
func TestG10DefaultBankIsCopy(t *testing.T) {
	var u arrow.User
	body := `{"status":"success","data":{"bankDetails":[{"id":"1","isDefault":true,"bankName":"A"}],"exchanges":["NSE"],"totpEnabled":true}}`
	if err := json.Unmarshal([]byte(body), &u); err != nil {
		t.Fatalf("G10: fixture must decode: %v", err)
	}
	got := u.GetDefaultBankAccount()
	if got == nil || got.BankName != "A" {
		t.Fatalf("G10/P1-205: expected default bank A, got %+v", got)
	}
	got.BankName = "MUTATED"
	again := u.GetDefaultBankAccount()
	if again.BankName != "A" {
		t.Fatalf("G10/P1-205: mutation leaked into User interior: %+v", again)
	}
	if !u.HasDefaultBankAccount() || !u.HasExchangeAccess("NSE") || !u.IsTotpEnabled() {
		t.Fatal("G10: positive helpers must return true on populated user")
	}
}
