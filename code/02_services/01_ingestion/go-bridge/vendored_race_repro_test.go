package main

import (
	"os"
	"sync"
	"testing"

	"github.com/arrow-trade/go-arrow/arrow"
)

// P1-031/P1-035 repro (read-only proof, expects -race to FIRE on vendored lines):
// concurrent SetToken (client.go write) vs GetToken (client.go read) on a shared
// *arrow.Client — the exact pair our multi-slot supervisor exercises when slot A
// refreshes (AutoLogin→Config.Token write) while slot B dials (token read).
// A race report with both sides in third_party/go-arrow proves the lock must be
// added upstream: own code cannot interpose on vendored struct fields.
func TestVendoredTokenRaceRepro(t *testing.T) {
	// Proof-only: run with PROVE_VENDORED_RACE=1 + -race to re-fire.
	// consult /tmp/w9race.txt (2026-09-08): both stacks in third_party/go-arrow/arrow/client.go:198/:233.
	if os.Getenv("PROVE_VENDORED_RACE") != "1" {
		t.Skip("proof-only repro; set PROVE_VENDORED_RACE=1 with -race to re-fire")
	}
	client := arrow.NewClient("app", "secret")
	client.SetToken("seed")
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				client.SetToken("tok")
			}
		}(i)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = client.GetToken()
			}
		}()
	}
	wg.Wait()
}
