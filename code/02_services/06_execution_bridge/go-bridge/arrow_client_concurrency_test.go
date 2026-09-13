package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/arrow-trade/go-arrow/arrow"
)

// P3-042 pin: the review reports that the bridge shares one *arrow.Client
// between live order traffic and the re-auth callback, that the SDK reads
// Config.Token on every request while Authenticate rewrites it, and therefore
// that live order flow can see torn token reads or lose a re-auth update.
//
// The premise is already closed inside the SDK (WAVE9-A / P1-031): every read
// and every write of Config.Token/RefreshToken takes c.mu. Reads under RLock:
// request (client.go:87), rawRequestAuth (client.go:124), GetToken/GetRefreshToken
// (client.go:245/255), ConnectDataStream/ConnectOrderStream (streams.go:117/411)
// and connectHFTDataStreamURL (hft_stream.go:71). Writes under Lock: Authenticate
// (auth.go:120) and SetToken (client.go:205). AutoLogin also snapshots AppID and
// BaseURL under RLock (auth.go:206). This module never mutates a client outside
// AutoLogin, and SetBaseURL/SetAppID do not exist, so AppID/BaseURL are immutable
// after construction.
//
// There is nothing to fix here; what is missing is a pin. This test drives the
// real wiring — ArrowBroker behind ReauthBroker over one shared client — against
// a fake venue while re-auths land underneath it, under -race, and asserts the
// venue never sees an empty or unknown token and does see the refreshed one. It
// fails if a future SDK change moves a token access out of the mutex, which is
// the regression it exists to catch. The stream connects carry the same RLock
// guard but dial a fixed production endpoint (orderStreamURL/dataStreamURL, no
// BaseURL override), so this test does not dial them.
func TestSharedArrowClientIsSafeUnderConcurrentReauth(t *testing.T) {
	// A valid base32 secret: generateTOTP runs before the 2FA request.
	const totpSecret = "JBSWY3DPEHPK3PXP"
	const reauths = 12
	const workers = 8
	const commandsPerWorker = 40

	var (
		mu          sync.Mutex
		logins      int
		issuedToken string
		seenTokens  = map[string]int{}
		orderCalls  int
		failures    []string
	)

	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/auth/app/login":
			// Each login mints a new generation; AutoLogin then walks 2FA and
			// authenticate-token to publish token-<n>.
			logins++
			_, _ = fmt.Fprintf(w, `{"status":"success","data":{"requestId":"RID-%d"}}`, logins)
		case "/auth/validate-2fa":
			_, _ = fmt.Fprintf(w, `{"status":"success","data":{"redirectUrl":"http://venue.invalid/2fa/done?request-token=RT-%d"}}`, logins)
		case "/auth/app/authenticate-token":
			issuedToken = fmt.Sprintf("token-%d", logins)
			_, _ = fmt.Fprintf(w, `{"status":"success","data":{"token":%q,"refreshToken":"refresh-%d"}}`, issuedToken, logins)
		case "/order/regular":
			orderCalls++
			token := r.Header.Get("token")
			seenTokens[token]++
			if token == "" {
				failures = append(failures, "venue saw an empty token on order traffic")
			}
			_, _ = w.Write([]byte(`{"status":"success","data":{"orderNo":"BRK-1","requestTime":"now"}}`))
		default:
			failures = append(failures, "unexpected venue request "+r.Method+" "+r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer venue.Close()

	client := arrow.NewClient("app", "secret")
	client.Config.BaseURL = venue.URL
	if err := client.AutoLogin("user", "password", totpSecret); err != nil {
		t.Fatalf("initial login against the fake venue: %v", err)
	}

	inner, err := NewArrowBroker(client)
	if err != nil {
		t.Fatal(err)
	}
	broker := NewReauthBroker(inner, func(context.Context) error {
		// main.go's re-auth callback shape: the same shared client.
		return client.AutoLogin("user", "password", totpSecret)
	})

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range commandsPerWorker {
				if result := broker.Place(context.Background(), validPlaceCommand()); result.Outcome != OutcomeSuccess {
					mu.Lock()
					failures = append(failures, fmt.Sprintf("place during re-auth: outcome=%s reason=%s", result.Outcome, result.Reason))
					mu.Unlock()
				}
				// The guarded getter, i.e. the cheapest reader of the shared field.
				_ = client.GetToken()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range reauths {
			if err := client.AutoLogin("user", "password", totpSecret); err != nil {
				mu.Lock()
				failures = append(failures, fmt.Sprintf("re-auth %d: %v", i, err))
				mu.Unlock()
			}
		}
	}()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(failures) > 0 {
		t.Errorf("concurrent re-auth broke live order flow: %v", failures)
	}
	if logins < 2 {
		t.Fatalf("logins=%d want >=2: no token rewrite overlapped the readers, so this run proves nothing", logins)
	}
	if orderCalls < workers {
		t.Fatalf("orderCalls=%d want >=%d: the venue never saw live traffic", orderCalls, workers)
	}
	for token := range seenTokens {
		if token == "" {
			t.Errorf("venue saw an empty token on order traffic: a reader observed an unpublished token")
		} else if !strings.HasPrefix(token, "token-") {
			t.Errorf("venue saw token %q, which no login issued: a reader observed a torn string", token)
		}
	}
	if seenTokens[issuedToken] == 0 {
		t.Errorf("venue never saw the newest token %q (saw %v): a re-auth update was lost", issuedToken, seenTokens)
	}
	if got := client.GetToken(); got != issuedToken {
		t.Errorf("client.GetToken()=%q want the newest %q", got, issuedToken)
	}
}
