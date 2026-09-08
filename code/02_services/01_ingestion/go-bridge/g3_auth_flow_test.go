package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/arrow-trade/go-arrow/arrow"
)

// G3 guard: Authenticate failure must carry the server reason (P1-161).
func TestG3AuthenticateSurfacesServerReason(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"error","message":"bad checksum supplied","error":"AUTH_001","data":{}}`))
	}))
	defer srv.Close()
	c := arrow.NewClient("id", "secret")
	c.Config.BaseURL = srv.URL
	if _, err := c.Authenticate("tok"); err == nil || !strings.Contains(err.Error(), "bad checksum supplied") {
		t.Fatalf("G3/P1-161: Authenticate error must carry server reason, got %v", err)
	}
}

// G3 guard: AutoLogin must derive auth URLs from BaseURL so staging/mock
// can redirect the full login flow (P1-163/P1-165).
func TestG3AutoLoginDerivesAuthHostsFromBaseURL(t *testing.T) {
	var sawLogin, saw2FA bool
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/app/login", func(w http.ResponseWriter, r *http.Request) {
		sawLogin = true
		w.Write([]byte(`{"status":"success","data":{"requestId":"RID"}}`))
	})
	mux.HandleFunc("/auth/validate-2fa", func(w http.ResponseWriter, r *http.Request) {
		saw2FA = true
		w.Write([]byte(`{"status":"error","message":"bad totp","data":{}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := arrow.NewClient("id", "secret")
	c.Config.BaseURL = srv.URL
	err := c.AutoLogin("u", "p", "AAAAAAAAAAAAAAAA")
	var ae *arrow.AuthError
	if !sawLogin || !saw2FA || !errors.As(err, &ae) {
		t.Fatalf("G3/P1-163/165: staging redirect must hit both endpoints with AuthError, login=%v 2fa=%v err=%v", sawLogin, saw2FA, err)
	}
}

// G3 guard: empty redirectUrl (2FA rejection) must fail distinctly from a
// present URL missing the token (P1-167).
func TestG3EmptyRedirectFailsDistinctly(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/auth/app/login", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"success","data":{"requestId":"RID"}}`))
	})
	mux.HandleFunc("/auth/validate-2fa", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"error","message":"bad totp","data":{}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := arrow.NewClient("id", "secret")
	c.Config.BaseURL = srv.URL
	err := c.AutoLogin("u", "p", "AAAAAAAAAAAAAAAA")
	if err == nil || !strings.Contains(err.Error(), "empty redirectUrl") {
		t.Fatalf("G3/P1-167: empty redirect must fail as empty-redirectUrl, got %v", err)
	}
}
