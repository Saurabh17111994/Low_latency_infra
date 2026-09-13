package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

// P5-013: the bridge must resolve its auth token from either a secret FILE
// (Swarm/compose `secrets:`) or direct env — and fail loud on any malformed
// input rather than boot unauthenticated.
func TestAuthTokenFromEnv(t *testing.T) {
	writeSecret := func(t *testing.T, content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "token")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	t.Run("file takes precedence over env", func(t *testing.T) {
		path := writeSecret(t, "file-token\n") // docker secrets carry a trailing newline
		t.Setenv("EXECUTION_BRIDGE_AUTH_TOKEN_FILE", path)
		t.Setenv("EXECUTION_BRIDGE_AUTH_TOKEN", "env-token")
		got, err := authTokenFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "file-token" {
			t.Fatalf("file must win, got %q", got)
		}
	})

	t.Run("file content is trimmed", func(t *testing.T) {
		path := writeSecret(t, "  spaced-token \n")
		t.Setenv("EXECUTION_BRIDGE_AUTH_TOKEN_FILE", path)
		got, err := authTokenFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "spaced-token" {
			t.Fatalf("token must be trimmed, got %q", got)
		}
	})

	t.Run("env used when file unset", func(t *testing.T) {
		t.Setenv("EXECUTION_BRIDGE_AUTH_TOKEN", "env-token")
		got, err := authTokenFromEnv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "env-token" {
			t.Fatalf("env must be used, got %q", got)
		}
	})

	t.Run("both unset is an error", func(t *testing.T) {
		if _, err := authTokenFromEnv(); err == nil {
			t.Fatal("expected error when neither file nor env is set")
		}
	})

	t.Run("unreadable file fails loud", func(t *testing.T) {
		t.Setenv("EXECUTION_BRIDGE_AUTH_TOKEN_FILE", filepath.Join(t.TempDir(), "does-not-exist"))
		_, err := authTokenFromEnv()
		if err == nil || !strings.Contains(err.Error(), "unreadable") {
			t.Fatalf("expected loud unreadable-file error, got %v", err)
		}
	})

	t.Run("empty file fails loud", func(t *testing.T) {
		path := writeSecret(t, "   \n")
		t.Setenv("EXECUTION_BRIDGE_AUTH_TOKEN_FILE", path)
		if _, err := authTokenFromEnv(); err == nil || !strings.Contains(err.Error(), "empty") {
			t.Fatalf("expected loud empty-file error, got %v", err)
		}
	})
}

// P3-247 — a mode=live boot posts to the venue, and "Arrow authentication
// failed" without the cause is unactionable: a DNS failure, bad credentials, a
// skewed TOTP and a contract change all read identically while the bridge is
// down and the operator is reproducing blind.
func TestStartupLoginPreservesTheCause(t *testing.T) {
	cause := &arrow.AuthError{Stage: "login", Err: errors.New("dial tcp: lookup api.arrow.trade: no such host")}
	err := startupLogin(func() error { return cause })
	if err == nil {
		t.Fatal("a failed login must not report success")
	}
	if !strings.Contains(err.Error(), "Arrow authentication failed") {
		t.Errorf("message %q lost the startup context operators grep for", err)
	}
	if !errors.Is(err, cause) {
		t.Errorf("message %q does not wrap the cause: network, credential, TOTP and parsing failures read identically", err)
	}
	var authErr *arrow.AuthError
	if !errors.As(err, &authErr) || authErr.Stage != "login" {
		t.Errorf("errors.As on %v did not recover the AuthError stage used for triage", err)
	}
}

// P3-472 — the private order path had only ReadHeaderTimeout: the body read
// (MaxBytesReader + JSON decode, before the command context exists) and the
// response write were unbounded, so one slow client could hold a handler
// goroutine and its connection open indefinitely.
func TestHTTPServerBoundsReadsWritesAndIdleConnections(t *testing.T) {
	commandTimeout := 10 * time.Second
	server := newHTTPServer("127.0.0.1:0", http.NotFoundHandler(), commandTimeout)

	if server.ReadHeaderTimeout <= 0 {
		t.Errorf("ReadHeaderTimeout=%s: a client dribbling headers pins the connection", server.ReadHeaderTimeout)
	}
	if server.ReadTimeout <= 0 {
		t.Errorf("ReadTimeout=%s: a client that stalls a body after its headers pins the handler", server.ReadTimeout)
	}
	if server.WriteTimeout <= commandTimeout {
		t.Errorf("WriteTimeout=%s must exceed the command timeout %s: an operator raising EXECUTION_BRIDGE_COMMAND_TIMEOUT_MS would otherwise have legal slow replies cut off mid-write",
			server.WriteTimeout, commandTimeout)
	}
	if server.IdleTimeout <= 0 {
		t.Errorf("IdleTimeout=%s: idle keep-alive connections accumulate", server.IdleTimeout)
	}
}

// The bounds above must actually fire, not just be set: this drives a real
// connection that sends headers and then stalls the body.
func TestHTTPServerDropsAStalledRequestBody(t *testing.T) {
	server := newHTTPServer("127.0.0.1:0", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // never completes against a stalled client
	}), 200*time.Millisecond)
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "POST /command HTTP/1.1\r\nHost: bridge\r\nContent-Length: 100000\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	// Any of a response, EOF or a reset means the handler was released; only
	// silence past the deadline means it is still pinned on the body read.
	if _, err := conn.Read(make([]byte, 1)); err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			t.Fatalf("a stalled body held the connection open past ReadTimeout: the handler is pinned by a slow client")
		}
	}
}

// P3-248 — the re-auth callback accepts the caller's command context but
// AutoLogin takes none, and it makes up to three sequential venue calls at 15s
// each. Unbounded, a stalled auth outlives the command deadline and pins the
// dispatch goroutine in ReauthBroker.doWithReauth.
func TestLoginWithinContextHonoursTheCallerDeadline(t *testing.T) {
	blocked := make(chan struct{})
	defer close(blocked)
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	returned := make(chan error, 1)
	go func() {
		returned <- loginWithinContext(ctx, func() error { <-blocked; return nil })
	}()

	select {
	case err := <-returned:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("loginWithinContext returned %v, want the caller's deadline error", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("loginWithinContext did not return within 2s: the login call ignores ctx and would hold the dispatch goroutine for the venue's full 3-step timeout")
	}
}

func TestLoginWithinContextReturnsTheLoginResult(t *testing.T) {
	cause := errors.New("bad totp")
	if err := loginWithinContext(t.Context(), func() error { return cause }); !errors.Is(err, cause) {
		t.Errorf("loginWithinContext returned %v, want the login failure %v", err, cause)
	}
	if err := loginWithinContext(t.Context(), func() error { return nil }); err != nil {
		t.Errorf("loginWithinContext returned %v for a successful login", err)
	}
}
