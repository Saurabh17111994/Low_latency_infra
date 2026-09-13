package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/arrow-trade/go-arrow/arrow"
)

func main() {
	authToken, err := authTokenFromEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "execution-bridge: %v\n", err)
		os.Exit(2)
	}
	mode := strings.ToLower(strings.TrimSpace(envOrDefault("EXECUTION_BRIDGE_MODE", "disabled")))
	broker, client, err := brokerFromEnvironment(mode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "execution-bridge: startup blocked: %v\n", err)
		os.Exit(2)
	}
	server, err := NewBridgeServer(broker, authToken, mode)
	if err != nil {
		fmt.Fprintf(os.Stderr, "execution-bridge: startup blocked: %v\n", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if client != nil {
		go RunPostbackLoop(ctx, func() (OrderUpdateSource, error) {
			return NewArrowOrderUpdateSource(client)
		}, server.hub.Publish, func(err error) {
			fmt.Fprintf(os.Stderr, "execution-bridge: postback: %s\n", sanitizeReason(err))
		})
	}

	addr := envOrDefault("EXECUTION_BRIDGE_LISTEN_ADDR", "127.0.0.1:8787")
	httpServer := newHTTPServer(addr, server.Handler(), server.commandTimeout)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
	}()
	fmt.Fprintf(os.Stderr, "execution-bridge: listening addr=%s mode=%s\n", addr, mode)
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "execution-bridge: server failed: %v\n", err)
		os.Exit(1)
	}
}

// authTokenFromEnv resolves the private-bridge auth token (P5-013). Docker
// and Swarm secrets are FILES, so secret-based deployments mount the token
// and point EXECUTION_BRIDGE_AUTH_TOKEN_FILE at it (the idiomatic _FILE
// pattern, same as the official postgres/redis images). Direct
// EXECUTION_BRIDGE_AUTH_TOKEN env still works for plain `docker run -e` and
// tests. A named-but-unreadable or empty file fails LOUD — the bridge must
// never silently fall back to running unauthenticated.
func authTokenFromEnv() (string, error) {
	if path := strings.TrimSpace(os.Getenv("EXECUTION_BRIDGE_AUTH_TOKEN_FILE")); path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("EXECUTION_BRIDGE_AUTH_TOKEN_FILE=%s unreadable: %w", path, err)
		}
		token := strings.TrimSpace(string(b))
		if token == "" {
			return "", fmt.Errorf("EXECUTION_BRIDGE_AUTH_TOKEN_FILE=%s is empty", path)
		}
		return token, nil
	}
	token := strings.TrimSpace(os.Getenv("EXECUTION_BRIDGE_AUTH_TOKEN"))
	if token == "" {
		return "", fmt.Errorf("EXECUTION_BRIDGE_AUTH_TOKEN (or _FILE) is required")
	}
	return token, nil
}

func brokerFromEnvironment(mode string) (Broker, *arrow.Client, error) {
	switch mode {
	case "disabled":
		return NewFakeBrokerWithDisabledResult(), nil, nil
	case "fake":
		return NewFakeBroker(), nil, nil
	case "live":
		appID := strings.TrimSpace(os.Getenv("ARROW_APP_ID"))
		appSecret := strings.TrimSpace(os.Getenv("ARROW_APP_SECRET"))
		if appID == "" || appSecret == "" {
			return nil, nil, fmt.Errorf("live mode requires Arrow credentials inside the bridge")
		}
		client := arrow.NewClient(appID, appSecret)
		user, password, totp := os.Getenv("ARROW_USER_ID"), os.Getenv("ARROW_PASSWORD"), os.Getenv("ARROW_TOTP_KEY")
		if user == "" || password == "" || totp == "" {
			return nil, nil, fmt.Errorf("live mode requires ARROW_USER_ID+PASSWORD+TOTP_KEY (ARROW_TOKEN removed 2026-08-24)")
		}
		if err := startupLogin(func() error { return client.AutoLogin(user, password, totp) }); err != nil {
			return nil, nil, err
		}
		inner, err := NewArrowBroker(client)
		if err != nil {
			return nil, nil, err
		}
		// Auto re-auth on 401/token_expired: refresh TOTP, retry once.
		broker := NewReauthBroker(inner, func(ctx context.Context) error {
			user, password, totp := os.Getenv("ARROW_USER_ID"), os.Getenv("ARROW_PASSWORD"), os.Getenv("ARROW_TOTP_KEY")
			if user == "" || password == "" || totp == "" {
				return fmt.Errorf("missing AutoLogin credentials for re-auth")
			}
			return loginWithinContext(ctx, func() error { return client.AutoLogin(user, password, totp) })
		})
		return broker, client, nil
	default:
		return nil, nil, fmt.Errorf("unsupported EXECUTION_BRIDGE_MODE %q", mode)
	}
}

func NewFakeBrokerWithDisabledResult() *FakeBroker {
	fake := NewFakeBroker()
	// A catch-all, not a deny list: an allow-by-omission list silently reports
	// SUCCESS for any command it predates while mode=disabled (P3-249).
	fake.SetDefaultResult(BrokerResult{Outcome: OutcomeUnknown, Reason: "broker_disabled"})
	return fake
}

// startupLogin is the mode=live boot login. It is a named seam so the cause
// handling is testable without posting to the venue (P3-247).
func startupLogin(login func() error) error {
	if err := login(); err != nil {
		// stderr here is operator-only, not the sanitized client boundary, so the
		// SDK's stage (login/totp/redirect/authenticate) and its cause are kept:
		// during a live outage "failed" alone forces blind reproduction.
		return fmt.Errorf("Arrow authentication failed: %w", err)
	}
	return nil
}

// newHTTPServer builds the bridge's HTTP server. Its timeouts are the only
// bound on a client that dribbles a body or reads a reply slowly (P3-472).
//
// Both bounds are derived, not fixed: Go resets WriteTimeout when the request
// header is read, so it spans the whole command including a venue call that is
// allowed to take commandTimeout, and a flat value would cut off legal slow
// replies for an operator who raised EXECUTION_BRIDGE_COMMAND_TIMEOUT_MS. Read
// only needs a small margin over the command because the body is 128KB and
// always precedes the handler (defaults: read 11s, write 15s).
func newHTTPServer(addr string, handler http.Handler, commandTimeout time.Duration) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       commandTimeout + httpReadTimeoutMargin,
		WriteTimeout:      commandTimeout + httpWriteTimeoutMargin,
		IdleTimeout:       httpIdleTimeout,
	}
}

// loginWithinContext runs an AutoLogin-shaped call under the caller's context
// (P3-248). AutoLogin takes no context and makes up to three sequential venue
// calls at 15s each, so without this a stalled auth outlives the command
// deadline and holds the dispatch goroutine in ReauthBroker.doWithReauth. The
// abandoned call runs to completion like brokerCall's, and its result is
// buffered so the goroutine cannot leak by blocking on the send.
func loginWithinContext(ctx context.Context, login func() error) error {
	result := make(chan error, 1)
	go func() { result <- login() }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-result:
		return err
	}
}

const (
	httpReadHeaderTimeout  = 5 * time.Second
	httpReadTimeoutMargin  = 1 * time.Second
	httpWriteTimeoutMargin = 5 * time.Second
	httpIdleTimeout        = 60 * time.Second
)

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
