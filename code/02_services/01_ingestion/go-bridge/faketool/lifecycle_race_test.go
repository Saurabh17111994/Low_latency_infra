//go:build faketool

package main

import (
	"bytes"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// TestFaketoolConnTeardownClean — P1-026: a client that subscribes, takes a
// few frames, then disconnects abruptly must not trip a data race or panic
// in the per-connection teardown (encoder closed while senders run). The
// server runs under -race; any race/panic in its stderr fails the test.
func TestFaketoolConnTeardownClean(t *testing.T) {
	if testing.Short() {
		t.Skip("needs a live subprocess server")
	}
	// Build once, exec the binary directly: `go run` wraps the server in a
	// child process that ignores the parent's Kill (hung srv.Wait).
	bin := t.TempDir() + "/faketool-race"
	if out, err := exec.Command("go", "build", "-race", "-tags", "faketool",
		"-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build race server: %v\n%s", err, out)
	}
	var errBuf bytes.Buffer
	srv := exec.Command(bin, "-port", "18991", "-tick-interval-ms", "20")
	srv.Stderr = &errBuf
	if err := srv.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	defer func() {
		_ = srv.Process.Kill()
		_ = srv.Wait()
	}()

	var conn *websocket.Conn
	for i := 0; i < 100; i++ {
		var err error
		conn, _, err = websocket.DefaultDialer.Dial("ws://127.0.0.1:18991/", nil)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if conn == nil {
		t.Fatalf("dial server: never came up; stderr:\n%s", errBuf.String())
	}
	if err := conn.WriteJSON(map[string]any{
		"code":   "sub",
		"symIds": []any{map[string]any{"ids": []any{float64(757614)}}},
	}); err != nil {
		t.Fatalf("sub write: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for i := 0; i < 3; i++ {
		if _, _, err := conn.ReadMessage(); err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
	}
	// Abrupt disconnect: raw TCP close, no close frame — the read loop must
	// die, done must stop the senders, encoder must close last.
	if err := conn.UnderlyingConn().Close(); err != nil {
		t.Fatalf("abrupt close: %v", err)
	}
	time.Sleep(2 * time.Second)

	out := errBuf.String()
	if strings.Contains(out, "WARNING: DATA RACE") {
		t.Fatalf("data race in teardown:\n%s", out)
	}
	if strings.Contains(out, "panic:") {
		t.Fatalf("panic in teardown:\n%s", out)
	}
}
