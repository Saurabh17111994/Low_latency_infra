//go:build faketool

package main

import (
	"bytes"
	"os/exec"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
)

// P1-151: closePending + real-rate must still serve ticks. The ticker gate
// once skipped closePending connections while real-rate also skips the
// snapshot burst, so that combo sent only sub responses — zero tick frames.
func TestFaketoolClosePendingRealRateServesTicks(t *testing.T) {
	if testing.Short() {
		t.Skip("needs a live subprocess server")
	}
	bin := t.TempDir() + "/faketool-p1151"
	if out, err := exec.Command("go", "build", "-tags", "faketool",
		"-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build server: %v\n%s", err, out)
	}
	var errBuf bytes.Buffer
	srv := exec.Command(bin, "-port", "18992", "-real-rate", "-real-rate-hz", "20",
		"-disconnect-after", "1", "-close-linger", "3s")
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
		conn, _, err = websocket.DefaultDialer.Dial("ws://127.0.0.1:18992/", nil)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if conn == nil {
		t.Fatalf("dial server: never came up; stderr:\n%s", errBuf.String())
	}
	defer conn.Close()
	if err := conn.WriteJSON(map[string]any{
		"code":   "sub",
		"symIds": []any{map[string]any{"ids": []any{float64(757614)}}},
	}); err != nil {
		t.Fatalf("sub write: %v", err)
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("zstd reader: %v", err)
	}
	defer dec.Close()
	ticks := 0
	deadline := time.Now().Add(2500 * time.Millisecond)
	for ticks < 3 && time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		mt, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}
		if mt != websocket.BinaryMessage {
			continue
		}
		raw, err := dec.DecodeAll(msg, nil)
		if err != nil {
			continue
		}
		if len(raw) == hftSizeFull && raw[2] == hftPktFull {
			ticks++
		}
	}
	if ticks < 3 {
		t.Fatalf("closePending+real-rate served %d tick frames, want >=3; stderr:\n%s",
			ticks, errBuf.String())
	}
}
