package main

import (
	"bytes"
	"encoding/binary"
	"net"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
)

// CHG-513 (operator fake-broker test, 2026-10-02): the faketool's full tick
// must carry the day-stats + 5-level book section, not just ltp/ltq/volume.
// The compute market snapshot's presence gates read exactly these fields; with
// them zero the operator's 42 market values never reach the sealed
// candle_features rows, so a fake-broker run could not prove the storage.
//
// The layout mirrored here is cmd/gen-corpus/buildFullFrame, which the golden
// corpus pins byte-for-byte against the bridge decoder. The test builds and
// runs the real faketool binary so the served wire frame is asserted, not a
// copy of the writer.
func TestFaketoolFullFrameCarriesTheBookAndStats(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the faketool binary")
	}
	bin := filepath.Join(t.TempDir(), "faketool-book")
	if out, err := exec.Command("go", "build", "-tags", "faketool",
		"-o", bin, "./faketool").CombinedOutput(); err != nil {
		t.Fatalf("build faketool: %v\n%s", err, out)
	}
	port := freeTestPort(t)
	var errBuf bytes.Buffer
	srv := exec.Command(bin, "-port", strconv.Itoa(port), "-real-rate", "-real-rate-hz", "20")
	srv.Stderr = &errBuf
	if err := srv.Start(); err != nil {
		t.Fatalf("start faketool: %v", err)
	}
	defer func() {
		_ = srv.Process.Kill()
		_ = srv.Wait()
	}()

	url := "ws://127.0.0.1:" + strconv.Itoa(port) + "/"
	var conn *websocket.Conn
	for i := 0; i < 100; i++ {
		var err error
		conn, _, err = websocket.DefaultDialer.Dial(url, nil)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if conn == nil {
		t.Fatalf("dial faketool: never came up; stderr:\n%s", errBuf.String())
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

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		mt, msg, err := conn.ReadMessage()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			break
		}
		if mt != websocket.BinaryMessage {
			continue
		}
		raw, err := dec.DecodeAll(msg, nil)
		if err != nil || len(raw) != hftSizeFull || raw[2] != hftPktFull {
			continue
		}
		assertBookAndStatsFrame(t, raw)
		return
	}
	t.Fatalf("faketool served no full frame in 3s; stderr:\n%s", errBuf.String())
}

func freeTestPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func assertBookAndStatsFrame(t *testing.T, raw []byte) {
	t.Helper()
	ltp := int32(binary.LittleEndian.Uint32(raw[8:12]))
	if ltp <= 0 {
		t.Fatalf("served full frame ltp=%d, want >0", ltp)
	}
	if binary.LittleEndian.Uint32(raw[16:20]) == 0 {
		t.Errorf("vwap [16:20] is zero")
	}
	for _, off := range []int{20, 24, 32} { // day open, high, low
		if binary.LittleEndian.Uint32(raw[off:off+4]) == 0 {
			t.Errorf("day stat at [%d:%d] is zero", off, off+4)
		}
	}
	if binary.LittleEndian.Uint64(raw[48:56]) == 0 || binary.LittleEndian.Uint64(raw[56:64]) == 0 {
		t.Errorf("total buy/sell qty [48:64] are zero")
	}
	for i := 0; i < 5; i++ {
		bidPx := int32(binary.LittleEndian.Uint32(raw[72+i*4:]))
		askPx := int32(binary.LittleEndian.Uint32(raw[92+i*4:]))
		if bidPx <= 0 || bidPx >= ltp {
			t.Errorf("level %d bid px=%d (ltp=%d): want 0 < bid < ltp", i+1, bidPx, ltp)
		}
		if askPx <= ltp {
			t.Errorf("level %d ask px=%d (ltp=%d): want ask > ltp", i+1, askPx, ltp)
		}
		if binary.LittleEndian.Uint32(raw[112+i*4:]) == 0 {
			t.Errorf("level %d bid size is zero", i+1)
		}
		if binary.LittleEndian.Uint32(raw[132+i*4:]) == 0 {
			t.Errorf("level %d ask size is zero", i+1)
		}
		if binary.LittleEndian.Uint16(raw[152+i*2:]) == 0 {
			t.Errorf("level %d bid orders is zero", i+1)
		}
		if binary.LittleEndian.Uint16(raw[162+i*2:]) == 0 {
			t.Errorf("level %d ask orders is zero", i+1)
		}
	}
	if binary.LittleEndian.Uint64(raw[172:180]) == 0 {
		t.Errorf("open interest [172:180] is zero")
	}
}
