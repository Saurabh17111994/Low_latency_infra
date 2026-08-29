package main

// Test-only subprocess helper (compiled with the test binary): re-executes
// the CURRENT test binary with -test.run=TestBridgeEmitterFatalProbe so the
// real initBridgeEmitter (which os.Exit(2)s on non-proto TRANSPORT) runs in
// a child process. The child prints the emitter type for proto/grpc and
// exits 2 for anything else — mirroring the bridge's FATAL path.

import (
	"fmt"
	"io"
	"os"
	"testing"
)

// TestBridgeEmitterFatalProbe is the subprocess entry: when the env marker
// BRIDGE_EMITTER_PROBE=1 is set, call the REAL initBridgeEmitter and exit.
func TestBridgeEmitterFatalProbe(t *testing.T) {
	if os.Getenv("BRIDGE_EMITTER_PROBE") != "1" {
		return // not the subprocess invocation
	}
	v := os.Getenv("TRANSPORT")
	switch v {
	case "proto", "grpc":
		e := initBridgeEmitter(io.Discard)
		fmt.Fprintf(os.Stderr, "emitter=%T\n", e)
		os.Exit(0)
	default:
		initBridgeEmitter(io.Discard) // exits 2 with FATAL stderr
		os.Exit(0)                    // unreachable
	}
}
