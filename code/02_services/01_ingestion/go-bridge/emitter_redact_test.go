package main

import (
	"strings"
	"testing"
)

// TestSanitizeDiagnosticRedaction — P1-024: quoted-JSON and generic
// name=value secret shapes must redact; plain prose must survive.
func TestSanitizeDiagnosticRedaction(t *testing.T) {
	leaks := []string{
		`"ARROW_TOKEN":"secret"`,
		`"ARROW_APP_SECRET" : "s"`,
		`{"token": "abc", "other": 1}`,
		`password=abc123`,
		`secret=topsecret`,
		`ARROW_PASSWORD=hunter2&next=1`,
		`Authorization=Bearer secretToken`,
	}
	for _, in := range leaks {
		got := sanitizeDiagnostic(in)
		if strings.Contains(got, "secret") && !strings.Contains(got, "REDACTED") {
			t.Fatalf("leak not redacted: %q -> %q", in, got)
		}
		if got == in {
			t.Fatalf("unchanged, nothing redacted: %q", in)
		}
		if strings.Contains(got, "abc123") || strings.Contains(got, "topsecret") || strings.Contains(got, "hunter2") {
			t.Fatalf("secret value survived: %q -> %q", in, got)
		}
	}
	// Bearer two-pass interplay: the real token after "Bearer " must go.
	if got := sanitizeDiagnostic(`Authorization=Bearer secretToken`); strings.Contains(got, "secretToken") {
		t.Fatalf("bearer token survived: %q", got)
	}

	clean := []string{
		`disconnect: token expired for slot hft-0`,
		`my token is abc`,
		`tokens=5`,
		`secretary=x`,
		`slot hft-0 connected, 1024 tokens subscribed`,
	}
	for _, in := range clean {
		if got := sanitizeDiagnostic(in); got != in {
			t.Fatalf("clean text altered: %q -> %q", in, got)
		}
	}
}
