"""P1-162: vendored Login() (stdin-blocking) must have zero own-code callers.

AutoLogin (TOTP) is the only auth path (ARROW_TOKEN removed 2026-08-24).
Login owns stdin (Scanln, no timeout) — any caller reintroduces the hang.
"""
from pathlib import Path

BRIDGE = Path(__file__).resolve().parents[3] / "code/02_services/01_ingestion/go-bridge"
ALLOW = {"third_party/go-arrow/arrow/auth.go"}  # definition site only


def test_P1_162_no_login_callers():
    hits = []
    for f in list(BRIDGE.glob("*.go")) + list(BRIDGE.glob("cmd/*/*.go")):
        text = f.read_text()
        if ".Login(" in text:
            hits.append(f.name)
    assert not hits, f"P1-162: .Login( callers must stay zero (AutoLogin-only): {hits}"


def test_P1_162_no_arow_token_env():
    """ARROW_TOKEN device flow stays removed (dead since 2026-08-24)."""
    hits = []
    for f in list(BRIDGE.glob("*.go")) + list(BRIDGE.glob("cmd/*/*.go")):
        for i, line in enumerate(f.read_text().splitlines(), 1):
            if "ARROW_TOKEN" in line and "removed" not in line and "REMOVED" not in line:
                hits.append(f"{f.name}:{i}")
    assert not hits, f"ARROW_TOKEN references must stay removal-notes only: {hits}"
