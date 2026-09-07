"""P1-162: vendored Login() (stdin-blocking) must have zero own-code callers repo-wide.

AutoLogin (TOTP) is the only auth path (ARROW_TOKEN removed 2026-08-24).
Login owns stdin (Scanln, no timeout) — any caller reintroduces the hang.
Scope: all own-code *.go under code/ (ingestion go-bridge, execution bridge,
capture-marketdata, faketool, ...); third_party excluded (definition site).
"""
from pathlib import Path

CODE = Path(__file__).resolve().parents[3] / "code"


def _own_go_files():
    return [f for f in CODE.rglob("*.go") if "third_party" not in f.parts]


def test_P1_162_no_login_callers():
    hits = []
    for f in _own_go_files():
        text = f.read_text()
        if ".Login(" in text:
            hits.append(str(f.relative_to(CODE)))
    assert not hits, f"P1-162: .Login( callers must stay zero (AutoLogin-only): {hits}"


def test_P1_162_no_arow_token_env():
    """ARROW_TOKEN device flow stays removed (dead since 2026-08-24)."""
    hits = []
    for f in _own_go_files():
        for i, line in enumerate(f.read_text().splitlines(), 1):
            if "ARROW_TOKEN" in line and "removed" not in line and "REMOVED" not in line:
                hits.append(f"{f.relative_to(CODE)}:{i}")
    assert not hits, f"ARROW_TOKEN references must stay removal-notes only: {hits}"
