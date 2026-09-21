"""T2/T6 — contract tests for values_at_rest_scan.py.

The plan's two workstation properties are only as good as their negative
controls, so each is exercised here as planted-then-removed:

* T2's control is `ARROW_APP_SECRET=dummy1234567890` planted under a scan root
  outside the repository — the scan must fail, and the value must not appear in
  its output;
* T6's control is `aws_secret_access_key = "dummy1234567890abcdef"` inside a
  transcript dated in the window — it must be flagged, and the same file must be
  skipped once `--since` moves past it. The transcript body is written as JSON,
  because that is how a real transcript stores it.

Three properties matter more than the matching itself: a hits-free result must
never come from a scan that did not run (a missing root is rc=2, a bad date is
rc=2), a hit inside this public repository is printed but only fails under
`--strict`, and no output line ever carries the raw value.

Pairs with CHG-289.
"""

import json
import os
import pathlib
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[4]
SCANNER = ROOT / "code/01_platform/04_scripts/values_at_rest_scan.py"
PLAN_DIR = ROOT / "docs/plans"
PLAN = PLAN_DIR / "2026-09-21-post-verification-plan.md"

T2_DECOY = "ARROW_APP_SECRET=dummy1234567890"
T6_DECOY = 'aws_secret_access_key = "dummy1234567890abcdef"'

CANONICAL_NAMES = {
    "arrow_app_secret", "arrow_password", "arrow_totp_key",
    "aws_access_key_id", "aws_secret_access_key",
    "execution_bridge_auth_token", "gateway_shared_secret",
    "o2_auth_basic", "o2_password",
}


def run(*args):
    proc = subprocess.run(
        [sys.executable, str(SCANNER), *args],
        capture_output=True, text=True,
    )
    return proc.returncode, proc.stdout, proc.stderr


def write(root, relative, body):
    path = pathlib.Path(root) / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    return path


def test_clean_root_passes(tmp_path):
    write(tmp_path, "notes/readme.txt", "nothing to see here\n")
    rc, out, _ = run("--root", str(tmp_path))
    assert rc == 0, out
    assert "hits=0" in out


def test_t2_decoy_outside_repo_fails_without_printing_the_value(tmp_path):
    path = write(tmp_path, "p2/prod.env", f"# local\n{T2_DECOY}\n")
    rc, out, _ = run("--root", str(tmp_path))
    assert rc == 1, out
    assert "arrow_app_secret" in out.lower()
    assert f"{path}:2" in out
    assert "len=15" in out
    # The value never reaches stdout — only its masked preview.
    assert "dummy1234567890" not in out
    assert "dumm...7890" in out
    # The plan's controls are decoys, and the report must say so without ever
    # printing why.
    assert "decoy=yes" in out


def test_decoy_removed_passes_again(tmp_path):
    path = write(tmp_path, "p2/prod.env", f"{T2_DECOY}\n")
    assert run("--root", str(tmp_path))[0] == 1
    path.unlink()
    assert run("--root", str(tmp_path))[0] == 0


def test_t6_decoy_in_a_new_transcript_fails_and_respects_since(tmp_path):
    body = json.dumps({"type": "user", "text": T6_DECOY}) + "\n"
    new = write(tmp_path, "sessions/2026-09-22T10-00-00-000Z_aaaa.jsonl", body)
    old = write(tmp_path, "sessions/2026-09-01T10-00-00-000Z_bbbb.jsonl", body)

    rc, out, _ = run("--root", str(tmp_path), "--since", "2026-09-21")
    assert rc == 1, out
    assert str(new) in out
    assert str(old) not in out
    assert "skipped_old=1" in out
    # The escaped-quote form is what a transcript actually holds.
    assert "dummy1234567890abcdef" not in out

    rc, out, _ = run("--root", str(tmp_path), "--since", "2026-01-01")
    assert rc == 1, out
    assert str(old) in out


def test_placeholders_paths_and_short_values_are_not_hits(tmp_path):
    write(tmp_path, "prod.env", "\n".join([
        "O2_PASSWORD=",
        "O2_PASSWORD=${O2_PASSWORD:?operator sets this on the VM at S7}",
        "ARROW_PASSWORD=<your-password>",
        'ARROW_TOTP_KEY=""',
        "AWS_SECRET_ACCESS_KEY_FILE=/run/secrets/aws_secret_access_key",
        # Four characters: below the documented six-character floor, so this is
        # the one line here that is genuinely not a value.
        "AWS_ACCESS_KEY_ID=ab12",
        "ARROW_APP_SECRET=changeme",
        "EXECUTION_BRIDGE_AUTH_TOKEN=$SOMETHING_FROM_ENV",
    ]) + "\n")
    rc, out, _ = run("--root", str(tmp_path))
    assert rc == 0, out
    assert "hits=0" in out


def test_instruction_style_and_relative_path_values_are_not_hits(tmp_path):
    """The two shapes a real 500k-file sweep of this workstation produced.

    `PUT_YOUR_SECRET_HERE` sat in the shell history and a relative
    `code/.../01_docker/.env` path was the "value" of a Makefile line. Neither
    can be a secret: one is an instruction to the reader, the other is a
    reference to a file. Both were reported as hits before the classification
    pass, which is why they are pinned here.
    """
    write(tmp_path, "prod.env", "\n".join([
        "ARROW_APP_SECRET=PUT_YOUR_SECRET_HERE",
        "O2_PASSWORD=code/01_platform/01_docker/.env",
        # A bare brace reference: an assignment regex that eats a leading `$`
        # reduces `${O2_PASSWORD}` to this shape.
        "O2_PASSWORD={O2_PASSWORD}",
    ]) + "\n")
    rc, out, _ = run("--root", str(tmp_path))
    assert rc == 0, out
    assert "hits=0" in out


def test_a_base64_like_value_is_still_a_hit(tmp_path):
    """The boundary of the path rule: base64 contains `/`, `+` and `=`.

    A path filter that is too eager would swallow a real secret, so this pins
    the line. Slashes alone must never demote a value to a path.
    """
    write(tmp_path, "prod.env",
          "AWS_SECRET_ACCESS_KEY=Zm9vL2JhcitxdXg9dGhpcy9sb29rcy9saWtlL2Jhc2U2NA==\n")
    rc, out, _ = run("--root", str(tmp_path))
    assert rc == 1, out
    assert "hits=1" in out
    # The mask shows the first and last four characters by design; what must
    # never appear is the value itself.
    assert "Zm9vL2JhcitxdXg9dGhpcy9sb29rcy9saWtlL2Jhc2U2NA==" not in out
    assert "preview=Zm9v...NA==" in out


def test_a_smoke_fixture_value_is_counted_as_a_decoy(tmp_path):
    """`smoke_secret` is still a hit, but it must not read as unclassified."""
    write(tmp_path, "prod.env", "ARROW_APP_SECRET=smoke_secret\n")
    rc, out, _ = run("--root", str(tmp_path))
    assert rc == 1, out
    assert "decoy=yes" in out


def test_repo_hits_are_printed_and_only_fail_under_strict():
    # The plan documents T2's decoy in its own must-fail column, so the
    # repository is a real, write-free source of in-repo hits.
    assert T2_DECOY in PLAN.read_text(), "plan decoy missing"
    rc, out, _ = run("--root", str(PLAN_DIR), "--names", "arrow_app_secret")
    assert rc == 0, out
    assert "in-repo" in out
    assert "in-repo=0" not in out

    rc, out, _ = run("--root", str(PLAN_DIR), "--names", "arrow_app_secret",
                     "--strict")
    assert rc == 1, out


def test_names_override_removes_the_hit(tmp_path):
    # Proves the hit came from the canonical name list, not from luck.
    write(tmp_path, "p2/prod.env", f"{T2_DECOY}\n")
    assert run("--root", str(tmp_path))[0] == 1
    rc, out, _ = run("--root", str(tmp_path), "--names", "some_other_key")
    assert rc == 0, out
    assert "hits=0" in out


def test_shape_pattern_catches_a_nameless_token(tmp_path):
    token = "ghp_" + "a1b2c3d4e5" * 4
    write(tmp_path, "scratch.txt", f"token = {token}\n")
    rc, out, _ = run("--root", str(tmp_path))
    assert rc == 1, out
    assert "github_token" in out
    assert token not in out
    assert "preview=ghp_" in out
    assert "decoy=no" in out


def test_missing_root_is_rc2_never_zero_hits(tmp_path):
    rc, out, err = run("--root", str(tmp_path / "does-not-exist"))
    assert rc == 2
    assert "not a directory" in err
    assert "hits=0" not in out


def test_bad_since_is_rc2(tmp_path):
    rc, _, err = run("--root", str(tmp_path), "--since", "21-09-2026")
    assert rc == 2
    assert "YYYY-MM-DD" in err


def test_json_evidence_carries_the_fields_and_counts(tmp_path):
    write(tmp_path, "p2/prod.env", f"{T2_DECOY}\n")
    rc, out, _ = run("--root", str(tmp_path), "--json")
    assert rc == 1
    payload = json.loads(out)
    assert payload["failed"]
    assert payload["counts"]["outside"] == 1
    assert payload["counts"]["total"] == 1
    hit = payload["hits"][0]
    assert hit["name"].lower() == "arrow_app_secret"
    assert hit["length"] == 15
    assert hit["preview"] == "dumm...7890"
    assert hit["decoy"] is True
    assert payload["counts"]["decoys"] == 1
    assert "dummy1234567890" not in out


def test_default_names_come_from_the_bootstrap_script(tmp_path):
    rc, out, _ = run("--root", str(tmp_path), "--json")
    assert rc == 0, out
    assert {n.lower() for n in json.loads(out)["names"]} == CANONICAL_NAMES


def test_a_named_pipe_is_counted_not_opened(tmp_path):
    """A fifo has no reader, so `open()` on one waits forever.

    Measured on this workstation: the first full `$HOME` scan sat in
    `wait_for_partner` for 26 minutes on `~/.steam/steam.pipe` and never
    produced a result. The timeout below is the regression guard — without the
    regular-file check this test fails loudly instead of hanging the suite.
    """
    os.mkfifo(tmp_path / "steam.pipe")
    write(tmp_path, "notes.txt", "plain text\n")
    proc = subprocess.run(
        [sys.executable, str(SCANNER), "--root", str(tmp_path)],
        capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 0, proc.stdout
    assert "skipped_special=1" in proc.stdout
    assert "hits=0" in proc.stdout
