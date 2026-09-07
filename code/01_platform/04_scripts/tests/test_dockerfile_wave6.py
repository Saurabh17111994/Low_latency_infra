"""Wave 6 Dockerfile security tests — P1-135/136/137 — static, no daemon."""
import re
from pathlib import Path

ROOT = Path(__file__).parents[4]
DOCKERFILE = ROOT / "code/02_services/01_ingestion/Dockerfile"

SHA = r"@sha256:[0-9a-f]{64}"


def read():
    return DOCKERFILE.read_text()


def test_P1_135_all_from_images_pinned():
    """P1-135: every FROM base is digest-pinned — a bare tag floats."""
    text = read()
    froms = re.findall(r"^FROM\s+(\S+)", text, re.M)
    assert len(froms) == 3, f"P1-135: expected 3 FROM stages, got {froms}"
    for img in froms:
        assert re.search(SHA, img), f"P1-135: base not digest-pinned: {img}"


def test_P1_136_otel_agent_checksummed():
    """P1-136: the OTel agent ADD carries --checksum — a hijacked release
    must fail the build, not own the fleet."""
    text = read()
    m = re.search(r"^ADD\s+(\S.*opentelemetry-javaagent\.jar\s+\S+)", text, re.M)
    assert m, "P1-136: OTel agent ADD missing"
    assert "--checksum=sha256:" in m.group(1), "P1-136: ADD lacks --checksum"


def test_P1_137_runtime_non_root():
    """P1-137: runtime drops root — USER 65532 AFTER the root-only setup
    (chmod/chown), with the log dir owned by the runtime uid."""
    text = read()
    assert re.search(r"^USER\s+65532", text, re.M), "P1-137: USER 65532 missing"
    user_pos = text.index("USER 65532")
    assert "ENTRYPOINT" in text[user_pos:], "P1-137: USER must precede ENTRYPOINT"
    assert "chown" in text and "65532" in text, "P1-137: log/app ownership not granted"
    # chmod of the binaries must stay in the root stage (before USER drop).
    assert text.index("chmod +x") < user_pos, "P1-137: chmod must run as root, before USER"


LOADGEN = ROOT / "code/02_services/01_ingestion/Dockerfile.loadgen"
ENTRYPOINT = ROOT / "code/02_services/01_ingestion/docker-entrypoint.sh"


def test_P1_135_loadgen_pins_match_production():
    """P1-135 follow-up: loadgen uses the SAME 3 digests as production —
    prod/loadgen base drift reintroduces the float in one image."""
    prod, load = read(), LOADGEN.read_text()
    prod_digests = set(re.findall(r"@sha256:[0-9a-f]{64}", prod))
    load_digests = set(re.findall(r"@sha256:[0-9a-f]{64}", load))
    assert len(prod_digests) == 3, f"P1-135: prod must pin 3 digests, got {prod_digests}"
    assert prod_digests == load_digests, (
        f"P1-135: loadgen digests {sorted(load_digests)} drift from prod {sorted(prod_digests)}")


def test_P1_137_parent_dir_owned():
    """P1-137 follow-up: /data/ingestion PARENT is chowned — the journal
    FILE lives there (UncertaintyJournal default), not in logs/."""
    text = read()
    assert "chown" in text and "/data/ingestion" in text, "P1-137: parent chown missing"
    # The chown RUN line must include the bare parent, not only logs/.
    run_lines = [ln for ln in text.splitlines() if "chown" in ln]
    assert any("/data/ingestion" in ln and "/data/ingestion/logs" not in ln.replace("/data/ingestion ", "") or "chown -R 65532:65532 /data/ingestion " in ln or ln.rstrip().endswith("/data/ingestion") or "/data/ingestion " in ln for ln in run_lines), \
        f"P1-137: chown RUN line misses bare parent: {run_lines}"


def test_P1_137_entrypoint_probes_writable_dirs():
    """P1-137 follow-up: entrypoint FATAL-probes LOG_DIR + journal parent
    before Java starts — a root-owned stale volume fails in 1s, not late."""
    text = ENTRYPOINT.read_text()
    assert "probe_writable_dir" in text, "P1-137: probe helper missing"
    assert "LOG_DIR" in text, "P1-137: LOG_DIR probe missing"
    assert "UNCERTAINTY_JOURNAL_PATH" in text, "P1-137: journal-parent probe missing"
    assert text.count("FATAL") >= 2, "P1-137: probes must FATAL with migration hint"
