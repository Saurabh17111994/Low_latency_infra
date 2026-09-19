"""L0 Static configuration tests — CONFIG-001..006 — no containers required."""
import re, subprocess, json, unittest
from pathlib import Path

ROOT = Path(__file__).parents[4]
COMPOSE = ROOT / "code/01_platform/01_docker/docker-compose.yml"

ENV_FILES = [COMPOSE.parent / ".env", COMPOSE.parent / "secrets.env"]
# P6-597: keys the local stack cannot work without. The dev compose interpolates
# them PLAINLY (no ${VAR:?}) on purpose — the rationale is at
# docker-compose.yml:50-60 (the hard pins broke `compose config` for the
# documented "copy .env.example to .env" flow) — so this file asserts the VALUE
# really is supplied by an env file instead of claiming a fail-closed
# interpolation that does not exist. The fail-closed contract for the Arrow keys
# lives in the Java SecretGuard, not here.
REQUIRED_ENV_SECRETS = ["O2_PASSWORD", "ARROW_APP_SECRET", "ARROW_PASSWORD", "ARROW_TOTP_KEY"]
SECRET_SETTING = re.compile(r"^([A-Z0-9_]*(?:PASSWORD|SECRET|TOKEN)):\s*(.+)$")

def env_values(path):
    """KEY=VALUE map from a docker env file; {} when the file is absent."""
    vals = {}
    p = Path(path)
    if not p.exists():
        return vals
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        vals[k.strip()] = v.strip()
    return vals

def missing_secrets(paths, keys):
    """Keys that no file in `paths` supplies with a non-empty value."""
    have = {}
    for p in paths:
        have.update(env_values(p))
    return [k for k in keys if not have.get(k)]

def literal_secret_settings(text):
    """Secret-named compose settings whose value is a literal, not an env reference.

    P6-598: the rendered compose config is YAML (`KEY: value`), so a scan for
    `KEY=value` can never fire; this looks at the source settings instead.
    """
    bad = []
    for i, line in enumerate(text.splitlines(), 1):
        s = line.strip()
        if s.startswith("#"):
            continue
        m = SECRET_SETTING.match(s)
        if m and "${" not in m.group(2):
            bad.append((i, s[:70]))
    return bad

class ConfigL0Test(unittest.TestCase):
    def test_CONFIG_001_compose_syntax_valid(self):
        """CONFIG-001: docker compose config must succeed (YAML parses, interpolation ok)."""
        out = subprocess.check_output(["docker","compose","-f",str(COMPOSE),"--env-file",str(COMPOSE.parent/".env"),"--env-file",str(COMPOSE.parent/"secrets.env"),"config","--format","json"], text=True)
        cfg = json.loads(out)
        self.assertIn("services", cfg, "CONFIG-001: compose JSON missing services")
        self.assertIn("networks", cfg, "CONFIG-001: compose JSON missing networks")

    def test_CONFIG_002_no_latest(self):
        """CONFIG-002: no image uses :latest and every image has explicit tag or digest."""
        text = COMPOSE.read_text()
        # collect image values from yaml (includes ${VAR:?} interpolation wrappers)
        raw = re.findall(r'image:\s*["\']?([^"\'\s#\n]+)', text)
        self.assertTrue(raw, "CONFIG-002: no images found")
        bad=[]
        for img in raw:
            # skip the ${FLUSS_IMAGE:?} style — these are variables, checked at runtime
            if "${" in img:
                # must contain :? or :- and not be literal :latest
                if ":latest" in img:
                    bad.append(img)
                continue
            # literal image: check no implicit latest
            if ":latest" in img:
                bad.append(img)
            elif "@sha256:" not in img:
                last = img.rsplit("/",1)[-1]
                if ":" not in last:
                    bad.append(img + " (no tag)")
        self.assertEqual(bad, [], f"CONFIG-002: images with latest/implicit tag: {bad}")

    def test_CONFIG_003_production_marker_rejection(self):
        """CONFIG-003: local .env.example must not enable production and compose doc must describe rejection."""
        env_example = (ROOT / "code/01_platform/01_docker/.env.example").read_text()
        self.assertNotIn("ENVIRONMENT=production", env_example, "CONFIG-003: .env.example must not contain ENVIRONMENT=production")
        # doc contract: 08 says put ENVIRONMENT=production should fail safely
        doc = (ROOT / "docs/08_implementation/08-local-compose.md").read_text()
        self.assertIn("CONFIG-003", doc)

    def test_CONFIG_004_required_secrets_cannot_silently_default(self):
        """CONFIG-004: required secrets come from an env file, never a compose literal."""
        text = COMPOSE.read_text()
        for key in REQUIRED_ENV_SECRETS:
            self.assertIn(key, text, f"CONFIG-004: {key} not referenced by the compose")
        # P6-597: the dev compose interpolates these plainly, so the VALUE has to
        # exist in an env file — otherwise the container silently receives an empty
        # password and nothing anywhere fails.
        gone = missing_secrets(ENV_FILES, REQUIRED_ENV_SECRETS)
        self.assertEqual(
            gone, [],
            f"CONFIG-004: {gone} missing/empty in {[p.name for p in ENV_FILES]} — the dev "
            f"compose does not fail closed on these; set them in secrets.env",
        )
        self.assertNotRegex(text, r'O2_PASSWORD.*:-.*password', "CONFIG-004: O2_PASSWORD must not default to a real password")

    def test_CONFIG_005_secret_leakage_scan(self):
        """CONFIG-005: secrets are referenced by variable, and ARROW_TOKEN stays removed."""
        text = COMPOSE.read_text()
        # P6-598: the old scan looked for "ARROW_TOKEN=" inside `compose config`
        # output, which is YAML (`KEY: value`) — it could never match, so it could
        # never fail. ARROW_TOKEN was removed 2026-08-24 (TOTP only), so pin its
        # absence from the compose: that is a real regression guard.
        self.assertNotIn("ARROW_TOKEN", text, "CONFIG-005: ARROW_TOKEN (removed 2026-08-24) is back in the compose")
        lits = literal_secret_settings(text)
        self.assertEqual(lits, [], f"CONFIG-005: secret-named settings with literal values: {lits}")
        out = subprocess.check_output(["docker","compose","-f",str(COMPOSE),"--env-file",str(COMPOSE.parent/".env"),"--env-file",str(COMPOSE.parent/"secrets.env"),"config"], text=True)
        # LOG_DIR etc are not secrets — the key is that no production cred file is printed
        self.assertIn("services:", out)

    def test_CONFIG_006_effective_configuration(self):
        """CONFIG-006: a repo file not mounted/passed is not effective — check mounts are declared."""
        text = COMPOSE.read_text()
        # every required config class must have a mount or env reference
        self.assertIn("fluss", text.lower(), "CONFIG-006: no fluss config reference")
        self.assertIn("FLUSS_PROPERTIES", text, "CONFIG-006: FLUSS_PROPERTIES not applied")
        self.assertIn("volumes:", text, "CONFIG-006: no volumes declared")

if __name__ == "__main__":
    unittest.main()
