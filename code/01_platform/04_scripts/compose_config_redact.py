#!/usr/bin/env python3
"""compose_config_redact.py — keep a rendered compose config, drop its values.

`docker compose config` interpolates: its output carries the real value of every
variable it resolves, from the `--env-file` arguments and from its own
environment. Logging that output therefore writes credentials to disk — measured
2026-09-21, when 15 soak logs held the live Arrow password and TOTP key, and the
mechanism would have produced a sixteenth.

The gate's compose-config step still has to run the render: that is how a missing
`${VAR:?}` is caught. So this filter sits between the render and the log and
replaces every value it can identify, leaving the shape the step validates.

Identification is by exact value, never by name pattern: candidates are the values
in the process environment and in each `--env-file`, minus the standard shell
variables (replacing `PATH`'s value would mangle the log for no security gain) and
minus values shorter than MIN_LEN or shaped like a number or a boolean (replacing
`true` or `5432` everywhere would destroy the log's readability). Over-redaction
is the safe direction; a fragment of a real value is not, so the longest candidate
is replaced first.

Usage:
    docker compose ... config 2>&1 | compose_config_redact.py --env-file .env
Exit: 0 when the input was read and written; 2 on a usage error. The caller must
      take the *render's* status from PIPESTATUS[0] — this filter never judges
      whether the config is valid, only what may be written down.
"""

import argparse
import os
import re
import sys

MIN_LEN = 8
PLACEHOLDER = "REDACTED"
# Not secrets: replacing these everywhere would make the log unreadable.
NOT_A_SECRET = re.compile(r"^(\d+(\.\d+)?|true|false|yes|no|on|off|null|none)$", re.IGNORECASE)
# Standard shell variables whose values are not credentials but do appear in logs.
STANDARD_ENV = frozenset({
    "PATH", "HOME", "PWD", "OLDPWD", "SHELL", "TERM", "LANG", "LANGUAGE", "LC_ALL",
    "USER", "LOGNAME", "HOSTNAME", "SHLVL", "_", "TMPDIR", "EDITOR", "PAGER", "PS1",
})


def candidate_values(env_files):
    """Every value worth replacing, longest first."""
    values = {value for name, value in os.environ.items() if name not in STANDARD_ENV}
    for path in env_files:
        try:
            text = open(path, encoding="utf-8", errors="ignore").read()
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            values.add(line.split("=", 1)[1].strip().strip('"').strip("'"))
    usable = [
        value for value in values
        if len(value) >= MIN_LEN and value != PLACEHOLDER and not NOT_A_SECRET.match(value)
    ]
    return sorted(usable, key=len, reverse=True)


def redact(text, values):
    """`text` with every candidate value replaced by the placeholder."""
    for value in values:
        text = text.replace(value, PLACEHOLDER)
    return text


def main(argv):
    parser = argparse.ArgumentParser(description="Redact values out of a rendered compose config.")
    parser.add_argument("--env-file", action="append", default=[], dest="env_files")
    args = parser.parse_args(argv)
    sys.stdout.write(redact(sys.stdin.read(), candidate_values(args.env_files)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
