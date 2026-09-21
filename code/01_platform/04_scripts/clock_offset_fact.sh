#!/usr/bin/env bash
# arrow-infra — publish the host clock offset for the executor's drift gate (CHG-288).
#
# WHY A FILE AND NOT CHRONYD'S SOCKET
#   The Debian package owns /run/chrony as 0700 _chrony:_chrony (measured 2026-09-21: a `nobody`
#   process is denied even with the socket's group added), and the command socket is
#   unauthenticated — whoever can read it can also run `chronyc makestep`/`add server`, i.e. move
#   the clock. The executor container runs as `nobody` and parses untrusted market data; it gets
#   the number, never the control channel.
#
# WHY THE NUMBER IS THE SAME NUMBER
#   Field 4 of `chronyc tracking`'s `System time` line — exactly what `vm-bootstrap.sh --check` and
#   `prod_node_check.py` read, so the three checks cannot disagree about what the clock says.
#
# WHY A DIRECTORY
#   The file is written by replacing it (`mv`), so a reader must resolve the name at open time. A
#   container that bind-mounts a *file* holds the inode and would keep reading the old sample
#   forever. The deck therefore mounts this directory read-only and the executor opens
#   `$CLOCK_OFFSET_DIR/offset` each sample.
#
# FAIL-CLOSED: any failure exits non-zero and leaves the previous sample in place. It does not
# write a zero, and it does not delete the file — an old sample plus the consumer's staleness limit
# is what turns "nobody is publishing" into a halt instead of a comfortable lie.
#
# Env: CHRONYC (default: chronyc on PATH), CLOCK_OFFSET_DIR (default /run/arrow-clock).
set -eu

CHRONYC="${CHRONYC:-chronyc}"
DIR="${CLOCK_OFFSET_DIR:-/run/arrow-clock}"
FACT="${DIR}/offset"

die() { printf 'clock_offset_fact: %s\n' "$1" >&2; exit 1; }

tracking="$("$CHRONYC" tracking)" || die "'$CHRONYC tracking' failed (is chronyd running?)"
seconds="$(printf '%s\n' "$tracking" | awk '/^System time/{print $4}')"
[ -n "$seconds" ] || die "no 'System time' offset in '$CHRONYC tracking' output"

# Seconds -> milliseconds, rounded half away from zero, matching the Rust parser's f64::round.
# The field itself is validated first: awk coerces a word to 0, so converting before validating
# would publish `offset_ms=0` for `System time : seconds fast of NTP time` — a false "clock is
# fine" where the honest answer is "cannot measure".
ms="$(printf '%s' "$seconds" | awk '
    $1 !~ /^-?[0-9]+(\.[0-9]+)?$/ { exit 1 }
    { v = $1 * 1000; printf "%d", (v < 0 ? v - 0.5 : v + 0.5) }
')" || die "'$seconds' is not a numeric offset"

mkdir -p "$DIR" || die "cannot create $DIR"
chmod 0755 "$DIR" || die "cannot make $DIR traversable for the container user"

# Docker creates a *directory* where a missing bind-mount source was expected; replace it rather
# than moving the sample inside it, which would leave the gate reading a directory forever.
if [ -e "$FACT" ] && [ ! -f "$FACT" ]; then
    printf 'clock_offset_fact: %s is not a regular file — replacing it\n' "$FACT" >&2
    rm -rf -- "$FACT" || die "cannot replace $FACT"
fi

tmp="${FACT}.tmp.$$"
printf 'offset_ms=%s\nmeasured_epoch_s=%s\nsource=chronyc-tracking-field4\n' \
    "$ms" "$(date -u +%s)" > "$tmp" || die "cannot write $tmp"
chmod 0644 "$tmp" || die "cannot chmod $tmp"
mv -f -- "$tmp" "$FACT" || die "cannot publish $FACT"
