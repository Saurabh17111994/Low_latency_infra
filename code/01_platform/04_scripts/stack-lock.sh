#!/usr/bin/env bash
# stack-lock.sh — serialize stack-mutating commands against a running gate.
#
# Why: one stack, two writers. A Monday gate holds an exclusive lock on
# logs/.monday-gates.lock, because every stack_generation it prints must describe
# a stack nobody is mutating. `make up` / `make images` / `make ddl-image` /
# `make down` / `make clean` mutate that same stack and, until this file existed,
# took no lock at all: on 2026-09-13 two certificate runs were refused by the
# preflight because a second worktree rebuilt the flink/compute images inside the
# window (and `make clean` — `compose down -v` — could have destroyed a running
# certificate's catalog outright).
#
# Usage:   stack-lock.sh <command> [args...]
# Default lock file: <repo>/logs/.monday-gates.lock — the SAME inode the gate
# locks, so the two cannot disagree about who holds the stack.
#
# Fail-closed by construction:
#   * another holder  -> print who, exit 4, run nothing
#   * flock(1) absent -> refuse; an unguarded mutation is not an option
#   * no arguments    -> usage error, and the lock is never taken
# STACK_LOCK_HELD (set by the gate once it holds the lock) is a pass-through:
# a command that is already inside a locked section cannot deadlock against it.
set -euo pipefail

if [ "$#" -eq 0 ]; then
	echo "usage: stack-lock.sh <command> [args...]" >&2
	exit 2
fi

# Already inside a locked section (the gate, or a script the gate called).
if [ -n "${STACK_LOCK_HELD:-}" ]; then
	exec "$@"
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
LOCK_DIR="${STACK_LOCK_DIR:-$ROOT/logs}"
LOCK_FILE="$LOCK_DIR/.monday-gates.lock"

mkdir -p "$LOCK_DIR"
if ! command -v flock >/dev/null 2>&1; then
	echo "stack-lock: flock(1) is missing — refusing to mutate the stack unguarded." >&2
	exit 4
fi

# 9 is the same descriptor number the gate uses. `<>` (not `>`) so that a
# contender cannot truncate the holder's pid record before its lock attempt fails.
exec 9<>"$LOCK_FILE"
if ! flock -n 9; then
	holder=$(cat "$LOCK_FILE" 2>/dev/null || true)
	echo "STACK BUSY — a gate or another stack command holds $LOCK_FILE${holder:+ (pid $holder)}." >&2
	echo "  Refusing to mutate the stack underneath it; re-run when it finishes." >&2
	exit 4
fi
printf '%s\n' "$$" >"$LOCK_FILE"

# exec keeps fd 9 (and the lock) alive for exactly as long as the command runs.
exec "$@"
