#!/usr/bin/env bash
# W41 Java probe — TokenCountReconcile column resolution and null accounting
# (P6-116, P6-425).
#
# The probe is a standalone `javac`-compiled class needing a live Fluss, so it
# cannot be executed here. What IS testable offline is the column contract and
# the accounting rules, via small harnesses compiled against the real Fluss
# client jar on the same classpath:
#
#   1. instrument_token resolves from the DDL-declared column order, not a
#      constant. Schema v3 (db513ee0) inserted event_day at index 0 and moved
#      instrument_token to 5; the old hardcoded 4 addressed connection_epoch,
#      which is also BIGINT — so the wrong column was counted with no error.
#      This asserts the resolved index against the DDL file itself, so any
#      future reordering fails here rather than in a reconciliation run.
#   2. A null token must not land in a real token's bucket (P6-425).
#
# Run directly:  bash test_token_count_reconcile.sh

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# tests/ → 04_scripts/ → 01_platform/ → code/
CODE_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
PROBE="$SCRIPT_DIR/../ing-tcp001/TokenCountReconcile.java"
RAW_DDL="$CODE_DIR/01_platform/02_sql/ddl/02_raw_table_1.sql"
QUAR_DDL="$CODE_DIR/01_platform/02_sql/ddl/21_ingestion_quarantine.sql"

FAILED=0
PASSED=0
ok() { echo "ok: $1"; PASSED=$((PASSED + 1)); }
bad() { echo "FAIL: $1"; FAILED=1; }

for f in "$PROBE" "$RAW_DDL" "$QUAR_DDL"; do
	if [ ! -r "$f" ]; then
		echo "FAIL: required file not readable: $f"
		exit 1
	fi
done

# ── Locate the Fluss client jars ─────────────────────────────────────────────
M2="${M2_REPO:-$HOME/.m2/repository}"
CLIENT_JAR="$(find "$M2/org/apache/fluss/fluss-client" -name 'fluss-client-*.jar' 2>/dev/null | head -1)"
COMMON_JAR="$(find "$M2/org/apache/fluss/fluss-common" -name 'fluss-common-*.jar' ! -name '*tests*' 2>/dev/null | head -1)"
if [ -z "$CLIENT_JAR" ] || [ -z "$COMMON_JAR" ]; then
	echo "SKIP: Fluss client jars not present under $M2 — cannot compile the probe."
	echo "W41 token-count reconcile: SKIP (jars unavailable)"
	exit 0
fi

# ── Compile the probe itself (P6-116/117/118/425/426/427 compile contract) ───
echo "=== contract: the probe compiles against the pinned Fluss client ==="
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
if javac -cp "$CLIENT_JAR:$COMMON_JAR" -d "$WORK/classes" "$PROBE" 2>"$WORK/javac.log"; then
	ok "TokenCountReconcile.java compiles"
else
	bad "TokenCountReconcile.java does not compile"
	sed 's/^/    /' "$WORK/javac.log" | head -20
	echo "W41 token-count reconcile: FAIL ($PASSED assertions passed)"
	exit 1
fi

# ── Column-order contract, read from the DDL ─────────────────────────────────
# column_index <table.sql> <column> — prints the 0-based index of a column in
# the CREATE TABLE body, in declaration order.
column_index() { # $1 = ddl file, $2 = column name
	awk '
		BEGIN { idx = 0 }
		/^CREATE TABLE/ { in_body = 1; next }
		in_body && /^\)/ { in_body = 0 }
		in_body {
			line = $0
			sub(/--.*/, "", line)
			gsub(/^[[:space:]]+/, "", line)
			gsub(/[[:space:]]+$/, "", line)
			if (line == "") next
			name = line
			sub(/[[:space:]].*$/, "", name)
			if (name == "" || name == "(") next
			if (name == col) { print idx; found = 1 }
			idx++
		}
		END { if (!found) print "absent" }
	' col="$2" "$1"
}

RAW_IDX="$(column_index "$RAW_DDL" instrument_token)"
QUAR_IDX="$(column_index "$QUAR_DDL" instrument_token)"
echo "=== DDL column order ==="
echo "  raw_table_1 instrument_token         -> index $RAW_IDX"
echo "  ingestion_quarantine instrument_token -> index $QUAR_IDX"

EVENT_DAY_IDX="$(column_index "$RAW_DDL" event_day)"
if [ "$EVENT_DAY_IDX" = "0" ]; then
	ok "event_day is column 0 (schema v3 partitioning) as expected"
else
	bad "event_day is at index $EVENT_DAY_IDX, not 0 — DDL order changed, update this test"
fi

# This is the regression the audit caught: the probe must not address index 4.
if [ "$RAW_IDX" != "4" ]; then
	ok "raw instrument_token is NOT at the old hardcoded index 4 (it is $RAW_IDX)"
else
	bad "raw instrument_token is back at index 4 — hardcoded-index bug would return"
fi

if [ "$QUAR_IDX" = "2" ]; then
	ok "quarantine instrument_token is at index 2 (unchanged)"
else
	bad "quarantine instrument_token is at index $QUAR_IDX, not 2 — DDL order changed"
fi

# The probe must not contain a positional token index for either table, and must
# resolve by name instead.
if grep -qE 'scan(Log|Batch)\(connection, [a-zA-Z]+, [0-9]+, ' "$PROBE"; then
	bad "probe still passes a positional token index to scanLog/scanBatch"
else
	ok "no positional token index remains in the scan calls"
fi
if grep -q 'TOKEN_COLUMN = "instrument_token"' "$PROBE"; then
	ok "probe resolves the token column by name"
else
	bad "probe does not define the token column name"
fi

# ── Null-token accounting (P6-425) ───────────────────────────────────────────
# A stub row drives countRow through the compiled class, so a null token must
# land in its own bucket and never in a real token's slot.
echo "=== null-token accounting (P6-425) ==="
mkdir -p "$WORK/harness"
cat > "$WORK/harness/NullAccountingHarness.java" <<'JAVA'
import java.lang.reflect.Method;
import java.util.Map;
import java.util.TreeMap;
import org.apache.fluss.row.InternalRow;
import org.apache.fluss.row.GenericRow;

public class NullAccountingHarness {
    public static void main(String[] args) throws Exception {
        Class<?> probe = Class.forName("TokenCountReconcile");
        Method countRow = probe.getDeclaredMethod(
                "countRow", InternalRow.class, int.class, Map.class, int.class);
        countRow.setAccessible(true);

        Map<Long, long[]> counts = new TreeMap<>();
        // Two rows with a real token, one with a null token. GenericRow is the
        // production row implementation, so null handling is the real one.
        countRow.invoke(null, GenericRow.of(7L), 0, counts, 0);
        countRow.invoke(null, GenericRow.of(7L), 0, counts, 0);
        countRow.invoke(null, GenericRow.of((Object) null), 0, counts, 0);

        long[] real = counts.get(7L);
        if (real == null || real[0] != 2) {
            System.out.println("FAIL: real token 7 got " + (real == null ? "no bucket" : real[0] + " rows"));
            System.exit(1);
        }
        if (counts.containsKey(-1L)) {
            System.out.println("FAIL: null token was attributed to real token -1");
            System.exit(1);
        }
        long nullTotal = 0;
        for (Map.Entry<Long, long[]> e : counts.entrySet()) {
            if (e.getKey() < 0) { nullTotal += e.getValue()[0]; }
        }
        if (nullTotal != 1) {
            System.out.println("FAIL: null token not kept in its own bucket (found " + nullTotal + ")");
            System.exit(1);
        }
        System.out.println("PASS: real token=2 rows, null token=1 row, token -1 untouched");
    }
}
JAVA
if javac -cp "$WORK/classes:$CLIENT_JAR:$COMMON_JAR" -d "$WORK/harness" \
	"$WORK/harness/NullAccountingHarness.java" 2>"$WORK/harness.log"; then
	if HARNESS_OUT="$(java -cp "$WORK/harness:$WORK/classes:$CLIENT_JAR:$COMMON_JAR" \
		NullAccountingHarness 2>&1)"; then
		ok "null token is not attributed to a real token (P6-425) — $HARNESS_OUT"
	else
		bad "null-token accounting: $HARNESS_OUT"
	fi
else
	bad "null-accounting harness did not compile"
	sed 's/^/    /' "$WORK/harness.log" | head -15
fi

# Comment-stripped probe source: patterns below match code, not the comments
# that record what was changed.
PROBE_CODE="$(sed 's:^[[:space:]]*//.*::' "$PROBE" | sed 's:[[:space:]]*//.*::')"

# ── Log scan must terminate (P6-117) ────────────────────────────────────────
# Guarding emptyPolls on "records seen" removes the old unconditional bound, so
# without a separate budget a genuinely empty bucket would spin forever instead
# of failing. Assert the budget exists.
echo "=== log scan terminates on an empty bucket (P6-117) ==="
if printf '%s\n' "$PROBE_CODE" | grep -q 'MAX_POLLS_BEFORE_FIRST_RECORD'; then
	ok "empty-bucket wait is bounded (no infinite poll loop)"
else
	bad "no first-record poll budget — an empty bucket would loop forever"
fi
if printf '%s\n' "$PROBE_CODE" | grep -q 'emptyPolls++'; then
	ok "end-of-log still requires empty polls after records were seen"
else
	bad "end-of-log detection lost its emptyPoll accounting"
fi

# ── Truncation cap must be gone (P6-118) ────────────────────────────────────
echo "=== full-table scan is not capped (P6-118) ==="
if printf '%s\n' "$PROBE_CODE" | grep -q 'limit(1_000_000_000)'; then
	bad "BatchScanner is still capped at 1e9 rows — counts can silently truncate"
else
	ok "BatchScanner has no row cap"
fi

# ── Slot parameter is honoured (P6-426, P6-427) ─────────────────────────────
echo "=== totals use the caller's slot (P6-426, P6-427) ==="
if grep -qE 'totals\[(0|1)\] = rows' "$PROBE"; then
	bad "a scan method still writes a hardcoded totals slot"
else
	ok "scan methods write totals[slot]"
fi

# ── Verdict ─────────────────────────────────────────────────────────────────
echo
if [ "$FAILED" = 1 ]; then
	echo "W41 token-count reconcile: FAIL ($PASSED assertions passed)"
	exit 1
fi
echo "W41 token-count reconcile: PASS ($PASSED assertions)"
