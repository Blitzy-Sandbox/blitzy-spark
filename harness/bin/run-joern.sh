#!/usr/bin/env bash
# run-joern.sh — Joern 4.0.607 over the code-property graph built from the pinned
# tree's bytecode.
#
# Loads the graph with importCpg (never importCode, which spawns a second JVM at the
# same heap) and runs the BOUNDED query set baked into harness/lib/joern-scan.sc.
#
# THE GRAPH'S IDENTITY IS COMPARED HERE, NOT MERELY PRINTED
# AAP 0.8.2 requires the graph's identity re-verified immediately before every load and
# 0.6.4 requires each check logged. Printing a size and a digest is not comparing them,
# so this runner runs the committed gate — harness/lib/preflight_graph_identity.py, in
# its --check-only form, which measures everything and writes nothing — against the
# HARNESS_CPG it is about to open, echoes the whole report to its own stdout, keeps a
# copy at $HARNESS_LOG_DIR/joern.preload-identity.log, and refuses the load (exit 78)
# on any non-zero gate status. The comparison is therefore structurally upstream of
# importCpg on the canonical no-argument path, rather than depending on a caller
# remembering an out-of-band preflight.
#
# THE HEAP FLOOR IS ENFORCED ON THE JVM THAT ACTUALLY RUNS THE QUERIES
# `joern --script` runs the script in a CHILD JVM and does not forward the launcher's
# -J-Xmx to it, so -J-Xmx alone leaves the queries at the child's default ergonomic
# heap (29.96875 GiB on this host) while the console says 64g. HARNESS_JOERN_HEAP is
# therefore floor-checked here (>= 64 GiB; AAP 0.8.2's rule is one-way — raising is
# permitted and reported, lowering is not), appended LAST to JAVA_TOOL_OPTIONS so the
# floor-checked value wins whatever a caller pre-set, measured inside the child by
# harness/lib/joern-scan.sc, and re-checked from that measurement after the run.
#
# Runs under JAVA_HOME_21 (Joern 4.x documents JDK 21 as its tested requirement) at
# HARNESS_JOERN_HEAP, with stdin closed (the REPL blocks on an open stdin) and
# SL_LOGGING_LEVEL=WARN (the default level floods the artifact: one earlier run produced
# a 379 MB artifact of per-method INFO lines). The raw console stream goes to the log
# directory; only the counted query output becomes the artifact.
#
# Joern has no --workspace flag and writes an ~800 MB ./workspace into whatever
# directory it runs from, so the runner works inside the per-clone scratch directory —
# never inside the repository.
# Configuration is baked in; this runner accepts NO arguments.
# Artifact: $HARNESS_RAW_DIR/joern.json (native JSON with a findings array)

if [ "$#" -ne 0 ]; then
  printf 'run-joern.sh: takes no arguments (configuration is baked in); refusing to scan\n' >&2
  exit 64
fi

set -u
_self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$_self/../env.sh"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$HARNESS_LIB_DIR/scope.sh"

TOOL=joern
ART="$HARNESS_RAW_DIR/$TOOL.json"
OUT="$HARNESS_LOG_DIR/$TOOL.stdout.log"
ERR="$HARNESS_LOG_DIR/$TOOL.stderr.log"
SCRIPT="$HARNESS_LIB_DIR/joern-scan.sc"
BOUND="${HARNESS_JOERN_QUERY_BOUND:-2000}"
GATE="$HARNESS_LIB_DIR/preflight_graph_identity.py"
GATE_LOG="$HARNESS_LOG_DIR/$TOOL.preload-identity.log"
HEAP_RECORD="$HARNESS_LOG_DIR/$TOOL.child-jvm.json"

command -v joern >/dev/null 2>&1 || scope_fail "joern is not on PATH"
[ -f "$SCRIPT" ] || scope_fail "baked query set not found: $SCRIPT"
[ -x "$JAVA_HOME_21/bin/java" ] || scope_fail "JAVA_HOME_21 does not hold a usable JDK: $JAVA_HOME_21"
# The identity gate is a prerequisite of the load, not an optional extra, so its two
# requirements are guarded exactly like joern and the JDK: a gate that cannot run is a
# configuration fault, never a load that quietly proceeds unchecked.
[ -f "$GATE" ] || scope_fail "pre-load graph identity gate not found: $GATE (the graph's identity must be compared before it is loaded; AAP 0.8.2)"
command -v python3 >/dev/null 2>&1 || scope_fail "python3 is not on PATH, so the pre-load graph identity gate $GATE cannot run"

# ------------------------------------------------------------------- heap floor
# 64 GiB, in bytes. AAP 0.5.4/0.9.1 require the Stage 3 Joern runner at a heap of at
# least this, and 0.8.2 gives the rule a DIRECTION: raising it is permitted and
# reported, lowering it is not, because a heap made to fit produces a truncated result
# whose silence cannot be told apart from a clean one. So the floor is enforced here,
# before anything is resolved or loaded, rather than discovered afterwards.
HEAP_FLOOR_BYTES=68719476736
case "$HARNESS_JOERN_HEAP" in
  *[0-9][gG]) HEAP_G="${HARNESS_JOERN_HEAP%[gG]}" ;;
  *) scope_fail "HARNESS_JOERN_HEAP must be whole gigabytes with a trailing g (for example 64g), got '$HARNESS_JOERN_HEAP'; an unreadable heap value cannot be floor-checked, so nothing is loaded" ;;
esac
case "$HEAP_G" in
  ''|*[!0-9]*) scope_fail "HARNESS_JOERN_HEAP must be whole gigabytes with a trailing g (for example 64g), got '$HARNESS_JOERN_HEAP'" ;;
esac
HEAP_G=$((10#$HEAP_G))
HEAP_BYTES=$((HEAP_G * 1024 * 1024 * 1024))
if [ "$HEAP_BYTES" -lt "$HEAP_FLOOR_BYTES" ]; then
  scope_fail "refusing a ${HEAP_G}g heap ($HEAP_BYTES bytes): the Stage 3 Joern JVM floor is $HEAP_FLOOR_BYTES bytes (64 GiB). AAP 0.8.2 permits RAISING the heap and reports it, and does not permit lowering it, because a truncated graph's silence cannot be told apart from a clean result. Raise HARNESS_JOERN_HEAP instead, having first proven the larger value committable with 'java -Xms<n>g -Xmx<n>g -XX:+AlwaysPreTouch -version'"
fi
# The child JVM's heap. `joern --script` forks a child and does NOT forward -J-Xmx to
# it, so the floor-checked value is appended LAST to whatever JAVA_TOOL_OPTIONS already
# holds: the last -Xmx in that string is the one the JVM applies, so a caller can raise
# the heap through HARNESS_JOERN_HEAP (which is floor-checked above) but cannot lower it
# by pre-setting JAVA_TOOL_OPTIONS.
CHILD_JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:+$JAVA_TOOL_OPTIONS }-Xmx$HARNESS_JOERN_HEAP"

# The graph is this runner's input. Exit 78 naming the missing graph is a
# configuration fault to fix, not a scanning outcome to classify.
if [ ! -e "$HARNESS_CPG" ]; then
  scope_fail "code-property graph not found at HARNESS_CPG=$HARNESS_CPG (build it with jimple2cpg before scanning)"
fi
CPG_REAL="$(readlink -f "$HARNESS_CPG")"
[ -f "$CPG_REAL" ] || scope_fail "HARNESS_CPG=$HARNESS_CPG does not resolve to a file (resolved: $CPG_REAL)"

scope_resolve_target
scope_begin "$TOOL"

WORKDIR="$HARNESS_SCRATCH_DIR/joern-run"
mkdir -p "$WORKDIR" || scope_fail "cannot create the Joern working directory $WORKDIR"

printf 'cpg             : %s -> %s\n' "$HARNESS_CPG" "$CPG_REAL"
printf 'cpg bytes       : %s\n' "$(stat -c%s "$CPG_REAL")"
printf 'cpg sha256      : %s\n' "$(sha256sum "$CPG_REAL" | cut -d' ' -f1)"
printf 'jdk             : %s\n' "$JAVA_HOME_21"
# Two JVMs, two mechanisms, printed separately: a single "heap : 64g" line describes the
# launcher and says nothing about the JVM that holds the graph, which is the JVM the
# floor is about.
printf 'heap parent     : -J-Xmx%s (the ReplBridge JVM the joern launcher starts)\n' "$HARNESS_JOERN_HEAP"
printf 'heap child      : JAVA_TOOL_OPTIONS=%s (the JVM that runs importCpg and the queries;\n' "$CHILD_JAVA_TOOL_OPTIONS"
printf '                  joern --script does not forward -J-Xmx to it, so this is the\n'
printf '                  mechanism that sets it; the last -Xmx in the string is applied)\n'
printf 'heap floor      : %s bytes (64 GiB), measured inside the child by the query set and\n' "$HEAP_FLOOR_BYTES"
printf '                  re-checked from that measurement after the run\n'
printf 'query set       : %s (bound %s per query)\n' "$SCRIPT" "$BOUND"
printf 'workspace       : %s (outside the repository; joern writes ./workspace)\n' "$WORKDIR"

# ------------------------------------------------- pre-load graph identity check
# The three lines above PRINTED this graph's current size and digest. Printing is not
# comparing, so the committed gate now COMPARES them against the graph's record of
# account, and the load is downstream of its exit status. --check-only is mandatory: the
# gate's default form rewrites harness/artifacts/logs/joern-preflight.log, a committed
# canonical deliverable, which would make this runner non-hermetic the moment a caller
# redirects HARNESS_LOG_DIR. --check-only performs every measurement and writes nothing.
# The whole report goes to this runner's own stdout as well as to the log, so the
# comparison lives in the console stream this pipeline preserves verbatim (AAP 0.6.4).
printf 'identity gate   : %s --check-only\n' "$GATE"
printf 'identity log    : %s\n' "$GATE_LOG"
gate_code=0
HARNESS_REPO_ROOT="$HARNESS_REPO_ROOT" python3 "$GATE" --check-only > "$GATE_LOG" 2>&1 || gate_code=$?
cat "$GATE_LOG"
printf 'identity gate   : exit %s\n' "$gate_code"
if [ "$gate_code" -ne 0 ]; then
  scope_fail "the pre-load graph identity gate exited $gate_code, so the graph at HARNESS_CPG=$HARNESS_CPG is not the graph its record of account describes. Nothing was loaded and no artifact was written or removed. The gate's full report is above and at $GATE_LOG. This is a configuration fault to correct -- a load against different bytes than the record describes produces conclusions about a graph nobody has -- not a scanning outcome to classify"
fi

rm -f "$ART"
# A stale record from an earlier run must never be read as this run's measurement.
rm -f "$HEAP_RECORD"
cd "$WORKDIR" || scope_fail "cannot enter $WORKDIR"
set +e
JAVA_HOME="$JAVA_HOME_21" SL_LOGGING_LEVEL="${SL_LOGGING_LEVEL:-WARN}" \
  JAVA_TOOL_OPTIONS="$CHILD_JAVA_TOOL_OPTIONS" \
  HARNESS_SCAN_CPG="$CPG_REAL" HARNESS_SCAN_OUT="$ART" HARNESS_SCAN_BOUND="$BOUND" \
  HARNESS_SCAN_HEAP_FLOOR_BYTES="$HEAP_FLOOR_BYTES" \
  HARNESS_SCAN_HEAP_RECORD="$HEAP_RECORD" \
  joern --script "$SCRIPT" \
    -J-Xmx"$HARNESS_JOERN_HEAP" \
    < /dev/null > "$OUT" 2> "$ERR"
code=$?
# joern's own status, kept separately: the heap check below may replace $code with a
# configuration fault, and the diagnostic must still be able to state what the tool did.
joern_code=$code
# Read back what the child JVM measured of itself. Parsed inside the set +e region so a
# missing or unreadable record leaves the values empty rather than terminating the
# runner before its status trailer is written.
child_heap="$(sed -n 's/.*"heap_max_bytes"[[:space:]]*:[[:space:]]*\([0-9]\{1,\}\).*/\1/p' "$HEAP_RECORD" 2>/dev/null | head -1)"
child_verdict="$(sed -n 's/.*"at_or_above_floor"[[:space:]]*:[[:space:]]*\(true\|false\).*/\1/p' "$HEAP_RECORD" 2>/dev/null | head -1)"
set -e

# ------------------------------------------- the heap floor, from the child's own words
# The floor is only enforced if the measurement is READ BACK: a run whose child never
# wrote a record, or wrote one nobody parsed, would be indistinguishable from one that
# ran at 64 GiB. So absent, unparsable and sub-floor all map to the same configuration
# fault, and the artifact is removed rather than retained — an artifact produced at an
# unestablished heap must not be able to reach the normalizer, because a truncated
# result's silence cannot be told apart from a clean one.
printf 'heap measured   : %s bytes in the child JVM (floor %s), record %s\n' \
  "${child_heap:-<unestablished>}" "$HEAP_FLOOR_BYTES" "$HEAP_RECORD"
heap_fault=""
if [ ! -f "$HEAP_RECORD" ]; then
  heap_fault="the child JVM wrote no heap record at $HEAP_RECORD, so the heap the queries actually ran at is unestablished"
elif [ -z "$child_heap" ] || [ -z "$child_verdict" ]; then
  heap_fault="the child JVM heap record $HEAP_RECORD is unparsable (heap_max_bytes='$child_heap', at_or_above_floor='$child_verdict'), so the heap the queries actually ran at is unestablished"
elif [ "$child_heap" -lt "$HEAP_FLOOR_BYTES" ] || [ "$child_verdict" != "true" ]; then
  heap_fault="the JVM that ran importCpg and the queries had $child_heap bytes, below the floor of $HEAP_FLOOR_BYTES bytes (64 GiB)"
fi
if [ -n "$heap_fault" ]; then
  # The outcome is decided BEFORE anything is printed, and printing is done with errexit
  # off: a diagnostic that failed to emit must not be able to skip the removal, the
  # status trailer or the 78. (A literal starting with '-' is passed as an ARGUMENT to a
  # '%s' format rather than as the format itself, because bash's printf reads a format
  # beginning with '-' as an option and fails - which under errexit would abort the
  # runner here, exactly the fail-open this branch exists to prevent.)
  code=78
  rm -f "$ART"
  set +e
  printf 'CONFIGURATION FAULT: %s\n' "$heap_fault" >&2
  printf 'joern itself exited %s; the artifact %s has been REMOVED so a result produced at\n' \
    "$joern_code" "$ART" >&2
  printf '%s\n' \
    'an unestablished or sub-floor heap can never be retained or normalized. The heap' \
    'reaches that JVM through JAVA_TOOL_OPTIONS=-Xmx (joern --script does not forward' \
    '-J-Xmx to the child): raise HARNESS_JOERN_HEAP, which is floor-checked, rather than' \
    'lowering it. AAP 0.8.2 permits raising a heap and reports it, and does not permit' \
    'lowering one.' >&2
  printf 'See %s, %s and %s.\n' "$HEAP_RECORD" "$OUT" "$ERR" >&2
  set -e
fi

scope_finish "$TOOL" "$ART" "$code"
exit "$code"
