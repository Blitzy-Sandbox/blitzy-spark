#!/usr/bin/env bash
# run-joern.sh — Joern 4.0.607 over the code-property graph built from the pinned
# tree's bytecode.
#
# Loads the graph with importCpg (never importCode, which spawns a second JVM at the
# same heap) and runs the BOUNDED query set baked into harness/lib/joern-scan.sc.
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

command -v joern >/dev/null 2>&1 || scope_fail "joern is not on PATH"
[ -f "$SCRIPT" ] || scope_fail "baked query set not found: $SCRIPT"
[ -x "$JAVA_HOME_21/bin/java" ] || scope_fail "JAVA_HOME_21 does not hold a usable JDK: $JAVA_HOME_21"

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
printf 'heap            : %s\n' "$HARNESS_JOERN_HEAP"
printf 'query set       : %s (bound %s per query)\n' "$SCRIPT" "$BOUND"
printf 'workspace       : %s (outside the repository; joern writes ./workspace)\n' "$WORKDIR"

rm -f "$ART"
cd "$WORKDIR" || scope_fail "cannot enter $WORKDIR"
set +e
JAVA_HOME="$JAVA_HOME_21" SL_LOGGING_LEVEL="${SL_LOGGING_LEVEL:-WARN}" \
  HARNESS_SCAN_CPG="$CPG_REAL" HARNESS_SCAN_OUT="$ART" HARNESS_SCAN_BOUND="$BOUND" \
  joern --script "$SCRIPT" \
    -J-Xmx"$HARNESS_JOERN_HEAP" \
    < /dev/null > "$OUT" 2> "$ERR"
code=$?
set -e

scope_finish "$TOOL" "$ART" "$code"
exit "$code"
