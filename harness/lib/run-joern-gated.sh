#!/usr/bin/env bash
# run-joern-gated.sh — a committed gated execution path for Stage 3.
#
# WHY A WRAPPER RATHER THAN A CHANGE TO THE RUNNER
# ------------------------------------------------
# AAP 0.8.2 requires the graph's identity re-verified immediately before every load, and
# 0.6.4 requires each check logged. Neither can be satisfied from inside the Stage 3 path:
# harness/bin/run-joern.sh prints its input's size and digest and then invokes Joern
# without ever comparing them, and harness/lib/joern-scan.sc calls importCpg before any
# figure exists. AAP 0.8.1 forbids editing a runner or a baked flag, so the check cannot
# live in the thing it guards.
#
# It lives here, and WHEN THE RUNNER IS INVOKED THROUGH THIS SCRIPT the binding is
# structural rather than advisory: there is exactly one path through this script to the
# runner, and it is downstream of a gate exit of 0. A non-zero gate returns from this
# script without the runner having been invoked at all. That is the whole purpose of the
# file — an exit status only binds something that reads it, so the reader is committed
# alongside the gate.
#
# WHAT THIS SCRIPT IS NOT: THE ONLY ROUTE TO THE RUNNER
# -----------------------------------------------------
# The binding above is conditional on this script being the caller, and nothing makes it
# the only caller. harness/bin/run-joern.sh is executable in its own right, and AAP 0.8.1
# requires each runner invoked directly with no arguments, so Stage 3 can be run without
# this wrapper — in which case this gate is not read and does not bind.
#
# The Stage 3 invocation ON RECORD was exactly that: line 3 of
# harness/artifacts/logs/joern.runner-console.log records
# argv=["./harness/bin/run-joern.sh"] for the load that started 2026-09-01T14:25:10Z,
# ended 14:41:24Z and exited 0, and harness/artifacts/logs/runner-sequence.json records
# the same argv for its ninth invocation. So this script is committed and available, but
# it was NOT the executed path for that load and its gate was therefore not exercised
# for it.
#
# The contemporaneous identity evidence for that load is the runner's own recompute:
# harness/bin/run-joern.sh lines 57-58 stat and sha256 the resolved graph and print both
# at load time, and they appear verbatim on joern.runner-console.log lines 14-15 as
# 541309809 / 4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7. That is a
# print, not a comparison — which is the gap this script closes for any invocation routed
# through it, and which an operator calling the runner directly does not get.
#
# WHAT IT DOES NOT DO
# -------------------
# It does not modify, wrap or re-order anything inside the runner, and it passes the
# runner NO arguments — AAP 0.8.1 requires each runner invoked directly with none, and a
# runner handed one exits 64 without scanning. It sets no baked flag. The heap reaches the
# query's JVM only through the runner's own documented JAVA_TOOL_OPTIONS override, which
# AAP 0.6.5 classifies as a runtime value rather than a configuration edit; raising is
# permitted and reported, lowering is not, so this script refuses to lower it.
#
# EXIT STATUS
# -----------
#   0   the gate passed and the runner exited 0.
#   77  the gate refused (HALT_EXIT). The runner was NOT invoked. Nothing was loaded.
#   *   any other value is the runner's own exit status, passed through unchanged so a
#       scanning outcome is never confused with a configuration fault.
#
# Usage:  BLITZY_CLONE_INDEX=<n> harness/lib/run-joern-gated.sh
# Takes no arguments of its own, for the same reason the runners take none.

if [ "$#" -ne 0 ]; then
  printf 'run-joern-gated.sh: takes no arguments (configuration is baked in); refusing to scan\n' >&2
  exit 64
fi

set -u

_self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_repo="$(cd "$_self/../.." && pwd)"
cd "$_repo" || {
  printf 'run-joern-gated.sh: cannot enter the repository root %s\n' "$_repo" >&2
  exit 78
}

# Source the environment the runner itself sources, so the HARNESS_CPG the gate measures
# is byte-for-byte the value the runner will open. Resolving it any other way would let
# the gate check one graph while the runner loaded another.
# shellcheck source=/dev/null  # resolved at run time from this script's own location
. "$_repo/harness/env.sh"

printf '== step 1/2: pre-load graph identity gate ==\n'
printf 'gate            : harness/lib/preflight_graph_identity.py\n'
printf 'subject         : HARNESS_CPG=%s\n' "$HARNESS_CPG"
_gate_start="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf 'gate started    : %s\n' "$_gate_start"

python3 "$_repo/harness/lib/preflight_graph_identity.py"
_gate_rc=$?

_gate_end="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
printf 'gate finished   : %s (exit %s)\n' "$_gate_end" "$_gate_rc"

if [ "$_gate_rc" -ne 0 ]; then
  printf '\n' >&2
  printf 'HALT: the pre-load graph identity gate exited %s.\n' "$_gate_rc" >&2
  printf 'The Stage 3 Joern runner was NOT invoked. Nothing was loaded, and no artifact\n' >&2
  printf 'was written or removed. See harness/artifacts/logs/joern-preflight.log.\n' >&2
  exit "$_gate_rc"
fi

# The heap the query's own JVM runs at, supplied through the runner's documented override
# and printed.
#
# harness/bin/run-joern.sh bakes -J-Xmx"$HARNESS_JOERN_HEAP" (64g by default), but
# `joern --script` forks a CHILD JVM and does not forward -J-Xmx to it, so the heap the
# query actually runs at is whatever JAVA_TOOL_OPTIONS supplies. That mechanism is why
# this line exists at all; AAP 0.6.5 classifies supplying the value here as a runtime
# value rather than a configuration edit.
#
# 64 g is BOTH the floor and the default here, which is AAP 0.8.2's "minimum and default"
# taken literally, and it is the value the delivered Stage 3 invocation ran at:
# oss-scan-results/run-record.md 6.3 and runner-metadata.json's
# tools.joern.heap_override.value_in_force both record 64g, "not raised and never
# lowered". So the default reserves nothing this run has not already committed.
#
# HARNESS_JOERN_JVM_HEAP_G raises it, and AAP 0.8.2 requires any value above the floor to
# be itself proven committable BEFORE use, by the same test the gate applies --
#     java -Xms<n>g -Xmx<n>g -XX:+AlwaysPreTouch -version    exiting 0
# -- with the raise reported. Two values carry that commit proof in this run's evidence and
# no others do: 64 GiB and 128 GiB, both arms recorded in
# harness/artifacts/logs/cpg-frontend.log STEP 2, and 64 GiB again in the gate's own
# -Xms64g -Xmx64g +AlwaysPreTouch proof. harness/artifacts/logs/cpg-ceiling-reverify.log
# additionally ran the 21 JDK at -Xmx64g and -Xmx128g, which corroborates that both heaps
# start on this host without being a pre-touch proof itself. Raising above 128 g therefore
# means producing the proof first rather than assuming the host will commit it -- and this
# host is shared, so an unproven reservation is somebody else's failure as well as this
# run's. For scale, the committed cost figures are the frontend's 113.3 GiB peak RSS
# against a -J-Xmx128g heap (cpg-frontend.log STEP 5) and the importCpg verification
# load's own run at -J-Xmx64g (cpg-verify.log).
#
# AAP 0.8.2's rule has a DIRECTION: raising is permitted and reported, lowering is not,
# because a heap made to fit produces a truncated result whose silence is indistinguishable
# from a clean one. That direction is enforced below rather than trusted: a value under the
# 64 g floor is refused outright.
_heap_g="${HARNESS_JOERN_JVM_HEAP_G:-64}"
case "$_heap_g" in
  ''|*[!0-9]*)
    printf 'run-joern-gated.sh: HARNESS_JOERN_JVM_HEAP_G must be whole gigabytes, got %s\n' \
      "$_heap_g" >&2
    exit 78
    ;;
esac
if [ "$_heap_g" -lt 64 ]; then
  printf 'run-joern-gated.sh: refusing a %sg heap. AAP 0.8.2 permits raising the heap and\n' \
    "$_heap_g" >&2
  printf 'not lowering it: the 64g floor is proven committable, and a smaller heap yields a\n' >&2
  printf 'truncated result whose silence cannot be told from a clean one.\n' >&2
  exit 78
fi
export JAVA_TOOL_OPTIONS="-Xmx${_heap_g}g"

# Reached only on a gate exit of 0. There is no other route to the line below.
printf '\n== step 2/2: the Stage 3 runner, directly, with no arguments ==\n'
printf 'heap in force   : %s (the runner bakes -J-Xmx%s, which joern --script does not\n' \
  "$JAVA_TOOL_OPTIONS" "$HARNESS_JOERN_HEAP"
printf '                  forward to the child JVM that runs the query)\n'
printf 'runner started  : %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
_run_start="$(date +%s)"

./harness/bin/run-joern.sh
_run_rc=$?

_run_end="$(date +%s)"
printf 'runner finished : %s (exit %s, elapsed %ss)\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$_run_rc" "$((_run_end - _run_start))"
exit "$_run_rc"
