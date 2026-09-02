#!/usr/bin/env bash
# run-joern-gated.sh — a committed gated execution path for Stage 3, now redundant.
#
# WHAT THIS SCRIPT IS, AS OF THE RUNNER'S SELF-BINDING
# ----------------------------------------------------
# AAP 0.8.2 requires the graph's identity re-verified immediately before every load, and
# 0.6.4 requires each check logged. This file was written on the reading that neither
# could be satisfied from inside the Stage 3 path — that harness/bin/run-joern.sh printed
# its input's size and digest and then invoked Joern without comparing them, and that
# editing the runner was forbidden, so the check could not live in the thing it guards.
#
# THAT IS NO LONGER TRUE, AND THE PROSE THAT SAID IT WAS HAS BEEN CORRECTED HERE. QA
# found that a gate nothing on the canonical path reads does not bind that path, so the
# comparison now lives in the runner itself: harness/bin/run-joern.sh runs
# harness/lib/preflight_graph_identity.py --check-only against the HARNESS_CPG it is
# about to open, echoes the whole report to its own stdout, keeps a copy at
# $HARNESS_LOG_DIR/joern.preload-identity.log, and exits 78 without loading anything on a
# non-zero gate status. The runner likewise floor-checks HARNESS_JOERN_HEAP up front,
# appends it to JAVA_TOOL_OPTIONS so the child JVM that runs importCpg inherits it, has
# harness/lib/joern-scan.sc measure that JVM's own heap before importCpg, and re-checks
# the measurement afterwards. The canonical direct no-argument invocation is therefore
# self-binding on both counts.
#
# So this script is a VALID BUT REDUNDANT belt-and-braces path: it performs the same
# identity comparison one step earlier, and there is still exactly one route through it
# to the runner, downstream of a gate exit of 0 — a non-zero gate returns from here
# without the runner having been invoked at all. An invocation routed through it is
# gated twice, by two independent readings of the same gate, and an invocation that skips
# it is gated once, by the runner. Neither is unguarded, which is the change: the
# guarantee no longer depends on which of the two callers an operator chose.
#
# THE INVOCATION ON RECORD, AND WHY IT IS STILL WORTH NAMING
# ----------------------------------------------------------
# The delivered Stage 3 load predates the runner's self-binding: line 3 of
# harness/artifacts/logs/joern.runner-console.log records
# argv=["./harness/bin/run-joern.sh"] for the load that started 2026-09-01T14:25:10Z,
# ended 14:41:24Z and exited 0, and harness/artifacts/logs/runner-sequence.json records
# the same argv for its ninth invocation. This script was committed and available but was
# NOT the executed path for that load, so its gate was not exercised for it; the
# contemporaneous identity evidence is the runner's own recompute, printed at load time
# and appearing verbatim on joern.runner-console.log lines 14-15 as 541309809 /
# 4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7. That was a print
# rather than a comparison. The runner now compares it, so an operator calling the runner
# directly gets the comparison too — which is exactly what this file's earlier prose said
# they did not get.
#
# THE FOUR STEPS, AND WHY THE FIRST TWO ARE SEPARATE PROGRAMS
# -----------------------------------------------------------
#   1/4  harness/lib/preflight_scan_target.py — the tree to be scanned is the pinned tree,
#        HARNESS_SMOKE_TARGET is absent from the environment entirely, and every harness
#        path value a runner interpolates is ordinary text. harness/lib/scope.sh accepts
#        any SPARK_SRC without comparing HEAD and takes the smoke override whenever it is
#        non-empty, and that file may not be edited either, so the control lives there and
#        is read here.
#   2/4  harness/lib/preflight_graph_identity.py — the graph on disk is byte-for-byte the
#        graph every record of account describes.
#   3/4  the heap: syntax, floor, ceiling, and a contemporaneous pre-touch commit proof
#        for any raise. Inline rather than a separate program because it is the value THIS
#        script exports, and a check that lived elsewhere would not be the thing that
#        decided what was exported.
#   4/4  the runner, directly, with no arguments.
# Each step reaches the next only on a zero status. There is exactly one route to step 4.
#
# WHAT IT DOES NOT DO
# -------------------
# It does not modify, wrap or re-order anything inside the runner, and it passes the
# runner NO arguments — AAP 0.8.1 requires each runner invoked directly with none, and a
# runner handed one exits 64 without scanning. It sets no baked flag. The heap reaches the
# query's JVM through the runner's own documented override, which AAP 0.6.5 classifies as a
# runtime value rather than a configuration edit; raising is permitted and reported,
# lowering is not, so this script refuses to lower it — and refuses to raise it beyond what
# this provisioning has proven, or without proving it.
#
# WHICH VARIABLE CARRIES AN ACCEPTED HEAP, AND WHY IT IS NOT JAVA_TOOL_OPTIONS ALONE
# ----------------------------------------------------------------------------------
# harness/bin/run-joern.sh appends -Xmx$HARNESS_JOERN_HEAP to JAVA_TOOL_OPTIONS LAST, and
# the last -Xmx in that string is the one the JVM applies, so a JAVA_TOOL_OPTIONS exported
# from here and nothing else would be overridden by the runner and have no effect on the
# child. An accepted heap is therefore exported through HARNESS_JOERN_HEAP — the documented
# variable the runner floor-checks and forwards — and JAVA_TOOL_OPTIONS is exported to the
# same figure so the two never disagree in a log that shows both.
#
# EXIT STATUS
# -----------
#   0   both gates passed, the heap was accepted, and the runner exited 0.
#   77  a gate module refused (HALT_EXIT). The runner was NOT invoked. Nothing was loaded.
#   78  a configuration fault: a gate module could not read a record it is required to
#       read, or the requested heap is malformed, below the floor, above the provisioned
#       ceiling, or unproven; or the runner's own — its identity gate, its heap floor, or
#       any other scope_fail. The runner was NOT invoked for any heap condition raised
#       here. 78 rather than 77 for those because every one of them is corrected in the
#       caller's own environment, which is exactly what scope.sh's scope_fail status means.
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

printf '== step 1/4: pre-scan target, smoke and path-value gate ==\n'
printf 'gate            : harness/lib/preflight_scan_target.py\n'
printf 'subject         : SPARK_SRC=%s\n' "${SPARK_SRC:-<unset>}"
printf 'gate started    : %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"

python3 "$_repo/harness/lib/preflight_scan_target.py"
_target_rc=$?

printf 'gate finished   : %s (exit %s)\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$_target_rc"

if [ "$_target_rc" -ne 0 ]; then
  printf '\n' >&2
  printf 'HALT: the pre-scan target/smoke/path gate exited %s.\n' "$_target_rc" >&2
  printf 'The Stage 3 Joern runner was NOT invoked. Nothing was loaded, and no artifact\n' >&2
  printf 'was written or removed. See harness/artifacts/logs/sec-gate-scan-target.log.\n' >&2
  exit "$_target_rc"
fi

printf '\n== step 2/4: pre-load graph identity gate ==\n'
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

printf '\n== step 3/4: heap validation and contemporaneous commit proof ==\n'

# The heap the query's own JVM runs at, expressed through HARNESS_JOERN_HEAP — the
# variable the runner floor-checks and then appends to JAVA_TOOL_OPTIONS itself.
#
# harness/bin/run-joern.sh bakes -J-Xmx"$HARNESS_JOERN_HEAP" for the parent ReplBridge
# JVM, and `joern --script` forks a CHILD JVM without forwarding -J-Xmx to it, so the
# heap the query actually runs at comes from JAVA_TOOL_OPTIONS. The runner now supplies
# that itself, appending -Xmx"$HARNESS_JOERN_HEAP" LAST — and the last -Xmx in the string
# wins — so a JAVA_TOOL_OPTIONS exported from here would be overridden and a raise
# expressed only that way would silently have no effect. HARNESS_JOERN_HEAP is therefore
# what this script sets; JAVA_TOOL_OPTIONS is exported alongside it, to the same value,
# only so the two agree wherever both are shown. AAP 0.6.5 classifies supplying either as
# a runtime value rather than a configuration edit.
#
# 64 g is the floor, and the default whenever nothing upstream has already raised
# HARNESS_JOERN_HEAP, which is AAP 0.8.2's "minimum and default" taken literally; it is
# also the value the delivered Stage 3 invocation ran at:
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
# from a clean one. That direction is enforced below rather than trusted.
#
# FOUR CHECKS, IN THIS ORDER, AND THE ORDER IS THE CONTROL
# --------------------------------------------------------
#   1. syntax   whole gigabytes, no leading zero (in shell arithmetic a leading zero is
#               OCTAL, so `0100` would silently mean 64 and `064` would mean 52).
#   2. floor    below 64 refused, per the direction above.
#   3. ceiling  above the provisioned maximum refused BEFORE any reservation is attempted.
#   4. proof    any value above the 64 g default must be proven committable IN THIS
#               invocation, by the AAP 0.8.2 test, before it is exported.
#
# Checks 3 and 4 are what this script previously lacked: it validated "whole digits" and
# ">= 64" and then exported, so 129 and 1,000,000 reached JAVA_TOOL_OPTIONS with no proof
# and no bound. The ceiling comes BEFORE the proof deliberately: a 1,000,000 g request must
# be refused by arithmetic, not by asking a JVM on a host shared with dozens of clones to
# try to commit a petabyte and fail.
#
# THE CEILING IS A CONSTANT OF THIS SCRIPT, NOT AN ENVIRONMENT VALUE
# ------------------------------------------------------------------
# It is deliberately not overridable: a bound a caller can raise is not a bound. Its value
# is justified from this run's own evidence rather than chosen -- exactly two heaps carry a
# commit proof here, and no others do:
#   * 64 GiB and 128 GiB, both arms recorded in harness/artifacts/logs/cpg-frontend.log
#     STEP 2 ("java -Xms64g -Xmx64g -XX:+AlwaysPreTouch -version exit=0" and the same at
#     128g, lines 153-157), and 64 GiB again in the gate's own pre-touch proof;
#   * harness/artifacts/logs/cpg-ceiling-reverify.log additionally started the 21 JDK at
#     -Xmx64g and -Xmx128g, which corroborates that both heaps start on this host without
#     itself being a pre-touch proof.
# So 128 is the largest heap this provisioning has ever demonstrated it can commit, and a
# request above it is refused. Raising the ceiling is a provisioning act: produce a
# pre-touch proof at the new value, record it in the evidence tree, and change this
# constant in the same change.
# ---- heap-validation:begin ---------------------------------------------------------
# The block between these two markers is self-contained: it reads only
# HARNESS_JOERN_JVM_HEAP_G, HARNESS_JOERN_HEAP, JAVA_HOME_21, HARNESS_LOG_DIR and
# BLITZY_CLONE_INDEX, and it either exits non-zero or exports HARNESS_JOERN_HEAP and
# JAVA_TOOL_OPTIONS. The markers exist so the boundary cases can be exercised on the
# VERBATIM committed text -- extracted with awk at test time -- without invoking a scanner
# or a Joern process, which is how
# harness/artifacts/logs/sec-gate-joern-heap-boundaries.log was produced.
_heap_ceiling_g=128
_heap_default_g=64
# The default is whatever HARNESS_JOERN_HEAP already carries rather than a flat 64, so a
# caller who raised that variable and then routed the invocation through this wrapper is
# not silently lowered back to the floor by the wrapper's own default. An inherited value
# is a REQUEST like any other and is put through all four checks below, so a raise arriving
# this way is proven committable exactly as an explicit one is. A non-conforming
# HARNESS_JOERN_HEAP falls back to the default here and is left for the runner's own parse
# to refuse, which is where that diagnostic belongs.
_inherited_heap_g="${HARNESS_JOERN_HEAP:-${_heap_default_g}g}"
_inherited_heap_g="${_inherited_heap_g%[gG]}"
case "$_inherited_heap_g" in
  ''|*[!0-9]*) _inherited_heap_g="$_heap_default_g" ;;
esac
_heap_g="${HARNESS_JOERN_JVM_HEAP_G:-$_inherited_heap_g}"
_heap_log="$HARNESS_LOG_DIR/sec-gate-joern-heap.log"

# 1. SYNTAX. Digits only, and a length bound before any arithmetic: a value of twenty
#    digits would overflow shell arithmetic, and a check that overflows is not a check.
case "$_heap_g" in
  ''|*[!0-9]*)
    printf 'run-joern-gated.sh: HARNESS_JOERN_JVM_HEAP_G must be whole gigabytes, got %s\n' \
      "$_heap_g" >&2
    exit 78
    ;;
  0?*)
    # `0?*` rather than `0*` so a bare `0` falls through to the floor check and gets the
    # floor's diagnostic, which is the accurate one for it.
    printf 'run-joern-gated.sh: refusing HARNESS_JOERN_JVM_HEAP_G=%s -- a leading zero is\n' \
      "$_heap_g" >&2
    printf 'read as octal in shell arithmetic, so the value the JVM would receive is not the\n' >&2
    printf 'value written. State the heap in plain decimal gigabytes.\n' >&2
    exit 78
    ;;
esac
if [ "${#_heap_g}" -gt 6 ]; then
  printf 'run-joern-gated.sh: refusing HARNESS_JOERN_JVM_HEAP_G=%s -- it exceeds the %sg\n' \
    "$_heap_g" "$_heap_ceiling_g" >&2
  printf 'provisioned maximum by orders of magnitude. Refused on its digit count, before any\n' >&2
  printf 'arithmetic and before any reservation: no JVM was started.\n' >&2
  exit 78
fi

# 2. FLOOR.
if [ "$_heap_g" -lt "$_heap_default_g" ]; then
  printf 'run-joern-gated.sh: refusing a %sg heap. AAP 0.8.2 permits raising the heap and\n' \
    "$_heap_g" >&2
  printf 'not lowering it: the %sg floor is proven committable, and a smaller heap yields a\n' \
    "$_heap_default_g" >&2
  printf 'truncated result whose silence cannot be told from a clean one.\n' >&2
  exit 78
fi

# 3. CEILING -- before any reservation is attempted.
if [ "$_heap_g" -gt "$_heap_ceiling_g" ]; then
  printf 'run-joern-gated.sh: refusing a %sg heap. The largest heap this provisioning has\n' \
    "$_heap_g" >&2
  printf 'proven committable is %sg (harness/artifacts/logs/cpg-frontend.log STEP 2), and\n' \
    "$_heap_ceiling_g" >&2
  printf 'this host is shared with dozens of clones, so an unproven reservation is somebody\n' >&2
  printf "else's failure as well as this run's. Refused by arithmetic: no JVM was started,\n" >&2
  printf 'and nothing was reserved. Raising the ceiling is a provisioning act -- produce a\n' >&2
  printf 'pre-touch proof at the new value and change the constant in the same change.\n' >&2
  exit 78
fi

# 4. PROOF -- contemporaneous, in this invocation, for any value above the default.
#    The default is not an override and needs no proof at invocation time: it is the value
#    the delivered Stage 3 invocation ran at and the value the recorded proofs cover, which
#    is also what keeps the ordinary path free of a 64 GiB pre-touch.
{
  printf '\n==== %s  clone %s  requested heap %sg ====\n' \
    "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${BLITZY_CLONE_INDEX:-0}" "$_heap_g"
  printf 'source          : HARNESS_JOERN_JVM_HEAP_G=%s (default %sg, ceiling %sg)\n' \
    "${HARNESS_JOERN_JVM_HEAP_G:-<unset>}" "$_heap_default_g" "$_heap_ceiling_g"
} >> "$_heap_log" 2>/dev/null || true

if [ "$_heap_g" -gt "$_heap_default_g" ]; then
  if [ ! -x "$JAVA_HOME_21/bin/java" ]; then
    printf 'run-joern-gated.sh: refusing a raised %sg heap: JAVA_HOME_21 holds no usable\n' \
      "$_heap_g" >&2
    printf 'JDK at %s/bin/java, so the AAP 0.8.2 commit proof cannot be taken. A raise whose\n' \
      "$JAVA_HOME_21" >&2
    printf 'proof is unavailable is refused, never assumed.\n' >&2
    printf 'proof           : NOT TAKEN -- %s/bin/java is not executable; REFUSED\n' \
      "$JAVA_HOME_21" >> "$_heap_log" 2>/dev/null || true
    exit 78
  fi
  printf 'heap proof      : -Xms%sg -Xmx%sg -XX:+AlwaysPreTouch under %s\n' \
    "$_heap_g" "$_heap_g" "$JAVA_HOME_21"
  _proof_start="$(date +%s)"
  # JAVA_TOOL_OPTIONS and _JAVA_OPTIONS are unset for the proof: an ambient -Xmx would
  # otherwise decide what was actually committed, and the proof would measure the
  # environment rather than the requested value.
  _proof_out="$(env -u JAVA_TOOL_OPTIONS -u _JAVA_OPTIONS "$JAVA_HOME_21/bin/java" \
    -Xms"${_heap_g}"g -Xmx"${_heap_g}"g -XX:+AlwaysPreTouch -version 2>&1)"
  _proof_rc=$?
  _proof_elapsed=$(( $(date +%s) - _proof_start ))
  {
    printf 'proof command   : %s/bin/java -Xms%sg -Xmx%sg -XX:+AlwaysPreTouch -version\n' \
      "$JAVA_HOME_21" "$_heap_g" "$_heap_g"
    printf 'proof exit      : %s (elapsed %ss)\n' "$_proof_rc" "$_proof_elapsed"
    printf 'proof output    :\n%s\n' "$_proof_out"
  } >> "$_heap_log" 2>/dev/null || true
  printf 'proof exit      : %s (elapsed %ss, logged to %s)\n' \
    "$_proof_rc" "$_proof_elapsed" "$_heap_log"
  if [ "$_proof_rc" -ne 0 ]; then
    printf 'run-joern-gated.sh: refusing a %sg heap -- the commit proof FAILED (exit %s).\n' \
      "$_heap_g" "$_proof_rc" >&2
    printf 'AAP 0.8.2 requires any value above the floor to be itself proven committable\n' >&2
    printf 'before use, by exactly this test. A heap the host will not commit produces a\n' >&2
    printf 'truncated result or a dead JVM, so the raise is refused rather than attempted.\n' >&2
    printf 'The proof output is in %s.\n' "$_heap_log" >&2
    exit 78
  fi
else
  printf 'heap proof      : not required -- %sg is the default rather than a raise, and is\n' \
    "$_heap_g"
  printf '                  the value the recorded proofs cover (cpg-frontend.log STEP 2)\n'
  printf 'proof           : not required (default heap, not a raise)\n' \
    >> "$_heap_log" 2>/dev/null || true
fi
printf 'heap accepted   : %sg (syntax, floor %sg, ceiling %sg, proof)\n' \
  "$_heap_g" "$_heap_default_g" "$_heap_ceiling_g" >> "$_heap_log" 2>/dev/null || true

# HARNESS_JOERN_HEAP is the operative one: the runner floor-checks it and appends it to
# JAVA_TOOL_OPTIONS last, so it is the value the child JVM ends up at. JAVA_TOOL_OPTIONS
# is exported to the same figure so the two never disagree in a log that shows both.
export HARNESS_JOERN_HEAP="${_heap_g}g"
export JAVA_TOOL_OPTIONS="-Xmx${_heap_g}g"
# ---- heap-validation:end -----------------------------------------------------------

# Reached only on a gate exit of 0 and an accepted, proven heap. There is no other route
# to the line below.
#
# NOTE FOR A READER OF THE OLDER EVIDENCE: harness/artifacts/logs/
# joern-preflight-negative-test.log establishes "the runner never ran" partly by counting
# the '== step 2/2' banner that immediately preceded the invocation when that test was
# taken. The marker's ROLE is unchanged -- the banner below is still the last line before
# the runner is invoked and nothing else prints it -- but it now reads 'step 4/4', because
# the scan-target gate and the heap commit proof were added ahead of it.
printf '\n== step 4/4: the Stage 3 runner, directly, with no arguments ==\n'
printf 'heap in force   : HARNESS_JOERN_HEAP=%s (the runner floor-checks it, bakes\n' \
  "$HARNESS_JOERN_HEAP"
printf '                  -J-Xmx%s for the parent ReplBridge JVM, and appends -Xmx%s to\n' \
  "$HARNESS_JOERN_HEAP" "$HARNESS_JOERN_HEAP"
printf '                  JAVA_TOOL_OPTIONS for the child JVM that runs the query, which\n'
printf '                  joern --script does not receive -J-Xmx from; JAVA_TOOL_OPTIONS=%s\n' \
  "$JAVA_TOOL_OPTIONS"
printf '                  is exported here to the same figure so the two agree)\n'
printf 'runner started  : %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
_run_start="$(date +%s)"

./harness/bin/run-joern.sh
_run_rc=$?

_run_end="$(date +%s)"
printf 'runner finished : %s (exit %s, elapsed %ss)\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$_run_rc" "$((_run_end - _run_start))"
exit "$_run_rc"
