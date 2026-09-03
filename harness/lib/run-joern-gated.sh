#!/usr/bin/env bash
# run-joern-gated.sh — one of the two paths that supply Stage 3's identity comparison and
# its 64 GiB child heap from OUTSIDE the provisioned runner, which supplies neither.
#
# WHAT THE PROVISIONED RUNNER DOES, AND THE TWO THINGS IT DOES NOT DO
# -------------------------------------------------------------------
# AAP 0.8.2 requires the graph's identity re-verified immediately before every load, and
# 0.6.4 requires each check logged. Neither can be satisfied from inside the runner:
# harness/bin/run-joern.sh is PROVISIONED, and AAP 0.6.1 lists every entry in harness/bin/
# as REFERENCE — read and classified, never written — 0.8.1 says a runner's own file and
# its baked flags are never edited, and 0.3.2 makes a shortfall in a runner "a condition to
# record, not a defect to repair". So the check cannot live in the thing it guards, and it
# lives outside it instead. Both statements below are read off the runner as provisioned
# (76 lines, 3,380 bytes, sha256
# 32dd647af10709b72d159d67a2b15bd6f1f258af97614a9d2bf577c7a1abe65f):
#
#   * IT PRINTS THE GRAPH'S IDENTITY AND DOES NOT COMPARE IT. Line 57 `stat -c%s` and
#     line 58 `sha256sum` the resolved HARNESS_CPG and print the pair; the engine is
#     invoked at lines 67-71. Nothing between them opens a record of account, so the
#     runner cannot tell the graph its record describes from any other file at that path.
#     Printing is not comparing, and the printed pair is evidence only after the fact.
#
#   * ITS -J-Xmx REACHES THE LAUNCHER, NOT THE JVM THAT HOLDS THE GRAPH. Line 70 passes
#     -J-Xmx"$HARNESS_JOERN_HEAP" to the `joern` LAUNCHER, which starts a parent
#     ReplBridge JVM. `joern --script` then forks a CHILD JVM
#     (replpp.scripting.NonForkingScriptRunner) which is what runs importCpg
#     (harness/lib/joern-scan.sc line 41) and every query — and the child does NOT inherit
#     -J-Xmx. Left to the runner alone the child therefore runs at its DEFAULT ERGONOMIC
#     HEAP, a quarter of RAM — measured at 32,178,700,288 bytes (29.97 GiB) on the
#     delivered Stage 3 load, oss-scan-results/run-record.md §6.3 — while the runner's
#     console line 60 prints `heap : $HARNESS_JOERN_HEAP`, which reads `heap : 64g` at this
#     provisioning's default: a line that describes the launcher and says nothing about the
#     JVM the floor is about. The provisioned runner sets no JAVA_TOOL_OPTIONS and does not
#     clear an ambient one, which is precisely what leaves the child's heap decidable from
#     outside it — a JVM started under it prints `Picked up JAVA_TOOL_OPTIONS: -Xmx…` to
#     stderr, and both the parent and the forked child print it, which is how an -Xmx
#     supplied from outside is observed to have reached the child rather than assumed to.
#
# WHERE THE TWO GUARANTEES COME FROM INSTEAD: THE INVOCATION'S ENVIRONMENT
# ------------------------------------------------------------------------
# Both are supplied by the environment the invocation is made in — a runtime value, which
# AAP 0.6.5 distinguishes from a configuration edit and expressly permits. This run has
# exactly two such paths, and they differ only in who assembles that environment:
#
#   * THIS WRAPPER, which assembles it programmatically and in one order:
#     preflight_scan_target.py, then preflight_graph_identity.py, then the heap decision,
#     then the runner — each step reached only on a zero status (the four steps below).
#   * THE RUN OF RECORD'S DIRECT NO-ARGUMENT INVOCATION, which is the form AAP 0.8.1
#     mandates as canonical — `./harness/bin/run-joern.sh` with no arguments — performed
#     inside an environment the run prepares around it: the gate's own report published
#     immediately before the load, and JAVA_TOOL_OPTIONS=-Xmx64g exported into that
#     environment so the forked child JVM starts at the floor. Because the runner neither
#     sets nor clears JAVA_TOOL_OPTIONS, an -Xmx exported ahead of it is the value the
#     child applies.
#
# AN INVOCATION THAT REACHES THE RUNNER WITH NEITHER IS UNGUARDED
# ---------------------------------------------------------------
# There is no third mechanism, and the runner has no fallback: called in a bare
# environment it will load whatever file sits at HARNESS_CPG, compared against nothing,
# in a child JVM at roughly 30 GiB, and it will print a `heap : 64g` line while doing it.
# That is a real gap and it is stated here rather than papered over. Closing it
# STRUCTURALLY — so the canonical direct path cannot load an uncompared graph or fork a
# sub-floor child whatever environment it is called in — means changing the runner, and
# changing a runner is a PROVISIONING act, not a scanning-run act.
#
# THE PATCH PROVISIONING SHOULD ADOPT ALREADY EXISTS. It is the diff this checkout
# reverted to restore harness/bin/** to REFERENCE status:
#     git diff a64216aed7f d933940aa3b -- harness/bin/run-joern.sh harness/lib/joern-scan.sc
# +136/-1 on the runner — preflight_graph_identity.py --check-only run ahead of the load
# with its report echoed and copied, exit 78 on a non-zero gate status, a HARNESS_JOERN_HEAP
# floor check before anything is resolved, -Xmx appended LAST to JAVA_TOOL_OPTIONS for the
# child, and a post-run read-back that replaces the exit code with 78 and removes the
# artifact when the measured child heap is absent, unparsable or sub-floor — and +121 on
# the query set, the child JVM measuring its own Runtime.getRuntime.maxMemory() into a
# record before importCpg. Applied by provisioning, where runner configuration is
# legitimately owned, that change makes the canonical path self-binding and makes this
# wrapper redundant. Applied by a scanning run it is a prohibited edit to a REFERENCE
# file, which is what QA Issue 11 found and what this checkout no longer contains.
#
# THE INVOCATION ON RECORD, AND WHAT ITS EVIDENCE IS
# --------------------------------------------------
# Line 3 of harness/artifacts/logs/joern.runner-console.log records
# argv=["./harness/bin/run-joern.sh"] for the delivered load that started
# 2026-09-01T14:25:10Z, ended 14:41:24Z and exited 0, and
# harness/artifacts/logs/runner-sequence.json records the same argv for its ninth
# invocation. This script was committed and available but was NOT the executed path for
# that load, so its gate was not exercised for it; the contemporaneous identity evidence
# is the runner's own print, from lines 57-58, appearing verbatim on
# joern.runner-console.log lines 14-15 as 541309809 /
# 4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7 — which is the
# 2026-09-01 GENERATION'S GRAPH, SINCE SUPERSEDED, and not the graph on disk now (see the
# note on identity below). That print was a print rather than a comparison, and with the
# runner restored to its provisioned bytes it still is: the comparison for any later load
# comes from this wrapper's step 2, or from the gate report the run of record publishes
# immediately before its direct invocation.
#
# THE GRAPH IDENTITY THIS WRAPPER'S STEP 2 WILL FIND ON DISK
# ----------------------------------------------------------
# The live graph is 547,980,224 bytes / sha256
# 325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6 — the graph the
# 2026-09-03T01:17:07Z re-provisioning built, agreed by
# /opt/blitzy-harness/provision-log/cpg-identity.txt and cpg-record.txt and by
# harness/ENVIRONMENT.md §7. The 541309809 / 4616845a… pair above belongs to the
# 2026-09-01 generation and appears here only as history of that load; a reader who finds
# it anywhere in this file is reading about the superseded graph, never the current one.
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
# runner handed one exits 64 without scanning. It sets no baked flag and it does not touch
# the runner's file. The heap reaches the query's JVM through the environment the runner
# passes through — JAVA_TOOL_OPTIONS for the child, HARNESS_JOERN_HEAP for the launcher's
# -J-Xmx — which AAP 0.6.5 classifies as a runtime value rather than a configuration edit;
# raising is permitted and reported, lowering is not, so this script refuses to lower it —
# and refuses to raise it beyond what this provisioning has proven, or without proving it.
#
# WHICH VARIABLE CARRIES AN ACCEPTED HEAP, AND WHY BOTH ARE EXPORTED
# -------------------------------------------------------------------
# Two variables, two JVMs, and the provisioned runner reads only one of them:
#   * HARNESS_JOERN_HEAP is what the runner interpolates into -J-Xmx at its line 70, so it
#     decides the PARENT ReplBridge JVM's heap and appears on the runner's `heap :` console
#     line. The runner does not validate it and does not forward it to the child.
#   * JAVA_TOOL_OPTIONS is what the CHILD JVM picks up — the JVM that runs importCpg and
#     the queries, and therefore the JVM AAP 0.8.2's floor is about. The runner neither
#     sets nor clears it, so the value exported here is the one the child applies, and it
#     is the operative mechanism for the floor.
# Both are therefore exported below, to the same accepted figure: the child gets its heap
# from JAVA_TOOL_OPTIONS, the parent from HARNESS_JOERN_HEAP, and a log that shows both
# never shows them disagreeing. Neither export edits the runner; AAP 0.6.5 classifies
# supplying an environment value the runner is written to consume as a runtime value.
#
# EXIT STATUS
# -----------
#   0   both gates passed, the heap was accepted, and the runner exited 0.
#   77  a gate module refused (HALT_EXIT). The runner was NOT invoked. Nothing was loaded.
#   78  a configuration fault: a gate module could not read a record it is required to
#       read, or the requested heap is malformed, below the floor, above the provisioned
#       ceiling, or unproven; or one of the runner's own scope_fail conditions — no joern
#       on PATH, no baked query set, JAVA_HOME_21 not holding a usable JDK, or a
#       HARNESS_CPG that is missing or does not resolve to a file. The runner was NOT
#       invoked for any heap or gate condition raised here. 78 rather than 77 for those
#       because every one of them is corrected in the caller's own environment, which is
#       exactly what scope.sh's scope_fail status means.
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

# The heap the query's own JVM runs at. It is decided here because the provisioned runner
# does not decide it: harness/bin/run-joern.sh interpolates HARNESS_JOERN_HEAP into
# -J-Xmx at its line 70 without validating it, and that flag reaches only the parent
# ReplBridge JVM. `joern --script` forks a CHILD JVM and does not forward -J-Xmx to it, so
# the heap the queries actually run at comes from JAVA_TOOL_OPTIONS — which the runner
# neither sets nor clears. Whatever this script exports in JAVA_TOOL_OPTIONS is therefore
# what the child applies, unmodified, and nothing downstream can raise or lower it. Both
# variables are exported below, to the same accepted figure: JAVA_TOOL_OPTIONS is the
# operative one for the floor, HARNESS_JOERN_HEAP keeps the launcher and the runner's own
# `heap :` console line in agreement with it. AAP 0.6.5 classifies supplying either as a
# runtime value rather than a configuration edit.
#
# 64 g is the floor, and the default whenever nothing upstream has already raised
# HARNESS_JOERN_HEAP, which is AAP 0.8.2's "minimum and default" taken literally; it is
# also the figure the delivered Stage 3 invocation requested —
# oss-scan-results/run-record.md 6.3 and runner-metadata.json's
# tools.joern.heap_override record 64g, "not raised and never lowered". On that delivered
# load the figure held for the PARENT only: with no JAVA_TOOL_OPTIONS in its environment
# the forked child measured 32,178,700,288 bytes (29.97 GiB), which is exactly the gap
# this step closes. So the default reserves nothing this run has not already committed,
# and it now reserves it in the JVM that matters.
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
# HARNESS_JOERN_HEAP falls back to the default here and never reaches the runner at all,
# because the accepted figure is re-exported over it below: the provisioned runner performs
# no parse of its own — it interpolates the value straight into -J-Xmx — so a malformed
# value left in place would surface as the JVM's error rather than as a diagnostic.
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

# JAVA_TOOL_OPTIONS is the operative one: the provisioned runner neither sets nor clears
# it, so it is inherited unmodified by the child JVM that runs importCpg and the queries --
# the JVM the floor is about. HARNESS_JOERN_HEAP is exported to the same figure because the
# runner interpolates it into -J-Xmx for the parent ReplBridge JVM and prints it on its
# `heap :` console line, so exporting both keeps the two JVMs, and every log that shows
# them, in agreement.
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
printf 'heap in force   : JAVA_TOOL_OPTIONS=%s for the CHILD JVM that runs importCpg and\n' \
  "$JAVA_TOOL_OPTIONS"
printf '                  the queries -- the JVM the floor is about. The runner neither\n'
printf '                  sets nor clears JAVA_TOOL_OPTIONS, so the child inherits this\n'
printf '                  value unmodified; joern --script does not forward the launcher\n'
printf '                  -J-Xmx to it. HARNESS_JOERN_HEAP=%s is exported to the same\n' \
  "$HARNESS_JOERN_HEAP"
printf '                  figure, which the runner interpolates into -J-Xmx for the parent\n'
printf '                  ReplBridge JVM and prints on its own `heap :` line\n'
printf 'runner started  : %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
_run_start="$(date +%s)"

./harness/bin/run-joern.sh
_run_rc=$?

_run_end="$(date +%s)"
printf 'runner finished : %s (exit %s, elapsed %ss)\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$_run_rc" "$((_run_end - _run_start))"
exit "$_run_rc"
