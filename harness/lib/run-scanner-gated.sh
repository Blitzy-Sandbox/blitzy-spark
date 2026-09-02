#!/usr/bin/env bash
# run-scanner-gated.sh — the committed gated entry point for all nine scanner runners.
#
# WHY IT EXISTS
# -------------
# The checkpoint requires the wrong-target and smoke controls to be "structurally required
# before every direct runner". A gate is only structurally required if something reads its
# exit status before the runner is reached, and until now only Joern had such a caller
# (harness/lib/run-joern-gated.sh). The other eight runners each source harness/env.sh and
# harness/lib/scope.sh and resolve their target themselves — and scope.sh lines 22-35
# accept ANY SPARK_SRC without comparing HEAD, while lines 43-48 make HARNESS_SMOKE_TARGET
# the entire scope whenever it is non-empty.
#
# Neither scope.sh nor any runner may be edited: AAP 0.3.2 forbids runner reconfiguration,
# 0.8.1 states that "the line is the runner's own file and its baked flags: those are never
# edited", 0.6.3 states that "no runner or harness helper is edited", and 0.6.1 marks every
# harness/bin/ entry REFERENCE. So this script is the reader: one route per tool, each
# downstream of a gate exit of 0.
#
# WHAT IT IS NOT: THE ONLY ROUTE
# ------------------------------
# Every harness/bin/run-<tool>.sh stays executable in its own right, and AAP 0.8.1 requires
# each runner invocable directly with no arguments. An invocation that does not come
# through this script does not read this gate and is not bound by it. That residual belongs
# to the provisioning rather than to a clone, and it is stated rather than implied — here,
# in preflight_scan_target.py's report, and in the checkpoint record.
#
# WHAT IT DOES NOT DO
# -------------------
# It does not modify, copy, wrap or re-order anything inside a runner, and it passes each
# runner NO arguments: AAP 0.8.1 requires exactly that, and a runner handed one exits 64
# without scanning. It sets no baked flag and no scan-shaping variable of its own. It runs
# ONE runner per invocation, because AAP 0.8.1 requires each runner invoked directly and
# individually and forbids an orchestrator's continue-on-error sequencing — so this is a
# gate plus a dispatch, never a loop over the nine.
#
# EXIT STATUS
# -----------
#   0   the gate passed and the runner exited 0.
#   64  this script's own arguments were wrong: not exactly one, or not one of the nine
#       canonical tool names. Deliberately the same status the runners use for a bad
#       argument, and it is returned BEFORE the environment is sourced or a gate is run.
#   77  the gate refused (its HALT status). No runner was invoked and nothing was scanned.
#   78  a configuration fault: the gate could not complete a measurement, or the harness's
#       own environment is incomplete. No runner was invoked.
#   *   anything else is the RUNNER's own exit status, passed through unchanged. Several of
#       the nine exit non-zero precisely because they found something (AAP 0.5.4: artifact
#       status and exit status are independent), so a scanning outcome must never be
#       rewritten into a gate status.
#
# Usage:
#   BLITZY_CLONE_INDEX=<n> harness/lib/run-scanner-gated.sh <tool>
#   HARNESS_GATED_DISPATCH_DRY_RUN=1 harness/lib/run-scanner-gated.sh <tool>
#
# The dry run exists so the routing can be AUDITED without scanning: it runs the gate for
# real, prints the exact command it would invoke, and returns without invoking it. It
# cannot be mistaken for a scan — it prints a DRY RUN marker as its last line, and a real
# scan is evidenced by $HARNESS_RAW_DIR/<tool>.* and $HARNESS_LOG_DIR/<tool>.status, none
# of which a dry run creates or touches.

# ---------------------------------------------------------------- argument validation
# Before `set -u`, before the environment is sourced, and before any gate: the same
# ordering the runners use, so a bad invocation cannot have a side effect.
if [ "$#" -ne 1 ]; then
  printf 'run-scanner-gated.sh: takes exactly one argument, the canonical tool name.\n' >&2
  printf 'Got %s. One runner per invocation: AAP 0.8.1 requires each runner invoked\n' "$#" >&2
  printf 'directly and individually, and forbids orchestrated sequencing.\n' >&2
  printf 'Usage: harness/lib/run-scanner-gated.sh <opengrep|semgrep|datadog-static-analyzer|\n' >&2
  printf '        gitleaks|checkov|trivy|osv-scanner|dependency-check|joern>\n' >&2
  exit 64
fi

# The nine canonical tool identifiers of AAP 0.5.4's scanner-class table, in its order.
# An allowlist rather than a pattern: the tool name becomes part of a path below, and a
# name validated by pattern is a name somebody eventually escapes.
_tool="$1"
case "$_tool" in
  opengrep|semgrep|datadog-static-analyzer|gitleaks|checkov|trivy|osv-scanner|dependency-check|joern)
    : ;;
  *)
    printf 'run-scanner-gated.sh: %s is not one of the nine canonical tools.\n' "$_tool" >&2
    printf 'Accepted: opengrep semgrep datadog-static-analyzer gitleaks checkov trivy\n' >&2
    printf '          osv-scanner dependency-check joern\n' >&2
    printf 'Refusing before sourcing the environment or running any gate.\n' >&2
    exit 64
    ;;
esac

set -u

_self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_repo="$(cd "$_self/../.." && pwd)"
cd "$_repo" || {
  printf 'run-scanner-gated.sh: cannot enter the repository root %s\n' "$_repo" >&2
  exit 78
}

# Source the same environment file the runner itself sources, so the values the gate
# measures are byte-for-byte the values the runner will consume. Resolving them any other
# way would let the gate check one environment while the runner ran in another.
# shellcheck source=/dev/null  # resolved at run time from this script's own location
. "$_repo/harness/env.sh"

# Joern's route is its own wrapper, which runs this same gate as its step 1 and then adds
# the graph-identity gate and the heap commit proof the other eight have no use for. The
# gate therefore runs twice on that route. That is deliberate: each entry point is
# independently fail-closed, and the cost is three `git rev-parse` calls.
if [ "$_tool" = "joern" ]; then
  _target="$_repo/harness/lib/run-joern-gated.sh"
  _why="the gated Joern path: this same gate, then the graph-identity gate, then the heap commit proof, then the runner"
else
  _target="$_repo/harness/bin/run-$_tool.sh"
  _why="the runner directly, with no arguments"
fi

printf '== run-scanner-gated.sh ==\n'
printf 'tool            : %s\n' "$_tool"
printf 'route           : %s\n' "$_target"
printf 'route rationale : %s\n' "$_why"
printf 'scan target     : SPARK_SRC=%s\n' "${SPARK_SRC:-<unset>}"
printf 'clone index     : %s\n' "${BLITZY_CLONE_INDEX:-0}"

if [ ! -x "$_target" ]; then
  printf 'run-scanner-gated.sh: %s is not an executable file. Refusing: a dispatch that\n' \
    "$_target" >&2
  printf 'cannot name its runner has not established anything about a scan.\n' >&2
  exit 78
fi

printf '\n== step 1/2: pre-scan target, smoke and path-value gate ==\n'
printf 'gate            : harness/lib/preflight_scan_target.py\n'
printf 'gate started    : %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"

python3 "$_repo/harness/lib/preflight_scan_target.py"
_gate_rc=$?

printf 'gate finished   : %s (exit %s)\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$_gate_rc"

if [ "$_gate_rc" -ne 0 ]; then
  printf '\n' >&2
  printf 'HALT: the pre-scan target/smoke/path gate exited %s.\n' "$_gate_rc" >&2
  printf 'The %s runner was NOT invoked. Nothing was scanned, and no artifact or status\n' \
    "$_tool" >&2
  printf 'file was written, removed or rewritten. See\n' >&2
  printf 'harness/artifacts/logs/sec-gate-scan-target.log for every check and its verdict.\n' >&2
  exit "$_gate_rc"
fi

# Reached only on a gate exit of 0. There is one route from here to the runner.
if [ "${HARNESS_GATED_DISPATCH_DRY_RUN:-}" = "1" ]; then
  printf '\n== step 2/2: dispatch NOT taken -- HARNESS_GATED_DISPATCH_DRY_RUN=1 ==\n'
  printf 'would invoke   : %s   (with no arguments)\n' "$_target"
  printf 'not created    : %s/%s.json or .sarif, %s/%s.status\n' \
    "$HARNESS_RAW_DIR" "$_tool" "$HARNESS_LOG_DIR" "$_tool"
  printf 'DRY RUN: the gate ran and passed; no scanner was invoked and nothing was scanned.\n'
  exit 0
fi

printf '\n== step 2/2: %s ==\n' "$_why"
printf 'runner started  : %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
_run_start="$(date +%s)"

"$_target"
_run_rc=$?

printf 'runner finished : %s (exit %s, elapsed %ss)\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$_run_rc" "$(( $(date +%s) - _run_start ))"
printf 'note            : that status is the runner'"'"'s own, passed through unchanged. A\n'
printf '                  non-zero value here is a scanning outcome, not a gate refusal --\n'
printf '                  several of the nine exit non-zero because they found something.\n'
exit "$_run_rc"
