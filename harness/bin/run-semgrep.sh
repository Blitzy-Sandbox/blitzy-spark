#!/usr/bin/env bash
# harness/bin/run-semgrep.sh — Semgrep CE runner. Takes NO arguments; configuration baked in.
#
#   role        : the control arm for Opengrep. Semgrep CE has NO cross-function taint engine
#                 (that moved behind the paid Pro Engine), so this runner enables no taint flag:
#                 the difference between this artifact and opengrep.sarif is the point.
#   scan target : the allowlist scope of $SPARK_SRC (expanded scope directories, no src/test),
#                 plus an explicit --exclude for src/test.
#   rulesets    : the pinned local clone $SEMGREP_RULES_DIR (semgrep/semgrep-rules), used through
#                 RELATIVE --config paths from the ruleset root so rule ids are canonical and
#                 directly comparable with the Opengrep run. Sets: scala, java, python,
#                 generic/secrets. Registry (`p/...`) configs are deliberately NOT used: a
#                 registry can change between two runs, a pinned clone cannot.
#   telemetry   : --metrics=off --disable-version-check (also set in the environment file).
#   determinism : --timeout 0 disables the per-rule-per-file time limit.
#   artifact    : $HARNESS_RAW_DIR/semgrep.sarif   (SARIF 2.1.0)
#   exit code   : Semgrep's own.
set -uo pipefail
[ $# -eq 0 ] || { echo "run-semgrep.sh: takes no arguments" >&2; exit 64; }

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
. "$HARNESS_DIR/env.sh"
# shellcheck source=/dev/null
. "$HARNESS_DIR/lib/scope.sh"

harness_begin semgrep
ARTIFACT="$HARNESS_RAW_DIR/semgrep.sarif"

mapfile -t TARGETS < <(harness_scope_dirs)
[ "${#TARGETS[@]}" -gt 0 ] || harness_die "allowlist expanded to no directory under $HARNESS_SCAN_ROOT"
[ -d "$SEMGREP_RULES_DIR" ] || harness_die "semgrep ruleset missing: $SEMGREP_RULES_DIR"

echo "harness: rulesets: $SEMGREP_RULES_DIR {scala,java,python,generic/secrets} (commit $(git -C "$SEMGREP_RULES_DIR" rev-parse HEAD 2>/dev/null))"
echo "harness: taint: none (Semgrep CE has no interprocedural taint engine; control arm)"
echo "harness: invocation: semgrep scan (${#TARGETS[@]} scope directories)"

cd "$SEMGREP_RULES_DIR" || harness_die "cannot cd to $SEMGREP_RULES_DIR"
semgrep scan \
  --metrics=off \
  --disable-version-check \
  --config scala \
  --config java \
  --config python \
  --config generic/secrets \
  --exclude 'src/test' \
  --timeout 0 \
  --sarif \
  --output "$ARTIFACT" \
  "${TARGETS[@]}" < /dev/null
rc=$?

harness_finish semgrep "$rc" "$ARTIFACT"
exit $rc
