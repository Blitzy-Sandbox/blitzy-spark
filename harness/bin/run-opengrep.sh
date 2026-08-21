#!/usr/bin/env bash
# harness/bin/run-opengrep.sh — Opengrep runner. Takes NO arguments; configuration baked in.
#
#   scan target : the allowlist scope of $SPARK_SRC (the expanded scope directories, which
#                 never include src/test), plus an explicit --exclude for src/test.
#   TAINT       : ENABLED. --taint-intrafile turns on intra-file inter-procedural taint
#                 analysis, which Opengrep 1.27.1 documents as supported for Scala (and Java,
#                 Python). --dataflow-traces attaches the dataflow trace to SARIF results.
#                 --pro / --pro-path-sensitive are NOT used: they require the proprietary
#                 Semgrep Pro Engine. --guarded-taint-signatures is NOT used: it requires
#                 --experimental. Both facts are recorded in harness/ENVIRONMENT.md.
#   rulesets    : the pinned local clone $OPENGREP_RULES_DIR (opengrep/opengrep-rules), used
#                 through RELATIVE --config paths from the ruleset root so rule ids come out in
#                 canonical form (scala.lang.security.audit.*, java.*, python.*). Sets:
#                 scala, java, python, generic/secrets. A local clone rather than a registry
#                 fetch is deliberate: a registry can change between two runs, a pinned clone cannot.
#   determinism : --timeout 0 disables the 5-second per-rule-per-file limit, so a slow rule
#                 cannot change the finding count from one run to the next.
#   artifact    : $HARNESS_RAW_DIR/opengrep.sarif   (SARIF 2.1.0)
#   exit code   : Opengrep's own.
set -uo pipefail
[ $# -eq 0 ] || { echo "run-opengrep.sh: takes no arguments" >&2; exit 64; }

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
. "$HARNESS_DIR/env.sh"
# shellcheck source=/dev/null
. "$HARNESS_DIR/lib/scope.sh"

harness_begin opengrep
ARTIFACT="$HARNESS_RAW_DIR/opengrep.sarif"

mapfile -t TARGETS < <(harness_scope_dirs)
[ "${#TARGETS[@]}" -gt 0 ] || harness_die "allowlist expanded to no directory under $HARNESS_SCAN_ROOT"
[ -d "$OPENGREP_RULES_DIR" ] || harness_die "opengrep ruleset missing: $OPENGREP_RULES_DIR"

echo "harness: rulesets: $OPENGREP_RULES_DIR {scala,java,python,generic/secrets} (commit $(git -C "$OPENGREP_RULES_DIR" rev-parse HEAD 2>/dev/null))"
echo "harness: taint: --taint-intrafile --dataflow-traces (Pro/experimental engines not used)"
echo "harness: invocation: opengrep scan (${#TARGETS[@]} scope directories)"

cd "$OPENGREP_RULES_DIR" || harness_die "cannot cd to $OPENGREP_RULES_DIR"
opengrep scan \
  --taint-intrafile \
  --dataflow-traces \
  --config scala \
  --config java \
  --config python \
  --config generic/secrets \
  --exclude 'src/test' \
  --timeout 0 \
  --sarif-output="$ARTIFACT" \
  "${TARGETS[@]}" < /dev/null
rc=$?

harness_finish opengrep "$rc" "$ARTIFACT"
exit $rc
