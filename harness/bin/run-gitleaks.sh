#!/usr/bin/env bash
# harness/bin/run-gitleaks.sh — Gitleaks runner. Takes NO arguments; configuration baked in.
#
#   scan target : the allowlist scope of $SPARK_SRC — harness/scope/allowlist.txt expanded to
#                 the directories that exist under the pinned tree. src/test never appears in
#                 that expansion, so test sources are excluded by construction.
#   mode        : `gitleaks dir` (filesystem scan, not git history) with the tool's default
#                 rule set. --redact is baked in so no matched secret value is written into the
#                 report or the log; rule id, description, file and line are unaffected.
#   artifact    : $HARNESS_RAW_DIR/gitleaks.json   (Gitleaks native JSON: a top-level array)
#   exit code   : Gitleaks' own (0 = no leaks, 1 = leaks found, 2 = error).
set -uo pipefail
[ $# -eq 0 ] || { echo "run-gitleaks.sh: takes no arguments" >&2; exit 64; }

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
. "$HARNESS_DIR/env.sh"
# shellcheck source=/dev/null
. "$HARNESS_DIR/lib/scope.sh"

harness_begin gitleaks
ARTIFACT="$HARNESS_RAW_DIR/gitleaks.json"

mapfile -t TARGETS < <(harness_scope_dirs)
[ "${#TARGETS[@]}" -gt 0 ] || harness_die "allowlist expanded to no directory under $HARNESS_SCAN_ROOT"
printf 'harness: target: %s\n' "${TARGETS[@]}"
echo "harness: invocation: gitleaks dir --redact --report-format json (${#TARGETS[@]} scope directories)"

gitleaks dir \
  --no-banner \
  --redact \
  --report-format json \
  --report-path "$ARTIFACT" \
  --log-level info \
  "${TARGETS[@]}"
rc=$?

harness_finish gitleaks "$rc" "$ARTIFACT"
exit $rc
