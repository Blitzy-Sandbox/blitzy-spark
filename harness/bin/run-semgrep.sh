#!/usr/bin/env bash
# run-semgrep.sh — Semgrep CE 1.173.0 SAST over the twelve authoritative scope roots.
# Community Edition, no token attached: Pro and interfile analysis are unavailable
# by design (the unlicensed capability is the measurement).
# Configuration is baked in; this runner accepts NO arguments.
# Artifact: $HARNESS_RAW_DIR/semgrep.sarif (SARIF 2.1.0)

if [ "$#" -ne 0 ]; then
  printf 'run-semgrep.sh: takes no arguments (configuration is baked in); refusing to scan\n' >&2
  exit 64
fi

set -u
_self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$_self/../env.sh"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$HARNESS_LIB_DIR/scope.sh"

TOOL=semgrep
ART="$HARNESS_RAW_DIR/$TOOL.sarif"
OUT="$HARNESS_LOG_DIR/$TOOL.stdout.log"
ERR="$HARNESS_LOG_DIR/$TOOL.stderr.log"

command -v semgrep >/dev/null 2>&1 || scope_fail "semgrep is not on PATH"
[ -d "$SEMGREP_RULES_DIR" ] || scope_fail "SEMGREP_RULES_DIR does not exist: $SEMGREP_RULES_DIR"

scope_resolve_target
scope_begin "$TOOL"

CFG=()
for d in "$SEMGREP_RULES_DIR"/*/; do
  name="$(basename "$d")"
  case "$name" in .*|stats|libsonnet|template*) continue ;; esac
  CFG+=(--config "$d")
done
[ "${#CFG[@]}" -gt 0 ] || scope_fail "no rule-bearing directories found under $SEMGREP_RULES_DIR"

mapfile -t DIRS < <(scope_dirs)
[ "${#DIRS[@]}" -gt 0 ] || scope_fail "the allowlist expanded to no existing directory under $SCAN_ROOT"

printf 'ruleset         : %s (commit %s)\n' "$SEMGREP_RULES_DIR" \
  "$(git -C "$SEMGREP_RULES_DIR" rev-parse HEAD 2>/dev/null || echo unknown)"
printf 'configs         : %s\n' "${#CFG[@]}"
printf 'credential      : SEMGREP_APP_TOKEN=%s\n' "$(scope_cred_state SEMGREP_APP_TOKEN)"

cd "$SCAN_ROOT" || scope_fail "cannot enter $SCAN_ROOT"
rm -f "$ART"

# REACH. Same reasoning as run-opengrep.sh, and this engine states the skip in its
# own summary: "Files matching .semgrepignore patterns: 845". Those bundled patterns
# skip `tests/` directories, dropping 846 of the 4,095 in-scope files — 834 of them
# the python/pyspark test modules the allowlist puts in scope. The ignore files are
# switched off so the runner's reach matches the authoritative scope (4,094 of 4,095
# selected; the one exclusion is a .png dropped as binary). --include cannot do this:
# it is applied after semgrepignore filtering and only narrows.
set +e
semgrep scan "${CFG[@]}" \
  --sarif --sarif-output "$ART" \
  --metrics=off --disable-version-check \
  --timeout 120 --timeout-threshold 0 \
  --max-target-bytes 20000000 \
  --x-ignore-semgrepignore-files \
  --oss-only \
  "${DIRS[@]}" > "$OUT" 2> "$ERR"
code=$?
set -e

scope_finish "$TOOL" "$ART" "$code"
exit "$code"
