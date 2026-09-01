#!/usr/bin/env bash
# run-checkov.sh — Checkov 3.3.12 IaC misconfiguration scanning over the twelve roots.
# Policies are the ones bundled with 3.3.12; --skip-download keeps the scan offline
# and stops it fetching external modules or policy metadata.
# Configuration is baked in; this runner accepts NO arguments.
# Artifact: $HARNESS_RAW_DIR/checkov.json (native JSON; object form or multi-framework array)

if [ "$#" -ne 0 ]; then
  printf 'run-checkov.sh: takes no arguments (configuration is baked in); refusing to scan\n' >&2
  exit 64
fi

set -u
_self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$_self/../env.sh"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$HARNESS_LIB_DIR/scope.sh"

TOOL=checkov
ART="$HARNESS_RAW_DIR/$TOOL.json"
OUT="$HARNESS_LOG_DIR/$TOOL.stdout.log"
ERR="$HARNESS_LOG_DIR/$TOOL.stderr.log"
WORK="$HARNESS_LOG_DIR/$TOOL.out"

command -v checkov >/dev/null 2>&1 || scope_fail "checkov is not on PATH"

scope_resolve_target
scope_begin "$TOOL"

mapfile -t DIRS < <(scope_dirs)
[ "${#DIRS[@]}" -gt 0 ] || scope_fail "the allowlist expanded to no existing directory under $SCAN_ROOT"

TARGETS=()
for d in "${DIRS[@]}"; do TARGETS+=(-d "$d"); done

printf 'policies        : bundled with checkov 3.3.12 (not separately versioned)\n'
printf 'frameworks      : all bundled frameworks (no --framework filter)\n'
printf 'flags           : --skip-download --quiet --compact -o json\n'
printf 'credential      : BC_API_KEY=%s (severities require a licence and stay absent)\n' "$(scope_cred_state BC_API_KEY)"

rm -rf "$WORK"; mkdir -p "$WORK"
rm -f "$ART"
cd "$SCAN_ROOT" || scope_fail "cannot enter $SCAN_ROOT"
set +e
checkov "${TARGETS[@]}" \
  --skip-download --quiet --compact \
  --output json --output-file-path "$WORK" \
  > "$OUT" 2> "$ERR"
code=$?
set -e

if [ -f "$WORK/results_json.json" ]; then
  cp "$WORK/results_json.json" "$ART"
fi

scope_finish "$TOOL" "$ART" "$code"
exit "$code"
