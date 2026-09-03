#!/usr/bin/env bash
# run-datadog-static-analyzer.sh — datadog-static-analyzer 0.9.1 (rev f76636e4) SAST.
# Rules come from the LOCAL captured SAST rule file (48 rulesets / 1,093 rules), so the
# scan makes no API call and the rule set is pinned by digest.
# Configuration is baked in; this runner accepts NO arguments.
# Artifact: $HARNESS_RAW_DIR/datadog-static-analyzer.sarif (SARIF 2.1.0)

if [ "$#" -ne 0 ]; then
  printf 'run-datadog-static-analyzer.sh: takes no arguments (configuration is baked in); refusing to scan\n' >&2
  exit 64
fi

set -u
_self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$_self/../env.sh"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$HARNESS_LIB_DIR/scope.sh"

TOOL=datadog-static-analyzer
ART="$HARNESS_RAW_DIR/$TOOL.sarif"
OUT="$HARNESS_LOG_DIR/$TOOL.stdout.log"
ERR="$HARNESS_LOG_DIR/$TOOL.stderr.log"

command -v datadog-static-analyzer >/dev/null 2>&1 || scope_fail "datadog-static-analyzer is not on PATH"
[ -f "$DD_SAST_RULES_FILE" ] || scope_fail "DD_SAST_RULES_FILE does not exist: $DD_SAST_RULES_FILE"

scope_resolve_target
scope_begin "$TOOL"

mapfile -t DIRS < <(scope_dirs)
[ "${#DIRS[@]}" -gt 0 ] || scope_fail "the allowlist expanded to no existing directory under $SCAN_ROOT"

SUBDIRS=()
for d in "${DIRS[@]}"; do SUBDIRS+=(-u "$d"); done

printf 'rules file      : %s (sha256 %s)\n' "$DD_SAST_RULES_FILE" \
  "$(sha256sum "$DD_SAST_RULES_FILE" | cut -d' ' -f1)"
printf 'credential      : DD_API_KEY=%s DD_APP_KEY=%s\n' \
  "$(scope_cred_state DD_API_KEY)" "$(scope_cred_state DD_APP_KEY)"
printf 'secrets scanner : disabled (requires Datadog API keys)\n'

cd "$SCAN_ROOT" || scope_fail "cannot enter $SCAN_ROOT"
rm -f "$ART"
set +e
datadog-static-analyzer \
  -i "$SCAN_ROOT" "${SUBDIRS[@]}" \
  -r "$DD_SAST_RULES_FILE" \
  -f sarif -o "$ART" \
  --enable-static-analysis true \
  --enable-secrets false \
  > "$OUT" 2> "$ERR"
code=$?
set -e

scope_finish "$TOOL" "$ART" "$code"
exit "$code"
