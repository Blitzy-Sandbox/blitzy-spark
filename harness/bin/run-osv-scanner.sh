#!/usr/bin/env bash
# run-osv-scanner.sh — OSV-Scanner 2.5.1 dependency vulnerability scanning.
#
# No local database: OSV-Scanner queries the OSV API live at scan time, which is a
# recorded reproducibility property of every count it produces.
#
# EXPECTED OUTCOME on this scope: the twelve authoritative roots contain ZERO
# dependency manifests, so the tool resolves no packages, prints
# "No package sources found, --help for usage information." and exits 128 without
# writing an artifact. That is the tool completing with nothing in scope to work on,
# in its own words — not a failure.
# Configuration is baked in; this runner accepts NO arguments.
# Artifact (only if packages are found): $HARNESS_RAW_DIR/osv-scanner.json

if [ "$#" -ne 0 ]; then
  printf 'run-osv-scanner.sh: takes no arguments (configuration is baked in); refusing to scan\n' >&2
  exit 64
fi

set -u
_self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$_self/../env.sh"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$HARNESS_LIB_DIR/scope.sh"

TOOL=osv-scanner
ART="$HARNESS_RAW_DIR/$TOOL.json"
OUT="$HARNESS_LOG_DIR/$TOOL.stdout.log"
ERR="$HARNESS_LOG_DIR/$TOOL.stderr.log"

command -v osv-scanner >/dev/null 2>&1 || scope_fail "osv-scanner is not on PATH"

scope_resolve_target
scope_begin "$TOOL"

mapfile -t DIRS < <(scope_dirs)
[ "${#DIRS[@]}" -gt 0 ] || scope_fail "the allowlist expanded to no existing directory under $SCAN_ROOT"

printf 'database        : none local - queries the OSV API (https://api.osv.dev) at scan time\n'
printf 'invocation      : one invocation over all %s scope directories, --recursive\n' "${#DIRS[@]}"

rm -f "$ART"
cd "$SCAN_ROOT" || scope_fail "cannot enter $SCAN_ROOT"
set +e
osv-scanner scan source --recursive \
  --format json --output-file "$ART" \
  --verbosity info \
  -- "${DIRS[@]}" > "$OUT" 2> "$ERR"
code=$?
set -e

# An artifact of zero bytes is not an artifact: leave the tree runner-clean so the
# absent-artifact case stays distinguishable from an empty parse.
if [ -f "$ART" ] && [ ! -s "$ART" ]; then rm -f "$ART"; fi

scope_finish "$TOOL" "$ART" "$code"
exit "$code"
