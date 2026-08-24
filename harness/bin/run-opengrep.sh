#!/usr/bin/env bash
# run-opengrep.sh — Opengrep 1.27.1 SAST over the twelve authoritative scope roots.
# Configuration is baked in; this runner accepts NO arguments.
# Artifact: $HARNESS_RAW_DIR/opengrep.sarif (SARIF 2.1.0)

if [ "$#" -ne 0 ]; then
  printf 'run-opengrep.sh: takes no arguments (configuration is baked in); refusing to scan\n' >&2
  exit 64
fi

set -u
_self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$_self/../env.sh"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$HARNESS_LIB_DIR/scope.sh"

TOOL=opengrep
ART="$HARNESS_RAW_DIR/$TOOL.sarif"
OUT="$HARNESS_LOG_DIR/$TOOL.stdout.log"
ERR="$HARNESS_LOG_DIR/$TOOL.stderr.log"

command -v opengrep >/dev/null 2>&1 || scope_fail "opengrep is not on PATH"
[ -d "$OPENGREP_RULES_DIR" ] || scope_fail "OPENGREP_RULES_DIR does not exist: $OPENGREP_RULES_DIR"

scope_resolve_target
scope_begin "$TOOL"

# One --config per rule-bearing top-level directory. Pointing --config at the
# ruleset root fails with InvalidRuleSchemaError on non-rule YAML
# (.github/stale.yml, stats/metacategory_to_support_tier.yml).
CFG=()
for d in "$OPENGREP_RULES_DIR"/*/; do
  name="$(basename "$d")"
  case "$name" in .*|stats|libsonnet|template*) continue ;; esac
  CFG+=(--config "$d")
done
[ "${#CFG[@]}" -gt 0 ] || scope_fail "no rule-bearing directories found under $OPENGREP_RULES_DIR"

mapfile -t DIRS < <(scope_dirs)
[ "${#DIRS[@]}" -gt 0 ] || scope_fail "the allowlist expanded to no existing directory under $SCAN_ROOT"

printf 'ruleset         : %s (commit %s)\n' "$OPENGREP_RULES_DIR" \
  "$(git -C "$OPENGREP_RULES_DIR" rev-parse HEAD 2>/dev/null || echo unknown)"
printf 'configs         : %s\n' "${#CFG[@]}"
printf 'credential      : SEMGREP_APP_TOKEN=%s (unused by opengrep)\n' "$(scope_cred_state SEMGREP_APP_TOKEN)"

# Paths in the SARIF are relative to the working directory, so run from the scan
# root and pass root-relative directories.
cd "$SCAN_ROOT" || scope_fail "cannot enter $SCAN_ROOT"
rm -f "$ART"

# REACH. By default this engine applies its bundled .semgrepignore patterns, which
# skip `tests/` directories: over the twelve roots that dropped 846 of the 4,095
# in-scope files, 834 of them the python/pyspark test modules that the allowlist
# puts squarely in scope. An in-scope file never analyzed reads exactly like a file
# with nothing to report, so the ignore files are switched off and the runner's
# reach is made to match the authoritative scope: 4,094 of 4,095 files are then
# selected, the one exclusion being a .png the engine drops as binary.
# --include cannot be used for this: it is applied AFTER semgrepignore filtering and
# only narrows. The flag is marked internal by the tool, which is acceptable at a
# pinned version and is recorded in harness/ENVIRONMENT.md.
set +e
opengrep scan "${CFG[@]}" \
  --sarif --sarif-output "$ART" \
  --timeout 120 --timeout-threshold 0 \
  --max-target-bytes 20000000 \
  --x-ignore-semgrepignore-files \
  --disable-version-check \
  "${DIRS[@]}" > "$OUT" 2> "$ERR"
code=$?
set -e

scope_finish "$TOOL" "$ART" "$code"
exit "$code"
