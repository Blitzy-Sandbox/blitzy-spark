#!/usr/bin/env bash
# run-dependency-check.sh — OWASP Dependency-Check 13.0.0 SCA over the twelve roots.
#
# Feed: the seeded keyless NIST NVD JSON 2.0 datafeed in $HARNESS_DC_DATA_DIR.
# --noupdate means no feed refresh at scan time. --disableOssIndex is passed
# explicitly: Sonatype OSS Index no longer answers anonymously (13.0.0 self-disables
# with "Authentication with token is now required"), so the disabling is ours and recorded.
# Runs under JAVA_HOME (Temurin 17) — JDK 21 is reserved for Joern.
# Configuration is baked in; this runner accepts NO arguments.
# Artifact: $HARNESS_RAW_DIR/dependency-check.json (native JSON report)

if [ "$#" -ne 0 ]; then
  printf 'run-dependency-check.sh: takes no arguments (configuration is baked in); refusing to scan\n' >&2
  exit 64
fi

set -u
_self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$_self/../env.sh"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$HARNESS_LIB_DIR/scope.sh"

TOOL=dependency-check
ART="$HARNESS_RAW_DIR/$TOOL.json"
OUT="$HARNESS_LOG_DIR/$TOOL.stdout.log"
ERR="$HARNESS_LOG_DIR/$TOOL.stderr.log"
WORK="$HARNESS_LOG_DIR/$TOOL.out"

[ -x "$DEPENDENCY_CHECK_HOME/bin/dependency-check.sh" ] || scope_fail "dependency-check.sh not executable under DEPENDENCY_CHECK_HOME=$DEPENDENCY_CHECK_HOME"
[ -f "$HARNESS_DC_DATA_DIR/odc.mv.db" ] || scope_fail "no seeded NVD database at HARNESS_DC_DATA_DIR=$HARNESS_DC_DATA_DIR (--noupdate needs a seeded feed)"

scope_resolve_target
scope_begin "$TOOL"

mapfile -t DIRS < <(scope_dirs)
[ "${#DIRS[@]}" -gt 0 ] || scope_fail "the allowlist expanded to no existing directory under $SCAN_ROOT"

SCANARGS=()
for d in "${DIRS[@]}"; do SCANARGS+=(--scan "$SCAN_ROOT/$d"); done

printf 'data dir        : %s (keyless NVD JSON 2.0 datafeed)\n' "$HARNESS_DC_DATA_DIR"
printf 'jdk             : %s\n' "$JAVA_HOME"
printf 'flags           : --noupdate --disableOssIndex --prettyPrint --format JSON\n'
printf 'credential      : NVD_API_KEY=%s  (OSS Index analyzer disabled explicitly)\n' "$(scope_cred_state NVD_API_KEY)"

rm -rf "$WORK"; mkdir -p "$WORK"
rm -f "$ART"
cd "$SCAN_ROOT" || scope_fail "cannot enter $SCAN_ROOT"
set +e
JAVA_HOME="$JAVA_HOME" "$DEPENDENCY_CHECK_HOME/bin/dependency-check.sh" \
  --project "spark-pinned-$SPARK_SRC_COMMIT" \
  --noupdate --disableOssIndex \
  --data "$HARNESS_DC_DATA_DIR" \
  "${SCANARGS[@]}" \
  --format JSON --prettyPrint --out "$WORK" \
  > "$OUT" 2> "$ERR"
code=$?
set -e

if [ -f "$WORK/dependency-check-report.json" ]; then
  cp "$WORK/dependency-check-report.json" "$ART"
fi

scope_finish "$TOOL" "$ART" "$code"
exit "$code"
