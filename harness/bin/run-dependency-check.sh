#!/usr/bin/env bash
# harness/bin/run-dependency-check.sh — OWASP Dependency-Check runner. Takes NO arguments.
#
#   scan target : $SPARK_SRC (the pinned Spark tree). The whole tree is deliberate: this is a
#                 Maven-native dependency scanner and its evidence is pom.xml plus the jars the
#                 setup build produced under */target/. src/test is excluded.
#   feed        : the local H2 database populated at setup time from the OFFICIAL NVD JSON 2.0
#                 data feeds (no NVD API key exists in this environment; Dependency-Check 13
#                 rejects keyless API use). The runner does NOT attempt an update — feed state
#                 is therefore "not attempted", with the populated-at timestamp recorded in
#                 harness/ENVIRONMENT.md. Set HARNESS_DC_UPDATE=1 to update via the same feeds.
#   data dir    : $HARNESS_DC_DATA_DIR. Concurrent clones MUST point this at their own copy
#                 (data-0/data-1/data-2 exist for that) — H2 does not take concurrent writers.
#   artifact    : $HARNESS_RAW_DIR/dependency-check.json   (Dependency-Check native JSON)
#   exit code   : Dependency-Check's own.
set -uo pipefail
[ $# -eq 0 ] || { echo "run-dependency-check.sh: takes no arguments" >&2; exit 64; }

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
. "$HARNESS_DIR/env.sh"
# shellcheck source=/dev/null
. "$HARNESS_DIR/lib/scope.sh"

harness_begin dependency-check
ARTIFACT="$HARNESS_RAW_DIR/dependency-check.json"
OUTDIR="$(mktemp -d "${TMPDIR:-/tmp}/harness-dc.XXXXXX")"

# Dependency-Check is verified on JDK 21 in this environment.
export JAVA_HOME="$JAVA_HOME_21"
export PATH="$JAVA_HOME/bin:$PATH"

UPDATE_FLAGS=(--noupdate)
if [ "${HARNESS_DC_UPDATE:-0}" = "1" ]; then
  UPDATE_FLAGS=(--nvdDatafeed "https://nvd.nist.gov/feeds/json/cve/2.0/nvdcve-2.0-{0}.json.gz")
fi

echo "harness: dependency-check data dir: $HARNESS_DC_DATA_DIR"
find "$HARNESS_DC_DATA_DIR" -maxdepth 1 -mindepth 1 -printf 'harness: data: %M %12s %f\n' 2>/dev/null || true
echo "harness: invocation: dependency-check.sh ${UPDATE_FLAGS[*]} --scan $HARNESS_SCAN_ROOT"

"$DEPENDENCY_CHECK_HOME/bin/dependency-check.sh" \
  --project "spark-pinned-${SPARK_SRC_COMMIT:0:12}" \
  --scan "$HARNESS_SCAN_ROOT" \
  --exclude "**/src/test/**" \
  --data "$HARNESS_DC_DATA_DIR" \
  "${UPDATE_FLAGS[@]}" \
  --format JSON \
  --prettyPrint \
  --out "$OUTDIR" < /dev/null
rc=$?

if [ -f "$OUTDIR/dependency-check-report.json" ]; then
  mv -f "$OUTDIR/dependency-check-report.json" "$ARTIFACT"
fi
rmdir "$OUTDIR" 2>/dev/null

harness_finish dependency-check "$rc" "$ARTIFACT"
exit $rc
