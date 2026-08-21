#!/usr/bin/env bash
# harness/bin/run-trivy.sh — Trivy runner. Takes NO arguments; its configuration is baked in.
#
#   scan target : $SPARK_SRC (the pinned Spark tree) — the whole tree, because Trivy's
#                 dependency scanner reads build manifests (pom.xml) and built jars that
#                 legitimately sit outside the source allowlist. src/test is excluded.
#   scanners    : vuln,secret,misconfig  (this is the one tool whose class varies per finding)
#   feed        : the vulnerability DB and Java DB warmed at setup time in $TRIVY_CACHE_DIR;
#                 --skip-db-update/--skip-java-db-update keep the run deterministic and the
#                 feed state attributable to a recorded timestamp (see harness/ENVIRONMENT.md).
#                 Set HARNESS_TRIVY_UPDATE=1 to let Trivy refresh its DBs instead.
#   artifact    : $HARNESS_RAW_DIR/trivy.json   (Trivy native JSON)
#   exit code   : Trivy's own.
set -uo pipefail
[ $# -eq 0 ] || { echo "run-trivy.sh: takes no arguments" >&2; exit 64; }

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
. "$HARNESS_DIR/env.sh"
# shellcheck source=/dev/null
. "$HARNESS_DIR/lib/scope.sh"

harness_begin trivy
ARTIFACT="$HARNESS_RAW_DIR/trivy.json"

DB_FLAGS=(--skip-db-update --skip-java-db-update)
[ "${HARNESS_TRIVY_UPDATE:-0}" = "1" ] && DB_FLAGS=()

echo "harness: trivy db metadata:"; cat "$TRIVY_CACHE_DIR/db/metadata.json" 2>/dev/null; echo
echo "harness: invocation: trivy fs --scanners vuln,secret,misconfig ${DB_FLAGS[*]} $HARNESS_SCAN_ROOT"

trivy fs \
  --cache-dir "$TRIVY_CACHE_DIR" \
  --scanners vuln,secret,misconfig \
  "${DB_FLAGS[@]}" \
  --skip-dirs '**/src/test' \
  --skip-dirs '**/src/test/**' \
  --format json \
  --output "$ARTIFACT" \
  --no-progress \
  "$HARNESS_SCAN_ROOT"
rc=$?

harness_finish trivy "$rc" "$ARTIFACT"
exit $rc
