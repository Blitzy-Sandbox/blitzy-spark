#!/usr/bin/env bash
# harness/bin/run-osv-scanner.sh — OSV-Scanner runner. Takes NO arguments; configuration baked in.
#
#   scan target : $SPARK_SRC (the pinned Spark tree), scanned recursively for dependency
#                 manifests. The whole tree is used deliberately: pom.xml files sit outside
#                 the source allowlist but are exactly what a dependency scanner must read.
#                 No module keeps a separate manifest under src/test, so test scope
#                 contributes no dependency manifest to exclude.
#   feed        : online OSV/deps.dev (no local database). api.osv.dev + api.deps.dev were
#                 reachable at setup time; see harness/ENVIRONMENT.md for the endpoint record.
#   artifact    : $HARNESS_RAW_DIR/osv-scanner.json   (OSV-Scanner native JSON)
#   exit code   : OSV-Scanner's own (0 = no vulns, 1 = vulns found, 127/128 = usage/no packages).
#                 --allow-no-lockfiles makes a manifest-free target a clean exit rather than an error.
set -uo pipefail
[ $# -eq 0 ] || { echo "run-osv-scanner.sh: takes no arguments" >&2; exit 64; }

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
. "$HARNESS_DIR/env.sh"
# shellcheck source=/dev/null
. "$HARNESS_DIR/lib/scope.sh"

harness_begin osv-scanner
ARTIFACT="$HARNESS_RAW_DIR/osv-scanner.json"

echo "harness: invocation: osv-scanner scan source -r --allow-no-lockfiles $HARNESS_SCAN_ROOT"

osv-scanner scan source \
  --recursive \
  --allow-no-lockfiles \
  --format json \
  --output "$ARTIFACT" \
  --verbosity info \
  "$HARNESS_SCAN_ROOT"
rc=$?

harness_finish osv-scanner "$rc" "$ARTIFACT"
exit $rc
