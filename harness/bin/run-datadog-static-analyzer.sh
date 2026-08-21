#!/usr/bin/env bash
# harness/bin/run-datadog-static-analyzer.sh — datadog-static-analyzer runner. NO arguments.
#
#   scan target : $SPARK_SRC with one -u (subdirectory) per expanded allowlist directory, so the
#                 scan is confined to the allowlist scope, plus -p ignore globs for src/test.
#   rules       : the analyzer's OWN BUNDLED rule set (1093 static-analysis rules). No
#                 static-analysis.datadog.yml is written into the pinned tree — the tree is
#                 read-only for this harness — and no rules.json is fetched, because that needs
#                 Datadog credentials. Bundled rule languages do NOT include Scala; they cover
#                 java, python, javascript, typescript, go, ruby, php, c#, kotlin, rust, bash,
#                 dart. That is recorded in harness/ENVIRONMENT.md as a capability fact.
#   AI path     : UNAVAILABLE in this environment. --enable-secrets (the Datadog-backed path) is
#                 documented as requiring Datadog API keys; the credential source is the env vars
#                 DD_API_KEY and DD_APP_KEY, which are absent here. No credential value is ever
#                 read into a file or a log. Set HARNESS_DD_SECRETS=1 only in an environment
#                 where those variables are genuinely present.
#   artifact    : $HARNESS_RAW_DIR/datadog-static-analyzer.sarif   (SARIF)
#   exit code   : the analyzer's own.
set -uo pipefail
[ $# -eq 0 ] || { echo "run-datadog-static-analyzer.sh: takes no arguments" >&2; exit 64; }

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
. "$HARNESS_DIR/env.sh"
# shellcheck source=/dev/null
. "$HARNESS_DIR/lib/scope.sh"

harness_begin datadog-static-analyzer
ARTIFACT="$HARNESS_RAW_DIR/datadog-static-analyzer.sarif"

SUB_ARGS=()
while IFS= read -r rel; do
  # "." means the scope IS the scan root, which -i already expresses; -u would be redundant.
  [ "$rel" = "." ] && continue
  SUB_ARGS+=(-u "$rel")
done < <(harness_scope_dirs_relative)
[ "$(harness_scope_dirs | wc -l)" -gt 0 ] || harness_die "allowlist expanded to no directory under $HARNESS_SCAN_ROOT"

SECRET_FLAGS=(--enable-secrets false)
if [ "${HARNESS_DD_SECRETS:-0}" = "1" ] && [ -n "${DD_API_KEY:-}" ] && [ -n "${DD_APP_KEY:-}" ]; then
  SECRET_FLAGS=(--enable-secrets true)
  echo "harness: datadog secrets/AI path: ENABLED (credentials present via DD_API_KEY/DD_APP_KEY)"
else
  echo "harness: datadog secrets/AI path: DISABLED (credential source DD_API_KEY/DD_APP_KEY: ${DD_API_KEY:+set}${DD_API_KEY:-absent}/${DD_APP_KEY:+set}${DD_APP_KEY:-absent})"
fi
echo "harness: invocation: datadog-static-analyzer -i $HARNESS_SCAN_ROOT ($(( ${#SUB_ARGS[@]} / 2 )) scope subdirectories)"

datadog-static-analyzer \
  -i "$HARNESS_SCAN_ROOT" \
  "${SUB_ARGS[@]}" \
  -p '**/src/test/**' \
  --enable-static-analysis true \
  "${SECRET_FLAGS[@]}" \
  -f sarif \
  -o "$ARTIFACT" < /dev/null
rc=$?

harness_finish datadog-static-analyzer "$rc" "$ARTIFACT"
exit $rc
