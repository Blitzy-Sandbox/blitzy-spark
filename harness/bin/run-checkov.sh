#!/usr/bin/env bash
# harness/bin/run-checkov.sh — Checkov runner. Takes NO arguments; configuration baked in.
#
#   scan target : the allowlist scope of $SPARK_SRC (one -d per expanded directory). src/test
#                 never appears in that expansion, so test resources are excluded.
#   frameworks  : kubernetes,dockerfile,yaml,json,helm,kustomize — Checkov's role here is
#                 Kubernetes/IaC misconfiguration. The `secrets` framework is deliberately NOT
#                 enabled: Gitleaks and Trivy own secrets, and Checkov's secret findings would
#                 put matched material into the artifact for no added coverage.
#   feed        : bundled policies only; --skip-download means no Prisma/Bridgecrew platform call.
#   artifact    : $HARNESS_RAW_DIR/checkov.json   (Checkov native JSON. NOTE: with more than one
#                 framework the top level is a JSON ARRAY of per-framework report objects, each
#                 {check_type, results:{failed_checks:[...]}} — see harness/ENVIRONMENT.md.)
#   exit code   : Checkov's own (0 = no failed checks, 1 = failed checks found).
set -uo pipefail
[ $# -eq 0 ] || { echo "run-checkov.sh: takes no arguments" >&2; exit 64; }

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
. "$HARNESS_DIR/env.sh"
# shellcheck source=/dev/null
. "$HARNESS_DIR/lib/scope.sh"

harness_begin checkov
ARTIFACT="$HARNESS_RAW_DIR/checkov.json"
OUTDIR="$(mktemp -d "${TMPDIR:-/tmp}/harness-checkov.XXXXXX")"

DIR_ARGS=()
while IFS= read -r d; do DIR_ARGS+=(-d "$d"); done < <(harness_scope_dirs)
[ "${#DIR_ARGS[@]}" -gt 0 ] || harness_die "allowlist expanded to no directory under $HARNESS_SCAN_ROOT"
echo "harness: invocation: checkov --framework kubernetes,dockerfile,yaml,json,helm,kustomize ($(( ${#DIR_ARGS[@]} / 2 )) scope directories)"

checkov \
  "${DIR_ARGS[@]}" \
  --framework kubernetes,dockerfile,yaml,json,helm,kustomize \
  --skip-path '.*/src/test/.*' \
  --skip-download \
  --compact \
  --quiet \
  --output json \
  --output-file-path "$OUTDIR" < /dev/null
rc=$?

if [ -f "$OUTDIR/results_json.json" ]; then
  mv -f "$OUTDIR/results_json.json" "$ARTIFACT"
fi
rmdir "$OUTDIR" 2>/dev/null

harness_finish checkov "$rc" "$ARTIFACT"
exit $rc
