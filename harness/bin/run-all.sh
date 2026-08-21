#!/usr/bin/env bash
# harness/bin/run-all.sh — convenience wrapper that invokes the nine runners in sequence.
#
# NOT A RUNNER, and NOT what the scanning run uses. The scanning run invokes the nine
# run-<tool>.sh scripts individually, so that elapsed time and exit status are attributable to
# exactly one tool; this wrapper exists only for a human wanting one command.
#
# It never aborts on a non-zero runner exit: a failing tool is recorded and the sequence continues.
# Per-tool stdout/stderr/metadata land in $HARNESS_LOG_DIR, artifacts in $HARNESS_RAW_DIR.
set -uo pipefail

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
. "$HARNESS_DIR/env.sh"

TOOLS=(trivy osv-scanner dependency-check gitleaks checkov opengrep semgrep joern datadog-static-analyzer)
mkdir -p "$HARNESS_RAW_DIR" "$HARNESS_LOG_DIR"

for tool in "${TOOLS[@]}"; do
  runner="$HARNESS_DIR/bin/run-$tool.sh"
  start_epoch="$(date -u +%s)"
  start_iso="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "=== $tool: $runner ==="
  "$runner" > "$HARNESS_LOG_DIR/$tool.stdout.log" 2> "$HARNESS_LOG_DIR/$tool.stderr.log"
  rc=$?
  end_epoch="$(date -u +%s)"
  printf '{\n "tool": "%s",\n "invocation": "%s",\n "exit_code": %d,\n "elapsed_seconds": %d,\n "started_at": "%s",\n "finished_at": "%s"\n}\n' \
    "$tool" "$runner" "$rc" "$(( end_epoch - start_epoch ))" "$start_iso" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    > "$HARNESS_LOG_DIR/$tool.meta.json"
  echo "--- $tool exit_code=$rc elapsed=$(( end_epoch - start_epoch ))s"
done

echo "=== artifacts ==="
ls -la "$HARNESS_RAW_DIR"
