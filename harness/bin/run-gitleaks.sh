#!/usr/bin/env bash
# run-gitleaks.sh — Gitleaks 8.30.1 secret scanning over the twelve authoritative roots.
#
# `gitleaks dir` takes EXACTLY ONE path and silently falls back to the working
# directory when handed more, so this runner invokes it ONCE PER SCOPE DIRECTORY
# from the scan root with a root-relative path, then concatenates the per-directory
# JSON arrays into one artifact. Path base of every record: the SPARK_SRC root.
#
# Redaction is on, so a matched secret's value never reaches the artifact.
# Configuration is baked in; this runner accepts NO arguments.
# Artifact: $HARNESS_RAW_DIR/gitleaks.json (native JSON array)

if [ "$#" -ne 0 ]; then
  printf 'run-gitleaks.sh: takes no arguments (configuration is baked in); refusing to scan\n' >&2
  exit 64
fi

set -u
_self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$_self/../env.sh"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$HARNESS_LIB_DIR/scope.sh"

TOOL=gitleaks
ART="$HARNESS_RAW_DIR/$TOOL.json"
OUT="$HARNESS_LOG_DIR/$TOOL.stdout.log"
ERR="$HARNESS_LOG_DIR/$TOOL.stderr.log"
PART_DIR="$HARNESS_LOG_DIR/$TOOL.parts"

command -v gitleaks >/dev/null 2>&1 || scope_fail "gitleaks is not on PATH"

scope_resolve_target
scope_begin "$TOOL"

mapfile -t DIRS < <(scope_dirs)
[ "${#DIRS[@]}" -gt 0 ] || scope_fail "the allowlist expanded to no existing directory under $SCAN_ROOT"

printf 'ruleset         : default rule set built into gitleaks 8.30.1\n'
# shellcheck disable=SC2016  # the backticks are literal text in a log line
printf 'invocation      : one `gitleaks dir <one-path>` per scope directory (%s invocations)\n' "${#DIRS[@]}"
printf 'path base       : %s (root-relative paths; cwd is the scan root)\n' "$SCAN_ROOT"
printf 'redaction       : --redact=100\n'

rm -rf "$PART_DIR"; mkdir -p "$PART_DIR"
rm -f "$ART" "$OUT" "$ERR"
cd "$SCAN_ROOT" || scope_fail "cannot enter $SCAN_ROOT"

worst=0
for d in "${DIRS[@]}"; do
  slug="$(printf '%s' "$d" | tr '/' '_')"
  set +e
  gitleaks dir "$d" \
    --report-format json --report-path "$PART_DIR/$slug.json" \
    --redact=100 --no-banner --no-color \
    --log-level warn --exit-code 2 >> "$OUT" 2>> "$ERR"
  rc=$?
  set -e
  printf 'invocation %-60s exit=%s\n' "$d" "$rc" >> "$OUT"
  # 0 = no leaks, 2 = leaks found (both are successful scans); anything else is a failure.
  if [ "$rc" -ne 0 ] && [ "$rc" -ne 2 ]; then worst="$rc"; fi
  if [ "$rc" -eq 2 ] && [ "$worst" -eq 0 ]; then worst=2; fi
done

# Concatenate the per-directory arrays into one JSON array artifact.
python3 - "$PART_DIR" "$ART" <<'PY'
import json, sys, pathlib
part_dir, art = pathlib.Path(sys.argv[1]), sys.argv[2]
rows = []
for f in sorted(part_dir.glob('*.json')):
    with f.open() as fh:
        data = json.load(fh)
    if isinstance(data, list):
        rows.extend(data)
with open(art, 'w') as fh:
    json.dump(rows, fh, indent=1)
print(f'merged {len(rows)} findings from {len(list(part_dir.glob("*.json")))} per-directory reports')
PY
merge=$?
[ "$merge" -eq 0 ] || scope_fail "merging the per-directory gitleaks reports failed"

scope_finish "$TOOL" "$ART" "$worst"
exit "$worst"
