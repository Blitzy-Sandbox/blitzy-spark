#!/usr/bin/env bash
# run-trivy.sh — Trivy 0.74.0 vulnerability + secret + misconfiguration scanning.
#
# `trivy fs` takes exactly one path, so this runner invokes it ONCE PER SCOPE
# DIRECTORY from the scan root with a root-relative path, then merges the per-directory
# reports into one artifact by concatenating their Results[] arrays. Every merged
# report is checked to carry no non-empty finding section outside
# Vulnerabilities/Secrets/Misconfigurations.
#
# PATH BASE. A per-directory report states Results[].Target relative to the single path
# that invocation was given and names that path in its own ArtifactName, so the merge
# prefixes every Target with its own report's ArtifactName. In the merged artifact every
# Target is therefore relative to the scan root ($SPARK_SRC) and ArtifactName is '.'.
# The per-directory reports are retained verbatim under $HARNESS_LOG_DIR/trivy.parts/.
#
# Feeds are the seeded local caches; --skip-db-update/--skip-java-db-update mean no
# database download, and --offline-scan means no Maven Central resolution (one earlier
# run without it took 429 Too Many Requests and wrote no artifact at all).
# Configuration is baked in; this runner accepts NO arguments.
# Artifact: $HARNESS_RAW_DIR/trivy.json (native JSON)

if [ "$#" -ne 0 ]; then
  printf 'run-trivy.sh: takes no arguments (configuration is baked in); refusing to scan\n' >&2
  exit 64
fi

set -u
_self="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$_self/../env.sh"
# shellcheck source=/dev/null  # resolved at run time from the script's own location
. "$HARNESS_LIB_DIR/scope.sh"

TOOL=trivy
ART="$HARNESS_RAW_DIR/$TOOL.json"
OUT="$HARNESS_LOG_DIR/$TOOL.stdout.log"
ERR="$HARNESS_LOG_DIR/$TOOL.stderr.log"
PART_DIR="$HARNESS_LOG_DIR/$TOOL.parts"

command -v trivy >/dev/null 2>&1 || scope_fail "trivy is not on PATH"
[ -d "$TRIVY_CACHE_DIR/db" ] || scope_fail "TRIVY_CACHE_DIR holds no seeded db: $TRIVY_CACHE_DIR (--skip-db-update fails on a first run)"
[ -d "$TRIVY_CACHE_DIR/java-db" ] || scope_fail "TRIVY_CACHE_DIR holds no seeded java-db: $TRIVY_CACHE_DIR"
export TRIVY_CACHE_DIR

scope_resolve_target
scope_begin "$TOOL"

mapfile -t DIRS < <(scope_dirs)
[ "${#DIRS[@]}" -gt 0 ] || scope_fail "the allowlist expanded to no existing directory under $SCAN_ROOT"

printf 'cache dir       : %s\n' "$TRIVY_CACHE_DIR"
printf 'vuln db         : %s\n' "$(python3 -c "import json;d=json.load(open('$TRIVY_CACHE_DIR/db/metadata.json'));print('v%s UpdatedAt=%s'%(d['Version'],d['UpdatedAt']))" 2>/dev/null || echo unknown)"
printf 'java db         : %s\n' "$(python3 -c "import json;d=json.load(open('$TRIVY_CACHE_DIR/java-db/metadata.json'));print('v%s UpdatedAt=%s'%(d['Version'],d['UpdatedAt']))" 2>/dev/null || echo unknown)"
printf 'scanners        : vuln,secret,misconfig\n'
printf 'flags           : --skip-db-update --skip-java-db-update --skip-check-update --offline-scan\n'
# shellcheck disable=SC2016  # the backticks are literal text in a log line
printf 'invocation      : one `trivy fs <one-path>` per scope directory (%s invocations)\n' "${#DIRS[@]}"

rm -rf "$PART_DIR"; mkdir -p "$PART_DIR"
rm -f "$ART" "$OUT" "$ERR"
cd "$SCAN_ROOT" || scope_fail "cannot enter $SCAN_ROOT"

worst=0
for d in "${DIRS[@]}"; do
  slug="$(printf '%s' "$d" | tr '/' '_')"
  set +e
  trivy fs "$d" \
    --scanners vuln,secret,misconfig \
    --format json --output "$PART_DIR/$slug.json" \
    --skip-db-update --skip-java-db-update --skip-check-update \
    --offline-scan --no-progress --quiet >> "$OUT" 2>> "$ERR"
  rc=$?
  set -e
  printf 'invocation %-60s exit=%s\n' "$d" "$rc" >> "$OUT"
  [ "$rc" -ne 0 ] && worst="$rc"
done

python3 - "$PART_DIR" "$ART" <<'PY'
import json, sys, pathlib, posixpath
part_dir, art = pathlib.Path(sys.argv[1]), sys.argv[2]
merged, sections, unsupported, prefixed = None, 0, {}, 0

# Each per-directory report expresses Results[].Target relative to the ONE path that
# invocation was given, and names that path in its own top-level ArtifactName. Merging
# the sections as-is would therefore lose the directory a section came from and leave
# `dockerfiles/spark/Dockerfile` unanchorable. So every Target is prefixed with its own
# report's ArtifactName, making every merged Target relative to the scan root, and the
# merged ArtifactName is set to '.' (the scan root) to say what the Targets are relative
# to. The per-directory reports are retained verbatim in this tool's .parts log directory.
for f in sorted(part_dir.glob('*.json')):
    with f.open() as fh:
        rep = json.load(fh)
    for key in ('Licenses', 'ExperimentalModifiedFindings'):
        for res in (rep.get('Results') or []):
            if res.get(key):
                unsupported.setdefault(key, 0)
                unsupported[key] += len(res[key])
    base = (rep.get('ArtifactName') or '').strip()
    results = rep.get('Results') or []
    for res in results:
        tgt = res.get('Target') or ''
        if base and tgt and not tgt.startswith('/') and tgt != base and not tgt.startswith(base + '/'):
            res['Target'] = posixpath.normpath(posixpath.join(base, tgt))
            prefixed += 1
    if merged is None:
        merged = {k: v for k, v in rep.items() if k != 'Results'}
        merged['Results'] = []
    merged['ArtifactName'] = '.'
    merged['Results'].extend(results)
    sections += len(results)
if merged is None:
    merged = {'ArtifactName': '.', 'Results': []}
with open(art, 'w') as fh:
    json.dump(merged, fh, indent=1)
print(f'merged {sections} Results sections from {len(list(part_dir.glob("*.json")))} per-directory reports')
print(f'root-anchored {prefixed} of {sections} Result Targets by prefixing each part\'s ArtifactName')
if unsupported:
    print(f'UNSUPPORTED NON-EMPTY SECTIONS PRESENT: {unsupported}')
PY
merge=$?
[ "$merge" -eq 0 ] || scope_fail "merging the per-directory trivy reports failed"

scope_finish "$TOOL" "$ART" "$worst"
exit "$worst"
