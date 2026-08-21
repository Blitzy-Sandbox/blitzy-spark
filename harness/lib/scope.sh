#!/usr/bin/env bash
# harness/lib/scope.sh — shared scope helper. NOT A RUNNER, never invoked directly:
# it is sourced by each harness/bin/run-<tool>.sh. harness/bin/ contains only the
# nine tool runners plus run-all.sh, so that "one runner per tool" stays literal.
#
# It provides:
#   harness_scan_root            -> the tree every runner scans ($SPARK_SRC, or the
#                                   setup-time smoke target when HARNESS_SMOKE_TARGET is set)
#   harness_scope_dirs           -> the allowlist globs expanded to existing directories
#                                   under the scan root, one absolute path per line
#   harness_scope_patterns       -> the allowlist globs verbatim, one per line
#   harness_begin <tool>         -> resolve paths, create the artifact dirs, print the banner
#   harness_finish <tool> <rc>   -> print the trailer with the tool's own exit code
# Test sources are excluded by every runner: the expansion never returns a src/test
# directory and each runner additionally passes its own tool-level src/test exclusion.

harness_die() { echo "harness: $*" >&2; exit 78; }   # 78 = EX_CONFIG

# The tree every runner scans. In normal operation this is ALWAYS the pinned tree at
# $SPARK_SRC. HARNESS_SMOKE_TARGET is a setup-time-only override, given as a path
# relative to $SPARK_SRC, used by the environment-setup run to prove the runners work
# against one small directory. The scanning run leaves it unset.
harness_scan_root() {
  local root="${SPARK_SRC:?SPARK_SRC is unset: source harness/env.sh first}"
  if [ -n "${HARNESS_SMOKE_TARGET:-}" ]; then
    printf '%s\n' "$root/${HARNESS_SMOKE_TARGET#/}"
  else
    printf '%s\n' "$root"
  fi
}

harness_scope_patterns() {
  [ -r "${HARNESS_SCOPE_FILE:?}" ] || harness_die "allowlist not readable: $HARNESS_SCOPE_FILE"
  grep -vE '^[[:space:]]*(#|$)' "$HARNESS_SCOPE_FILE"
}

# Expand the allowlist globs into directories that exist under the scan root.
# A glob such as sql/connect/**/src/main/** expands through however many
# intermediate module directories exist; python/pyspark/** expands to the
# package root. Nothing under a src/test/ segment is ever returned.
harness_scope_dirs() {
  local root pattern base
  root="$(harness_scan_root)"
  [ -d "$root" ] || harness_die "scan root is not a directory: $root"
  if [ -n "${HARNESS_SMOKE_TARGET:-}" ]; then
    # Setup-time smoke proof: the scope is the smoke directory itself.
    printf '%s\n' "$root"
    return 0
  fi
  # shellcheck disable=SC2016
  ( shopt -s globstar nullglob dotglob
    cd "$root" || exit 0
    while IFS= read -r pattern; do
      # Strip the trailing /** — we want the containing directory, not its files.
      base="${pattern%/\*\*}"
      for d in $base; do
        [ -d "$d" ] || continue
        case "$d" in */src/test/*|*/src/test) continue ;; esac
        printf '%s/%s\n' "$root" "$d"
      done
    done < <(harness_scope_patterns)
  ) | sort -u
}

# Same set, expressed relative to the scan root (some tools take relative subdirectories).
# An entry equal to the scan root itself comes out as "." — a caller that cannot express
# that should pass no subdirectory restriction at all, since the scan root already is the scope.
harness_scope_dirs_relative() {
  local root; root="$(harness_scan_root)"
  harness_scope_dirs | sed -e "s|^$root/||" -e "s|^$root\$|.|"
}

harness_begin() {
  local tool="$1"
  HARNESS_TOOL="$tool"
  HARNESS_SCAN_ROOT="$(harness_scan_root)"
  export HARNESS_TOOL HARNESS_SCAN_ROOT
  mkdir -p "$HARNESS_RAW_DIR" "$HARNESS_LOG_DIR" || harness_die "cannot create artifact dirs"
  HARNESS_START_EPOCH="$(date -u +%s)"
  echo "harness: tool=$tool"
  echo "harness: scan_root=$HARNESS_SCAN_ROOT"
  echo "harness: raw_dir=$HARNESS_RAW_DIR"
  echo "harness: allowlist=$HARNESS_SCOPE_FILE"
  echo "harness: started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}

harness_finish() {
  local tool="$1" rc="$2" artifact="${3:-}"
  echo "harness: tool=$tool exit_code=$rc elapsed_seconds=$(( $(date -u +%s) - HARNESS_START_EPOCH ))"
  if [ -n "$artifact" ]; then
    if [ -f "$artifact" ]; then
      echo "harness: artifact=$artifact bytes=$(wc -c < "$artifact")"
    else
      echo "harness: artifact=$artifact MISSING"
    fi
  fi
  echo "harness: finished_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  return "$rc"
}
