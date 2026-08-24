#!/usr/bin/env bash
# harness/lib/scope.sh — the shared scope contract every runner sources.
#
# Not a runner: sourced only. Provides
#   scope_fail <msg>          diagnostic to stderr, exit 78 (configuration fault)
#   scope_resolve_target      sets SCAN_ROOT from SPARK_SRC (or the smoke override)
#   scope_dirs                prints the allowlist's existing directories, one per line
#   scope_begin <tool>        prints the run header, starts the timer
#   scope_finish <tool> <artifact> <exit>   prints exit code, elapsed, artifact size
#   scope_cred_state <VAR>    prints exactly "set" or "absent" (never the value)
#
# The argument guard lives at the top of each runner, before this file or the
# environment file is sourced, so a runner handed an argument exits without
# resolving a target or invoking a tool.

# Exit 78 == EX_CONFIG: a configuration fault to correct, not a scan outcome.
scope_fail() {
  printf 'CONFIGURATION FAULT: %s\n' "$*" >&2
  exit 78
}

scope_resolve_target() {
  if [ -n "${HARNESS_SMOKE_TARGET:-}" ]; then
    # Setup-time verification only. Never a fallback for a real scan.
    [ -d "$HARNESS_SMOKE_TARGET" ] || scope_fail "HARNESS_SMOKE_TARGET is set but is not a directory: $HARNESS_SMOKE_TARGET"
    SCAN_ROOT="$HARNESS_SMOKE_TARGET"
    SCAN_ROOT_SOURCE="HARNESS_SMOKE_TARGET (SETUP-TIME OVERRIDE - NOT A REAL SCAN)"
  else
    [ -n "${SPARK_SRC:-}" ] || scope_fail "SPARK_SRC is not set; the scan target must come from the environment, never from the working directory"
    [ -d "$SPARK_SRC" ] || scope_fail "SPARK_SRC is not a directory: $SPARK_SRC"
    SCAN_ROOT="$(cd "$SPARK_SRC" && pwd)"
    SCAN_ROOT_SOURCE="SPARK_SRC"
  fi
  export SCAN_ROOT SCAN_ROOT_SOURCE
}

# Expand the twelve authoritative globs against SCAN_ROOT. Arithmetic on the
# allowlist, never an extension of it: the trailing /** is stripped to yield the
# directory each glob names, and any path containing src/test is skipped.
scope_dirs() {
  [ -n "${SCAN_ROOT:-}" ] || scope_fail "scope_dirs called before scope_resolve_target"
  [ -f "${HARNESS_SCOPE_FILE:-}" ] || scope_fail "allowlist not found: ${HARNESS_SCOPE_FILE:-<unset>}"
  if [ -n "${HARNESS_SMOKE_TARGET:-}" ]; then
    # Setup-time override: the smoke directory IS the single scope directory. The
    # allowlist is untouched; a real scan leaves HARNESS_SMOKE_TARGET unset.
    printf '.\n'
    return 0
  fi
  (
    cd "$SCAN_ROOT" || scope_fail "cannot enter scan root $SCAN_ROOT"
    shopt -s globstar nullglob dotglob
    while IFS= read -r glob || [ -n "$glob" ]; do
      case "$glob" in ''|\#*) continue ;; esac
      base="${glob%/\*\*}"
      for path in $base; do
        [ -d "$path" ] || continue
        case "$path" in *src/test*) continue ;; esac
        printf '%s\n' "$path"
      done
    done < "$HARNESS_SCOPE_FILE"
  ) | sort -u
}

scope_begin() {
  SCOPE_TOOL="$1"
  SCOPE_T0="$(date +%s)"
  export SCOPE_TOOL SCOPE_T0
  [ -d "${HARNESS_RAW_DIR:-}" ] || scope_fail "HARNESS_RAW_DIR does not exist: ${HARNESS_RAW_DIR:-<unset>}"
  [ -d "${HARNESS_LOG_DIR:-}" ] || scope_fail "HARNESS_LOG_DIR does not exist: ${HARNESS_LOG_DIR:-<unset>}"
  printf 'tool            : %s\n' "$SCOPE_TOOL"
  printf 'scan root       : %s (from %s)\n' "$SCAN_ROOT" "$SCAN_ROOT_SOURCE"
  printf 'raw dir         : %s\n' "$HARNESS_RAW_DIR"
  printf 'log dir         : %s\n' "$HARNESS_LOG_DIR"
  printf 'allowlist       : %s (sha256 %s)\n' "$HARNESS_SCOPE_FILE" "$(sha256sum "$HARNESS_SCOPE_FILE" | cut -d' ' -f1)"
  printf 'scope dirs      : %s\n' "$(scope_dirs | wc -l | tr -d ' ')"
  printf 'start           : %s\n' "$(date -Is)"
}

scope_finish() {
  tool="$1"; artifact="$2"; code="$3"
  elapsed=$(( $(date +%s) - ${SCOPE_T0:-$(date +%s)} ))
  if [ -f "$artifact" ]; then
    size="$(stat -c%s "$artifact")"
  else
    size="MISSING"
  fi
  printf 'exit code       : %s\n' "$code"
  printf 'elapsed seconds : %s\n' "$elapsed"
  printf 'artifact        : %s (%s bytes)\n' "$artifact" "$size"
  printf 'end             : %s\n' "$(date -Is)"
  {
    printf 'tool=%s\n' "$tool"
    printf 'exit_code=%s\n' "$code"
    printf 'elapsed_seconds=%s\n' "$elapsed"
    printf 'artifact=%s\n' "$artifact"
    printf 'artifact_bytes=%s\n' "$size"
    printf 'scan_root=%s\n' "$SCAN_ROOT"
    printf 'scan_root_source=%s\n' "$SCAN_ROOT_SOURCE"
  } > "$HARNESS_LOG_DIR/$tool.status"
}

# Prints a fixed token. Uses ${VAR:+set} only: ${VAR:-absent} would emit the
# variable's own value in the set-arm and write a live credential into a log
# this pipeline preserves verbatim.
scope_cred_state() {
  eval "_state=\${$1:+set}"
  if [ "${_state:-}" = "set" ]; then printf 'set\n'; else printf 'absent\n'; fi
  unset _state
}
