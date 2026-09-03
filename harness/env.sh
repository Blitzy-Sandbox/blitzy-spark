#!/usr/bin/env bash
# harness/env.sh — the single environment file for the OSS security-scanner harness.
#
# Source this (do not execute it) in a fresh non-login shell before invoking any
# runner:   . harness/env.sh
#
# Every value below was established by provisioning and is recorded in
# harness/ENVIRONMENT.md. Values already present in the environment win, so a
# caller can override any of them per clone without editing this file.

# ---------------------------------------------------------------- harness paths
# HARNESS_DIR is derived from this file's own location so the harness works in
# any clone of the repository without editing.
if [ -n "${BASH_SOURCE[0]:-}" ]; then
  _harness_self="${BASH_SOURCE[0]}"
else
  _harness_self="$0"
fi
HARNESS_DIR="$(cd "$(dirname "$_harness_self")" && pwd)"
export HARNESS_DIR
HARNESS_REPO_ROOT="$(cd "$HARNESS_DIR/.." && pwd)"
export HARNESS_REPO_ROOT
unset _harness_self

export HARNESS_RAW_DIR="${HARNESS_RAW_DIR:-$HARNESS_DIR/artifacts/raw}"
export HARNESS_LOG_DIR="${HARNESS_LOG_DIR:-$HARNESS_DIR/artifacts/logs}"
export HARNESS_SCOPE_FILE="${HARNESS_SCOPE_FILE:-$HARNESS_DIR/scope/allowlist.txt}"
export HARNESS_CPG="${HARNESS_CPG:-$HARNESS_DIR/cpg/spark.cpg}"
export HARNESS_LIB_DIR="${HARNESS_LIB_DIR:-$HARNESS_DIR/lib}"

# Host-global provisioning roots (shared, read-only for scanning runs).
export HARNESS_SHARED_DIR="${HARNESS_SHARED_DIR:-/opt/blitzy-harness}"
export HARNESS_TOOLS_DIR="${HARNESS_TOOLS_DIR:-/opt/blitzy-tools}"

# Per-clone scratch. Concurrent clones MUST NOT share this: Joern writes a
# ./workspace tree into whatever directory it runs from, and two runs in one
# directory corrupt each other. BLITZY_CLONE_INDEX is the 0-based clone index.
export HARNESS_SCRATCH_DIR="${HARNESS_SCRATCH_DIR:-/tmp/blitzy-harness-scratch/${BLITZY_CLONE_INDEX:-0}}"

# ------------------------------------------------------------- the scanned tree
# SPARK_SRC is the pinned Apache Spark clone — the ONLY tree anything scans.
# The working checkout this harness lives in is never built and never scanned.
export SPARK_SRC="${SPARK_SRC:-/opt/spark-src}"
export SPARK_SRC_COMMIT="${SPARK_SRC_COMMIT:-59b8a4489c878fa3a9aa6b7fbae760f2fc80eb9d}"

# ------------------------------------------------------------------- toolchain
export JAVA_HOME="${JAVA_HOME:-$HARNESS_TOOLS_DIR/jdk/jdk-17.0.20+8}"
export JAVA_HOME_21="${JAVA_HOME_21:-$HARNESS_TOOLS_DIR/jdk/jdk-21.0.12.1+1}"
export JOERN_HOME="${JOERN_HOME:-$HARNESS_TOOLS_DIR/joern-cli}"
export DEPENDENCY_CHECK_HOME="${DEPENDENCY_CHECK_HOME:-$HARNESS_TOOLS_DIR/dependency-check}"
export MAVEN_HOME="${MAVEN_HOME:-$HARNESS_TOOLS_DIR/apache-maven-3.9.11}"
export SCALA_HOME="${SCALA_HOME:-$HARNESS_TOOLS_DIR/scala-2.13.17}"

case ":$PATH:" in
  *":$HARNESS_TOOLS_DIR/bin:"*) : ;;
  *) PATH="$HARNESS_TOOLS_DIR/bin:$PATH" ;;
esac
case ":$PATH:" in
  *":$JAVA_HOME/bin:"*) : ;;
  *) PATH="$JAVA_HOME/bin:$PATH" ;;
esac
export PATH

# ------------------------------------------------- pinned rulesets and feeds
export OPENGREP_RULES_DIR="${OPENGREP_RULES_DIR:-$HARNESS_SHARED_DIR/rules/opengrep-rules}"
export SEMGREP_RULES_DIR="${SEMGREP_RULES_DIR:-$HARNESS_SHARED_DIR/rules/semgrep-rules}"
export DD_SAST_RULES_FILE="${DD_SAST_RULES_FILE:-$HARNESS_SHARED_DIR/rules/datadog/datadog-sast-rules.json}"
export TRIVY_CACHE_DIR="${TRIVY_CACHE_DIR:-$HARNESS_SHARED_DIR/trivy-cache}"
export HARNESS_DC_DATA_DIR="${HARNESS_DC_DATA_DIR:-$HARNESS_SHARED_DIR/dc-data}"

# ------------------------------------------------------------------- behaviour
# UTF-8 is mandatory: with LANG unset, Opengrep aborts reading its own rule
# files with UnicodeDecodeError ('ascii' codec can't decode byte 0xe2).
export LANG="${LANG:-C.utf8}"
export LC_ALL="${LC_ALL:-C.utf8}"
export PYTHONUTF8="${PYTHONUTF8:-1}"

# Joern's default log level floods its own artifact (one run produced a 379 MB
# artifact of per-method INFO lines).
export SL_LOGGING_LEVEL="${SL_LOGGING_LEVEL:-WARN}"

# Heap for every Joern/CPG JVM. This is a minimum, not a ceiling: raising it is
# permitted and must be reported, lowering it produces a truncated graph whose
# silence cannot be told apart from a clean result.
export HARNESS_JOERN_HEAP="${HARNESS_JOERN_HEAP:-64g}"

# HARNESS_SMOKE_TARGET redirects every runner at one small directory. It exists
# for setup-time verification only and MUST be left unset for a real scan.
# (Deliberately not exported here.)

mkdir -p "$HARNESS_RAW_DIR" "$HARNESS_LOG_DIR" "$HARNESS_SCRATCH_DIR" 2>/dev/null || true
