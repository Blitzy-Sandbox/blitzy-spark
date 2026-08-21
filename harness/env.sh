#!/usr/bin/env bash
# harness/env.sh — the environment file for the OSS security-scanner harness.
#
# Sourcing this file is how a non-login shell ENTERS the recorded environment.
# It installs nothing, changes nothing, and is idempotent.
#
#   . harness/env.sh
#
# Everything heavy (the nine scanners, both JDKs, Maven, Scala, the pinned Spark
# clone at $SPARK_SRC, the persisted code-property graph, the vulnerability feeds
# and the pinned rulesets) lives in one host-global root that is shared by every
# clone of this repository on this host. This file is the thin, per-clone entry
# point to it: it exports HARNESS_DIR for the clone it sits in, then sources the
# shared root's env.sh.
#
# Authority for versions, feed state, module JAR outcomes and the CPG method
# count is harness/ENVIRONMENT.md. Nothing here supersedes it.

# Absolute path of the harness directory that contains THIS file (per clone).
HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
export HARNESS_DIR

# The repository root that contains harness/ (the anchor for every relative path
# the scanning run writes: oss-scan-results/, queries/joern/).
HARNESS_REPO_ROOT="$(cd "$HARNESS_DIR/.." && pwd)"
export HARNESS_REPO_ROOT

# Per-clone artifact trees. The runners write here; nothing host-global is shared.
export HARNESS_RAW_DIR="${HARNESS_RAW_DIR:-$HARNESS_DIR/artifacts/raw}"
export HARNESS_LOG_DIR="${HARNESS_LOG_DIR:-$HARNESS_DIR/artifacts/logs}"
export HARNESS_SCOPE_FILE="${HARNESS_SCOPE_FILE:-$HARNESS_DIR/scope/allowlist.txt}"

# The shared, host-global part of the environment (tools, feeds, rules, spark-src,
# cpg). Overridable only to relocate the whole shared root.
export BLITZY_HARNESS_ROOT="${BLITZY_HARNESS_ROOT:-/opt/blitzy-harness}"

if [ -r "$BLITZY_HARNESS_ROOT/env.sh" ]; then
  # shellcheck source=/dev/null
  . "$BLITZY_HARNESS_ROOT/env.sh"
else
  echo "harness/env.sh: shared harness root not readable: $BLITZY_HARNESS_ROOT/env.sh" >&2
  echo "harness/env.sh: the recorded environment is absent; see harness/ENVIRONMENT.md" >&2
  # Works whether this file is sourced (return) or executed (exit). shellcheck's
  # reachability analysis cannot model that, so the note is silenced here only.
  # shellcheck disable=SC2317
  return 1 2>/dev/null || exit 1
fi

# Prefer the clone-local CPG path (a symlink to the shared graph) when present,
# so query scripts written against harness/cpg/spark.cpg resolve inside the clone.
if [ -e "$HARNESS_DIR/cpg/spark.cpg" ]; then
  export HARNESS_CPG="$HARNESS_DIR/cpg/spark.cpg"
fi
