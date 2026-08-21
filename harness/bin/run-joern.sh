#!/usr/bin/env bash
# harness/bin/run-joern.sh — Joern runner. Takes NO arguments; configuration baked in.
#
#   input       : the PERSISTED code-property graph at $HARNESS_CPG (harness/cpg/spark.cpg,
#                 a symlink to the shared graph). The graph is LOADED with importCpg.
#                 importCode is never used anywhere in this harness: no graph is ever built here.
#   scan target : the graph, which was built from the jars of the in-scope modules of the pinned
#                 tree (see harness/ENVIRONMENT.md for the per-module JAR list). Test sources are
#                 excluded because only src/main bytecode was compiled into those jars.
#   query set   : harness/lib/joern-baked-queries.sc — five queries: process-launch sites,
#                 Java deserialization sites, reflective class loading, weak hash algorithms, and
#                 deploy-package RPC handlers that reach a process launch over the call graph.
#   runtime     : JDK 21 (Joern 4.x requires it) with a private Joern workspace per invocation,
#                 so concurrent clones never share workspace state.
#   artifact    : $HARNESS_RAW_DIR/joern.json  — a JSON object; `findings` is the row array, and
#                 each row's `path` is $SPARK_SRC-relative (mapped from the graph's bytecode class
#                 path by harness/lib/joern_collect.py) or null with path_resolution explaining why.
#   exit code   : Joern's own exit code from the query run.
set -uo pipefail
[ $# -eq 0 ] || { echo "run-joern.sh: takes no arguments" >&2; exit 64; }

HARNESS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
. "$HARNESS_DIR/env.sh"
# shellcheck source=/dev/null
. "$HARNESS_DIR/lib/scope.sh"

harness_begin joern
ARTIFACT="$HARNESS_RAW_DIR/joern.json"
QUERIES="$HARNESS_DIR/lib/joern-baked-queries.sc"
COLLECT="$HARNESS_DIR/lib/joern_collect.py"

[ -e "$HARNESS_CPG" ] || harness_die "persisted CPG missing: $HARNESS_CPG"
[ -r "$QUERIES" ]     || harness_die "baked query set missing: $QUERIES"

export JAVA_HOME="$JAVA_HOME_21"
export PATH="$JAVA_HOME/bin:$PATH"
export JAVA_OPTS="${JAVA_OPTS:--Xmx48g}"

WORKSPACE="$(mktemp -d "${TMPDIR:-/tmp}/harness-joern-ws.XXXXXX")"
JOERN_OUT="$HARNESS_LOG_DIR/joern.query-output.log"

echo "harness: cpg=$HARNESS_CPG ($(stat -Lc %s "$HARNESS_CPG" 2>/dev/null) bytes)"
echo "harness: query set=$QUERIES"
echo "harness: joern workspace=$WORKSPACE (private to this invocation)"
echo "harness: invocation: joern --script lib/joern-baked-queries.sc --param cpgPath=$HARNESS_CPG"

( cd "$WORKSPACE" && "$JOERN_HOME/joern" --script "$QUERIES" --param "cpgPath=$HARNESS_CPG" < /dev/null ) \
  > "$JOERN_OUT" 2>&1
rc=$?

echo "harness: joern query output -> $JOERN_OUT ($(wc -l < "$JOERN_OUT") lines)"
grep -E '^HARNESS_JOERN_(METHODS|TYPEDECLS|QUERY_COUNTS)=' "$JOERN_OUT" | sed 's/^/harness: /'
python3 "$COLLECT" "$JOERN_OUT" "$ARTIFACT" "$SPARK_SRC" | sed 's/^/harness: /'

harness_finish joern "$rc" "$ARTIFACT"
exit $rc
