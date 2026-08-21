#!/usr/bin/env python3
"""harness/lib/joern_collect.py — turn the baked Joern query output into the Joern artifact.

Not a runner: harness/bin/run-joern.sh calls this with the captured Joern stdout.

Two jobs, both mechanical:

1. Collect the JSON rows the query script printed between its markers.
2. Map each row's `class_file` (a bytecode class path from the CPG build, e.g.
   /tmp/jimple2cpg-<id>/org/apache/spark/deploy/master/Master.class) to the
   $SPARK_SRC-relative SOURCE path that declares it, by indexing the source trees
   under $SPARK_SRC. Where no source file matches, `path` is null and
   `path_resolution` says why — nothing is invented.

Artifact shape (documented in harness/ENVIRONMENT.md so the scanning run does not guess):

    {
      "tool": "joern",
      "cpg_path": "...",
      "generated_at": "...",
      "cpg_methods": 445567,
      "queries": [{"id": "...", "count": N}, ...],
      "findings": [
        {"rule_id": "...", "message": "...", "path": "core/src/main/scala/...", "start_line": 408,
         "method_full_name": "...", "class_file": "...", "path_resolution": "source-index"}
      ]
    }
"""
from __future__ import annotations

import datetime
import json
import os
import re
import sys

BEGIN = "---HARNESS-JOERN-BEGIN---"
END = "---HARNESS-JOERN-END---"
SOURCE_SUFFIXES = (".scala", ".java")


DECL_RE = re.compile(
    r"^\s*(?:@\w+(?:\([^)]*\))?\s+)*"
    r"(?:(?:private|protected)(?:\[[\w.]+\])?\s+|(?:public|final|abstract|sealed|case|implicit|static|override|open)\s+)*"
    r"(?:class|object|trait|interface|enum|record)\s+([A-Za-z_$][\w$]*)",
    re.MULTILINE,
)


def build_source_index(spark_src: str) -> tuple[dict[str, str], dict[str, str]]:
    """Two indexes over every src/main source tree, both keyed on package path:

    by_filename : <package path>/<FileBaseName> -> repo-relative source path
    by_decl     : <package path>/<DeclaredTypeName> -> repo-relative source path
                  (Scala allows several top-level types per file, so RangePartitioner
                  resolves to Partitioner.scala only through this second index)
    """
    by_filename: dict[str, str] = {}
    by_decl: dict[str, str] = {}
    for dirpath, dirnames, filenames in os.walk(spark_src):
        dirnames[:] = [d for d in dirnames if d not in (".git", "target", "node_modules")]
        parts = dirpath.split(os.sep)
        if "src" not in parts:
            continue
        i = parts.index("src")
        if i + 1 >= len(parts) or parts[i + 1] != "main":
            continue
        for fn in filenames:
            if not fn.endswith(SOURCE_SUFFIXES):
                continue
            full = os.path.join(dirpath, fn)
            rel = os.path.relpath(full, spark_src)
            rp = rel.split(os.sep)
            if "main" not in rp:
                continue
            j = rp.index("main")
            pkg_path = os.sep.join(rp[j + 2:])              # drop main/<lang>
            pkg_dir = os.path.dirname(pkg_path)
            base = os.path.splitext(fn)[0]
            by_filename.setdefault(f"{pkg_dir}/{base}" if pkg_dir else base, rel)
            try:
                with open(full, encoding="utf-8", errors="replace") as fh:
                    text = fh.read()
            except OSError:
                continue
            for name in DECL_RE.findall(text):
                key = f"{pkg_dir}/{name}" if pkg_dir else name
                by_decl.setdefault(key, rel)
    return by_filename, by_decl


JIMPLE_TMP_RE = re.compile(r"^.*?/jimple2cpg-\d+/")


def class_key(class_file: str) -> str | None:
    """Bytecode class path -> package-qualified outer class key.

    /tmp/jimple2cpg-17.../org/apache/spark/deploy/worker/ProcessBuilderLike$$anon$3.class
        -> org/apache/spark/deploy/worker/ProcessBuilderLike
    Any root package is handled (org/apache/hive/... included), and the trailing `$`
    of a Scala companion object as well as any nested/anonymous suffix is stripped.
    """
    if not class_file or not class_file.endswith(".class"):
        return None
    path = JIMPLE_TMP_RE.sub("", class_file).lstrip("/")
    path = path[: -len(".class")]
    directory, _, name = path.rpartition("/")
    name = name.partition("$")[0]
    if not name:
        return None
    return f"{directory}/{name}" if directory else name


def main() -> int:
    if len(sys.argv) != 4:
        print("usage: joern_collect.py <joern-stdout-log> <artifact-out> <spark-src>", file=sys.stderr)
        return 64
    log_path, out_path, spark_src = sys.argv[1:4]
    with open(log_path, encoding="utf-8", errors="replace") as fh:
        text = fh.read()

    rows: list[dict] = []
    if BEGIN in text and END in text:
        block = text.split(BEGIN, 1)[1].split(END, 1)[0]
        for line in block.splitlines():
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                print(f"joern_collect: unparseable row skipped: {exc}", file=sys.stderr)

    def grab(pattern: str, default: str = "") -> str:
        m = re.search(pattern, text, flags=re.MULTILINE)
        return m.group(1).strip() if m else default

    cpg_path = grab(r"^HARNESS_JOERN_CPG=(.*)$")
    methods = grab(r"^HARNESS_JOERN_METHODS=(\d+)$", "0")
    typedecls = grab(r"^HARNESS_JOERN_TYPEDECLS=(\d+)$", "0")
    counts_raw = grab(r"^HARNESS_JOERN_QUERY_COUNTS=(.*)$")
    queries = []
    for part in [p for p in counts_raw.split(",") if "=" in p]:
        qid, _, cnt = part.partition("=")
        queries.append({"id": f"joern.{qid}", "count": int(cnt) if cnt.isdigit() else None})

    by_filename, by_decl = build_source_index(spark_src)
    findings = []
    for r in rows:
        key = class_key(r.get("class_file", ""))
        src = None
        how = "unresolved-bytecode-only"
        if key:
            if key in by_filename:
                src, how = by_filename[key], "source-index-filename"
            elif key in by_decl:
                src, how = by_decl[key], "source-index-declaration"
        findings.append(
            {
                "rule_id": r.get("rule_id"),
                "message": r.get("message"),
                "path": src,
                "start_line": r.get("start_line"),
                "method_full_name": r.get("method_full_name"),
                "class_file": r.get("class_file"),
                "path_resolution": how,
            }
        )

    artifact = {
        "tool": "joern",
        "cpg_path": cpg_path,
        "generated_at": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cpg_methods": int(methods) if methods.isdigit() else None,
        "cpg_typedecls": int(typedecls) if typedecls.isdigit() else None,
        "source_index_size": len(by_filename),
        "declaration_index_size": len(by_decl),
        "queries": queries,
        "findings": findings,
    }
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(artifact, fh, indent=1)
        fh.write("\n")
    resolved = sum(1 for f in findings if f["path"])
    print(
        f"joern_collect: rows={len(findings)} path_resolved={resolved} "
        f"path_unresolved={len(findings) - resolved} source_index={len(by_filename)} "
        f"declaration_index={len(by_decl)} -> {out_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
