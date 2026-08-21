#!/usr/bin/env python3
"""harness/lib/smoke_verify.py — setup-time smoke verification.

Not a runner and not part of the scanning run. It answers exactly the questions the
environment-setup instructions ask of the smoke output:

  * does every runner's artifact parse as valid SARIF 2.1.0 or valid JSON?
  * how many findings does each artifact carry, counted from the artifact's own structure?
  * do two consecutive runs against the same target produce identical finding counts?

Usage: smoke_verify.py <pass1-raw-dir> [<pass2-raw-dir>]
Counting rules per tool mirror the record locators documented in harness/ENVIRONMENT.md.
"""
from __future__ import annotations

import json
import os
import sys

TOOLS = {
    "trivy": "trivy.json",
    "osv-scanner": "osv-scanner.json",
    "dependency-check": "dependency-check.json",
    "gitleaks": "gitleaks.json",
    "checkov": "checkov.json",
    "opengrep": "opengrep.sarif",
    "semgrep": "semgrep.sarif",
    "joern": "joern.json",
    "datadog-static-analyzer": "datadog-static-analyzer.sarif",
}


def count_sarif(doc) -> int:
    return sum(len(run.get("results") or []) for run in doc.get("runs") or [])


def count(tool: str, doc) -> int:
    if tool in ("opengrep", "semgrep", "datadog-static-analyzer"):
        return count_sarif(doc)
    if tool == "trivy":
        n = 0
        for res in doc.get("Results") or []:
            for key in ("Vulnerabilities", "Secrets", "Misconfigurations"):
                n += len(res.get(key) or [])
        return n
    if tool == "osv-scanner":
        n = 0
        for res in doc.get("results") or []:
            for pkg in res.get("packages") or []:
                n += len(pkg.get("vulnerabilities") or [])
        return n
    if tool == "dependency-check":
        return sum(len(dep.get("vulnerabilities") or []) for dep in doc.get("dependencies") or [])
    if tool == "gitleaks":
        return len(doc or [])
    if tool == "checkov":
        reports = doc if isinstance(doc, list) else [doc]
        return sum(len((r.get("results") or {}).get("failed_checks") or []) for r in reports)
    if tool == "joern":
        return len(doc.get("findings") or [])
    raise KeyError(tool)


def inspect(raw_dir: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for tool, name in TOOLS.items():
        path = os.path.join(raw_dir, name)
        entry: dict[str, object] = {"artifact": name}
        if not os.path.exists(path):
            entry.update(present=False, parses=False, findings=None, note="artifact absent")
            out[tool] = entry
            continue
        entry["present"] = True
        entry["bytes"] = os.path.getsize(path)
        try:
            with open(path, encoding="utf-8") as fh:
                doc = json.load(fh)
        except Exception as exc:  # noqa: BLE001 - report whatever the parser said
            entry.update(parses=False, findings=None, note=f"JSON parse failed: {exc}")
            out[tool] = entry
            continue
        entry["parses"] = True
        if name.endswith(".sarif"):
            entry["format"] = f"SARIF {doc.get('version')}"
            entry["sarif_valid"] = doc.get("version") == "2.1.0" and isinstance(doc.get("runs"), list)
        else:
            entry["format"] = "JSON " + type(doc).__name__
            entry["sarif_valid"] = None
        entry["findings"] = count(tool, doc)
        out[tool] = entry
    return out


def main() -> int:
    if len(sys.argv) not in (2, 3):
        print(__doc__)
        return 64
    first = inspect(sys.argv[1])
    second = inspect(sys.argv[2]) if len(sys.argv) == 3 else None

    hdr = f"{'tool':<26}{'present':<9}{'parses':<8}{'format':<16}{'findings':>9}"
    if second:
        hdr += f"{'findings#2':>12}{'identical':>11}"
    print(hdr)
    ok = True
    for tool in TOOLS:
        a = first[tool]
        line = (
            f"{tool:<26}{a['present']!s:<9}{a.get('parses')!s:<8}"
            f"{a.get('format','-')!s:<16}{a.get('findings')!s:>9}"
        )
        if second:
            b = second[tool]
            same = a.get("findings") == b.get("findings")
            line += f"{b.get('findings')!s:>12}{('yes' if same else 'NO'):>11}"
            ok = ok and same
        print(line)
        if not a["present"] or not a.get("parses"):
            ok = False
        if a.get("sarif_valid") is False:
            ok = False
            print(f"    {tool}: SARIF shape invalid")
        if a.get("note"):
            print(f"    {tool}: {a['note']}")
    print("\nSMOKE_VERIFY:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
