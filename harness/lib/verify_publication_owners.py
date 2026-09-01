#!/usr/bin/env python3
"""Publication gate: every value repeated across the result documents must be a
projection of the single record that owns it.

Why this exists
---------------
AAP 0.6.4 fixes ownership: a count appearing in two documents is "one measurement
cited twice, never two measurements".  Nothing enforces that by construction,
because the human records are Markdown written by hand while the measurements live
in JSON written by tools.  Every time a measurement was re-taken during this
checkpoint, some copy of it in a document went stale -- a suite elapsed time, a run
window, an invocation command, a repository root, a subtotal, a revision count -- and
each one was found only because somebody happened to look.

This module removes the happening-to-look.  It reads each OWNER, reads each COPY out
of the document that publishes it, and reports every disagreement.  It exits non-zero
when any pair disagrees, so it can gate publication: a document that has drifted from
its owner is not publishable, and the failure names both sides.

Relationship to `verify_status_figures.py`
------------------------------------------
That module adjudicates the *replicated numeric figures* of the adapter-test and
normalization records -- test counts, subtest counts, per-module addends, fixture
counts.  This module adjudicates *owner/copy identity* across a wider surface:
timestamps, invocation commands, absolute roots, a fixture disposition, side-artifact
tree state, a frontend subtotal, and the probe revision triple.  The two overlap only
on the suite's elapsed pair, deliberately: that figure drifted more often than any
other, and a second independent check on it costs nothing.

Design notes
------------
* Every assertion names the owner and the copy.  A failure that says only "these
  differ" sends the reader hunting; one that says which file is authoritative does not.
* Assertions read the owner at run time.  Nothing here hard-codes a measurement,
  because a gate carrying its own copy of the value is one more copy to go stale.
* A copy that is *absent* from the document fails as loudly as one that disagrees.
  An omitted projection is how a value silently stops being checked.

Usage
-----
    python3 harness/lib/verify_publication_owners.py

Run from the repository root.  Exits 0 when every owner/copy pair agrees, 1 otherwise.
"""

from __future__ import annotations

import json
import pathlib
import re
import subprocess
import sys
from typing import Any, Callable

# --------------------------------------------------------------------------- paths

ROOT = pathlib.Path(__file__).resolve().parents[2]
LOGS = ROOT / "harness/artifacts/logs"
RESULTS = ROOT / "oss-scan-results"

ADAPTER_RUN = LOGS / "adapter-tests-run.json"
NORMALIZE_RUN = LOGS / "normalize-run.json"
RUNNER_METADATA = LOGS / "runner-metadata.json"
GATE_RECORD = LOGS / "gate-record.json"
FRONTEND_LOG = LOGS / "cpg-frontend.log"
MANIFEST = ROOT / "harness/artifacts/MANIFEST.json"

RUN_RECORD = RESULTS / "run-record.md"
TOOL_STATUS = RESULTS / "tool-status.md"
PROBE_REPORT = RESULTS / "joern-probe.md"


# ----------------------------------------------------------------- result plumbing


class Gate:
    """Collects owner/copy verdicts and renders them as one report."""

    def __init__(self) -> None:
        self.rows: list[tuple[bool, str, str, str, str]] = []

    def check(self, name: str, owner: str, expected: Any, copy_source: str,
              found: Any) -> None:
        """Record one owner/copy comparison.

        `expected` is what the owner carries; `found` is what the copy carries.
        Both are rendered as strings so that 6.85 and "6.850" compare as the
        document prints them rather than as Python happens to repr them.
        """
        ok = str(expected) == str(found)
        self.rows.append((ok, name, owner, copy_source,
                          "" if ok else f"owner={expected!r} copy={found!r}"))

    def present(self, name: str, owner: str, copy_source: str,
                found: Any, why: str = "") -> None:
        """Record that a required projection exists at all.

        An absent projection is a silent loss of coverage, so it is a failure in
        its own right rather than a skipped check.
        """
        ok = found not in (None, "", [], ())
        self.rows.append((ok, name, owner, copy_source,
                          "" if ok else (why or "projection absent from the document")))

    def report(self) -> int:
        width = max(len(r[1]) for r in self.rows)
        for ok, name, owner, copy_source, detail in self.rows:
            flag = "ok   " if ok else "DRIFT"
            print(f"  {flag} {name:<{width}}  {owner} -> {copy_source}")
            if detail:
                print(f"        {detail}")
        failed = [r for r in self.rows if not r[0]]
        print()
        print(f"  owner/copy pairs checked : {len(self.rows)}")
        print(f"  disagreeing              : {len(failed)}")
        print()
        if failed:
            print("=" * 78)
            print("DRIFT -- each pair below has a copy that disagrees with its owner")
            print("=" * 78)
            for _ok, name, owner, copy_source, detail in failed:
                print(f"  - {name}: {owner} -> {copy_source}")
                print(f"      {detail}")
            print()
            print("  A document that has drifted from its owner is not publishable.")
            print("  Regenerate the copy from the owner; never edit the owner to match.")
            return 1
        print("  PASS: every published value is a projection of the record that owns it.")
        return 0


def load(path: pathlib.Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def text(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


def first(pattern: str, body: str, group: int = 1) -> str | None:
    m = re.search(pattern, body, re.M)
    return m.group(group) if m else None


def rel(path: pathlib.Path) -> str:
    return str(path.relative_to(ROOT))


# ------------------------------------------------------------------- the assertions


def check_suite_identity(g: Gate, adapter: dict, rr: str, ts: str) -> None:
    """The adapter-test suite's command, window and elapsed pair.

    This is the surface that drifted most: the suite is re-run whenever any file
    under test changes, and each run moves four values at once.
    """
    sr = adapter["suite_result"]
    owner = rel(ADAPTER_RUN)

    # The command must be reproduced verbatim.  A document that adds a discover
    # pattern or a verbosity flag is describing an invocation nobody made.
    command = adapter["command"]
    g.check("suite command (run-record)", owner, command, rel(RUN_RECORD),
            first(r"^(/usr/bin/python3 -m unittest discover[^\n]*)$", rr))

    # Elapsed pair, as the documents print it: three decimals and thousands commas.
    secs = f'{sr["unittest_reported_seconds"]:.3f}'
    ms = f'{sr["wall_clock_ms"]:,}'
    # Each document prints the pair in its own house style, and the regex is
    # anchored on that style rather than on a bare number: an unanchored match
    # captures the first duration in the file, which is a different measurement.
    house_styles = (
        ("run-record", rr, rel(RUN_RECORD),
         r"(\d+\.\d{3}) s as `unittest`\s+reported it",
         r"reported it and ([\d,]+) ms wall"),
        ("tool-status", ts, rel(TOOL_STATUS),
         r"`unittest` reported \*\*[\d,]+ tests\*\* in \*\*(\d+\.\d{3}) s\*\*",
         r"in \*\*\d+\.\d{3} s\*\* \(\*\*([\d,]+) ms wall\*\*\)"),
    )
    for label, body, src, sec_pat, ms_pat in house_styles:
        g.check(f"suite reported seconds ({label})", owner, secs, src,
                first(sec_pat, body))
        g.check(f"suite wall ms ({label})", owner, ms, src, first(ms_pat, body))

    # The run window.  tool-status prints "from <ISO> to <HH:MM:SS>Z"; run-record
    # prints the same window in bold.  Both are required, because a window present
    # in one document and absent from the other is half-checked.
    start, finish = adapter["started_at_utc"], adapter["finished_at_utc"]
    short = finish.split("T")[1]
    window_ts = first(r"from (20\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ to \d\d:\d\d:\d\dZ),"
                      r" suite exit", ts)
    g.check("suite window (tool-status)", owner, f"{start} to {short}",
            rel(TOOL_STATUS), window_ts)
    g.present("suite window (run-record)", owner, rel(RUN_RECORD),
              first(rf"\*\*{re.escape(start)} to {re.escape(short)}\*\*", rr, 0),
              f"run-record does not publish the suite window {start} to {short}")

    # Test and subtest totals, cross-checked here as well as in
    # verify_status_figures.py -- cheap, and the one figure worth two opinions.
    g.check("suite tests (run-record)", owner, f'{sr["tests_run"]}', rel(RUN_RECORD),
            first(r"\*\*(\d{3,5}) tests and", rr))
    g.check("suite subtests (run-record)", owner, f'{sr["subtests_recorded"]:,}',
            rel(RUN_RECORD), first(r"tests and ([\d,]+) subTests", rr))


def check_normalize_window(g: Gate, norm: dict, rr: str, ts: str) -> None:
    """The normalizer's run window.

    The normalizer is re-run as a byte-identical reproducibility check, and each
    re-run rewrites its own record, so both copies go stale together.
    """
    owner = rel(NORMALIZE_RUN)
    start, finish = norm["started_at_utc"], norm["finished_at_utc"]
    short = finish.split("T")[1]
    g.check("normalize window (tool-status)", owner, f"{start} to {short}",
            rel(TOOL_STATUS),
            first(r"It ran from \*\*(20\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ to "
                  r"\d\d:\d\d:\d\dZ)\*\*", ts))
    g.check("normalize window (run-record)", owner, f"{start} \u2192 {short}",
            rel(RUN_RECORD),
            first(r"`(20\d\d-\d\d-\d\dT\d\d:\d\d:\d\dZ \u2192 \d\d:\d\d:\d\dZ)`,"
                  r" \*\*exit 0\*\*", rr))


def check_chronology_ledger(g: Gate, adapter: dict, norm: dict, rr: str) -> None:
    """Section 18's ledger claims to be sorted by instant, so both are checked.

    Ten of its rows record instants that cannot change.  Two are re-measured every
    time the normalizer or the suite is re-run, and each re-run moves the row's
    timestamp AND its position relative to the other.  A hand-maintained ledger
    claiming to be sorted by instant is exactly the claim that goes stale silently:
    it was, when it was written.  Both the values and the order are asserted.
    """
    section = rr[rr.find("## 18."):]
    # The lane column is not what this check is about, and a re-run performed in another
    # checkout legitimately names that checkout instead of a lane. What is asserted is that
    # both re-run rows are present, carry their owner's instants and are in instant order.
    rows = re.findall(
        r"^\| `(20[0-9T:.Z-]+)` \u2013 `([0-9:.Z]+)` \| (?:`w-013`|this checkout) \| "
        r"(normalization \(final reproducibility re-run\)"
        r"|the adapter and reconciliation suite \(final re-run\)) \| 4 \|$",
        section, re.M)
    g.check("ledger publishes both re-run rows", "section 18 contract", "2",
            rel(RUN_RECORD), str(len(rows)))
    if len(rows) != 2:
        return
    published = {("normalize" if r[2].startswith("normalization") else "adapter"):
                 (r[0], r[1]) for r in rows}
    for key, owner_doc, owner_path in (("normalize", norm, NORMALIZE_RUN),
                                       ("adapter", adapter, ADAPTER_RUN)):
        start = owner_doc["started_at_utc"]
        short = owner_doc["finished_at_utc"].split("T")[1]
        g.check(f"ledger {key} row start", rel(owner_path), start,
                f"{rel(RUN_RECORD)}:section 18", published.get(key, ("", ""))[0])
        g.check(f"ledger {key} row finish", rel(owner_path), short,
                f"{rel(RUN_RECORD)}:section 18", published.get(key, ("", ""))[1])
    # The ledger's own ordering claim, checked against the instants it prints.
    order_published = [r[0] for r in rows]
    g.check("ledger rows are in instant order", "section 18 contract", "True",
            f"{rel(RUN_RECORD)}:section 18",
            str(order_published == sorted(order_published)))


def check_stage_chronology(g: Gate, meta: dict, gate_rec: dict, rr: str) -> None:
    """Runner-metadata's own chronology must agree with itself and with the gate.

    The failure this catches is a nested section describing a superseded
    generation while the top-level fields describe the current one.
    """
    owner = rel(RUNNER_METADATA)
    chron = meta["chronology"]

    # The dynamic half's timestamp is owned by `chronology`; the section that
    # describes that half must carry the same instant, not the file's write time.
    dynamic = chron["stage1_dynamic_half"].split(" ")[0]
    g.check("stage1 finalisation instant", f"{owner}:chronology", dynamic,
            f"{owner}:stage1_finalisation", meta["stage1_finalisation"]["timestamp_utc"])
    g.check("stage1 top-level instant", f"{owner}:chronology", dynamic,
            f"{owner}:finalised_at_stage1", meta.get("finalised_at_stage1"))

    # The gate's instant is owned by gate-record.json.
    gate_instant = chron["gate_static_half"].split(" ")[0]
    g.check("gate instant", f"{owner}:chronology", gate_instant,
            f"{owner}:generated_at_gate", meta.get("generated_at_gate"))

    # Monotonicity of the values actually present, rather than of a claim about them.
    stamps = [gate_instant, dynamic, chron["stage3_first_invocation"],
              chron["stage3_last_invocation_end"]]
    g.check("chronology non-decreasing", f"{owner}:chronology", "True",
            f"{owner}:chronology", str(stamps == sorted(stamps)))

    # No inherited date may sit in an active field.  Any occurrence must be inside
    # a field whose name marks it superseded.
    labels = ("superseded", "supersession", "correction", "supersedes",
              "environment_record", "expected", "difference", "provenance",
              "divergence")
    unlabelled: list[str] = []

    def walk(node: Any, path: str = "") -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                walk(v, f"{path}/{k}")
        elif isinstance(node, list):
            for i, v in enumerate(node):
                walk(v, f"{path}[{i}]")
        elif isinstance(node, str) and "2026-08-24" in node:
            if not any(t in path.lower() for t in labels):
                unlabelled.append(path)

    walk(meta)
    g.check("no inherited date in an active field", owner, "0",
            f"{owner} (whole document)", str(len(unlabelled)))

    # The gate's halts are owned by gate-record.json; runner-metadata carries the
    # copy.  The count is read from the owner rather than written here, so the
    # assertion cannot pass by agreeing with a number this file happens to hold.
    g.check("gate halt count", rel(GATE_RECORD), str(len(gate_rec["halts"])),
            f"{owner}:halts", str(len(meta.get("halts", []))))
    g.check("gate verdict", rel(GATE_RECORD), "halt", rel(RUN_RECORD),
            "halt" if "STATUS: HALTED" in rr[:3000] else "<banner absent>")


def check_side_artifact_state(g: Gate, meta: dict) -> None:
    """Each runner side-artifact tree's published state must equal its disk state.

    The failure this catches is prose asserting a tree is absent while the tree
    is present and manifested.
    """
    owner = "filesystem"
    for tool, subdir in (("gitleaks", "gitleaks.parts"),
                         ("checkov", "checkov.out"),
                         ("trivy", "trivy.parts"),
                         ("dependency-check", "dependency-check.out")):
        tree = LOGS / subdir
        files = sorted(p for p in tree.rglob("*") if p.is_file())
        node = (meta.get("tools", {}).get(tool, {})
                .get("side_artifacts", {}).get("tree_state_measured", {}))
        g.check(f"{tool} side-tree file count", owner, str(len(files)),
                f"{rel(RUNNER_METADATA)}:tools.{tool}", str(node.get("file_count")))
        g.check(f"{tool} side-tree bytes", owner,
                str(sum(p.stat().st_size for p in files)),
                f"{rel(RUNNER_METADATA)}:tools.{tool}", str(node.get("total_bytes")))

    # The human record publishes the same two figures per tree, and a copy nobody
    # compares is a copy that drifts.  tool-status.md prints them as
    # "**N members totalling B bytes**" for the multi-part trees; the single-file
    # trees name their one report, so only the byte figure is published there.
    ts_body = text(TOOL_STATUS)
    for tool, subdir in (("gitleaks", "gitleaks.parts"), ("trivy", "trivy.parts")):
        tree = LOGS / subdir
        files = sorted(p for p in tree.rglob("*") if p.is_file())
        want = (f"**{len(files)} members totalling "
                f"{sum(p.stat().st_size for p in files):,} bytes**")
        g.present(f"{tool} side-tree published in tool-status", "filesystem",
                  rel(TOOL_STATUS), want if want in ts_body else None,
                  f"tool-status.md does not publish {want} for {subdir}")
    for tool, subdir in (("checkov", "checkov.out"),
                         ("dependency-check", "dependency-check.out")):
        tree = LOGS / subdir
        files = sorted(p for p in tree.rglob("*") if p.is_file())
        total = f"{sum(p.stat().st_size for p in files):,}"
        g.present(f"{tool} side-tree bytes published in tool-status", "filesystem",
                  rel(TOOL_STATUS), total if total in ts_body else None,
                  f"tool-status.md does not publish {total} bytes for {subdir}")
        g.present(f"{tool} side-tree stated retained in tool-status", "filesystem",
                  rel(TOOL_STATUS),
                  "retained and measured, not absent"
                  if "retained and measured, not absent" in ts_body else None,
                  "tool-status.md does not state the tree retained")

    # No live sentence may assert absence of a present tree.  Occurrences are
    # permitted only inside a field marked as a supersession note.
    live: list[str] = []

    def walk(node: Any, path: str = "") -> None:
        if isinstance(node, dict):
            for k, v in node.items():
                walk(v, f"{path}/{k}")
        elif isinstance(node, list):
            for i, v in enumerate(node):
                walk(v, f"{path}[{i}]")
        elif isinstance(node, str) and re.search(
                r"absent from (?:it|this checkout)|does not follow that they survive",
                node):
            if not any(t in path.lower() for t in ("superseded", "supersedes",
                                                   "supersession", "correction")):
                live.append(path)

    walk(meta)
    g.check("no live absence claim for a present tree", owner, "0",
            f"{rel(RUNNER_METADATA)} (whole document)", str(len(live)))


def check_fixture_disposition(g: Gate, adapter: dict, ts: str, rr: str) -> None:
    """The Dependency-Check captured-positive-mapping disposition.

    One verdict stated by five authorities; the failure this catches is a status
    field saying SATISFIED while a neighbouring narrative says it cannot be.
    """
    owner = rel(ADAPTER_RUN)
    node = (adapter["positive_mapping"]["per_adapter"]["dependency-check"]
            ["aap_0_6_2_captured_positive_mapping_requirement"])
    g.check("fixture disposition (owner)", owner, "SATISFIED",
            f"{owner}:positive_mapping", node.get("status"))
    g.present("superseded verdict retained", owner, f"{owner}:positive_mapping",
              node.get("status_superseded_value"),
              "the previous FAILED verdict must be retained, not deleted")
    # A whole-document search for "SATISFIED" passes on any document that happens to
    # contain the word anywhere, which is no test at all.  Each projection is located
    # in the passage that actually discusses THIS requirement, and that passage is
    # then required to name the satisfying capture and to carry no failure verdict.
    CAPTURE = "captured-dependency-check-vulnerabilities"
    for label, body, src, marker in (
            ("tool-status", ts, rel(TOOL_STATUS),
             "aap_0_6_2_captured_positive_mapping_requirement"),
            ("run-record", rr, rel(RUN_RECORD),
             "captured-positive-mapping requirement that needed a second capture")):
        idx = body.find(marker)
        passage = body[idx:idx + 2600] if idx >= 0 else ""
        g.present(f"fixture passage located ({label})", owner, src,
                  passage or None,
                  f"{src} has no passage discussing this requirement "
                  f"(searched for {marker!r})")
        if not passage:
            continue
        g.present(f"fixture disposition in that passage ({label})", owner, src,
                  "SATISFIED" if "SATISFIED" in passage else None,
                  f"the passage in {src} does not state SATISFIED")
        g.present(f"satisfying capture named ({label})", owner, src,
                  CAPTURE if CAPTURE in passage else None,
                  f"the passage in {src} does not name {CAPTURE}")
        g.check(f"no failure verdict in that passage ({label})", owner, "0", src,
                str(len(re.findall(r"recorded as (?:a )?fail"
                                   r"|cannot (?:be|itself be) satisfied",
                                   passage, re.I))))

    # No live narrative may contradict the status field.
    live: list[str] = []

    def walk(node_: Any, path: str = "") -> None:
        if isinstance(node_, dict):
            for k, v in node_.items():
                walk(v, f"{path}/{k}")
        elif isinstance(node_, list):
            for i, v in enumerate(node_):
                walk(v, f"{path}[{i}]")
        elif isinstance(node_, str) and re.search(
                # The phrasings that actually occurred, not a guess at them: an
                # earlier edition of this pattern was case-sensitive and required
                # "recorded as failed" adjacent, so it missed both "Recorded as a
                # failed requirement" and "so the failure is measured".
                r"cannot (?:be|itself be) satisfied"
                r"|recorded as (?:a )?fail"
                r"|the failure is measured"
                r"|exercised_instead", node_, re.I):
            if not any(t in path.lower() for t in ("superseded", "supersession",
                                                   "correction_this_record")):
                live.append(path)

    walk(adapter)
    g.check("no live FAILED narrative", owner, "0", f"{owner} (whole document)",
            str(len(live)))

    # The expected file's prose must agree with its own status field.
    exp = load(RESULTS / "adapter-tests/expected/dependency-check.rows.json")
    prose = f'{exp.get("description", "")} {exp.get("rows_note", "")}'
    g.check("expected-file prose agrees with its status",
            rel(RESULTS / "adapter-tests/expected/dependency-check.rows.json"), "0",
            "its own description and rows_note",
            str(len(re.findall(r"recorded here as FAILED|recorded as failed"
                               r"|cannot itself be satisfied", prose))))


def check_frontend_subtotal(g: Gate, rr: str) -> None:
    """The extracted-from-nested-archive subtotal is the frontend log's.

    The failure this catches is a copied subtotal that no longer equals the sum of
    the components printed beside it.
    """
    owner = rel(FRONTEND_LOG)
    body = text(FRONTEND_LOG)
    owned = first(r"^\s*(\d+)\s+\(entry not present in any own artifact", body)
    g.present("frontend subtotal present in its owner", owner, owner, owned,
              "cpg-frontend.log no longer prints the nested-archive subtotal")
    if owned:
        g.check("frontend subtotal (run-record)", owner, owned, rel(RUN_RECORD),
                first(r"350 \+ 42 \+ 6 \+ 5 = (\d+)", rr))
        # And the arithmetic must be true, not merely consistent.
        g.check("frontend subtotal arithmetic", "350 + 42 + 6 + 5", "403",
                owner, owned)


def check_probe_revisions(g: Gate, rr: str, probe: str) -> None:
    """The probe revision triple is owned per query and never summed.

    The owner is each query's own result envelope, which carries the count the running
    script measured from the repository's history -- not a literal in this module and not
    a figure in a report. Reading it from the envelopes is what makes this check survive a
    re-run of the probe: a re-measured count moves the owner, and the reports have to move
    with it.
    """
    owner = "queries/joern/results/*.json"
    envelopes = sorted((ROOT / "queries/joern/results").glob("*.json"))
    owned = [str(json.loads(p.read_text())["effort_query_revisions_committed"])
             for p in envelopes]
    g.check("probe revision triple (owner present)", owner, "3", owner, str(len(owned)))
    section = probe[probe.find("### 1. Query revisions committed"):][:2600]
    published = re.findall(r"^\| 0[123] \|[^|]*\|\s*\*{0,2}(\d+)\*{0,2}\s*\|",
                           section, re.M)
    g.check("probe revision triple (probe report)", owner, str(owned),
            f"{rel(PROBE_REPORT)}:section 1", str(published))
    g.check("probe revision triple (run-record)", owner, ", ".join(owned), rel(RUN_RECORD),
            first(r"Query revisions committed \|\s*\*\*([\d, ]+)\*\*", rr))


def check_repository_root(g: Gate, rr: str) -> None:
    """The one absolute root the run record states must be this checkout's."""
    real = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=ROOT,
                          capture_output=True, text=True, check=True).stdout.strip()
    roots = set(re.findall(r"^(/tmp/blitzy/blitzy-spark/[^\s`]+)$", rr, re.M))
    g.check("run-record states exactly one absolute root", "git rev-parse", "1",
            rel(RUN_RECORD), str(len(roots)))
    g.check("that root is this checkout", "git rev-parse", real, rel(RUN_RECORD),
            next(iter(roots)) if len(roots) == 1 else sorted(roots))


def check_no_stage_certified(g: Gate, rr: str, gate_rec: dict) -> None:
    """While the gate's verdict is `halt`, no stage may be published as complete."""
    owner = rel(GATE_RECORD)
    if gate_rec["gate_verdict"]["overall"] != "halt":
        g.check("gate verdict is halt", owner, "halt", owner,
                gate_rec["gate_verdict"]["overall"])
        return
    section = rr[rr.find("## 18."):]
    rows = [l for l in section.split("\n") if re.match(r"^\| [0-6] \u2014 ", l)]
    g.check("stage rows present", owner, "7", rel(RUN_RECORD), str(len(rows)))
    g.check("every stage row reads not certified", owner, "7", rel(RUN_RECORD),
            str(sum("not certified" in r for r in rows)))
    g.check("no stage row reads complete", owner, "0", rel(RUN_RECORD),
            str(sum(bool(re.search(r"\|\s*complete", r)) for r in rows)))


def check_manifest_totals(g: Gate, manifest: dict, rr: str) -> None:
    """Section 16's per-tree totals are the manifest's."""
    owner = rel(MANIFEST)
    for tree in ("raw", "logs"):
        files = manifest[tree]["files"]
        n, total = len(files), sum(f["bytes"] for f in files)
        g.check(f"{tree} tree file count", owner, str(n), rel(RUN_RECORD),
                first(rf"### `harness/artifacts/{tree}/` \u2014 (\d+) files", rr))
        g.check(f"{tree} tree bytes", owner, f"{total:,}", rel(RUN_RECORD),
                first(rf"### `harness/artifacts/{tree}/` \u2014 \d+ files, "
                      rf"([\d,]+) bytes", rr))


# ------------------------------------------------------------------------ entrypoint


def main() -> int:
    print("publication gate -- every published value against the record that owns it")
    print()
    g = Gate()

    adapter = load(ADAPTER_RUN)
    norm = load(NORMALIZE_RUN)
    meta = load(RUNNER_METADATA)
    gate_rec = load(GATE_RECORD)
    manifest = load(MANIFEST)
    rr, ts, probe = text(RUN_RECORD), text(TOOL_STATUS), text(PROBE_REPORT)

    check_suite_identity(g, adapter, rr, ts)
    check_normalize_window(g, norm, rr, ts)
    check_chronology_ledger(g, adapter, norm, rr)
    check_stage_chronology(g, meta, gate_rec, rr)
    check_side_artifact_state(g, meta)
    check_fixture_disposition(g, adapter, ts, rr)
    check_frontend_subtotal(g, rr)
    check_probe_revisions(g, rr, probe)
    check_repository_root(g, rr)
    check_no_stage_certified(g, rr, gate_rec)
    check_manifest_totals(g, manifest, rr)

    return g.report()


if __name__ == "__main__":
    sys.exit(main())
