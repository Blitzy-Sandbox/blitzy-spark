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

The three citation families, and the failure that put them here
--------------------------------------------------------------
Neither module originally checked the *shape* of a citation, only the value it
carried, and a whole class of defect lives in that gap.  One commit replaced the nine
``<tool>.status`` files with the runners' own seven-line trailers and replaced
``cpg-verify.log``, both correct changes -- and left sixteen prose citations pointing
at fields and line numbers that had ceased to exist, plus four passages asserting that
files the same commit had *restored* were absent.  Every one of those citations named a
file that exists, so no owner/copy value disagreed and both gates passed while a reader
following the document was sent nowhere.  AAP 0.9.4 requires every number to name a
file that exists; what it means in practice is that the *locator* must resolve too.

So three further families are adjudicated here, each over every result document:

* **``.status`` field citations.**  A ``<tool>.status`` file is the runner's verbatim
  ``scope_finish`` trailer and carries exactly seven ``key=value`` lines.  The owner is
  the file itself, read at run time, so the check survives a runner that adds a field.
  A citation naming anything else fails.
* **Line-number citations.**  ``file line N`` and ``file:N-M``, resolved against the
  cited file's actual length.  Restricted to this run's own surface -- the evidence
  trees, the harness, the queries and the result documents -- because a citation into
  the pinned Spark tree or the private build clone is expressed against another root
  and cannot be adjudicated from here.
* **Absence claims.**  A path published as absent must be absent, and the converse:
  the failure observed was a table listing six present files as unresolvable, which is
  worse than a broken citation because it is load-bearing for a conclusion.

Each family distinguishes a **live** citation from one **labelled as history**.  A
document that says "an earlier generation quoted an enriched ``joern.status`` at lines
274-275; that field no longer exists" is doing exactly what AAP 0.9.4 asks -- naming a
superseded locator so a reader is not sent to it -- and must not fail for quoting the
thing it is retracting.  The markers are a fixed vocabulary, listed in
``HISTORY_MARKERS`` and scoped to the containing table row or paragraph, so a
document cannot excuse a live citation by mentioning the word "superseded" elsewhere.
History-marked citations are counted and reported rather than hidden, because a gate
that can be silenced by a marker needs its silences visible.

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
BUILD_RECORD = RESULTS / "build-record.md"
SEVERITY_MAP = RESULTS / "severity-map.md"

# Every result document the citation families are adjudicated over.  Named as a tuple
# rather than globbed so that adding a result document is a deliberate act: a new file
# that nobody added here would go unchecked, and silently.
RESULT_DOCUMENTS = (RUN_RECORD, TOOL_STATUS, PROBE_REPORT, BUILD_RECORD, SEVERITY_MAP)

# Phrases that mark a citation as one the document is RETRACTING rather than relying
# on.  Deliberately narrow, and matched only inside the citation's own table row or
# paragraph: a document must not be able to excuse a live citation by using the word
# "superseded" three sections away.
HISTORY_MARKERS = (
    "supersed",           # superseded, supersedes, supersession
    "earlier generation",
    "an earlier revision",
    "earlier edition",
    "previous edition",
    "as history",
    "no longer",
    "does not carry",
    "holds no",
    "ceased to exist",
    "was replaced",
    "were replaced",
    "replaced all nine",
    "not a source",
    "which no longer exists",
    "previously listed",
    "previously published",
    "have been removed from the table",
)

# The subtrees a line-number citation can be adjudicated against.  A citation into the
# pinned Spark tree (`pom.xml:29`), into the private by-SHA build clone, or into a
# staging tree that no longer exists is expressed against another root and is out of
# this gate's reach -- saying so is honest, and pretending otherwise would make the
# gate fail on correct citations.
ADJUDICABLE_PREFIXES = (
    "harness/artifacts/logs/",
    "harness/artifacts/raw/",
    "harness/artifacts/MANIFEST.json",
    "harness/lib/",
    "harness/bin/",
    "harness/scope/",
    "harness/ENVIRONMENT.md",
    "harness/env.sh",
    "queries/joern/",
    "oss-scan-results/",
)


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

    def clear(self, name: str, owner: str, copy_source: str,
              offenders: list[str]) -> None:
        """Record that a whole class of citation resolves, naming every one that does not.

        The owner/copy form above compares two values.  A citation family has no single
        value to compare -- what it has is a population, and the assertion is that the
        population of unresolvable members is empty.  Every offender is named in the
        detail, because a count alone sends the reader hunting for which one broke.
        """
        ok = not offenders
        detail = "" if ok else (
            f"{len(offenders)} unresolvable: " + "; ".join(offenders[:12])
            + ("" if len(offenders) <= 12 else f"; (+{len(offenders) - 12} more)")
        )
        self.rows.append((ok, name, owner, copy_source, detail))

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


# --------------------------------------------------- the three citation families

def units(body: str) -> list[tuple[int, str]]:
    """Split a document into the scopes a history marker is allowed to cover.

    A table row is one scope and a blank-line-separated paragraph is another, which is
    the granularity these documents actually reason at: a divergence register row
    retracts one citation, and the row beside it must not inherit the retraction.
    Returns (start offset, text) so a citation's offset can be mapped back to its scope.
    """
    out: list[tuple[int, str]] = []
    offset = 0
    buf: list[str] = []
    buf_start = 0
    for line in body.splitlines(keepends=True):
        stripped = line.lstrip()
        if stripped.startswith("|"):
            if buf:
                out.append((buf_start, "".join(buf)))
                buf = []
            out.append((offset, line))
        elif not stripped:
            if buf:
                out.append((buf_start, "".join(buf)))
                buf = []
        else:
            if not buf:
                buf_start = offset
            buf.append(line)
        offset += len(line)
    if buf:
        out.append((buf_start, "".join(buf)))
    return out


def scope_bounds(offset: int, scopes: list[tuple[int, str]]) -> tuple[int, str]:
    """The containing scope's start offset and text, so a local offset is derivable."""
    found = (0, "")
    for start, body in scopes:
        if start <= offset < start + len(body):
            found = (start, body)
        elif start > offset:
            break
    return found


def scope_of(offset: int, scopes: list[tuple[int, str]]) -> str:
    """The table row or paragraph containing `offset`."""
    found = ""
    for start, body in scopes:
        if start <= offset < start + len(body):
            found = body
        elif start > offset:
            break
    return found


def history_marked(scope: str) -> bool:
    low = scope.lower()
    return any(marker in low for marker in HISTORY_MARKERS)


# Clause boundaries: a sentence end, a semicolon, or a spaced em/en dash.  These
# documents retract a citation in the same breath as making it ("an earlier generation
# quoted `joern.status` at lines 274-275; commit 0e3e742a5ad replaced all nine"), so a
# clause is the unit over which a retraction speaks.
_CLAUSE_SPLIT = re.compile(r"(?<=[.;:])\s+|\s+[\u2013\u2014]\s+")


def history_marked_locally(scope: str, at: int) -> bool:
    """Is the citation at offset `at` retracted by a marker in its OWN clause?

    Scope-wide marking is too coarse for a citation, and the coarseness is not
    hypothetical: the probe report writes

        `cpg-verify.log` records the **current** pair ... at its lines 33-34 and again
        at 47-50 ... and mentions the earlier one only at its lines 76-80

    in one paragraph.  "the earlier one" is a history marker, so a paragraph-scoped
    exemption excuses EVERY citation in that paragraph -- including the two live ones,
    and including a mutation of them to lines that do not exist.  That was a
    demonstrated false negative: mutating 33-34/47-50 to 3333-3434/4747-5050 left the
    gate reporting clear.  A retraction therefore has to sit in the citation's own
    clause to excuse it, which is where these documents actually put it.
    """
    cuts = [0]
    for m in _CLAUSE_SPLIT.finditer(scope):
        cuts.append(m.end())
    cuts.append(len(scope))
    for lo, hi in zip(cuts, cuts[1:]):
        if lo <= at < hi:
            return history_marked(scope[lo:hi])
    return history_marked(scope)


def status_trailer_fields() -> tuple[set[str], dict[str, int]]:
    """The nine trailers' actual field names and line counts, read from disk.

    The owner is the file, never a literal here: a runner that grew its trailer would
    move this set, and a gate carrying its own copy of it would then fail on a correct
    citation.
    """
    fields: set[str] = set()
    lines: dict[str, int] = {}
    for path in sorted(LOGS.glob("*.status")):
        body = text(path)
        lines[path.name] = len(body.splitlines())
        for line in body.splitlines():
            if "=" in line:
                fields.add(line.split("=", 1)[0].strip())
    return fields, lines


def shell_function_names() -> set[str]:
    """Function names the shared scope library defines.

    `scope_finish` is the function that WRITES a trailer, so a document naming it beside
    a `.status` citation is naming the writer rather than claiming a field.  Derived from
    the library so a renamed function cannot turn into a false positive here.
    """
    names: set[str] = set()
    lib = ROOT / "harness/lib/scope.sh"
    if lib.exists():
        names |= set(re.findall(r"^([A-Za-z_][A-Za-z0-9_]*)\s*\(\)\s*\{", text(lib), re.M))
    return names


def check_status_field_citations(g: Gate, docs: dict[str, str]) -> None:
    """Every `.status` field a document cites must be a field the trailer carries.

    Three citation idioms are recognised, because all three appear in these documents:
    `field `x``/`fields `x`, `y`` after the filename, a backticked `x=value` beside it,
    and the bare ``joern.status` `elapsed_seconds`` juxtaposition.  A token that is a
    sha256, a shell function name or a filename is not a field claim and is not read as
    one.
    """
    fields, _lines = status_trailer_fields()
    owner = "harness/artifacts/logs/*.status (nine verbatim trailers)"
    g.present("status trailers readable as the owner of their own field set",
              owner, owner, sorted(fields),
              "no harness/artifacts/logs/*.status file could be read")
    functions = shell_function_names()

    STATUS_FILE = re.compile(r"`?([a-z0-9\-]+\.status)`?")
    # Idiom 1: the word "field" IS the claim, so it is searched over the whole row or
    # paragraph.  A fixed character window after the filename is exactly the
    # construction that made the line-citation family blind to its own documents, and
    # the same window sat here.
    CLAIM_FIELD = re.compile(r"\bfields?\s+`([a-z_][a-z0-9_]*)`")
    # Idioms 2 and 3 are adjacency-only: an assignment `x=value` or a bare backticked
    # token is a field claim when it sits beside the filename and not otherwise.  Read
    # scope-wide they claim any backticked token in the paragraph -- `packaging=pom` in
    # a build record is a Maven value, and reading it as a trailer field is a false
    # accusation, which is as damaging to a gate as a missed one.
    CLAIM_ADJACENT = re.compile(r"\s*`([a-z_][a-z0-9_]*)(?:=[^`]*)?`")

    for name, body in docs.items():
        offenders: list[str] = []
        history = 0
        live = 0
        skipped = 0
        for scope_start, scope in units(body):
            statuses = sorted((m.start(), m.end(), m.group(1))
                              for m in STATUS_FILE.finditer(scope))
            if not statuses:
                continue

            def preceding(at: int) -> str | None:
                """The nearest `.status` filename at or before `at` in this scope."""
                best = None
                for off, _end, fname in statuses:
                    if off <= at:
                        best = fname
                    else:
                        break
                return best

            claims: list[tuple[int, str, str]] = []
            for m in CLAIM_FIELD.finditer(scope):
                fname = preceding(m.start())
                if fname is not None:
                    claims.append((m.start(), fname, m.group(1)))
            for off, end_off, fname in statuses:
                adjacent = CLAIM_ADJACENT.match(scope, end_off)
                if adjacent:
                    claims.append((adjacent.start(1), fname, adjacent.group(1)))

            for at, fname, token in claims:
                if token in fields:
                    live += 1
                    continue
                if token in functions:
                    skipped += 1
                    continue
                if history_marked_locally(scope, at):
                    history += 1
                    continue
                line_no = body.count("\n", 0, scope_start + at) + 1
                offenders.append(f"{name}:{line_no} cites `{token}` in {fname}")
        g.clear(f"status field citations resolve ({name})", owner, name, offenders)
        # The same population assertion as the line family, for the same reason: a
        # flat document-wide sweep that shares no code with the scoped attribution
        # above, so a form the scoped path misses becomes a mismatch rather than a
        # silence.  The flat sweep counts a field-word claim only where a `.status`
        # filename precedes it somewhere in the document, which is the widest reading
        # of "a claim about a trailer field" available without attribution.
        # The mirror traversal: FORWARD from each filename to the claims that follow it
        # inside its own scope, where the attribution above works BACKWARD from each
        # claim to the filename before it.  Same population, opposite direction, no
        # shared code -- so the two agreeing is evidence, and a form only one of them
        # can see is a mismatch.  A document-wide subject test was tried first and
        # over-counted: `packaging=pom` in a build record is a Maven value, and the
        # nearest `.status` mention being anywhere earlier in the file does not make it
        # a trailer field.
        flat = 0
        for _fstart, fscope in units(body):
            marks = [m.start() for m in STATUS_FILE.finditer(fscope)]
            if not marks:
                continue
            for m in STATUS_FILE.finditer(fscope):
                stop = next((k for k in marks if k > m.start()), len(fscope))
                flat += len(CLAIM_FIELD.findall(fscope[m.end():stop]))
                if CLAIM_ADJACENT.match(fscope, m.end()):
                    flat += 1
        classified = live + history + skipped + len(offenders)
        g.check(f"every trailer-field claim in {name} is classified",
                "a flat document-wide sweep of the same document",
                flat, f"{name} (scoped attribution)", classified)
        print(f"        [{name}: {live} live field citations resolved, "
              f"{history} retracted as history; {classified} of {flat} claims "
              f"classified]")


def check_line_number_citations(g: Gate, docs: dict[str, str]) -> None:
    """Every line citation into this run's own surface must be within the file.

    The failure this catches is the one that actually happened: a 516-line log replaced
    by a 7-line trailer, leaving `joern.status:391-398` and `lines 274-275` pointing past
    the end of a file that still exists.

    ATTRIBUTION IS STRUCTURAL, NOT WINDOWED
    ---------------------------------------
    A first version of this check looked ahead a fixed number of characters from the
    filename to the word "line", and it silently recognised NOTHING in one whole
    document, because these documents write citations the way people do:

        `cpg-verify.log` records the current pair -- 541,309,809 / `4616845a...`, at its
        lines 33-34 and again at 47-50 -- and mentions the earlier one only at its
        lines 76-80

    Three citations, one filename, and prose of arbitrary length between them.  A window
    wide enough for that is wide enough to attribute a line number to the wrong file, so
    the window is gone.  Instead every line reference is attributed to the NEAREST
    PRECEDING backticked filename inside its own scope -- which is how the sentence
    above reads, and which cannot cross into another file's citation because an
    intervening filename becomes the nearer one.  Bare continuations ("and again at
    47-50", "at its lines 76-80") therefore resolve against the file the sentence is
    about, which is the whole point.

    AND THE POPULATION IS ASSERTED, NOT ASSUMED
    -------------------------------------------
    A checker that recognises nothing also reports no violations, so "no offenders" is
    not sufficient evidence on its own.  Every line reference that sits in a scope
    naming an adjudicable file must be attributed and adjudicated; one that is not is
    reported as a COVERAGE gap under its own offender class.  That is what makes a
    future citation form fail loudly instead of being skipped.
    """
    owner = "the cited files themselves, measured on disk"
    resolved_cache: dict[str, int | None] = {}

    def line_count(candidate: str) -> int | None:
        """Length of a cited file, or None when the citation is not ours to adjudicate.

        Not-ours covers two genuinely different things and deliberately returns None for
        both: a path outside the declared adjudicable surface (a pom in the pinned clone,
        upstream `.gitignore`), and a path inside it that does not exist -- absence is
        `check_absence_claims`'s subject, not this one's.
        """
        if candidate in resolved_cache:
            return resolved_cache[candidate]
        target: pathlib.Path | None = None
        if candidate.startswith(ADJUDICABLE_PREFIXES):
            target = ROOT / candidate
        elif "/" not in candidate:
            probe = LOGS / candidate
            if probe.exists():
                target = probe
        count = None
        if target is not None and target.is_file():
            try:
                count = len(target.read_text(encoding="utf-8", errors="replace")
                            .splitlines())
            except OSError:
                count = None
        resolved_cache[candidate] = count
        return count

    # A backticked filename, with an optional `file:12-34` locator attached.
    FILENAME = re.compile(
        # A dotfile (`.gitignore`) or a name.ext, optionally carrying its own locator.
        # Dotfiles are recognised so that a line reference beside one attributes to it
        # rather than reading as an unattributed reference.
        r"`((?:\.[A-Za-z0-9_-]+|[A-Za-z0-9_./\-]+"
        r"\.(?:log|status|json|txt|sarif|md|py|sh|sc|xml|scala|csv)))"
        r"(?::(\d+)(?:\s*[\u2013\u2014-]\s*(\d+))?)?`")
    # The same filename pattern, but only where it carries its own `:N` locator -- the
    # flat sweep counts citations, not filename mentions.
    FILENAME_LOCATOR = re.compile(
        r"`(?:\.[A-Za-z0-9_-]+|[A-Za-z0-9_./\-]+"
        r"\.(?:log|status|json|txt|sarif|md|py|sh|sc|xml|scala|csv))"
        r":(\d+)(?:\s*[\u2013\u2014-]\s*(\d+))?`")
    # A line reference in prose: "line 12", "lines 12-34", "line **12**".
    # A number, excluding a thousands-grouped one: "34" and "34," are line numbers,
    # "925,445" is a method count that happened to follow the word "line".
    N = r"(\d+)(?!\d)(?!,\d)"
    LINEREF = re.compile(
        r"\blines?\s+\*{0,2}" + N + r"\*{0,2}"
        r"(?:\*{0,2}[\u2013\u2014-]\*{0,2}" + N + r")?")
    # A bare continuation of the reference before it: "and again at 47-50".
    CONTINUATION = re.compile(
        r"\band\s+again\s+at\s+" + N + r"(?:[\u2013\u2014-]" + N + r")?")

    # A TYPED REFERENT.  "runner line 50" names its file by type rather than by name,
    # and these documents use it throughout their per-tool sections.  The noun
    # constrains the type, so no other kind of file can capture it -- but the SECTION
    # decides which runner it is.  Document-order resolution was tried first and put
    # trivy's "runner line 61" onto `run-checkov.sh`, the last runner named before it,
    # reporting a defect in a citation that was correct.  A heading is what names the
    # tool, so a heading is the unit this resolves in.  A bare reference still gets no
    # such treatment: attributing "line 67" to whatever file was mentioned last is how
    # a citation silently acquires the wrong owner.
    RUNNER_REF = re.compile(r"\brunner\s+lines?\s+" + N
                            + r"(?:\s*[\u2013\u2014-]\s*" + N + r")?")
    RUNNER_FILE = re.compile(r"`(harness/bin/run-[a-z0-9\-]+\.sh)`")
    HEADING = re.compile(r"^#{1,6}\s+(.+?)\s*$", re.M)

    for name, body in docs.items():
        offenders: list[str] = []
        live = history = foreign = unattributed = unscoped = 0
        runner_at = sorted((m.start(), m.group(1))
                           for m in RUNNER_FILE.finditer(body))
        headings = sorted((m.start(), m.group(1)) for m in HEADING.finditer(body))

        def section_of(at: int) -> tuple[int, int, str]:
            """The enclosing heading's span and title."""
            start, title = 0, ""
            end = len(body)
            for i, (off, text_) in enumerate(headings):
                if off <= at:
                    start, title = off, text_
                    end = (headings[i + 1][0] if i + 1 < len(headings)
                           else len(body))
                else:
                    break
            return start, end, title

        def runner_for(at: int) -> str | None:
            """The runner a typed referent at `at` names, decided by its section.

            First the heading: a section titled with a tool identifier names that
            tool's runner.  Failing that, a runner named in backticks inside the same
            section.  Never one from another section.
            """
            sec_start, sec_end, title = section_of(at)
            slug = title.strip().strip("*` ").lower()
            candidate = ROOT / f"harness/bin/run-{slug}.sh"
            if candidate.is_file():
                return f"harness/bin/run-{slug}.sh"
            best = None
            for off, fname in runner_at:
                if sec_start <= off <= at < sec_end:
                    best = fname
            return best

        # Typed referents first, document-wide, so they are classified wherever they sit.
        typed_spans: list[tuple[int, int]] = []
        for m in RUNNER_REF.finditer(body):
            typed_spans.append((m.start(), m.end()))
            fname = runner_for(m.start())
            if fname is None:
                unscoped += 1
                continue
            total = None
            target = ROOT / fname
            if target.is_file():
                total = len(target.read_text(encoding="utf-8",
                                             errors="replace").splitlines())
            if total is None:
                foreign += 1
                continue
            worst = max(int(m.group(1)), int(m.group(2) or m.group(1)))
            if worst <= total:
                live += 1
            else:
                line_no = body.count("\n", 0, m.start()) + 1
                offenders.append(
                    f"{name}:{line_no} cites {fname} line {worst}, "
                    f"but that file has {total} lines")

        for scope_start, scope in units(body):
            # EVERY filename in the scope, adjudicable or not.  Attributing only to
            # adjudicable ones is what mis-read "`sql/hive/pom.xml`, at line 209" as a
            # citation into some log named elsewhere in the same table row.
            names = sorted((m.start(), m.group(1), m.group(2), m.group(3))
                           for m in FILENAME.finditer(scope))
            if not names:
                # No filename in this row or paragraph, so nothing here can be
                # attributed by name.  These are counted rather than skipped: a
                # reference that leaves no trace in any bucket is the silence this
                # family exists to prevent.  Typed referents were already classified
                # above and are not double-counted.
                for pattern in (LINEREF, CONTINUATION):
                    for m in pattern.finditer(scope):
                        at = scope_start + m.start()
                        if any(lo <= at < hi for lo, hi in typed_spans):
                            continue
                        unscoped += 1
                continue

            def preceding(at: int) -> str | None:
                """The nearest backticked filename at or before `at` in this scope."""
                best = None
                for off, fname, _lo, _hi in names:
                    if off <= at:
                        best = fname
                    else:
                        break
                return best

            def adjudicate(fname: str, low: int, high: str | None, at: int) -> None:
                nonlocal live, history, foreign
                total = line_count(fname)
                if total is None:
                    foreign += 1
                    return
                worst = max(low, int(high) if high else low)
                if worst <= total:
                    live += 1
                    return
                if history_marked_locally(scope, at):
                    history += 1
                    return
                line_no = body.count("\n", 0, scope_start + at) + 1
                offenders.append(
                    f"{name}:{line_no} cites {fname} line {worst}, "
                    f"but that file has {total} lines")

            # 1. `file:12-34` -- the filename carries its own locator, so attribution is
            #    not in question.
            for off, fname, lo, hi in names:
                if lo is not None:
                    adjudicate(fname, int(lo), hi, off)

            # 2. Prose references and their bare continuations, each attributed to the
            #    nearest preceding filename.  A reference with no filename before it
            #    names its referent in prose ("the runner", "the record") and is NOT
            #    guessed at -- guessing would attribute it to whatever file the sentence
            #    mentions next, which is a different file.  It is reported instead.
            for pattern in (LINEREF, CONTINUATION):
                for m in pattern.finditer(scope):
                    if any(lo <= scope_start + m.start() < hi
                           for lo, hi in typed_spans):
                        continue
                    fname = preceding(m.start())
                    if fname is None:
                        unattributed += 1
                        line_no = body.count("\n", 0, scope_start + m.start()) + 1
                        offenders.append(
                            f"UNATTRIBUTED {name}:{line_no} {m.group(0)!r} has no "
                            f"backticked filename before it in its row or paragraph, so "
                            f"which file it indexes cannot be established mechanically")
                        continue
                    adjudicate(fname, int(m.group(1)), m.group(2), m.start())

        # The coverage assertion.  Every reference found is classified into exactly one
        # bucket, and the buckets are printed.  A future citation form that this checker
        # cannot read shows up as UNATTRIBUTED rather than as silence, which is the
        # failure mode that let a windowed earlier version of this check recognise zero
        # citations in a document holding eleven of them and still report clear.
        g.clear(f"line citations resolve ({name})", owner, name, offenders)
        # THE POPULATION ASSERTION.
        #
        # "No offenders" is not evidence on its own, because a checker that recognises
        # nothing also reports nothing -- the exact failure this family had.  So the
        # population is counted a second time by a deliberately different traversal:
        # a flat, document-wide sweep with no scoping and no attribution, which cannot
        # share a bug with the scoped classification above.  Every reference the flat
        # sweep sees must have been classified into exactly one bucket.  A future
        # citation form that the scoped path cannot reach therefore shows up here as a
        # count mismatch instead of as silence.
        flat = len(FILENAME_LOCATOR.findall(body)) + len(LINEREF.findall(body)) \
            + len(CONTINUATION.findall(body))
        classified = (live + history + foreign + unattributed + unscoped
                      + len(offenders))
        g.check(f"every line reference in {name} is classified",
                "a flat document-wide sweep of the same document",
                flat, f"{name} (scoped classification)", classified)
        print(f"        [{name}: {live} adjudicated live, {history} retracted as "
              f"history, {foreign} outside the adjudicable surface, "
              f"{unscoped} naming no file in their own row or paragraph, "
              f"{unattributed} unattributed; {classified} of {flat} references "
              f"classified]")


def check_absence_claims(g: Gate, docs: dict[str, str]) -> None:
    """A path published as absent must be absent, and one published as present present.

    Both directions matter and both have failed.  A path wrongly listed as absent is the
    worse of the two, because a conclusion gets built on it -- "no re-execution is
    possible here" rested on six files that were present all along.

    Scoped to repository-relative paths deliberately.  A claim about a path in another
    clone's scratch directory is a claim about this host at the moment the document was
    written, and a sibling clone can create or remove such a path at any time; a gate
    that flipped with it would be reporting the host's weather rather than the
    document's correctness.  What is durable, and what failed, is the repository surface.
    """
    owner = "the filesystem of this checkout"
    # The verdict cell must OPEN with the absence verdict.  A cell that merely contains
    # the word -- "its 62 archives, the 31 present, the 7 absent" is a description of a
    # reactor, not a claim about the file -- is not an absence claim, and reading it as
    # one would fail the gate on a correct row.
    # Rows are parsed cell by cell rather than by position.  An earlier positional form
    # required the verdict to be the SECOND cell, so the same claim made in a third
    # column went unchecked -- a coverage hole invisible from the pass line.
    VERDICT_OPENS = re.compile(
        r"^\**\s*(?:absent|not present|not resolvable|missing)\b", re.I)
    table_row = re.compile(r"^\|.*\|\s*$", re.M)
    inline = re.compile(
        r"`((?:harness|queries|oss-scan-results)/[A-Za-z0-9_./\-]+)`"
        r"[^.\n|]{0,60}?\b(?:is|are)\s+\*{0,2}absent\b")
    for name, body in docs.items():
        scopes = units(body)
        offenders: list[str] = []
        checked = 0
        for m in table_row.finditer(body):
            cells = [c.strip() for c in m.group(0).strip().strip("|").split("|")]
            # The verdict is whichever cell OPENS with the absence verdict, in any
            # column.  A cell that merely contains the word -- "its 62 archives, the 31
            # present, the 7 absent" describes a reactor -- is not a claim about a file.
            verdicts = [c for c in cells if VERDICT_OPENS.match(c)]
            if not verdicts:
                continue
            # For a table verdict the history scope is the VERDICT CELL, not the whole
            # row.  A divergence-register row runs to several hundred words and can
            # mention a superseded generation for an unrelated reason; letting that
            # exempt the row's own absence verdict is how a claim stops being checked.
            # Prose is different and keeps the paragraph scope below, because prose
            # genuinely does retract an earlier edition's absence claims.
            if all(history_marked(v) for v in verdicts):
                continue
            line_no = body.count("\n", 0, m.start()) + 1
            # The subject is cited in the row's leading cells, up to the verdict.
            subject_cells = cells[:cells.index(verdicts[0])] or cells[:1]
            for cell in subject_cells:
                for cited in re.findall(r"`([A-Za-z0-9_./\-]+)`", cell):
                    if not cited.startswith(("harness/", "queries/",
                                             "oss-scan-results/")):
                        continue
                    checked += 1
                    if (ROOT / cited).exists():
                        offenders.append(
                            f"{name}:{line_no} publishes `{cited}` as absent; "
                            f"it is present")
        for m in inline.finditer(body):
            scope = scope_of(m.start(), scopes)
            if history_marked(scope):
                continue
            cited = m.group(1)
            checked += 1
            if (ROOT / cited).exists():
                line_no = body.count("\n", 0, m.start()) + 1
                offenders.append(
                    f"{name}:{line_no} states `{cited}` is absent; it is present")
        g.clear(f"absence claims are true ({name})", owner, name, offenders)
        print(f"        [{name}: {checked} absence claims tested]")


# ------------------------------------------------------------------------ entrypoint


SELF_TEST_CASES: tuple[tuple[str, bool, str, str], ...] = (
    # (label, must_be_refused, family, document text)
    #
    # LINE-CITATION FAMILY.  One case per form the documents actually use, each in the
    # refused and the accepted direction.  `cpg-verify.log` is 420 lines and
    # `joern.status` is 7, both read from disk by the check itself, so these cases stay
    # true as those files change size -- they cite absurd or safe numbers, not edge ones.
    ("attached locator, out of range",
     True, "line", "`harness/artifacts/logs/cpg-verify.log:99999` states it."),
    ("attached locator range, out of range",
     True, "line", "`harness/artifacts/logs/cpg-verify.log:99998-99999` states it."),
    ("attached locator, in range",
     False, "line", "`harness/artifacts/logs/cpg-verify.log:33` states it."),
    # The form that a windowed earlier version of this check could not see at all: the
    # filename, then a digit-and-backtick-laden clause, then the citation.  This is the
    # shape of oss-scan-results/joern-probe.md's cpg-verify.log paragraph, and a
    # mutation of it to lines that do not exist passed the gate before this case existed.
    ("prose citation behind an intervening clause, out of range",
     True, "line",
     "`harness/artifacts/logs/cpg-verify.log` records the **current** pair -- "
     "541,309,809 / `4616845a...`, at its lines 99998-99999 and again at 99000-99001 "
     "-- and mentions nothing else."),
    ("prose citation behind an intervening clause, in range",
     False, "line",
     "`harness/artifacts/logs/cpg-verify.log` records the **current** pair -- "
     "541,309,809 / `4616845a...`, at its lines 33-34 and again at 47-50 "
     "-- and mentions nothing else."),
    ("bare continuation, out of range",
     True, "line",
     "`harness/artifacts/logs/cpg-verify.log` at its lines 33-34 and again at "
     "99998-99999."),
    # The history exemption must be local.  A paragraph that mentions an earlier
    # generation for its own reasons must not excuse a live citation elsewhere in it --
    # that is how the mutation above was swallowed even once the citation was seen.
    ("out-of-range citation in a paragraph whose history marker is in another clause",
     True, "line",
     "`harness/artifacts/logs/cpg-verify.log` records the current pair at its lines "
     "99998-99999. A superseded earlier generation of this record said otherwise."),
    ("out-of-range citation retracted in its own clause",
     False, "line",
     "An earlier generation quoted `harness/artifacts/logs/joern.status` at its lines "
     "274-275; commit 0e3e742a5ad replaced all nine with the runner trailers."),
    ("line reference with no filename before it",
     True, "line",
     "The runner sets the heap at line 70, and `harness/artifacts/logs/cpg-verify.log` "
     "records the result."),
    # The typed referent, both directions.  run-joern.sh is 76 lines.
    ("typed runner referent, out of range",
     True, "line",
     "`harness/bin/run-joern.sh` bakes the set, and the heap is applied at runner "
     "line 99999."),
    ("typed runner referent, in range",
     False, "line",
     "`harness/bin/run-joern.sh` bakes the set, and the heap is applied at runner "
     "line 70."),
    # The regression that document-order resolution caused: a citation correct for its
    # own section's runner, made in a section whose runner is never backticked, with a
    # SHORTER runner named in the section before it.  run-checkov.sh is 58 lines and
    # run-trivy.sh is 124, so document order refuses line 61 and the section accepts it.
    ("typed referent resolves in its own section, not the preceding one",
     False, "line",
     "## checkov\n\nThe runner `harness/bin/run-checkov.sh` writes the report.\n\n"
     "## trivy\n\n| Working directory | `/opt/spark-src` (`cd \"$SCAN_ROOT\"`, "
     "runner line 61) |"),
    ("typed referent out of range for its own section's runner",
     True, "line",
     "## checkov\n\nThe runner `harness/bin/run-checkov.sh` writes the report.\n\n"
     "## trivy\n\n| Working directory | `/opt/spark-src` (`cd \"$SCAN_ROOT\"`, "
     "runner line 99999) |"),
    ("line reference into a file outside the adjudicable surface",
     False, "line", "`sql/hive/pom.xml`, at line 209, gates the profile."),
    ("a thousands-grouped number after the word line is not a range end",
     False, "line",
     "`harness/artifacts/logs/cpg-verify.log` at its line 93 -- **925,445** methods."),

    # STATUS-FIELD FAMILY.  The trailers carry seven fields; anything else cited as a
    # field is the defect this family exists for.
    ("live citation of a field no trailer carries",
     True, "field",
     "`harness/artifacts/logs/joern.status` records field `heap_used` for the run."),
    ("field citation retracted in its own clause",
     False, "field",
     "An earlier generation of this entry cited `command_source_lines`; commit "
     "0e3e742a5ad replaced all nine `.status` files with the runners' trailers."),
    ("live citation of a field the trailers do carry",
     False, "field",
     "`harness/artifacts/logs/joern.status` records field `elapsed_seconds` for the run."),
    ("field claim far beyond any fixed window from the filename",
     True, "field",
     "`harness/artifacts/logs/joern.status` is the runner's verbatim trailer, and the "
     "sequence ledger binds it by size and sha256 alongside the artifact, the stdout "
     "stream, the stderr stream and the runner console log, none of which is enriched "
     "in any way by this run or any earlier one; it nonetheless records field "
     "`heap_used` for the run."),
    ("a backticked token far from the filename is not claimed as a field",
     False, "field",
     "`harness/artifacts/logs/joern.status` is the runner's verbatim trailer, and the "
     "normalizer reads it; a separate record, `runner-metadata.json`, carries "
     "`invocation_form` instead."),
    ("a backticked assignment elsewhere in the scope is not a trailer field",
     False, "field",
     "`harness/artifacts/logs/joern.status` is the trailer. Separately, the two "
     "aggregator projects are marked *produced none -- EXPECTED, `packaging=pom`*."),
    ("an assignment beside the filename IS a field claim",
     True, "field",
     "`harness/artifacts/logs/joern.status` `heap_used=64g` for the run."),
    ("a shell function name is not a field claim",
     False, "field",
     "The trailer is written by `scope_finish`, whose fields are fixed."),

    # ABSENCE FAMILY.  A file that exists must not be published as absent.
    ("table verdict publishing a present file as absent, verdict in the second cell",
     True, "absence",
     "| `harness/env.sh` | absent from this checkout |"),
    ("table verdict publishing a present file as absent, verdict in the third cell",
     True, "absence",
     "| `harness/env.sh` | the environment file | absent from this checkout |"),
    ("a cell that merely contains the word absent is not a claim about a file",
     False, "absence",
     "| `harness/env.sh` | its 62 archives, the 31 present, the 7 absent | present |"),
    ("inline prose publishing a present file as absent",
     True, "absence",
     "`harness/lib/scope.sh` is absent from this checkout, so nothing reads it."),
    ("table verdict publishing a genuinely absent path as absent",
     False, "absence",
     "| `harness/artifacts/raw/osv-scanner.json` | the artifact | absent from this "
     "checkout, the tool wrote none |"),
    ("absence retracted as history in the verdict cell",
     False, "absence",
     "| `harness/env.sh` | the environment file | absent, said a superseded earlier "
     "generation of this row |"),
)


def self_test() -> int:
    """Prove each citation family refuses each defect it exists to catch.

    A checker that recognises nothing reports no violations, which is indistinguishable
    from a clean document -- and that is not hypothetical here: the first version of the
    line-citation family read zero citations in a document holding eleven of them, and a
    mutation of two of them to lines that do not exist passed.  So the gate carries its
    own negative cases and runs them on demand.  Each case is a document fragment, a
    family, and the verdict that family must reach on it.
    """
    print("publication gate -- self test of the citation families")
    print()
    families = {
        "field": check_status_field_citations,
        "line": check_line_number_citations,
        "absence": check_absence_claims,
    }
    failures = 0
    for label, must_refuse, family, body in SELF_TEST_CASES:
        probe = Gate()
        import io
        import contextlib
        with contextlib.redirect_stdout(io.StringIO()):
            families[family](probe, {"self-test-case.md": body})
        refused = any(not ok for ok, *_ in probe.rows)
        good = refused == must_refuse
        failures += 0 if good else 1
        want = "refuse" if must_refuse else "accept"
        got = "refused" if refused else "accepted"
        print(f"  {'ok   ' if good else 'FAIL '} [{family}] must {want}, {got}: {label}")
    print()
    print(f"  cases                    : {len(SELF_TEST_CASES)}")
    print(f"  wrong verdicts           : {failures}")
    print()
    if failures:
        print("SELF TEST FAILED -- a citation family does not refuse a defect it must, "
              "or refuses a form the documents legitimately use.")
        return 1
    print("SELF TEST PASS -- every citation form is recognised, and every defect in it "
          "is refused.")
    return 0


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

    # The three citation families, over every result document rather than the three
    # this module already had loaded: the cascade these catch reached build-record.md
    # and joern-probe.md as well.
    docs = {rel(path): text(path) for path in RESULT_DOCUMENTS if path.exists()}
    check_status_field_citations(g, docs)
    check_line_number_citations(g, docs)
    check_absence_claims(g, docs)

    return g.report()


if __name__ == "__main__":
    if "--self-test" in sys.argv[1:]:
        sys.exit(self_test())
    sys.exit(main())
