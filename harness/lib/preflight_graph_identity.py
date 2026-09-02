#!/usr/bin/env python3
"""Pre-load graph-identity gate: refuse to load a graph the write-time record does not describe.

WHY THIS EXISTS AS A SEPARATE, EXECUTABLE STEP
==============================================
AAP 0.8.2 requires the graph's identity re-verified immediately before every load, and
0.6.4 requires each check logged, because "a load against different bytes than the record
describes produces conclusions about a graph nobody has".

The measurement and the comparison are two different acts, and the Stage 3 path used to
perform only the first:

* ``harness/bin/run-joern.sh`` resolved its input and PRINTED the path, the byte size and
  the digest -- and then invoked Joern. Printing a value is not checking it.
* ``harness/lib/joern-scan.sc`` calls ``importCpg`` and then counts. By the time any
  figure exists inside the script, the bytes are already in the engine.

This module is that comparison, kept as one executable step because it resolves the
record of account by provenance and returns a single adjudicated status a caller can
refuse on -- logic that belongs in one place read by every load path rather than
duplicated in each.

**The Stage 3 runner now reads it.** ``harness/bin/run-joern.sh`` runs this module in its
``--check-only`` form against the ``HARNESS_CPG`` it is about to open, echoes the whole
report to its own stdout, keeps a copy at
``$HARNESS_LOG_DIR/joern.preload-identity.log``, and exits 78 without loading anything on
any non-zero status. So the canonical direct no-argument invocation is bound by this gate
structurally rather than by convention: the comparison is upstream of ``importCpg`` on the
path Stage 3 actually takes. ``harness/lib/run-joern-gated.sh`` remains a valid, now
redundant, belt-and-braces caller that performs the same comparison one step earlier. The
EXIT STATUS section below names both and says which route the Stage 3 invocation on record
actually took.

WHAT IT COMPARES, AND WHY BOTH VALUES
=====================================
The single write-time identity pair is the ``bytes:``/``sha256:`` lines of the
machine-readable identity block in ``harness/artifacts/logs/cpg-frontend.log``, the
record of account written by the frontend that produced the graph. This module asserts
that block carries EXACTLY ONE of each before it will use them: two candidate pairs
would let a later reader pick the one that matches and call it a check.

WHEN THE FRONTEND LOG OWNS NO IDENTITY, AND WHERE THE PAIR COMES FROM THEN
=========================================================================
A write-time record exists only if a write happened here. This run's own frontend
invocation over the complete input manifest terminated IN PERSISTENCE and produced no
graph (run-record.md divergence D1), so ``cpg-frontend.log`` is the record of a failed
build: the only identity it carries is the rejected truncated partial's, and
run-record.md section 5 states in terms that the file does not own the identity of
record. A gate that insisted on reading the pair from there would halt on the FORMAT of
a log rather than on the identity of a graph -- a diagnostic that sends a reader after
the wrong thing entirely.

So the record of account is resolved by PROVENANCE, in a fixed order, and never by which
candidate happens to match:

1. ``harness/artifacts/logs/cpg-frontend.log``, when it carries exactly one strict
   ``bytes:``/``sha256:`` pair. That is the write-time record of a graph THIS CHECKOUT's
   frontend wrote, and when it exists it governs.
2. Otherwise the provisioning record of account that sits beside the resolved graph:
   ``<graph's directory>/../provision-log/cpg-identity.txt``, one line of
   ``<bytes> <sha256>``, corroborated by ``cpg-record.txt`` in the same directory. The
   path is DERIVED from the graph the runner would actually open, not hardcoded, so a
   clone whose ``HARNESS_CPG`` points elsewhere is checked against that graph's own
   record rather than against this one's.
3. And, ALWAYS, the DECLARED record: section 7 of ``harness/ENVIRONMENT.md``, the
   authoritative environment record the runbook requires read first and AAP 0.6.1 marks
   REFERENCE. Its ``Bytes`` and ``sha256`` rows are a statement about the same graph its
   own ``Path`` row names, so a gate that never read them could pass while the document of
   record described a different graph -- which is precisely what was observed here, and
   is why this candidate exists (see the next section).

Every candidate that exists is read, and any disagreement between them is FATAL -- as is
more than one distinct pair inside any single record. The precedence orders provenance
(who wrote the bytes), so it cannot be used to select an outcome: if the records disagree,
no pair is chosen and nothing is loaded.

THE DECLARED RECORD IS READ EVEN WHEN A WRITE-TIME RECORD EXISTS
================================================================
Candidates 1 and 2 are ordered by who wrote the bytes. Candidate 3 is not an alternative
to them -- it is the environment's own claim about the same graph, and it is read in every
case rather than only when the others are missing, because a claim nobody compares is not
a record. Concretely, in this checkout:

* the provisioning record beside the graph states 541,309,809 bytes /
  ``4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7``, which is what the
  bytes on disk measure;
* ``harness/ENVIRONMENT.md`` section 7 DECLARES 541,255,894 bytes /
  ``26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc`` for the very same
  ``/opt/blitzy-harness/cpg/spark.cpg`` path;

so the two records disagree, and this gate therefore HALTS where before it passed. That
outcome is intended, not a regression: the graph on disk is not the graph the authoritative
environment record describes, one of the two is wrong about 541 MB of provisioned state,
and "fail closed" means the gate declines to pick the one that happens to match the bytes
in front of it. AAP 0.1.3's fourth case is the authority -- an observable fact
contradicting the environment record on a field the expected-values table does not anchor
is recorded with BOTH values and the run stops.

Resolving it is a PROVISIONING act, not something a clone may do: the graph and its record
must be replaced together so that one identity describes one set of bytes. Neither
``harness/ENVIRONMENT.md`` (REFERENCE under AAP 0.6.1) nor the shared graph may be edited
from here, so this gate's job ends at refusing and saying exactly what disagrees.

A section 7 that cannot be parsed is its own named CONFIGURATION FAULT (exit 78) rather
than a silent agreement: "the record could not be read" and "the record agrees" are
opposite facts, and a parser that returned the second for the first would be the one bug
that makes every later verdict meaningless.

Both values are recomputed from the bytes on disk and both must match. Size alone is a
weak test -- a graph rebuilt over a different input set can land on the same length --
and a digest alone leaves a truncated read undetected as a size discrepancy.

THE RUNNER'S ACTUAL INPUT, NOT A NAME THAT RESEMBLES IT
======================================================
``harness/env.sh`` exports ``HARNESS_CPG`` as ``${HARNESS_CPG:-$HARNESS_DIR/cpg/spark.cpg}``,
so the value the runner opens is whatever the environment holds -- overridable, and only
equal to the AAP-named path when nothing overrode it. A gate that measured the AAP paths
alone would be checking one graph while the runner loaded another, and the check would
pass on a graph nobody was about to read.

So the effective ``HARNESS_CPG`` is resolved here the same way ``env.sh`` resolves it and
measured as a first-class subject, LABELLED as the runner's actual input. When an override
points it somewhere else, that path is measured too and every one of them must match the
write-time pair; when nothing overrides it, it coincides with an AAP name and the report
says so rather than silently listing the same file twice.

THE SUBJECT LIST IS WHAT EXISTS, AND ITS ABSENCES ARE STATED
===========================================================
The two AAP 0.6.4 names are always measured. A third subject -- an in-checkout write
location at ``harness/artifacts/cpg/spark.cpg`` -- exists only in a run whose frontend
wrote the graph inside the checkout, so it is measured when it is there and reported as
absent when it is not, rather than failing as a missing file. Its absence removes no
coverage: the plan names two paths and both are still measured, and the runner's actual
input is measured as its own subject. Asserting a third path that this run never wrote
would fail the gate on a fact about D1 rather than on the graph's identity.

BOTH AAP NAMES, MEASURED SEPARATELY
===================================
AAP 0.6.4 requires that ``harness/cpg/spark.cpg`` and whatever the environment exports
both resolve to the bytes this run wrote. Each name is therefore resolved and measured
in its own right rather than one being assumed to follow the other, and the resolved
targets are compared so that "both names agree" is an observation rather than an
inference from the symlink's existence.

The measurement FOLLOWS the link. A size taken without following it reports the length
of the target path string -- a few dozen bytes -- which would describe nothing at all.
That reading is recorded anyway, explicitly marked as discarded, so it cannot later be
mistaken for the graph's size.

RE-RUNNING IT WITHOUT DESTROYING THE EVIDENCE THAT IT RAN
========================================================
A normal run REPLACES ``harness/artifacts/logs/joern-preflight.log``, and that file is the
record showing the gate preceded the Stage 3 invocation -- its ``Checked at`` stamp is what
``joern.status``'s ``gate_finished_at`` is consistent with. So anyone re-running this
module to audit it silently overwrites the ordering evidence for the run of record, which
was observed happening during this run's own final validation.

``--check-only`` performs every measurement and prints the whole report but writes
nothing. That is the form ``harness/bin/run-joern.sh`` calls, for two reasons: the
runner keeps its own copy of the report under ``$HARNESS_LOG_DIR``, so it needs no write
here, and a runner that rewrote a committed canonical deliverable would stop being
hermetic the moment a caller redirected ``HARNESS_LOG_DIR``. Use the default form only
when a caller genuinely wants that durable record replaced. The negative test needs the
writing form, so it saves that record first and restores it afterwards, asserting byte
equality.

EXIT STATUS, AND THE CALLERS THAT MAKE IT BINDING
=================================================
``0``  every check passed; the caller may invoke the runner.
``77`` a fatal mismatch or a missing prerequisite -- including two records of account
       that disagree. The caller MUST NOT invoke the runner. 77 rather than 1 so a caller
       cannot confuse it with an ordinary error, and distinct from the runners' own 64
       (bad argument) so the two are never conflated in a log.
``78`` a CONFIGURATION FAULT: a record of account exists but could not be read, which is
       ``scope.sh``'s ``scope_fail`` status for the same class of condition. Today the one
       such condition is a ``harness/ENVIRONMENT.md`` whose section 7 is absent,
       unparsable or ambiguous. The caller MUST NOT invoke the runner either way; the
       distinct status exists because "correct the record" and "the graph is not the
       recorded graph" send a reader to different places.

An exit status only binds something that reads it, so the readers are committed alongside
it. There are two, and the first is the one that matters:

* ``harness/bin/run-joern.sh``, the canonical Stage 3 runner, invoked directly with no
  arguments as AAP 0.8.1 requires. It runs this module ``--check-only`` after printing
  its input's identity and before ``rm -f`` on its artifact or any ``joern`` invocation,
  and maps a non-zero status to its own configuration fault (78) with the gate's report
  echoed to its console and copied to ``$HARNESS_LOG_DIR/joern.preload-identity.log``.
  Nothing is loaded and no artifact is written or removed on a refusal.
* ``harness/lib/run-joern-gated.sh``, a committed gated path that sources ``env.sh``,
  runs this module against the effective ``HARNESS_CPG`` and reaches the runner only on
  0 -- one route through it, no branch that invokes Joern after a non-zero gate. Since
  the runner gained its own check this wrapper is redundant rather than load-bearing: an
  invocation routed through it is adjudicated twice, by two independent readings of this
  same gate.

The invocation on record predates the runner's self-binding and took the direct route:
``argv=["./harness/bin/run-joern.sh"]`` at line 3 of
``harness/artifacts/logs/joern.runner-console.log``, so the wrapper's gate was not
exercised for that load and its contemporaneous identity evidence is the runner's own
recompute, printed rather than compared. That is the gap the runner's own gate closes for
every subsequent direct invocation; the wrapper's header records the same history from its
side.

The negative test proving both directions is preserved verbatim beside this module's own
output, at ``harness/artifacts/logs/joern-preflight-negative-test.log``: it drives the
GATED WRAPPER, not this module alone, and records that the runner produced no output and
left its artifact untouched when the gate refused. A gate nobody has seen refuse is a
gate nobody has tested, and a wrapper nobody has seen decline to run the runner is a
wrapper whose binding is unproven.
"""

from __future__ import annotations

import hashlib
import os
import re
import sys
import time
from pathlib import Path
from typing import Final, NamedTuple

#: Exit status meaning "do not invoke the runner". Chosen not to collide with the
#: runners' 64 or with an ordinary non-zero exit.
HALT_EXIT: Final[int] = 77

#: Exit status meaning "a record of account exists but could not be read". Identical to
#: scope.sh's scope_fail status (EX_CONFIG) and to preflight_scan_target.CONFIG_EXIT, so
#: one number means one thing across the harness.
CONFIG_EXIT: Final[int] = 78

#: Read in 1 MiB blocks: the graph is hundreds of megabytes and reading it whole to
#: hash it would hold all of it in memory for no benefit.
_BLOCK: Final[int] = 1 << 20

#: The authoritative environment record, relative to the repository root, and the heading
#: of the section that declares the graph's identity. Both are matched loosely enough to
#: survive an editorial change to the heading's wording but strictly enough that a
#: DIFFERENT section can never be read in its place.
ENVIRONMENT_RECORD: Final[str] = "harness/ENVIRONMENT.md"
GRAPH_SECTION_NUMBER: Final[str] = "7"


class ConfigurationFault(Exception):
    """A record of account exists but could not be read.

    Distinct from ``ValueError`` -- which this module already uses for "the records
    disagree" and "one record is ambiguous" -- because the two demand different responses
    and carry different exit statuses. Raising the same type for both would force the
    caller to parse a message to tell "fix the document" from "the graph is not the
    recorded graph".
    """


def repo_root() -> Path:
    """Locate the repository root without hardcoding a checkout path.

    ``HARNESS_REPO_ROOT`` wins when set, so a caller can point the gate at a specific
    checkout. Otherwise the root is derived from this file's own location -- this module
    lives at ``<root>/harness/lib/`` -- which is the same technique ``harness/env.sh``
    uses to stay correct in any clone with no edit.
    """
    env = os.environ.get("HARNESS_REPO_ROOT")
    if env:
        return Path(env).resolve()
    return Path(__file__).resolve().parent.parent.parent


def sha256_of(path: Path) -> str:
    """Return the sha256 of ``path``'s contents, reading it in bounded blocks."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(_BLOCK), b""):
            digest.update(block)
    return digest.hexdigest()


class RecordOfAccount(NamedTuple):
    """The one identity pair the gate will adjudicate against, and where it came from."""

    size: int
    sha256: str
    source: Path
    provenance: str
    corroborated_by: tuple[Path, ...]


def strict_identity(record: Path) -> tuple[int, str] | None:
    """Return the strict ``bytes:``/``sha256:`` pair on ``record``, or ``None``.

    ``None`` means the record carries neither -- which is a fact about the record, not a
    failure, because a frontend that produced no graph writes no write-time identity.
    Raises ``ValueError`` when it carries more than one distinct candidate pair, or one
    half of a pair without the other: ambiguity is refused rather than resolved, since a
    check that accepts whichever candidate matches is not a check.
    """
    text = record.read_text(errors="replace")
    sizes = {int(value) for value in re.findall(r"^\s*bytes:\s*(\d+)\s*$", text, re.M)}
    digests = set(re.findall(r"^\s*sha256:\s*([0-9a-f]{64})\s*$", text, re.M))
    if not sizes and not digests:
        return None
    if len(sizes) != 1 or len(digests) != 1:
        raise ValueError(
            f"{record} must carry exactly one 'bytes:' value and one 'sha256:' value; "
            f"found {len(sizes)} and {len(digests)} distinct. An ambiguous record cannot "
            f"adjudicate an identity check."
        )
    return sizes.pop(), digests.pop()


def provisioning_identity(record: Path) -> tuple[int, str] | None:
    """Return the identity pair a provisioning record carries, or ``None``.

    Two shapes are accepted, both written by the provisioner beside the graph itself: a
    single ``<bytes> <sha256>`` line (``cpg-identity.txt``) and a labelled
    ``Bytes : <n>`` / ``sha256 : <hex>`` pair (``cpg-record.txt``). Exactly one distinct
    pair must be derivable, for the same reason ``strict_identity`` demands it.
    """
    if not record.is_file():
        return None
    text = record.read_text(errors="replace")
    # Shape, not plausibility: a line that is a size then a 64-hex digest and nothing
    # else. A size threshold would silently stop recognising a record whose graph was
    # small, and ambiguity is fatal anyway, so a false positive halts rather than passes.
    inline = re.findall(r"^\s*(\d+)\s+([0-9a-f]{64})\s*$", text, re.M)
    sizes = {int(size) for size, _ in inline}
    digests = {digest for _, digest in inline}
    sizes |= {int(v.replace(",", ""))
              for v in re.findall(r"^\s*[Bb]ytes\s*:\s*([\d,]+)\s*$", text, re.M)}
    digests |= set(re.findall(r"^\s*sha256\s*:\s*([0-9a-f]{64})\s*$", text, re.M))
    if not sizes and not digests:
        return None
    if len(sizes) != 1 or len(digests) != 1:
        raise ValueError(
            f"{record} must yield exactly one identity pair; found {len(sizes)} distinct "
            f"sizes and {len(digests)} distinct digests. An ambiguous record cannot "
            f"adjudicate an identity check."
        )
    return sizes.pop(), digests.pop()


def _table_cells(line: str) -> list[str]:
    """Split one Markdown table row into its cells, or return ``[]``.

    A row is a line whose first non-space character is ``|``. The leading and trailing
    empty cells that the pipe delimiters produce are dropped, and each cell is stripped of
    the emphasis and code markers the record uses for presentation (``*`` and a backtick),
    so ``| **Bytes** | **541,255,894** |`` yields ``['Bytes', '541,255,894']``. Anything
    that is not a table row yields no cells, which is what keeps PROSE mentioning a digest
    -- and section 7 has such prose -- out of the parse.
    """
    stripped = line.strip()
    if not stripped.startswith("|"):
        return []
    parts = stripped.split("|")
    if len(parts) >= 2 and parts[0].strip() == "":
        parts = parts[1:]
    if parts and parts[-1].strip() == "":
        parts = parts[:-1]
    return [cell.strip().strip("*").strip("`").strip() for cell in parts]


def graph_section(record: Path) -> list[str]:
    """Return the lines of the environment record's graph section.

    The section is located by its ATX heading -- ``## 7.`` -- and ends at the next
    heading at the same level. Located by NUMBER rather than by title so a reworded title
    still resolves, and bounded by the next ``## `` so a value from section 8 can never be
    read as section 7's.

    Raises ``ConfigurationFault`` when the record or the section is absent, because
    "the record does not exist" must not be reachable from the same code path as "the
    record agrees".
    """
    if not record.is_file():
        raise ConfigurationFault(
            f"the authoritative environment record {record} is absent, so its declared "
            f"graph identity could not be read. It is provisioned rather than tracked "
            f"(AAP 0.6.1 marks it REFERENCE); a clone without it is incompletely "
            f"provisioned, and this gate will not certify a graph against a record that "
            f"is not there."
        )
    lines = record.read_text(errors="replace").splitlines()
    start: int | None = None
    for index, line in enumerate(lines):
        if re.match(rf"^##\s+{GRAPH_SECTION_NUMBER}\.\s", line):
            if start is not None:
                raise ConfigurationFault(
                    f"{record} carries more than one '## {GRAPH_SECTION_NUMBER}.' "
                    f"heading (lines {start + 1} and {index + 1}), so the section that "
                    f"declares the graph's identity is ambiguous."
                )
            start = index
    if start is None:
        raise ConfigurationFault(
            f"{record} carries no '## {GRAPH_SECTION_NUMBER}.' section, so the declared "
            f"graph identity could not be located. Section {GRAPH_SECTION_NUMBER} is "
            f"where the record states the graph's byte size and sha256."
        )
    for index in range(start + 1, len(lines)):
        if lines[index].startswith("## "):
            return lines[start:index]
    return lines[start:]


def declared_identity(record: Path) -> tuple[int, str]:
    """Return the identity pair the environment record DECLARES for the graph.

    Read from the table rows of section 7 whose first cell is ``Bytes`` and ``sha256``.
    Exactly one of each is required -- the same rule ``strict_identity`` and
    ``provisioning_identity`` apply, and for the same reason: two candidate pairs inside
    one record would let a reader pick the one that matched.

    Every failure is a ``ConfigurationFault``, never a ``None`` and never a guess. A
    record that cannot be parsed is a document to correct; treating it as silence would
    remove the only comparison that catches a stale declaration, which is exactly the
    defect this candidate exists to catch.
    """
    section = graph_section(record)
    sizes: set[int] = set()
    digests: set[str] = set()
    for line in section:
        cells = _table_cells(line)
        if len(cells) < 2:
            continue
        label = cells[0].lower().rstrip(":").strip()
        value = cells[1]
        if label == "bytes":
            match = re.fullmatch(r"([\d,]+)", value)
            if not match:
                raise ConfigurationFault(
                    f"{record} section {GRAPH_SECTION_NUMBER} has a 'Bytes' row whose "
                    f"value is not a decimal byte count: {value!r}. A byte count that "
                    f"cannot be read cannot be compared."
                )
            sizes.add(int(match.group(1).replace(",", "")))
        elif label == "sha256":
            match = re.fullmatch(r"([0-9a-f]{64})", value.lower())
            if not match:
                raise ConfigurationFault(
                    f"{record} section {GRAPH_SECTION_NUMBER} has a 'sha256' row whose "
                    f"value is not a 64-character hex digest: {value!r}."
                )
            digests.add(match.group(1))
    if len(sizes) != 1 or len(digests) != 1:
        raise ConfigurationFault(
            f"{record} section {GRAPH_SECTION_NUMBER} must declare exactly one 'Bytes' "
            f"row and one 'sha256' row; found {len(sizes)} distinct byte count(s) and "
            f"{len(digests)} distinct digest(s). Neither an absent declaration nor an "
            f"ambiguous one can adjudicate an identity check, and neither may be read as "
            f"agreement with the bytes on disk."
        )
    return sizes.pop(), digests.pop()


def provisioning_records(graph: Path) -> list[Path]:
    """Return the provisioning records that sit beside ``graph``, most specific first.

    Derived from the graph the runner would actually open -- ``<graph dir>/../
    provision-log/`` -- rather than hardcoded, so a clone pointing ``HARNESS_CPG``
    somewhere else is adjudicated against that graph's own record.
    """
    try:
        base = graph.resolve().parent.parent / "provision-log"
    except OSError:
        return []
    return [base / "cpg-identity.txt", base / "cpg-record.txt"]


def record_of_account(root: Path, graph: Path) -> RecordOfAccount:
    """Resolve the one identity pair to adjudicate against, ordered by provenance.

    The frontend log governs when it carries a write-time pair, because such a pair
    exists only if this checkout's frontend wrote a graph. When it carries none, the
    provisioning record beside the resolved graph governs. The environment record's
    DECLARED pair is read in every case, because it is the authoritative document's own
    claim about the same graph. Every candidate that exists is read and any disagreement
    is fatal, so the order selects a WRITER, never an outcome.
    """
    frontend = root / "harness/artifacts/logs/cpg-frontend.log"
    candidates: list[tuple[Path, str, tuple[int, str]]] = []
    if frontend.is_file():
        pair = strict_identity(frontend)
        if pair is not None:
            candidates.append(
                (frontend, "write-time record: this checkout's frontend wrote the graph", pair))
    for record in provisioning_records(graph):
        pair = provisioning_identity(record)
        if pair is not None:
            candidates.append(
                (record,
                 "provisioning record of account for the graph this run did not write",
                 pair))
    # Always read, never conditional on the others: a declaration nobody compares is not a
    # record. A parse failure raises ConfigurationFault and is reported as exit 78 -- it
    # is deliberately NOT caught here, because falling through would let an unreadable
    # declaration behave exactly like an agreeing one.
    environment = root / ENVIRONMENT_RECORD
    candidates.append(
        (environment,
         f"declared record: {ENVIRONMENT_RECORD} section {GRAPH_SECTION_NUMBER}, the "
         f"authoritative environment record",
         declared_identity(environment)))
    if not candidates:
        raise ValueError(
            f"no record of account carries an identity pair for {graph}: "
            f"{frontend} records no accepted graph (see run-record.md D1) and no "
            f"provisioning record was found beside the resolved graph at "
            f"{', '.join(str(r) for r in provisioning_records(graph)) or '<none>'}"
        )
    distinct = {pair for _, _, pair in candidates}
    if len(distinct) != 1:
        detail = "; ".join(
            f"{path} ({provenance}) states {pair[0]:,} bytes / {pair[1]}"
            for path, provenance, pair in candidates)
        raise ValueError(
            "the records of account DISAGREE, so no identity can be adjudicated and "
            f"nothing may be loaded. {detail}. This gate does not resolve the "
            "disagreement by adopting whichever record matches the bytes on disk -- a "
            "check that picks the answer it likes is not a check. AAP 0.1.3's fourth "
            "case governs: an observable fact contradicting the environment record on a "
            "field the expected-values table does not anchor is recorded with BOTH values "
            "and the run stops. Resolving it is a PROVISIONING act -- replace the graph "
            "and its record together, atomically, so one identity describes one set of "
            f"bytes -- and not something a clone may do: {ENVIRONMENT_RECORD} is "
            "REFERENCE under AAP 0.6.1 and the graph is provisioned shared state."
        )
    source, provenance, (size, digest) = candidates[0]
    return RecordOfAccount(size, digest, source, provenance,
                           tuple(path for path, _, _ in candidates[1:]))


def effective_harness_cpg(root: Path) -> Path:
    """Return the path the Stage 3 runner will actually open, however it was set.

    ``harness/env.sh`` line 28 exports ``HARNESS_CPG`` as
    ``${HARNESS_CPG:-$HARNESS_DIR/cpg/spark.cpg}``. This reproduces that resolution
    exactly -- an environment value wins, and the fallback is the same default -- so the
    gate measures the runner's input rather than a path that merely resembles it.
    """
    env = os.environ.get("HARNESS_CPG")
    if env:
        return Path(env)
    return root / "harness/cpg/spark.cpg"


def subjects(root: Path) -> list[tuple[str, Path]]:
    """Return every path that must match the write-time identity, each with its role.

    The runner's actual input comes first because it is the one that decides what gets
    loaded; the two AAP 0.6.4 names follow because the plan requires both to resolve to
    the bytes this run wrote. A path appearing in more than one role is listed once, under
    every role it holds, so the report cannot read as though two independent files agreed
    when one file was measured twice.
    """
    ordered: list[tuple[str, Path]] = [
        ("the runner's actual input ($HARNESS_CPG as env.sh resolves it)",
         effective_harness_cpg(root)),
        ("AAP 0.6.1 named path (harness/cpg/spark.cpg)", root / "harness/cpg/spark.cpg"),
    ]
    # An in-checkout write location exists only in a run whose frontend wrote the graph
    # here. Measured when present; its absence is reported by the caller rather than
    # failing the gate, because the two AAP names above are what decide the load.
    written_here = root / "harness/artifacts/cpg/spark.cpg"
    if written_here.exists() or written_here.is_symlink():
        ordered.append(
            ("the graph this run wrote (harness/artifacts/cpg/spark.cpg)", written_here))
    merged: list[tuple[str, Path]] = []
    for role, path in ordered:
        for index, (existing_role, existing_path) in enumerate(merged):
            if str(path) == str(existing_path):
                merged[index] = (f"{existing_role}; also {role}", existing_path)
                break
        else:
            merged.append((role, path))
    return merged


def main(argv: list[str] | None = None) -> int:
    """Run every check, write the durable record, and return the process exit status.

    ``--check-only`` suppresses the write. Every measurement and the whole report are
    still produced; what is skipped is replacing the durable record, because that record
    is the evidence the gate ran BEFORE the invocation and an audit run would otherwise
    overwrite it with a later timestamp.
    """
    check_only = "--check-only" in (argv if argv is not None else sys.argv[1:])
    root = repo_root()
    logs = root / "harness/artifacts/logs"
    names = subjects(root)
    # Resolved before the header is emitted so the report can NAME the record it
    # adjudicated against and where that record came from, rather than naming a file it
    # may not have used.
    record: RecordOfAccount | None = None
    record_error: str | None = None
    # A configuration fault -- a record that exists but cannot be read -- is tracked
    # separately from a mismatch so the two can carry different exit statuses. Both
    # forbid the load; only the status tells a reader which document to go and fix.
    config_error: str | None = None
    try:
        record = record_of_account(root, effective_harness_cpg(root))
    except ConfigurationFault as exc:
        config_error = str(exc)
    except (ValueError, OSError) as exc:
        record_error = str(exc)

    report: list[str] = []
    fatal: list[str] = []

    def emit(text: str = "") -> None:
        report.append(text)
        print(text)

    def emit_wrapped(indent: str, text: str) -> None:
        """Emit a long diagnostic as readable lines rather than as one enormous one.

        The disagreement message names two identities, two provenances and the authority
        for stopping, which is several hundred characters -- unreadable on one line, and
        this file is read by a person deciding whether a graph may be loaded.
        """
        for line in _wrap(text, 92):
            emit(f"{indent}{line}")

    emit("=" * 84)
    emit("STAGE 3 PRE-LOAD GRAPH IDENTITY CHECK")
    emit("=" * 84)
    emit()
    emit("  Refuses to invoke the Stage 3 Joern runner unless the graph on disk is")
    emit("  byte-for-byte the graph EVERY record of account describes -- the frontend log")
    emit("  when this checkout's frontend wrote one, the provisioning record beside the")
    emit("  graph itself, and always the identity declared by")
    emit(f"  {ENVIRONMENT_RECORD} section {GRAPH_SECTION_NUMBER}. Records that disagree")
    emit("  are fatal and none is preferred for matching. The runner MEASURES and PRINTS")
    emit("  its input's size and digest, and harness/lib/joern-scan.sc calls importCpg and")
    emit("  then counts, so without this comparison a mismatch would reach the engine.")
    emit(f"  A mismatch here exits {HALT_EXIT}, and both committed callers refuse on it:")
    emit("  harness/bin/run-joern.sh -- the canonical direct runner -- runs this gate")
    emit("  --check-only before it touches its artifact or invokes joern and maps a")
    emit("  non-zero status to its own configuration fault (78), and")
    emit("  harness/lib/run-joern-gated.sh has no branch that reaches the runner after a")
    emit("  non-zero gate. The direct path is therefore bound by this status too, which")
    emit("  the invocation on record predates.")
    emit()
    emit(f"  Gate source             : harness/lib/preflight_graph_identity.py")
    emit(f"  Binding callers         : harness/bin/run-joern.sh (canonical, --check-only)")
    emit(f"                            harness/lib/run-joern-gated.sh (step 2 of 4)")
    emit(f"  Checked at (UTC)        : {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}")
    emit(f"  Clone index             : {os.environ.get('BLITZY_CLONE_INDEX', '0')}")
    if record is not None:
        try:
            shown_record = str(record.source.relative_to(root))
        except ValueError:
            shown_record = str(record.source)
        emit(f"  Record of account       : {shown_record}")
        emit(f"  Its provenance          : {record.provenance}")
        if record.corroborated_by:
            emit(f"  Corroborated by         : "
                 f"{', '.join(str(path) for path in record.corroborated_by)}, which agree")
    elif config_error is not None:
        emit(f"  Record of account       : NOT READABLE -- configuration fault "
             f"(exit {CONFIG_EXIT})")
    else:
        emit(f"  Record of account       : NONE RESOLVED -- see the FATAL detail below, "
             f"which names every candidate")
    written_here = root / "harness/artifacts/cpg/spark.cpg"
    if not (written_here.exists() or written_here.is_symlink()):
        emit("  In-checkout write path  : harness/artifacts/cpg/spark.cpg is absent -- no")
        emit("                            graph was written inside this checkout, so the two")
        emit("                            AAP 0.6.4 names are the subjects that decide the load")
    emit(f"  HARNESS_CPG in env      : "
         f"{os.environ.get('HARNESS_CPG', '<unset -- env.sh default applies>')}")
    emit()

    if config_error is not None:
        # Deliberately NOT appended to `fatal`: a record that cannot be read is a
        # different outcome from a graph that does not match its record, and the two
        # carry different exit statuses. No subject is measured, because there is nothing
        # to measure against.
        emit("  CONFIGURATION FAULT:")
        emit_wrapped("    ", config_error)
        emit()
        emit("  Nothing was measured against a record, and nothing may be loaded. Correct")
        emit(f"  the record ({ENVIRONMENT_RECORD} section {GRAPH_SECTION_NUMBER}) and")
        emit("  re-run; this gate does not repair a document it is required to read.")
    elif record is None:
        fatal.append(record_error or "no record of account could be resolved")
        emit("  FATAL:")
        emit_wrapped("    ", fatal[-1])
    else:
        want_size, want_sha = record.size, record.sha256
        emit(f"  Recorded size           : {want_size:,} bytes")
        emit(f"  Recorded sha256         : {want_sha}")
        emit()
        emit("  Every subject, each resolved and re-measured in its own right:")
        emit()
        resolved: list[Path] = []
        for role, name in names:
            try:
                shown = str(name.relative_to(root))
            except ValueError:
                # An overridden HARNESS_CPG can point outside the checkout. Show the
                # absolute path rather than failing to describe the thing under test.
                shown = str(name)
            emit(f"    {shown}")
            emit(f"      role      : {role}")
            if name.is_symlink():
                emit(f"      symlink to: {os.readlink(name)}")
            if not name.exists():
                fatal.append(f"{shown} does not resolve to an existing file")
                emit(f"      FATAL     : {fatal[-1]}")
                emit()
                continue
            real = name.resolve()
            resolved.append(real)
            size = real.stat().st_size
            got = sha256_of(real)
            emit(f"      realpath  : {real}")
            emit(f"      size      : {size:,} bytes   "
                 f"{'MATCH' if size == want_size else 'MISMATCH'}")
            emit(f"      sha256    : {got}   "
                 f"{'MATCH' if got == want_sha else 'MISMATCH'}")
            if name.is_symlink():
                emit(f"      link size : {name.lstat().st_size} bytes "
                     f"(the link itself, not its target -- recorded only to discard it)")
            if size != want_size:
                fatal.append(f"{shown} is {size} bytes, recorded {want_size}")
            if got != want_sha:
                fatal.append(f"{shown} is sha256 {got}, recorded {want_sha}")
            emit()
        if len(resolved) == len(names):
            same = len({str(p) for p in resolved}) == 1
            emit(f"  All {len(names)} subject(s) resolve to one file: "
                 f"{'yes' if same else 'NO'}")
            if not same:
                fatal.append(
                    "the subjects resolve to different files, so the graph checked is "
                    "not the graph the runner would load"
                )
            emit()

    # One decision, three outcomes, so the printed verdict and the exit status cannot
    # drift apart: a record that could not be read is 78, a graph that does not match its
    # record is 77, and everything else is 0.
    if config_error is not None:
        verdict, status = "CONFIGURATION FAULT", CONFIG_EXIT
    elif fatal:
        verdict, status = "HALT", HALT_EXIT
    else:
        verdict, status = "PASS", 0

    emit("=" * 84)
    emit(f"VERDICT: {verdict}")
    emit("=" * 84)
    if fatal:
        emit()
        for item in fatal:
            emit("  -")
            emit_wrapped("    ", item)
        emit()
        emit("  The Stage 3 runner MUST NOT be invoked. Nothing was loaded.")
    elif config_error is not None:
        emit()
        emit("  -")
        emit_wrapped("    ", config_error)
        emit()
        emit("  The Stage 3 runner MUST NOT be invoked. Nothing was loaded.")
    emit()

    out = logs / "joern-preflight.log"
    if check_only:
        # The record may not exist yet -- an audit run in a clone where the gate has
        # never run must still report its verdict and return the documented status,
        # rather than dying on a stat of a file whose absence it was asked not to fix.
        state = (f"left untouched ({out.stat().st_size} B, unchanged)"
                 if out.is_file() else "not written and not created (it does not exist)")
        print(f"--check-only: {out} {state} so the run-of-record's ordering "
              f"evidence is not overwritten by an audit run")
    else:
        logs.mkdir(parents=True, exist_ok=True)
        out.write_text("\n".join(report).rstrip("\n") + "\n")
        print(f"wrote {out} ({out.stat().st_size} B)")
    return status


def _wrap(text: str, width: int) -> list[str]:
    """Wrap ``text`` to ``width`` on word boundaries, preserving every character.

    Hand-rolled rather than ``textwrap`` so a long unbroken token -- a digest, a path --
    is emitted whole on its own line instead of split at an arbitrary column, which would
    stop a reader copying it back out of the report. Identical to
    ``preflight_scan_target._wrap``: the two gates' reports are read together and must
    look the same.
    """
    words = text.split()
    if not words:
        return [""]
    lines: list[str] = []
    current = words[0]
    for word in words[1:]:
        if len(current) + 1 + len(word) <= width:
            current = f"{current} {word}"
        else:
            lines.append(current)
            current = word
    lines.append(current)
    return lines


if __name__ == "__main__":
    sys.exit(main())
