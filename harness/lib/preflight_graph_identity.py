#!/usr/bin/env python3
"""Pre-load graph gate: refuse to load a graph the records of account do not describe, and
refuse to load one whose method count is below the floor AAP 0.9.2 stops the run for.

WHY THIS EXISTS AS A SEPARATE, EXECUTABLE STEP
==============================================
AAP 0.8.2 requires the graph's identity re-verified immediately before every load, and
0.6.4 requires each check logged, because "a load against different bytes than the record
describes produces conclusions about a graph nobody has". AAP 0.9.2 additionally stops the
run on "a method count below 853,420", which is the truncation signature and which no
other executable in this harness checks.

The measurement and the comparison are two different acts, and the Stage 3 path performs
only the first:

* ``harness/bin/run-joern.sh`` resolved its input and PRINTED the path, the byte size and
  the digest -- and then invoked Joern. Printing a value is not checking it.
* ``harness/lib/joern-scan.sc`` calls ``importCpg`` and then counts. By the time any
  figure exists inside the script, the bytes are already in the engine.

This module is that comparison, kept as one executable step because it resolves the
record of account by provenance and returns a single adjudicated status a caller can
refuse on -- logic that belongs in one place read by every load path rather than
duplicated in each.

**WHO READS THIS STATUS, STATED AS IT IS RATHER THAN AS IT WOULD BE CONVENIENT.**
``harness/bin/run-joern.sh`` does NOT read it. The runner is provisioned and AAP 0.6.1
marks every ``harness/bin/`` entry REFERENCE, so it resolves its input, prints the path,
the byte size and the digest at its lines 112-113, and invokes Joern. Printing a value is
not checking it. An earlier generation of this run did edit the runner to call this gate;
that edit was a prohibited post-provisioning write to a REFERENCE file and has been
reverted, so the claim that "the canonical direct no-argument invocation is bound by this
gate structurally" is no longer true and is not made here. Closing the comparison inside
the runner is a PROVISIONING change, and the exact patch is recorded in
``oss-scan-results/run-record.md`` rather than applied from a clone.

What does bind it, and all that binds it:

* ``harness/lib/run-joern-gated.sh``, the committed gated path, which runs this module as
  step 2 of 4 and has no branch that reaches the runner after a non-zero status;
* the run of record, which runs this module and publishes its report to
  ``harness/artifacts/logs/joern-preflight.log`` immediately before the Stage 3 load, so
  the ordering is evidenced by that file's ``Checked at`` stamp rather than asserted.

A direct ``./harness/bin/run-joern.sh`` therefore remains unbound by this status. That
residual is a property of a provisioning a clone may not edit, and it is reported here and
in ``oss-scan-results/run-record.md`` rather than papered over. The EXIT STATUS section
below names every reader.

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

* the provisioning record beside the graph states 547,980,224 bytes /
  ``325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6``, which is what the
  bytes on disk measure;
* ``harness/ENVIRONMENT.md`` section 7 DECLARES the same 547,980,224 /
  ``325887cf6c65377b1c5b9c127b1ea16807463313e82baf14cabb0e5c5aba3dc6`` for the same
  ``/opt/blitzy-harness/cpg/spark.cpg`` path, having been re-anchored to this graph's
  write-time record of account on 2026-09-03;

so the records AGREE and this gate passes. It has not always: two superseded generations
of the graph are named here because a reader comparing an older report against this one
needs to know which bytes each describes, and because a record re-anchored to the wrong
generation is exactly what this candidate exists to catch.

* SUPERSEDED, 2026-09-01 generation: 541,309,809 /
  ``4616845ab2b0de2b8e7d43598de0e18c2302be233149b933af3098b0aa4730c7``.
* SUPERSEDED, 2026-08-24 generation: 541,255,894 /
  ``26d327ccee096aa4c8d67018b32669f2a318331cf873922286774734177fcffc``.

While section 7 still declared the 2026-08-24 pair against a disk holding the 2026-09-01
pair, this gate HALTED where before it had passed -- the correct outcome, and the reason
this candidate is read unconditionally. "Fail closed" means the gate declines to pick the
record that happens to match the bytes in front of it. AAP 0.1.3's fourth case is the
authority -- an observable fact contradicting the environment record on a field the
expected-values table does not anchor is recorded with BOTH values and the run stops.

Resolving such a disagreement is a PROVISIONING act, not something a clone may do: the
graph and its record must be replaced together so that one identity describes one set of
bytes. Neither ``harness/ENVIRONMENT.md`` (REFERENCE under AAP 0.6.1) nor the shared graph
may be edited from here, so this gate's job ends at refusing and saying exactly what
disagrees.

A section 7 that cannot be parsed is its own named CONFIGURATION FAULT (exit 78) rather
than a silent agreement: "the record could not be read" and "the record agrees" are
opposite facts, and a parser that returned the second for the first would be the one bug
that makes every later verdict meaningless.

Both values are recomputed from the bytes on disk and both must match. Size alone is a
weak test -- a graph rebuilt over a different input set can land on the same length --
and a digest alone leaves a truncated read undetected as a size discrepancy.

THE METHOD-COUNT FLOOR, THE SECOND QUESTION THIS GATE ANSWERS
=============================================================
Identity and completeness are different facts, and a graph can satisfy the first while
failing the second: a truncated graph persisted once has a perfectly consistent identity
in every record that describes it. AAP 0.9.2 therefore names "a method count below
853,420" among the conditions that STOP the run -- "fewer methods from more JARs is the
truncation signature, and a truncated graph's silence is indistinguishable from a clean
result" -- and AAP 0.9.1 requires the count reported against its expected value at the
verification load.

Before this check existed the floor was stated in seven result documents and enforced in
no code: ``853420`` occurred in zero executable files, so the one mandated halt whose
signature is silence was the one condition nothing could detect. It is enforced here, in
the gate that already adjudicates the graph's record of account, because the records that
state the identity are the records that state the count and reading them twice in two
places is how the two answers start to disagree.

``METHOD_COUNT_FLOOR`` is authored as a constant of this module for the same reason
``preflight_scan_target.PINNED_SPARK_COMMIT`` is: a threshold a caller can move is not a
threshold. The claim sources are exactly the three records of account named above, and
nothing else -- they have just been adjudicated to describe ONE set of bytes, so a count
any of them states is a count about the graph that is about to be loaded. A count from a
stage log describing a DIFFERENT identity is a count about a different graph;
``harness/artifacts/logs/cpg-verify.log`` is the live instance, recording 1,396,899
methods for the superseded 541,309,809-byte generation, and admitting it here would
manufacture a disagreement out of two correct measurements.

Four outcomes, each decided rather than deferred:

* AT OR ABOVE the floor: satisfied, and the report states it as ``1,398,964 >= 853,420``.
  The bound is ONE-SIDED. AAP 0.9.3 makes a count above the 898,336 anchor a RECORDED
  DIFFERENCE and never a halt -- the anchor was measured over 32 JAR producers where a
  full reactor contributes 38, and more JARs cannot yield fewer methods -- so no upper
  comparison is performed, and anyone tempted to "fix" this into a window would be adding
  a halt for succeeding.
* BELOW the floor: fatal, at the same ``HALT`` status an identity mismatch carries, naming
  the observed count, the floor and the shortfall. The input set is never trimmed to move
  a count.
* NO record states a count: fatal. An unestablished count is not a satisfied floor, and a
  gate that read silence as agreement would report the graph as checked while checking
  nothing.
* Two records state DIFFERENT counts: fatal, with every claim recorded and none preferred
  -- handled exactly like an identity disagreement, and for the same reason. Resolving it
  is a provisioning act: the graph and every record of its counts are replaced together.

All four are measured under ``--check-only`` as well; what that flag suppresses is the
write, never a measurement.

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

``--check-only`` performs every measurement -- the identity comparison and the
method-count floor alike -- and prints the whole report, but writes nothing. That is the
form every audit and every validation run must use: the durable record belongs to the run
of record, whose gate invocation precedes the Stage 3 load, and an audit run that replaced
it would destroy the very ordering it was checking. Use the default form only when a caller
genuinely wants that durable record replaced -- ``harness/lib/run-joern-gated.sh`` does,
at step 2 of 4, because for an invocation routed through it the gate's report IS that
invocation's ordering evidence. The negative test needs the writing form too, so it saves
the record first and restores it afterwards, asserting byte equality.

EXIT STATUS, AND THE CALLERS THAT MAKE IT BINDING
=================================================
``0``  every check passed; the caller may invoke the runner.
``77`` a fatal mismatch or a missing prerequisite -- including two records of account that
       disagree about the identity, a method count below AAP 0.9.2's floor, a method count
       no record establishes, and two records that disagree about the count. The caller
       MUST NOT invoke the runner. 77 rather than 1 so a caller cannot confuse it with an
       ordinary error, and distinct from the runners' own 64 (bad argument) so the two are
       never conflated in a log.
``78`` a CONFIGURATION FAULT: a record of account exists but could not be read, which is
       ``scope.sh``'s ``scope_fail`` status for the same class of condition. Today the one
       such condition is a ``harness/ENVIRONMENT.md`` whose section 7 is absent,
       unparsable or ambiguous. The caller MUST NOT invoke the runner either way; the
       distinct status exists because "correct the record" and "the graph is not the
       recorded graph" send a reader to different places. The floor's failures are
       deliberately NOT split across the two statuses: for a floor, "the record could not
       be read" and "the count is not established" are the same fact, and AAP 0.9.2 makes
       an unsatisfied floor a stop rather than a document to correct.

An exit status only binds something that reads it, so the readers are named rather than
assumed. There are two, and neither is the runner:

* ``harness/lib/run-joern-gated.sh``, a committed gated path that sources ``env.sh``, runs
  this module against the effective ``HARNESS_CPG`` as step 2 of 4, and reaches the runner
  only on 0 -- one route through it, no branch that invokes Joern after a non-zero gate.
* the run of record, which runs this module and publishes its report to
  ``harness/artifacts/logs/joern-preflight.log`` immediately before the Stage 3 load. That
  file's ``Checked at`` stamp is the ordering evidence, which is why an audit run must use
  ``--check-only``.

``harness/bin/run-joern.sh`` is not a reader. It recomputes and PRINTS its input's size and
digest at its lines 112-113 and then invokes Joern, and it is REFERENCE under AAP 0.6.1, so
the comparison cannot be moved inside it from a clone: that is a provisioning change, and
the patch is recorded in ``oss-scan-results/run-record.md``. A direct
``./harness/bin/run-joern.sh`` is consequently NOT bound by this status, which is the
residual a reader of this module needs to know about rather than discover.

The Stage 3 invocation on record took that direct route --
``argv=["./harness/bin/run-joern.sh"]`` at line 3 of
``harness/artifacts/logs/joern.runner-console.log`` -- so its contemporaneous identity
evidence is the runner's own recompute, printed rather than compared, and the comparison
for that load is this gate's separately published report.

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

#: AAP 0.9.2's method-count floor: "a method count below 853,420" is named among the
#: conditions that STOP the run, because "fewer methods from more JARs is the truncation
#: signature, and a truncated graph's silence is indistinguishable from a clean result".
#: Authored as a constant of THIS module rather than read from a document, for the same
#: reason PINNED_SPARK_COMMIT is authored in preflight_scan_target.py: a threshold a
#: caller can move is not a threshold. Seven result documents state this number in prose
#: and none of them is executable; this is where it is enforced.
METHOD_COUNT_FLOOR: Final[int] = 853_420

#: The measured anchor the floor is derived from (AAP 0.2.1: 853,420 is the 5% lower bound
#: around 898,336). Carried so the report can say what the floor MEANS, and so nobody
#: later reads the pair as a window: the bound is ONE-SIDED. AAP 0.9.3 makes a count above
#: the anchor a RECORDED DIFFERENCE and never a halt -- the anchor was measured over 32
#: JAR producers and a full reactor contributes 38, and more JARs cannot yield fewer
#: methods -- so no upper comparison is performed here and adding one would halt the run
#: for succeeding.
METHOD_COUNT_ANCHOR: Final[int] = 898_336


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
    so ``| **Bytes** | **547,980,224** |`` yields ``['Bytes', '547,980,224']``. Anything
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


class MethodCountClaim(NamedTuple):
    """One record's stated method count for the graph, and where the statement came from.

    ``shown`` is the display form of ``source`` -- repository-relative where the record is
    inside the checkout, absolute where it is provisioned shared state -- so the report
    names a path a reader can open.
    """

    count: int
    source: Path
    shown: str
    provenance: str
    evidence: str


class MethodFloorResult(NamedTuple):
    """The whole outcome of the method-count floor check, ready to be reported.

    ``fatal`` empty means the floor is satisfied by an established, agreed count.
    ``count`` is set only when exactly one distinct count was established, so a caller
    can never print a number the records did not agree on.
    """

    claims: tuple[MethodCountClaim, ...]
    silent: tuple[tuple[str, str], ...]
    count: int | None
    fatal: tuple[str, ...]


def _method_count_in_text(record: Path, shown: str, text: str) -> tuple[int, str] | None:
    """Return the one method count a plain-text record of account states, or ``None``.

    Two strict shapes are accepted, both line-anchored on a LABEL so that a number in
    prose can never be read as a count:

    * ``METHODS <n>`` as the provisioning record writes it -- ``importCpg verify: METHODS
      1398964 (internal 1308974), TYPEDECLS 119860, FILES 45037``;
    * a labelled ``methods: <n>`` / ``methods = <n>`` line, which is the machine-readable
      form a frontend or verification log would use.

    ``None`` means the record states no count. That is a FACT about the record rather than
    a failure -- this checkout's ``cpg-frontend.log`` is the record of a frontend that
    persisted nothing, so it owns no count to state -- and the caller reports it as a
    silent record. Raises ``ValueError`` when one record states more than one distinct
    count: an ambiguous record cannot adjudicate a floor, exactly as it cannot adjudicate
    an identity.
    """
    counts = {
        int(value.replace(",", ""))
        for value in re.findall(r"\bMETHODS[ \t]+([\d,]+)\b", text)
    }
    counts |= {
        int(value.replace(",", ""))
        for value in re.findall(r"^[ \t]*methods[ \t]*[:=][ \t]*([\d,]+)\b", text, re.M | re.I)
    }
    if not counts:
        return None
    if len(counts) != 1:
        raise ValueError(
            f"{shown} states {len(counts)} distinct method counts "
            f"({', '.join(format(value, ',') for value in sorted(counts))}), so it cannot "
            f"adjudicate the AAP 0.9.2 floor. An ambiguous record would let a reader pick "
            f"the count that clears the floor and call it a check."
        )
    count = counts.pop()
    return count, f"{count:,} methods, read from {record.name}"


def declared_method_count(record: Path) -> tuple[int, str] | None:
    """Return the method count the environment record DECLARES, or ``None``.

    Read from the table row of section 7 whose first cell is ``Methods``, the row that
    sits beside the ``Bytes`` and ``sha256`` rows ``declared_identity`` reads -- so the
    declared count and the declared identity are statements about the same graph, which is
    what makes them comparable with the provisioning record's pair.

    The value cell carries the internal-method figure in parentheses after the total
    (``**1,398,964** (internal 1,308,974)``), so the TOTAL is taken from the head of the
    cell and the parenthetical is left alone: AAP 0.9.2's floor is stated against the
    method count, not against the internal subset, and reading the smaller number would
    fail a graph that satisfies the requirement.

    Raises ``ConfigurationFault`` when section 7 cannot be located at all -- the same
    condition, with the same status, that ``declared_identity`` raises it for -- and
    ``ValueError`` when the section carries more than one distinct ``Methods`` row or a
    row whose value is not a decimal count.
    """
    section = graph_section(record)
    counts: set[int] = set()
    for line in section:
        cells = _table_cells(line)
        if len(cells) < 2:
            continue
        if cells[0].lower().rstrip(":").strip() != "methods":
            continue
        match = re.match(r"\**[ \t]*([\d,]+)", cells[1])
        if not match:
            raise ValueError(
                f"{ENVIRONMENT_RECORD} section {GRAPH_SECTION_NUMBER} has a 'Methods' row "
                f"whose value does not begin with a decimal count: {cells[1]!r}. A count "
                f"that cannot be read is not a count that clears the floor."
            )
        counts.add(int(match.group(1).replace(",", "")))
    if not counts:
        return None
    if len(counts) != 1:
        raise ValueError(
            f"{ENVIRONMENT_RECORD} section {GRAPH_SECTION_NUMBER} declares "
            f"{len(counts)} distinct method counts "
            f"({', '.join(format(value, ',') for value in sorted(counts))}); an ambiguous "
            f"declaration cannot adjudicate the AAP 0.9.2 floor."
        )
    count = counts.pop()
    return count, (f"{count:,} methods, declared by {ENVIRONMENT_RECORD} section "
                   f"{GRAPH_SECTION_NUMBER}")


def method_floor(root: Path, graph: Path) -> MethodFloorResult:
    """Adjudicate the graph's method count against AAP 0.9.2's floor.

    The claim sources are EXACTLY the records of account ``record_of_account`` resolves --
    this checkout's frontend log, the provisioning records beside the resolved graph, and
    the environment record's section 7 -- and nothing else. That restriction is the point
    rather than an economy: those three have just been adjudicated to describe ONE set of
    bytes, so a count any of them states is a count about the graph that is about to be
    loaded. A count taken from a stage log describing a DIFFERENT identity would be a
    count about a different graph, and comparing it here would manufacture a disagreement
    out of two correct measurements. ``harness/artifacts/logs/cpg-verify.log`` is the live
    instance: it records 1,396,899 methods for the superseded 541,309,809-byte generation.

    Four outcomes, and every one of them is decided here rather than by the caller:

    * at or above the floor -- the floor is satisfied, and the report says so with both
      numbers and with AAP 0.9.3's rule that a count above the anchor is a recorded
      difference and never a halt;
    * below the floor -- FATAL, naming the observed count and the floor. This is the
      truncation signature AAP 0.9.2 stops the run for;
    * no record states a count -- FATAL. An unestablished count is not a satisfied floor,
      and a gate that treated silence as agreement would report the graph as checked while
      checking nothing;
    * two records state DIFFERENT counts -- FATAL, with every claim recorded and none
      preferred, exactly as a disagreement between two identity records is fatal. The
      resolution is a provisioning act: the graph and its records are replaced together.

    Every failure mode returns a message rather than raising, so the caller can report all
    of them in one pass and map them onto the single HALT status.
    """
    claims: list[MethodCountClaim] = []
    silent: list[tuple[str, str]] = []
    fatal: list[str] = []

    def shown_for(path: Path) -> str:
        try:
            return str(path.relative_to(root))
        except ValueError:
            return str(path)

    plain: list[tuple[Path, str]] = [
        (root / "harness/artifacts/logs/cpg-frontend.log",
         "write-time record: this checkout's frontend"),
    ]
    plain.extend(
        (record, "provisioning record of account beside the resolved graph")
        for record in provisioning_records(graph)
    )
    for record, provenance in plain:
        shown = shown_for(record)
        if not record.is_file():
            silent.append((shown, "the record is absent, so it states no method count"))
            continue
        try:
            found = _method_count_in_text(record, shown, record.read_text(errors="replace"))
        except (ValueError, OSError) as exc:
            fatal.append(str(exc))
            continue
        if found is None:
            silent.append((
                shown,
                "the record states no method count in either accepted form "
                "('METHODS <n>' or a labelled 'methods:' line), which is a fact about the "
                "record rather than a failure",
            ))
            continue
        count, evidence = found
        claims.append(MethodCountClaim(count, record, shown, provenance, evidence))

    environment = root / ENVIRONMENT_RECORD
    shown = shown_for(environment)
    declared: tuple[int, str] | None = None
    # Tracked separately from `fatal`, which by this point may already hold a failure from
    # a plain-text record: "the declaration could not be read" and "the declaration is
    # absent" are different facts about this record, and only the second is silence.
    environment_unreadable = False
    try:
        declared = declared_method_count(environment)
    except ConfigurationFault as exc:
        # Reported as fatal rather than re-raised: the identity half of this gate already
        # maps an unreadable section 7 onto CONFIG_EXIT, and duplicating that decision
        # here could only make the two halves disagree about the status.
        fatal.append(str(exc))
        environment_unreadable = True
    except (ValueError, OSError) as exc:
        fatal.append(str(exc))
        environment_unreadable = True
    if declared is not None:
        count, evidence = declared
        claims.append(MethodCountClaim(
            count, environment, shown,
            f"declared record: {ENVIRONMENT_RECORD} section {GRAPH_SECTION_NUMBER}, the "
            f"authoritative environment record",
            evidence))
    elif not environment_unreadable:
        silent.append((shown, f"section {GRAPH_SECTION_NUMBER} declares no 'Methods' row"))

    distinct = {claim.count for claim in claims}
    if not claims and not fatal:
        fatal.append(
            "no record of account states a method count for the graph, so AAP 0.9.2's "
            f"floor of {METHOD_COUNT_FLOOR:,} methods could not be established. An "
            "unestablished count is not a satisfied floor: a truncated graph answers every "
            "query with silence, and silence is indistinguishable from a clean result. The "
            f"records read were: "
            + "; ".join(f"{path} ({reason})" for path, reason in silent)
        )
    elif len(distinct) > 1:
        fatal.append(
            "the records of account state DIFFERENT method counts, so no count can be "
            "adjudicated against AAP 0.9.2's floor and nothing may be loaded. "
            + "; ".join(
                f"{claim.shown} ({claim.provenance}) states {claim.count:,} methods"
                for claim in claims)
            + ". No claim is preferred for clearing the floor -- a check that picks the "
              "number it likes is not a check -- and the disagreement itself means one "
              "record describes a graph that is not the graph on disk. Resolving it is a "
              "PROVISIONING act: replace the graph and every record of its counts "
              "together, so one set of counts describes one set of bytes."
        )
    count = distinct.pop() if len(distinct) == 1 else None
    if count is not None and count < METHOD_COUNT_FLOOR:
        fatal.append(
            f"the graph states {count:,} methods, below AAP 0.9.2's floor of "
            f"{METHOD_COUNT_FLOOR:,} ({count:,} < {METHOD_COUNT_FLOOR:,}, short by "
            f"{METHOD_COUNT_FLOOR - count:,}). AAP 0.9.2 lists a method count below "
            f"{METHOD_COUNT_FLOOR:,} among the conditions that stop the run: fewer methods "
            f"from more JARs is the truncation signature, and a truncated graph's silence "
            f"is indistinguishable from a clean result. The floor is the 5% lower bound "
            f"around the {METHOD_COUNT_ANCHOR:,}-method anchor (AAP 0.2.1) and it is "
            f"one-sided, so this is a shortfall and never a window violation. The input "
            f"set is NEVER trimmed to move a count: a graph this small was built over less "
            f"than the complete input manifest, or persisted less than it built."
        )
    return MethodFloorResult(tuple(claims), tuple(silent), count, tuple(fatal))


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
    # The method-count floor is measured unconditionally and never raises: it asks a
    # different question from the identity comparison -- "is this graph whole?" rather
    # than "is this the recorded graph?" -- and a run whose identity record is unreadable
    # still needs the floor's verdict reported rather than skipped.
    floor = method_floor(root, effective_harness_cpg(root))

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
    emit("  are fatal and none is preferred for matching. It also adjudicates the graph's")
    emit(f"  METHOD COUNT against AAP 0.9.2's floor of {METHOD_COUNT_FLOOR:,}, because")
    emit("  identity and completeness are different facts: a truncated graph persisted")
    emit("  once has a consistent identity in every record that describes it. The runner")
    emit("  MEASURES and PRINTS its input's size and digest, and harness/lib/joern-scan.sc")
    emit("  calls importCpg and then counts, so without this comparison a mismatch or a")
    emit("  truncation would reach the engine.")
    emit()
    emit(f"  Any fatal finding here exits {HALT_EXIT}. The callers that refuse on it are")
    emit("  harness/lib/run-joern-gated.sh, which has no branch reaching the runner after")
    emit("  a non-zero gate, and the run of record, which publishes this report to")
    emit("  harness/artifacts/logs/joern-preflight.log immediately before the Stage 3")
    emit("  load. harness/bin/run-joern.sh does NOT read it: it prints its input's")
    emit("  identity without comparing it, and it is REFERENCE under AAP 0.6.1, so closing")
    emit("  that inside the runner is a provisioning change whose patch is recorded in")
    emit("  oss-scan-results/run-record.md. A direct invocation of that runner is")
    emit("  therefore not bound by this status.")
    emit()
    emit(f"  Gate source             : harness/lib/preflight_graph_identity.py")
    emit(f"  Binding callers         : harness/lib/run-joern-gated.sh (step 2 of 4)")
    emit(f"                            the run of record, publishing to")
    emit(f"                            harness/artifacts/logs/joern-preflight.log before")
    emit(f"                            the Stage 3 load")
    emit(f"  NOT a caller            : harness/bin/run-joern.sh (REFERENCE, AAP 0.6.1")
    emit(f"                            -- it prints its input's identity without")
    emit(f"                            comparing it)")
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
    emit(f"  Method-count floor      : {METHOD_COUNT_FLOOR:,} methods (AAP 0.9.2, "
         f"authored in this module)")
    emit()

    if config_error is not None:
        # Deliberately NOT appended to `fatal`: a record that cannot be read is a
        # different outcome from a graph that does not match its record, and the two
        # carry different exit statuses. No subject is measured, because there is nothing
        # to measure against.
        emit("  CONFIGURATION FAULT:")
        emit_wrapped("    ", config_error)
        emit()
        emit("  No subject was measured against a recorded identity pair, and nothing may")
        emit(f"  be loaded. Correct the record ({ENVIRONMENT_RECORD} section")
        emit(f"  {GRAPH_SECTION_NUMBER}) and re-run; this gate does not repair a document")
        emit("  it is required to read. The method-count floor below is reported anyway --")
        emit("  it reads the same records for a different figure, and an unreadable")
        emit("  identity does not excuse an unchecked floor.")
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

    # ---------------------------------------------------------------- method-count floor
    # A second, independent question about the same graph: the identity comparison above
    # establishes that these are the RECORDED bytes, and this establishes that the graph
    # those bytes hold is WHOLE. Neither substitutes for the other -- a truncated graph
    # persisted once has a perfectly consistent identity in every record that describes
    # it -- so both are measured on every load.
    emit("  METHOD-COUNT FLOOR (AAP 0.9.2), adjudicated from the same records of account:")
    emit()
    emit(f"    Floor                 : {METHOD_COUNT_FLOOR:,} methods -- the 5% lower "
         f"bound around")
    emit(f"                            AAP 0.2.1's {METHOD_COUNT_ANCHOR:,}-method anchor, "
         f"and ONE-SIDED:")
    emit("                            AAP 0.9.3 makes a count ABOVE the anchor a recorded")
    emit("                            difference and never a halt, because the anchor was")
    emit("                            measured over 32 JAR producers where a full reactor")
    emit("                            contributes 38, and more JARs cannot yield fewer")
    emit("                            methods. No upper comparison is performed, and")
    emit("                            adding one would halt the run for succeeding.")
    for claim in floor.claims:
        emit(f"    Claim                 : {claim.count:,} methods")
        emit(f"      stated by           : {claim.shown}")
        emit(f"      its provenance      : {claim.provenance}")
        emit(f"      evidence            : {claim.evidence}")
    for path, reason in floor.silent:
        emit(f"    States no count       : {path}")
        for line_index, line in enumerate(_wrap(reason, 56)):
            emit("      "
                 + ("reason              : " if line_index == 0 else " " * 22)
                 + line)
    if floor.fatal:
        for item in floor.fatal:
            emit("    FATAL:")
            emit_wrapped("      ", item)
        fatal.extend(floor.fatal)
    elif floor.count is not None:
        emit(f"    Adjudicated count     : {floor.count:,} methods, agreed by "
             f"{len(floor.claims)} record(s) of account")
        emit(f"    Verdict               : FLOOR SATISFIED -- {floor.count:,} >= "
             f"{METHOD_COUNT_FLOOR:,}")
        if floor.count > METHOD_COUNT_ANCHOR:
            emit(f"                            {floor.count:,} also exceeds the "
                 f"{METHOD_COUNT_ANCHOR:,} anchor, which")
            emit("                            AAP 0.9.3 RECORDS as a difference rather than")
            emit("                            halting on. This is not a window and must not")
            emit("                            be turned into one.")
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
