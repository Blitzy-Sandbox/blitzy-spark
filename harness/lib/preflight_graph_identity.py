#!/usr/bin/env python3
"""Pre-load graph-identity gate: refuse to load a graph the write-time record does not describe.

WHY THIS EXISTS AS A SEPARATE, EXECUTABLE STEP
==============================================
AAP 0.8.2 requires the graph's identity re-verified immediately before every load, and
0.6.4 requires each check logged, because "a load against different bytes than the record
describes produces conclusions about a graph nobody has".

Two things in the Stage 3 path make that impossible to satisfy from inside the runner:

* ``harness/bin/run-joern.sh`` resolves its input and PRINTS the path, the byte size and
  the digest -- and then invokes Joern. It never COMPARES any of it against the
  write-time pair. Printing a value is not checking it.
* ``harness/lib/joern-scan.sc`` calls ``importCpg`` and then counts. By the time any
  figure exists, the bytes are already in the engine.

Neither file may be edited: AAP 0.8.1 forbids changing a runner or a baked flag, and
0.3.2 forbids runner reconfiguration. So the check cannot live inside the thing it
guards. It lives here instead, and **for an invocation routed through a caller that
reads this exit status** the guarantee is structural rather than advisory: this module
exits non-zero before that caller reaches the runner, so a mismatch cannot reach the
engine. An invocation that does not read the status is not bound by it -- the EXIT STATUS
section below names the committed gated caller and says which route the Stage 3
invocation on record actually took.

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

Every candidate that exists is read, and any disagreement between them is FATAL -- as is
more than one distinct pair inside any single record. The precedence orders provenance
(who wrote the bytes), so it cannot be used to select an outcome: if the two records
disagree, no pair is chosen and nothing is loaded.

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
nothing. Use it to verify the gate; use the default form only when actually gating an
invocation. The negative test needs the writing form, so it saves that record first and
restores it afterwards, asserting byte equality.

EXIT STATUS, AND THE CALLER THAT MAKES IT BINDING
=================================================
``0``  every check passed; the caller may invoke the runner.
``77`` a fatal mismatch or a missing prerequisite. The caller MUST NOT invoke the
       runner. 77 rather than 1 so a caller cannot confuse it with an ordinary error,
       and distinct from the runners' own 64 (bad argument) and 78 (configuration
       fault) so the three are never conflated in a log.

An exit status only binds something that reads it, so a caller is committed alongside it:
``harness/lib/run-joern-gated.sh`` is a committed gated path for Stage 3. It sources
``env.sh``, runs this module against the effective ``HARNESS_CPG``, and reaches the
runner only on 0 -- there is one route through it to the runner and no branch that
invokes Joern after a non-zero gate. That makes the guarantee structural **for an
invocation routed through that wrapper**, rather than a convention a future caller could
forget.

It does not make the wrapper the only route. ``harness/bin/run-joern.sh`` is executable in
its own right and AAP 0.8.1 requires each runner invoked directly with no arguments, so
Stage 3 can be -- and for the invocation on record was -- started without the wrapper, in
which case this exit status is never read and this gate does not bind that load. The
wrapper's own header records which route the delivered Stage 3 invocation took and names
the contemporaneous identity evidence for it.

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
#: runners' 64/78 or with an ordinary non-zero exit.
HALT_EXIT: Final[int] = 77

#: Read in 1 MiB blocks: the graph is hundreds of megabytes and reading it whole to
#: hash it would hold all of it in memory for no benefit.
_BLOCK: Final[int] = 1 << 20


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
    provisioning record beside the resolved graph governs. Every candidate that exists is
    read and any disagreement is fatal, so the order selects a WRITER, never an outcome.
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
    if not candidates:
        raise ValueError(
            f"no record of account carries an identity pair for {graph}: "
            f"{frontend} records no accepted graph (see run-record.md D1) and no "
            f"provisioning record was found beside the resolved graph at "
            f"{', '.join(str(r) for r in provisioning_records(graph)) or '<none>'}"
        )
    distinct = {pair for _, _, pair in candidates}
    if len(distinct) != 1:
        detail = "; ".join(f"{path} says {pair[0]} / {pair[1]}" for path, _, pair in candidates)
        raise ValueError(
            "the records of account disagree, so no identity can be adjudicated and "
            f"nothing may be loaded: {detail}"
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
    try:
        record = record_of_account(root, effective_harness_cpg(root))
    except (ValueError, OSError) as exc:
        record_error = str(exc)

    report: list[str] = []
    fatal: list[str] = []

    def emit(text: str = "") -> None:
        report.append(text)
        print(text)

    emit("=" * 84)
    emit("STAGE 3 PRE-LOAD GRAPH IDENTITY CHECK")
    emit("=" * 84)
    emit()
    emit("  Refuses to invoke the Stage 3 Joern runner unless the graph on disk is")
    emit("  byte-for-byte the graph its record of account describes -- the frontend log")
    emit("  when this checkout's frontend wrote one, and otherwise the provisioning record")
    emit("  beside the graph itself. The runner resolves and PRINTS its")
    emit("  input without comparing it, and harness/lib/joern-scan.sc calls importCpg and")
    emit("  then counts, so a mismatch would otherwise reach the engine. A mismatch here")
    emit(f"  exits {HALT_EXIT}, and harness/lib/run-joern-gated.sh -- a committed gated")
    emit("  path for Stage 3 -- has no branch that reaches the runner after a non-zero")
    emit("  gate, so an invocation routed through that wrapper never reaches the load.")
    emit("  The runner is also invocable directly, and an invocation that does not read")
    emit("  this exit status is not bound by it.")
    emit()
    emit(f"  Gate source             : harness/lib/preflight_graph_identity.py")
    emit(f"  Binding caller          : harness/lib/run-joern-gated.sh")
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
    else:
        emit(f"  Record of account       : NONE RESOLVED")
    written_here = root / "harness/artifacts/cpg/spark.cpg"
    if not (written_here.exists() or written_here.is_symlink()):
        emit("  In-checkout write path  : harness/artifacts/cpg/spark.cpg is absent -- no")
        emit("                            graph was written inside this checkout, so the two")
        emit("                            AAP 0.6.4 names are the subjects that decide the load")
    emit(f"  HARNESS_CPG in env      : "
         f"{os.environ.get('HARNESS_CPG', '<unset -- env.sh default applies>')}")
    emit()

    if record is None:
        fatal.append(record_error or "no record of account could be resolved")
        emit(f"  FATAL: {fatal[-1]}")
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

    emit("=" * 84)
    emit(f"VERDICT: {'PASS' if not fatal else 'HALT'}")
    emit("=" * 84)
    if fatal:
        emit()
        for item in fatal:
            emit(f"  - {item}")
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
    return HALT_EXIT if fatal else 0


if __name__ == "__main__":
    sys.exit(main())
