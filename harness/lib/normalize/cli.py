"""harness/lib/normalize/cli.py -- the normalizer's entry point and its only composer.

Purpose, verbatim from AAP 0.6.1: *"Entry point: read the runner metadata and the raw
directory, route each artifact, build rows, reconcile, write both files, typed re-parse,
publish."*

This is the **only** module in ``harness/lib/normalize/`` that couples the others.
AAP 0.6.4: *"``cli.py`` composes the other five modules and the adapters."*  Every
sibling is a leaf or near-leaf -- ``shape`` names an adapter by string key and imports
none, ``reconcile`` imports nothing from this package at all, ``emit`` imports nothing
either, and each adapter depends on ``paths`` and ``severity`` and on nothing else.  The
coupling lives here deliberately, so the adapters and ``reconcile`` stay independently
testable from ``oss-scan-results/adapter-tests/``.

COMPOSITION ORDER -- the order this module runs, and the reason for it
---------------------------------------------------------------------
1.  **Vocabulary invariants.**  The four canonical-tool tuples (``shape``, ``paths``,
    ``reconcile``, ``severity``) are asserted to describe one set of nine identifiers,
    and ``emit.FIELDS`` to be the twelve fields.  A disagreement there would be a
    programming fault that corrupts every count downstream, so it is settled first.
2.  **Runner metadata** (``paths.load_runner_metadata``).  The direction AAP 0.6.4 fixes:
    Stage 1 writes ``runner-metadata.json``, the normalizer reads it as *input*, and
    ``oss-scan-results/tool-status.md`` is rendered *afterwards* from that metadata joined
    with these results.  *"The Markdown is an output of the pipeline, never an input to
    it."*  **This module never reads ``tool-status.md``**, and nothing here writes any
    Markdown.
3.  **The scan root.**  Taken from the argument, checked against the root the metadata
    records, and required absolute: every path in the dataset is expressed against it, so
    a wrong root makes every row wrong in the same direction -- far harder to notice than
    an error here.
4.  **The allowlist** (``paths.load_allowlist``).  This module owns the check that the
    file still holds the twelve authoritative globs, byte-exact and in order
    (``paths.allowlist_matches_authoritative_globs``).  The globs are passed to every
    adapter, which evaluates the ``in_scope`` predicate through ``paths``; the file is
    never rewritten, widened or narrowed here (AAP 0.8.2).
5.  **The raw directory.**  Enumerated, bounded to the nine fixed artifact filenames, and
    asserted to be the only tree read (see the boundary section below).
6.  **Per artifact, in one fixed order:** parse the JSON, ``shape.route_artifact`` it,
    take the independent record count with ``reconcile.count_records``, resolve the
    adapter through the registry defined *here*, and call ``adapter.adapt(...)``.
7.  **Reconciliation, stages A and B, before anything is written.**  A dataset whose
    identity already fails is never written to disk.
8.  **``emit.publish_findings``** -- both files rendered from the same validated rows,
    staged in their target directories, read back and compared field for field under
    typed coercion, and only then moved into place.  The two deliverables are one
    generation of one row list: they are published together or not at all, and both
    carry one content-derived publication identifier recorded in the run record.
9.  **Reconciliation stage C** -- the parsed ``findings.json`` and parsed ``findings.csv``
    row counts against the stage B identity, separately and against each other.
10. **``harness/artifacts/logs/normalize-run.json``** -- written on every path out of this
    module, including every halt, and published through ``emit.publish_document`` under
    the same validated-directory, exclusive-no-follow, fsync-and-rename protocol as the
    dataset, so no reader ever sees a truncated record and no symlink at either path can
    redirect one.

THE CLI CONTRACT
----------------
Every input is an explicit argument and no repository-relative path is hardcoded.  Defaults
come only from the environment ``harness/env.sh`` exports -- ``HARNESS_RAW_DIR``,
``HARNESS_LOG_DIR``, ``HARNESS_SCOPE_FILE``, ``HARNESS_REPO_ROOT`` and ``SPARK_SRC`` -- and
an explicit argument always wins.  Nothing is read at import time: no file, no environment
variable, no filesystem probe.  A required input that can be neither supplied nor defaulted
is a configuration fault naming the flag and the variable that would have supplied it.

    --raw-dir           the runner-only artifact tree            (HARNESS_RAW_DIR)
    --runner-metadata   runner-metadata.json                     ($HARNESS_LOG_DIR/...)
    --allowlist         the twelve authoritative globs           (HARNESS_SCOPE_FILE)
    --log-dir           the per-tool streams and status files    (HARNESS_LOG_DIR)
    --spark-src         the pinned clone every path is relative to (SPARK_SRC)
    --findings-json     oss-scan-results/findings.json           ($HARNESS_REPO_ROOT/...)
    --findings-csv      oss-scan-results/findings.csv            ($HARNESS_REPO_ROOT/...)
    --repo-root         the root that owns those two files       (HARNESS_REPO_ROOT)
    --run-record        normalize-run.json                       ($HARNESS_LOG_DIR/...)

The exact command as invoked, the interpreter's absolute path and its reported version are
recorded (AAP 0.6.1).  The version is compared against the expected ``3.13.7`` (AAP 0.4.1)
and any difference -- major, minor or patch -- is **recorded with both values while the run
continues**.  It is never a halt.

WHERE AN OUTPUT MAY BE WRITTEN -- three rules, all of them halts
---------------------------------------------------------------
The three configured outputs are the only files this module writes, and each has exactly
one owner root.  ``findings.json`` and ``findings.csv`` must resolve inside the repository
root the run declares (``--repo-root``, then ``$HARNESS_REPO_ROOT``, then this module's own
repository); ``normalize-run.json`` must resolve inside the log tree.  A path outside its
owner is a configuration fault naming the path and the root (CWE-73), not a location.  The
three must be three distinct files and none of them may name an input -- the raw directory,
any artifact in it, the allowlist or ``runner-metadata.json`` -- because an output written
over an input destroys the evidence the dataset was derived from, and two outputs at one
path make the second write destroy the first while every count still reconciles.  Finally
the target and every component at or below its owner root are checked with ``lstat``, and a
symbolic link anywhere among them is refused with the component named (CWE-59).

Every write then goes through ONE discipline in ``emit.py``, which this module uses rather
than copies -- the dataset's two members and ``normalize-run.json`` alike: the parent is
walked one component at a time with ``O_NOFOLLOW`` and held open as a descriptor, the
staged file is created in it with ``O_CREAT|O_EXCL|O_WRONLY|O_NOFOLLOW`` under an
unguessable name, its mode is assigned with ``fchmod``, the bytes are fsynced, the file is
measured through a descriptor bound to the inode they were written into, and publication is
one ``renameat`` against the held descriptor followed by an fsync of the directory.  Every
member is staged and validated before the first rename, so a fault anywhere before it
leaves every previously published file exactly as it was, and no staged file survives to be
mistaken for a deliverable.  There is no working-directory fallback for the run record's
location: two sources supply it, ``--run-record`` and ``$HARNESS_LOG_DIR``, and its absence
is a configuration fault rather than a record written wherever the run was started from.

REQUIRED EVIDENCE FAILS CLOSED
------------------------------
``harness/artifacts/logs/normalize-run.json`` is written on every path out of this module,
**and a run that could not write and verify it never reports success**.  The record is
staged, read back from disk and parsed as a JSON object before anything is renamed, then
published by that one rename, then read back AGAIN through a descriptor bound to the inode
the publication verified and parsed once more -- the first read establishes that what is
about to be published is a document, the second that the document now at the published path
is that same file rather than whatever the pathname resolves to afterwards (CWE-367).  Where
any of that fails the diagnostic goes to stderr and the failure becomes the process's
outcome with a non-zero exit code (CWE-703).  A dataset whose run record was lost is a
dataset nobody can trace, so the honest outcome is the loss rather than the dataset.

READ ONLY FROM THE RAW DIRECTORY -- an asserted boundary, not an assumption
--------------------------------------------------------------------------
AAP 0.8.1: ``harness/artifacts/raw/`` stays runner-only, *"receiving exactly one artifact
per tool that writes one and nothing else ever"*.  Two tools appear twice in the run **by
design** (AAP 0.1.3): Opengrep is also the taint A/B subject, whose arms write
``harness/artifacts/logs/taint-ab-{on,off}.{sarif,log}``, and Joern is also the
capability-probe subject, whose results land under ``queries/joern/results/``.  Both second
appearances *"write outside ``harness/artifacts/raw/`` and contribute no dataset row"*, and
*"reading the double appearance as a duplication would corrupt both counts."*

The taint A/B arms are valid SARIF and would route perfectly, which is why the boundary is
enforced rather than trusted: only the nine fixed filenames are read, only as direct
children, and each one's real path must still sit inside the tree.  A symlink to a log-tree
file halts; so does an unexpected direct child, whose two conditions and filesystem-level
evidence :func:`_enumerate_raw_directory` states -- a dataset reconciling while an
unadapted input sits beside the ones its rows came from is the one failure reconciliation
cannot catch, and no document there is opened or fingerprinted to identify a writer.

THE FOUR PARSE STATUSES (AAP 0.5.4)
-----------------------------------
``clean``    artifact present, every record parsed.  The exit code is recorded as a fact
             and used for nothing else.
``partial``  artifact present, some records rejected.  Every parsable record is still
             emitted, every rejection counted under its named class, the parser error
             retained verbatim.
``failed``   artifact present but matching no known shape -- **the run halts**, with the
             observed structure quoted in the run record.
``absent``   artifact absent **and the tool stated a no-work reason in its own output** --
             that output is quoted verbatim, the tool contributes zero rows and the run
             continues.  OSV-Scanner's exit 128 with *"No package sources found, --help
             for usage information."* is the expected instance, and it is recognised by
             matching that exact sentence: "stated a reason" is decided against the
             per-tool table in :data:`_NO_WORK_STATEMENTS`, never by the streams being
             non-empty, because a stack trace and a permission error are output rather
             than an account of a tool having nothing to do.  Eight of the nine tools
             have no such statement at all and are expected to write an artifact.  An
             artifact absent with **no** words, or with words matching no recognised
             statement, halts -- under two separately named reasons -- and
             ``exit_status: timeout`` names how a process ended while *"it does not
             excuse a missing artifact"*.

Artifact status and exit status are independent.  A valid artifact is never suppressed
because its runner exited non-zero, and two of the nine exit non-zero precisely because
they found something -- Gitleaks ``2`` for a leak, Checkov ``1`` for a failed check.  A
Joern runner that died at its graph guard exits ``78`` with a diagnostic naming the missing
graph; AAP 0.2.3 classifies that as *"a configuration fault to correct at the gate, not an
unexplained missing artifact"*, so its stderr is carried through rather than quietly
classified as a scanning outcome.

DETERMINISTIC ORDER
-------------------
Artifacts are processed in ``shape.CANONICAL_TOOLS`` order -- ``opengrep``, ``semgrep``,
``datadog-static-analyzer``, ``gitleaks``, ``checkov``, ``trivy``, ``osv-scanner``,
``dependency-check``, ``joern`` -- and each artifact's records keep the order its adapter
returned them in, which is document order.  That single sequence is what **both** output
files carry, so two runs over identical artifacts produce byte-identical files.  Nothing is
sorted, grouped, ranked or deduplicated (AAP 0.3.2): two tools reporting the same location
produce two rows and no comment.

WHAT THIS MODULE NEVER DOES
---------------------------
It invokes no runner and uses no orchestrator: AAP 0.8.1 requires each runner invoked
directly, individually, with no arguments, one at a time, and an orchestrator's
continue-on-error sequencing *"would carry the run past a condition that must halt it"*.
This module reads artifacts that already exist.  It writes no Markdown, judges no finding,
compares no tool against another, deduplicates nothing, and repairs no count to make an
identity hold.

UNTRUSTED VERBATIM TEXT: A RESIDUAL RISK DISCLOSED AND MEASURED, NOT REMOVED
(CWE-116, CWE-117)
---------------------------------------------------------------------------
``normalize-run.json`` embeds text this pipeline did not author and is **required** not to
rewrite: a runner's own stream words, its own ``.status`` lines, and the ``stated_reason``
projected from them.  AAP 0.5.4 requires the parser error *"retained verbatim"* and settles
the absent-artifact verdict *"using only the tool's own stated words"*; AAP 0.6.1 and 0.6.2
require an absent artifact's stderr *"verbatim"* and a reduced-reach condition *"in the
tool's own words"*.  A rewritten byte is a rewritten verdict, so those bytes stay exactly as
the tool wrote them.

That leaves a real risk, and it is disclosed rather than absorbed.  ``tool-status.md`` is
rendered from this record, CommonMark permits raw HTML, and a backtick run inside a value
closes a fence of equal or shorter length -- so untrusted text rendered unescaped can inject
markup or restructure a document (CWE-116), and a bare CR, LF or ESC reaching a terminal or
a line-oriented log can overwrite or forge a record (CWE-117).  Three things answer it:

* **The container is safe.**  RFC 8259 section 7 requires every character below U+0020
  escaped in JSON's encoded form and a parser returns them as data, so no value carried
  here can break out of this document or add a member to it.  That is why the record can
  hold these bytes at all, and it is a claim about JSON rather than about the bytes.
* **The hazard is measured beside every field that carries it.**  A control-character
  inventory -- which code points occur, how many times each, and what each does to a
  consumer that renders it unescaped -- published next to the text, never reproducing the
  character it reports.  ``occurrences: 0`` is a measurement that found none, which is a
  different fact from ``measured: false``, and both are said.
* **The obligation is named.**  The run record carries one
  :data:`UNTRUSTED_TEXT_CONTRACT_KEY` block listing every verbatim field, the AAP clauses
  that make the retention mandatory, and what a consumer must do before rendering one.

**This module renders none of it.**  It writes no Markdown and emits no fenced block, so it
has nothing of its own to escape; the three ``queries/joern/*.sc`` reports, which do author
Markdown and carry no verbatim-retention obligation, take the other route instead -- an
escaper for untrusted inline and table-cell text and a fence measured one backtick longer
than the longest run in the payload.

TWO PATH MEASUREMENTS, AND NEITHER SUBSTITUTES FOR THE OTHER
-----------------------------------------------------------
``totals.path_kinds`` classifies every resolved path by its **form** -- a tree file, a
coordinate outside the root, an archive member, a bytecode-derived source -- which a
resolver decides with no filesystem at all, and it carries the non-filesystem count and
proportion AAP 0.6.1 puts in ``run-record.md``.  ``totals.paths_not_on_disk`` asks the
other question AAP 0.1.1 requires answered: whether the thing each row names is
**actually present in the pinned tree**.  A ``tree_file`` naming a file the pin does not
carry is invisible to the first and counted by the second, which is why both are recorded.

The second is measured **once**, by :func:`_paths_not_on_disk`, against the same root every
path in the dataset was expressed against, and it is written into the record with its
denominator, its per-reason and per-tool breakdown and a bounded set of examples -- so a
count of zero reads as "none of 9,430 rows" rather than as an absent field.  A row counted
there is kept, never dropped: an external coordinate, an archive member and a virtual
reference are legitimate coordinates (AAP 0.9.3).

NO RAW ARTIFACT STRING REACHES A DURABLE RECORD
-----------------------------------------------
``normalize-run.json`` is persisted and quoted into ``tool-status.md``, and much of what a
halt or a rejection says is composed from text this module did not author -- an artifact's
own rule identifiers, messages and URIs, and a runner status record's fields.  Two hazards
follow: a terminal control sequence rewrites what an operator reading the record sees, and
a URI carrying userinfo puts whatever credential the artifact happened to contain into a
durable file.  Rejecting a record does not help, because a rejected record is still a
*recorded* record.

So there is one renderer, :func:`paths.safe_diagnostic` and its siblings, and this module
uses it at three places: :meth:`NormalizeHalt.as_dict` and :attr:`NormalizeHalt.safe_message`
(the persistence boundary for every halt), the adapter-contract halt (whose text the adapter
composed from the artifact), and the unexpected-error path, where
:func:`_safe_exception_chain` renders frames from :func:`traceback.format_tb` -- this
repository's own source -- while *describing* each exception message rather than quoting it.
``traceback.format_exc()`` is never written to the record: its final line is the exception's
``str()``, which on an unexpected error is composed from whatever artifact content was being
processed at the time.

EVERY FILE IS BOUNDED BEFORE IT IS ALLOCATED
--------------------------------------------
Every file this module reads -- nine artifacts, nine status files, eighteen streams, the
runner metadata -- was written by something else, and the largest of them is 70 MB of JSON.
Reading one whole and then deciding about it is not a bound: the read is the allocation, so
a check afterwards runs only for files that were already affordable (CWE-400, CWE-770).

So each read is bounded at its own cap and each cap is a measurement plus a margin, stated
with the measurement in :data:`INGESTION_BOUNDS` and published in the run record.  An
artifact is refused by :func:`os.stat` above :data:`ARTIFACT_BYTE_LIMIT` before a byte is
read; read at ``cap + 1`` bytes so a file that grew since the stat is refused too; decoded
strictly; **bounded on structure by :func:`_validate_document_bounds` while it is still
text**; only then parsed; and then measured again by :func:`_measure_document` for depth,
node count and longest string **before it is routed to a shape**.  A stream is read at the
excerpt limit plus one character rather than read whole and sliced.  A status file above
its own cap is measured, retained and named as a defect rather than parsed, and a numeric
field is converted only through a digit-bounded predicate that actually implies
convertibility.

THE STRUCTURAL CAPS ARE ENFORCED BEFORE THE PARSE, NOT AFTER IT
--------------------------------------------------------------
The byte cap does not bound the object graph, and that gap is the whole reason the depth,
node, string and digit caps exist: a compact document well inside 512 MiB can encode tens
of millions of values, and ``json.loads`` allocates every one of them before any check
over a *parsed* document can run.  A cap that runs after the parse therefore names an
exhaustion that has already happened rather than preventing it, and ``MemoryError`` is a
diagnosis, not a limit.

So :func:`_validate_document_bounds` enforces all four caps on the decoded **text**, in one
bounded left-to-right token scan that never materialises a value: nesting depth on an
explicit integer counter, node count on the same definition :func:`_measure_document` uses,
string length on the raw token including its escapes, and the digit count of every numeric
literal, so a literal above CPython's 4,300-digit conversion limit is refused before
``int()`` or ``float()`` is reached.  A document it refuses becomes exactly the halt the
corresponding post-parse bound would have named -- the vocabulary is unchanged -- and a
document it accepts is one ``json.loads`` can be handed.  Deciding whether a document is
well-formed JSON is deliberately **not** its business: the invalid-JSON verdict stays with
``json.loads`` and its ``JSONDecodeError``, so :data:`HALT_ARTIFACT_INVALID_JSON` still
means what it always meant.

:func:`_measure_document` still runs after the parse, and is now an *independent
confirmation* rather than the only gate.  Two walks over two representations of one
document must agree, in one direction: the parse can only lose values (duplicate object
members collapse onto the last), so the post-parse measurement is at most the pre-parse
verdict, and a post-parse figure ABOVE it means one of the two walks is wrong.  That is
checked on the accepted path rather than assumed.

Both traversals are iterative -- one iterator per open container in the parsed walk, one
integer counter and one container-position stack in the text scan.  A recursive depth check
against a document deep enough to matter is the stack overflow it is trying to detect, and
a stack of pending children would grow with the document's breadth rather than its depth
(CWE-674).

The metadata load takes the same order.  ``paths.load_runner_metadata`` owns the read and
the parse of ``runner-metadata.json``, so the pre-parse gate sits on this side of that call:
:func:`_load_metadata` stats the file, reads it under the same bounded reader, validates the
text against the metadata caps that module publishes, and only then calls it.  That module's
own post-parse shape check is retained and is the second opinion, exactly as
:func:`_measure_document` is for an artifact.

Three exceptions are converted rather than left to escape -- ``MemoryError``,
``RecursionError``, and a ``ValueError`` that is not a ``JSONDecodeError`` (which is what
CPython raises for an integer literal above its 4,300-digit conversion limit).  All three
are ``Exception`` subclasses, so the catch-all in :func:`main` would record them either
way; it would record them under one unnamed ``unexpected-error`` reason, with the message
described rather than quoted, and a reader could not tell which file, which stage or which
limit was reached.  Each is instead a named halt with the artifact, the stage and the cap
in its details, and every one of those names is in :data:`HALT_REASONS`.

Exit codes
----------
``0``  the dataset was written and all three reconciliation stages and the typed
       comparison passed.
``1``  a halting condition in the data: an unknown artifact shape, an unexpected or
       out-of-tree entry in the raw tree, an absent artifact with no stated reason, an
       adapter's structural halt, a failed reconciliation identity or output comparison.
``2``  argparse usage error (argparse's own convention).
``78`` a configuration fault, the same ``EX_CONFIG`` the provisioned runners use through
       ``harness/lib/scope.sh``: unreadable or incomplete runner metadata, a scan root that
       contradicts the recorded one, an allowlist that is not the twelve authoritative
       globs, a missing raw tree, a missing conditional adapter module, an output path
       outside its owner root or aliasing another path or carrying a symlinked component,
       and a run record that could not be written and verified.

``harness/artifacts/logs/normalize-run.json`` is written on **every** one of those paths, so
a halt is diagnosable from the record rather than only from the console.  That tree is
git-ignored (``.gitignore:31`` is ``artifacts/``), so the record is published through the
per-file size-and-sha256 manifest (AAP 0.1.3) rather than by ``git add`` -- which is why it
is written self-describing, carrying the byte size and sha256 of **every** file it names
that exists on disk: every input, every output, every runner artifact and every runner
stream, including the tens-of-megabytes stdout logs, which are digested in bounded chunks
rather than read whole.  A null measurement appears only where the file is absent, where
it exists but could not be read, or where the entry names no file, and each such entry
carries a ``null_reason`` saying which -- so no reader has to work out whether a null
means "not measured" or "nothing to measure".  The record's own ``file_measurement`` block
states that contract beside the data.

Imports and the ``sys.path`` bootstrap
--------------------------------------
Standard library only -- no third-party import, no manifest, no lockfile, no install step
(AAP 0.4.1), and the module runs on the base interpreter the gate recorded, independent of
any scanner's virtualenv.  Internal imports are absolute and rooted at the ``normalize``
package (``from normalize import ...``), never a bare sibling import, which would resolve
only when this module's own directory happens to be ``sys.path[0]`` and would break the
adapter-test import route.

There is deliberately no ``__init__.py`` anywhere under ``harness/lib/normalize/``: PEP 420
implicit namespace packages make ``import normalize.cli`` work once ``harness/lib`` is on
``sys.path``.  A one-time insertion under an ``if __name__ == "__main__"`` guard performs
it, so the module runs both as ``python3 -m normalize.cli`` (with ``harness/lib`` already
on the path) and as ``python3 harness/lib/normalize/cli.py``.  That guard sits **above the
package imports** rather than at the foot of the file, because the imports it exists to
enable execute first: a bottom-of-file insertion would run after they had already failed.
The test modules under ``oss-scan-results/adapter-tests/`` reach these modules with the
same two lines.

No user-specified rule governs this file (AAP 0.7, 0.10.2), so enterprise-standard best
practice applies in its place, held to the AAP's own bar: verification independent of the
thing verified, and a record rejected rather than inferred into a field (AAP 0.1.3), with
every number this module publishes traceable to a file that exists on disk (AAP 0.6.2).
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import platform
import re
import shlex
import sys
import traceback
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType, ModuleType
from typing import Any

# --------------------------------------------------------------------------- #
# The one-time sys.path bootstrap this module owns.                           #
#                                                                             #
# There is deliberately no __init__.py under harness/lib/normalize/: PEP 420  #
# implicit namespace packages make "import normalize.cli" work once           #
# harness/lib is on sys.path. parents[1] of this file IS harness/lib.         #
#                                                                             #
# Run as "python3 -m normalize.cli", __package__ is "normalize" and that path #
# entry is already present, so this does nothing. Run as                      #
# "python3 harness/lib/normalize/cli.py", sys.path[0] is this file's own      #
# directory instead, and the absolute "from normalize import ..." imports     #
# below cannot resolve -- so the entry is inserted here, before those imports #
# execute, which is the only place it can be and still work. It is not stray  #
# path munging: the alternative, a bare sibling import, would resolve only by #
# accident of sys.path[0] and would break the adapter-test import route. The  #
# test modules under oss-scan-results/adapter-tests/ reach these modules with #
# the same two lines.                                                         #
# --------------------------------------------------------------------------- #
if __name__ == "__main__" and __package__ in (None, ""):  # pragma: no cover
    _LIB_DIR = str(Path(__file__).resolve().parents[1])
    if _LIB_DIR not in sys.path:
        sys.path.insert(0, _LIB_DIR)

from normalize import emit, paths, reconcile, severity, shape  # noqa: E402
from normalize.adapters import (  # noqa: E402
    checkov,
    dependency_check,
    gitleaks,
    joern,
    sarif,
    trivy,
)

__all__ = [
    "SCHEMA_VERSION",
    "EXPECTED_INTERPRETER_VERSION",
    "STATUS_DEFECT_DUPLICATE_KEY",
    "STATUS_DEFECT_UNPARSABLE_LINE",
    "STATUS_DEFECT_FILE_TOO_LARGE",
    "STATUS_DEFECT_NUMERIC_LITERAL_TOO_LONG",
    "STATUS_LINE_INVALID_KEY",
    "STATUS_LINE_NO_SEPARATOR",
    "ARTIFACT_BYTE_LIMIT",
    "JSON_DEPTH_LIMIT",
    "JSON_NODE_LIMIT",
    "JSON_STRING_LIMIT",
    "STATUS_FILE_BYTE_LIMIT",
    "STATUS_NUMERIC_DIGIT_LIMIT",
    "RUNNER_METADATA_BYTE_LIMIT",
    "TOOL_WORDS_EXCERPT_LIMIT",
    "UNTRUSTED_TEXT_CONTRACT_KEY",
    "CONTROL_CLASS_C0",
    "CONTROL_CLASS_DELETE",
    "CONTROL_CLASS_C1",
    "CONTROL_CLASS_BIDIRECTIONAL",
    "BIDIRECTIONAL_FORMATTING_CODE_POINTS",
    "INGESTION_BOUNDS",
    "ARTIFACT_ORDER",
    "ADAPTER_REGISTRY",
    "CONDITIONAL_ADAPTER_MODULES",
    "PARSE_STATUS_CLEAN",
    "PARSE_STATUS_PARTIAL",
    "PARSE_STATUS_FAILED",
    "PARSE_STATUS_ABSENT",
    "PARSE_STATUSES",
    "EXIT_OK",
    "EXIT_HALT",
    "EXIT_USAGE",
    "EXIT_CONFIG",
    "EXIT_STATUS_EXITED",
    "EXIT_STATUS_TIMEOUT",
    "EXIT_STATUS_UNRECORDED",
    "HALT_REASONS",
    "NOT_ON_DISK_REASONS",
    "PATHS_NOT_ON_DISK_EXAMPLE_LIMIT",
    "NoWorkStatement",
    "no_work_statements",
    "NormalizeHalt",
    "ConfigurationFault",
    "MissingAdapterModule",
    "Inputs",
    "ArtifactOutcome",
    "build_parser",
    "resolve_inputs",
    "resolve_adapter",
    "interpreter_record",
    "main",
]


# --------------------------------------------------------------------------- #
# Vocabulary and constants                                                    #
# --------------------------------------------------------------------------- #

#: Schema of ``normalize-run.json``.  Versioned because the record is published by
#: manifest rather than by git, so a consumer has no diff to read it against.
SCHEMA_VERSION: str = "normalize-run/1.0.0"

#: The document this module writes, named in AAP 0.6.1.
RUN_RECORD_DOCUMENT: str = "harness/artifacts/logs/normalize-run.json"

#: The role the run record is published under.  ``emit.publish_document`` records the role
#: rather than deriving one from the filename, because a caller may write the record to a
#: scratch path and a reader still has to know which document it is looking at.
RUN_RECORD_ROLE: str = "normalize_run_record"

#: The interpreter AAP 0.4.1 expects.  A difference is recorded with both values and the
#: run continues -- major, minor or patch alike.  This is never a halt.
EXPECTED_INTERPRETER_VERSION: str = "3.13.7"

#: The order artifacts are processed and rows emitted in.  ``shape``, ``paths`` and
#: ``reconcile`` all author this same order; ``severity`` authors the same nine identifiers
#: in a different order and is used only to render the literal tally.
ARTIFACT_ORDER: tuple[str, ...] = tuple(shape.CANONICAL_TOOLS)

#: Default filenames, joined onto an environment-derived directory rather than onto a
#: hardcoded repository-relative path.
RUNNER_METADATA_FILENAME: str = "runner-metadata.json"
#: The dataset's completion manifest, published LAST by the emitter's commit protocol
#: and required by it immediately afterwards.  It lives in the log tree rather than
#: beside the two deliverables because AAP 0.6.1 enumerates `oss-scan-results/` file by
#: file while `harness/artifacts/logs/*.json` is an enumerated pattern -- so the commit
#: record lands where this run's evidence legitimately accumulates rather than adding an
#: unlisted file to the result tree.
PUBLICATION_MANIFEST_FILENAME: str = "findings-publication.json"

RUN_RECORD_FILENAME: str = "normalize-run.json"
FINDINGS_JSON_RELATIVE: str = "oss-scan-results/findings.json"
FINDINGS_CSV_RELATIVE: str = "oss-scan-results/findings.csv"

#: The four parse statuses of AAP 0.5.4, by name.
PARSE_STATUS_CLEAN: str = "clean"
PARSE_STATUS_PARTIAL: str = "partial"
PARSE_STATUS_FAILED: str = "failed"
PARSE_STATUS_ABSENT: str = "absent"
PARSE_STATUSES: tuple[str, ...] = (
    PARSE_STATUS_CLEAN,
    PARSE_STATUS_PARTIAL,
    PARSE_STATUS_FAILED,
    PARSE_STATUS_ABSENT,
)

#: Process exit codes.  ``78`` is ``EX_CONFIG``, the same code the provisioned runners use
#: through ``harness/lib/scope.sh``'s ``scope_fail`` for a fault to correct rather than a
#: scanning outcome to classify.
EXIT_OK: int = 0
EXIT_HALT: int = 1
EXIT_USAGE: int = 2
EXIT_CONFIG: int = 78

#: How a runner's process ended, as recorded in ``<tool>.status``.  ``timeout`` is the
#: single name AAP 0.8.1 gives a termination that produced no exit code -- it names the
#: status and does not excuse a missing artifact.
EXIT_STATUS_EXITED: str = "exited"
EXIT_STATUS_TIMEOUT: str = "timeout"
EXIT_STATUS_UNRECORDED: str = "unrecorded"

#: Every halting reason this module can name, so the run record's ``halt.reason`` is drawn
#: from a closed set a reader can enumerate.
HALT_VOCABULARY_MISMATCH: str = "vocabulary-mismatch"
HALT_MISSING_INPUT: str = "missing-required-input"
HALT_RUNNER_METADATA: str = "runner-metadata-unusable"
HALT_SCAN_ROOT_DISAGREEMENT: str = "scan-root-disagreement"
HALT_SCAN_ROOT_NOT_ABSOLUTE: str = "scan-root-not-absolute"
HALT_ALLOWLIST_UNREADABLE: str = "allowlist-unreadable"
HALT_ALLOWLIST_NOT_AUTHORITATIVE: str = "allowlist-not-authoritative"
HALT_RAW_DIRECTORY_MISSING: str = "raw-directory-missing"
HALT_RAW_DIRECTORY_BOUNDARY: str = "raw-directory-boundary-violation"
#: A direct child of the raw tree that this module will not consume: a name outside the
#: nine runner artifacts, or one of those nine names carried by something that is not a
#: regular file.  Its own reason rather than a detail on the boundary violation above,
#: because the two are different diagnoses -- that one is an artifact reached from
#: OUTSIDE the tree, this one is a document sitting INSIDE it that nothing adapted --
#: and a reader enumerating this vocabulary should not have to open the details to tell
#: them apart.  Both are conditions in the input tree rather than in the invocation, so
#: both exit 1.
HALT_RAW_DIRECTORY_UNEXPECTED: str = "raw-directory-unexpected-entry"
HALT_ARTIFACT_UNREADABLE: str = "artifact-unreadable"
HALT_ARTIFACT_INVALID_JSON: str = "artifact-invalid-json"
HALT_ARTIFACT_NOT_UTF8: str = "artifact-not-utf-8"
#: The four ingestion bounds, one halting reason each.  They are separate names rather
#: than one "bound exceeded" reason with a discriminating detail because the diagnosis
#: differs: a 700 MB artifact is a different fault from a 449,681-character string inside
#: a 5 MB one, and a reader enumerating this vocabulary should not have to open the
#: details to tell them apart (CWE-400, CWE-674, CWE-770).
HALT_ARTIFACT_TOO_LARGE: str = "artifact-exceeds-byte-cap"
HALT_ARTIFACT_TOO_DEEP: str = "artifact-exceeds-depth-cap"
HALT_ARTIFACT_TOO_MANY_NODES: str = "artifact-exceeds-node-cap"
HALT_ARTIFACT_STRING_TOO_LONG: str = "artifact-exceeds-string-cap"
#: The two resource exhaustions that are not a bound this module set but a limit the
#: interpreter itself imposed.  Both are converted rather than left to escape: they are
#: ``Exception`` subclasses, so :func:`main` would otherwise record them under the single
#: unnamed ``unexpected-error`` reason with the message *described* rather than quoted --
#: a record that cannot say which file, which stage or which limit was reached.
HALT_ARTIFACT_EXHAUSTED_MEMORY: str = "artifact-ingestion-exhausted-memory"
HALT_ARTIFACT_EXHAUSTED_STACK: str = "artifact-ingestion-exhausted-stack"
#: A ``ValueError`` out of ``json.loads`` that is not a ``JSONDecodeError``: the document's
#: syntax is fine and one of its literals is not convertible.  The documented instance is
#: an integer literal above CPython's 4,300-digit ``sys.set_int_max_str_digits`` limit,
#: which is a valid JSON number and an ``int()`` this interpreter refuses.
HALT_ARTIFACT_LITERAL_UNCONVERTIBLE: str = "artifact-literal-not-convertible"
HALT_UNKNOWN_ARTIFACT_SHAPE: str = "unknown-artifact-shape"
HALT_MISSING_ADAPTER_MODULE: str = "missing-adapter-module"
HALT_ADAPTER_STRUCTURAL: str = "adapter-structural-halt"
HALT_ADAPTER_CONTRACT: str = "adapter-contract-fault"
HALT_ABSENT_WITHOUT_STATED_REASON: str = "artifact-absent-without-stated-reason"
HALT_ABSENT_WITHOUT_NO_WORK_STATEMENT: str = "artifact-absent-without-no-work-statement"
HALT_WRONG_SCAN_ROOT_EVIDENCE: str = "runner-resolved-another-tree"
HALT_SMOKE_OVERRIDE_EVIDENCE: str = "runner-scanned-smoke-target"
HALT_SOURCE_INDEX_EMPTY: str = "source-index-empty"
HALT_SOURCE_INDEX_INCOMPLETE: str = "source-index-incomplete"
HALT_RECONCILIATION: str = "reconciliation-failed"
HALT_EMIT: str = "output-write-failed"
HALT_OUTPUT_COMPARISON: str = "output-comparison-failed"
HALT_OUTPUT_OUTSIDE_OWNER: str = "output-path-outside-its-owner-root"
HALT_OUTPUT_ALIASED: str = "output-path-aliases-another-path"
HALT_OUTPUT_SYMLINKED: str = "output-path-has-a-symlinked-component"
HALT_RUN_RECORD_NOT_PERSISTED: str = "run-record-not-persisted"
HALT_UNEXPECTED: str = "unexpected-error"
HALT_REASONS: tuple[str, ...] = (
    HALT_VOCABULARY_MISMATCH,
    HALT_MISSING_INPUT,
    HALT_RUNNER_METADATA,
    HALT_SCAN_ROOT_DISAGREEMENT,
    HALT_SCAN_ROOT_NOT_ABSOLUTE,
    HALT_ALLOWLIST_UNREADABLE,
    HALT_ALLOWLIST_NOT_AUTHORITATIVE,
    HALT_RAW_DIRECTORY_MISSING,
    HALT_RAW_DIRECTORY_BOUNDARY,
    HALT_RAW_DIRECTORY_UNEXPECTED,
    HALT_ARTIFACT_UNREADABLE,
    HALT_ARTIFACT_INVALID_JSON,
    HALT_ARTIFACT_NOT_UTF8,
    HALT_ARTIFACT_TOO_LARGE,
    HALT_ARTIFACT_TOO_DEEP,
    HALT_ARTIFACT_TOO_MANY_NODES,
    HALT_ARTIFACT_STRING_TOO_LONG,
    HALT_ARTIFACT_EXHAUSTED_MEMORY,
    HALT_ARTIFACT_EXHAUSTED_STACK,
    HALT_ARTIFACT_LITERAL_UNCONVERTIBLE,
    HALT_UNKNOWN_ARTIFACT_SHAPE,
    HALT_MISSING_ADAPTER_MODULE,
    HALT_ADAPTER_STRUCTURAL,
    HALT_ADAPTER_CONTRACT,
    HALT_ABSENT_WITHOUT_STATED_REASON,
    HALT_ABSENT_WITHOUT_NO_WORK_STATEMENT,
    HALT_WRONG_SCAN_ROOT_EVIDENCE,
    HALT_SMOKE_OVERRIDE_EVIDENCE,
    HALT_SOURCE_INDEX_EMPTY,
    HALT_SOURCE_INDEX_INCOMPLETE,
    HALT_RECONCILIATION,
    HALT_EMIT,
    HALT_OUTPUT_COMPARISON,
    HALT_OUTPUT_OUTSIDE_OWNER,
    HALT_OUTPUT_ALIASED,
    HALT_OUTPUT_SYMLINKED,
    HALT_RUN_RECORD_NOT_PERSISTED,
    HALT_UNEXPECTED,
)

#: How much of a tool's own words is carried into the run record for an absent artifact.
#: The stream's byte size and sha256 are always recorded beside the excerpt, and the log
#: file itself stays on disk, so a cap can never lose evidence silently: an excerpt that
#: was cut says so.
TOOL_WORDS_EXCERPT_LIMIT: int = 20_000

#: Bytes read per hashing step.  Artifacts and logs run to tens of megabytes.
_DIGEST_CHUNK: int = 1 << 20

# --------------------------------------------------------------------------- #
# Untrusted verbatim text -- the residual risk this module documents and       #
# measures rather than removes (CWE-116, CWE-117)                             #
#                                                                             #
# A handful of fields in this record carry text this pipeline did not author   #
# and must not rewrite: a runner's own stream words, and its own .status       #
# lines. AAP 0.5.4 requires the parser error "retained verbatim" and settles   #
# the absent-artifact verdict "using only the tool's own stated words"; AAP    #
# 0.6.1 and 0.6.2 require an absent artifact's stderr "verbatim" and a         #
# reduced-reach condition "in the tool's own words". Escaping those bytes      #
# here would satisfy a rendering concern by destroying the evidence contract   #
# three AAP clauses make mandatory -- a rewritten byte is a rewritten verdict. #
#                                                                             #
# So this module takes the other route the finding allows: keep the bytes,     #
# MEASURE the hazard beside them, and state the obligation the consumer        #
# inherits. The measurement is a control-character inventory -- which code     #
# points occur and how many times each -- and it never reproduces the          #
# character it reports, because a record that printed the ESC it warns about   #
# would carry the hazard into every terminal that cats it.                    #
#                                                                             #
# JSON is a safe container for these bytes and that is why the record can      #
# hold them at all: RFC 8259 section 7 requires every character below U+0020   #
# escaped in the encoded form, and a parser returns them as data rather than   #
# as structure, so nothing here can be broken out of by the text it carries.   #
# Markdown, HTML and a terminal are not safe containers, which is why the      #
# contract in the run record is addressed to whoever renders them. The three   #
# queries/joern/*.sc reports take the OTHER route for their own output --      #
# escape and a measured fence -- because they author the Markdown and are      #
# under no verbatim-retention obligation.                                     #
# --------------------------------------------------------------------------- #

#: The run record key carrying the escaping contract for untrusted verbatim text.  A named
#: constant so an entry that carries such text can point a reader at the contract --
#: through its own ``text_escaping_contract`` field -- rather than restating it once per
#: stream, and so a consumer can look the contract up by name instead of by prose.
UNTRUSTED_TEXT_CONTRACT_KEY: str = "untrusted_verbatim_text"

#: The four classes of character the inventory counts, by name.  A closed vocabulary, so a
#: consumer branches on the class rather than re-deriving a code point range.
CONTROL_CLASS_C0: str = "c0-control"
CONTROL_CLASS_DELETE: str = "delete"
CONTROL_CLASS_C1: str = "c1-control"
CONTROL_CLASS_BIDIRECTIONAL: str = "bidirectional-formatting"

#: The bidirectional formatting characters -- Unicode general category ``Cf`` -- that
#: reorder rendered text while being invisible themselves.  Counted alongside the ``Cc``
#: controls because they change what a reader *sees* rather than what the bytes *say*,
#: which is the same defect as an unescaped ESC arriving at a terminal (CWE-451; the
#: citable instance is the 2021 "Trojan Source" work, CVE-2021-42574).
BIDIRECTIONAL_FORMATTING_CODE_POINTS: tuple[int, ...] = (
    0x061C,  # ARABIC LETTER MARK
    0x200E,  # LEFT-TO-RIGHT MARK
    0x200F,  # RIGHT-TO-LEFT MARK
    0x202A,  # LEFT-TO-RIGHT EMBEDDING
    0x202B,  # RIGHT-TO-LEFT EMBEDDING
    0x202C,  # POP DIRECTIONAL FORMATTING
    0x202D,  # LEFT-TO-RIGHT OVERRIDE
    0x202E,  # RIGHT-TO-LEFT OVERRIDE
    0x2066,  # LEFT-TO-RIGHT ISOLATE
    0x2067,  # RIGHT-TO-LEFT ISOLATE
    0x2068,  # FIRST STRONG ISOLATE
    0x2069,  # POP DIRECTIONAL ISOLATE
)

#: The standard abbreviation for every code point the inventory counts: the C0 set
#: (U+0000-U+001F), DELETE (U+007F), the C1 set (U+0080-U+009F) and the twelve
#: bidirectional formatting characters above.  A fixed table rather than
#: ``unicodedata.name``, which raises ``ValueError`` for every one of the sixty-five
#: controls: Unicode assigns a control character no name at all, only these aliases (ASCII
#: for C0 and DEL, ISO 6429 for C1).  The abbreviation is what makes an inventory readable
#: without the character being present in it.
_CONTROL_ABBREVIATIONS: dict[int, str] = {
    0x00: "NUL", 0x01: "SOH", 0x02: "STX", 0x03: "ETX",
    0x04: "EOT", 0x05: "ENQ", 0x06: "ACK", 0x07: "BEL",
    0x08: "BS", 0x09: "HT", 0x0A: "LF", 0x0B: "VT",
    0x0C: "FF", 0x0D: "CR", 0x0E: "SO", 0x0F: "SI",
    0x10: "DLE", 0x11: "DC1", 0x12: "DC2", 0x13: "DC3",
    0x14: "DC4", 0x15: "NAK", 0x16: "SYN", 0x17: "ETB",
    0x18: "CAN", 0x19: "EM", 0x1A: "SUB", 0x1B: "ESC",
    0x1C: "FS", 0x1D: "GS", 0x1E: "RS", 0x1F: "US",
    0x7F: "DEL",
    0x80: "PAD", 0x81: "HOP", 0x82: "BPH", 0x83: "NBH",
    0x84: "IND", 0x85: "NEL", 0x86: "SSA", 0x87: "ESA",
    0x88: "HTS", 0x89: "HTJ", 0x8A: "VTS", 0x8B: "PLD",
    0x8C: "PLU", 0x8D: "RI", 0x8E: "SS2", 0x8F: "SS3",
    0x90: "DCS", 0x91: "PU1", 0x92: "PU2", 0x93: "STS",
    0x94: "CCH", 0x95: "MW", 0x96: "SPA", 0x97: "EPA",
    0x98: "SOS", 0x99: "SGC", 0x9A: "SCI", 0x9B: "CSI",
    0x9C: "ST", 0x9D: "OSC", 0x9E: "PM", 0x9F: "APC",
    0x061C: "ALM", 0x200E: "LRM", 0x200F: "RLM",
    0x202A: "LRE", 0x202B: "RLE", 0x202C: "PDF",
    0x202D: "LRO", 0x202E: "RLO", 0x2066: "LRI",
    0x2067: "RLI", 0x2068: "FSI", 0x2069: "PDI",
}

#: What a consumer sees when one of these code points reaches it unescaped.  Present for
#: the code points with a specific documented effect on a Markdown renderer, an HTML
#: renderer or a terminal; every other member of the subject set falls back to
#: :data:`_CONTROL_CLASS_EFFECTS`, which states what is known about its class.
_CONTROL_RENDERING_EFFECTS: dict[int, str] = {
    0x00: (
        "terminates the value in a consumer that reads C strings, so every character "
        "after it is silently dropped rather than rendered"
    ),
    0x07: "rings a terminal bell",
    0x08: (
        "moves a terminal cursor back one column, so the text that follows overwrites "
        "what was already printed"
    ),
    0x09: (
        "advances to the next tab stop; harmless inline, and in a column-aligned log it "
        "moves the text after it into another column"
    ),
    0x0A: (
        "ends the line: a consumer writing one record per line sees two records, and a "
        "Markdown table row ends in the middle of a cell (CWE-117)"
    ),
    0x0B: (
        "advances a terminal to the next line while CommonMark treats it as ordinary "
        "whitespace, so a rendered document and a terminal disagree about where the "
        "value ends"
    ),
    0x0C: (
        "advances a terminal to the next page while CommonMark treats it as ordinary "
        "whitespace, with the same disagreement as U+000B"
    ),
    0x0D: (
        "returns a terminal cursor to column zero, so the text that follows overwrites "
        "the line already printed; paired with U+000A it is a second line ending, which "
        "is how one record becomes two (CWE-117)"
    ),
    0x1A: (
        "ends the stream for a consumer that reads it as an end-of-file marker, so the "
        "rest of the value is never seen"
    ),
    0x1B: (
        "introduces an ANSI escape sequence: a consumer printing this text to a terminal "
        "can have its cursor moved, its colours changed, its window title relabelled or "
        "its own input buffer written (CWE-117)"
    ),
    0x7F: (
        "is not printable, and terminals disagree about whether to discard it or show a "
        "placeholder glyph, so two consumers disagree about the value's length"
    ),
    0x85: (
        "is a line ending in ISO 6429 and in Python's own str.splitlines(), so a consumer "
        "splitting this text into lines sees two records where a byte reader sees one "
        "(CWE-117)"
    ),
    0x9B: (
        "is the single-character control sequence introducer: it does what U+001B "
        "followed by '[' does, with no ESC present for a filter to look for (CWE-117)"
    ),
}

#: The effect stated for a code point with no entry of its own, by class.  Not a
#: placeholder standing in for an unwritten value: a C0 transmission control with no
#: rendering behaviour still has to be escaped, and its class is the whole of what is
#: known about it.  Total over the four classes, so the lookup cannot fail.
_CONTROL_CLASS_EFFECTS: dict[str, str] = {
    CONTROL_CLASS_C0: (
        "a C0 transmission control with no defined rendering; a consumer must escape it "
        "like any other control character"
    ),
    CONTROL_CLASS_DELETE: (
        "the DELETE control, which has no defined rendering and must be escaped"
    ),
    CONTROL_CLASS_C1: (
        "a C1 control with no defined rendering here; UTF-8 carries it as two bytes and a "
        "terminal in an 8-bit mode may still act on it"
    ),
    CONTROL_CLASS_BIDIRECTIONAL: (
        "reorders the rendered text around it while being invisible itself, so what a "
        "reader sees is not the order of the bytes (CWE-451)"
    ),
}


def _build_control_table() -> dict[int, tuple[str, str]]:
    """Build the code point -> ``(class, abbreviation)`` table the inventory counts against.

    The subject set is defined by the RANGES here and the abbreviations by
    :data:`_CONTROL_ABBREVIATIONS`; building one from the other is what keeps the two from
    drifting apart.  A code point in the set with no abbreviation raises ``KeyError`` at
    import, and an abbreviation for a code point outside the set is reported as a spare --
    both are faults in this table rather than conditions in any input, so both fail when
    the module loads rather than on the first stream that happens to carry the character.
    """
    table: dict[int, tuple[str, str]] = {}
    for code_point in range(0x00, 0x20):
        table[code_point] = (CONTROL_CLASS_C0, _CONTROL_ABBREVIATIONS[code_point])
    table[0x7F] = (CONTROL_CLASS_DELETE, _CONTROL_ABBREVIATIONS[0x7F])
    for code_point in range(0x80, 0xA0):
        table[code_point] = (CONTROL_CLASS_C1, _CONTROL_ABBREVIATIONS[code_point])
    for code_point in BIDIRECTIONAL_FORMATTING_CODE_POINTS:
        table[code_point] = (
            CONTROL_CLASS_BIDIRECTIONAL,
            _CONTROL_ABBREVIATIONS[code_point],
        )
    spare = sorted(set(_CONTROL_ABBREVIATIONS) - set(table))
    if spare:
        raise AssertionError(
            "_CONTROL_ABBREVIATIONS carries entries outside the inventory's subject set: "
            + ", ".join(f"U+{code_point:04X}" for code_point in spare)
        )
    return table


#: Code point -> ``(class, abbreviation)``, total over the inventory's subject set.
_CONTROL_TABLE: Mapping[int, tuple[str, str]] = MappingProxyType(_build_control_table())

#: How the inventory names its own subject set.  Carried in every inventory produced, so
#: the definition of the measurement travels with the numbers rather than living only in
#: this source: a reader of ``normalize-run.json`` can tell what was counted.
_CONTROL_SUBJECT_SET: str = (
    "Unicode general category Cc -- the C0 controls U+0000-U+001F, DELETE U+007F and the "
    "C1 controls U+0080-U+009F -- together with the twelve bidirectional formatting "
    "characters of category Cf that reorder rendered text without being visible"
)

#: How the inventory is taken.  Named in the record for the same reason as the subject
#: set, and because the method is what makes the result reproducible: the same text
#: produces the same inventory on every run, with no timestamp and no ordering by
#: first appearance.
_CONTROL_METHOD: str = (
    "one pass over the distinct characters of the embedded text, then str.count for each "
    "distinct character that is in the subject set; ordered by code point ascending, so "
    "the same text produces the same inventory on every run"
)

# --------------------------------------------------------------------------- #
# Ingestion bounds (CWE-400, CWE-674, CWE-770)                                #
#                                                                             #
# Every file this module reads is provisioning-supplied evidence written by a  #
# tool this module did not run, and the largest of them is 70 MB of JSON. An   #
# unbounded read-then-parse of such a file has three failure modes that are    #
# not hypothetical: a file large enough to exhaust memory (the read allocates  #
# the whole of it, then json.loads allocates the object graph again), a        #
# document nested deeply enough that json's own C scanner recurses past the    #
# interpreter's stack limit, and a single string or numeric literal long       #
# enough that one field is the allocation. None of the three is a JSON syntax  #
# error, so the JSONDecodeError arm below sees none of them.                   #
#                                                                             #
# So each is bounded, and each bound is a MEASUREMENT plus a margin rather     #
# than a number chosen to look safe. The measurements below were taken over    #
# the eight committed artifacts in harness/artifacts/raw/ with the same        #
# _measure_document walk this module ships (a node is a JSON value; an object  #
# member name is length-checked but is not itself a node), so the observed     #
# column and the enforced column are produced by one definition rather than    #
# by two:                                                                     #
#                                                                             #
#   quantity          observed maximum                  cap here      margin  #
#   ---------------------------------------------------------------------------#
#   artifact bytes    73,768,116  (opengrep.sarif)       512 MiB       7.3x    #
#   nesting depth             13  (datadog SARIF)             64       4.9x    #
#   node count           230,900  (datadog SARIF)      5,000,000      21.7x    #
#   longest string       449,681  (opengrep, semgrep)     8 MiB       18.6x    #
#   status file bytes        278  (datadog .status)        4 MiB   15,087x     #
#   numeric literal            8  (artifact_bytes)            64       8.0x    #
#                                                                             #
# The margins are deliberately large: a bound tuned close to today's data      #
# would halt a legitimate future run, and the point of a cap here is to make   #
# an ADVERSARIAL input impossible rather than to police a real one. Every cap  #
# is nonetheless far below what would actually hurt this host, which is the    #
# other half of the requirement -- 512 MiB of artifact is refused long before  #
# a 64 GB machine is in trouble, and 64 levels of nesting is refused long      #
# before CPython's default 1,000-frame recursion limit is.                     #
#                                                                             #
# WHERE EACH CAP IS ENFORCED, which is what makes it a bound rather than a     #
# diagnosis: the byte cap by os.stat and by the bounded read, before any of    #
# the file is decoded; the depth, node, string and numeric-literal caps by     #
# _validate_document_bounds over the decoded TEXT, before json.loads is        #
# called, because the object graph a document allocates is not bounded by its  #
# byte size and the parse is the allocation; and the same depth, node and      #
# string caps a second time by _measure_document over the parsed document, as  #
# an independent confirmation of the first verdict. The status-file caps are   #
# enforced by the status reader, which never parses JSON at all.               #
# --------------------------------------------------------------------------- #

#: The largest artifact this module will read at all.  ``os.stat`` decides it before any
#: allocation happens, and the bounded read that follows re-checks it, because a file can
#: grow between the two.
ARTIFACT_BYTE_LIMIT: int = 512 * 1024 * 1024

#: The deepest JSON nesting an artifact may carry.  Checked by an ITERATIVE walk, because
#: a recursive depth check against a document deep enough to matter is itself the stack
#: overflow it is trying to detect.
JSON_DEPTH_LIMIT: int = 64

#: The most JSON values an artifact's document may contain.  This is the bound on the
#: object graph rather than on the bytes: a compact 5 MB document holds far more nodes
#: than a pretty-printed 70 MB one, so bytes alone do not bound the traversal that follows.
JSON_NODE_LIMIT: int = 5_000_000

#: The longest string -- value or object member name -- an artifact may carry.  A single
#: field this long is not evidence any consumer of this dataset can use, and it reaches
#: ``paths``, ``severity`` and the rejection records as one value.
JSON_STRING_LIMIT: int = 8 * 1024 * 1024

#: The largest ``<tool>.status`` file this module will parse.  A status file is the
#: runner's own key=value account of its invocation, not bulk output; the largest
#: committed one is 278 bytes (``datadog-static-analyzer.status``), and an earlier
#: generation's ``joern.status`` was a 31,913-byte stream capture (commit 232d0d9cca3),
#: which is why the cap is here at all.  Over this bound the file is a named defect
#: and not parsed -- it is log-side evidence, so refusing to parse it costs the record one
#: measurement rather than costing the run its dataset.
STATUS_FILE_BYTE_LIMIT: int = 4 * 1024 * 1024

#: The most digits an untrusted numeric literal may carry before it is refused as a
#: number.  CPython raises ``ValueError`` from ``int()`` above 4,300 digits (the
#: ``sys.set_int_max_str_digits`` limit), which ``str.isdigit()`` does not predict, so a
#: guard that tests only for digits and then converts is a ValueError waiting for a
#: hostile status file.  The longest literal any committed status file carries is 8
#: digits (``artifact_bytes=73768116`` in ``opengrep.status``).
#:
#: This is the package's ONE digit bound and it has three enforcement points, deliberately
#: sharing a single number rather than growing a second one: a ``<tool>.status`` field,
#: through the digit-bounded predicate that reads it; every numeric literal in an untrusted
#: artifact, through :func:`_validate_document_bounds` before ``json.loads`` reaches
#: ``int()`` or ``float()``; and every numeric literal in ``runner-metadata.json``, through
#: the same walk called inside ``paths.load_runner_metadata``.  The longest literal any
#: committed JSON input carries is 9 digits (``runner-metadata.json``); 7 is the artifact
#: maximum.  Two bindings of one cap is the defect F10 recorded in ``emit.py`` -- the last
#: binding wins and no comment says so -- so the number itself is declared once, by the
#: module that owns the walk which now enforces it on both documents, and assigned here to
#: keep the name this module publishes.  Same pattern, same reason, as
#: :data:`RUNNER_METADATA_BYTE_LIMIT`.
STATUS_NUMERIC_DIGIT_LIMIT: int = paths.STATUS_NUMERIC_DIGIT_LIMIT

#: The largest ``runner-metadata.json`` this module will hand to ``paths``.  Checked here,
#: by size, before the read happens: ``paths.load_runner_metadata`` owns the parse and is
#: another module's file, so the allocation bound belongs on this side of the call.  The
#: committed metadata is 222,083 bytes.
#: The byte cap on ``runner-metadata.json``, taken from the module that owns the read
#: rather than restated here.  ``paths.load_runner_metadata`` enforces the same cap on its
#: own side, together with the depth, node and string caps this side cannot reach, and one
#: binding of a cap is the point: two bindings of one constant is what let a 0o666 file mode
#: silently override a 0o644 one in ``emit.py``, because the last binding wins and nothing
#: in either comment says so.  Assigning here keeps the name this module publishes while
#: leaving exactly one number in the package.
RUNNER_METADATA_BYTE_LIMIT: int = paths.METADATA_BYTE_LIMIT

#: The bounds as the run record publishes them, so a reader sees what was enforced without
#: reading this source, and a later run's record can be compared against this one's.
INGESTION_BOUNDS: Mapping[str, Any] = MappingProxyType(
    {
        "artifact_bytes": ARTIFACT_BYTE_LIMIT,
        "json_depth": JSON_DEPTH_LIMIT,
        "json_nodes": JSON_NODE_LIMIT,
        "json_string_characters": JSON_STRING_LIMIT,
        "status_file_bytes": STATUS_FILE_BYTE_LIMIT,
        "status_numeric_digits": STATUS_NUMERIC_DIGIT_LIMIT,
        "runner_metadata_bytes": RUNNER_METADATA_BYTE_LIMIT,
        # The metadata document is bounded on shape as well as on size, by the module that
        # owns its read; the caps are published here so the record states every bound that
        # was enforced, not only the ones this module checks itself.
        "runner_metadata_depth": paths.METADATA_DEPTH_LIMIT,
        "runner_metadata_nodes": paths.METADATA_NODE_LIMIT,
        "runner_metadata_string_characters": paths.METADATA_STRING_LIMIT,
        "tool_words_excerpt_characters": TOOL_WORDS_EXCERPT_LIMIT,
        "observed_maxima": {
            "measured_over": "the eight committed artifacts in harness/artifacts/raw/",
            "artifact_bytes": 73_768_116,
            "artifact_bytes_file": "opengrep.sarif",
            "json_depth": 13,
            "json_nodes": 230_900,
            "json_depth_and_nodes_file": "datadog-static-analyzer.sarif",
            "json_string_characters": 449_681,
            "json_string_characters_files": ["opengrep.sarif", "semgrep.sarif"],
            "status_file_bytes": 278,
            "status_file_bytes_file": "datadog-static-analyzer.status",
            "status_numeric_digits": 8,
            "runner_metadata_bytes": 224_049,
            "runner_metadata_depth": 9,
            "runner_metadata_nodes": 2_371,
            "runner_metadata_string_characters": 1_862,
            "node_definition": (
                "a node is a JSON value; an object member name is length-checked against "
                "the string cap but is not itself counted as a node"
            ),
        },
    }
)

#: The adapter registry.  ``shape.py`` deliberately imports no adapter and names one by
#: string key; this module inverts that direction, which is what keeps the import graph
#: acyclic and lets the routing tests run without importing six adapters.
ADAPTER_REGISTRY: Mapping[str, ModuleType] = MappingProxyType(
    {
        "sarif": sarif,
        "trivy": trivy,
        "gitleaks": gitleaks,
        "checkov": checkov,
        "dependency_check": dependency_check,
        "joern": joern,
    }
)

#: The adapter modules AAP 0.6.1 creates *"if and only if"* their tool writes an artifact.
#: Absent in the expected case, which is why the import is guarded rather than top-level.
CONDITIONAL_ADAPTER_MODULES: tuple[str, ...] = ("osv_scanner",)

#: Counter keys every adapter that can produce them authors under the same name.  A tool
#: whose adapter defines none of a given counter records ``null`` rather than ``0``: a
#: field the adapter never counts is not a measurement of zero.
_SUMMARY_COUNTERS: tuple[str, ...] = (
    "multi_location_records",
    "multi_valued_cwe_records",
    "multi_valued_cve_records",
    "non_filesystem_paths",
    "rows_in_scope",
    "rows_out_of_scope",
)

#: Prefix the adapters use for their per-path-kind counters.
_PATH_KIND_COUNTER_PREFIX: str = "path_kind_"


# --------------------------------------------------------------------------- #
# Halting conditions                                                          #
# --------------------------------------------------------------------------- #


class NormalizeHalt(Exception):
    """A condition that stops the run, carrying everything the record must quote.

    Raised rather than returned so no caller can overlook it, and carrying its own
    serialisable detail so ``normalize-run.json`` quotes one measurement rather than a
    second reconstruction of the same fault.  ``main`` catches it, writes the record and
    exits with :attr:`exit_code`.

    Args:
        reason: One of :data:`HALT_REASONS`.
        message: The diagnostic, in enough words to act on without re-reading the
            artifact.
        details: The observed structure, counts or paths behind the message.
        exit_code: :data:`EXIT_HALT` for a condition in the data, :data:`EXIT_CONFIG` for
            a fault in the configuration handed to this module.
    """

    def __init__(
        self,
        reason: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
        exit_code: int = EXIT_HALT,
    ) -> None:
        if reason not in HALT_REASONS:
            raise ValueError(
                f"unknown halt reason {reason!r}; the closed set is "
                f"{', '.join(HALT_REASONS)}"
            )
        self.reason = reason
        self.message = message
        self.details: dict[str, Any] = dict(details or {})
        self.exit_code = exit_code
        super().__init__(message)

    @property
    def safe_message(self) -> str:
        """The message with URI userinfo redacted and control characters escaped.

        Used for the terminal report as well as the record, because a control sequence
        in a message printed to a terminal is the more immediate of the two hazards: it
        rewrites what the operator reading the halt sees.  ``\\n`` and ``\\t`` survive,
        so a multi-line authored message still reads as one.
        """
        return paths.sanitise_diagnostic(self.message, limit=None).text

    def as_dict(self) -> dict[str, Any]:
        """Return the halt as a JSON-serialisable mapping, safe to persist.

        This is the persistence boundary for every halt: ``main`` writes the result into
        ``normalize-run.json``.  A halt message and its details are composed partly from
        externally supplied text -- an artifact's own strings, a runner status record's
        fields, a path the caller passed -- so both go through the one renderer in
        ``paths``: URI userinfo redacted, control characters escaped.

        **Length is bounded where the artifact decides it, not here.**  The bound belongs
        to the site that owns the evidence contract: an artifact-supplied value reaches a
        message through :func:`paths.safe_diagnostic`, which bounds it at
        :data:`paths.DIAGNOSTIC_VALUE_LIMIT` and records its digest;
        ``shape.UnknownArtifactShape`` bounds its own observed-structure excerpts; and a
        runner's own words are bounded at :data:`TOOL_WORDS_EXCERPT_LIMIT` with the byte
        size and sha256 recorded beside them.  Truncating again here would cut a verbatim
        excerpt AAP 0.5.4 requires quoted in full, and would truncate authored prose that
        was never the hazard -- so the boundary redacts and escapes, and leaves lengths to
        the contracts that state them.

        The exception's own ``message`` and ``details`` attributes are left as composed,
        so an assertion reads what the code said while the durable record reads what is
        safe to keep.
        """
        return {
            "reason": self.reason,
            "message": self.safe_message,
            "exit_code": self.exit_code,
            "details": paths.sanitise_persisted(self.details, limit=None),
        }


class ConfigurationFault(NormalizeHalt):
    """A fault in what this module was handed, rather than in a scanner's output.

    Exits ``78`` (``EX_CONFIG``), the same code ``harness/lib/scope.sh``'s ``scope_fail``
    uses, so a reader of an exit code sees the same distinction the runners draw: a
    configuration fault to correct, not a scanning outcome to classify.
    """

    def __init__(
        self,
        reason: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(reason, message, details=details, exit_code=EXIT_CONFIG)


class MissingAdapterModule(ConfigurationFault):
    """An artifact was written by a tool whose conditional adapter does not exist.

    AAP 0.6.1 creates ``harness/lib/normalize/adapters/osv_scanner.py`` *"if and only if
    OSV-Scanner writes an artifact"*, and expects it writes none.  That expectation is not
    a certainty -- the provisioned runner passes ``--output-file`` unconditionally -- so an
    artifact present with no adapter must fail by **naming the missing module**, rather
    than falling into ``shape.py``'s generic unknown-shape halt, which would stop the run
    for a tool doing exactly what it was configured to do.
    """


def _fault(
    reason: str,
    message: str,
    /,
    **details: Any,
) -> ConfigurationFault:
    """Build a :class:`ConfigurationFault` with its details as keyword arguments.

    ``reason`` and ``message`` are positional-only, so a detail may legitimately be called
    ``reason`` or ``message`` -- which it is, whenever the details come straight from
    another module's own halt record (``shape.UnknownArtifactShape.details()`` carries a
    ``reason``, and Trivy's structural halt carries both).
    """
    return ConfigurationFault(reason, message, details=details)


def _halt(
    reason: str,
    message: str,
    /,
    **details: Any,
) -> NormalizeHalt:
    """Build a :class:`NormalizeHalt` with its details as keyword arguments.

    Positional-only for the same reason as :func:`_fault`: a foreign halt's details are
    passed through verbatim and may carry a ``reason`` or a ``message`` key of their own.
    """
    return NormalizeHalt(reason, message, details=details)


#: How many links of an exception chain are rendered for an unexpected error.  A chain is
#: normally one or two links; the bound exists so a pathological ``raise ... from`` cycle
#: of wrapped errors cannot decide how large this pipeline's record is.
UNEXPECTED_ERROR_CHAIN_MAX_DEPTH: int = 8

#: How many stack frames are rendered per link.  Frames are this repository's own source,
#: so they are safe to quote; the bound is against a runaway recursion, not against
#: content.
UNEXPECTED_ERROR_FRAME_LIMIT: int = 50


def _safe_exception_chain(error: BaseException) -> list[dict[str, Any]]:
    """Render an exception and its chain for the record, without any raw artifact string.

    ``traceback.format_exc()`` is not usable in a durable record.  Its final line is the
    exception's own ``str()``, and for every error this module can raise unexpectedly that
    string is composed from artifact-supplied text -- a ``KeyError`` naming an observed
    key, a ``ValueError`` quoting an observed value, a ``UnicodeDecodeError`` carrying the
    offending bytes.  Writing it unchanged is how a secret or a terminal control sequence
    reaches ``normalize-run.json`` on the one path nobody planned for.

    So the two halves are separated.  The **frames** come from
    :func:`traceback.format_tb`, which renders file, line, function and source line --
    all of it this repository's own authored source, never artifact content -- bounded by
    :data:`UNEXPECTED_ERROR_FRAME_LIMIT`.  The **message** is rendered by
    :func:`paths.safe_diagnostic`, so it is described (type, length, digest, bounded
    redacted excerpt) rather than shown.

    The chain is walked because ``raise ... from`` is this module's normal idiom: the
    adapter-contract halt chains the adapter's own error, and the artifact-supplied text
    lives on the *cause*, not on the wrapper.  ``__cause__`` is preferred over
    ``__context__`` where both exist, matching what :mod:`traceback` itself reports, and
    the walk keeps an identity set so a self-referential chain terminates.

    Returns:
        One mapping per link, outermost first, each carrying ``exception_type``,
        ``exception_module``, ``message`` (a :meth:`paths.SafeDiagnostic.as_dict`
        rendering), ``frames``, ``frames_truncated`` and ``linked_by``.
    """
    chain: list[dict[str, Any]] = []
    seen: set[int] = set()
    current: BaseException | None = error
    linked_by: str | None = None
    while current is not None and len(chain) < UNEXPECTED_ERROR_CHAIN_MAX_DEPTH:
        if id(current) in seen:
            chain.append(
                {
                    "exception_type": type(current).__name__,
                    "exception_module": type(current).__module__,
                    "message": None,
                    "frames": [],
                    "frames_truncated": False,
                    "linked_by": linked_by,
                    "note": "chain revisits an exception already rendered; walk stopped",
                }
            )
            break
        seen.add(id(current))
        frames = traceback.format_tb(
            current.__traceback__, limit=UNEXPECTED_ERROR_FRAME_LIMIT
        )
        total_frames = len(traceback.format_tb(current.__traceback__))
        chain.append(
            {
                "exception_type": type(current).__name__,
                "exception_module": type(current).__module__,
                "message": paths.safe_diagnostic(
                    str(current), context=f"{type(current).__name__} message"
                ).as_dict(),
                "frames": [frame.rstrip("\n") for frame in frames],
                "frames_truncated": total_frames > len(frames),
                "linked_by": linked_by,
            }
        )
        if current.__cause__ is not None:
            current, linked_by = current.__cause__, "cause"
        elif current.__context__ is not None and not current.__suppress_context__:
            current, linked_by = current.__context__, "context"
        else:
            break
    return chain


# --------------------------------------------------------------------------- #
# Small helpers -- timestamps, digests, and the runner's own side records      #
# --------------------------------------------------------------------------- #


def _utc_now() -> str:
    """Return the current UTC instant as an ISO-8601 string with a ``Z`` suffix.

    The only value in the run record that legitimately differs between two runs over
    identical inputs, which is what makes the determinism check *"both run records agree
    apart from timestamps"* a meaningful assertion.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _sha256(path: Path) -> str:
    """Return the sha256 of the file at ``path``, read in bounded chunks."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(_DIGEST_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_bounded_text(
    path: Path,
    limit: int,
    *,
    encoding: str = "utf-8",
    errors: str | None = None,
) -> tuple[str, bool]:
    """Read at most ``limit`` characters of ``path``, and say whether more existed.

    The bound is on the READ, not on a slice taken afterwards.  ``read_text()`` followed
    by ``text[:limit]`` allocates the whole file first, so a 40 MB stdout log costs 40 MB
    of memory to produce a 20,000-character excerpt and a hostile one costs whatever it is
    -- which is the shape of every unbounded ingestion in CWE-400.  Reading ``limit + 1``
    characters costs the excerpt plus one character and still answers the only question
    the caller has beyond the text itself: was there more?

    ``limit + 1`` rather than ``limit`` is what makes "was it cut" measurable rather than
    ambiguous.  A read that returns exactly ``limit`` characters cannot distinguish a file
    of exactly that length from one that continues, so the extra character is read and
    then discarded by the caller's slice.

    ``newline=""`` -- NO NEWLINE TRANSLATION, BECAUSE "VERBATIM" MEANS VERBATIM
    -------------------------------------------------------------------------
    Python's default (``newline=None``) is universal-newline mode, which rewrites every
    ``\\r\\n`` and every lone ``\\r`` to ``\\n`` on the way in.  This function feeds the two
    places that keep a tool's own text byte for byte -- :func:`_stream_record`, whose
    excerpt AAP 0.5.4 settles the absent-artifact verdict from *"using only the tool's own
    stated words"* and AAP 0.6.1 and 0.6.2 require *"verbatim"*, and
    :func:`_runner_status`, whose fields ``tool-status.md`` quotes *"in the tool's own
    words"* -- so a silent rewrite there is a rewritten verdict, and it was measured rather
    than supposed: a stream written as ``b"line one\\rOVERWRITTEN..."`` came back as
    ``b"line one\\nOVERWRITTEN..."``.

    It also decided what the control-character inventory can see.  Carriage return is the
    primary CWE-117 hazard -- it returns a terminal cursor to column zero so the following
    text overwrites the line already printed -- and under universal newlines no CR could
    ever be reported, because the reader had already destroyed it.  ``newline=""`` is what
    makes the disclosure this module publishes true of the bytes rather than of a
    translation of them.

    Nothing else changes.  The status parser splits with ``str.splitlines()``, which splits
    on ``\\r``, ``\\n`` and ``\\r\\n`` alike, so a CRLF status file parses into the same
    fields it did before; the no-work classifier matches sentences that contain no CR; and
    no committed stream or status file in this tree carries a CR at all, so the published
    record is byte-identical either way.  The one difference is arithmetic: a ``\\r\\n``
    now counts as the two characters it is against ``limit``.

    Returns:
        The text read, truncated to ``limit`` characters, and whether the file carried
        more than ``limit`` characters.

    Raises:
        OSError: the file could not be opened or read.
        UnicodeDecodeError: ``errors`` is ``None`` and the bytes are not valid ``encoding``.
    """
    with path.open("r", encoding=encoding, errors=errors, newline="") as handle:
        text = handle.read(limit + 1)
    if len(text) > limit:
        return text[:limit], True
    return text, False


def _file_record(path: Path) -> dict[str, Any]:
    """Describe one file for the record: its path, byte size and sha256.

    ``harness/artifacts/**`` is git-ignored, so this record is published through the
    per-file size-and-sha256 manifest (AAP 0.1.3) rather than by ``git add``.  Every file
    this module names is therefore described here well enough for a reader to verify it
    independently, and *"every file it names carries that file's byte size and sha256"* is
    a claim this function is the whole of.

    So there is no ``digest=False``: a named file that exists is always digested.  The
    digest is streamed in :data:`_DIGEST_CHUNK` chunks, which is what makes that
    affordable -- the largest stream this run names is a 40 MB stdout log and it hashes in
    well under a second, so there was never a size at which skipping the measurement was
    the cheaper option.

    Where a value is legitimately ``null`` the record says why, in a ``null_reason`` field
    beside it: a file that does not exist has no bytes to size and no bytes to digest, and
    a file that cannot be read says which error stopped it.  A reader therefore never has
    to guess whether a ``null`` means "not measured", "not measurable" or "measured as
    nothing", and the claim above and the data below cannot drift apart.
    """
    record: dict[str, Any] = {"path": str(path), "present": path.is_file()}
    if not record["present"]:
        record["bytes"] = None
        record["sha256"] = None
        record["null_reason"] = (
            "no file exists at this path, so there are no bytes to size and no bytes to "
            "digest; both measurements are null because the file is absent, not because "
            "it was skipped"
        )
        return record
    try:
        record["bytes"] = path.stat().st_size
        record["sha256"] = _sha256(path)
    except OSError as error:  # unreadable rather than absent -- say which
        record["bytes"] = None
        record["sha256"] = None
        record["read_error"] = f"{type(error).__name__}: {error}"
        record["null_reason"] = (
            "the file exists but could not be read, so neither measurement could be "
            f"taken: {type(error).__name__}: {error}"
        )
    return record


def _control_character_inventory(text: str | None) -> dict[str, Any]:
    """Measure -- without reproducing -- the control characters in untrusted verbatim text.

    The measured half of the residual-risk disclosure this module publishes instead of
    escaping (CWE-116, CWE-117).  The text this describes is a runner's own words or its
    own status lines, retained byte for byte because AAP 0.5.4, 0.6.1 and 0.6.2 require
    exactly that, and this inventory is what turns "these bytes are untrusted" from an
    assertion into a number a reader can check.

    WHAT IT REPORTS, AND WHAT IT DELIBERATELY DOES NOT
    -------------------------------------------------
    Per code point in the subject set: the ``U+XXXX`` form, its class, its standard
    abbreviation, how many times it occurs, and what it does to a consumer that renders it
    unescaped.  **The character itself never appears in the result**, which is the point:
    an inventory that quoted the ESC it is warning about would move the hazard from the
    stream into the record, and from the record into every terminal that prints it.
    ``character_reproduced: false`` is that claim stated in the data, and the adapter tests
    assert it against a stream carrying CR, ESC and NUL.

    It reports and it does not judge.  A stream with no controls at all still gets an
    inventory -- ``occurrences: 0`` over an empty ``by_code_point`` -- because "measured
    and none present" and "not measured" are different facts and a reader must be able to
    tell them apart.  A ``null`` text gets the same shape with ``measured: false`` and a
    ``null_reason``, matching the convention every other entry in this record follows.

    DETERMINISM AND COST
    --------------------
    ``set(text)`` then ``str.count`` per distinct in-set character: one C-level pass to
    find the distinct characters and at most seventy-seven more, rather than a Python-level
    loop over every character of a 20,000-character excerpt or a 4 MB status file.  The
    output is ordered by code point ascending and carries no timestamp, no ordering by
    first appearance and no host path, so an unchanged input produces a byte-identical
    inventory -- which is what keeps the run record comparable between runs.

    Args:
        text: the exact text embedded in the record beside this inventory, or ``None``
            where the entry embeds no text.

    Returns:
        The inventory, JSON-serialisable, with the same keys in both cases.
    """
    if text is None:
        return {
            "measured": False,
            "characters_measured": None,
            "occurrences": None,
            "distinct_code_points": None,
            "by_code_point": [],
            "subject_set": _CONTROL_SUBJECT_SET,
            "method": _CONTROL_METHOD,
            "character_reproduced": False,
            "escaping_contract": UNTRUSTED_TEXT_CONTRACT_KEY,
            "null_reason": (
                "no text is embedded at this entry, so there is nothing to measure; these "
                "nulls describe an absent value rather than a measurement that was skipped"
            ),
        }

    counts: dict[int, int] = {}
    for character in set(text):
        code_point = ord(character)
        if code_point in _CONTROL_TABLE:
            counts[code_point] = text.count(character)

    by_code_point: list[dict[str, Any]] = []
    for code_point in sorted(counts):
        control_class, abbreviation = _CONTROL_TABLE[code_point]
        by_code_point.append(
            {
                "code_point": f"U+{code_point:04X}",
                "class": control_class,
                "abbreviation": abbreviation,
                "count": counts[code_point],
                "rendering_effect": _CONTROL_RENDERING_EFFECTS.get(
                    code_point, _CONTROL_CLASS_EFFECTS[control_class]
                ),
            }
        )

    return {
        "measured": True,
        "characters_measured": len(text),
        "occurrences": sum(counts.values()),
        "distinct_code_points": len(counts),
        "by_code_point": by_code_point,
        "subject_set": _CONTROL_SUBJECT_SET,
        "method": _CONTROL_METHOD,
        "character_reproduced": False,
        "escaping_contract": UNTRUSTED_TEXT_CONTRACT_KEY,
    }


def _stream_record(path: Path, *, with_text: bool) -> dict[str, Any]:
    """Describe one runner stream, optionally carrying the tool's own words verbatim.

    The stream's byte size and sha256 are recorded **always**, because they describe the
    file and the file exists either way.  ``with_text`` governs one thing only: whether
    the tool's own words are embedded beside them.  It is true where the classification
    depends on the content -- the absent-artifact case, whose verdict AAP 0.5.4 settles
    *"using only the tool's own stated words"* -- and false where the artifact is present
    and the verdict comes from the artifact, in which case the stream is described,
    retained on disk, and cited by digest rather than copied into the record.

    The excerpt is bounded by :data:`TOOL_WORDS_EXCERPT_LIMIT` and says so when it was
    cut.  Between the bound, the digest and the retained file, a cap can never lose
    evidence without a reader seeing that it did.

    THE BOUND IS ON THE READ (CWE-400)
    ----------------------------------
    The excerpt limit used to be applied by slicing text that had already been read whole.
    The largest stream this run names is a 40,661,230-byte stdout log, so producing a
    20,000-character excerpt cost 40 MB of memory, and a stream this module did not write
    is a file whose size it does not control.  :func:`_read_bounded_text` reads
    ``TOOL_WORDS_EXCERPT_LIMIT + 1`` characters instead: the stored excerpt, the
    ``text_truncated`` flag and this function's whole observable behaviour are unchanged,
    and the allocation is now a property of the cap rather than of the file.

    Nothing is lost by the bound that was not already lost by the cap: the size and the
    sha256 above are measured over the whole file by :func:`_file_record`, the file itself
    is retained verbatim on disk at the path named, and every consumer of these words --
    :func:`_classify_no_work` included -- reads the stored excerpt rather than the file.

    THE WORDS ARE KEPT VERBATIM AND THE HAZARD IS MEASURED BESIDE THEM
    (CWE-116, CWE-117)
    -----------------------------------------------------------------
    ``text`` is the single largest piece of untrusted text this record embeds, and it is
    embedded **unescaped and unrewritten**.  That is mandatory rather than convenient: AAP
    0.5.4 settles the absent-artifact verdict *"using only the tool's own stated words"*
    and requires the parser error *"retained verbatim"*, and AAP 0.6.1 and 0.6.2 require an
    absent artifact's stderr *"verbatim"* and a reduced-reach condition *"in the tool's own
    words"*.  Escaping here would change the bytes a human adjudicates the halt from, so
    the byte-for-byte excerpt stays and the risk is disclosed instead.

    Two things carry the disclosure.  ``text_control_characters`` is the measurement --
    :func:`_control_character_inventory` over exactly the characters stored, naming which
    control code points occur and how many times each, and never reproducing one.
    ``text_escaping_contract`` names the run record's own
    :data:`UNTRUSTED_TEXT_CONTRACT_KEY` block, which states the whole contract in one
    place: JSON is a safe container for these bytes, and a consumer rendering them into
    Markdown, HTML or a terminal must escape them first.  **This module renders none of
    them** -- it writes no Markdown and emits no fenced block -- so the obligation belongs
    to whatever produces ``tool-status.md`` from this record.

    The residual risk is therefore stated rather than removed: the text is hostile-capable,
    and nothing here makes it safe to paste into a rendered document.  It is latent rather
    than live -- no committed stream carries such a payload -- and the inventory is what
    lets a reader confirm that for themselves instead of taking it on trust.
    """
    record = _file_record(path)
    record["text"] = None
    record["text_truncated"] = False
    # Every stream entry carries the same two disclosure keys whichever branch below runs,
    # so a consumer never has to test for the inventory's presence: an entry embedding no
    # text carries the measured=false form with its null_reason, and the one branch that
    # does embed text replaces it with the measurement over exactly those characters.
    record["text_control_characters"] = _control_character_inventory(None)
    record["text_escaping_contract"] = UNTRUSTED_TEXT_CONTRACT_KEY
    if not with_text:
        record["text_null_reason"] = (
            "the artifact is present, so its classification comes from the artifact "
            "rather than from the tool's own words; this stream is measured by size and "
            "sha256 above and retained verbatim on disk at the path named, so nothing is "
            "lost by not embedding it here"
        )
        return record
    if not record["present"]:
        record["text_null_reason"] = (
            "no file exists at this path, so there are no words to carry"
        )
        return record
    try:
        text, truncated = _read_bounded_text(
            path, TOOL_WORDS_EXCERPT_LIMIT, errors="replace"
        )
    except OSError as error:
        record["read_error"] = f"{type(error).__name__}: {error}"
        record["text_null_reason"] = (
            f"the stream could not be read: {type(error).__name__}: {error}"
        )
        return record
    record["text"] = text
    # Measured over exactly the characters stored above, so the inventory describes the
    # embedded excerpt rather than the whole file: a reader escaping what this record
    # carries needs the hazard in what it carries (CWE-116, CWE-117).
    record["text_control_characters"] = _control_character_inventory(text)
    record["text_read_bounded_at"] = TOOL_WORDS_EXCERPT_LIMIT
    if truncated:
        record["text_truncated"] = True
        record["text_excerpt_limit"] = TOOL_WORDS_EXCERPT_LIMIT
    return record


@dataclass(frozen=True)
class NoWorkStatement:
    """One exact sentence a named tool is documented to print when it has no work.

    A *classifier*, not a heuristic.  ``sentence`` is the tool's own wording and is
    matched literally, case-sensitively, as a substring of one stream -- a substring
    because a runner's stream legitimately carries the sentence inside its own framing
    (``harness/lib/scope.sh``'s begin/finish lines surround every invocation), and
    literally because the whole value of the classifier is that it recognises *this*
    statement rather than text that resembles one.

    ``exit_codes`` records the exit codes the statement has been observed with.  It is
    **corroboration and never the test**: AAP 0.5.4 settles this classification *"using
    only the tool's own stated words"*, so gating on a code would decide the case on
    something other than the words.  Agreement or difference is recorded either way, which
    is what lets a reader see that the recorded outcome was reproduced rather than assumed.

    ``authority`` names where the wording comes from, so the table is checkable against a
    primary source rather than against this file.
    """

    tool: str
    sentence: str
    exit_codes: tuple[int, ...]
    authority: str

    def matches(self, text: str) -> bool:
        """Return ``True`` where *text* carries this statement verbatim."""
        return self.sentence in text

    def as_dict(self) -> dict[str, Any]:
        """Return the statement for the structured run record."""
        return {
            "tool": self.tool,
            "sentence": self.sentence,
            "exit_codes": list(self.exit_codes),
            "authority": self.authority,
        }


#: The exact no-work statements this pipeline recognises, one entry per canonical tool.
#:
#: Eight of the nine tools carry an EMPTY tuple, and that is the substance of the table
#: rather than a gap in it.  AAP 0.5.4 admits the ``absent`` status only where *"the tool
#: stated a no-work reason in its own output"*, and only OSV-Scanner has such a statement:
#: it resolves zero packages over a scope holding no dependency manifest, prints its own
#: sentence and exits 128 (AAP 0.2.2, from the tool's own issue tracker).  Every other
#: runner is expected to write an artifact, so an absence from one of them is a condition
#: nobody has an account of -- and a stack trace, a permission error or a truncated crash
#: message is text, not an account.  Treating any non-empty stream as a statement is what
#: turns a failure into a continuing zero, and a zero-row tool is invisible in a row-only
#: ``findings.json`` by construction.
#:
#: A tool acquiring a documented no-work statement is a table entry, added with its
#: primary source.  It is deliberately not something the code can infer.
_NO_WORK_STATEMENTS: Mapping[str, tuple[NoWorkStatement, ...]] = MappingProxyType(
    {
        "opengrep": (),
        "semgrep": (),
        "datadog-static-analyzer": (),
        "gitleaks": (),
        "checkov": (),
        "trivy": (),
        "osv-scanner": (
            NoWorkStatement(
                tool="osv-scanner",
                sentence="No package sources found",
                exit_codes=(128,),
                authority=(
                    "OSV-Scanner's own stated zero-package behaviour: it prints \"No "
                    "package sources found, --help for usage information.\" and exits 128 "
                    "when it resolves no package source. Long-standing documented "
                    "behaviour rather than a crash -- google/osv-scanner issues 348 and "
                    "93, cited in AAP 0.2.2 -- and AAP 0.2.1 records why this scope "
                    "reaches it: the one manifest-shaped file in the twelve globs, "
                    "core/src/main/resources/org/apache/spark/ui/static/package.json, "
                    "carries a name, a license and a type and no dependencies block."
                ),
            ),
        ),
        "dependency-check": (),
        "joern": (),
    }
)


def no_work_statements(tool: str) -> tuple[NoWorkStatement, ...]:
    """Return the no-work statements recognised for *tool*, possibly none.

    Raises ``KeyError`` for an identifier outside the nine, which is a caller bug rather
    than an artifact condition: the table is keyed by the same canonical vocabulary
    ``_verify_vocabularies`` has already established agreement over.
    """
    return _NO_WORK_STATEMENTS[tool]


def _classify_no_work(
    tool: str,
    tool_words: Mapping[str, Any],
    runner_status: Mapping[str, Any],
) -> dict[str, Any]:
    """Decide whether *tool*'s streams carry a recognised no-work statement.

    The classification AAP 0.5.4 requires, and it is deliberately narrow.  Both streams
    are searched, in the fixed order ``stderr`` then ``stdout``, because the two documented
    accounts of OSV-Scanner's outcome disagree about which one carries the sentence; the
    first stream carrying a recognised statement is named in the record, and both streams
    are described either way whether or not they matched.

    Three outcomes, all of them recorded rather than inferred:

    * **no words at all** -- neither stream carries non-whitespace text.  There is nothing
      to classify, and the caller halts under
      :data:`HALT_ABSENT_WITHOUT_STATED_REASON`.
    * **words that match no statement** -- the streams say something and it is not one of
      the sentences this pipeline recognises for this tool.  The caller halts under
      :data:`HALT_ABSENT_WITHOUT_NO_WORK_STATEMENT`, with both streams preserved verbatim
      so a reader adjudicates from the tool's own text rather than from this verdict.
    * **a recognised statement** -- the ``absent`` status, zero rows, run continues.

    The exit code is compared against the codes the statement is recorded with and the
    agreement is reported, never enforced: the words decide.  A tool with an empty entry in
    the table can only reach the second outcome, which is the point of its emptiness.
    """
    streams: Mapping[str, Any] = tool_words.get("streams") or {}
    statements = no_work_statements(tool)
    record: dict[str, Any] = {
        "tool": tool,
        "recognised_statements": [statement.as_dict() for statement in statements],
        "words_present": bool(tool_words.get("stated_reason_present")),
        "words_stream": tool_words.get("stated_reason_stream"),
        "classified": False,
        "matched_sentence": None,
        "matched_stream": None,
        "matched_statement": None,
        "exit_code": runner_status.get("exit_code"),
        "exit_status": runner_status.get("exit_status"),
        "exit_code_agrees_with_statement": None,
        "streams_searched": [],
        "basis": None,
    }

    for stream in ("stderr", "stdout"):
        text = (streams.get(stream) or {}).get("text")
        searched = {
            "stream": stream,
            "text_present": isinstance(text, str) and bool(text.strip()),
            "text_truncated": bool((streams.get(stream) or {}).get("text_truncated")),
            "matched": False,
        }
        if isinstance(text, str) and not record["classified"]:
            for statement in statements:
                if statement.matches(text):
                    searched["matched"] = True
                    record["classified"] = True
                    record["matched_sentence"] = statement.sentence
                    record["matched_stream"] = stream
                    record["matched_statement"] = statement.as_dict()
                    break
        record["streams_searched"].append(searched)

    if record["classified"]:
        codes = tuple(record["matched_statement"]["exit_codes"])
        code = record["exit_code"]
        record["exit_code_agrees_with_statement"] = (
            None if not isinstance(code, int) else code in codes
        )
        record["basis"] = (
            f"the tool's own words on {record['matched_stream']}: "
            f"{record['matched_sentence']!r}"
        )
    elif not record["words_present"]:
        record["basis"] = "no words on either stream; there is nothing to classify"
    else:
        record["basis"] = (
            "the streams carry text and none of it is a no-work statement this pipeline "
            f"recognises for {tool!r}"
            + (
                " -- no statement is recognised for this tool at all, because it is "
                "expected to write an artifact"
                if not statements
                else ""
            )
        )
    return record


def _tool_words(log_dir: Path | None, tool: str, *, with_text: bool) -> dict[str, Any]:
    """Collect a runner's own streams, and whether they carry any words at all.

    ``stated_reason`` is the first non-empty stream's text, stderr preferred, and it means
    exactly what it says: *some* text was written.  It is **evidence, not a verdict** --
    whether those words state a no-work reason is :func:`_classify_no_work`'s decision
    against the exact-sentence table, because a stack trace, a permission error and a
    truncated crash message are all non-empty and none of them is an account of a tool
    having nothing to do (AAP 0.5.4).

    Both streams are collected because the two documented accounts of OSV-Scanner's
    zero-package outcome disagree about which one carries the sentence -- the runbook
    reports it on stdout, the environment record on stderr.  Both are described either way,
    and the classifier searches both rather than only the one this function preferred.

    ``stated_reason`` IS one stream's verbatim text, so it inherits that text's residual
    risk in full: it is untrusted, unescaped, and mandatory to keep that way (AAP 0.5.4,
    0.6.1, 0.6.2 -- see :func:`_stream_record`).  It is published beside
    ``stated_reason_control_characters``, which is the chosen stream's inventory projected
    rather than measured again, and ``stated_reason_escaping_contract``, which names the
    run record's contract block for a consumer that renders it (CWE-116, CWE-117).
    """
    streams: dict[str, Any] = {}
    for stream in ("stderr", "stdout"):
        if log_dir is None:
            # No log directory was resolved, so this entry names no file at all. The
            # nulls are stated as such rather than left to look like an unmeasured
            # file: the record's claim covers every file it names, and this names none.
            streams[stream] = {
                "path": None,
                "present": False,
                "bytes": None,
                "sha256": None,
                "null_reason": (
                    "no log directory was resolved for this run, so this entry names no "
                    "file; there is nothing to size or digest rather than something that "
                    "went unmeasured"
                ),
                "text": None,
                "text_truncated": False,
                # The same two disclosure keys _stream_record sets, so an entry naming no
                # file has the same shape as one that does and a consumer reading the
                # inventory never has to branch on which produced the entry.
                "text_control_characters": _control_character_inventory(None),
                "text_escaping_contract": UNTRUSTED_TEXT_CONTRACT_KEY,
                "text_null_reason": (
                    "no log directory was resolved, so there is no stream to carry"
                ),
            }
            continue
        streams[stream] = _stream_record(
            log_dir / f"{tool}.{stream}.log", with_text=with_text
        )

    stated: str | None = None
    stated_stream: str | None = None
    for stream in ("stderr", "stdout"):
        text = streams[stream].get("text")
        if isinstance(text, str) and text.strip():
            stated = text
            stated_stream = stream
            break

    return {
        "streams": streams,
        "stated_reason": stated,
        "stated_reason_stream": stated_stream,
        "stated_reason_present": stated is not None,
        # The stated reason IS one stream's text, so its control-character inventory is
        # that stream's inventory projected here rather than a second measurement of the
        # same characters -- the same discipline _network_fetch_disclosure follows for the
        # status fields it projects. The source is named either way, so a reader can see
        # which stream the numbers were taken over instead of inferring it (CWE-116,
        # CWE-117; see UNTRUSTED_TEXT_CONTRACT_KEY in the run record).
        "stated_reason_control_characters": (
            _control_character_inventory(None)
            if stated_stream is None
            else streams[stated_stream]["text_control_characters"]
        ),
        "stated_reason_control_characters_source": (
            "no stream carried words, so there is no inventory to project"
            if stated_stream is None
            else f"streams.{stated_stream}.text_control_characters"
        ),
        "stated_reason_escaping_contract": UNTRUSTED_TEXT_CONTRACT_KEY,
    }


#: A status line whose first non-space character is this is a comment and is never a
#: field, however many ``=`` characters it carries.
_STATUS_COMMENT_PREFIX: str = "#"

#: The syntax a status key must have, as ``harness/lib/scope.sh``'s ``scope_finish``
#: writes it: a plain field name, at column zero, with no surrounding whitespace.
_STATUS_KEY_PATTERN: str = r"[A-Za-z_][A-Za-z0-9_]*"
_STATUS_KEY_RE = re.compile(rf"\A{_STATUS_KEY_PATTERN}\Z")

#: How much of a rejected line the record quotes.  Status files carry prose lines
#: hundreds of characters long; the count of rejected lines is always exact and the
#: excerpt is bounded, and an excerpt that was cut says so.
_STATUS_LINE_EXCERPT_LIMIT: int = 200

#: Why a line carrying an ``=`` was not made a field.  A closed vocabulary, so a reader
#: of the run record can enumerate the reasons rather than parse a sentence.
STATUS_LINE_NO_SEPARATOR: str = "no-key-value-separator"
STATUS_LINE_INVALID_KEY: str = "text-before-the-first-equals-is-not-a-field-name"

#: The defects a status file can carry, by name.  Both are conditions in the *file*
#: rather than in this parser, and both are recorded with their counts.
STATUS_DEFECT_DUPLICATE_KEY: str = "duplicate-key"
STATUS_DEFECT_UNPARSABLE_LINE: str = "unparsable-line"
#: The status file is larger than :data:`STATUS_FILE_BYTE_LIMIT`, so it was measured and
#: retained but not parsed.  A defect rather than a halt: a status file is log-side
#: evidence about one runner, and a file grown to gigabytes says something is wrong with
#: that runner's stream capture, not with the dataset the artifacts produce (CWE-400).
STATUS_DEFECT_FILE_TOO_LARGE: str = "file-exceeds-byte-cap"
#: A field's value is all digits but carries more than :data:`STATUS_NUMERIC_DIGIT_LIMIT`
#: of them, so it was not converted to an integer.  ``int()`` raises ``ValueError`` above
#: CPython's 4,300-digit conversion limit, which ``str.isdigit()`` does not predict; the
#: literal is retained verbatim beside the null and the field is treated exactly as an
#: unreadable one (CWE-770).
STATUS_DEFECT_NUMERIC_LITERAL_TOO_LONG: str = "numeric-literal-exceeds-digit-cap"


#: What a status field's value must be, exactly, to be converted to an integer: ASCII
#: digits with an optional leading sign and nothing else.  Deliberately narrower than
#: ``str.isdigit()``, which the two conversions below used to gate on and which is true for
#: characters ``int()`` refuses -- ``"\u00b2".isdigit()`` is ``True`` and ``int("\u00b2")``
#: raises ``ValueError``.  A status file is provisioning-supplied text, so a predicate that
#: does not imply convertibility is a ValueError waiting for one (CWE-770).
_STATUS_INTEGER_RE = re.compile(r"-?[0-9]+\Z")


def _status_integer(
    literal: str | None, *, field: str, signed: bool
) -> tuple[int | None, dict[str, Any] | None]:
    """Convert one status field's literal to an integer, refusing an unbounded one.

    Three outcomes, and they are different things a reader should be able to tell apart:

    * the literal is absent, or is not a plain ASCII integer -- ``(None, None)``.  There is
      no number here and no defect either; a status file legitimately carries
      ``artifact_bytes=MISSING``, and a code that cannot be read is what
      :data:`EXIT_STATUS_TIMEOUT` already names.
    * the literal is a plain integer of at most :data:`STATUS_NUMERIC_DIGIT_LIMIT` digits
      -- ``(value, None)``.
    * the literal is a plain integer with MORE digits than that -- ``(None, defect)``.  It
      is not converted, the defect names the observed digit count and the cap, and the
      literal itself is retained verbatim beside the null by the caller.

    The third case is the bound.  ``int()`` raises ``ValueError`` above CPython's
    4,300-digit ``sys.set_int_max_str_digits`` limit, and the conversion of a shorter but
    still enormous literal is quadratic in its length, so a status file carrying a
    megabyte of digits either crashes the conversion or spends real time on it -- neither
    of which is a way to read an exit code (CWE-400, CWE-770).  ``signed`` is per field
    because ``exit_code`` may legitimately be negative and ``elapsed_seconds`` may not, and
    this function must not widen either.
    """
    if literal is None:
        return None, None
    candidate = literal.strip()
    if not _STATUS_INTEGER_RE.fullmatch(candidate):
        return None, None
    if not signed and candidate.startswith("-"):
        return None, None
    digits = candidate.lstrip("-")
    if len(digits) > STATUS_NUMERIC_DIGIT_LIMIT:
        return None, {
            "class": STATUS_DEFECT_NUMERIC_LITERAL_TOO_LONG,
            "count": 1,
            "field": field,
            "observed_digits": len(digits),
            "digit_cap": STATUS_NUMERIC_DIGIT_LIMIT,
            "handling": (
                "the literal is retained verbatim beside a null value and was not "
                "converted; the field is treated exactly as an unreadable one"
            ),
        }
    return int(candidate), None


def _status_line_excerpt(line: str) -> str:
    """Return a bounded excerpt of a rejected status line, saying when it was cut."""
    if len(line) <= _STATUS_LINE_EXCERPT_LIMIT:
        return line
    return line[:_STATUS_LINE_EXCERPT_LIMIT] + f"... [cut at {_STATUS_LINE_EXCERPT_LIMIT}]"


def _runner_status(log_dir: Path | None, tool: str) -> dict[str, Any]:
    """Read ``<tool>.status`` -- the key=value record ``scope_finish`` writes.

    ``harness/lib/scope.sh`` writes ``tool``, ``exit_code``, ``elapsed_seconds``,
    ``artifact``, ``artifact_bytes`` (or the literal ``MISSING``), ``scan_root`` and
    ``scan_root_source``.  Those lines are how the root and base recorded in
    ``runner-metadata.json`` become independently checkable; nothing here re-derives them.

    The exit code is read as a fact and used for nothing else (AAP 0.5.4).  Where the file
    exists but carries no readable code, the status is :data:`EXIT_STATUS_TIMEOUT` -- the
    single name AAP 0.8.1 gives a termination that produced no exit code, which names the
    status and does not excuse a missing artifact.  Where there is no status file at all
    the status is :data:`EXIT_STATUS_UNRECORDED`, which is a different thing and is said to
    be.

    WHAT COUNTS AS A FIELD, AND WHY THE RULE IS THIS NARROW
    ------------------------------------------------------
    A ``.status`` file in this tree is a runner-written key=value block **plus** the
    composed record around it: a commented header, fenced command examples, indented
    prose.  Any of those can contain an ``=`` -- ``dependency-check.status`` carries six
    such comment lines -- so splitting every line on its first ``=`` and assigning the
    result invents fields out of prose, and a duplicate assignment silently keeps the
    last one.  Between them those two behaviours let a comment shadow authoritative
    state: a line in an indented example whose text before the ``=`` strips to
    ``exit_code`` would overwrite the runner's own value with a fragment of documentation.

    So a line becomes a field only where all of this holds, and nothing is inferred:

    * it is not blank and its first non-space character is not ``#``;
    * it carries an ``=``;
    * the text before the first ``=`` is a plain field name at column zero --
      :data:`_STATUS_KEY_RE`, no leading or trailing space, which is exactly what
      ``scope_finish`` writes.  Requiring column zero is what closes the indented-example
      hole, and it costs nothing: across all nine committed status files it excludes zero
      lines that were ever fields.
    * the key has not been seen before.  A **duplicate is a defect in the status file**,
      not a value to resolve: the FIRST occurrence is kept, because it is the one the
      runner wrote before any composed commentary was appended, and the duplicate is
      recorded with its line number and value under ``duplicate_fields`` so nothing is
      lost silently.

    Every line the rule rejects is recorded under ``unparsed_lines`` with its number, an
    excerpt and the reason, and both conditions are also named in ``defects`` with their
    counts -- so a reader sees "this file has 6 lines that look like fields and are not"
    rather than a field list quietly a few entries longer than the runner's.

    WHAT THIS PARSER REFUSES TO ALLOCATE (CWE-400, CWE-770)
    ------------------------------------------------------
    Two bounds, both on a file this module did not write.  A file above
    :data:`STATUS_FILE_BYTE_LIMIT` is measured, retained and named as a
    :data:`STATUS_DEFECT_FILE_TOO_LARGE` defect rather than parsed -- the largest committed
    status file is 278 bytes, so a file above 4 MB is a stream capture that went wrong
    rather than a longer account of one invocation.  And a numeric field is converted only
    through :func:`_status_integer`, which bounds the literal's digit count: the two
    conversions here used to gate on ``str.isdigit()`` and then call ``int()``, and that
    pair raises ``ValueError`` both for a literal above CPython's 4,300-digit conversion
    limit and for a character like ``"\u00b2"`` that is a digit but not convertible.
    Neither bound loses evidence: the file's size and sha256 are recorded, the file stays
    on disk, and a refused literal is retained verbatim beside its null.  A refused file's
    ``exit_status`` is :data:`EXIT_STATUS_UNRECORDED` -- nothing was recorded *from* it --
    and the defect beside it is what distinguishes that from the no-file-at-all case.

    THE FIELDS ARE THE RUNNER'S OWN TEXT, KEPT AS WRITTEN (CWE-116, CWE-117)
    ----------------------------------------------------------------------
    Every value under ``fields``, every ``unparsed_lines[].excerpt`` and every
    ``duplicate_fields[].value`` is a string the runner wrote, retained byte for byte
    because ``tool-status.md`` quotes the runner's own reduced-reach condition *"in the
    tool's own words"* (AAP 0.6.1, 0.6.2).  So the same disclosure a stream's words carry
    applies here: ``text_control_characters`` is one inventory over the whole file those
    strings are cut from, and ``text_escaping_contract`` names the run record's contract
    block for the consumer that renders them.  This parser escapes nothing and this module
    writes no Markdown; the obligation is the renderer's.
    """
    path = None if log_dir is None else log_dir / f"{tool}.status"
    record: dict[str, Any] = {
        "path": None if path is None else str(path),
        "present": bool(path is not None and path.is_file()),
        # This entry names a file, so it carries that file's measurement like every
        # other entry that names one: the record's self-describing claim is that every
        # named file which exists is sized and digested, and a parsed record is still a
        # named file.  Filled in below once the path is known to exist.
        "bytes": None,
        "sha256": None,
        "fields": {},
        "field_count": 0,
        "comment_lines": 0,
        "blank_lines": 0,
        "unparsed_lines": [],
        "unparsed_line_count": 0,
        "duplicate_fields": [],
        "duplicate_field_count": 0,
        "defects": [],
        "parser_contract": (
            "A field is a line whose first non-space character is not '#', which carries "
            "an '=', and whose text before the first '=' is a plain field name at column "
            "zero (" + _STATUS_KEY_PATTERN + ") not already seen. A duplicate key is a "
            "defect in the status file: the first occurrence is kept and the duplicate is "
            "recorded under duplicate_fields. Every other line carrying an '=' is recorded "
            "under unparsed_lines rather than becoming a field. A numeric field is "
            "converted only where its value is ASCII digits with an optional leading sign "
            f"and at most {STATUS_NUMERIC_DIGIT_LIMIT} of them; a longer literal is "
            "retained verbatim beside a null value and recorded under defects. A file "
            f"above {STATUS_FILE_BYTE_LIMIT} bytes is measured and retained but not "
            "parsed at all."
        ),
        # The status file is the OTHER untrusted verbatim text this record embeds: every
        # value under `fields`, every rejected line's `excerpt` and every duplicate's
        # `value` is a runner-written string kept byte for byte, and tool-status.md is
        # rendered from them. Measured with the same inventory as a stream's words and
        # pointed at the same contract, so both sites disclose the same hazard in the same
        # shape (CWE-116, CWE-117). Set here so every early return below carries the keys.
        "text_control_characters": _control_character_inventory(None),
        "text_escaping_contract": UNTRUSTED_TEXT_CONTRACT_KEY,
        "exit_code": None,
        "exit_code_literal": None,
        "exit_status": EXIT_STATUS_UNRECORDED,
        "elapsed_seconds": None,
        "artifact_bytes_literal": None,
        "scan_root": None,
        "scan_root_source": None,
    }
    if path is None or not record["present"]:
        record["null_reason"] = (
            "no status file exists at this path (or no log directory was given), so "
            "there are no bytes to size and no bytes to digest; both measurements are "
            "null because the file is absent, not because they were skipped"
        )
        return record
    try:
        record["bytes"] = path.stat().st_size
        record["sha256"] = _sha256(path)
    except OSError as error:  # unreadable rather than absent -- say which
        record["read_error"] = f"{type(error).__name__}: {error}"
        record["null_reason"] = (
            "the status file exists but could not be measured: "
            f"{type(error).__name__}: {error}"
        )
        return record

    # Bounded before it is read (CWE-400). A status file is one runner's key=value account
    # of its own invocation -- the largest committed one is 278 bytes -- so a file above
    # STATUS_FILE_BYTE_LIMIT is not a bigger status file, it is a stream capture that went
    # wrong. It is measured and retained above and named as a defect here rather than
    # parsed, and rather than halting the run: this is log-side evidence about one tool,
    # and the dataset comes from the artifacts.
    size = record["bytes"]
    if isinstance(size, int) and size > STATUS_FILE_BYTE_LIMIT:
        record["defects"] = [
            {
                "class": STATUS_DEFECT_FILE_TOO_LARGE,
                "count": 1,
                "observed_bytes": size,
                "byte_cap": STATUS_FILE_BYTE_LIMIT,
                "handling": (
                    "the file was measured by size and sha256 and is retained verbatim on "
                    "disk at the path named, but it was not parsed: no field, exit code, "
                    "elapsed time or scan root is taken from it. Every value below is "
                    "therefore null because the file was refused, not because it was empty."
                ),
            }
        ]
        record["null_reason"] = (
            f"the status file is {size} bytes, above the {STATUS_FILE_BYTE_LIMIT}-byte "
            "cap on a runner's key=value status record, so it was not parsed"
        )
        return record

    try:
        text, text_truncated = _read_bounded_text(
            path, STATUS_FILE_BYTE_LIMIT, errors="replace"
        )
    except OSError as error:
        record["read_error"] = f"{type(error).__name__}: {error}"
        return record
    if text_truncated:
        # The file grew past the cap between the stat above and this read. Treated exactly
        # as the over-cap case: a partially parsed status record would carry fields from a
        # file whose end nobody saw.
        record["defects"] = [
            {
                "class": STATUS_DEFECT_FILE_TOO_LARGE,
                "count": 1,
                "observed_bytes": size,
                "byte_cap": STATUS_FILE_BYTE_LIMIT,
                "handling": (
                    "the file exceeded the cap on read even though its recorded size did "
                    "not, so it grew between the two; it was not parsed"
                ),
            }
        ]
        record["null_reason"] = (
            "the status file exceeded the "
            f"{STATUS_FILE_BYTE_LIMIT}-byte cap while being read, so it was not parsed"
        )
        return record

    # Measured over the text every field, excerpt and duplicate value below is cut from,
    # so one inventory covers all of them rather than one per string: a consumer escaping
    # this record's status side needs to know what is in the file it was taken from.
    record["text_control_characters"] = _control_character_inventory(text)

    fields: dict[str, str] = {}
    first_line_of: dict[str, int] = {}
    duplicates: list[dict[str, Any]] = []
    unparsed: list[dict[str, Any]] = []
    comment_lines = 0
    blank_lines = 0
    for number, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            blank_lines += 1
            continue
        if stripped.startswith(_STATUS_COMMENT_PREFIX):
            comment_lines += 1
            continue
        key, separator, value = line.partition("=")
        if not separator:
            unparsed.append(
                {
                    "line": number,
                    "excerpt": _status_line_excerpt(line),
                    "reason": STATUS_LINE_NO_SEPARATOR,
                }
            )
            continue
        if not _STATUS_KEY_RE.match(key):
            unparsed.append(
                {
                    "line": number,
                    "excerpt": _status_line_excerpt(line),
                    "reason": STATUS_LINE_INVALID_KEY,
                }
            )
            continue
        if key in fields:
            duplicates.append(
                {
                    "line": number,
                    "key": key,
                    "value": value,
                    "first_seen_on_line": first_line_of[key],
                    "kept": "the first occurrence",
                }
            )
            continue
        fields[key] = value
        first_line_of[key] = number

    defects: list[dict[str, Any]] = []
    if duplicates:
        defects.append(
            {
                "class": STATUS_DEFECT_DUPLICATE_KEY,
                "count": len(duplicates),
                "keys": sorted({entry["key"] for entry in duplicates}),
                "handling": (
                    "the first occurrence is the value used; every duplicate is recorded "
                    "under duplicate_fields with its line number and value"
                ),
            }
        )
    if unparsed:
        defects.append(
            {
                "class": STATUS_DEFECT_UNPARSABLE_LINE,
                "count": len(unparsed),
                "handling": (
                    "recorded under unparsed_lines with the reason; no such line becomes "
                    "a field, so prose carrying an '=' cannot shadow runner-written state"
                ),
            }
        )

    record["fields"] = fields
    record["field_count"] = len(fields)
    record["comment_lines"] = comment_lines
    record["blank_lines"] = blank_lines
    record["unparsed_lines"] = unparsed
    record["unparsed_line_count"] = len(unparsed)
    record["duplicate_fields"] = duplicates
    record["duplicate_field_count"] = len(duplicates)
    record["defects"] = defects

    # Both conversions go through _status_integer: bounded by digit count, ASCII-only, and
    # signed only where the field legitimately is. A literal it refuses leaves the value
    # null and the literal verbatim, which is what the two branches below already did for
    # an unreadable code -- the bound adds a named defect rather than a new behaviour.
    numeric_defects: list[dict[str, Any]] = []
    literal = fields.get("exit_code")
    record["exit_code_literal"] = literal
    exit_code, exit_code_defect = _status_integer(literal, field="exit_code", signed=True)
    if exit_code_defect is not None:
        numeric_defects.append(exit_code_defect)
    if exit_code is not None:
        record["exit_code"] = exit_code
        record["exit_status"] = EXIT_STATUS_EXITED
    else:
        # A status file with no readable code is a process that ended without one.
        record["exit_status"] = EXIT_STATUS_TIMEOUT
    elapsed_seconds, elapsed_defect = _status_integer(
        fields.get("elapsed_seconds"), field="elapsed_seconds", signed=False
    )
    if elapsed_defect is not None:
        numeric_defects.append(elapsed_defect)
    if elapsed_seconds is not None:
        record["elapsed_seconds"] = elapsed_seconds
    if numeric_defects:
        record["defects"] = list(record["defects"]) + numeric_defects
    record["artifact_bytes_literal"] = fields.get("artifact_bytes")
    record["scan_root"] = fields.get("scan_root")
    record["scan_root_source"] = fields.get("scan_root_source")
    return record


#: Status-field name fragments that carry a statement about data a tool consulted or
#: fetched at invocation time. Matched by *name* against the runner's own status fields,
#: because the runner is the only witness to what its tool did on the network and the
#: normalizer must not infer network activity from an artifact's contents.
_FETCH_FIELD_FRAGMENTS: tuple[str, ...] = (
    "network",
    "fetch",
    "feed_state",
    "feed_identity",
    "database",
    "db_update",
    "skip_download",
    "noupdate",
    "ruleset_source",
    "ruleset_location",
    "ruleset_commit",
    "ruleset_identity",
    "ruleset_sha256",
    "data_source",
    "queried",
    "offline",
    "reproducibility_gap",
    "rate_limit",
)


def _network_fetch_disclosure(runner_status: Mapping[str, Any]) -> dict[str, Any]:
    """Project the runner's own statements about an invocation-time fetch.

    AAP 0.5.4 requires the record to disclose *"any network fetch a tool performed at
    invocation time"*, because a rule set or feed fetched with no digest available is a
    reproducibility gap the dataset must carry rather than absorb.  A tool that assembles
    its rules from an API mid-scan can contribute a large share of the rows while nothing
    on disk identifies the rules those rows came from, so the disclosure has to travel
    beside the count for the count to mean anything later.

    The statements are the runner's, not this module's. ``runner_status['fields']`` is
    already carried verbatim in the record; this selects the subset that speaks to what
    was consulted or fetched, so the disclosure is addressable rather than buried among a
    couple of hundred status lines. It is a projection of one measurement, with its source
    named, and never a second measurement of it.

    Where a runner said nothing, that is recorded as *no statement found* -- which is a
    strictly weaker claim than "no fetch occurred", and deliberately so: the normalizer
    observes artifacts, not sockets, and inventing the stronger claim would be a
    fabricated measurement.
    """
    fields = runner_status.get("fields") or {}
    statements = {
        name: value
        for name, value in fields.items()
        if any(fragment in name.lower() for fragment in _FETCH_FIELD_FRAGMENTS)
    }
    return {
        "source": runner_status.get("path"),
        "status_fields_scanned": len(fields),
        "statements": statements,
        "statement_count": len(statements),
        "note": (
            "Selected by field name from the runner's own status record, which is carried "
            "verbatim in 'runner_status' above; this is that one measurement projected, "
            "not a second one. An empty 'statements' means the runner stated nothing "
            "about an invocation-time fetch -- which is not the same claim as no fetch "
            "having occurred, and is left as the weaker statement on purpose."
        ),
    }


def _same_root(left: str, right: str) -> bool:
    """Return whether two recorded roots name the same directory.

    Compared both as normalised path strings and as real paths, so a symlinked clone does
    not read as a runner that scanned another tree, while a genuinely different tree still
    does.
    """
    normalised_left = paths.normalise_reported_path(left).rstrip("/")
    normalised_right = paths.normalise_reported_path(right).rstrip("/")
    if normalised_left == normalised_right:
        return True
    try:
        return os.path.realpath(left) == os.path.realpath(right)
    except OSError:  # pragma: no cover -- realpath does not raise for plain strings
        return False


# --------------------------------------------------------------------------- #
# The command-line interface                                                  #
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class Inputs:
    """Every path this run reads or writes, resolved to an absolute location.

    Frozen, and built once by :func:`resolve_inputs` from the parsed arguments and the
    environment.  Nothing here is read at import time, and no value is a repository-relative
    default resolved against whatever directory the caller happened to be in.

    ``output_guards`` is not a path: it is the record of the containment, distinctness and
    symlink checks these three write targets passed, carried here because
    :func:`resolve_inputs` is where they are established and the run record must publish
    that one measurement rather than take a second one later.
    """

    raw_dir: Path
    runner_metadata: Path
    allowlist: Path
    log_dir: Path
    spark_src: str
    findings_json: Path
    findings_csv: Path
    run_record: Path
    output_guards: Mapping[str, Any] = MappingProxyType({})

    def as_dict(self) -> dict[str, Any]:
        """Return the resolved inputs as a JSON-serialisable mapping."""
        return {
            "raw_dir": str(self.raw_dir),
            "runner_metadata": str(self.runner_metadata),
            "allowlist": str(self.allowlist),
            "log_dir": str(self.log_dir),
            "spark_src": self.spark_src,
            "findings_json": str(self.findings_json),
            "findings_csv": str(self.findings_csv),
            "run_record": str(self.run_record),
            "output_guards": dict(self.output_guards),
        }


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser.

    Every input is an explicit option.  None carries a default here: defaults are derived
    from the environment inside :func:`resolve_inputs`, at call time, so importing this
    module reads nothing and a test can supply an environment of its own.
    """
    parser = argparse.ArgumentParser(
        prog="normalize.cli",
        description=(
            "Normalize the nine raw scanner artifacts in harness/artifacts/raw/ into the "
            "twelve-field dataset (oss-scan-results/findings.json and findings.csv), "
            "reconcile the counts, and write harness/artifacts/logs/normalize-run.json."
        ),
        epilog=(
            "Reads only harness/artifacts/raw/. Writes no Markdown and never reads "
            "oss-scan-results/tool-status.md, which is rendered from this run's outputs "
            "afterwards. Exit codes: 0 success, 1 halting condition in the data, "
            "2 usage error, 78 configuration fault."
        ),
    )
    parser.add_argument(
        "--raw-dir",
        metavar="DIR",
        default=None,
        help=(
            "the runner-only raw artifact tree holding the nine fixed artifact "
            "filenames; defaults to $HARNESS_RAW_DIR"
        ),
    )
    parser.add_argument(
        "--runner-metadata",
        metavar="FILE",
        default=None,
        help=(
            "runner-metadata.json, the normalizer's input for every tool's scan root and "
            f"path base; defaults to $HARNESS_LOG_DIR/{RUNNER_METADATA_FILENAME}"
        ),
    )
    parser.add_argument(
        "--allowlist",
        metavar="FILE",
        default=None,
        help=(
            "harness/scope/allowlist.txt, the twelve authoritative globs that decide "
            "in_scope; defaults to $HARNESS_SCOPE_FILE"
        ),
    )
    parser.add_argument(
        "--log-dir",
        metavar="DIR",
        default=None,
        help=(
            "the per-tool stream and status tree written by the runners; defaults to "
            "$HARNESS_LOG_DIR, else the directory holding --runner-metadata"
        ),
    )
    parser.add_argument(
        "--spark-src",
        metavar="DIR",
        default=None,
        help=(
            "the pinned Spark clone every emitted path is expressed relative to; "
            "defaults to $SPARK_SRC and is checked against the root the runner metadata "
            "records"
        ),
    )
    parser.add_argument(
        "--findings-json",
        metavar="FILE",
        default=None,
        help=(
            "where findings.json is written; defaults to "
            f"$HARNESS_REPO_ROOT/{FINDINGS_JSON_RELATIVE}"
        ),
    )
    parser.add_argument(
        "--findings-csv",
        metavar="FILE",
        default=None,
        help=(
            "where findings.csv is written; defaults to "
            f"$HARNESS_REPO_ROOT/{FINDINGS_CSV_RELATIVE}"
        ),
    )
    parser.add_argument(
        "--repo-root",
        metavar="DIR",
        default=None,
        help=(
            "the repository root that owns findings.json and findings.csv -- both are "
            "defaulted under it and both are refused outside it; defaults to "
            "$HARNESS_REPO_ROOT, else the repository containing "
            "harness/lib/normalize/cli.py"
        ),
    )
    parser.add_argument(
        "--run-record",
        metavar="FILE",
        default=None,
        help=(
            "where this run's structured record is written -- always, including on a "
            f"halt; defaults to $HARNESS_LOG_DIR/{RUN_RECORD_FILENAME}"
        ),
    )
    return parser


def _environment_value(environ: Mapping[str, str], name: str) -> str | None:
    """Return ``environ[name]`` where it carries content, else ``None``.

    An exported-but-empty variable is treated as unset: ``harness/env.sh`` writes every
    value through ``${VAR:-default}``, so an empty string is an override nobody intended
    rather than a location.
    """
    value = environ.get(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _absolute(value: str) -> Path:
    """Return ``value`` as an absolute path, expanding ``~`` and never resolving symlinks.

    Symlinks are deliberately preserved: the recorded path is the one the caller named, and
    the containment check that needs a real path computes it where it needs it --
    :func:`_path_identity` for aliasing, :func:`_within` for containment, and
    ``emit.assert_safe_output_path`` for the per-component ``lstat`` walk.  Resolving here
    instead would make the record name a path the caller never wrote and would silently
    accept a symlinked target as its own destination (CWE-59).
    """
    return Path(os.path.abspath(os.path.expanduser(value)))


# --------------------------------------------------------------------------- #
# Where an output may be written -- containment, distinctness, no symlink      #
#                                                                             #
# The three configured outputs are the only files this module writes:          #
# findings.json, findings.csv and normalize-run.json.  Each has exactly one     #
# owner root, is refused anywhere else, may not be a second name for any other  #
# of the three, and may not name an INPUT -- the raw artifacts, the allowlist   #
# or runner-metadata.json.  Every check below is a fault to correct in the      #
# invocation rather than an outcome to classify, so all of them are             #
# ConfigurationFault (exit 78) and all of them happen before anything is read   #
# or written.                                                                  #
# --------------------------------------------------------------------------- #

#: Where this module's own repository root is, relative to this file:
#: ``harness/lib/normalize/cli.py`` -> ``normalize`` -> ``lib`` -> ``harness`` -> the root.
_REPO_ROOT_DEPTH: int = 3


def _path_identity(path: Path) -> str:
    """Return the identity two configured paths are compared on for aliasing.

    The real path of the *parent* joined with the name.  Two properties this needs and a
    plain string comparison does not have: it answers for a file that does not exist yet,
    which every output does before it is written; and it sees through a symlinked or
    otherwise differently-spelled directory, so ``$HARNESS_REPO_ROOT/oss-scan-results`` and
    a second route to the same directory compare equal.  The file's own name is never
    resolved, because resolving it is exactly what the symlink refusal exists to prevent.
    """
    try:
        parent = os.path.realpath(path.parent)
    except OSError:  # pragma: no cover -- realpath does not raise for a plain string
        parent = str(path.parent)
    return str(Path(parent) / path.name)


def _within(path: Path, root: Path) -> bool:
    """Return whether ``path`` would be written inside ``root``.

    Compared on real paths of the two *directories*, so a differently-spelled route to one
    directory is containment and a genuinely different tree is not.
    """
    try:
        real_root = Path(os.path.realpath(root))
        real_parent = Path(os.path.realpath(path.parent))
    except OSError:  # pragma: no cover -- realpath does not raise for a plain string
        return False
    return real_parent == real_root or real_root in real_parent.parents


def _declared_repository_root(
    environ: Mapping[str, str],
    argument: str | None = None,
) -> tuple[Path, str]:
    """Return the repository root that owns the dataset files, and where it came from.

    Three candidates, in one precedence, and the label of whichever supplied the answer is
    returned with it -- ``output_guards["repository_root_source"]`` publishes that label in
    the run record, so a reader can see *which* declaration the containment check was
    answered against rather than only what it decided:

    1.  ``--repo-root``, the explicit declaration.  It exists because the owner root was
        the one input with no flag of its own, which contradicted this module's own CLI
        contract (*"Every input is an explicit argument … and an explicit argument always
        wins"*) and left an isolated run unable to declare its owner root *before* sourcing
        ``harness/env.sh`` -- that file assigns ``HARNESS_REPO_ROOT`` unconditionally, so
        a caller's pre-set value does not survive it and the containment check below then
        correctly refused the isolated output the caller had asked for.  The flag closes
        that gap on this side, without editing the environment file (AAP 0.3.1 reads it and
        never writes it; AAP 0.6.3 permits changing exactly two provisioned paths under
        ``harness/`` and neither is that one).
    2.  ``$HARNESS_REPO_ROOT`` where ``harness/env.sh`` exported it -- the run's own
        declaration, and the same variable the ``--findings-json`` default derives from.
    3.  Failing both, *this module's own repository*, three levels above this file, which
        is a fact about the installed harness rather than about whatever directory the
        caller happened to be in.

    An explicit value that is empty or whitespace is treated as unset, exactly as
    :func:`_environment_value` treats an exported-but-empty variable: it is an override
    nobody intended rather than a location, and reading it as one would resolve every
    output against ``/`` -- a root that contains every path, which is the containment check
    passing while checking nothing.

    The working directory is deliberately not a candidate at any level: an output root
    taken from the cwd is an output that lands wherever the run was started (CWE-73).
    """
    if argument is not None:
        explicit = argument.strip()
        if explicit:
            return _absolute(explicit), "--repo-root"
    declared = _environment_value(environ, "HARNESS_REPO_ROOT")
    if declared is not None:
        return _absolute(declared), "$HARNESS_REPO_ROOT"
    return (
        Path(__file__).resolve().parents[_REPO_ROOT_DEPTH],
        "the repository containing harness/lib/normalize/cli.py",
    )


def _require_within(
    path: Path,
    *,
    name: str,
    owner: Path,
    owner_name: str,
) -> None:
    """Refuse ``path`` where it would be written outside the root that owns it."""
    if not _within(path, owner):
        raise _fault(
            HALT_OUTPUT_OUTSIDE_OWNER,
            f"{name} would be written to {path}, which is outside {owner_name} "
            f"({owner}). Every output this module writes belongs to exactly one root: "
            "the dataset files to the repository root the run declares and the run "
            "record to the log tree. A path outside its owner is a fault in the "
            "invocation, not a location.",
            output=name,
            path=str(path),
            owner_root=str(owner),
            owner_root_source=owner_name,
        )


def _require_safe_components(path: Path, *, name: str, owner: Path) -> dict[str, Any]:
    """Refuse ``path`` where the target or a component at or below ``owner`` is a link."""
    try:
        return emit.assert_safe_output_path(path, boundary=owner)
    except emit.UnsafeOutputPath as error:
        raise _fault(
            HALT_OUTPUT_SYMLINKED,
            f"{name} cannot be written: {error}",
            output=name,
            path=str(path),
            owner_root=str(owner),
            error=str(error),
        ) from error


def _validate_output_targets(
    inputs: Inputs,
    environ: Mapping[str, str],
    repo_root_argument: str | None = None,
) -> dict[str, Any]:
    """Establish that the three outputs are contained, distinct and unaliased.

    ``repo_root_argument`` is ``--repo-root`` as the caller passed it, or ``None``: the
    dataset's owner root is adjudicated against the same three-level declaration
    :func:`resolve_inputs` defaulted the two dataset paths from, so a run cannot be
    defaulted under one root and then contained against another.

    Three questions, each asked of every output, and each a halt rather than a warning:

    1.  **Containment.**  ``findings.json`` and ``findings.csv`` must resolve inside the
        repository root the run declares; ``normalize-run.json`` must resolve inside the
        log tree.  A configured path is otherwise free to land anywhere the process can
        write, which turns a mis-set variable into a write outside the deliverable trees
        (CWE-73).
    2.  **Distinctness.**  The three must be three different files, and none of them may be
        an input: not the raw directory or any artifact in it, not the allowlist, not
        ``runner-metadata.json``.  Writing an output over an input destroys the evidence
        the dataset was derived from, and two outputs at one path make the second write
        destroy the first while every count still reconciles.
    3.  **No symlinked component.**  The target and every component at or below its owner
        root are checked with ``lstat``; the first link is named.  ``emit.py`` owns that
        walk and the exclusive no-follow write that follows it, so there is one
        implementation of the rule rather than one per caller.

    The order of 1 and 3 decides which message a reader gets, and it is deliberate.
    Containment is answered on *real* paths, so a symlinked component that points **out**
    of the owner root is reported as the escape it is, with the declared root named --
    which is the fact that matters, whether the escape was spelled with a link or with
    ``..``.  A symlinked component pointing **inside** the root passes containment and is
    then refused by the component walk, which names the link itself.  Every path that
    reaches the write is therefore both contained on real paths and free of links below
    its root.

    Returns:
        The record of what was checked, carried in :attr:`Inputs.output_guards` and
        published in ``normalize-run.json``.

    Raises:
        ConfigurationFault: On the first condition that fails, naming the path, the owner
            root and -- for an alias -- the other path it collides with.
    """
    repo_root, repo_root_source = _declared_repository_root(
        environ, repo_root_argument
    )
    owners: tuple[tuple[str, Path, Path, str], ...] = (
        ("--findings-json", inputs.findings_json, repo_root, f"the repository root ({repo_root_source})"),
        ("--findings-csv", inputs.findings_csv, repo_root, f"the repository root ({repo_root_source})"),
        ("--run-record", inputs.run_record, inputs.log_dir, "the log tree (--log-dir / $HARNESS_LOG_DIR)"),
    )

    component_checks: dict[str, Any] = {}
    for name, path, owner, owner_name in owners:
        _require_within(path, name=name, owner=owner, owner_name=owner_name)
        component_checks[name] = _require_safe_components(path, name=name, owner=owner)

    # Distinctness among the outputs themselves.
    seen: dict[str, str] = {}
    for name, path, _owner, _owner_name in owners:
        identity = _path_identity(path)
        if identity in seen:
            raise _fault(
                HALT_OUTPUT_ALIASED,
                f"{name} and {seen[identity]} both name {identity}. The three outputs are "
                "three different files; two names for one file would make the second "
                "write destroy the first while every count still reconciled.",
                output=name,
                collides_with=seen[identity],
                identity=identity,
            )
        seen[identity] = name

    # Distinctness against every input. The raw tree is listed as the directory, the nine
    # fixed artifact filenames inside it and whatever else is actually there, because an
    # output aimed at a raw artifact would destroy a runner's own output -- the one thing
    # in this pipeline nothing can reproduce.
    input_identities: dict[str, str] = {
        _path_identity(inputs.raw_dir): "--raw-dir",
        _path_identity(inputs.runner_metadata): "--runner-metadata",
        _path_identity(inputs.allowlist): "--allowlist",
    }
    for tool in ARTIFACT_ORDER:
        artifact = inputs.raw_dir / shape.artifact_filename_for(tool)
        input_identities.setdefault(
            _path_identity(artifact), f"the {tool} raw artifact"
        )
    try:
        present_entries = sorted(entry.name for entry in inputs.raw_dir.iterdir())
    except OSError:
        # A missing or unreadable raw tree is _enumerate_raw_directory's halt to raise,
        # with its own message; nothing here pre-empts it.
        present_entries = []
    for entry_name in present_entries:
        input_identities.setdefault(
            _path_identity(inputs.raw_dir / entry_name),
            f"the raw-tree entry {entry_name}",
        )

    for name, path, _owner, _owner_name in owners:
        identity = _path_identity(path)
        if identity in input_identities:
            raise _fault(
                HALT_OUTPUT_ALIASED,
                f"{name} would be written to {identity}, which is {input_identities[identity]} "
                "-- an input to this run. Writing an output over an input destroys the "
                "evidence the dataset was derived from.",
                output=name,
                path=str(path),
                collides_with=input_identities[identity],
                identity=identity,
            )

    return {
        "repository_root": str(repo_root),
        "repository_root_source": repo_root_source,
        "log_tree": str(inputs.log_dir),
        "owners": {
            "--findings-json": str(repo_root),
            "--findings-csv": str(repo_root),
            "--run-record": str(inputs.log_dir),
        },
        "identities": {
            name: _path_identity(path) for name, path, _owner, _owner_name in owners
        },
        "input_identities_compared_against": len(input_identities),
        "component_checks": component_checks,
        "checks_passed": [
            "each output resolves inside the root that owns it",
            "the three outputs are three distinct files",
            "no output names the raw directory, a raw artifact, the allowlist or the "
            "runner metadata",
            "no output target or component at or below its owner root is a symbolic link",
        ],
        "note": (
            "Every check is a ConfigurationFault (exit 78) rather than a warning, and all "
            "of them run before any artifact is read. emit.py owns the per-component lstat "
            "walk and the exclusive no-follow staged write that follows it, so the rule has "
            "one implementation for the dataset files and the run record alike."
        ),
    }


def _log_tree_values(
    namespace: argparse.Namespace,
    environ: Mapping[str, str],
) -> tuple[str | None, str | None]:
    """Return the log tree and the runner-metadata path, by one precedence.

    ``--log-dir``, else ``$HARNESS_LOG_DIR``; the metadata defaults to
    ``<log tree>/runner-metadata.json``, and where the caller named the metadata file
    explicitly its directory *is* the log tree, since that is where the runners write it.

    Factored out because two callers need the same answer -- :func:`resolve_inputs` for the
    inputs and :func:`_run_record_target` for the record's location before those inputs
    exist -- and two copies of a precedence are two precedences the first time either is
    edited.
    """
    log_dir_value = namespace.log_dir or _environment_value(environ, "HARNESS_LOG_DIR")
    metadata_value = namespace.runner_metadata
    if metadata_value is None and log_dir_value is not None:
        metadata_value = os.path.join(log_dir_value, RUNNER_METADATA_FILENAME)
    if log_dir_value is None and metadata_value is not None:
        log_dir_value = os.path.dirname(os.path.abspath(metadata_value)) or None
    return log_dir_value, metadata_value


def _run_record_target(
    namespace: argparse.Namespace,
    environ: Mapping[str, str],
) -> tuple[Path, Path]:
    """Return where ``normalize-run.json`` is written, before the inputs are resolved.

    ``main`` needs this path before :func:`resolve_inputs` runs, because the record is
    written on **every** path out of this module including a fault inside that call.  Two
    sources supply it and no third: ``--run-record``, or ``$HARNESS_LOG_DIR`` (equivalently
    ``--log-dir``, or the directory of an explicitly named ``--runner-metadata``) joined
    with the fixed filename.

    There is deliberately **no working-directory fallback**.  A record written to
    ``os.getcwd()`` lands wherever the run happened to be started, which is not the log
    tree, is not published by the manifest, and is not where any reader looks -- so the
    required audit evidence would be silently written somewhere nobody sees it (CWE-73).
    Its absence is a configuration fault naming both sources instead.

    The path is checked for containment in the log tree and for a symlinked component here
    as well as in :func:`_validate_output_targets`, because this is the one output whose
    location is needed before the full validation can run.

    Returns:
        The record's absolute path and the root that owns it -- the log tree, or the
        record's own directory in the one case where a caller named ``--run-record``
        explicitly and no log tree is known at all.

    Raises:
        ConfigurationFault: Where neither source supplies a location, or the location is
            outside the log tree or has a symlinked component.
    """
    log_dir_value, _metadata_value = _log_tree_values(namespace, environ)
    if namespace.run_record is not None:
        value = namespace.run_record
    elif log_dir_value is not None:
        value = os.path.join(log_dir_value, RUN_RECORD_FILENAME)
    else:
        raise _fault(
            HALT_MISSING_INPUT,
            "the run record's location could not be resolved: pass --run-record, or "
            "source harness/env.sh so $HARNESS_LOG_DIR names the log tree. There is no "
            "working-directory fallback -- a record written relative to whatever "
            f"directory the run started in is not {RUN_RECORD_DOCUMENT} and is not "
            "published by the artifact manifest.",
            missing=[
                {"input": "--run-record", "defaulted_from": "$HARNESS_LOG_DIR/" + RUN_RECORD_FILENAME}
            ],
        )
    target = _absolute(value)
    if log_dir_value is None:
        # Reachable only with an explicit --run-record and no log tree named anywhere;
        # resolve_inputs will fault on the missing --log-dir immediately afterwards, and
        # until it does the record's own directory is the only root there is to declare.
        owner = target.parent
    else:
        owner = _absolute(log_dir_value)
        _require_within(
            target,
            name="--run-record",
            owner=owner,
            owner_name="the log tree (--log-dir / $HARNESS_LOG_DIR)",
        )
    _require_safe_components(target, name="--run-record", owner=owner)
    return target, owner


def resolve_inputs(
    namespace: argparse.Namespace,
    environ: Mapping[str, str] | None = None,
) -> Inputs:
    """Resolve every input from the parsed arguments and the environment.

    An explicit argument always wins.  Where one is absent, the default is derived from the
    environment the provisioned ``harness/env.sh`` exports and from nothing else.  A
    required input that can be neither supplied nor defaulted raises a
    :class:`ConfigurationFault` naming the flag *and* the variable that would have supplied
    it, so the message says what to do rather than only what went wrong.

    Args:
        namespace: The parsed arguments from :func:`build_parser`.
        environ: The environment to read defaults from; ``os.environ`` when omitted.  An
            argument rather than a module-level read, so a test supplies its own.

    Returns:
        The resolved :class:`Inputs`.

    Raises:
        ConfigurationFault: Where a required input is missing.
    """
    env = os.environ if environ is None else environ
    missing: list[dict[str, str]] = []

    def require(
        value: str | None,
        *,
        flag: str,
        source: str,
    ) -> str | None:
        if value is None:
            missing.append({"input": flag, "defaulted_from": source})
        return value

    log_dir_value, metadata_value = _log_tree_values(namespace, env)

    # The dataset's owner root, by the same three-level precedence
    # _declared_repository_root applies -- the explicit flag, then the variable, then this
    # module's own repository. Only the first two can supply a *default* for the two
    # dataset paths: the third is a fallback for adjudicating containment, and defaulting
    # a deliverable into the installed harness's own checkout because neither declaration
    # was made would write the dataset somewhere nobody asked for.
    repo_root_argument = namespace.repo_root.strip() if namespace.repo_root else None
    repo_root_value = repo_root_argument or _environment_value(
        env, "HARNESS_REPO_ROOT"
    )
    findings_json_value = namespace.findings_json
    if findings_json_value is None and repo_root_value is not None:
        findings_json_value = os.path.join(repo_root_value, FINDINGS_JSON_RELATIVE)
    findings_csv_value = namespace.findings_csv
    if findings_csv_value is None and repo_root_value is not None:
        findings_csv_value = os.path.join(repo_root_value, FINDINGS_CSV_RELATIVE)
    run_record_value = namespace.run_record
    if run_record_value is None and log_dir_value is not None:
        run_record_value = os.path.join(log_dir_value, RUN_RECORD_FILENAME)

    raw_dir_value = require(
        namespace.raw_dir or _environment_value(env, "HARNESS_RAW_DIR"),
        flag="--raw-dir",
        source="$HARNESS_RAW_DIR",
    )
    metadata_value = require(
        metadata_value,
        flag="--runner-metadata",
        source=f"$HARNESS_LOG_DIR/{RUNNER_METADATA_FILENAME}",
    )
    allowlist_value = require(
        namespace.allowlist or _environment_value(env, "HARNESS_SCOPE_FILE"),
        flag="--allowlist",
        source="$HARNESS_SCOPE_FILE",
    )
    log_dir_value = require(
        log_dir_value,
        flag="--log-dir",
        source="$HARNESS_LOG_DIR",
    )
    spark_src_value = require(
        namespace.spark_src or _environment_value(env, "SPARK_SRC"),
        flag="--spark-src",
        source="$SPARK_SRC",
    )
    findings_json_value = require(
        findings_json_value,
        flag="--findings-json",
        source=f"$HARNESS_REPO_ROOT/{FINDINGS_JSON_RELATIVE}, or --repo-root",
    )
    findings_csv_value = require(
        findings_csv_value,
        flag="--findings-csv",
        source=f"$HARNESS_REPO_ROOT/{FINDINGS_CSV_RELATIVE}, or --repo-root",
    )
    run_record_value = require(
        run_record_value,
        flag="--run-record",
        source=f"$HARNESS_LOG_DIR/{RUN_RECORD_FILENAME}",
    )

    if missing:
        listed = ", ".join(
            f"{entry['input']} (no default: {entry['defaulted_from']} is not set)"
            for entry in missing
        )
        raise _fault(
            HALT_MISSING_INPUT,
            f"missing required input(s): {listed}. Source harness/env.sh, or pass the "
            "flag explicitly; nothing is read at import time and no repository-relative "
            "path is assumed.",
            missing=missing,
        )

    # Every value is present past this point; the assertions keep the type checker and a
    # future editor honest about that.
    assert raw_dir_value is not None
    assert metadata_value is not None
    assert allowlist_value is not None
    assert log_dir_value is not None
    assert spark_src_value is not None
    assert findings_json_value is not None
    assert findings_csv_value is not None
    assert run_record_value is not None

    inputs = Inputs(
        raw_dir=_absolute(raw_dir_value),
        runner_metadata=_absolute(metadata_value),
        allowlist=_absolute(allowlist_value),
        log_dir=_absolute(log_dir_value),
        spark_src=str(_absolute(spark_src_value)),
        findings_json=_absolute(findings_json_value),
        findings_csv=_absolute(findings_csv_value),
        run_record=_absolute(run_record_value),
    )
    # Every write target is adjudicated here, once, before anything is read or written:
    # a fault in where an output would land is a fault in the invocation, and finding it
    # after the artifacts have been parsed would only mean finding it later.
    return dataclasses.replace(
        inputs,
        output_guards=MappingProxyType(
            _validate_output_targets(inputs, env, repo_root_argument)
        ),
    )


def interpreter_record() -> dict[str, Any]:
    """Describe the interpreter running this module, and compare it with the expectation.

    AAP 0.6.1 requires ``normalize-run.json`` to carry *"its exact command, interpreter path
    and version"*, and AAP 0.4.1 requires the version to be compared against the expected
    ``3.13.7``: *"a major or minor difference is recorded with both values and the run
    continues; a patch difference likewise."*  So this returns a comparison, never a
    verdict that stops anything.
    """
    observed = platform.python_version()
    expected = EXPECTED_INTERPRETER_VERSION
    observed_parts = observed.split(".")
    expected_parts = expected.split(".")
    if observed == expected:
        comparison = "matches"
    elif observed_parts[:1] != expected_parts[:1]:
        comparison = "major_difference"
    elif observed_parts[:2] != expected_parts[:2]:
        comparison = "minor_difference"
    else:
        comparison = "patch_difference"
    return {
        "executable": sys.executable,
        "observed_version": observed,
        "expected_version": expected,
        "comparison": comparison,
        "version_matches_expected": observed == expected,
        "halts_on_difference": False,
        "implementation": platform.python_implementation(),
        "version_string": sys.version.replace("\n", " "),
        "note": (
            "A difference of any kind is recorded with both values and the run continues "
            "(AAP 0.4.1). The normalizer uses the standard library only, so it runs on "
            "this base interpreter independently of any scanner's virtualenv."
        ),
    }


def resolve_adapter(decision: shape.RoutingDecision) -> ModuleType:
    """Return the adapter module ``decision`` names, importing a conditional one on demand.

    ``shape.py`` names an adapter by string key and imports none; this module holds the
    registry that inverts that direction.  The six adapters that always exist are imported
    at module import time.  ``osv_scanner`` is imported here, guarded, because AAP 0.6.1
    creates it *"if and only if OSV-Scanner writes an artifact"* -- so its absence in the
    expected case is not an error at all, and its absence *with* an artifact present is a
    :class:`MissingAdapterModule` that names the module rather than a generic halt.

    Raises:
        MissingAdapterModule: Where the conditional adapter for a present artifact does not
            exist.
        ConfigurationFault: Where the registry has no entry for the key at all, or the
            module exposes no callable ``adapt`` -- both programming faults rather than
            artifact conditions.
    """
    key = decision.adapter
    module = ADAPTER_REGISTRY.get(key)
    if module is None and key in CONDITIONAL_ADAPTER_MODULES:
        # One key, one guarded import: a dynamic import by name would hide from a reader
        # (and from a grep) exactly which module is conditional and why.
        if key == "osv_scanner":
            try:
                from normalize.adapters import osv_scanner  # noqa: PLC0415
            except ImportError as error:
                raise MissingAdapterModule(
                    HALT_MISSING_ADAPTER_MODULE,
                    f"{decision.tool} wrote {decision.artifact_path!r}, but its adapter "
                    f"module {decision.adapter_module_name} "
                    "(harness/lib/normalize/adapters/osv_scanner.py) does not exist. "
                    "AAP 0.6.1 creates that adapter if and only if OSV-Scanner writes an "
                    "artifact; an artifact is present, so the adapter is required. This is "
                    "a missing module, not an unknown artifact shape.",
                    details={
                        "tool": decision.tool,
                        "adapter_module": decision.adapter_module_name,
                        "expected_file": (
                            "harness/lib/normalize/adapters/osv_scanner.py"
                        ),
                        "artifact_path": decision.artifact_path,
                        "import_error": f"{type(error).__name__}: {error}",
                    },
                ) from error
            module = osv_scanner
    if module is None:
        raise _fault(
            HALT_MISSING_ADAPTER_MODULE,
            f"no adapter is registered for key {key!r} (tool {decision.tool!r}); the "
            f"registry holds {', '.join(sorted(ADAPTER_REGISTRY))} and the conditional "
            f"modules are {', '.join(CONDITIONAL_ADAPTER_MODULES)}",
            tool=decision.tool,
            adapter=key,
            registered=sorted(ADAPTER_REGISTRY),
            conditional=list(CONDITIONAL_ADAPTER_MODULES),
        )
    entry_point = getattr(module, "adapt", None)
    if not callable(entry_point):
        raise _fault(
            HALT_MISSING_ADAPTER_MODULE,
            f"adapter module {decision.adapter_module_name} exposes no callable 'adapt'; "
            "every adapter in this package presents the same entry point",
            tool=decision.tool,
            adapter=key,
            adapter_module=decision.adapter_module_name,
        )
    return module


# --------------------------------------------------------------------------- #
# Per-artifact outcomes                                                       #
# --------------------------------------------------------------------------- #


@dataclass
class ArtifactOutcome:
    """One tool's outcome: what was found, what it produced, and what it stated.

    One of these exists for every one of the nine canonical identifiers, present or absent,
    because ``findings.json`` and ``findings.csv`` are row-only and a tool that produced no
    row is invisible in them by construction (AAP 0.5.4).  ``tool-status.md`` and
    ``severity-map.md`` are the authoritative inventory of all nine, and they are rendered
    from these outcomes joined with ``runner-metadata.json`` afterwards -- never the other
    way round.
    """

    tool: str
    scanner_class: str
    artifact_filename: str
    present: bool
    parse_status: str
    artifact: dict[str, Any]
    artifact_expected: bool | None = None
    routing: dict[str, Any] | None = None
    raw_records: int | None = None
    emitted_rows: int = 0
    rejected_records: int = 0
    rejections_by_class: dict[str, int] = field(default_factory=dict)
    rejections: list[dict[str, Any]] = field(default_factory=list)
    counters: dict[str, int] = field(default_factory=dict)
    counter_summary: dict[str, Any] = field(default_factory=dict)
    path_kinds: dict[str, Any] = field(default_factory=dict)
    runner_status: dict[str, Any] = field(default_factory=dict)
    network_fetch: dict[str, Any] = field(default_factory=dict)
    tool_words: dict[str, Any] = field(default_factory=dict)
    extras: dict[str, Any] = field(default_factory=dict)
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        """Return the outcome as a JSON-serialisable mapping, in a fixed key order."""
        return {
            "tool": self.tool,
            "scanner_class": self.scanner_class,
            "artifact_filename": self.artifact_filename,
            "artifact_expected": self.artifact_expected,
            "present": self.present,
            "parse_status": self.parse_status,
            "artifact": self.artifact,
            "routing": self.routing,
            "raw_records": self.raw_records,
            "emitted_rows": self.emitted_rows,
            "rejected_records": self.rejected_records,
            "rejections_by_class": dict(self.rejections_by_class),
            "rejections": list(self.rejections),
            "counters": dict(self.counters),
            "counter_summary": self.counter_summary,
            "path_kinds": self.path_kinds,
            "runner_status": self.runner_status,
            "network_fetch": self.network_fetch,
            "tool_words": self.tool_words,
            "extras": self.extras,
            "notes": list(self.notes),
        }


def _scanner_class_label(tool: str) -> str:
    """Return a tool's ``scanner_class`` as a label the record can carry.

    Trivy's class is decided per record from the section a record was read from, so
    ``shape`` hands out a sentinel that deliberately refuses to be stringified.  It is
    rendered here as ``shape.PER_RECORD_LABEL`` for the record only; no dataset row ever
    carries that label.
    """
    resolved = shape.scanner_class_for(tool)
    if shape.is_per_record(resolved):
        return shape.PER_RECORD_LABEL
    return str(resolved)


def _counter_summary(counters: Mapping[str, int]) -> dict[str, Any]:
    """Summarise the three counts AAP 0.5.4 requires reported, plus the scope split.

    Records ``null`` rather than ``0`` for a counter the adapter does not author: Gitleaks
    and Joern emit no ``cwe``/``cve`` at all and so define no multi-identifier counter, and
    "this adapter never counts that" is a different statement from "it counted none".
    """
    summary: dict[str, Any] = {}
    not_defined: list[str] = []
    for key in _SUMMARY_COUNTERS:
        if key in counters:
            summary[key] = counters[key]
        else:
            summary[key] = None
            not_defined.append(key)
    summary["counters_not_defined_by_adapter"] = not_defined
    return summary


def _path_kind_counts(counters: Mapping[str, int]) -> Iterable[tuple[str, int]]:
    """Yield ``(kind, count)`` for every path-kind counter in ``counters``.

    One place selects the counters and strips the prefix, so the per-artifact tally and
    the dataset tally cannot come to disagree about which keys are path kinds.  The kind
    is not validated here: :meth:`paths.PathKindTally.add_many` owns that check against
    the closed set, and duplicating it would let the two copies drift.
    """
    prefix_length = len(_PATH_KIND_COUNTER_PREFIX)
    for key, count in counters.items():
        if key.startswith(_PATH_KIND_COUNTER_PREFIX):
            yield key[prefix_length:], count


def _path_kind_tally(counters: Mapping[str, int]) -> paths.PathKindTally:
    """Build a :class:`normalize.paths.PathKindTally` from an adapter's counters.

    Counted through the discriminator rather than beside it:
    :meth:`paths.PathKindTally.add_many` validates every kind against the closed set, so
    this tally cannot drift from ``paths.NON_FILESYSTEM_PATH_KINDS`` the way a private
    counter could.

    The counters are already aggregated -- an adapter reports ``path_kind_tree_file: 1322``,
    not 1,322 observations -- so the count is added in one step.  Replaying it as ``count``
    separate ``add`` calls re-enumerated every resolution in the dataset to recompute a sum
    that was already known, and did it twice: once here and once in
    :func:`_merge_path_kinds`.  ``add_many`` also refuses a negative count rather than
    clamping it to zero, which the replay silently did: a negative counter is an adapter
    fault, and clamping it made the reported proportion wrong with nothing recording why.
    """
    tally = paths.PathKindTally()
    for kind, count in _path_kind_counts(counters):
        tally.add_many(kind, count)
    return tally


def _merge_path_kinds(
    total: paths.PathKindTally,
    counters: Mapping[str, int],
) -> None:
    """Fold one artifact's path kinds into the dataset-level tally, in one step per kind.

    The dataset tally is the sum of the per-artifact ones, so it is folded from the same
    aggregated counters through the same validated bulk operation -- never by replaying
    each artifact's resolutions a second time.
    """
    for kind, count in _path_kind_counts(counters):
        total.add_many(kind, count)


def _check_runner_root_evidence(
    tool: str,
    status: Mapping[str, Any],
    root: str,
) -> None:
    """Halt where a runner's own status record shows it scanned something else.

    The ``<tool>.status`` file is the runner's own account of the root it resolved, written
    by ``scope_finish``.  Two conditions in it stop the run rather than being normalized
    past, both from AAP 0.9.2: a runner that *"resolved a tree other than SPARK_SRC"*, and
    a runner that ran under the smoke override, which redirects every runner at one small
    directory and *"is never a fallback"* for a real scan.  Both are established from the
    runner's record, never by inspecting individual rows -- an individual finding whose
    coordinate falls outside the tree is a legitimate coordinate and is kept.
    """
    # Both fields below come out of a file this module did not write, so both reach the
    # record through paths.safe_diagnostic rather than through {value!r}: bounded, with the
    # digest of the whole value, control characters escaped and URI userinfo redacted. A
    # status record is provisioning-supplied evidence, and a halt quoting it is persisted.
    source = status.get("scan_root_source")
    if isinstance(source, str) and "HARNESS_SMOKE_TARGET" in source:
        rendered_source = paths.safe_diagnostic(
            source, context=f"{tool}.status scan_root_source"
        )
        raise _halt(
            HALT_SMOKE_OVERRIDE_EVIDENCE,
            f"{tool}: its status record names {rendered_source} as the source of the scan "
            "root, so this artifact came from the setup-time smoke override rather than "
            "the pinned tree. The override exists for setup-time verification only and is "
            "never a fallback for a real scan.",
            tool=tool,
            scan_root_source=rendered_source.as_dict(),
            recorded_scan_root=status.get("scan_root"),
            status_path=status.get("path"),
        )
    recorded = status.get("scan_root")
    if isinstance(recorded, str) and recorded and not _same_root(recorded, root):
        rendered_recorded = paths.safe_diagnostic(
            recorded, context=f"{tool}.status scan_root"
        )
        raise _halt(
            HALT_WRONG_SCAN_ROOT_EVIDENCE,
            f"{tool}: its status record says it resolved {rendered_recorded}, which is not "
            f"the root this dataset is expressed against ({root!r}). Every finding it "
            "produced would be about another tree, so this is a targeting fault rather "
            "than a coordinate to keep.",
            tool=tool,
            recorded_scan_root=rendered_recorded.as_dict(),
            expected_scan_root=root,
            status_path=status.get("path"),
        )


def _adapter_extras(
    tool: str,
    document: Any,
    source_index: paths.SourceIndex | None,
) -> dict[str, Any]:
    """Collect the per-tool evidence ``tool-status.md`` needs beyond rows and counts.

    Checkov's ``parsing_errors`` are status evidence rather than findings, and its per-report
    ``check_type``/``summary`` pairs say which frameworks reported at all; ``checkov.py``
    names this module as the carrier of both.  The Joern entry records the shape of the
    source index its class-to-source resolution was made against, so a rejection count can
    be read against the index that produced it.
    """
    if tool == "checkov":
        return {
            "parsing_errors": [dict(entry) for entry in checkov.collect_parsing_errors(document)],
            "report_summaries": [dict(entry) for entry in checkov.report_summaries(document)],
            "never_emitted_sections": list(checkov.NEVER_EMITTED_RESULT_SECTIONS),
        }
    if tool == "joern" and source_index is not None:
        return {"source_index": source_index.statistics()}
    if tool == "trivy":
        return {
            "supported_sections": dict(trivy.SUPPORTED_SECTIONS),
            "unsupported_finding_sections_validated_empty": list(
                trivy.UNSUPPORTED_FINDING_SECTIONS
            ),
        }
    return {}


# --------------------------------------------------------------------------- #
# Bounded artifact ingestion (CWE-400, CWE-674, CWE-770)                      #
# --------------------------------------------------------------------------- #


#: Which of a refusal's halt details are also copied into the artifact's own ``ingestion``
#: entry.  Measurements only: a byte count, a cap, a stage name.  The prose, the tool
#: identifier and the underlying error text stay in the halt, which is the one place they
#: are rendered through the persistence boundary.
_INGESTION_RECORD_KEYS: frozenset[str] = frozenset(
    {
        "observed_bytes",
        "bytes_read",
        "measured_bytes_before_read",
        "byte_cap",
        "byte_offset",
        "bound",
        "bound_cap",
        "bound_observed",
        "depth_cap",
        "node_cap",
        "recursion_limit",
        "stage",
    }
)


#: One ingestion bound was crossed, naming which, by how much.  THE SAME CLASS the shared
#: lexical gate in ``paths`` raises, bound to this module's own historical name rather than
#: redefined here: two classes for one condition would mean the artifact route catching one
#: and the metadata route the other, which is how a bound crossing escapes as an unnamed
#: error.  Raised by both walks that enforce the structural caps --
#: :func:`_validate_document_bounds` over a document's text, before it is parsed, and
#: :func:`_measure_document` over the parsed document afterwards.  Neither carries a halt
#: reason: the bound is the measurement, and :data:`_BOUND_HALT_REASONS` maps it to the
#: name from this module's closed vocabulary, because that vocabulary is this module's and
#: a measurement module has no business composing a halt.
_IngestionBoundExceeded = paths.DocumentBoundExceeded

#: The artifact-side halt name for each bound the gate can raise.  Total against
#: ``paths.DOCUMENT_BOUNDS`` -- :func:`_verify_vocabularies` asserts that, so a bound added
#: to the gate cannot reach this module with no halt name and be recorded under the unnamed
#: ``unexpected-error`` reason.  Two bounds share one name deliberately: a member name and a
#: value are both strings, and the vocabulary has always carried one reason for both.
_BOUND_HALT_REASONS: Mapping[str, str] = MappingProxyType(
    {
        paths.DOCUMENT_BOUND_DEPTH: HALT_ARTIFACT_TOO_DEEP,
        paths.DOCUMENT_BOUND_NODES: HALT_ARTIFACT_TOO_MANY_NODES,
        paths.DOCUMENT_BOUND_STRING: HALT_ARTIFACT_STRING_TOO_LONG,
        paths.DOCUMENT_BOUND_MEMBER_NAME: HALT_ARTIFACT_STRING_TOO_LONG,
        paths.DOCUMENT_BOUND_NUMERIC_DIGITS: HALT_ARTIFACT_LITERAL_UNCONVERTIBLE,
        paths.DOCUMENT_BOUND_LITERAL_TOKEN: HALT_ARTIFACT_LITERAL_UNCONVERTIBLE,
    }
)


def _halt_reason_for(error: paths.DocumentBoundExceeded) -> str:
    """Return the halt reason this module records for the bound ``error`` names.

    The mapping is total against ``paths.DOCUMENT_BOUNDS`` and that totality is asserted in
    :func:`_verify_vocabularies`, so this cannot silently fall back.  An unmapped bound is
    :data:`HALT_UNEXPECTED` rather than a ``KeyError``, because a bound crossing that this
    module failed to name is still a refusal the record has to carry (CWE-703).
    """
    return _BOUND_HALT_REASONS.get(error.bound, HALT_UNEXPECTED)


def _read_bounded_artifact_text(path: Path, limit: int) -> tuple[str, int, bool]:
    """Read at most ``limit`` bytes of an untrusted JSON file and decode them as UTF-8.

    Used for an artifact against :data:`ARTIFACT_BYTE_LIMIT`, and for
    ``runner-metadata.json`` against :data:`RUNNER_METADATA_BYTE_LIMIT`, which is the same
    contract at a different cap: the caller needs the text in order to bound the document's
    structure before anything parses it.

    The bound is on the read because the read is the first allocation: ``read_text()``
    materialises the whole file before anything has a chance to check its size, so a
    check-then-read-whole-file pair bounds nothing that a hostile file has to respect.
    ``limit + 1`` bytes are requested so that "the file is over the cap" is measurable from
    the read itself, which closes the gap between the ``os.stat`` the caller took and this
    read -- a file that grew in between is refused here rather than ingested.

    Decoding is strict: an artifact this module cannot decode is not an artifact it should
    guess at, and ``errors="replace"`` would silently substitute replacement characters
    into rule identifiers, messages and paths that end up in the dataset.

    Returns:
        The decoded text, the number of bytes read, and whether the file exceeded ``limit``.
        On exceeding it the text is empty: nothing is decoded, so the second allocation
        never happens.

    Raises:
        OSError: the file could not be opened or read.
        UnicodeDecodeError: the bytes read are not valid UTF-8.
        MemoryError: the host could not allocate the bytes or their decoding.
    """
    with path.open("rb") as handle:
        data = handle.read(limit + 1)
    if len(data) > limit:
        return "", len(data), True
    return data.decode("utf-8"), len(data), False


# --------------------------------------------------------------------------- #
# The pre-parse structural gate (CWE-400, CWE-674, CWE-770)                   #
#                                                                             #
# ONE IMPLEMENTATION, IN ONE PLACE. The token scanner that enforces the depth, #
# node, string and literal caps on a document's TEXT lives in `paths`, which   #
# is the module that owns the read and the parse of runner-metadata.json and   #
# publishes that document's caps. Both routes into a parse therefore reach the #
# same code: this module's artifact loader through the names below, and        #
# paths.load_runner_metadata internally, on its own read and on a caller's     #
# validated_text alike. Two implementations of one bound is the defect where   #
# the last one silently wins and no reader can see which is in force, which is #
# why these are bindings rather than a second copy of the algorithm (F12).     #
#                                                                             #
# Why the gate exists at all, why its token pattern cannot backtrack           #
# superlinearly, why the string scanner is index arithmetic rather than either  #
# quadratic regex spelling, and why no token is ever materialised, are         #
# documented at the implementation in paths.py -- one account of one algorithm. #
#                                                                             #
# The scan does not adjudicate well-formedness, and json.loads remains the     #
# only thing that has ever produced HALT_ARTIFACT_INVALID_JSON and its message.#
# --------------------------------------------------------------------------- #

#: The end of the string token opening at an index, or ``-1`` where it is unterminated.
#: The shared implementation, bound to the name this module has always used for it.
_string_token_end = paths.string_token_end

#: Bound an untrusted JSON document's structure BEFORE it is parsed: depth, node count,
#: string length and literal length, over the text, in one left-to-right token scan that
#: materialises no value.  The shared implementation in ``paths``, bound to the name this
#: module's artifact loader calls and the tests inspect -- so what is asserted of this name
#: is asserted of the code every route runs, including the metadata route inside
#: ``paths.load_runner_metadata`` (F12).  Raises :data:`_IngestionBoundExceeded`, whose
#: ``bound`` :func:`_halt_reason_for` maps to this module's own halt vocabulary.
_validate_document_bounds = paths.validate_document_bounds

#: The keys :func:`_validate_document_bounds` returns for the three quantities
#: :func:`_measure_document` also measures, so a caller comparing the two verdicts does not
#: have to restate them.  Published by the module that owns the walk.
_STRUCTURAL_MEASUREMENT_KEYS: tuple[str, ...] = paths.STRUCTURAL_MEASUREMENT_KEYS


def _measure_document(document: Any) -> dict[str, int]:
    """Measure a parsed artifact's shape, refusing it the moment a bound is crossed.

    THE SECOND OPINION, NOT THE GATE
    --------------------------------
    :func:`_validate_document_bounds` has already enforced these caps on the document's
    text, before ``json.loads`` allocated anything, which is what makes them bounds rather
    than diagnoses.  This walk runs afterwards over the parsed object graph and is an
    INDEPENDENT CONFIRMATION of that verdict: a different traversal, over a different
    representation, of the same document.  The two must agree, and the direction of the
    agreement is fixed -- the parse can only lose values, because duplicate object members
    collapse onto the last one, so ``measured <= validated`` for all three quantities.  A
    measured figure ABOVE the pre-parse verdict would mean one of the two walks is wrong,
    which is why :func:`_ingest_artifact_document` checks it rather than assuming it.

    Three quantities, each with its own cap: nesting depth (:data:`JSON_DEPTH_LIMIT`),
    node count (:data:`JSON_NODE_LIMIT`) and the longest string (:data:`JSON_STRING_LIMIT`).
    A node is a JSON value; an object member NAME is checked against the string cap but is
    not itself counted as a node, because a hostile 100 MB key is as much of an allocation
    as a hostile 100 MB value and counting it twice would make the node figure mean two
    things.  That definition is the one the observed maxima in :data:`INGESTION_BOUNDS`
    were measured with, so the observed column and the enforced column agree.

    THE TRAVERSAL IS ITERATIVE, AND SO IS ITS MEMORY (CWE-674)
    ---------------------------------------------------------
    Recursion is not an implementation detail here: a recursive depth check against a
    document deep enough to matter overflows the stack while measuring it, which is the
    condition it exists to detect.  So the traversal is an explicit loop.

    The stack holds one ITERATOR per open container rather than one entry per pending
    child, which bounds the traversal's own memory by the depth cap -- 64 iterators --
    rather than by the document's breadth.  A stack of pending children would instead grow
    with the widest array in the document: a 512 MiB artifact can encode on the order of
    a hundred million array elements, and a per-child stack would allocate a tuple for
    every one of them, so the bound checker would be the memory exhaustion.

    Returns:
        ``depth``, ``nodes`` and ``longest_string`` as measured, all three within their caps.

    Raises:
        _IngestionBoundExceeded: a cap was crossed.  Raised at the crossing, so the walk
            does not finish traversing a document it has already refused.
    """
    nodes = 0
    max_depth = 0
    longest = 0
    # One iterator per container currently open. Bounded by JSON_DEPTH_LIMIT + 1 entries.
    stack: list[Iterator[Any]] = []
    value: Any = document
    depth = 1

    while True:
        nodes += 1
        if nodes > JSON_NODE_LIMIT:
            raise _IngestionBoundExceeded(
                paths.DOCUMENT_BOUND_NODES, JSON_NODE_LIMIT, nodes
            )
        if depth > max_depth:
            max_depth = depth
            if max_depth > JSON_DEPTH_LIMIT:
                raise _IngestionBoundExceeded(
                    paths.DOCUMENT_BOUND_DEPTH, JSON_DEPTH_LIMIT, max_depth
                )

        if isinstance(value, str):
            if len(value) > longest:
                longest = len(value)
                if longest > JSON_STRING_LIMIT:
                    raise _IngestionBoundExceeded(
                        paths.DOCUMENT_BOUND_STRING, JSON_STRING_LIMIT, longest
                    )
        elif isinstance(value, dict):
            for name in value:
                # Member names are strings the artifact chose, so they are bounded like
                # any other string. json guarantees they are str, so len() is safe.
                if len(name) > longest:
                    longest = len(name)
                    if longest > JSON_STRING_LIMIT:
                        raise _IngestionBoundExceeded(
                            paths.DOCUMENT_BOUND_MEMBER_NAME,
                            JSON_STRING_LIMIT,
                            longest,
                        )
            stack.append(iter(value.values()))
            depth += 1
        elif isinstance(value, list):
            stack.append(iter(value))
            depth += 1

        # Advance to the next value: the next child of the innermost open container, or,
        # where that container is exhausted, the next child of the one enclosing it.
        while stack:
            try:
                value = next(stack[-1])
            except StopIteration:
                stack.pop()
                depth -= 1
                continue
            break
        else:
            break

    return {"depth": max_depth, "nodes": nodes, "longest_string": longest}


def _ingest_artifact_document(
    tool: str,
    artifact_path: Path,
    *,
    outcome: ArtifactOutcome,
) -> Any:
    """Read, parse and bound one artifact, and record what it measured.

    The whole of the hostile-input surface for an artifact is here, in one order that never
    allocates before it has checked:

    1. ``os.stat`` -- refused above :data:`ARTIFACT_BYTE_LIMIT` before a byte is read;
    2. a bounded read of at most ``cap + 1`` bytes, refused again if the file grew;
    3. a strict UTF-8 decode;
    4. :func:`_validate_document_bounds` over the decoded TEXT -- depth, node count,
       longest string and numeric-literal digits, all four refused before anything is
       allocated for them, because the parse is the allocation and a cap that runs after it
       names an exhaustion instead of preventing one;
    5. ``json.loads``;
    6. :func:`_measure_document` over the parsed document, as an independent confirmation
       of step 4's verdict, with their agreement checked rather than assumed.

    Every failure becomes a NAMED halt through the same machinery as the JSONDecodeError it
    sits beside, and that includes the three exceptions that are not this module's own
    verdict: ``MemoryError``, ``RecursionError`` and a ``ValueError`` that is not a
    ``JSONDecodeError``.  All three are ``Exception`` subclasses, so :func:`main` would
    record them either way -- but it would record them under the single unnamed
    ``unexpected-error`` reason, with the message *described* rather than quoted (it is
    composed from artifact content) and this repository's frames rendered beside it.  That
    record cannot say which artifact, which stage or which limit was reached, and a run
    record that cannot say what stopped the run is the failure this pipeline's evidence
    discipline exists to prevent (AAP 0.1.1).

    Two of those arms are now defence in depth rather than the first line of it, and they
    are retained deliberately.  ``RecursionError`` out of ``json``'s C scanner -- which
    recurses once per nesting level -- and the ``ValueError`` an over-long integer literal
    provokes are both refused by step 4 before ``json.loads`` sees the document, so neither
    should be reachable through the depth or digit dimension any more.  Removing the arms
    on that reasoning would be trusting one function to be correct: they stay, so that a
    document which reaches the parser by any route this walk did not anticipate is still a
    named halt rather than an unnamed one.

    Returns:
        The parsed document, measured and within every bound.

    Raises:
        ConfigurationFault: the artifact exists and could not be read or measured -- a
            fault to correct, exit ``78``.
        NormalizeHalt: the artifact's content was refused -- a halting condition in the
            data, exit ``1``.  ``outcome.parse_status`` is set to
            :data:`PARSE_STATUS_FAILED` and ``outcome.artifact["ingestion"]`` records the
            refusal before either is raised, so the record carries the failure even though
            the exception is what stops the run.
    """
    bounds = {
        "artifact_bytes": ARTIFACT_BYTE_LIMIT,
        "json_depth": JSON_DEPTH_LIMIT,
        "json_nodes": JSON_NODE_LIMIT,
        "json_string_characters": JSON_STRING_LIMIT,
    }

    def refuse(
        reason: str, message: str, /, *, fault: bool = False, **details: Any
    ) -> NormalizeHalt:
        """Mark the outcome failed, record the refusal, and build the named halt.

        The measurements are copied into ``outcome.artifact["ingestion"]`` as well as into
        the halt
        because the two are read by different readers: the halt is the run's stated reason
        for stopping, and the artifact's own entry is where a reader looking at that
        artifact finds what was measured.  Only the measurement keys cross over --
        :data:`_INGESTION_RECORD_KEYS` -- so the artifact entry does not become a second,
        divergent copy of the halt's prose.

        ``fault`` preserves the distinction the exit codes draw.  An artifact that exists
        and cannot be read is a configuration fault to correct (``78``, the same
        ``EX_CONFIG`` the runners use); an artifact whose *content* is refused is a halting
        condition in the data (``1``).  Which of the two a given reason is was decided
        before these bounds existed and is not changed by them.
        """
        outcome.parse_status = PARSE_STATUS_FAILED
        outcome.artifact["ingestion"] = {
            "bounds": dict(bounds),
            "refused": True,
            "refused_reason": reason,
            **{
                key: value
                for key, value in details.items()
                if key in _INGESTION_RECORD_KEYS
            },
        }
        if fault:
            return _fault(reason, message, **details)
        return _halt(reason, message, **details)

    try:
        size = os.stat(artifact_path).st_size
    except OSError as error:
        raise refuse(
            HALT_ARTIFACT_UNREADABLE,
            f"{tool}: the artifact at {artifact_path} exists but cannot be measured: "
            f"{type(error).__name__}: {error}",
            fault=True,
            tool=tool,
            artifact_path=str(artifact_path),
            error=f"{type(error).__name__}: {error}",
        ) from error

    if size > ARTIFACT_BYTE_LIMIT:
        raise refuse(
            HALT_ARTIFACT_TOO_LARGE,
            f"{tool}: the artifact at {artifact_path} is {size} bytes, above the "
            f"{ARTIFACT_BYTE_LIMIT}-byte ingestion cap, so it was refused before any of it "
            "was read. The largest artifact this pipeline has observed is 73,768,116 "
            "bytes; a file this size is not a bigger scan result.",
            tool=tool,
            artifact_path=str(artifact_path),
            observed_bytes=size,
            byte_cap=ARTIFACT_BYTE_LIMIT,
        )

    try:
        text, bytes_read, over_cap = _read_bounded_artifact_text(
            artifact_path, ARTIFACT_BYTE_LIMIT
        )
    except OSError as error:
        raise refuse(
            HALT_ARTIFACT_UNREADABLE,
            f"{tool}: the artifact at {artifact_path} exists but cannot be read: "
            f"{type(error).__name__}: {error}",
            fault=True,
            tool=tool,
            artifact_path=str(artifact_path),
            error=f"{type(error).__name__}: {error}",
        ) from error
    except UnicodeDecodeError as error:
        raise refuse(
            HALT_ARTIFACT_NOT_UTF8,
            f"{tool}: the artifact at {artifact_path} is not valid UTF-8 at byte offset "
            f"{error.start}, so it matches no known shape. It is decoded strictly rather "
            "than with replacement, because a replacement character substituted into a "
            "rule identifier, a message or a path would reach the dataset as data.",
            tool=tool,
            artifact_path=str(artifact_path),
            decode_error=error.reason,
            byte_offset=error.start,
            observed_bytes=size,
        ) from error
    except MemoryError as error:
        raise refuse(
            HALT_ARTIFACT_EXHAUSTED_MEMORY,
            f"{tool}: reading the artifact at {artifact_path} ({size} bytes) exhausted "
            "memory. The read is bounded at "
            f"{ARTIFACT_BYTE_LIMIT} bytes, so this is the host's available memory rather "
            "than an unbounded read.",
            tool=tool,
            artifact_path=str(artifact_path),
            stage="read",
            observed_bytes=size,
            byte_cap=ARTIFACT_BYTE_LIMIT,
        ) from error

    if over_cap:
        raise refuse(
            HALT_ARTIFACT_TOO_LARGE,
            f"{tool}: the artifact at {artifact_path} exceeded the "
            f"{ARTIFACT_BYTE_LIMIT}-byte ingestion cap on read, having measured {size} "
            "bytes a moment earlier, so it grew between the two. Nothing was decoded.",
            tool=tool,
            artifact_path=str(artifact_path),
            observed_bytes=bytes_read,
            measured_bytes_before_read=size,
            byte_cap=ARTIFACT_BYTE_LIMIT,
        )

    # STEP 4: the structural caps, on the text, BEFORE json.loads builds anything. The byte
    # cap above bounds the file and says nothing about the object graph the file encodes, so
    # this is where depth, node count, string length and numeric-literal digits are decided
    # (CWE-400, CWE-674, CWE-770). A refusal here is the same named halt the post-parse walk
    # would have raised, with the same details, so no consumer of the vocabulary changes.
    try:
        validated = _validate_document_bounds(
            text,
            depth_limit=JSON_DEPTH_LIMIT,
            node_limit=JSON_NODE_LIMIT,
            string_limit=JSON_STRING_LIMIT,
            digit_limit=STATUS_NUMERIC_DIGIT_LIMIT,
        )
    except _IngestionBoundExceeded as error:
        raise refuse(
            _halt_reason_for(error),
            f"{tool}: the artifact at {artifact_path} was refused before it was parsed, "
            f"and therefore before it was routed to a shape: {error}. The caps are "
            "measurements plus a margin, published in the run record under "
            "vocabularies.ingestion_bounds, and they are enforced on the document's text "
            "because the object graph a document allocates is not bounded by its byte "
            "size.",
            tool=tool,
            artifact_path=str(artifact_path),
            stage="validate",
            observed_bytes=size,
            **error.details(),
        ) from error
    except MemoryError as error:
        raise refuse(
            HALT_ARTIFACT_EXHAUSTED_MEMORY,
            f"{tool}: validating the artifact at {artifact_path} ({size} bytes) exhausted "
            "memory. The scan holds one entry per open container, bounded by the depth cap "
            f"of {JSON_DEPTH_LIMIT}, and materialises no value, so this is the host's "
            "available memory rather than a walk that grows with the document.",
            tool=tool,
            artifact_path=str(artifact_path),
            stage="validate",
            observed_bytes=size,
            depth_cap=JSON_DEPTH_LIMIT,
        ) from error

    try:
        document = json.loads(text)
    except json.JSONDecodeError as error:
        raise refuse(
            HALT_ARTIFACT_INVALID_JSON,
            f"{tool}: the artifact at {artifact_path} is not valid JSON, so it matches no "
            f"known shape: {error}",
            tool=tool,
            artifact_path=str(artifact_path),
            parser_error=str(error),
            line=error.lineno,
            column=error.colno,
            character_offset=error.pos,
        ) from error
    except RecursionError as error:
        raise refuse(
            HALT_ARTIFACT_EXHAUSTED_STACK,
            f"{tool}: parsing the artifact at {artifact_path} exhausted the interpreter's "
            "stack. json's scanner recurses once per nesting level, so this document is "
            f"nested past the recursion limit -- far past the depth cap of "
            f"{JSON_DEPTH_LIMIT} it would have been refused by had it parsed at all.",
            tool=tool,
            artifact_path=str(artifact_path),
            stage="parse",
            depth_cap=JSON_DEPTH_LIMIT,
            recursion_limit=sys.getrecursionlimit(),
            observed_bytes=size,
        ) from error
    except MemoryError as error:
        raise refuse(
            HALT_ARTIFACT_EXHAUSTED_MEMORY,
            f"{tool}: parsing the artifact at {artifact_path} ({size} bytes) exhausted "
            "memory. The object graph a document allocates is not bounded by its byte "
            "size, which is why the node, depth, string and digit caps are enforced on the "
            f"text before this point -- this document was inside all four "
            f"({validated['nodes']} nodes, depth {validated['depth']}, longest string "
            f"{validated['longest_string']}), so this is the host's available memory rather "
            "than an unbounded document reaching the parser.",
            tool=tool,
            artifact_path=str(artifact_path),
            stage="parse",
            observed_bytes=size,
            node_cap=JSON_NODE_LIMIT,
        ) from error
    except ValueError as error:
        # A ValueError from json.loads that is NOT a JSONDecodeError: the syntax is valid
        # and a literal is not convertible. The documented instance on this interpreter is
        # an integer literal above the 4,300-digit sys.set_int_max_str_digits limit, which
        # is a well-formed JSON number and an int() CPython refuses.
        raise refuse(
            HALT_ARTIFACT_LITERAL_UNCONVERTIBLE,
            f"{tool}: the artifact at {artifact_path} is syntactically valid JSON carrying "
            f"a literal this interpreter refuses to convert: {type(error).__name__}. An "
            "integer literal above CPython's 4,300-digit conversion limit is the "
            "documented case; it is a refusal to allocate rather than a syntax error, so "
            "it is named separately from an invalid document.",
            tool=tool,
            artifact_path=str(artifact_path),
            error_type=type(error).__name__,
            observed_bytes=size,
        ) from error

    try:
        # STEP 6: the same three caps again, over the parsed object graph, as an
        # INDEPENDENT CONFIRMATION of step 4's verdict rather than as the gate. Step 4 is
        # the gate; this is a second traversal of a second representation, and the two
        # disagreeing is a defect in one of them rather than a property of the artifact.
        measurements = _measure_document(document)
    except _IngestionBoundExceeded as error:
        raise refuse(
            _halt_reason_for(error),
            f"{tool}: the artifact at {artifact_path} was refused before it was routed to "
            f"a shape: {error}. The caps are measurements plus a margin, published in the "
            "run record under vocabularies.ingestion_bounds. The pre-parse gate measured "
            f"{validated['nodes']} nodes, depth {validated['depth']} and longest string "
            f"{validated['longest_string']} and accepted the document, so a refusal here "
            "means the two walks disagree and the pre-parse verdict is the one to fix.",
            tool=tool,
            artifact_path=str(artifact_path),
            observed_bytes=size,
            **error.details(),
        ) from error
    except MemoryError as error:
        raise refuse(
            HALT_ARTIFACT_EXHAUSTED_MEMORY,
            f"{tool}: measuring the artifact at {artifact_path} ({size} bytes) exhausted "
            "memory. The walk holds one iterator per open container, bounded by the depth "
            f"cap of {JSON_DEPTH_LIMIT}, so this is the host's available memory rather "
            "than a traversal that grows with the document.",
            tool=tool,
            artifact_path=str(artifact_path),
            stage="measure",
            observed_bytes=size,
            depth_cap=JSON_DEPTH_LIMIT,
        ) from error

    # THE TWO WALKS MUST AGREE, AND IN ONE DIRECTION.
    #
    # The pre-parse scan counts tokens; this walk counts values; and the parse between them
    # can only LOSE values, because duplicate object members collapse onto the last one and
    # a decoded string is never longer than the raw token it came from. So a measured figure
    # at or below the validated one is expected, and a measured figure ABOVE it is arithmetic
    # neither walk can produce from the same document -- it means one of them is wrong. That
    # is a defect in this module rather than a fault in the artifact, so it is refused
    # explicitly here rather than left to a reader to notice in the record. Three integer
    # comparisons on the accepted path.
    disagreements = [
        f"{key}: the pre-parse gate measured {validated[key]} and the confirmation walk "
        f"measured {measurements[key]}"
        for key in _STRUCTURAL_MEASUREMENT_KEYS
        if measurements[key] > validated[key]
    ]
    if disagreements:
        raise refuse(
            HALT_UNEXPECTED,
            f"{tool}: the artifact at {artifact_path} measured larger after parsing than "
            "the pre-parse gate measured before it, which no document can do -- the parse "
            "only ever collapses duplicate object members and shortens escaped strings. "
            f"One of the two walks is wrong: {'; '.join(disagreements)}.",
            tool=tool,
            artifact_path=str(artifact_path),
            stage="confirm",
            observed_bytes=size,
        )

    # Recorded per artifact rather than asserted once: a bound that was never crossed is
    # only visible as a measurement beside its cap, and the same figures are what a later
    # run's record is compared against.
    outcome.artifact["ingestion"] = {
        "bounds": dict(bounds),
        "refused": False,
        "observed_bytes": size,
        "bytes_read": bytes_read,
        "depth": measurements["depth"],
        "nodes": measurements["nodes"],
        "longest_string": measurements["longest_string"],
        "node_definition": (
            "a node is a JSON value; an object member name is length-checked against the "
            "string cap but is not itself counted as a node"
        ),
        "walk": (
            "iterative, one iterator per open container, so the traversal's own memory is "
            "bounded by the depth cap rather than by the document's breadth"
        ),
        "measured_before_routing": True,
    }
    return document


def _process_present_artifact(
    tool: str,
    artifact_path: Path,
    *,
    metadata: Mapping[str, Any],
    root: str,
    globs: Sequence[str],
    tally: severity.LiteralTally,
    source_index: paths.SourceIndex | None,
    log_dir: Path,
    outcome: ArtifactOutcome,
) -> tuple[list[dict[str, Any]], reconcile.ArtifactCounts]:
    """Parse, route and adapt one present artifact, mutating ``outcome`` as it goes.

    ``outcome`` is already in the run record's artifact list when this is called, so every
    halt below leaves the evidence gathered so far in the record rather than only in the
    exception -- which is what makes a halt diagnosable from the record.

    The independent record count is taken *before* the adapter runs: it is the half of the
    reconciliation that builds nothing, and taking it first means even an adapter that halts
    leaves the artifact's true record count behind.

    Returns:
        The rows this artifact contributed, and its :class:`normalize.reconcile.ArtifactCounts`.
    """
    outcome.runner_status = _runner_status(log_dir, tool)
    outcome.network_fetch = _network_fetch_disclosure(outcome.runner_status)
    _check_runner_root_evidence(tool, outcome.runner_status, root)
    # Streams are described but not read for a present artifact: the classification does
    # not depend on their content, and one of them can run to hundreds of megabytes.
    outcome.tool_words = _tool_words(log_dir, tool, with_text=False)

    # Read, parsed and bounded in one place, before anything routes it: an artifact is
    # provisioning-supplied input this module did not write, and every one of the bounds
    # and every one of the named halts is in _ingest_artifact_document.
    document = _ingest_artifact_document(tool, artifact_path, outcome=outcome)

    try:
        decision = shape.route_artifact(artifact_path, document)
    except shape.UnknownArtifactShape as error:
        outcome.parse_status = PARSE_STATUS_FAILED
        # The observed structure is quoted verbatim from shape.py's own record, so the halt
        # cites one measurement rather than a second reconstruction of it. Its 'reason' is
        # renamed on the way in: this halt's reason is the halting condition, and the
        # detection reason is which of the three shape tests failed (shape.HALT_REASONS).
        detection = dict(error.details())
        detection["detection_reason"] = detection.pop("reason", None)
        raise _halt(
            HALT_UNKNOWN_ARTIFACT_SHAPE,
            f"{tool}: {error}",
            **detection,
        ) from error
    # The decision, and separately the evidence for it. AAP 0.6.1 wants the detection
    # outcome per artifact "including the evidence (the two field checks)": a native
    # artifact recorded as native must be *seen* to have failed both tests rather than
    # asserted to have failed them, because a permissive detector that accepted one as
    # SARIF would yield an empty result set rather than an error -- and an empty result
    # set is indistinguishable from a clean scan. The evidence comes from shape.py, the
    # module that owns the test, so the record cites the decision's own measurement.
    outcome.routing = {
        **decision.as_dict(),
        "detection": shape.detection_evidence(document),
    }

    # The independent traversal: it walks the count units and builds no row.
    outcome.raw_records = reconcile.count_records(tool, document)

    try:
        tool_base = paths.tool_path_base(metadata, decision.tool)
    except paths.RunnerMetadataError as error:
        raise _fault(
            HALT_RUNNER_METADATA,
            f"{tool}: wrote an artifact, but the runner metadata supplies no usable path "
            f"base for it: {error}. A tool's base is not something to infer, and "
            "defaulting to the root would make every one of its paths wrong in the same "
            "direction.",
            tool=tool,
            runner_metadata_error=str(error),
        ) from error

    adapter = resolve_adapter(decision)
    keywords: dict[str, Any] = {
        "tool": decision.tool,
        "root": root,
        "tool_base": tool_base,
        "allowlist": globs,
        "tally": tally,
    }
    if decision.tool == "joern":
        # The one adapter that takes an index: built once per run and injected, so the
        # tree is walked once rather than once per artifact.
        keywords["source_index"] = source_index

    try:
        rows, rejections, counters = adapter.adapt(document, **keywords)
    except trivy.UnsupportedTrivySection as error:
        outcome.parse_status = PARSE_STATUS_FAILED
        raise _halt(
            HALT_ADAPTER_STRUCTURAL,
            f"{tool}: {error}",
            tool=tool,
            artifact_path=str(artifact_path),
            adapter_reason=error.reason,
            section=error.section,
            target=error.target,
            result_index=error.result_index,
            element_count=error.element_count,
            structure=dict(error.structure),
            adapter_note=error.note,
        ) from error
    except (ValueError, TypeError) as error:
        # Every adapter's contract error derives from ValueError -- SarifAdapterError,
        # TrivyAdapterError, GitleaksAdapterError, CheckovAdapterError,
        # DependencyCheckAdapterError, JoernAdapterError, paths.PathPolicyError and
        # severity.SeverityPolicyError all do. None is absorbed into a rejection count.
        #
        # The adapter composes that error's text from the artifact -- an observed rule
        # identifier, an observed URI, an observed section name -- and this halt is
        # persisted into normalize-run.json and quoted into tool-status.md. So the error's
        # message is described through paths.safe_diagnostic (type, length, digest,
        # bounded redacted excerpt) rather than interpolated raw: a control sequence would
        # otherwise rewrite what an operator reading the halt sees, and a URI carrying
        # userinfo would put whatever credential the artifact held into a durable file.
        # The exception type is named in full, because that is the actionable half and it
        # is this repository's own vocabulary rather than the artifact's.
        outcome.parse_status = PARSE_STATUS_FAILED
        rendered = paths.safe_diagnostic(
            str(error), context=f"{type(error).__name__} from {decision.adapter} adapter"
        )
        raise _halt(
            HALT_ADAPTER_CONTRACT,
            f"{tool}: its adapter refused the artifact or the arguments it was given: "
            f"{type(error).__name__}: {rendered}",
            tool=tool,
            artifact_path=str(artifact_path),
            adapter=decision.adapter,
            adapter_module=decision.adapter_module_name,
            error_type=type(error).__name__,
            error=rendered.as_dict(),
        ) from error

    rejections_by_class: dict[str, int] = {}
    for rejection in rejections:
        rejections_by_class[rejection.reject_class] = (
            rejections_by_class.get(rejection.reject_class, 0) + 1
        )

    outcome.emitted_rows = len(rows)
    outcome.rejected_records = len(rejections)
    outcome.rejections_by_class = rejections_by_class
    # Complete rather than sampled: AAP 0.5.4 requires every rejected record counted under
    # its named class with its sub-reason retained verbatim.
    outcome.rejections = [rejection.as_dict() for rejection in rejections]
    outcome.counters = dict(counters)
    outcome.counter_summary = _counter_summary(counters)
    outcome.path_kinds = _path_kind_tally(counters).as_dict()
    outcome.extras = _adapter_extras(decision.tool, document, source_index)
    outcome.parse_status = (
        PARSE_STATUS_PARTIAL if rejections else PARSE_STATUS_CLEAN
    )
    if outcome.artifact_expected is False:
        outcome.notes.append(
            "The runner metadata records artifact_expected=false for this tool, and an "
            "artifact is present. Recorded as a difference; the artifact is normalized on "
            "its own merits."
        )
    exit_code = outcome.runner_status.get("exit_code")
    if isinstance(exit_code, int) and exit_code != 0:
        outcome.notes.append(
            f"The runner exited {exit_code} and wrote a parsable artifact. Artifact status "
            "and exit status are independent (AAP 0.5.4); the code is recorded as a fact "
            "and used for nothing else."
        )
    if outcome.runner_status.get("exit_status") == EXIT_STATUS_TIMEOUT:
        outcome.notes.append(
            "The status record carries no readable exit code, so exit_status is "
            "'timeout'. The artifact is present and parsable, which is the condition "
            "AAP 0.9.3 records rather than halts on."
        )

    return list(rows), reconcile.ArtifactCounts.for_present_artifact(
        tool,
        raw_records=outcome.raw_records,
        emitted_rows=outcome.emitted_rows,
        rejected_records=outcome.rejected_records,
        rejections_by_class=rejections_by_class,
    )


def _process_absent_artifact(
    tool: str,
    *,
    root: str,
    log_dir: Path,
    outcome: ArtifactOutcome,
) -> reconcile.ArtifactCounts:
    """Classify one absent artifact using only the tool's own stated words.

    AAP 0.5.4 draws the whole distinction here.  An artifact absent *and* a no-work reason
    stated by the tool itself is the ``absent`` status: the words are quoted verbatim, the
    tool contributes zero rows and the run continues -- OSV-Scanner's exit 128 with *"No
    package sources found, --help for usage information."* being the expected instance.  An
    artifact absent with **no** stated reason halts, and a termination that produced no exit
    code does not change that: ``exit_status: timeout`` names how the process ended, it does
    not excuse the absence.

    "Stated a reason" is decided by :func:`_classify_no_work` against the exact-sentence
    table, never by the streams merely being non-empty.  That distinction is the whole of
    this function's correctness: a stack trace, a permission error, a truncated crash
    message and a JVM heap failure are all non-empty output, none of them is an account of
    a tool having nothing to do, and admitting any of them would convert a failed runner
    into a continuing ``absent`` status carrying zero rows -- invisible in a row-only
    ``findings.json`` by construction.  So the two halts are separated by name: streams
    with no words at all halt under :data:`HALT_ABSENT_WITHOUT_STATED_REASON`, and streams
    whose words match no recognised statement halt under
    :data:`HALT_ABSENT_WITHOUT_NO_WORK_STATEMENT`.  Both preserve the streams verbatim in
    ``tool_words`` and name their paths in the halt details, because the verdict this
    function reaches is not a substitute for the tool's own text -- a reader adjudicates
    from that text, and a halt that quoted nothing would make them go looking for it.
    """
    outcome.runner_status = _runner_status(log_dir, tool)
    outcome.network_fetch = _network_fetch_disclosure(outcome.runner_status)
    # A runner that scanned the wrong tree is a targeting fault whether or not it wrote
    # anything, so the evidence is checked here too.
    _check_runner_root_evidence(tool, outcome.runner_status, root)
    outcome.tool_words = _tool_words(log_dir, tool, with_text=True)
    classification = _classify_no_work(tool, outcome.tool_words, outcome.runner_status)
    outcome.tool_words["no_work_classification"] = classification

    if not classification["classified"]:
        words_present = classification["words_present"]
        reason = (
            HALT_ABSENT_WITHOUT_NO_WORK_STATEMENT
            if words_present
            else HALT_ABSENT_WITHOUT_STATED_REASON
        )
        message = (
            f"{tool}: no artifact in the raw tree, and its output states no no-work "
            "reason this pipeline recognises for it. The streams carry text and that "
            "text is not one of the tool's documented no-work statements, so it does not "
            "establish that the tool completed with nothing in scope to work on -- a "
            "stack trace, a permission error or a truncated crash message is output, not "
            "an account. Both streams are preserved verbatim in tool_words and their "
            "paths are in 'details'; the recognised statements for this tool, if any, are "
            "in no_work_classification."
            if words_present
            else f"{tool}: no artifact in the raw tree and no reason stated in its own "
            "output. Only the tool's own words can settle whether it completed with "
            "nothing in scope to work on or failed, so this halts rather than being "
            "recorded as a zero. Looked for the artifact and for the tool's words at the "
            "paths in 'details'."
        )
        raise _halt(
            reason,
            message,
            tool=tool,
            artifact_path=outcome.artifact.get("path"),
            stderr_log=outcome.tool_words["streams"]["stderr"].get("path"),
            stdout_log=outcome.tool_words["streams"]["stdout"].get("path"),
            status_path=outcome.runner_status.get("path"),
            exit_code=outcome.runner_status.get("exit_code"),
            exit_status=outcome.runner_status.get("exit_status"),
            artifact_expected=outcome.artifact_expected,
            words_present=words_present,
            words_stream=classification["words_stream"],
            no_work_classification=classification,
            note=(
                "exit_status names how the process ended; it does not excuse a missing "
                "artifact (AAP 0.8.1). The absent status requires a no-work reason the "
                "tool stated in its own words (AAP 0.5.4), matched against the exact "
                "sentences recorded for it rather than against the presence of output."
            ),
        )

    outcome.parse_status = PARSE_STATUS_ABSENT
    outcome.raw_records = None
    outcome.emitted_rows = 0
    outcome.rejected_records = 0
    outcome.notes.append(
        "Classified from the tool's own words in "
        f"{classification['matched_stream']}, matching its recorded no-work statement "
        f"{classification['matched_sentence']!r} and quoted verbatim in tool_words. "
        "Zero rows, and the reconciliation for this tool is the not-applicable sentinel "
        "rather than 0 = 0 + 0, which would be a passing assertion over an artifact nobody "
        "looked at."
    )
    agreement = classification["exit_code_agrees_with_statement"]
    if agreement is False:
        outcome.notes.append(
            f"The runner exited {classification['exit_code']}, which is not among the "
            f"codes this statement has been recorded with "
            f"({classification['matched_statement']['exit_codes']}). Recorded as a "
            "difference: the classification rests on the tool's own words (AAP 0.5.4) "
            "and the exit code is corroboration, so a disagreement is reported rather "
            "than allowed to decide the case."
        )
    elif agreement is None:
        outcome.notes.append(
            "No readable exit code is recorded for this runner, so the statement's "
            "recorded exit code could not be corroborated. The classification rests on "
            "the tool's own words either way."
        )
    if outcome.artifact_expected is True:
        outcome.notes.append(
            "The runner metadata records artifact_expected=true for this tool and no "
            "artifact is present. Recorded as a difference, with the tool's own stated "
            "reason above."
        )
    if outcome.runner_status.get("exit_status") == EXIT_STATUS_TIMEOUT:
        outcome.notes.append(
            "The status record carries no readable exit code, so exit_status is "
            "'timeout'. It names how the process ended and does not excuse the absent "
            "artifact; the tool's own stated reason is what settles this case."
        )
    literal = outcome.runner_status.get("artifact_bytes_literal")
    if isinstance(literal, str) and literal.strip() == "MISSING":
        outcome.notes.append(
            "The runner's own status record independently reports the artifact as MISSING."
        )
    return reconcile.ArtifactCounts.for_absent_artifact(tool)


# --------------------------------------------------------------------------- #
# The stages, each returning what the next one needs                          #
# --------------------------------------------------------------------------- #


def _verify_vocabularies(record: dict[str, Any]) -> None:
    """Assert the four canonical-tool tuples and the field list agree with each other.

    ``shape``, ``paths``, ``reconcile`` and ``severity`` each author the nine canonical
    identifiers.  A disagreement between them would silently exclude a tool from one
    document while including it in another, so it is settled before anything is read.  The
    twelve fields are ``emit``'s to author; this checks the count and the boundary members
    rather than writing a second copy of the list.
    """
    vocabularies = {
        "shape": tuple(shape.CANONICAL_TOOLS),
        "paths": tuple(paths.CANONICAL_TOOLS),
        "reconcile": tuple(reconcile.CANONICAL_TOOLS),
        "severity": tuple(severity.CANONICAL_TOOLS),
    }
    as_sets = {name: frozenset(value) for name, value in vocabularies.items()}
    distinct = set(as_sets.values())
    record["vocabularies"] = {
        "canonical_tools": {name: list(value) for name, value in vocabularies.items()},
        "processing_order": list(ARTIFACT_ORDER),
        "processing_order_source": "normalize.shape.CANONICAL_TOOLS",
        "fields": list(emit.FIELDS),
        "optional_fields": sorted(emit.OPTIONAL_FIELDS),
        "artifact_filenames": list(shape.ARTIFACT_FILENAMES),
        "reject_classes": list(paths.REJECT_CLASSES),
        "parse_statuses": list(PARSE_STATUSES),
        # The closed set of halting reasons, published so that a reader can enumerate what
        # this module can stop for without reading its source -- and so that a halt reason
        # a record carries is checkable against the vocabulary of the run that wrote it.
        "halt_reasons": list(HALT_REASONS),
        "status_defect_classes": [
            STATUS_DEFECT_DUPLICATE_KEY,
            STATUS_DEFECT_UNPARSABLE_LINE,
            STATUS_DEFECT_FILE_TOO_LARGE,
            STATUS_DEFECT_NUMERIC_LITERAL_TOO_LONG,
        ],
        # What was enforced on every file read, with the measurements the caps were set
        # from. A bound that never fires is only visible here.
        # dict() rather than the mapping proxy itself: the proxy is not JSON-serialisable,
        # and a copy is what the record should carry anyway -- publishing the live mapping
        # would let a later mutation appear to have been what this run enforced.
        "ingestion_bounds": dict(INGESTION_BOUNDS),
    }
    if len(distinct) != 1:
        raise _fault(
            HALT_VOCABULARY_MISMATCH,
            "the canonical tool vocabularies do not describe one set of identifiers: "
            + "; ".join(f"{name}={list(value)}" for name, value in vocabularies.items()),
            vocabularies={name: list(value) for name, value in vocabularies.items()},
        )
    if len(emit.FIELDS) != 12 or emit.FIELDS[0] != "tool" or emit.FIELDS[-1] != "in_scope":
        raise _fault(
            HALT_VOCABULARY_MISMATCH,
            "emit.FIELDS is not the twelve fields in the request's order, first 'tool' and "
            f"last 'in_scope'; observed {list(emit.FIELDS)}",
            fields=list(emit.FIELDS),
        )
    if not frozenset(emit.OPTIONAL_FIELDS) < frozenset(emit.FIELDS):
        raise _fault(
            HALT_VOCABULARY_MISMATCH,
            "emit.OPTIONAL_FIELDS is not a proper subset of emit.FIELDS; the absence "
            "convention would then permit a field the schema does not carry",
            fields=list(emit.FIELDS),
            optional_fields=sorted(emit.OPTIONAL_FIELDS),
        )
    if len(shape.ARTIFACT_FILENAMES) != len(ARTIFACT_ORDER):
        raise _fault(
            HALT_VOCABULARY_MISMATCH,
            "shape.ARTIFACT_FILENAMES does not carry one filename per canonical tool; "
            f"observed {list(shape.ARTIFACT_FILENAMES)}",
            artifact_filenames=list(shape.ARTIFACT_FILENAMES),
        )
    # The bound-to-halt mapping must be total in both directions against the bounds the
    # shared gate can raise, and every reason it names must be in the closed vocabulary.
    # A bound with no halt name would be recorded under the unnamed unexpected-error
    # reason, which cannot say which cap the document crossed; a reason outside the
    # vocabulary would raise inside NormalizeHalt at the moment of the refusal. Neither is
    # published as a new record key: this is an invariant of the source, and the record
    # already carries both vocabularies it relates.
    if set(_BOUND_HALT_REASONS) != set(paths.DOCUMENT_BOUNDS) or not set(
        _BOUND_HALT_REASONS.values()
    ) <= set(HALT_REASONS):
        raise _fault(
            HALT_VOCABULARY_MISMATCH,
            "the ingestion bounds and the halt reasons they map to do not describe one "
            f"vocabulary: bounds={list(paths.DOCUMENT_BOUNDS)}, "
            f"mapped={sorted(_BOUND_HALT_REASONS)}",
            document_bounds=list(paths.DOCUMENT_BOUNDS),
            mapped_bounds=sorted(_BOUND_HALT_REASONS),
        )


def _load_metadata(inputs: Inputs, record: dict[str, Any]) -> Mapping[str, Any]:
    """Read ``runner-metadata.json`` -- the normalizer's declared input.

    AAP 0.6.4 fixes the direction: Stage 1 writes this file, the normalizer reads it, and
    ``tool-status.md`` is rendered afterwards from it joined with these results.  Nothing
    here reads any Markdown.

    BOUNDED BEFORE IT IS PARSED, AND ON BOTH SIDES OF THE CALL (CWE-400, CWE-674, CWE-770)
    --------------------------------------------------------------------------------------
    The metadata is the same kind of input as an artifact -- a JSON file this module did
    not write -- so it gets the same treatment in the same order.  ``os.stat`` against
    :data:`RUNNER_METADATA_BYTE_LIMIT` first, with the committed metadata at 222,083 bytes
    for scale.  Then the file is read once through :func:`_read_bounded_artifact_text` and
    its text is handed to :func:`_validate_document_bounds` against the three shape caps
    ``paths`` publishes -- ``paths.METADATA_DEPTH_LIMIT`` / ``METADATA_NODE_LIMIT`` /
    ``METADATA_STRING_LIMIT`` -- plus the package's numeric-literal digit bound.  Only then
    is ``paths.load_runner_metadata`` called.

    THE GATE IS ON BOTH SIDES, AND IT IS THE SAME GATE.  ``_validate_document_bounds`` is a
    binding of the walk ``paths`` owns, and ``paths.load_runner_metadata`` runs that same
    walk internally on whatever text it is about to parse -- its own read or the
    ``validated_text`` handed here.  So the refusal below is not the only thing standing
    between a hostile document and ``json.loads``: a document swapped between this side's
    read and that function's parse is refused there, on the text, before the parse (F12).
    What this side adds is the fault the run record carries, with the stage and the
    measurement a reader of ``normalize-run.json`` needs.

    THE FILE IS READ ONCE, ON EVERY ROUTE.  The text this side validated is handed to
    ``paths.load_runner_metadata`` through its ``validated_text`` parameter, so that
    function parses those exact bytes instead of reading the file again.  A replacement
    between a validating read and a parsing read therefore has no window to land in
    (CWE-367).  That module's post-parse shape check still runs over the parsed document
    as the second opinion, exactly as :func:`_measure_document` is for an artifact.

    A FAILURE TO DECODE IS ANSWERED HERE, FROM THE BYTES ALREADY HELD.  Handing the path
    over for a second read after this side's read failed would reopen exactly the window
    ``validated_text`` closes: a file this side could not decode, replaced with
    structurally hostile valid JSON, would then be read afresh by another function
    (CWE-367).  So the decode failure is this module's fault to raise, naming the metadata,
    its measured size and the decode stage, and no second read of any kind happens.

    All the caps are published in :data:`INGESTION_BOUNDS`, so the record states every bound
    that was enforced rather than only the ones enforced here.  ``MemoryError``,
    ``RecursionError`` and a ``ValueError`` raised inside that parse are converted here too,
    for the same reason the artifact path converts them: all three are ``Exception``
    subclasses that would otherwise be recorded under the single unnamed ``unexpected-error``
    reason, which cannot say that it was the metadata that failed or at what size.
    """
    try:
        metadata_bytes = os.stat(inputs.runner_metadata).st_size
    except OSError as error:
        raise _fault(
            HALT_RUNNER_METADATA,
            f"the runner metadata at {inputs.runner_metadata} cannot be measured: "
            f"{type(error).__name__}: {error}. It is Stage 1's output and this module's "
            "input; no path base can be established without it.",
            runner_metadata=str(inputs.runner_metadata),
            error=f"{type(error).__name__}: {error}",
        ) from error
    if metadata_bytes > RUNNER_METADATA_BYTE_LIMIT:
        raise _fault(
            HALT_RUNNER_METADATA,
            f"the runner metadata at {inputs.runner_metadata} is {metadata_bytes} bytes, "
            f"above the {RUNNER_METADATA_BYTE_LIMIT}-byte cap on this input, so it was "
            "refused before any of it was read. The committed metadata is 222,083 bytes; "
            "a file this size is not a longer account of nine runners.",
            runner_metadata=str(inputs.runner_metadata),
            observed_bytes=metadata_bytes,
            byte_cap=RUNNER_METADATA_BYTE_LIMIT,
        )

    # The structural caps, on the text, before paths.load_runner_metadata parses anything.
    # The caps are that module's own published numbers -- one binding, read here rather than
    # restated -- so the two sides cannot enforce two different bounds on one file.
    try:
        text, _, grew = _read_bounded_artifact_text(
            inputs.runner_metadata, RUNNER_METADATA_BYTE_LIMIT
        )
    except UnicodeDecodeError as error:
        # Refused from the bytes already held, and NOT delegated by handing the path over
        # for a second read: a file this side could not decode, replaced with structurally
        # hostile but valid JSON before another function reads it, is exactly the
        # check-then-use window this side exists to close (CWE-367). The verdict keeps the
        # `decode` stage it has always carried, and the diagnostic keeps naming the
        # metadata, the size measured a moment earlier, and what was wrong with the bytes.
        raise _fault(
            HALT_RUNNER_METADATA,
            f"the runner metadata at {inputs.runner_metadata} is {metadata_bytes} bytes "
            f"and is not valid UTF-8: {type(error).__name__}: {error}. It is Stage 1's "
            "output and this module's input; a document that cannot be decoded cannot be "
            "bounded or parsed, and it is refused from the bytes this read already holds "
            "rather than re-read by anything else.",
            runner_metadata=str(inputs.runner_metadata),
            stage="decode",
            observed_bytes=metadata_bytes,
            error=f"{type(error).__name__}: {error}",
        ) from error
    except OSError as error:
        raise _fault(
            HALT_RUNNER_METADATA,
            f"the runner metadata at {inputs.runner_metadata} cannot be read: "
            f"{type(error).__name__}: {error}. It is Stage 1's output and this module's "
            "input; no path base can be established without it.",
            runner_metadata=str(inputs.runner_metadata),
            stage="read",
            error=f"{type(error).__name__}: {error}",
        ) from error
    except MemoryError as error:
        raise _fault(
            HALT_RUNNER_METADATA,
            f"reading the runner metadata at {inputs.runner_metadata} "
            f"({metadata_bytes} bytes) exhausted memory. The read is bounded at "
            f"{RUNNER_METADATA_BYTE_LIMIT} bytes, so this is the host's available memory "
            "rather than an unbounded read.",
            runner_metadata=str(inputs.runner_metadata),
            stage="read",
            observed_bytes=metadata_bytes,
            byte_cap=RUNNER_METADATA_BYTE_LIMIT,
        ) from error
    else:
        if grew:
            raise _fault(
                HALT_RUNNER_METADATA,
                f"the runner metadata at {inputs.runner_metadata} exceeded the "
                f"{RUNNER_METADATA_BYTE_LIMIT}-byte cap on read, having measured "
                f"{metadata_bytes} bytes a moment earlier, so it grew between the two. "
                "Nothing was decoded.",
                runner_metadata=str(inputs.runner_metadata),
                stage="read",
                observed_bytes=metadata_bytes,
                byte_cap=RUNNER_METADATA_BYTE_LIMIT,
            )
    # Unconditional: every route out of the read above either raised or produced text, so
    # there is no arm on which this gate is skipped and no `None` to test for.
    try:
        _validate_document_bounds(
            text,
            depth_limit=paths.METADATA_DEPTH_LIMIT,
            node_limit=paths.METADATA_NODE_LIMIT,
            string_limit=paths.METADATA_STRING_LIMIT,
            digit_limit=STATUS_NUMERIC_DIGIT_LIMIT,
        )
    except _IngestionBoundExceeded as error:
        # One reason, not a new one: a metadata refusal has always been
        # HALT_RUNNER_METADATA, and `error.bound` names the cap the document crossed, so it
        # is the measurement that is forwarded rather than the artifact-side halt name.
        raise _fault(
            HALT_RUNNER_METADATA,
            f"the runner metadata at {inputs.runner_metadata} was refused on shape "
            f"before it was parsed: its {error.bound} is {error.observed}, above the "
            f"{error.limit} cap on this input. The caps are enforced on the document's "
            "text because the object graph a document allocates is not bounded by its "
            "byte size, and a runner metadata document is a flat record of nine "
            "runners: the committed one reaches depth 7, holds 1,315 values and carries "
            "no string longer than 1,421 characters.",
            runner_metadata=str(inputs.runner_metadata),
            stage="validate",
            observed_bytes=metadata_bytes,
            **error.details(),
        ) from error
    except MemoryError as error:
        raise _fault(
            HALT_RUNNER_METADATA,
            f"validating the runner metadata at {inputs.runner_metadata} "
            f"({metadata_bytes} bytes) exhausted memory. The scan holds one entry per "
            "open container and materialises no value, so this is the host's available "
            "memory rather than a walk that grows with the document.",
            runner_metadata=str(inputs.runner_metadata),
            stage="validate",
            observed_bytes=metadata_bytes,
        ) from error
    try:
        # The validated text is handed over rather than the file being read a second
        # time: the bytes parsed are then exactly the bytes the gate above accepted,
        # which closes the check-then-use window between the two reads (CWE-367). That
        # function re-runs this same walk on the text it is handed before it parses it, so
        # the parameter saves the read and never the check.
        document = paths.load_runner_metadata(
            inputs.runner_metadata, validated_text=text
        )
    except paths.RunnerMetadataError as error:
        # The raising module attaches its own structured detail -- which step refused the
        # document, the value it measured and the cap it crossed -- and it is forwarded
        # rather than flattened into the message, so the record says at which step the
        # metadata failed whichever module detected it (CWE-703).  `str(error)` is kept
        # under `error` for a reader who wants the sentence as one line.
        raise _fault(
            HALT_RUNNER_METADATA,
            f"the runner metadata at {inputs.runner_metadata} cannot be used: {error}",
            runner_metadata=str(inputs.runner_metadata),
            error=str(error),
            **{
                key: value
                for key, value in getattr(error, "details", {}).items()
                if key not in ("runner_metadata", "error")
            },
        ) from error
    except OSError as error:
        raise _fault(
            HALT_RUNNER_METADATA,
            f"the runner metadata at {inputs.runner_metadata} cannot be read: "
            f"{type(error).__name__}: {error}. It is Stage 1's output and this module's "
            "input; no path base can be established without it.",
            runner_metadata=str(inputs.runner_metadata),
            error=f"{type(error).__name__}: {error}",
        ) from error
    except RecursionError as error:
        # json's scanner recurses per nesting level, so a metadata document nested past
        # the interpreter's limit raises this from inside the parse rather than returning
        # something a schema check could reject (CWE-674).
        raise _fault(
            HALT_RUNNER_METADATA,
            f"parsing the runner metadata at {inputs.runner_metadata} "
            f"({metadata_bytes} bytes) exhausted the interpreter's stack: it is nested "
            "past the recursion limit. A runner metadata document is a flat record of nine "
            "runners; this one is not that.",
            runner_metadata=str(inputs.runner_metadata),
            stage="parse",
            observed_bytes=metadata_bytes,
            recursion_limit=sys.getrecursionlimit(),
        ) from error
    except MemoryError as error:
        raise _fault(
            HALT_RUNNER_METADATA,
            f"reading the runner metadata at {inputs.runner_metadata} "
            f"({metadata_bytes} bytes) exhausted memory. The size is bounded at "
            f"{RUNNER_METADATA_BYTE_LIMIT} bytes, so this is the host's available memory "
            "rather than an unbounded read.",
            runner_metadata=str(inputs.runner_metadata),
            stage="read",
            observed_bytes=metadata_bytes,
            byte_cap=RUNNER_METADATA_BYTE_LIMIT,
        ) from error
    except ValueError as error:
        # Every ValueError the parse can raise that paths.py did not convert into its own
        # RunnerMetadataError: a malformed document, or a literal this interpreter refuses
        # to convert (an integer above CPython's 4,300-digit limit). Named here rather than
        # left to main's unnamed unexpected-error arm.
        raise _fault(
            HALT_RUNNER_METADATA,
            f"the runner metadata at {inputs.runner_metadata} could not be parsed: "
            f"{type(error).__name__}. It is Stage 1's output and this module's input; no "
            "path base can be established without it.",
            runner_metadata=str(inputs.runner_metadata),
            error_type=type(error).__name__,
            observed_bytes=metadata_bytes,
        ) from error

    recorded_tools = list(paths.metadata_tools(document))
    smoke = document.get("smoke_override")
    record["runner_metadata"] = {
        "file": _file_record(inputs.runner_metadata),
        "schema_version": document.get("schema_version"),
        "spark_src": document.get("spark_src"),
        "spark_src_commit": document.get("spark_src_commit"),
        "generated_at_gate": document.get("generated_at_gate"),
        "finalised_at_stage1": document.get("finalised_at_stage1"),
        "tools_recorded": recorded_tools,
        "tools_missing_from_metadata": [
            tool for tool in ARTIFACT_ORDER if tool not in recorded_tools
        ],
        "smoke_override_state_as_recorded": (
            smoke.get("state") if isinstance(smoke, Mapping) else None
        ),
        "smoke_override_live_environment": (
            "set" if os.environ.get("HARNESS_SMOKE_TARGET") else "absent"
        ),
        "direction": (
            "Stage 1 writes runner-metadata.json -> the normalizer reads it as input -> "
            "oss-scan-results/tool-status.md is rendered afterwards from this metadata "
            "joined with these results. This module never reads tool-status.md."
        ),
    }
    return document


def _resolve_scan_root(
    inputs: Inputs,
    metadata: Mapping[str, Any],
    record: dict[str, Any],
) -> str:
    """Settle the one root every emitted path is expressed against.

    The argument is authoritative for the run and is required to agree with the root the
    runner metadata records: every relativization is against this value, so a wrong root
    would make every path in the dataset wrong in the same direction.
    """
    try:
        metadata_root = paths.metadata_scan_root(metadata)
    except paths.RunnerMetadataError as error:
        raise _fault(
            HALT_RUNNER_METADATA,
            f"the runner metadata records no usable scan root: {error}",
            runner_metadata=str(inputs.runner_metadata),
            error=str(error),
        ) from error

    root = paths.normalise_reported_path(inputs.spark_src)
    record["scan_root"] = {
        "argument": root,
        "argument_source": "--spark-src, defaulted from $SPARK_SRC",
        "runner_metadata": metadata_root,
        "agree": _same_root(root, metadata_root),
        "exists": os.path.exists(root),
        "is_directory": os.path.isdir(root),
        "note": (
            "Every emitted path is expressed relative to this root, and no absolute path "
            "is ever emitted (AAP 0.8.2)."
        ),
    }
    if not paths.is_absolute_path(root):
        raise _fault(
            HALT_SCAN_ROOT_NOT_ABSOLUTE,
            f"the scan root {root!r} is not absolute; every path in the dataset is "
            "expressed against it, so a relative root would make every row wrong in the "
            "same direction",
            scan_root=root,
        )
    if not record["scan_root"]["agree"]:
        raise _fault(
            HALT_SCAN_ROOT_DISAGREEMENT,
            f"the scan root given to the normalizer ({root!r}) is not the root the runner "
            f"metadata records ({metadata_root!r}). Resolving paths against a root the "
            "runners did not use would silently corrupt every path in the dataset.",
            argument=root,
            runner_metadata=metadata_root,
        )
    return root


def _load_globs(inputs: Inputs, root: str, record: dict[str, Any]) -> tuple[str, ...]:
    """Read the allowlist, and assert it is still the twelve authoritative globs.

    This is the check AAP 0.6.4 and three adapters name as this module's own
    (``paths.allowlist_matches_authoritative_globs``).  The file is the authority and is
    never rewritten here; the globs it holds decide the ``in_scope`` field of every row and
    nothing else.  An allowlist that has drifted would mis-scope the whole dataset in a way
    that reads exactly like a clean result, so it stops the run.
    """
    try:
        globs = paths.load_allowlist(inputs.allowlist)
    except paths.PathPolicyError as error:
        raise _fault(
            HALT_ALLOWLIST_UNREADABLE,
            f"the allowlist at {inputs.allowlist} yields no usable scope: {error}",
            allowlist=str(inputs.allowlist),
            error=str(error),
        ) from error
    except OSError as error:
        raise _fault(
            HALT_ALLOWLIST_UNREADABLE,
            f"the allowlist at {inputs.allowlist} cannot be read: "
            f"{type(error).__name__}: {error}",
            allowlist=str(inputs.allowlist),
            error=f"{type(error).__name__}: {error}",
        ) from error

    authoritative = paths.allowlist_matches_authoritative_globs(globs)
    allowlist_record: dict[str, Any] = {
        "file": _file_record(inputs.allowlist),
        "globs": list(globs),
        "glob_count": len(globs),
        "matches_authoritative_globs": authoritative,
        "authoritative_globs": list(paths.ALLOWLIST_GLOBS),
        "consumers": (
            "This module is the normalizer's only consumer of the allowlist and passes it "
            "to every adapter, which evaluates in_scope through normalize.paths. The "
            "provisioned harness/lib/scope.sh reads the same file to derive each "
            "file-based runner's target set; that is a fact about those runners' reach, "
            "recorded rather than corrected, and no licence to change the file."
        ),
        "in_scope_policy": (
            "in_scope is decided by the allowlist alone. A row from a directory a runner "
            "reached but the allowlist does not cover takes in_scope: false and is kept "
            "(AAP 0.6.4)."
        ),
    }
    record["allowlist"] = allowlist_record
    if not authoritative:
        raise _fault(
            HALT_ALLOWLIST_NOT_AUTHORITATIVE,
            f"the allowlist at {inputs.allowlist} is not the twelve authoritative globs, "
            "byte-exact and in order. Every row's in_scope field would then be decided by "
            "a scope policy the request did not specify, and a silently mis-scoped dataset "
            "reads exactly like a clean one. Both the observed and the authoritative globs "
            "are in 'details'.",
            allowlist=str(inputs.allowlist),
            observed=list(globs),
            authoritative=list(paths.ALLOWLIST_GLOBS),
        )

    if os.path.isdir(root):
        # Evidence only: the expansion the file-based runners' reach was derived from.
        try:
            directories = paths.expand_scope_directories(root, globs)
        except OSError as error:  # pragma: no cover -- glob does not normally raise
            allowlist_record["expansion"] = {
                "error": f"{type(error).__name__}: {error}"
            }
        else:
            allowlist_record["expansion"] = {
                "directories": list(directories),
                "directory_count": len(directories),
                "expected_directory_count": paths.PINNED_EXPANSION_DIRECTORIES,
                "matches_expected": (
                    len(directories) == paths.PINNED_EXPANSION_DIRECTORIES
                ),
                "note": (
                    "Expanding a glob is arithmetic on the allowlist, never an extension "
                    "of it (AAP 0.8.2)."
                ),
            }
    else:
        allowlist_record["expansion"] = {
            "directories": None,
            "note": (
                "Not computed: the scan root is not a directory on this host, so the "
                "expansion could not be measured. Recorded rather than assumed."
            ),
        }
    return globs


def _enumerate_raw_directory(
    inputs: Inputs,
    record: dict[str, Any],
) -> dict[str, Path]:
    """Enumerate the raw tree, bounded to the nine fixed artifact filenames.

    The boundary AAP 0.8.1 fixes, enforced rather than assumed: only those nine names, only
    as direct children, and each one's real path asserted to still sit inside this
    directory.  The Opengrep taint A/B arms under ``harness/artifacts/logs/`` are valid
    SARIF that would route perfectly, and the Joern probe results under
    ``queries/joern/results/`` are equally readable -- both are second appearances that
    contribute no dataset row, so a symlink that reached one would corrupt both that tool's
    count and the dataset total.

    AN UNEXPECTED DIRECT CHILD HALTS THE RUN
    ----------------------------------------
    AAP 0.8.1 gives this tree exactly one admissible content -- *"one artifact per tool
    that writes one and nothing else ever"* -- and AAP 0.6.1 enumerates that content file
    by file.  A direct child outside it is therefore an input this module will not consume,
    and continuing past it publishes a dataset that reconciles perfectly *while an
    unadapted input sits beside the ones it was built from*: every per-artifact identity
    holds, the dataset sum holds, and nothing in the row-only ``findings.json`` can show
    that a document was skipped.  A complete-looking dataset over an incompletely consumed
    input tree is the one failure a reconciliation cannot catch, so the condition is a halt
    (:data:`HALT_RAW_DIRECTORY_UNEXPECTED`) rather than a line on stderr.

    Two conditions reach it and both are named individually in the evidence, because they
    are corrected differently: a name that is not one of the nine runner artifacts is
    something that does not belong in the tree, while one of those nine names carried by
    something that is not a regular file -- a directory called ``trivy.json``, say -- is
    that tool's artifact missing and its name occupied.

    ``record["raw_directory"]`` is written **before** the halt is raised, so the run record
    published on the halting path carries the same enumeration a successful run would.

    THE EVIDENCE IS FILESYSTEM-LEVEL ONLY, DELIBERATELY
    --------------------------------------------------
    Each unexpected entry is described by its name, whether it is a directory, whether it
    is a symbolic link, its byte size where it is a regular file, whether its name is one
    of the nine, and which of the two conditions it met.  Nothing here opens it, reads it,
    parses it or inspects its structure: this is a narrower evidence scope than "quote the
    document's safe top-level structure" on purpose, because reading an unexpected file on
    the halting path would take an untrusted document through none of the bounds every
    other read in this module passes (the byte cap, the pre-parse structural gate, the
    strict decode), and AAP 0.8.1 forbids fingerprinting a document in this tree to decide
    who wrote it -- ``shape.py`` keys the native adapter by the runner that wrote it, by
    filename, and never by content.
    """
    raw_dir = inputs.raw_dir
    if not raw_dir.is_dir():
        raise _fault(
            HALT_RAW_DIRECTORY_MISSING,
            f"the raw artifact directory {raw_dir} does not exist or is not a directory. "
            "It is the runner-only tree this module reads and the only tree it reads; this "
            "module neither creates nor clears it.",
            raw_dir=str(raw_dir),
        )

    try:
        entries = sorted(os.listdir(raw_dir))
    except OSError as error:
        raise _fault(
            HALT_RAW_DIRECTORY_MISSING,
            f"the raw artifact directory {raw_dir} cannot be listed: "
            f"{type(error).__name__}: {error}",
            raw_dir=str(raw_dir),
            error=f"{type(error).__name__}: {error}",
        ) from error

    present: dict[str, Path] = {}
    unexpected: list[dict[str, Any]] = []
    for name in entries:
        candidate = raw_dir / name
        tool = shape.TOOL_BY_ARTIFACT_FILENAME.get(name)
        if tool is not None and candidate.is_file():
            present[tool] = candidate
            continue
        # Filesystem-level evidence only: nothing here opens, reads or parses the entry.
        # candidate.is_dir() and is_file() follow a link, which is what a reader needs to
        # know about the thing occupying the name; is_symlink() reports the spelling
        # separately, so a link to a directory is not silently indistinguishable from a
        # directory.
        unexpected.append(
            {
                "name": name,
                "path": str(candidate),
                "is_directory": candidate.is_dir(),
                "is_symlink": candidate.is_symlink(),
                "is_expected_artifact_name": tool is not None,
                "bytes": (
                    candidate.stat().st_size if candidate.is_file() else None
                ),
                "condition": (
                    "an expected artifact name that is not a regular file"
                    if tool is not None
                    else "a name that is not one of the nine runner artifacts"
                ),
            }
        )

    raw_real = os.path.realpath(raw_dir)
    for tool, artifact_path in present.items():
        real = os.path.realpath(artifact_path)
        if os.path.dirname(real) != raw_real:
            raise _halt(
                HALT_RAW_DIRECTORY_BOUNDARY,
                f"{tool}: the artifact at {artifact_path} resolves to {real}, which is "
                f"outside the raw tree {raw_real}. harness/artifacts/raw/ is runner-only; "
                "a file reached from elsewhere -- a taint A/B arm or a probe result, both "
                "of which are readable second appearances that contribute no dataset row "
                "-- would corrupt both that tool's count and the dataset total.",
                tool=tool,
                artifact_path=str(artifact_path),
                resolved_path=real,
                raw_dir=raw_real,
            )

    record["raw_directory"] = {
        "path": str(raw_dir),
        "resolved_path": raw_real,
        "entries": entries,
        "entry_count": len(entries),
        "expected_artifact_filenames": list(shape.ARTIFACT_FILENAMES),
        "artifacts_present": [tool for tool in ARTIFACT_ORDER if tool in present],
        "artifacts_absent": [tool for tool in ARTIFACT_ORDER if tool not in present],
        "unexpected_entries": unexpected,
        "unexpected_entry_count": len(unexpected),
        "boundary": (
            "Only the nine fixed artifact filenames are read, only as direct children of "
            "this directory, and each one's real path is asserted to remain inside it. "
            "The Opengrep taint A/B arms (harness/artifacts/logs/taint-ab-*.sarif) and the "
            "Joern probe results (queries/joern/results/) are deliberate second "
            "appearances that contribute no dataset row (AAP 0.1.3). An unexpected direct "
            "child halts the run rather than being reported past: this tree admits one "
            "artifact per tool that writes one and nothing else ever (AAP 0.8.1, AAP "
            "0.6.1), so an unconsumed child would leave the dataset reconciling while an "
            "input nobody adapted sat beside the ones it was built from."
        ),
    }
    if unexpected:
        # The record above is already written, so the run record published on this path
        # carries the same enumeration a successful run would -- and the halt's own
        # details name every offending entry, so a reader diagnosing from the record
        # alone never has to list the directory again. No document is opened: the
        # evidence is the filesystem's, not the file's (see this function's docstring).
        described = "; ".join(
            f"{entry['name']} ({entry['condition']})" for entry in unexpected
        )
        raise _halt(
            HALT_RAW_DIRECTORY_UNEXPECTED,
            f"{len(unexpected)} unexpected direct "
            f"{'child' if len(unexpected) == 1 else 'children'} of the raw artifact tree "
            f"{raw_dir}: {described}. harness/artifacts/raw/ is runner-only and admits "
            "exactly one artifact per tool that writes one and nothing else ever (AAP "
            "0.8.1, AAP 0.6.1), so this is a halt rather than a condition to continue "
            "past: an unconsumed child means the dataset would reconcile -- every "
            "per-artifact identity and the dataset sum alike -- while an input nobody "
            "adapted sat beside the ones the rows were built from, and a row-only "
            "findings.json cannot show that a document was skipped. Move the file out of "
            "this tree, or give it the runner artifact name whose adapter should consume "
            "it. Nothing here was opened, read or parsed: no document in this tree is "
            "ever fingerprinted to identify a writer.",
            raw_dir=str(raw_dir),
            unexpected_entries=unexpected,
            unexpected_entry_count=len(unexpected),
            expected_artifact_filenames=list(shape.ARTIFACT_FILENAMES),
            evidence_scope=(
                "filesystem-level only -- name, directory, symlink, byte size, whether "
                "the name is one of the nine, and which condition it met. No unexpected "
                "document is opened, decoded, parsed or structurally quoted, because "
                "that read would bypass every ingestion bound this module applies to the "
                "inputs it does consume."
            ),
        )
    return present


def _build_source_index(
    root: str,
    present: Mapping[str, Path],
    record: dict[str, Any],
) -> paths.SourceIndex | None:
    """Build the bytecode-to-source index once, and only where Joern wrote an artifact.

    Injected into the Joern adapter so the tree is walked once per run rather than once per
    record.  The index spans ``src/main`` **and** ``src/test``, because every ``-tests``
    artifact the build produced is in the graph input, so a Joern finding can legitimately
    name bytecode compiled from a test tree -- retained with ``in_scope: false`` rather than
    dropped.

    An index over zero files would reject every Joern record for a reason that has nothing
    to do with the artifact, so that is a configuration fault rather than a rejection count.

    A *partial* index is the same fault, one degree harder to see, which is why
    ``paths.build_source_index`` now raises on a traversal or a source-read failure rather
    than skipping the entry.  A directory ``os.walk`` could not list, or a file whose
    declarations could not be read, removes resolutions the index is supposed to make and
    turns each affected record into an ``unresolvable_path`` rejection that is
    indistinguishable from the shaded-third-party outcome this resolver produces
    legitimately for five findings in six.  The :class:`OSError` is caught here and named
    with the failing path under :data:`HALT_SOURCE_INDEX_INCOMPLETE`, kept separate from
    :data:`HALT_SOURCE_INDEX_EMPTY` because the two need different corrections: an empty
    index means the scan root is not the pinned tree, while an incomplete one means part
    of that tree could not be read.
    """
    if "joern" not in present:
        record["source_index"] = {
            "built": False,
            "reason": "joern wrote no artifact, so no class-to-source index was needed.",
        }
        return None
    try:
        index = paths.build_source_index(root)
    except OSError as error:
        failing_path = getattr(error, "filename", None)
        raise _fault(
            HALT_SOURCE_INDEX_INCOMPLETE,
            f"the source index over {root} could not be built: "
            f"{type(error).__name__}: {error}. Part of the tree could not be read, so "
            "the index would claim a completeness it does not have and every class it "
            "lost would be reported as unresolvable.",
            root=root,
            error=f"{type(error).__name__}: {error}",
            failing_path=failing_path,
            errno=getattr(error, "errno", None),
        ) from error
    statistics = index.statistics()
    record["source_index"] = {"built": True, "root": root, **statistics}
    if not statistics.get("files_indexed"):
        raise _fault(
            HALT_SOURCE_INDEX_EMPTY,
            f"the source index over {root} indexed no files, so every Joern record would "
            "be rejected as unresolvable for a reason that has nothing to do with the "
            "artifact. The scan root must be the pinned tree the graph was built over.",
            root=root,
            statistics=statistics,
        )
    return index


def _severity_record(tally: severity.LiteralTally) -> dict[str, Any]:
    """Drain the literal tally into the record, with all nine identifiers present.

    ``findings.json`` and ``findings.csv`` are row-only, so a tool that produced no row is
    invisible in them; ``severity-map.md`` and ``tool-status.md`` are the authoritative
    inventory of all nine (AAP 0.5.4) and are rendered from this.  The tally is seeded with
    every canonical identifier, so a zero-row tool reaches the record with an empty literal
    list and a row count of zero rather than with no entry at all, and every native literal
    the dataset does carry reaches ``severity-map.md`` with its row count and with the entry
    that governed its band where a score rather than a label decided it -- so no literal in
    the dataset is unaccounted for and no tool drops out of the inventory by reporting
    nothing.

    The mapping policy itself lives in ``normalize/severity.py``; only its statement names
    are carried here, so no second copy of the policy text can drift from the first.

    **Which entry governed each band reaches the record with the literal that carries it.**
    AAP 0.5.4 requires that *"either way the entry used is recorded -- the label, or the
    score with its source and version"*, and a per-record selection that is dropped when
    rows are aggregated is a selection ``severity-map.md`` cannot state.  So the tally keys
    on the selected entry decomposed into scalars, and each serialised literal carries
    ``selected_label``, ``selected_score``, ``selected_source`` and ``selected_version``
    beside its band and basis.  Two things make that flow without a mapping layer here:
    ``severity.LiteralCount`` is a dataclass, so ``dataclasses.asdict`` carries every field
    it gains, and ``literal_key`` publishes the field names that constitute a literal's
    identity so a consumer reads them rather than inferring them.  ``selected_entry_policy``
    quotes the authored contract, which lives in ``severity.py`` for the same
    no-second-copy reason as the four mapping statements.
    """
    by_tool = tally.by_tool()
    unmapped = tally.unmapped_by_tool()
    return {
        "bands": list(severity.SEVERITY_NORM),
        "bases": list(severity.BASIS_VALUES),
        "policy_statements": [name for name, _ in severity.POLICY_STATEMENTS],
        "policy_source": "harness/lib/normalize/severity.py",
        "literal_key": list(severity.LITERAL_KEY_FIELDS),
        "selected_entry_policy": severity.POLICY_SELECTED_ENTRY_TALLY,
        "total_rows": tally.total_rows(),
        "tools_reported": list(by_tool),
        "tools": {
            tool: {
                "rows": tally.row_count(tool),
                "bands": tally.band_counts(tool),
                "literals": [dataclasses.asdict(entry) for entry in entries],
                "unmapped_literals": [
                    dataclasses.asdict(entry) for entry in unmapped.get(tool, ())
                ],
            }
            for tool, entries in by_tool.items()
        },
    }


def _by_tool(
    outcomes: Iterable[ArtifactOutcome],
    key: str,
) -> dict[str, Any]:
    """Return one summary counter per tool, keyed by identifier."""
    return {outcome.tool: outcome.counter_summary.get(key) for outcome in outcomes}


#: How many rows whose path names nothing on disk are quoted as examples in the record.
#: Bounded because the count is the measurement and the examples are illustration: an
#: unbounded list would let one misconfigured tool decide the size of the run record.
PATHS_NOT_ON_DISK_EXAMPLE_LIMIT: int = 25

#: Why a row's path was found not to name a file in the tree.  A closed vocabulary, so the
#: figure ``run-record.md`` cites can be read as a breakdown rather than as one opaque
#: number.
NOT_ON_DISK_ARCHIVE_MEMBER: str = "archive-member-not-a-tree-file"
NOT_ON_DISK_OUTSIDE_ROOT: str = "outside-root-not-stat-ed"
NOT_ON_DISK_ABSENT_FROM_TREE: str = "absent-from-the-pinned-tree"
NOT_ON_DISK_NOT_A_REGULAR_FILE: str = "present-but-not-a-regular-file"
NOT_ON_DISK_REASONS: tuple[str, ...] = (
    NOT_ON_DISK_ARCHIVE_MEMBER,
    NOT_ON_DISK_OUTSIDE_ROOT,
    NOT_ON_DISK_ABSENT_FROM_TREE,
    NOT_ON_DISK_NOT_A_REGULAR_FILE,
)


def _not_on_disk_reason(row_path: str, root_path: Path) -> str | None:
    """Return why ``row_path`` names no file in the tree, or ``None`` where it does.

    Four outcomes, each named in :data:`NOT_ON_DISK_REASONS`, and two of them are decided
    **without touching the filesystem** because touching it would be wrong rather than
    merely slow:

    * an **archive member** (``<container>!<member>``) names a member inside a container,
      which is not a file in the tree however present the container is.  Stat-ing the
      joined string would ask about a path that does not exist by construction and would
      report the right answer for the wrong reason;
    * a path that **escapes the root** carries ``..`` segments the SARIF errata require
      preserved, and resolving them would reach outside the tree this dataset is expressed
      against.  This module does not stat outside its own root, so the coordinate is
      counted as naming no file in the tree and the record says the check was refused
      rather than failed.

    The remaining two are measured: a path inside the root is joined to it lexically --
    never through ``Path.resolve()``, which would collapse ``..`` and follow symlinks --
    and classified as absent, as present but not a regular file (a directory, a socket, a
    dangling symlink), or as a file.

    An ``OSError`` from the stat is reported as absent rather than raised: this is a
    reported metric over paths a scanner supplied, and one unreadable path must not stop a
    run whose dataset is already written and reconciled.  The reason vocabulary keeps it
    distinguishable from a path that was simply not there.
    """
    if paths.split_archive_reference(row_path) is not None:
        return NOT_ON_DISK_ARCHIVE_MEMBER
    if paths.analyse_containment(row_path).escapes_root:
        return NOT_ON_DISK_OUTSIDE_ROOT
    candidate = root_path / row_path
    try:
        if candidate.is_file():
            return None
        if candidate.exists():
            return NOT_ON_DISK_NOT_A_REGULAR_FILE
    except OSError:
        return NOT_ON_DISK_ABSENT_FROM_TREE
    return NOT_ON_DISK_ABSENT_FROM_TREE


def _paths_not_on_disk(
    rows: Sequence[Mapping[str, Any]],
    root: str,
) -> dict[str, Any]:
    """Measure the rows whose ``path`` does not name a file on disk, once, against ``root``.

    AAP 0.1.1 and 0.6.1 require this figure published: *"count and report the rows whose
    path names something that is not a file on disk"*, and ``run-record.md`` carries *"the
    non-filesystem path count and proportion"*.  It is a different question from the
    path-kind tally beside it, and both are needed.  The tally classifies a path by its
    **form** -- an archive member, a coordinate outside the root -- which a resolver can
    decide with no filesystem at all.  This asks whether the thing a row names is
    **actually there** in the pinned tree, which only the tree can answer, and a
    ``tree_file`` naming a file the pin does not carry is invisible to the tally.

    Taken **once**, here, against the same root every path in the dataset was expressed
    against, because a second measurement against a second root is how two documents come
    to quote different numbers for one figure (AAP 0.6.4).

    Bounded by construction rather than by row count: the distinct paths are collected
    first and each is classified once, so a dataset in which many rows share a path costs
    one stat per path rather than one per row. The dataset this run emits carries 9,430 rows
    over 712 distinct paths, so the memoised classification turns 9,430 row checks into 712
    filesystem stats, and the verdict a row sees is the same one every other row naming that
    path saw.

    Returns:
        A mapping carrying ``count``, ``rows_examined`` (the denominator, always the real
        row count so a zero is readable as *"none of 9,430"* rather than as an absence),
        ``proportion``, ``by_reason``, ``by_tool``, a bounded ``examples`` list in row
        order, ``distinct_paths_examined``, and the ``method`` and ``root`` the figure was
        taken with.
    """
    root_path = Path(root)
    verdicts: dict[str, str | None] = {}
    by_reason: dict[str, int] = {reason: 0 for reason in NOT_ON_DISK_REASONS}
    by_tool: dict[str, int] = {}
    examples: list[dict[str, Any]] = []
    count = 0

    for row in rows:
        row_path = row.get("path")
        tool = row.get("tool")
        if not isinstance(row_path, str):
            # Unreachable through emit.validate_rows, which requires a non-empty str, but
            # this is a measurement rather than a validator: it reports what it was given.
            reason: str | None = NOT_ON_DISK_ABSENT_FROM_TREE
        else:
            if row_path not in verdicts:
                verdicts[row_path] = _not_on_disk_reason(row_path, root_path)
            reason = verdicts[row_path]
        if reason is None:
            continue
        count += 1
        by_reason[reason] = by_reason.get(reason, 0) + 1
        key = tool if isinstance(tool, str) else "<unattributed>"
        by_tool[key] = by_tool.get(key, 0) + 1
        if len(examples) < PATHS_NOT_ON_DISK_EXAMPLE_LIMIT:
            # Row order, not sorted: the dataset's order is already deterministic, and
            # taking the first N of it keeps the examples tied to a position a reader can
            # find in findings.json.
            examples.append({"tool": key, "path": row_path, "reason": reason})

    examined = len(rows)
    return {
        "count": count,
        "rows_examined": examined,
        "proportion": (count / examined) if examined else 0.0,
        "by_reason": {reason: by_reason[reason] for reason in NOT_ON_DISK_REASONS},
        "by_tool": {tool: by_tool[tool] for tool in sorted(by_tool)},
        "examples": examples,
        "example_limit": PATHS_NOT_ON_DISK_EXAMPLE_LIMIT,
        "examples_truncated": count > len(examples),
        "distinct_paths_examined": len(verdicts),
        "root": root,
        "method": (
            "Each row's path is joined to the scan root lexically -- never through "
            "Path.resolve(), which would collapse the '..' segments the SARIF 2.1.0 "
            "errata require preserved -- and tested with Path.is_file(). An archive "
            "member and a coordinate that escapes the root are classified without a stat: "
            "the first names a member inside a container rather than a file in the tree, "
            "and stat-ing the second would reach outside the root this dataset is "
            "expressed against. Distinct paths are classified once and the verdict reused, "
            "so the cost is one stat per distinct path rather than one per row."
        ),
        "note": (
            "A row counted here is kept, never dropped (AAP 0.9.3): an external "
            "coordinate, an archive member and a virtual reference are legitimate "
            "coordinates. This figure is a fact about the dataset, not a judgement on any "
            "tool, and it is distinct from the path-kind tally beside it -- that "
            "classifies a path by its form, this asks whether the thing it names is there."
        ),
    }


def _totals_record(
    outcomes: Sequence[ArtifactOutcome],
    rows: Sequence[Mapping[str, Any]],
    path_kinds: paths.PathKindTally,
    root: str,
) -> dict[str, Any]:
    """Aggregate the dataset-level counts every downstream document needs.

    The per-tool multi-location, multi-identifier and non-filesystem counts AAP 0.5.4
    requires reported, plus the non-filesystem proportion AAP 0.6.1 puts in
    ``run-record.md``.  Each figure is one measurement, cited here once, so a document that
    quotes it is quoting this file rather than recomputing it.

    ``root`` is taken because two of those figures are about the tree rather than about the
    rows: :func:`_paths_not_on_disk` answers *"do these paths name files that are there"*,
    which the form-based path-kind tally cannot, and it is measured here so that it is
    measured exactly once, against the same root every path was expressed against.
    """
    rejections_by_class: dict[str, int] = {}
    for outcome in outcomes:
        for reject_class, count in outcome.rejections_by_class.items():
            rejections_by_class[reject_class] = (
                rejections_by_class.get(reject_class, 0) + count
            )
    return {
        "rows": len(rows),
        "rows_by_tool": {outcome.tool: outcome.emitted_rows for outcome in outcomes},
        "raw_records_by_tool": {outcome.tool: outcome.raw_records for outcome in outcomes},
        "rejected_records": sum(outcome.rejected_records for outcome in outcomes),
        "rejections_by_class": {
            reject_class: rejections_by_class[reject_class]
            for reject_class in paths.REJECT_CLASSES
            if reject_class in rejections_by_class
        },
        "rejections_by_tool": {
            outcome.tool: dict(outcome.rejections_by_class) for outcome in outcomes
        },
        "parse_status_by_tool": {
            outcome.tool: outcome.parse_status for outcome in outcomes
        },
        "artifacts_present": sum(1 for outcome in outcomes if outcome.present),
        "artifacts_absent": sum(1 for outcome in outcomes if not outcome.present),
        "multi_location_records_by_tool": _by_tool(outcomes, "multi_location_records"),
        "multi_valued_cwe_records_by_tool": _by_tool(
            outcomes, "multi_valued_cwe_records"
        ),
        "multi_valued_cve_records_by_tool": _by_tool(
            outcomes, "multi_valued_cve_records"
        ),
        "non_filesystem_paths_by_tool": _by_tool(outcomes, "non_filesystem_paths"),
        "rows_in_scope_by_tool": _by_tool(outcomes, "rows_in_scope"),
        "rows_out_of_scope_by_tool": _by_tool(outcomes, "rows_out_of_scope"),
        "path_kinds": path_kinds.as_dict(),
        "non_filesystem_paths": path_kinds.non_filesystem,
        "non_filesystem_proportion": path_kinds.non_filesystem_proportion,
        "paths_not_on_disk": _paths_not_on_disk(rows, root),
        "notes": [
            "A record carrying more than one location contributes one row, from its first "
            "location, and still counts once (AAP 0.5.4).",
            "A record carrying several CWE or CVE identifiers emits one of each, chosen by "
            "ascending numeric identifier.",
            "Nothing is deduplicated, ranked or compared across tools: two tools reporting "
            "the same location produce two rows and no comment (AAP 0.3.2).",
            "'path_kinds' classifies each path by its form and needs no filesystem; "
            "'paths_not_on_disk' asks whether the thing each path names is present in the "
            "pinned tree. Both are required (AAP 0.1.1, AAP 0.6.1) and neither substitutes "
            "for the other: a tree_file naming a file the pin does not carry is invisible "
            "to the first and counted by the second.",
        ],
    }


def _stage_failures(
    stage_a: Sequence[reconcile.ArtifactReconciliation],
    stage_b: reconcile.DatasetReconciliation,
    stage_c: Sequence[reconcile.OutputCountComparison],
) -> list[str]:
    """Collect the failed assertions across the stages supplied, in stage order.

    The same wording ``reconcile.run_three_stage_validation`` uses, because the report
    assembled from these stages must read identically whichever route produced it.
    """
    failures: list[str] = []
    for artifact in stage_a:
        if artifact.passed is False:
            failures.append(f"stage A [{artifact.tool}]: {artifact.detail}")
    if not stage_b.passed:
        failures.append(f"stage B [dataset]: {stage_b.detail}")
    for comparison in stage_c:
        if not comparison.passed:
            failures.append(f"stage C [{comparison.name}]: {comparison.detail}")
    return failures


def _reconcile_before_write(
    counts: Sequence[reconcile.ArtifactCounts],
    record: dict[str, Any],
) -> tuple[list[reconcile.ArtifactReconciliation], reconcile.DatasetReconciliation]:
    """Establish stages A and B **before** either output file is written.

    ``paths.REJECT_CLASSES`` is passed in as the vocabulary: ``reconcile.py`` imports
    nothing from this package on purpose, so the vocabulary arrives as a parameter and its
    independence stays enforced by the import graph rather than promised in a comment.

    A dataset whose per-artifact or dataset identity already fails is never written to disk,
    and no count is ever adjusted to make an identity hold.
    """
    try:
        stage_a = reconcile.run_stage_a(counts, reject_classes=paths.REJECT_CLASSES)
        stage_b = reconcile.run_stage_b(stage_a)
    except reconcile.ReconciliationError as error:
        raise _halt(
            HALT_RECONCILIATION,
            f"reconciliation could not be established: {error}",
            error=str(error),
            stage="A/B",
        ) from error
    except ValueError as error:
        raise _halt(
            HALT_RECONCILIATION,
            f"the artifact counts handed to reconciliation are malformed: {error}",
            error=str(error),
            stage="A/B",
        ) from error

    failures = _stage_failures(stage_a, stage_b, ())
    record["reconciliation"] = {
        "reject_class_vocabulary": list(paths.REJECT_CLASSES),
        "vocabulary_source": "normalize.paths.REJECT_CLASSES, passed in as a parameter",
        "identity": "raw finding records = dataset rows for that tool + rejected records",
        "not_applicable_sentinel": reconcile.NOT_APPLICABLE_ABSENT,
        "stage_a": [artifact.as_dict() for artifact in stage_a],
        "stage_b": stage_b.as_dict(),
        "stage_c": None,
        "pre_write_gate": {
            "checked": ["stage_a", "stage_b"],
            "passed": not failures,
            "failures": failures,
            "note": (
                "Stages A and B are established before either output file is written, so a "
                "dataset whose identity already fails never reaches disk."
            ),
        },
        "passed": None,
        "failures": None,
    }
    if failures:
        raise _halt(
            HALT_RECONCILIATION,
            "the reconciliation identity failed before anything was written: "
            + "; ".join(failures),
            failures=failures,
            stage="A/B",
        )
    return stage_a, stage_b


def _write_outputs(
    rows: Sequence[Mapping[str, Any]],
    inputs: Inputs,
    record: dict[str, Any],
) -> emit.ComparisonResult:
    """Publish both files from the same rows as one generation, proving they agree first.

    ``emit.publish_findings`` validates the rows once, renders each file from those same
    rows -- neither derived from the other -- stages both in their target directories,
    reads both staged files back from disk, coerces the CSV cells to the types their
    fields carry, compares in order field by field, and only then moves both into place.
    Nothing counts lines: a ``message`` field carrying an embedded newline spans several
    physical lines, so a row count is established by parsing both written files and never by
    counting the lines in them -- this dataset's 9,430 parsed rows occupy 9,439 physical CSV
    lines.

    The publication is all-or-nothing on purpose (AAP 0.9.4 -- the two files are one
    dataset): a fault part way through leaves both previous deliverables exactly as they
    were, rather than this run's ``findings.json`` beside the previous run's
    ``findings.csv``.  Both members carry one content-derived publication identifier, and
    it is recorded here with each member's byte size and digest so a consumer can detect a
    mixed generation without trusting this sentence.
    """
    boundary = inputs.output_guards.get("repository_root")
    try:
        publication = emit.publish_findings(
            rows,
            inputs.findings_json,
            inputs.findings_csv,
            manifest_path=inputs.log_dir / PUBLICATION_MANIFEST_FILENAME,
        )
    except emit.ComparisonFailed as error:
        # The two staged files did not agree, so neither was published and both previous
        # deliverables are untouched.  The comparison travels on the exception, so the
        # record carries the same measurement it would have carried on a pass.
        record["output_comparison"] = error.comparison.as_dict()
        raise _halt(
            HALT_OUTPUT_COMPARISON,
            "findings.json and findings.csv do not agree under typed re-parse, so "
            "neither was published: "
            + (
                error.comparison.first_mismatch.detail
                if error.comparison.first_mismatch is not None
                else "no mismatch was located, which is itself a fault"
            ),
            comparison=error.comparison.as_dict(),
        ) from error
    except emit.EmitError as error:
        raise _halt(
            HALT_EMIT,
            f"the dataset could not be written or read back as this schema: {error}",
            error=str(error),
            findings_json=str(inputs.findings_json),
            findings_csv=str(inputs.findings_csv),
        ) from error
    except OSError as error:
        raise _fault(
            HALT_EMIT,
            f"an output file could not be written: {type(error).__name__}: {error}",
            error=f"{type(error).__name__}: {error}",
            findings_json=str(inputs.findings_json),
            findings_csv=str(inputs.findings_csv),
        ) from error

    comparison = publication.comparison
    if comparison is None:  # pragma: no cover -- publish_findings always establishes it
        raise _halt(
            HALT_OUTPUT_COMPARISON,
            "the dataset was published without the typed re-parse comparison being "
            "established, which is itself a fault",
            publication=publication.as_dict(),
        )

    record["output_comparison"] = comparison.as_dict()
    record["outputs"] = {
        "findings_json": _file_record(inputs.findings_json),
        "findings_csv": _file_record(inputs.findings_csv),
        # The publication: one identifier both members carry, each member's byte size and
        # sha256 as measured off the disk, and the write protocol they were published
        # under.  Recorded rather than asserted, because "both files came from one run" is
        # exactly the claim a reader of two git-ignored files cannot otherwise check.
        "publication": publication.as_dict(),
        # emit.validate_rows already refused any row that broke the schema, so a write
        # that got this far proves the schema held -- but it proves it by the absence of
        # an exception, and an absence is not a number. This is the same assertion as a
        # measurement: rows carrying exactly twelve fields, path and severity_norm never
        # absent, absence confined to the five optional fields, and no absolute path
        # emitted (AAP 0.8.2). It is computed by emit.py from its own rules.
        "row_validation": emit.validation_summary(rows),
        "row_order": (
            "Artifacts in normalize.shape.CANONICAL_TOOLS order; within an artifact, the "
            "order its adapter returned, which is document order. Both files carry that "
            "one sequence, so two runs over identical artifacts produce byte-identical "
            "files."
        ),
        "absence_convention": (
            "JSON null and an empty CSV field, permitted for severity_native, start_line, "
            "cwe, cve and package_coordinate only; path and severity_norm are never absent."
        ),
        # The CSV's bytes can differ from a tool's literal text by exactly one leading
        # character, so the rule and the number of cells it changed are disclosed here:
        # a reader comparing findings.csv against a scanner's own output otherwise has
        # no way to tell a neutralised cell from a tool that reported a leading
        # apostrophe.  The count is the writer's own, taken as it rendered.
        "csv_spreadsheet_neutralisation": publication.csv_neutralisation,
    }
    if not comparison.passed:  # pragma: no cover -- emit refuses to publish a failed pair
        # Unreachable while emit.publish_findings raises ComparisonFailed rather than
        # publishing a pair that disagrees, and kept because a halt condition that
        # depends on another module continuing to refuse is one this module states for
        # itself as well.
        raise _halt(
            HALT_OUTPUT_COMPARISON,
            "findings.json and findings.csv do not agree under typed re-parse: "
            + (
                comparison.first_mismatch.detail
                if comparison.first_mismatch is not None
                else "no mismatch was located, which is itself a fault"
            ),
            comparison=comparison.as_dict(),
        )
    return comparison


def _reconcile_after_write(
    stage_a: Sequence[reconcile.ArtifactReconciliation],
    stage_b: reconcile.DatasetReconciliation,
    inputs: Inputs,
    record: dict[str, Any],
) -> reconcile.ReconciliationReport:
    """Establish stage C, and assemble the report from the stages already measured.

    Stage C compares the parsed ``findings.json`` row count and the parsed ``findings.csv``
    row count against the stage B identity **separately**, and against each other.  The
    report reuses the stage A and stage B objects measured before the write rather than
    recomputing them: a count that appears in two places must be one measurement cited
    twice, never two measurements.
    """
    try:
        stage_c = reconcile.run_stage_c(
            stage_b, inputs.findings_json, inputs.findings_csv
        )
    except reconcile.ReconciliationError as error:
        raise _halt(
            HALT_RECONCILIATION,
            f"stage C could not be established: {error}",
            error=str(error),
            stage="C",
        ) from error
    except (ValueError, OSError, json.JSONDecodeError) as error:
        raise _halt(
            HALT_RECONCILIATION,
            f"stage C could not parse an output file to count its rows: "
            f"{type(error).__name__}: {error}",
            error=f"{type(error).__name__}: {error}",
            stage="C",
        ) from error

    failures = _stage_failures(stage_a, stage_b, stage_c)
    report = reconcile.ReconciliationReport(
        stage_a=tuple(stage_a),
        stage_b=stage_b,
        stage_c=tuple(stage_c),
        passed=not failures,
        failures=tuple(failures),
    )
    reconciliation = record["reconciliation"]
    reconciliation["stage_c"] = [comparison.as_dict() for comparison in stage_c]
    reconciliation["passed"] = report.passed
    reconciliation["failures"] = list(report.failures)
    if not report.passed:
        raise _halt(
            HALT_RECONCILIATION,
            "the reconciliation failed: " + "; ".join(report.failures),
            failures=list(report.failures),
            stage="C",
        )
    return report


# --------------------------------------------------------------------------- #
# The run                                                                     #
# --------------------------------------------------------------------------- #


def _execute(inputs: Inputs, record: dict[str, Any]) -> None:
    """Run the whole composition, mutating ``record`` as each stage establishes a fact.

    Every stage writes what it established into ``record`` before the next one runs, so a
    halt anywhere leaves the record describing exactly how far the run got.
    """
    _verify_vocabularies(record)
    metadata = _load_metadata(inputs, record)
    root = _resolve_scan_root(inputs, metadata, record)
    globs = _load_globs(inputs, root, record)
    present = _enumerate_raw_directory(inputs, record)
    source_index = _build_source_index(root, present, record)

    tally = severity.LiteralTally.with_all_tools()
    outcomes: list[ArtifactOutcome] = record["artifacts"]
    rows: list[dict[str, Any]] = []
    counts: list[reconcile.ArtifactCounts] = []
    path_kinds = paths.PathKindTally()
    tool_entries = metadata.get(paths.METADATA_TOOLS_KEY)
    tool_entries = tool_entries if isinstance(tool_entries, Mapping) else {}

    for tool in ARTIFACT_ORDER:
        filename = shape.artifact_filename_for(tool)
        artifact_path = inputs.raw_dir / filename
        entry = tool_entries.get(tool)
        expected = (
            entry.get("artifact_expected") if isinstance(entry, Mapping) else None
        )
        outcome = ArtifactOutcome(
            tool=tool,
            scanner_class=_scanner_class_label(tool),
            artifact_filename=filename,
            present=tool in present,
            # Overwritten by whichever branch runs; a halt before either leaves 'failed',
            # which is the honest status for an artifact nobody could classify.
            parse_status=PARSE_STATUS_FAILED,
            artifact=_file_record(artifact_path),
            artifact_expected=expected if isinstance(expected, bool) else None,
        )
        outcomes.append(outcome)

        if tool in present:
            artifact_rows, artifact_counts = _process_present_artifact(
                tool,
                present[tool],
                metadata=metadata,
                root=root,
                globs=globs,
                tally=tally,
                source_index=source_index,
                log_dir=inputs.log_dir,
                outcome=outcome,
            )
            rows.extend(artifact_rows)
            _merge_path_kinds(path_kinds, outcome.counters)
        else:
            artifact_counts = _process_absent_artifact(
                tool,
                root=root,
                log_dir=inputs.log_dir,
                outcome=outcome,
            )
        counts.append(artifact_counts)

    record["severity_literals"] = _severity_record(tally)
    record["totals"] = _totals_record(outcomes, rows, path_kinds, root)

    stage_a, stage_b = _reconcile_before_write(counts, record)
    _write_outputs(rows, inputs, record)
    _reconcile_after_write(stage_a, stage_b, inputs, record)


def _new_record(argv: Sequence[str], started_at: str) -> dict[str, Any]:
    """Build the run record's skeleton, so a halt at any point still writes a full shape.

    Every key AAP 0.6.1 requires is present from the outset -- *"its exact command,
    interpreter path and version, per-artifact routing decisions, per-artifact parsed and
    rejected counts, every reconciliation assertion and its result, and the exit status"* --
    and a stage that never ran leaves its value ``null`` rather than absent, so a reader can
    tell "not reached" from "not recorded".
    """
    # The exact command, and exactly it: the interpreter, the program it was handed, and
    # the arguments. `sys.argv[0]` is what makes the recorded line reproducible -- drop it
    # and the record reads as though the bare interpreter had produced this dataset. It is
    # empty for `python -c` and absent when a test drives `main(argv)` in process, and in
    # both of those cases there is no program to name, so none is invented.
    program = sys.argv[0] if sys.argv and sys.argv[0] else None
    command = [sys.executable, *([program] if program else []), *argv]
    return {
        "schema_version": SCHEMA_VERSION,
        "document": RUN_RECORD_DOCUMENT,
        "produced_by": "harness/lib/normalize/cli.py",
        "purpose": (
            "The normalizer's own run record: its exact command, interpreter path and "
            "version, per-artifact routing decisions, per-artifact parsed and rejected "
            "counts, every reconciliation assertion and its result, and the exit status "
            "(AAP 0.6.1). oss-scan-results/tool-status.md, severity-map.md and "
            "run-record.md are rendered from this joined with runner-metadata.json; this "
            "file is never rendered from them."
        ),
        "publication": (
            "harness/artifacts/** is git-ignored (.gitignore:31 is artifacts/), so this "
            "record is published through the per-file size-and-sha256 manifest (AAP 0.1.3) "
            "rather than by git add. It is written self-describing for that reason: every "
            "file it names that exists on disk carries that file's byte size and its "
            "sha256, computed over the whole file. Where an entry carries a null "
            "measurement it also carries a null_reason saying why -- the file is absent, "
            "the file could not be read, or the entry names no file at all -- so the "
            "claim and the data cannot disagree. See file_measurement below."
        ),
        "file_measurement": {
            "fields": ["path", "present", "bytes", "sha256"],
            "digest": "sha256, lowercase hex, over the entire file",
            "digest_chunk_bytes": _DIGEST_CHUNK,
            "method": (
                "os.stat for the byte size and hashlib.sha256 over the file read in "
                "digest_chunk_bytes chunks, taken at the moment the entry was written"
            ),
            "null_convention": (
                "bytes and sha256 are null only where the file is absent, where it "
                "exists but could not be read, or where the entry names no file; each "
                "such entry carries a null_reason stating which"
            ),
            "embedded_text": (
                "a runner stream's own words are embedded only where the artifact is "
                "absent and the classification therefore depends on them (AAP 0.5.4), "
                "bounded by text_excerpt_limit and flagged text_truncated when cut. "
                "Where the words are not embedded the entry carries a text_null_reason: "
                "the stream is still measured by size and sha256 and retained verbatim "
                "on disk, so the bound never loses evidence silently."
            ),
            "text_excerpt_limit": TOOL_WORDS_EXCERPT_LIMIT,
        },
        # The escaping contract, named rather than implied. The fields below carry text
        # this pipeline did not author and is required to keep byte for byte, so the
        # hazard is disclosed and measured here instead of escaped away (CWE-116,
        # CWE-117). Every claim in it is checkable from the record itself: the field list
        # against the entries, the measurement against the inventories beside them.
        UNTRUSTED_TEXT_CONTRACT_KEY: {
            "what": (
                "the fields listed below carry text this pipeline did not author -- a "
                "runner's own stream words and its own .status lines -- byte for byte, "
                "unescaped and unrewritten"
            ),
            "fields": [
                "artifacts[].tool_words.streams.stderr.text",
                "artifacts[].tool_words.streams.stdout.text",
                "artifacts[].tool_words.stated_reason",
                "artifacts[].runner_status.fields.*",
                "artifacts[].runner_status.exit_code_literal",
                "artifacts[].runner_status.artifact_bytes_literal",
                "artifacts[].runner_status.scan_root",
                "artifacts[].runner_status.scan_root_source",
                "artifacts[].runner_status.unparsed_lines[].excerpt",
                "artifacts[].runner_status.duplicate_fields[].value",
                "artifacts[].network_fetch.statements.*",
            ],
            "why_verbatim": (
                "AAP 0.5.4 requires the parser error 'retained verbatim' and settles the "
                "absent-artifact verdict 'using only the tool's own stated words'; AAP "
                "0.6.1 and AAP 0.6.2 require an absent artifact's stderr 'verbatim' and a "
                "tool's reduced-reach condition 'in the tool's own words'. A rewritten "
                "byte is a rewritten verdict, so escaping these fields would satisfy a "
                "rendering concern by destroying the evidence contract three AAP clauses "
                "make mandatory. Retention here is required, not preferred."
            ),
            "container": (
                "JSON is a safe container for these bytes: RFC 8259 section 7 requires "
                "every character below U+0020 to be escaped in the encoded form, and a "
                "parser returns them as data rather than as structure. No value carried "
                "here can break out of this document or add a member to it."
            ),
            "consumer_obligation": (
                "a consumer rendering any field above into Markdown, HTML or a terminal "
                "MUST escape it first. CommonMark permits raw HTML, and a backtick run "
                "inside a value closes a fence of equal or shorter length, so unescaped "
                "text can inject markup or restructure the document (CWE-116); a bare CR, "
                "LF or ESC reaching a terminal or a line-oriented log can overwrite or "
                "forge a record (CWE-117). The minimum is: neutralise backslash, "
                "backtick, pipe, less-than, greater-than and ampersand, neutralise the "
                "characters that open a block construct when a value starts a line, "
                "describe every control character rather than reproducing it, and choose "
                "a fence at least one backtick longer than the longest backtick run in "
                "the payload. queries/joern/*.sc do exactly this for the reports they "
                "author, in mdSafe, plainSafe and mdFence."
            ),
            "measurement": (
                "each field above is published beside a control-character inventory "
                "measured over the exact text embedded: which code points occur, how many "
                "times each, the class and standard abbreviation of each, and what each "
                "one does to a consumer that renders it unescaped. The inventory never "
                "reproduces the character it reports -- character_reproduced is false in "
                "every one -- so a reader sees the hazard without inheriting it. An "
                "inventory with occurrences 0 is a measurement that found none, which is "
                "a different fact from measured false, and both are said."
            ),
            "measurement_fields": [
                "artifacts[].tool_words.streams.stderr.text_control_characters",
                "artifacts[].tool_words.streams.stdout.text_control_characters",
                "artifacts[].tool_words.stated_reason_control_characters",
                "artifacts[].runner_status.text_control_characters",
            ],
            "subject_set": _CONTROL_SUBJECT_SET,
            "method": _CONTROL_METHOD,
            "deliberately_not_listed": (
                "a halt's message and details, and every rejection, are NOT in the list "
                "above: they are composed prose rather than retained evidence, and each "
                "is already neutralised at its own persistence boundary -- "
                "NormalizeHalt.as_dict through paths.sanitise_diagnostic and "
                "paths.sanitise_persisted, and paths.Rejection.as_dict through "
                "sanitise_diagnostic -- with URI userinfo redacted and control characters "
                "escaped. The list was taken by tracing every string in this record back "
                "to its author, not by pattern."
            ),
            "residual_risk": (
                "the bytes above are untrusted and are retained, and nothing in this "
                "module makes them safe to paste into a rendered document. The risk is "
                "disclosed and measured rather than removed, because removing it is "
                "prohibited by the AAP clauses named above; the residue is that a hostile "
                "runner stream could carry markup, a fence-closing backtick run or a "
                "terminal escape into any consumer that renders it without escaping. It "
                "is latent rather than live: no committed stream or status file carries "
                "such a payload, and the inventories are what let a reader confirm that "
                "rather than take it on trust."
            ),
            "not_applicable_here": (
                "this module writes no Markdown and emits no fenced block of its own, so "
                "it has nothing to escape on its own output (AAP 0.6.4: 'The Markdown is "
                "an output of the pipeline, never an input to it'). The obligation is "
                "the renderer's, and it is stated here because this record is where the "
                "renderer's input comes from."
            ),
            "cwe": ["CWE-116", "CWE-117"],
        },
        "started_at_utc": started_at,
        "finished_at_utc": None,
        "command": {
            # The program is named separately as well as joined into the line, so a
            # reader never has to infer from `argv` -- which carries the arguments and
            # not the program -- what was actually executed.
            "program": program,
            "argv": list(argv),
            "command_line": shlex.join(command),
            "working_directory": os.getcwd(),
        },
        "interpreter": interpreter_record(),
        "inputs": None,
        "vocabularies": None,
        "runner_metadata": None,
        "scan_root": None,
        "allowlist": None,
        "raw_directory": None,
        "source_index": None,
        "artifacts": [],
        "severity_literals": None,
        "totals": None,
        "reconciliation": None,
        "output_comparison": None,
        "outputs": None,
        "halt": None,
        "exit_status": None,
    }


def _json_default(value: Any) -> Any:
    """Render the record's non-JSON values: dataclasses, paths, mappings and sets.

    A last resort rather than a licence: anything reaching the fallback is rendered as its
    ``repr`` so it appears in the record instead of aborting the write of everything else.
    """
    as_dict = getattr(value, "as_dict", None)
    if callable(as_dict):
        return as_dict()
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, (set, frozenset)):
        return sorted(str(item) for item in value)
    if isinstance(value, (tuple, list)):
        return list(value)
    return repr(value)


class RunRecordNotPersisted(Exception):
    """The run record could not be written and read back, so it is not evidence.

    Raised by :func:`_write_run_record` and caught only by :func:`main`, which turns it
    into the process's outcome.  It is deliberately not a :class:`NormalizeHalt`: a halt is
    something this module records, and this is the failure of the recording itself.
    """


def _write_run_record(
    path: Path, record: Mapping[str, Any], *, owner: Path | None = None
) -> dict[str, Any]:
    """Publish ``normalize-run.json`` and verify it, on every path out of this module.

    Not best effort.  ``normalize-run.json`` is required evidence -- AAP 0.6.1 names it,
    AAP 0.9.1 requires the normalizer's run to have a structured status record in the log
    tree, and AAP 0.1.3 publishes it by manifest because the tree is git-ignored.  A run
    whose record was lost has produced a dataset nobody can trace, so a failure here is the
    process's outcome rather than a line on stderr beside a success (CWE-703).

    THE RECORD IS PUBLISHED THROUGH THE SAME DESCRIPTOR-BOUND PROTOCOL AS THE DATASET.
    ``emit.publish_document`` is that protocol: the parent directory is walked one
    component at a time with ``O_NOFOLLOW`` and held open as a descriptor, the staged file
    is created in it with ``O_CREAT|O_EXCL|O_WRONLY|O_NOFOLLOW`` under an unguessable name,
    its mode is assigned with ``fchmod``, the bytes are fsynced, the file is measured
    through a descriptor bound to the inode they were written into, the rename is issued
    against the held directory descriptor, and the directory itself is fsynced.  This
    module previously wrote the record with a pathname-based sequence -- validate the
    string, ``mkdir(parents=True)``, ``os.open`` the computed path, ``os.replace`` -- which
    re-resolved the name at every step: a parent or a final component swapped between the
    check and the open redirects or replaces the publication, and neither is detectable
    afterwards (CWE-367, CWE-59).  One protocol, one place to audit.

    Five things happen, in this order, and all five must succeed:

    1.  Where an ``owner`` root is declared, containment is asserted first
        (:func:`emit.assert_safe_output_path`, CWE-73).  This is the *predicate* -- is
        this path inside the tree that owns it -- and not the write guard; the write
        guard is the descriptor-bound sequence below, which refuses a symlinked
        component whether or not an owner was declared.
    2.  The record is serialised to a string.  A record that cannot be serialised -- a
        circular reference is the realistic case -- therefore fails before any file
        exists, rather than leaving a truncated document behind.
    3.  The bytes are written to a **staged** file beside the target and fsynced.
    4.  The staged file is **read back from disk and parsed** as a JSON object, before
        anything is renamed.  Serialising without re-reading proves the encoder ran, not
        that the bytes are on the device and are a document -- and this record's own halt
        fields are the only account of a halt.  A failure here publishes nothing: the
        previously published record is untouched and the staged file is removed.
    5.  Only then is it renamed into place, atomically, and the **published** bytes are
        re-read through a descriptor bound to the inode the publication verified
        (:func:`emit.open_verified_member`) and parsed again.  Step 4 establishes that
        what is about to be published is a document; step 5 establishes that the document
        now at the published path is that same file rather than whatever the pathname
        resolves to afterwards.

    Args:
        path: Where the record is published.
        record: The record to serialise.
        owner: The declared owner root the target must sit inside -- the log tree.
            ``None`` where the caller has already bound the target (the publisher still
            refuses a symlinked target or component), so containment is enforced once
            rather than assumed twice.

    Returns:
        A JSON-serialisable description of what was published: the target, its byte size,
        its sha256, its measured permission bits, the publication identifier and the two
        verifications that passed.  It is returned rather than folded into the record
        because a document cannot contain its own digest -- the record's own measurement
        is published in the log tree's per-file manifest (AAP 0.1.3) and reported on the
        console here.

    Raises:
        RunRecordNotPersisted: Where the record could not be serialised, staged, read
            back, parsed, published or re-read.  The message names the condition and the
            path.
    """

    def parse_object(candidate: Path) -> None:
        """Read ``candidate`` and require it to parse as a JSON object.

        Used twice: on the staged bytes before the rename, and on the published bytes
        after it.  The published read goes through ``emit.open_verified_member`` and is
        bound to the verified inode; this one takes a path because the staged file is not
        published yet and there is nothing to bind to but the file the publisher just
        wrote and measured.
        """
        parsed = json.loads(candidate.read_text(encoding="utf-8"))
        if not isinstance(parsed, dict):
            raise ValueError(
                f"the record at {candidate} parsed as "
                f"{type(parsed).__name__} rather than an object"
            )

    try:
        if owner is not None:
            emit.assert_safe_output_path(path, boundary=owner)
        text = json.dumps(
            record,
            indent=1,
            sort_keys=False,
            ensure_ascii=False,
            default=_json_default,
        )
        member = emit.publish_document(
            path,
            lambda handle: handle.write(text + "\n"),
            role=RUN_RECORD_ROLE,
            validate=parse_object,
        )
        # The published bytes, bound to the inode the publication measured. Opening the
        # pathname again would establish nothing: the name can be repointed between the
        # rename and this read, and a record read through a repointed name is not the
        # record this run published (CWE-367).
        with emit.open_verified_member(member.path, member.identity) as handle:
            republished = json.load(handle)
        if not isinstance(republished, dict):
            raise ValueError(
                f"the published record at {member.path} parsed as "
                f"{type(republished).__name__} rather than an object"
            )
    except (OSError, TypeError, ValueError, emit.EmitError) as error:
        raise RunRecordNotPersisted(
            f"{RUN_RECORD_DOCUMENT} could not be written and verified at {path}: "
            f"{type(error).__name__}: {error}"
        ) from error
    return {
        "path": member.path,
        "role": member.role,
        # Named `bytes_written` because that is what it is and what every caller reads,
        # and measured off the published file rather than taken from the length of the
        # string that was serialised.
        "bytes_written": member.size_bytes,
        "sha256": member.sha256,
        "mode": member.mode,
        "mode_octal": f"0o{member.mode:o}",
        "publication_id": member.publication_id,
        "verified": (
            "the staged file was read back from disk and parsed as a JSON object before "
            "anything was renamed, and the published file was read back through a "
            "descriptor bound to the verified inode and parsed again; publication is one "
            "atomic rename against a held directory descriptor, followed by an fsync of "
            "that directory"
        ),
        "protocol": (
            "normalize.emit.publish_document -- the same descriptor-bound protocol the "
            "dataset is published under, reported as data by "
            "normalize.emit.staging_protocol()"
        ),
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Normalize the raw artifacts into the dataset, and return the process exit code.

    ``0`` on success; ``1`` on a halting condition in the data; ``2`` on an argparse usage
    error; ``78`` on a configuration fault.  The run record is written on every one of those
    paths, so a halt is diagnosable from the record rather than only from the console -- and
    **a run whose record could not be written and verified never reports success**: that
    condition becomes the outcome, with its own exit code, because a dataset whose run
    record was lost is a dataset nobody can trace (CWE-703).
    """
    arguments = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    namespace = parser.parse_args(arguments)

    record = _new_record(arguments, _utc_now())
    exit_code = EXIT_OK
    outcome = "completed"
    persistence: dict[str, Any] | None = None

    # Resolved before anything else, because the record is written on every path out of
    # this function -- including a fault raised inside resolve_inputs. Two sources supply
    # it and there is no working-directory fallback; where neither is set there is nowhere
    # to write the required evidence, so the run says exactly that and stops.
    try:
        run_record_path, run_record_owner = _run_record_target(namespace, os.environ)
    except ConfigurationFault as fault:
        print(
            f"normalize: configuration fault [{fault.reason}]: {fault.message}",
            file=sys.stderr,
        )
        print(
            "normalize: no run record was written -- its own location could not be "
            "resolved, and nothing was read or written by this run.",
            file=sys.stderr,
        )
        return fault.exit_code

    try:
        inputs = resolve_inputs(namespace, os.environ)
        run_record_path = inputs.run_record
        record["inputs"] = inputs.as_dict()
        _execute(inputs, record)
    except NormalizeHalt as halt:
        exit_code = halt.exit_code
        outcome = (
            "configuration_fault" if exit_code == EXIT_CONFIG else "halted"
        )
        record["halt"] = halt.as_dict()
        print(
            f"normalize: {outcome.replace('_', ' ')} [{halt.reason}]: "
            f"{halt.safe_message}",
            file=sys.stderr,
        )
    except Exception as error:  # noqa: BLE001 -- recorded, then re-reported and returned
        # An unexpected exception is the one path nobody designed, which is exactly why it
        # must not be the path that writes raw artifact text into a durable record. The
        # exception's own str() is composed from whatever was being processed when it
        # failed -- an observed key, an observed value, offending bytes -- so it is
        # described rather than shown, and the frames (this repository's own source) are
        # rendered separately. See _safe_exception_chain.
        exit_code = EXIT_HALT
        outcome = "unexpected_error"
        chain = _safe_exception_chain(error)
        rendered = paths.safe_diagnostic(
            str(error), context=f"{type(error).__name__} message"
        )
        record["halt"] = {
            "reason": HALT_UNEXPECTED,
            "message": (
                f"{type(error).__name__}: {rendered}"
                if str(error)
                else f"{type(error).__name__}: <no message>"
            ),
            "exit_code": exit_code,
            "details": {
                "exception_chain": chain,
                "chain_depth_limit": UNEXPECTED_ERROR_CHAIN_MAX_DEPTH,
                "frame_limit_per_link": UNEXPECTED_ERROR_FRAME_LIMIT,
                "note": (
                    "The exception message is described (type, length, sha256, bounded "
                    "redacted excerpt) rather than quoted: on an unexpected error it is "
                    "composed from whatever artifact content was being processed, and a "
                    "durable record must not carry that verbatim. The frames are this "
                    "repository's own source and are quoted."
                ),
            },
        }
        print(
            f"normalize: unexpected error: {type(error).__name__}: {rendered}\n"
            + "\n".join(
                "\n".join(link["frames"]) for link in chain if link["frames"]
            ),
            file=sys.stderr,
        )
    finally:
        record["finished_at_utc"] = _utc_now()
        record["exit_status"] = {"code": exit_code, "outcome": outcome}
        try:
            persistence = _write_run_record(
                run_record_path, record, owner=run_record_owner
            )
        except RunRecordNotPersisted as error:
            # The one condition that can turn a completed run into a failed one at the very
            # last step. It is reported here and it changes the outcome: reporting success
            # beside a lost record would publish a dataset with no traceable account of how
            # it was produced.
            print(f"normalize: {error}", file=sys.stderr)
            print(
                "normalize: the run record is required evidence, so this run's outcome is "
                "that it could not be persisted; the dataset and every count in it are "
                "unattributable without it.",
                file=sys.stderr,
            )
            if exit_code == EXIT_OK:
                exit_code = EXIT_CONFIG
                outcome = "run_record_not_persisted"
            else:
                outcome = f"{outcome}_and_run_record_not_persisted"
            record["halt"] = {
                "reason": HALT_RUN_RECORD_NOT_PERSISTED,
                "message": str(error),
                "exit_code": exit_code,
                "details": {"run_record": str(run_record_path)},
            }
            record["exit_status"] = {"code": exit_code, "outcome": outcome}

    if exit_code == EXIT_OK:
        totals = record.get("totals") or {}
        print(
            "normalize: wrote {rows} row(s) from {present} artifact(s) "
            "({absent} absent); all three reconciliation stages and the typed re-parse "
            "comparison passed. Run record: {record} ({bytes} bytes, read back and "
            "parsed before it was promoted)".format(
                rows=totals.get("rows"),
                present=totals.get("artifacts_present"),
                absent=totals.get("artifacts_absent"),
                record=run_record_path,
                bytes=None if persistence is None else persistence["bytes_written"],
            )
        )
    elif persistence is None:
        # The record is what a halt is diagnosed from, so its absence is stated in its own
        # words rather than left to be inferred from the diagnostics above it.
        print(
            f"normalize: NO run record was persisted at {run_record_path}; the "
            "diagnostics above are this run's only account of it.",
            file=sys.stderr,
        )
    else:
        print(
            f"normalize: run record written to {run_record_path} "
            f"({persistence['bytes_written']} bytes, read back and parsed before it was "
            "promoted)",
            file=sys.stderr,
        )
    return exit_code


if __name__ == "__main__":
    # The bootstrap this needs already ran above, before the package imports, which is the
    # only position at which it can make execution by path work.
    raise SystemExit(main())
