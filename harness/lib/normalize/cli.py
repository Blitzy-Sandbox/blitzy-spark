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
Every input is an explicit argument and no repository-relative path is hardcoded.
Defaults are derived only from the environment the provisioned ``harness/env.sh``
exports -- ``HARNESS_RAW_DIR``, ``HARNESS_LOG_DIR``, ``HARNESS_SCOPE_FILE``,
``HARNESS_REPO_ROOT`` and ``SPARK_SRC`` -- and an explicit argument always wins.  Nothing
is read at import time: no file, no environment variable, no filesystem probe.  A required
input that can be neither supplied nor defaulted is a configuration fault naming the flag
and the variable that would have supplied it.

    --raw-dir           the runner-only artifact tree            (HARNESS_RAW_DIR)
    --runner-metadata   runner-metadata.json                     ($HARNESS_LOG_DIR/...)
    --allowlist         the twelve authoritative globs           (HARNESS_SCOPE_FILE)
    --log-dir           the per-tool streams and status files    (HARNESS_LOG_DIR)
    --spark-src         the pinned clone every path is relative to (SPARK_SRC)
    --findings-json     oss-scan-results/findings.json           ($HARNESS_REPO_ROOT/...)
    --findings-csv      oss-scan-results/findings.csv            ($HARNESS_REPO_ROOT/...)
    --run-record        normalize-run.json                       ($HARNESS_LOG_DIR/...)

The exact command as invoked, the interpreter's absolute path and its reported version are
recorded (AAP 0.6.1).  The version is compared against the expected ``3.13.7`` (AAP 0.4.1)
and any difference -- major, minor or patch -- is **recorded with both values while the run
continues**.  It is never a halt.

WHERE AN OUTPUT MAY BE WRITTEN -- three rules, all of them halts
---------------------------------------------------------------
The three configured outputs are the only files this module writes, and each has exactly
one owner root.  ``findings.json`` and ``findings.csv`` must resolve inside the repository
root the run declares (``$HARNESS_REPO_ROOT``, else the repository this module is installed
in); ``normalize-run.json`` must resolve inside the log tree.  A path outside its owner is a
configuration fault naming the path and the root (CWE-73), not a location.  The three must
be three distinct files and none of them may name an input -- the raw directory, any
artifact in it, the allowlist or ``runner-metadata.json`` -- because an output written over
an input destroys the evidence the dataset was derived from, and two outputs at one path
make the second write destroy the first while every count still reconciles.  Finally the
target and every component at or below its owner root are checked with ``lstat``, and a
symbolic link anywhere among them is refused with the component named (CWE-59).

Every write then goes through ``emit.py``'s discipline, which this module uses rather than
copies: an exclusive, no-follow staged file with an unguessable name, verified, and promoted
by an atomic rename with the previously published set restored on failure.  There is no
working-directory fallback for the run record's location: two sources supply it,
``--run-record`` and ``$HARNESS_LOG_DIR``, and its absence is a configuration fault rather
than a record written wherever the run was started from.

REQUIRED EVIDENCE FAILS CLOSED
------------------------------
``harness/artifacts/logs/normalize-run.json`` is written on every path out of this module,
**and a run that could not write and verify it never reports success**.  The record is
staged, read back from disk, parsed as JSON and only then promoted; where any of that fails
the diagnostic goes to stderr and the failure becomes the process's outcome with a non-zero
exit code (CWE-703).  A dataset whose run record was lost is a dataset nobody can trace, so
the honest outcome is the loss rather than the dataset.

READ ONLY FROM THE RAW DIRECTORY -- an asserted boundary, not an assumption
--------------------------------------------------------------------------
AAP 0.8.1: ``harness/artifacts/raw/`` stays runner-only, *"receiving exactly one artifact
per tool that writes one and nothing else ever"*.  Two tools appear twice in the run **by
design** (AAP 0.1.3): Opengrep is also the taint A/B subject, whose arms write
``harness/artifacts/logs/taint-ab-{on,off}.{sarif,log}``, and Joern is also the
capability-probe subject, whose results land under ``queries/joern/results/``.  Both second
appearances *"write outside ``harness/artifacts/raw/`` and contribute no dataset row"*, and
*"reading the double appearance as a duplication would corrupt both counts."*

The taint A/B arms are valid SARIF and would route perfectly, which is exactly why the
boundary is enforced rather than trusted: only the nine fixed filenames are read, only as
direct children of the raw directory, and each one's real path is asserted to still sit
inside that directory -- so a symlink pointing at a log-tree file is a halt rather than a
silent extra count.  An unexpected filename in the raw tree is a **reported condition**
(AAP: not something to guess at); no document there is ever fingerprinted to identify a
writer, because ``shape.py`` keys the native adapter by the runner that wrote it.

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
count of zero reads as "none of 9,466 rows" rather than as an absent field.  A row counted
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

Exit codes
----------
``0``  the dataset was written and all three reconciliation stages and the typed
       comparison passed.
``1``  a halting condition in the data: an unknown artifact shape, an absent artifact with
       no stated reason, an adapter's structural halt, a failed reconciliation identity or
       a failed output comparison.
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

No user-specified rules govern this file -- ``review_rules`` reports "No user rules
provided.", corroborated by AAP 0.7 and 0.10.2 -- so enterprise-standard best practice
applies in their place, held to the AAP's own bar.
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
from collections.abc import Iterable, Mapping, Sequence
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
    "STATUS_LINE_INVALID_KEY",
    "STATUS_LINE_NO_SEPARATOR",
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
HALT_ARTIFACT_UNREADABLE: str = "artifact-unreadable"
HALT_ARTIFACT_INVALID_JSON: str = "artifact-invalid-json"
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
    HALT_ARTIFACT_UNREADABLE,
    HALT_ARTIFACT_INVALID_JSON,
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
    """
    record = _file_record(path)
    record["text"] = None
    record["text_truncated"] = False
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
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as error:
        record["read_error"] = f"{type(error).__name__}: {error}"
        record["text_null_reason"] = (
            f"the stream could not be read: {type(error).__name__}: {error}"
        )
        return record
    if len(text) > TOOL_WORDS_EXCERPT_LIMIT:
        record["text"] = text[:TOOL_WORDS_EXCERPT_LIMIT]
        record["text_truncated"] = True
        record["text_excerpt_limit"] = TOOL_WORDS_EXCERPT_LIMIT
    else:
        record["text"] = text
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
            "under unparsed_lines rather than becoming a field."
        ),
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
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as error:
        record["read_error"] = f"{type(error).__name__}: {error}"
        return record

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

    literal = fields.get("exit_code")
    record["exit_code_literal"] = literal
    if literal is not None and literal.strip().lstrip("-").isdigit():
        record["exit_code"] = int(literal.strip())
        record["exit_status"] = EXIT_STATUS_EXITED
    else:
        # A status file with no readable code is a process that ended without one.
        record["exit_status"] = EXIT_STATUS_TIMEOUT
    elapsed = fields.get("elapsed_seconds")
    if elapsed is not None and elapsed.strip().isdigit():
        record["elapsed_seconds"] = int(elapsed.strip())
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
    invocation time"*, because a rule set or feed fetched with no recorded digest is a
    reproducibility gap the dataset must carry rather than absorb -- a prior run had one
    tool fetch 1,093 rules from its API mid-scan and contribute two thirds of the dataset
    from a rule set with no digest behind it.

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


def _declared_repository_root(environ: Mapping[str, str]) -> tuple[Path, str]:
    """Return the repository root that owns the dataset files, and where it came from.

    ``$HARNESS_REPO_ROOT`` where the provisioned ``harness/env.sh`` exported it -- the run's
    own declaration, and the same variable the ``--findings-json`` default is derived from.
    Where it is unset, the root is *this module's own repository*, three levels above this
    file, which is a fact about the installed harness rather than about whatever directory
    the caller happened to be in.  The working directory is deliberately not a candidate:
    an output root taken from the cwd is an output that lands wherever the run was started
    (CWE-73).
    """
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
) -> dict[str, Any]:
    """Establish that the three outputs are contained, distinct and unaliased.

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
    repo_root, repo_root_source = _declared_repository_root(environ)
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

    repo_root_value = _environment_value(env, "HARNESS_REPO_ROOT")
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
        source=f"$HARNESS_REPO_ROOT/{FINDINGS_JSON_RELATIVE}",
    )
    findings_csv_value = require(
        findings_csv_value,
        flag="--findings-csv",
        source=f"$HARNESS_REPO_ROOT/{FINDINGS_CSV_RELATIVE}",
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
        output_guards=MappingProxyType(_validate_output_targets(inputs, env)),
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

    try:
        text = artifact_path.read_text(encoding="utf-8")
    except OSError as error:
        outcome.parse_status = PARSE_STATUS_FAILED
        raise _fault(
            HALT_ARTIFACT_UNREADABLE,
            f"{tool}: the artifact at {artifact_path} exists but cannot be read: "
            f"{type(error).__name__}: {error}",
            tool=tool,
            artifact_path=str(artifact_path),
            error=f"{type(error).__name__}: {error}",
        ) from error

    try:
        document = json.loads(text)
    except json.JSONDecodeError as error:
        outcome.parse_status = PARSE_STATUS_FAILED
        raise _halt(
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


def _load_metadata(inputs: Inputs, record: dict[str, Any]) -> Mapping[str, Any]:
    """Read ``runner-metadata.json`` -- the normalizer's declared input.

    AAP 0.6.4 fixes the direction: Stage 1 writes this file, the normalizer reads it, and
    ``tool-status.md`` is rendered afterwards from it joined with these results.  Nothing
    here reads any Markdown.
    """
    try:
        document = paths.load_runner_metadata(inputs.runner_metadata)
    except paths.RunnerMetadataError as error:
        raise _fault(
            HALT_RUNNER_METADATA,
            f"the runner metadata at {inputs.runner_metadata} cannot be used: {error}",
            runner_metadata=str(inputs.runner_metadata),
            error=str(error),
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

    An unexpected entry is reported rather than guessed at: no document is fingerprinted to
    identify a writer.
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
        unexpected.append(
            {
                "name": name,
                "path": str(candidate),
                "is_directory": candidate.is_dir(),
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
            "appearances that contribute no dataset row (AAP 0.1.3)."
        ),
    }
    if unexpected:
        print(
            f"normalize: reported condition: {len(unexpected)} unexpected entr"
            f"{'y' if len(unexpected) == 1 else 'ies'} in {raw_dir}: "
            + ", ".join(entry["name"] for entry in unexpected)
            + " -- harness/artifacts/raw/ is runner-only. Recorded in the run record; no "
            "document there is fingerprinted to identify a writer.",
            file=sys.stderr,
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
    list and a row count of zero -- which is not hypothetical: in the precedent dataset one
    tool contributed zero rows while eight produced 10,178 between them.

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
    one stat per path rather than one per row. The 9,466-row dataset this run emits carries
    fewer distinct paths than rows, and the classification is memoised across both.

    Returns:
        A mapping carrying ``count``, ``rows_examined`` (the denominator, always the real
        row count so a zero is readable as *"none of 9,466"* rather than as an absence),
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
    Nothing counts lines: the precedent dataset held 10,178 parsed rows over 12,762
    physical lines because ``message`` fields carry embedded newlines, so a line count
    over-reports by about a quarter.

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
    """Write ``normalize-run.json`` and verify it, on every path out of this module.

    Not best effort.  ``normalize-run.json`` is required evidence -- AAP 0.6.1 names it,
    AAP 0.9.1 requires the normalizer's run to have a structured status record in the log
    tree, and AAP 0.1.3 publishes it by manifest because the tree is git-ignored.  A run
    whose record was lost has produced a dataset nobody can trace, so a failure here is the
    process's outcome rather than a line on stderr beside a success (CWE-703).

    Three things happen, in this order, and all three must succeed:

    1.  The record is serialised to a **staged** file through ``emit.stage_text``, which
        refuses a symlinked target or component and opens an exclusive, no-follow
        temporary with an unguessable name (CWE-59).
    2.  The staged file is **read back from disk and parsed** as JSON.  Serialising without
        re-reading proves the encoder ran, not that the bytes are on the device and are a
        document -- and this record's own halt fields are the only account of a halt.
    3.  Only then is it promoted into place, atomically.  A failure at any step discards
        the staged file, leaves any previously published record untouched, and raises.

    Args:
        path: Where the record is written.
        record: The record to serialise.
        owner: The declared owner root the target must sit inside -- the log tree.
            ``None`` where the caller has already bound the target (the primitive still
            refuses a symlinked target or component), so containment is enforced once
            rather than assumed twice.

    Returns:
        A JSON-serialisable description of what was written: the target, its byte size and
        the row of verification that passed.

    Raises:
        RunRecordNotPersisted: Where the record could not be serialised, staged, read back,
            parsed or promoted.  The message names the condition and the path.
    """
    staged: list[emit.StagedWrite] = []
    try:
        text = json.dumps(
            record,
            indent=1,
            sort_keys=False,
            ensure_ascii=False,
            default=_json_default,
        )
        staged.append(emit.stage_text(path, text + "\n", boundary=owner))
        verified = json.loads(staged[0].temporary.read_text(encoding="utf-8"))
        if not isinstance(verified, dict):
            raise ValueError(
                "the staged record parsed as "
                f"{type(verified).__name__} rather than an object"
            )
        emit.promote_staged(staged)
    except (OSError, TypeError, ValueError, emit.EmitError) as error:
        emit.discard_staged(staged)
        raise RunRecordNotPersisted(
            f"{RUN_RECORD_DOCUMENT} could not be written and verified at {path}: "
            f"{type(error).__name__}: {error}"
        ) from error
    return {
        "path": str(path),
        "bytes_written": staged[0].bytes_written,
        "verified": (
            "the staged file was read back from disk and parsed as a JSON object before "
            "it was promoted; promotion is an atomic rename"
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
