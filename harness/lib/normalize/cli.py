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
8.  **``emit.emit_findings``** -- both files from the same validated rows, then the typed
    re-parse comparison.
9.  **Reconciliation stage C** -- the parsed ``findings.json`` and parsed ``findings.csv``
    row counts against the stage B identity, separately and against each other.
10. **``harness/artifacts/logs/normalize-run.json``** -- written on every path out of this
    module, including every halt.

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
             for usage information."* is the expected instance.  An artifact absent with
             **no** stated reason halts: ``exit_status: timeout`` names how a process ended
             and *"it does not excuse a missing artifact"*.

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
       globs, a missing raw tree, or a missing conditional adapter module.

``harness/artifacts/logs/normalize-run.json`` is written on **every** one of those paths, so
a halt is diagnosable from the record rather than only from the console.  That tree is
git-ignored (``.gitignore:31`` is ``artifacts/``), so the record is published through the
per-file size-and-sha256 manifest (AAP 0.1.3) rather than by ``git add`` -- which is why it
is written self-describing, carrying the byte size and sha256 of every input and output it
names.

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
HALT_WRONG_SCAN_ROOT_EVIDENCE: str = "runner-resolved-another-tree"
HALT_SMOKE_OVERRIDE_EVIDENCE: str = "runner-scanned-smoke-target"
HALT_SOURCE_INDEX_EMPTY: str = "source-index-empty"
HALT_RECONCILIATION: str = "reconciliation-failed"
HALT_EMIT: str = "output-write-failed"
HALT_OUTPUT_COMPARISON: str = "output-comparison-failed"
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
    HALT_WRONG_SCAN_ROOT_EVIDENCE,
    HALT_SMOKE_OVERRIDE_EVIDENCE,
    HALT_SOURCE_INDEX_EMPTY,
    HALT_RECONCILIATION,
    HALT_EMIT,
    HALT_OUTPUT_COMPARISON,
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

    def as_dict(self) -> dict[str, Any]:
        """Return the halt as a JSON-serialisable mapping for the run record."""
        return {
            "reason": self.reason,
            "message": self.message,
            "exit_code": self.exit_code,
            "details": self.details,
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


def _file_record(path: Path, *, digest: bool = True) -> dict[str, Any]:
    """Describe one file for the record: its path, byte size and sha256.

    ``harness/artifacts/**`` is git-ignored, so this record is published through the
    per-file size-and-sha256 manifest (AAP 0.1.3) rather than by ``git add``.  Every file
    this module names is therefore described here well enough for a reader to verify it
    independently.  A file that is absent says so rather than being omitted.
    """
    record: dict[str, Any] = {"path": str(path), "present": path.is_file()}
    if not record["present"]:
        record["bytes"] = None
        record["sha256"] = None
        return record
    try:
        record["bytes"] = path.stat().st_size
        record["sha256"] = _sha256(path) if digest else None
    except OSError as error:  # unreadable rather than absent -- say which
        record["bytes"] = None
        record["sha256"] = None
        record["read_error"] = f"{type(error).__name__}: {error}"
    return record


def _stream_record(path: Path, *, with_text: bool) -> dict[str, Any]:
    """Describe one runner stream, optionally carrying the tool's own words verbatim.

    ``with_text`` is true only where the classification depends on the content -- the
    absent-artifact case, whose verdict AAP 0.5.4 settles *"using only the tool's own
    stated words"*.  The excerpt is bounded by :data:`TOOL_WORDS_EXCERPT_LIMIT` and says so
    when it was cut, and the stream's byte size and sha256 sit beside it, so a cap can
    never lose evidence without a reader seeing that it did.
    """
    record = _file_record(path, digest=with_text)
    record["text"] = None
    record["text_truncated"] = False
    if not with_text or not record["present"]:
        return record
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as error:
        record["read_error"] = f"{type(error).__name__}: {error}"
        return record
    if len(text) > TOOL_WORDS_EXCERPT_LIMIT:
        record["text"] = text[:TOOL_WORDS_EXCERPT_LIMIT]
        record["text_truncated"] = True
        record["text_excerpt_limit"] = TOOL_WORDS_EXCERPT_LIMIT
    else:
        record["text"] = text
    return record


def _tool_words(log_dir: Path | None, tool: str, *, with_text: bool) -> dict[str, Any]:
    """Collect a runner's own streams, and whether they state a reason at all.

    Both streams are looked at because the two documented accounts of OSV-Scanner's
    zero-package outcome disagree about which one carries the sentence -- the runbook
    reports it on stdout, the environment record on stderr.  Stderr is preferred where both
    carry text; whichever supplied it is named in the record, and both are described either
    way.
    """
    streams: dict[str, Any] = {}
    for stream in ("stderr", "stdout"):
        if log_dir is None:
            streams[stream] = {
                "path": None,
                "present": False,
                "bytes": None,
                "sha256": None,
                "text": None,
                "text_truncated": False,
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
    """
    path = None if log_dir is None else log_dir / f"{tool}.status"
    record: dict[str, Any] = {
        "path": None if path is None else str(path),
        "present": bool(path is not None and path.is_file()),
        "fields": {},
        "exit_code": None,
        "exit_code_literal": None,
        "exit_status": EXIT_STATUS_UNRECORDED,
        "elapsed_seconds": None,
        "artifact_bytes_literal": None,
        "scan_root": None,
        "scan_root_source": None,
    }
    if path is None or not record["present"]:
        return record
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as error:
        record["read_error"] = f"{type(error).__name__}: {error}"
        return record

    fields: dict[str, str] = {}
    for line in text.splitlines():
        key, separator, value = line.partition("=")
        if not separator:
            continue
        fields[key.strip()] = value
    record["fields"] = fields

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
    """

    raw_dir: Path
    runner_metadata: Path
    allowlist: Path
    log_dir: Path
    spark_src: str
    findings_json: Path
    findings_csv: Path
    run_record: Path

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
    the containment check that needs a real path computes it where it needs it.
    """
    return Path(os.path.abspath(os.path.expanduser(value)))


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

    log_dir_value = namespace.log_dir or _environment_value(env, "HARNESS_LOG_DIR")
    metadata_value = namespace.runner_metadata
    if metadata_value is None and log_dir_value is not None:
        metadata_value = os.path.join(log_dir_value, RUNNER_METADATA_FILENAME)
    if log_dir_value is None and metadata_value is not None:
        # The metadata lives in the log tree, so its directory is the log tree whenever
        # the caller named the file explicitly.
        log_dir_value = os.path.dirname(os.path.abspath(metadata_value)) or None

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

    return Inputs(
        raw_dir=_absolute(raw_dir_value),
        runner_metadata=_absolute(metadata_value),
        allowlist=_absolute(allowlist_value),
        log_dir=_absolute(log_dir_value),
        spark_src=str(_absolute(spark_src_value)),
        findings_json=_absolute(findings_json_value),
        findings_csv=_absolute(findings_csv_value),
        run_record=_absolute(run_record_value),
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


def _path_kind_tally(counters: Mapping[str, int]) -> paths.PathKindTally:
    """Build a :class:`normalize.paths.PathKindTally` from an adapter's counters.

    Counted through the discriminator rather than beside it: ``paths.PathKindTally.add``
    validates every kind against the closed set, so this tally cannot drift from
    ``paths.NON_FILESYSTEM_PATH_KINDS`` the way a private counter could.
    """
    tally = paths.PathKindTally()
    for key, count in counters.items():
        if not key.startswith(_PATH_KIND_COUNTER_PREFIX):
            continue
        kind = key[len(_PATH_KIND_COUNTER_PREFIX) :]
        for _ in range(max(0, int(count))):
            tally.add(kind)
    return tally


def _merge_path_kinds(
    total: paths.PathKindTally,
    counters: Mapping[str, int],
) -> None:
    """Fold one artifact's path kinds into the dataset-level tally."""
    for key, count in counters.items():
        if not key.startswith(_PATH_KIND_COUNTER_PREFIX):
            continue
        kind = key[len(_PATH_KIND_COUNTER_PREFIX) :]
        for _ in range(max(0, int(count))):
            total.add(kind)


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
    source = status.get("scan_root_source")
    if isinstance(source, str) and "HARNESS_SMOKE_TARGET" in source:
        raise _halt(
            HALT_SMOKE_OVERRIDE_EVIDENCE,
            f"{tool}: its status record names {source!r} as the source of the scan root, "
            "so this artifact came from the setup-time smoke override rather than the "
            "pinned tree. The override exists for setup-time verification only and is "
            "never a fallback for a real scan.",
            tool=tool,
            scan_root_source=source,
            recorded_scan_root=status.get("scan_root"),
            status_path=status.get("path"),
        )
    recorded = status.get("scan_root")
    if isinstance(recorded, str) and recorded and not _same_root(recorded, root):
        raise _halt(
            HALT_WRONG_SCAN_ROOT_EVIDENCE,
            f"{tool}: its status record says it resolved {recorded!r}, which is not the "
            f"root this dataset is expressed against ({root!r}). Every finding it produced "
            "would be about another tree, so this is a targeting fault rather than a "
            "coordinate to keep.",
            tool=tool,
            recorded_scan_root=recorded,
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
        # detection reason is which of the two shape tests failed.
        detection = dict(error.details())
        detection["detection_reason"] = detection.pop("reason", None)
        raise _halt(
            HALT_UNKNOWN_ARTIFACT_SHAPE,
            f"{tool}: {error}",
            **detection,
        ) from error
    outcome.routing = decision.as_dict()

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
        outcome.parse_status = PARSE_STATUS_FAILED
        raise _halt(
            HALT_ADAPTER_CONTRACT,
            f"{tool}: its adapter refused the artifact or the arguments it was given: "
            f"{type(error).__name__}: {error}",
            tool=tool,
            artifact_path=str(artifact_path),
            adapter=decision.adapter,
            adapter_module=decision.adapter_module_name,
            error=f"{type(error).__name__}: {error}",
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
    """
    outcome.runner_status = _runner_status(log_dir, tool)
    # A runner that scanned the wrong tree is a targeting fault whether or not it wrote
    # anything, so the evidence is checked here too.
    _check_runner_root_evidence(tool, outcome.runner_status, root)
    outcome.tool_words = _tool_words(log_dir, tool, with_text=True)

    if not outcome.tool_words["stated_reason_present"]:
        raise _halt(
            HALT_ABSENT_WITHOUT_STATED_REASON,
            f"{tool}: no artifact in the raw tree and no reason stated in its own output. "
            "Only the tool's own words can settle whether it completed with nothing in "
            "scope to work on or failed, so this halts rather than being recorded as a "
            "zero. Looked for the artifact and for the tool's words at the paths in "
            "'details'.",
            tool=tool,
            artifact_path=outcome.artifact.get("path"),
            stderr_log=outcome.tool_words["streams"]["stderr"].get("path"),
            stdout_log=outcome.tool_words["streams"]["stdout"].get("path"),
            status_path=outcome.runner_status.get("path"),
            exit_code=outcome.runner_status.get("exit_code"),
            exit_status=outcome.runner_status.get("exit_status"),
            artifact_expected=outcome.artifact_expected,
            note=(
                "exit_status names how the process ended; it does not excuse a missing "
                "artifact (AAP 0.8.1)."
            ),
        )

    outcome.parse_status = PARSE_STATUS_ABSENT
    outcome.raw_records = None
    outcome.emitted_rows = 0
    outcome.rejected_records = 0
    outcome.notes.append(
        "Classified from the tool's own words in "
        f"{outcome.tool_words['stated_reason_stream']}, quoted verbatim in tool_words. "
        "Zero rows, and the reconciliation for this tool is the not-applicable sentinel "
        "rather than 0 = 0 + 0, which would be a passing assertion over an artifact nobody "
        "looked at."
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
        raise _fault(
            HALT_SOURCE_INDEX_EMPTY,
            f"the source index over {root} could not be built: "
            f"{type(error).__name__}: {error}",
            root=root,
            error=f"{type(error).__name__}: {error}",
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
    """
    by_tool = tally.by_tool()
    unmapped = tally.unmapped_by_tool()
    return {
        "bands": list(severity.SEVERITY_NORM),
        "bases": list(severity.BASIS_VALUES),
        "policy_statements": [name for name, _ in severity.POLICY_STATEMENTS],
        "policy_source": "harness/lib/normalize/severity.py",
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


def _totals_record(
    outcomes: Sequence[ArtifactOutcome],
    rows: Sequence[Mapping[str, Any]],
    path_kinds: paths.PathKindTally,
) -> dict[str, Any]:
    """Aggregate the dataset-level counts every downstream document needs.

    The per-tool multi-location, multi-identifier and non-filesystem counts AAP 0.5.4
    requires reported, plus the non-filesystem proportion AAP 0.6.1 puts in
    ``run-record.md``.  Each figure is one measurement, cited here once, so a document that
    quotes it is quoting this file rather than recomputing it.
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
        "notes": [
            "A record carrying more than one location contributes one row, from its first "
            "location, and still counts once (AAP 0.5.4).",
            "A record carrying several CWE or CVE identifiers emits one of each, chosen by "
            "ascending numeric identifier.",
            "Nothing is deduplicated, ranked or compared across tools: two tools reporting "
            "the same location produce two rows and no comment (AAP 0.3.2).",
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
    """Write both files from the same rows, then prove they agree by parsing both.

    ``emit.emit_findings`` validates the rows once, writes each file from those same rows --
    neither derived from the other -- reads both back from disk, coerces the CSV cells to
    the types their fields carry, and compares in order field by field.  Nothing counts
    lines: the precedent dataset held 10,178 parsed rows over 12,762 physical lines because
    ``message`` fields carry embedded newlines, so a line count over-reports by about a
    quarter.
    """
    try:
        comparison = emit.emit_findings(rows, inputs.findings_json, inputs.findings_csv)
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

    record["output_comparison"] = comparison.as_dict()
    record["outputs"] = {
        "findings_json": _file_record(inputs.findings_json),
        "findings_csv": _file_record(inputs.findings_csv),
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
    }
    if not comparison.passed:
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
    record["totals"] = _totals_record(outcomes, rows, path_kinds)

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
    command = [sys.executable, *argv] if argv else [sys.executable]
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
            "file it names carries that file's byte size and sha256."
        ),
        "started_at_utc": started_at,
        "finished_at_utc": None,
        "command": {
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


def _write_run_record(path: Path, record: Mapping[str, Any]) -> None:
    """Write ``normalize-run.json``, on every path out of this module including a halt.

    Best effort by design: a failure to write the record is reported on stderr and never
    replaces the outcome the run already reached, because losing the reason for a halt to a
    second fault while writing it down is the worst of the available outcomes.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="\n") as handle:
            json.dump(
                record,
                handle,
                indent=1,
                sort_keys=False,
                ensure_ascii=False,
                default=_json_default,
            )
            handle.write("\n")
    except (OSError, TypeError, ValueError) as error:
        print(
            f"normalize: the run record at {path} could not be written: "
            f"{type(error).__name__}: {error}",
            file=sys.stderr,
        )


def main(argv: Sequence[str] | None = None) -> int:
    """Normalize the raw artifacts into the dataset, and return the process exit code.

    ``0`` on success; ``1`` on a halting condition in the data; ``2`` on an argparse usage
    error; ``78`` on a configuration fault.  The run record is written on every one of those
    paths, so a halt is diagnosable from the record rather than only from the console.
    """
    arguments = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    namespace = parser.parse_args(arguments)

    record = _new_record(arguments, _utc_now())
    exit_code = EXIT_OK
    outcome = "completed"
    run_record_path = _absolute(
        namespace.run_record
        or os.path.join(
            _environment_value(os.environ, "HARNESS_LOG_DIR") or os.getcwd(),
            RUN_RECORD_FILENAME,
        )
    )

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
            f"normalize: {outcome.replace('_', ' ')} [{halt.reason}]: {halt.message}",
            file=sys.stderr,
        )
    except Exception as error:  # noqa: BLE001 -- recorded, then re-reported and returned
        exit_code = EXIT_HALT
        outcome = "unexpected_error"
        record["halt"] = {
            "reason": HALT_UNEXPECTED,
            "message": f"{type(error).__name__}: {error}",
            "exit_code": exit_code,
            "details": {"traceback": traceback.format_exc()},
        }
        print(
            f"normalize: unexpected error: {type(error).__name__}: {error}\n"
            f"{traceback.format_exc()}",
            file=sys.stderr,
        )
    finally:
        record["finished_at_utc"] = _utc_now()
        record["exit_status"] = {"code": exit_code, "outcome": outcome}
        _write_run_record(run_record_path, record)

    if exit_code == EXIT_OK:
        totals = record.get("totals") or {}
        print(
            "normalize: wrote {rows} row(s) from {present} artifact(s) "
            "({absent} absent); all three reconciliation stages and the typed re-parse "
            "comparison passed. Run record: {record}".format(
                rows=totals.get("rows"),
                present=totals.get("artifacts_present"),
                absent=totals.get("artifacts_absent"),
                record=run_record_path,
            )
        )
    else:
        print(f"normalize: run record written to {run_record_path}", file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    # The bootstrap this needs already ran above, before the package imports, which is the
    # only position at which it can make execution by path work.
    raise SystemExit(main())
