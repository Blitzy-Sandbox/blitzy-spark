"""Reconciliation: the second, deliberately independent traversal.

This module answers one question and refuses to help answer any other: *how many
finding records does a raw scanner artifact actually contain?*  It then checks that
number against what the normalizer emitted and rejected.

Why it is a separate module at all
----------------------------------
A count taken from the same traversal that builds the rows satisfies the assertion
while testing nothing.  So the counting traversal here walks each artifact's *count
units* and **builds no rows**: it never resolves a path, never maps a severity,
never extracts a finding field, never validates a record's content.  It reads only
the container keys it needs in order to walk, and returns an integer.

That independence is enforced by the import graph rather than promised in a
comment.  This module imports **nothing** from the ``normalize`` package -- not
``paths``, not ``severity``, not ``shape``, not any adapter -- and no third-party
package.  Standard library only.  Where the rejection-class vocabulary is needed in
order to validate rejection class names, it is **passed in** by the caller
(``cli.py`` passes ``paths.REJECT_CLASSES``), so a reviewer can confirm the
independence with a single grep for ``import``.

The count units, one per artifact shape
---------------------------------------
======================================  =====================================================
tool                                    count unit (one record each)
======================================  =====================================================
``opengrep``                            ``runs[].results[]``            -- one result object
``semgrep``                             ``runs[].results[]``            -- one result object
``datadog-static-analyzer``             ``runs[].results[]``            -- one result object
``trivy``                               ``Results[]`` x one element of ``Vulnerabilities[]``,
                                        ``Secrets[]`` or ``Misconfigurations[]``
``gitleaks``                            the top-level array             -- one element
``checkov``                             ``results.failed_checks[]``, and in the
                                        multi-framework form the **union** of every report
                                        element's ``results.failed_checks[]``
``dependency-check``                    ``dependencies[].vulnerabilities[]`` -- one vulnerability
``osv-scanner``                         ``results[].packages[].vulnerabilities[]`` -- one
                                        vulnerability per package per source
``joern``                               ``findings[]``                  -- one finding
======================================  =====================================================

Three exclusions in that table are load-bearing rather than incidental:

* **Trivy** is counted over those three sections and no others.  Version 0.74.0 can
  also emit ``Licenses`` and ``ExperimentalModifiedFindings``; the Trivy adapter
  halts the run when either is non-empty, and counting them here would mask exactly
  the defect that halt exists to catch.
* **Checkov** counts ``failed_checks`` only.  ``passed_checks`` and
  ``skipped_checks`` are neither counted nor emitted in either shape, and
  ``parsing_errors`` are status evidence for ``tool-status.md`` rather than
  findings.  Counting a pass would break the identity for a reason that has nothing
  to do with the adapter.
* A **missing container counts as zero and never raises**.  A run without
  ``results``, an artifact without ``Results``, a ``None`` document: all contribute
  zero.  Shape detection and halting on an unrecognised artifact belong to
  ``shape.py``; this module counts.

  What that division must not be read as is a claim that such a document is
  *acceptable*.  ``shape.NATIVE_SIGNATURES`` is what keeps it from being one: a
  document that is not the named writer's native shape halts in ``shape.route``
  before any adapter or any counter is reached (AAP 0.5.4, AAP 0.9.2).  Absent that
  halt, a ``checkov.json`` holding ``{}`` or a ``joern.json`` holding
  ``{"findings": null}`` would count zero here, adapt to zero rows and zero
  rejections there, and balance the identity at ``0 = 0 + 0`` with parse status
  ``clean`` -- a malformed artifact indistinguishable from a scan that found nothing.
  The zero-rather-than-raise reading below is deliberately **not** hardened into a
  second shape test: a second copy of that test could disagree with the first, and
  this traversal's whole value is that it shares no code and no judgement with the row
  builder.  What it is reached with is a container legitimately absent inside a
  document of the right shape, such as a SARIF run carrying no ``results`` or a Trivy
  target carrying no section.

The identity
------------
For each artifact::

    raw finding records = dataset rows for that tool + rejected records

with the left-hand side coming from the traversal above -- the one that builds
nothing.  The dataset-level assertion is the **sum of the per-artifact identities**,
never an independent global recount, so a per-tool discrepancy cannot cancel out
against another tool's.

The not-applicable sentinel
---------------------------
A tool that wrote no artifact reconciles as the exact string held in
:data:`NOT_APPLICABLE_ABSENT`, which is ``not applicable — artifact absent`` with an
em dash (U+2014) -- and **not** as ``0 = 0 + 0``.  Zero-equals-zero would be a passing
assertion over an artifact nobody looked at.  A *present* artifact containing zero
records is a different outcome entirely: it is a real ``0 = 0 + 0`` reconciliation
over a document that was actually traversed.  Both cases occur in practice --
``dependency-check`` writes a parsable artifact with zero vulnerabilities, while
``osv-scanner`` writes no artifact at all -- and ``tool-status.md`` must report
which is which.

The three-stage validation
--------------------------
* **Stage A** -- the per-artifact identity for every artifact present, plus the
  not-applicable record for every tool absent.  All nine canonical tool identifiers
  appear in the output, present or absent, because ``tool-status.md`` needs an entry
  for each of the nine.
* **Stage B** -- the dataset level: the sum of the Stage A identities, compared
  against the total emitted rows and total rejections.
* **Stage C** -- the output files: the parsed ``findings.json`` row count and the
  parsed ``findings.csv`` row count each compared against Stage B **separately**,
  and to each other.  Never by counting lines: a ``message`` field carrying an
  embedded newline spans several physical lines, so a physical-line count is not a
  row count.  This dataset holds 9,430 rows in ``findings.json`` and 9,430 parsed
  rows in ``findings.csv``, over 9,439 physical lines.  ``emit.py`` owns the
  field-by-field typed comparison of the two files; this module owns the counts, and
  :func:`count_json_rows` / :func:`count_csv_rows` take them by parsing.

Results are returned as data, never printed and never as bare booleans, so that
``cli.py`` can serialise every assertion and its result into
``harness/artifacts/logs/normalize-run.json`` and render them per tool into
``oss-scan-results/tool-status.md``.  A failed identity is a halt: the entry point
raises :class:`ReconciliationError` with the full report attached, so the failure
cannot be accidentally ignored and can still be serialised before the run stops.

What this module deliberately does not do
-----------------------------------------
It does not deduplicate -- two identical records in one artifact are two records.
It does not judge, rank or compare tools; counting per tool and summing is
arithmetic, whereas observing that one tool "found more" would be interpretation.
It does not consult any tool's exit code: artifact status and exit status are
independent, and two of the nine runners exit non-zero precisely because they found
something.  It does not treat a partial parse as a failure -- rejections sit on the
right-hand side of the identity, which is the whole point of counting them.  And it
counts only artifacts from the runner-only raw tree: the Opengrep taint A/B arms
under ``harness/artifacts/logs/`` are valid SARIF that would count perfectly, and
the Joern capability-probe results under ``queries/joern/results/`` are equally
countable, yet both are second appearances that contribute no dataset row, so
counting either would corrupt both that tool's count and the dataset total.
"""

from __future__ import annotations

import csv
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

__all__ = [
    "CANONICAL_TOOLS",
    "SARIF_TOOLS",
    "TRIVY_SUPPORTED_SECTIONS",
    "CHECKOV_COUNTED_SECTION",
    "NOT_APPLICABLE_ABSENT",
    "STATUS_PASS",
    "STATUS_FAIL",
    "STATUS_NOT_APPLICABLE",
    "ReconciliationError",
    "UnknownToolError",
    "UnknownRejectionClassError",
    "ArtifactCounts",
    "ArtifactReconciliation",
    "DatasetReconciliation",
    "OutputCountComparison",
    "ReconciliationReport",
    "count_records",
    "validate_rejection_classes",
    "reconcile_artifact",
    "reconcile_absent_artifact",
    "count_json_rows",
    "count_csv_rows",
    "run_stage_a",
    "run_stage_b",
    "run_stage_c",
    "run_three_stage_validation",
]

# --------------------------------------------------------------------------------------
# Vocabulary
# --------------------------------------------------------------------------------------

#: The nine canonical tool identifiers, each the stem of its runner and its artifact.
#: Every one of them appears in Stage A output whether or not it wrote an artifact,
#: because ``tool-status.md`` and ``severity-map.md`` carry an entry for all nine.
CANONICAL_TOOLS: tuple[str, ...] = (
    "opengrep",
    "semgrep",
    "datadog-static-analyzer",
    "gitleaks",
    "checkov",
    "trivy",
    "osv-scanner",
    "dependency-check",
    "joern",
)

#: The tools whose artifact is SARIF 2.1.0 and whose count unit is ``runs[].results[]``.
SARIF_TOOLS: frozenset[str] = frozenset(
    {"opengrep", "semgrep", "datadog-static-analyzer"}
)

#: The only Trivy finding sections this traversal counts.  ``Licenses`` and
#: ``ExperimentalModifiedFindings`` are excluded on purpose: the Trivy adapter halts
#: the run when either is non-empty, and counting them here would let a dropped
#: section pass reconciliation unnoticed.
TRIVY_SUPPORTED_SECTIONS: tuple[str, ...] = (
    "Vulnerabilities",
    "Secrets",
    "Misconfigurations",
)

#: The only Checkov result section that holds findings.  ``passed_checks``,
#: ``skipped_checks`` and ``parsing_errors`` are never counted.
CHECKOV_COUNTED_SECTION: str = "failed_checks"

#: The exact reconciliation verdict for a tool that wrote no artifact.  Held as a
#: constant so the string -- em dash included -- reaches ``tool-status.md``
#: unaltered.  It is emphatically not ``0 = 0 + 0``.
NOT_APPLICABLE_ABSENT: str = "not applicable \u2014 artifact absent"

#: Assertion outcome literals, used verbatim in the serialised record.
STATUS_PASS: str = "pass"
STATUS_FAIL: str = "fail"
STATUS_NOT_APPLICABLE: str = "not_applicable"


# --------------------------------------------------------------------------------------
# Errors
# --------------------------------------------------------------------------------------


class ReconciliationError(Exception):
    """A reconciliation fault that must stop the run.

    A failed identity is a halting condition, so it is raised rather than returned
    as a value a caller might overlook.  Where the failure was established by the
    three-stage validation, the full :class:`ReconciliationReport` is attached as
    :attr:`report` so the caller can serialise every assertion into
    ``normalize-run.json`` before halting.  Nothing here ever repairs a count to
    make an identity hold.
    """

    def __init__(self, message: str, report: "ReconciliationReport | None" = None) -> None:
        super().__init__(message)
        self.report = report


class UnknownToolError(ReconciliationError):
    """A tool identifier outside the nine canonical ones.

    Counting an unknown identifier as zero would silently exclude a whole artifact
    from the dataset-level sum, so an unknown tool is an error rather than a zero.
    """


class UnknownRejectionClassError(ReconciliationError):
    """A rejection class name absent from the vocabulary the caller supplied.

    Every rejected record must be counted under a *named* class.  A typo'd class
    name would otherwise create a category nobody tests, so an unrecognised name is
    an error rather than a new bucket.
    """


# --------------------------------------------------------------------------------------
# Container helpers -- the only structural reading this module performs
# --------------------------------------------------------------------------------------


def _as_mapping(value: Any) -> Mapping[str, Any]:
    """Return ``value`` if it is a mapping, else an empty mapping.

    A non-mapping where a mapping was expected contributes zero rather than
    raising: a bare array top level is legitimate for two of the nine shapes, and a
    malformed container is ``shape.py``'s halt to make, not this traversal's.  That
    halt covers the per-writer case as well -- ``shape.NATIVE_SIGNATURES`` refuses a
    document that is not the named writer's shape -- so the zero here is reached by a
    legitimately absent inner container rather than by a malformed artifact that slipped
    through.
    """
    return value if isinstance(value, Mapping) else {}


def _as_sequence(value: Any) -> Sequence[Any]:
    """Return ``value`` if it is a non-string sequence, else an empty tuple.

    Strings and bytes are excluded deliberately: ``len()`` over a string would
    count characters as findings.
    """
    if isinstance(value, (str, bytes, bytearray)):
        return ()
    return value if isinstance(value, Sequence) else ()


def _length(value: Any) -> int:
    """Count the elements of a JSON array, treating anything else as zero."""
    return len(_as_sequence(value))


# --------------------------------------------------------------------------------------
# Phase 1 -- the counting traversal
# --------------------------------------------------------------------------------------


def count_records(tool: str, doc: Any) -> int:
    """Count the finding records in one raw artifact, building nothing.

    This is the independent half of the reconciliation.  It walks the count units
    for ``tool`` -- the containers named in this module's docstring -- and returns
    how many there are.  It constructs no row, extracts no finding field, resolves
    no path, maps no severity and validates no record content.  Everything it reads
    is a container key it needs in order to walk.

    Args:
        tool: One of the nine canonical tool identifiers in :data:`CANONICAL_TOOLS`.
        doc: The parsed artifact -- a mapping for seven of the shapes, a list for
            Gitleaks and for Checkov's multi-framework form.  ``None`` and any other
            unexpected top level contribute zero.

    Returns:
        The number of finding records present, as an integer.

    Raises:
        UnknownToolError: If ``tool`` is not one of the nine canonical identifiers.
    """
    if tool in SARIF_TOOLS:
        return _count_sarif(doc)
    if tool == "trivy":
        return _count_trivy(doc)
    if tool == "gitleaks":
        return _count_gitleaks(doc)
    if tool == "checkov":
        return _count_checkov(doc)
    if tool == "dependency-check":
        return _count_dependency_check(doc)
    if tool == "osv-scanner":
        return _count_osv_scanner(doc)
    if tool == "joern":
        return _count_joern(doc)
    raise UnknownToolError(
        f"unknown tool identifier {tool!r}; expected one of "
        f"{', '.join(CANONICAL_TOOLS)}"
    )


def _count_sarif(doc: Any) -> int:
    """Count ``runs[].results[]`` across every run in a SARIF 2.1.0 artifact.

    A run carrying no ``results`` array contributes zero without raising, which is
    the ordinary shape of a clean SARIF run.
    """
    return sum(
        _length(_as_mapping(run).get("results"))
        for run in _as_sequence(_as_mapping(doc).get("runs"))
    )


def _count_trivy(doc: Any) -> int:
    """Count the supported finding sections of every ``Results[]`` element.

    Only :data:`TRIVY_SUPPORTED_SECTIONS` is counted.  Any other finding array --
    ``Licenses``, ``ExperimentalModifiedFindings`` -- contributes zero here; the
    Trivy adapter owns the halt on a non-empty unsupported section, and counting one
    would mask it.
    """
    total = 0
    for result in _as_sequence(_as_mapping(doc).get("Results")):
        section_holder = _as_mapping(result)
        for section in TRIVY_SUPPORTED_SECTIONS:
            total += _length(section_holder.get(section))
    return total


def _count_gitleaks(doc: Any) -> int:
    """Count the elements of Gitleaks' top-level array.

    The artifact is a bare JSON array, so the document itself is the container.  An
    empty array, ``null`` or a mapping all contribute zero.
    """
    return _length(doc)


def _count_checkov(doc: Any) -> int:
    """Count Checkov's failed checks across both output shapes.

    The object form carries ``results.failed_checks``; the multi-framework form is a
    top-level array of report objects, and the count is the **union** of every
    element's failed checks.  ``passed_checks``, ``skipped_checks`` and
    ``parsing_errors`` are never counted in either shape.
    """
    reports = doc if isinstance(doc, list) else [doc]
    return sum(
        _length(_as_mapping(_as_mapping(report).get("results")).get(CHECKOV_COUNTED_SECTION))
        for report in reports
    )


def _count_dependency_check(doc: Any) -> int:
    """Count ``dependencies[].vulnerabilities[]`` across the report.

    A dependency with no vulnerabilities contributes zero, which is the expected
    shape for most of the scanned surface.
    """
    return sum(
        _length(_as_mapping(dependency).get("vulnerabilities"))
        for dependency in _as_sequence(_as_mapping(doc).get("dependencies"))
    )


def _count_osv_scanner(doc: Any) -> int:
    """Count ``results[].packages[].vulnerabilities[]`` -- per package, per source."""
    total = 0
    for result in _as_sequence(_as_mapping(doc).get("results")):
        for package in _as_sequence(_as_mapping(result).get("packages")):
            total += _length(_as_mapping(package).get("vulnerabilities"))
    return total


def _count_joern(doc: Any) -> int:
    """Count the elements of the Joern collector's ``findings`` array."""
    return _length(_as_mapping(doc).get("findings"))


# --------------------------------------------------------------------------------------
# Count coercion -- shared argument validation for the identity
# --------------------------------------------------------------------------------------


def _require_count(value: Any, label: str) -> int:
    """Return ``value`` as a non-negative record count, or raise ``ValueError``.

    Booleans are rejected even though ``bool`` is a subclass of ``int``: ``True``
    silently arriving as a count of one is the kind of defect a reconciliation is
    supposed to expose rather than absorb.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} must be an int, got {type(value).__name__}: {value!r}")
    if value < 0:
        raise ValueError(f"{label} must not be negative, got {value}")
    return value


def _require_tool(tool: Any, tools: Sequence[str] = CANONICAL_TOOLS) -> str:
    """Return ``tool`` if it is one of the canonical identifiers, else raise."""
    if not isinstance(tool, str) or tool not in tools:
        raise UnknownToolError(
            f"unknown tool identifier {tool!r}; expected one of {', '.join(tools)}"
        )
    return tool


# --------------------------------------------------------------------------------------
# Phase 2 -- the identity
# --------------------------------------------------------------------------------------


def validate_rejection_classes(
    rejections_by_class: Mapping[str, Any] | None,
    reject_classes: Iterable[str] | None,
) -> dict[str, int]:
    """Validate rejection class names against the vocabulary the caller passed in.

    The vocabulary is a parameter rather than an import: this module depends on
    nothing that builds rows, so it cannot reach ``paths.REJECT_CLASSES`` itself.
    ``cli.py`` passes it in.

    An unrecognised class name is an error rather than a new bucket.  Every rejected
    record must be counted under a *named* class, and a typo'd name would otherwise
    create a category nobody tests and no document reports.

    Args:
        rejections_by_class: Mapping of rejection class name to record count.
            ``None`` and an empty mapping both validate trivially.
        reject_classes: The permitted class names -- any iterable of strings, or a
            mapping whose keys are the class names.  Required whenever
            ``rejections_by_class`` is non-empty, since there is nothing to validate
            against otherwise.

    Returns:
        A plain ``dict`` copy of the validated mapping, preserving insertion order.

    Raises:
        ValueError: If the arguments are malformed, or if rejection classes were
            supplied with no vocabulary to validate them against.
        UnknownRejectionClassError: If a class name is absent from the vocabulary.
    """
    if rejections_by_class is None:
        return {}
    if not isinstance(rejections_by_class, Mapping):
        raise ValueError(
            "rejections_by_class must be a mapping of class name to count, got "
            f"{type(rejections_by_class).__name__}"
        )
    if not rejections_by_class:
        return {}

    if reject_classes is None:
        raise ValueError(
            "reject_classes must be supplied in order to validate rejection class "
            "names; cli.py passes paths.REJECT_CLASSES. An unvalidated class name "
            "would create a rejection category nobody tests."
        )
    vocabulary = _normalise_vocabulary(reject_classes)

    validated: dict[str, int] = {}
    for name, count in rejections_by_class.items():
        if not isinstance(name, str) or not name:
            raise ValueError(f"rejection class name must be a non-empty string, got {name!r}")
        if name not in vocabulary:
            raise UnknownRejectionClassError(
                f"rejection class {name!r} is not in the supplied vocabulary "
                f"({', '.join(sorted(vocabulary)) or '<empty>'}); an unrecognised "
                "class is an error, not a new bucket"
            )
        validated[name] = _require_count(count, f"rejection count for class {name!r}")
    return validated


def _normalise_vocabulary(reject_classes: Iterable[str]) -> frozenset[str]:
    """Coerce a rejection-class vocabulary into a frozen set of names.

    A mapping contributes its keys, so a caller may pass either a set of names or a
    mapping of name to description without adapting it first.
    """
    names = reject_classes.keys() if isinstance(reject_classes, Mapping) else reject_classes
    if isinstance(names, (str, bytes)):
        raise ValueError("reject_classes must be a collection of names, not a single string")
    try:
        collected = list(names)
    except TypeError as exc:  # not iterable
        raise ValueError(
            f"reject_classes must be iterable, got {type(reject_classes).__name__}"
        ) from exc
    for name in collected:
        if not isinstance(name, str) or not name:
            raise ValueError(
                f"every rejection class in the vocabulary must be a non-empty string, "
                f"got {name!r}"
            )
    return frozenset(collected)


@dataclass(frozen=True)
class ArtifactCounts:
    """One artifact's counts, as handed to the three-stage validation.

    This is the *input* record.  ``present`` is the absent-versus-empty distinction
    made explicit: a present artifact carries a traversed ``raw_records`` count --
    possibly zero -- while an absent artifact carries none at all, because nobody
    looked at a document that was never written.

    Attributes:
        tool: One of the nine canonical tool identifiers.
        present: Whether the runner wrote an artifact into the raw tree.
        raw_records: The count from :func:`count_records`. Required when ``present``,
            and must be ``None`` when absent.
        emitted_rows: Dataset rows emitted for this tool.
        rejected_records: Records rejected for this tool. May be derived from
            ``rejections_by_class`` when that is supplied instead.
        rejections_by_class: Per-class rejection counts, validated against the
            vocabulary the caller passes to the reconciliation.
    """

    tool: str
    present: bool
    raw_records: int | None = None
    emitted_rows: int = 0
    rejected_records: int | None = None
    rejections_by_class: Mapping[str, int] | None = None

    @classmethod
    def for_present_artifact(
        cls,
        tool: str,
        raw_records: int,
        emitted_rows: int,
        rejected_records: int | None = None,
        rejections_by_class: Mapping[str, int] | None = None,
    ) -> "ArtifactCounts":
        """Build the record for an artifact that was written and traversed."""
        return cls(
            tool=tool,
            present=True,
            raw_records=raw_records,
            emitted_rows=emitted_rows,
            rejected_records=rejected_records,
            rejections_by_class=rejections_by_class,
        )

    @classmethod
    def for_absent_artifact(cls, tool: str) -> "ArtifactCounts":
        """Build the record for a tool that wrote no artifact."""
        return cls(tool=tool, present=False)

    @classmethod
    def coerce(cls, value: "ArtifactCounts | Mapping[str, Any]") -> "ArtifactCounts":
        """Accept either an :class:`ArtifactCounts` or a plain mapping of its fields.

        The mapping form exists so a caller can hand over deserialised JSON, or its
        own dictionaries, without importing this dataclass.  ``present`` defaults to
        ``True`` when ``raw_records`` is supplied and ``False`` when it is not, since
        a traversed count is precisely the evidence that an artifact existed.
        """
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise ValueError(
                "artifact counts must be an ArtifactCounts or a mapping, got "
                f"{type(value).__name__}"
            )
        unknown = set(value) - {
            "tool",
            "present",
            "raw_records",
            "emitted_rows",
            "rejected_records",
            "rejections_by_class",
        }
        if unknown:
            raise ValueError(
                f"unknown artifact-count field(s): {', '.join(sorted(unknown))}"
            )
        if "tool" not in value:
            raise ValueError("artifact counts must name a tool")
        raw_records = value.get("raw_records")
        present = value.get("present", raw_records is not None)
        return cls(
            tool=value["tool"],
            present=bool(present),
            raw_records=raw_records,
            emitted_rows=value.get("emitted_rows", 0),
            rejected_records=value.get("rejected_records"),
            rejections_by_class=value.get("rejections_by_class"),
        )


@dataclass(frozen=True)
class ArtifactReconciliation:
    """The per-artifact identity and its verdict -- one Stage A record.

    For a present artifact ``identity`` reads ``"<raw> = <rows> + <rejected>"`` and
    ``passed`` is the arithmetic.  For an absent artifact ``identity`` is exactly
    :data:`NOT_APPLICABLE_ABSENT`, ``passed`` is ``None`` and the three counts are
    ``None`` -- never ``0``, so no reader can mistake it for a ``0 = 0 + 0`` pass
    over an artifact nobody looked at.
    """

    tool: str
    artifact_present: bool
    identity: str
    status: str
    raw_records: int | None = None
    emitted_rows: int | None = None
    rejected_records: int | None = None
    rejections_by_class: Mapping[str, int] = field(default_factory=dict)
    passed: bool | None = None
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable record for ``normalize-run.json``."""
        return {
            "tool": self.tool,
            "artifact_present": self.artifact_present,
            "raw_records": self.raw_records,
            "emitted_rows": self.emitted_rows,
            "rejected_records": self.rejected_records,
            "rejections_by_class": dict(self.rejections_by_class),
            "identity": self.identity,
            "passed": self.passed,
            "status": self.status,
            "detail": self.detail,
        }


def reconcile_artifact(
    tool: str,
    raw_records: int,
    emitted_rows: int,
    rejected_records: int | None = None,
    *,
    rejections_by_class: Mapping[str, int] | None = None,
    reject_classes: Iterable[str] | None = None,
) -> ArtifactReconciliation:
    """Assert ``raw finding records = dataset rows + rejected records`` for one tool.

    The left-hand side must come from :func:`count_records` -- the traversal that
    builds nothing.  Passing a count taken from row construction would satisfy the
    assertion while testing nothing, which is the failure mode this whole module
    exists to prevent.

    A partial parse is a first-class outcome rather than a failure: its rejected
    records sit on the right-hand side, so the identity still holds.  The tool's exit
    code is never consulted -- artifact status and exit status are independent.

    Args:
        tool: One of the nine canonical tool identifiers.
        raw_records: The independently counted finding records in the artifact.
        emitted_rows: Dataset rows emitted for this tool.
        rejected_records: Records rejected for this tool. May be omitted only when
            ``rejections_by_class`` is supplied, in which case it is derived from it;
            pass ``0`` explicitly where nothing was rejected.
        rejections_by_class: Per-class rejection counts, validated against
            ``reject_classes``. When both this and ``rejected_records`` are given the
            two must agree.
        reject_classes: The rejection-class vocabulary, required whenever
            ``rejections_by_class`` is non-empty.

    Returns:
        An :class:`ArtifactReconciliation` carrying all three counts, the computed
        identity string and a pass/fail. A failure is reported here rather than
        raised, so that every assertion reaches ``normalize-run.json``; the halt is
        raised by :func:`run_three_stage_validation`.

    Raises:
        UnknownToolError: If ``tool`` is not canonical.
        UnknownRejectionClassError: If a rejection class is outside the vocabulary.
        ValueError: If the counts are malformed, or if the scalar rejected count
            disagrees with the per-class mapping.
    """
    canonical = _require_tool(tool)
    raw = _require_count(raw_records, f"{canonical} raw_records")
    rows = _require_count(emitted_rows, f"{canonical} emitted_rows")

    validated_classes = validate_rejection_classes(rejections_by_class, reject_classes)
    derived = sum(validated_classes.values()) if validated_classes else None

    if rejected_records is None:
        if derived is None:
            raise ValueError(
                f"{canonical}: the rejected-record count must be supplied explicitly; "
                "pass 0 where nothing was rejected, or supply rejections_by_class"
            )
        rejected = derived
    else:
        rejected = _require_count(rejected_records, f"{canonical} rejected_records")
        if derived is not None and derived != rejected:
            raise ValueError(
                f"{canonical}: rejected_records is {rejected} but the per-class "
                f"rejection counts sum to {derived}; the two must agree"
            )

    identity = f"{raw} = {rows} + {rejected}"
    passed = raw == rows + rejected
    detail = (
        "raw finding records = dataset rows + rejected records"
        if passed
        else (
            f"identity failed: {raw} raw finding records counted independently, but "
            f"{rows} rows emitted plus {rejected} rejected records is {rows + rejected}"
        )
    )
    return ArtifactReconciliation(
        tool=canonical,
        artifact_present=True,
        identity=identity,
        status=STATUS_PASS if passed else STATUS_FAIL,
        raw_records=raw,
        emitted_rows=rows,
        rejected_records=rejected,
        rejections_by_class=validated_classes,
        passed=passed,
        detail=detail,
    )


def reconcile_absent_artifact(
    tool: str,
    emitted_rows: int = 0,
    rejected_records: int = 0,
) -> ArtifactReconciliation:
    """Record the not-applicable reconciliation for a tool that wrote no artifact.

    The verdict is the exact string :data:`NOT_APPLICABLE_ABSENT`, and the three
    counts stay ``None``.  ``0 = 0 + 0`` would be a passing assertion over an
    artifact nobody looked at, and would erase the distinction ``tool-status.md``
    has to report -- a tool that wrote nothing against a tool that wrote an artifact
    containing nothing.

    Args:
        tool: One of the nine canonical tool identifiers.
        emitted_rows: Must be ``0``. Accepted as an argument only so an inconsistent
            caller is caught rather than ignored.
        rejected_records: Must be ``0``, for the same reason.

    Returns:
        The not-applicable :class:`ArtifactReconciliation` record.

    Raises:
        UnknownToolError: If ``tool`` is not canonical.
        ReconciliationError: If rows or rejections are claimed for an artifact that
            does not exist -- a contradiction rather than a count to reconcile.
    """
    canonical = _require_tool(tool)
    rows = _require_count(emitted_rows, f"{canonical} emitted_rows")
    rejected = _require_count(rejected_records, f"{canonical} rejected_records")
    if rows or rejected:
        raise ReconciliationError(
            f"{canonical}: {rows} rows and {rejected} rejections were reported for a "
            "tool whose artifact is absent; records cannot come from an artifact that "
            "was never written"
        )
    return ArtifactReconciliation(
        tool=canonical,
        artifact_present=False,
        identity=NOT_APPLICABLE_ABSENT,
        status=STATUS_NOT_APPLICABLE,
        raw_records=None,
        emitted_rows=None,
        rejected_records=None,
        rejections_by_class={},
        passed=None,
        detail=(
            "no artifact was written, so there is nothing to traverse and no identity "
            "to assert; this is not a zero-equals-zero pass"
        ),
    )



# --------------------------------------------------------------------------------------
# Phase 3 -- the three-stage validation
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class DatasetReconciliation:
    """The dataset-level identity -- the Stage B record.

    Its counts are the **sum of the Stage A per-artifact identities**, never an
    independent recount of the dataset.  ``passed`` additionally requires every
    per-artifact identity to have held, so a discrepancy in one tool cannot cancel
    out against an opposite discrepancy in another and present as a dataset pass.
    """

    raw_records: int
    emitted_rows: int
    rejected_records: int
    identity: str
    status: str
    passed: bool
    artifacts_total: int
    artifacts_present: int
    artifacts_absent: int
    present_tools: tuple[str, ...]
    absent_tools: tuple[str, ...]
    failed_tools: tuple[str, ...]
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable record for ``normalize-run.json``."""
        return {
            "raw_records": self.raw_records,
            "emitted_rows": self.emitted_rows,
            "rejected_records": self.rejected_records,
            "identity": self.identity,
            "passed": self.passed,
            "status": self.status,
            "artifacts_total": self.artifacts_total,
            "artifacts_present": self.artifacts_present,
            "artifacts_absent": self.artifacts_absent,
            "present_tools": list(self.present_tools),
            "absent_tools": list(self.absent_tools),
            "failed_tools": list(self.failed_tools),
            "detail": self.detail,
        }


@dataclass(frozen=True)
class OutputCountComparison:
    """One Stage C count comparison, recorded on its own.

    Stage C produces three of these: the parsed ``findings.json`` row count against
    Stage B, the parsed ``findings.csv`` row count against Stage B, and the two
    parsed counts against each other.  The first two are deliberately separate
    assertions -- comparing one and inferring the other would remove the check.
    """

    name: str
    left_label: str
    left: int
    right_label: str
    right: int
    passed: bool
    status: str
    method: str
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable record for ``normalize-run.json``."""
        return {
            "name": self.name,
            "left_label": self.left_label,
            "left": self.left,
            "right_label": self.right_label,
            "right": self.right,
            "passed": self.passed,
            "status": self.status,
            "method": self.method,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class ReconciliationReport:
    """All three stages of one reconciliation, as data.

    ``cli.py`` serialises :meth:`as_dict` into
    ``harness/artifacts/logs/normalize-run.json``, which must carry every
    reconciliation assertion and its result, and renders the per-tool records into
    ``oss-scan-results/tool-status.md``.  Nothing here is printed, and no assertion
    is reduced to a bare boolean.
    """

    stage_a: tuple[ArtifactReconciliation, ...]
    stage_b: DatasetReconciliation
    stage_c: tuple[OutputCountComparison, ...]
    passed: bool
    failures: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        """Return the whole report as JSON-serialisable data."""
        return {
            "passed": self.passed,
            "failures": list(self.failures),
            "stage_a": {
                "description": (
                    "per-artifact identity: raw finding records = dataset rows + "
                    "rejected records, counted by an independent traversal"
                ),
                "artifacts": [record.as_dict() for record in self.stage_a],
            },
            "stage_b": {
                "description": (
                    "dataset-level identity, computed as the sum of the Stage A "
                    "per-artifact identities rather than as an independent recount"
                ),
                "dataset": self.stage_b.as_dict(),
            },
            "stage_c": {
                "description": (
                    "output-file row counts, taken by parsing both files and compared "
                    "against Stage B separately and to each other; never by counting "
                    "lines, because message fields carry embedded newlines"
                ),
                "comparisons": [comparison.as_dict() for comparison in self.stage_c],
            },
        }

    def for_tool(self, tool: str) -> ArtifactReconciliation:
        """Return the Stage A record for ``tool``.

        Provided so ``tool-status.md`` can render each tool's reconciliation from the
        same single measurement, rather than taking a second one.
        """
        for record in self.stage_a:
            if record.tool == tool:
                return record
        raise UnknownToolError(f"no Stage A record for tool {tool!r}")

    def raise_for_failures(self) -> None:
        """Raise :class:`ReconciliationError` if any stage failed.

        A failed reconciliation identity halts the run.  Call this after serialising
        the report, so the evidence is durable before the run stops.  Never repair a
        count to make an identity hold.
        """
        if not self.passed:
            raise ReconciliationError(
                "reconciliation failed: " + "; ".join(self.failures),
                report=self,
            )


def count_json_rows(path: str | Path) -> int:
    """Count the rows in ``findings.json`` by parsing it.

    The dataset file is row-only with no metadata envelope, so the parsed document is
    a JSON array and its length is the row count.

    Args:
        path: Path to ``findings.json``.

    Returns:
        The number of parsed row objects.

    Raises:
        ReconciliationError: If the file cannot be parsed, or if its top level is not
            an array -- a metadata envelope would mean the emitter wrote a shape the
            schema does not permit.
    """
    target = Path(path)
    try:
        with target.open("r", encoding="utf-8") as handle:
            document = json.load(handle)
    except OSError as exc:
        raise ReconciliationError(f"cannot read {target}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ReconciliationError(f"cannot parse {target} as JSON: {exc}") from exc
    if not isinstance(document, list):
        raise ReconciliationError(
            f"{target} must be a row-only JSON array with no metadata envelope, got "
            f"{type(document).__name__}"
        )
    return len(document)


def count_csv_rows(path: str | Path, has_header: bool = True) -> int:
    """Count the data rows in ``findings.csv`` by parsing it.

    Parsed with :mod:`csv`, which is the point: a message field carrying an embedded
    newline spans several physical lines, so counting lines over-reports.  This
    dataset's 9,430 rows span 9,439 physical lines, which is why no count in this
    module ever comes from a line tally.
    The file is opened with ``newline=""`` as :mod:`csv` requires for embedded
    newlines to be read correctly.

    Args:
        path: Path to ``findings.csv``.
        has_header: Whether the first parsed record is the twelve-field header row.
            True for the dataset file.

    Returns:
        The number of parsed data rows, excluding the header and any blank line.

    Raises:
        ReconciliationError: If the file cannot be read or parsed.
    """
    target = Path(path)
    rows = 0
    header_seen = not has_header
    try:
        with target.open("r", encoding="utf-8", newline="") as handle:
            for record in csv.reader(handle):
                if not record:
                    # A blank physical line carries no fields and is not a row.
                    continue
                if not header_seen:
                    header_seen = True
                    continue
                rows += 1
    except OSError as exc:
        raise ReconciliationError(f"cannot read {target}: {exc}") from exc
    except csv.Error as exc:
        raise ReconciliationError(f"cannot parse {target} as CSV: {exc}") from exc
    return rows


def run_stage_a(
    artifact_counts: Iterable[ArtifactCounts | Mapping[str, Any]],
    reject_classes: Iterable[str] | None = None,
    tools: Sequence[str] = CANONICAL_TOOLS,
) -> list[ArtifactReconciliation]:
    """Stage A -- the per-artifact identity, for every one of the nine tools.

    Every tool in ``tools`` appears in the result whether or not it wrote an
    artifact, because ``tool-status.md`` carries an entry for all nine and the
    row-only dataset files cannot show a tool that contributed no row.  A tool the
    caller did not mention at all is treated as absent and takes the
    :data:`NOT_APPLICABLE_ABSENT` verdict.

    Args:
        artifact_counts: One :class:`ArtifactCounts` -- or an equivalent mapping --
            per tool. Order is irrelevant; the output follows ``tools``.
        reject_classes: The rejection-class vocabulary, passed through to
            :func:`validate_rejection_classes`.
        tools: The canonical identifiers to report on. Defaults to all nine.

    Returns:
        One :class:`ArtifactReconciliation` per tool, in ``tools`` order.

    Raises:
        UnknownToolError: If an entry names a non-canonical tool.
        ReconciliationError: If two entries name the same tool -- one artifact per
            tool per run, and a duplicate would double-count it in Stage B.
        ValueError: If an entry is malformed, or claims presence without a traversed
            record count.
    """
    supplied: dict[str, ArtifactCounts] = {}
    for entry in artifact_counts:
        counts = ArtifactCounts.coerce(entry)
        canonical = _require_tool(counts.tool, tools)
        if canonical in supplied:
            raise ReconciliationError(
                f"{canonical}: two artifact-count entries for one tool; the raw tree "
                "holds exactly one artifact per tool, and a duplicate entry would "
                "double-count it"
            )
        supplied[canonical] = counts

    stage_a: list[ArtifactReconciliation] = []
    for tool in tools:
        counts = supplied.get(tool)
        if counts is None or not counts.present:
            if counts is None:
                stage_a.append(reconcile_absent_artifact(tool))
            else:
                if counts.raw_records is not None:
                    raise ValueError(
                        f"{tool}: an absent artifact cannot carry a raw record count "
                        f"({counts.raw_records}); presence is what a traversed count "
                        "means"
                    )
                stage_a.append(
                    reconcile_absent_artifact(
                        tool,
                        emitted_rows=counts.emitted_rows,
                        rejected_records=counts.rejected_records or 0,
                    )
                )
            continue
        if counts.raw_records is None:
            raise ValueError(
                f"{tool}: a present artifact must carry the independently counted "
                "raw_records; pass 0 for an artifact that parsed with no records"
            )
        stage_a.append(
            reconcile_artifact(
                tool,
                counts.raw_records,
                counts.emitted_rows,
                counts.rejected_records,
                rejections_by_class=counts.rejections_by_class,
                reject_classes=reject_classes,
            )
        )
    return stage_a


def run_stage_b(stage_a: Sequence[ArtifactReconciliation]) -> DatasetReconciliation:
    """Stage B -- the dataset identity, as the sum of the Stage A identities.

    The three totals are accumulated from the Stage A records themselves, so this is
    arithmetic over the per-artifact measurements rather than a second traversal of
    the dataset.  The verdict requires both the summed identity to hold **and** every
    per-artifact identity to have held: a dataset-level sum can balance while two
    tools are wrong in opposite directions, and that must not read as a pass.

    Absent artifacts contribute nothing to any total -- they have no counts to
    contribute -- while a present artifact holding zero records contributes a
    genuine zero.

    Args:
        stage_a: The Stage A records, as returned by :func:`run_stage_a`.

    Returns:
        The :class:`DatasetReconciliation` record.
    """
    raw_total = 0
    row_total = 0
    rejected_total = 0
    present: list[str] = []
    absent: list[str] = []
    failed: list[str] = []

    for record in stage_a:
        if not record.artifact_present:
            absent.append(record.tool)
            continue
        present.append(record.tool)
        raw_total += record.raw_records or 0
        row_total += record.emitted_rows or 0
        rejected_total += record.rejected_records or 0
        if record.passed is False:
            failed.append(record.tool)

    identity = f"{raw_total} = {row_total} + {rejected_total}"
    arithmetic_holds = raw_total == row_total + rejected_total
    passed = arithmetic_holds and not failed

    if passed:
        detail = (
            "the sum of the per-artifact identities holds, and every per-artifact "
            "identity held individually"
        )
    elif arithmetic_holds:
        detail = (
            "the summed totals balance but the per-artifact identities did not all "
            f"hold ({', '.join(failed)}); a dataset sum can cancel opposite per-tool "
            "discrepancies, so this is a failure"
        )
    else:
        detail = (
            f"dataset identity failed: {raw_total} raw finding records summed across "
            f"{len(present)} artifacts, but {row_total} rows plus {rejected_total} "
            f"rejections is {row_total + rejected_total}"
        )

    return DatasetReconciliation(
        raw_records=raw_total,
        emitted_rows=row_total,
        rejected_records=rejected_total,
        identity=identity,
        status=STATUS_PASS if passed else STATUS_FAIL,
        passed=passed,
        artifacts_total=len(stage_a),
        artifacts_present=len(present),
        artifacts_absent=len(absent),
        present_tools=tuple(present),
        absent_tools=tuple(absent),
        failed_tools=tuple(failed),
        detail=detail,
    )


def run_stage_c(
    stage_b: DatasetReconciliation,
    json_rows: int | str | Path,
    csv_rows: int | str | Path,
) -> list[OutputCountComparison]:
    """Stage C -- the output files' row counts against Stage B, and each other.

    Three separate comparisons are recorded.  ``findings.json`` against Stage B and
    ``findings.csv`` against Stage B are asserted independently rather than one being
    inferred from the other, and the two parsed counts are then compared directly.
    Both counts are obtained by parsing -- pass a path and this module measures it
    with :func:`count_json_rows` / :func:`count_csv_rows` -- never by counting lines.

    ``emit.py`` owns the field-by-field typed comparison of the two files; this stage
    owns the counts.

    Args:
        stage_b: The dataset-level record whose ``emitted_rows`` is the expectation.
        json_rows: The parsed ``findings.json`` row count, or a path to parse.
        csv_rows: The parsed ``findings.csv`` row count, or a path to parse.

    Returns:
        Three :class:`OutputCountComparison` records, in a fixed order.

    Raises:
        ReconciliationError: If a supplied path cannot be parsed.
        ValueError: If a supplied count is not a non-negative integer.
    """
    json_count, json_method = _resolve_row_count(json_rows, "findings.json", count_json_rows)
    csv_count, csv_method = _resolve_row_count(csv_rows, "findings.csv", count_csv_rows)
    expected = stage_b.emitted_rows

    return [
        _comparison(
            name="findings_json_rows_vs_dataset",
            left_label="parsed findings.json rows",
            left=json_count,
            right_label="Stage B emitted rows",
            right=expected,
            method=json_method,
            on_pass="the parsed JSON row count equals the dataset-level emitted rows",
        ),
        _comparison(
            name="findings_csv_rows_vs_dataset",
            left_label="parsed findings.csv rows",
            left=csv_count,
            right_label="Stage B emitted rows",
            right=expected,
            method=csv_method,
            on_pass=(
                "the parsed CSV row count equals the dataset-level emitted rows, "
                "asserted separately from the JSON comparison rather than inferred "
                "from it"
            ),
        ),
        _comparison(
            name="findings_json_rows_vs_findings_csv_rows",
            left_label="parsed findings.json rows",
            left=json_count,
            right_label="parsed findings.csv rows",
            right=csv_count,
            method=f"{json_method}; {csv_method}",
            on_pass="both output files parse to the same number of rows",
        ),
    ]


def _resolve_row_count(
    value: int | str | Path,
    label: str,
    parser: Any,
) -> tuple[int, str]:
    """Return a row count and a description of how it was obtained.

    An ``int`` is taken as a count the caller already measured by parsing; a path is
    measured here.  Either way the recorded method names the provenance, because a
    count whose provenance is unrecorded is a count nobody can check.
    """
    if isinstance(value, (str, Path)):
        return parser(value), f"parsed {label} at {value}"
    return (
        _require_count(value, f"{label} row count"),
        f"row count supplied by the caller, taken by parsing {label}",
    )


def _comparison(
    *,
    name: str,
    left_label: str,
    left: int,
    right_label: str,
    right: int,
    method: str,
    on_pass: str,
) -> OutputCountComparison:
    """Build one Stage C comparison record."""
    passed = left == right
    return OutputCountComparison(
        name=name,
        left_label=left_label,
        left=left,
        right_label=right_label,
        right=right,
        passed=passed,
        status=STATUS_PASS if passed else STATUS_FAIL,
        method=method,
        detail=(
            on_pass
            if passed
            else f"count mismatch: {left_label} is {left}, {right_label} is {right}"
        ),
    )


def run_three_stage_validation(
    artifact_counts: Iterable[ArtifactCounts | Mapping[str, Any]],
    *,
    json_rows: int | str | Path,
    csv_rows: int | str | Path,
    reject_classes: Iterable[str] | None = None,
    tools: Sequence[str] = CANONICAL_TOOLS,
    raise_on_failure: bool = True,
) -> ReconciliationReport:
    """Establish all three reconciliation stages in one call.

    Stage A asserts the per-artifact identity for every tool, present or absent;
    Stage B sums those identities into the dataset-level assertion; Stage C compares
    the two parsed output-file row counts against Stage B separately and against each
    other.  ``json_rows`` and ``csv_rows`` are keyword-only and have no default, so
    Stage C cannot be skipped by omission.

    Args:
        artifact_counts: One entry per tool that wrote an artifact -- and optionally
            explicit absent entries; any tool not mentioned is reported absent.
        json_rows: The parsed ``findings.json`` row count, or its path.
        csv_rows: The parsed ``findings.csv`` row count, or its path.
        reject_classes: The rejection-class vocabulary to validate class names
            against. ``cli.py`` passes ``paths.REJECT_CLASSES``.
        tools: The canonical identifiers to report on. Defaults to all nine.
        raise_on_failure: Raise on a failed assertion, which is the default because a
            failed reconciliation identity halts the run. Pass ``False`` to obtain the
            report first -- for instance to serialise it into ``normalize-run.json``
            -- and then call :meth:`ReconciliationReport.raise_for_failures`.

    Returns:
        The :class:`ReconciliationReport` for all three stages.

    Raises:
        ReconciliationError: If any assertion failed and ``raise_on_failure`` is
            true. The report is attached as :attr:`ReconciliationError.report` so the
            evidence survives the halt.
        UnknownToolError: If an entry names a non-canonical tool.
        UnknownRejectionClassError: If a rejection class is outside the vocabulary.
    """
    stage_a = run_stage_a(artifact_counts, reject_classes=reject_classes, tools=tools)
    stage_b = run_stage_b(stage_a)
    stage_c = run_stage_c(stage_b, json_rows, csv_rows)

    failures: list[str] = []
    for record in stage_a:
        if record.passed is False:
            failures.append(f"stage A [{record.tool}]: {record.detail}")
    if not stage_b.passed:
        failures.append(f"stage B [dataset]: {stage_b.detail}")
    for comparison in stage_c:
        if not comparison.passed:
            failures.append(f"stage C [{comparison.name}]: {comparison.detail}")

    report = ReconciliationReport(
        stage_a=tuple(stage_a),
        stage_b=stage_b,
        stage_c=tuple(stage_c),
        passed=not failures,
        failures=tuple(failures),
    )
    if raise_on_failure:
        report.raise_for_failures()
    return report
