"""harness/lib/normalize/adapters/sarif.py — the one shared adapter for every SARIF producer.

Serves all three SARIF 2.1.0 producers in this pipeline: ``opengrep``, ``semgrep``
and ``datadog-static-analyzer``.  AAP 0.6.1 specifies *"One shared adapter for all
SARIF producers"* and AAP 0.5.4's per-shape table groups the three as a single row,
so there is deliberately **no** ``opengrep.py``, ``semgrep.py`` or
``datadog_static_analyzer.py``.  ``joern`` is also ``sast`` but writes a native shape
and has its own adapter; it is not served here.

No user-specified rule governs this file; enterprise-standard best practice applies
in its place (AAP 0.7, AAP 0.10.2), held to the AAP's own bar: verification
independent of the thing verified, reject rather than infer, and a policy fixed
before any output is observed.  Everything cited below is an AAP *requirement*;
none of it is a rule.

Position in the normalizer
--------------------------
A leaf that depends on exactly two modules.  AAP 0.6.4: *"each adapter depends on
``paths`` and ``severity`` and on nothing else."*  Taken literally --
:mod:`normalize.shape`, :mod:`normalize.cli`, :mod:`normalize.emit`,
:mod:`normalize.reconcile` and every sibling adapter are **not** imported, and
neither is any third-party package (AAP 0.4.1: standard library only, so this run
introduces no manifest, no lockfile and no install step, which AAP 0.4.3 forbids).

Two consequences are structural rather than stylistic:

* ``reconcile`` is unreachable from here, so the counting traversal that forms the
  left-hand side of ``raw finding records = dataset rows + rejected records`` cannot
  reuse a single line of row-building code.  That is the point: a count taken from
  the traversal that builds the rows satisfies the identity while testing nothing.
* ``emit.FIELDS`` and ``shape.SCANNER_CLASS_BY_TOOL`` cannot be imported, so
  :data:`FIELDS` and :data:`SCANNER_CLASS` below are authored copies that must agree
  with them **by construction**.  ``shape.py`` names an adapter by string key rather
  than importing it, for the same cycle-free reason.

There is no ``__init__.py`` under ``harness/lib/normalize/`` or in this directory,
by design: the package is a PEP 420 implicit namespace package on the pinned
CPython 3.13.7, resolved once ``harness/lib`` is on ``sys.path``.  Imports are
therefore absolute and rooted at the package (``from normalize import paths``),
never a bare sibling import.

Nothing here reads a file, an environment variable or a global, and nothing happens
at import time beyond defining constants.  The document, the root, the runner
metadata, the allowlist and the tally all arrive as arguments, which is what makes
:func:`adapt` callable on an already-parsed fixture with no live filesystem.  This
module writes no file: writing belongs to ``emit.py`` and ``cli.py``.  ``os`` is
imported for :func:`os.fspath` alone -- named directly so that no environment access
is even in scope.

The count unit, and the invariant that rests on it
--------------------------------------------------
The count unit is ``runs[].results[]``: **one result object is one record**, which
is exactly the unit ``reconcile.py``'s independent traversal walks for SARIF.
Every result therefore yields **exactly one outcome -- one row or one rejection,
never both and never neither**.  :func:`_adapt_result` returns a single value of one
of those two types, so the invariant is structural rather than asserted.

The traversal mirrors ``reconcile._count_sarif`` element for element, because a
divergence in what counts as "one record" would break the identity silently while
every individual assertion still passed:

=========================================  ================================
document shape                             contribution
=========================================  ================================
a ``runs`` element that is not an object    nothing (counted, not rejected)
a ``results`` value that is not an array    nothing (counted, not rejected)
an element of ``results``                   exactly one row or one rejection
=========================================  ================================

Document order is preserved in the returned rows, since both output files use it
and ``emit.py`` compares them ordered row by row.

Path resolution is delegated in full to ``paths.py``
----------------------------------------------------
Not one base is resolved here.  The raw ``uri``, the raw ``uriBaseId``, the
enclosing run's ``originalUriBaseIds``, the root and the per-tool
:class:`~normalize.paths.ToolPathBase` are handed to
:func:`normalize.paths.resolve_sarif_location`, and whatever it returns is used.
The chained walk, its cycle and depth guards, the metadata-backed fallback, the
relativization, the ``<container>!<member>`` serialization and the ``in_scope``
matcher all live there.  The facts that make the delegation correct, verified
against primary sources in AAP 0.2.2, are recorded at the call site so that a later
reader does not "simplify" it into a one-level lookup:

* SARIF 2.1.0 section 3.4.4 states a **normative** consumer procedure -- use a value
  the end user configured for the ``uriBaseId`` if one exists, and otherwise resolve
  it from ``run.originalUriBaseIds``;
* bases **chain**: the specification's own section 3.14.14 example carries a
  ``uriBaseId`` on a base entry so that ``SRCROOT`` is expressed relative to
  ``PROJECTROOT``, so reading one level is wrong;
* errata issue 480 (amending section 3.4.3) permits a relative reference to begin
  with a single slash where required to distinguish items in an archive format, so a
  leading slash here is a legitimate in-archive shape rather than a Checkov-style
  scan-root artefact;
* the section 3.10.2 amendment forbids a consumer from normalizing ``..`` segments
  out of a path, so a path landing outside the root keeps its ``../`` segments;
* two producer gaps make the fallback necessary rather than defensive: a
  ``uriBaseId`` emitted with no matching ``originalUriBaseIds`` entry (semgrep issue
  10591) and ``ROOTPATH`` emitted as ``file:///`` inside a git repository (trivy
  issue 10364).

In this provisioning the fallback is the **live** path for two of the three tools
rather than an edge case: ``harness/artifacts/logs/runner-metadata.json`` records
``opengrep`` and ``semgrep`` emitting ``uriBaseId`` ``%SRCROOT%`` with
``originalUriBaseIds`` absent, so the section 3.4.4 procedure cannot complete and
the runner-recorded base is the only one available, while
``datadog-static-analyzer`` emits no ``uriBaseId`` at all.  All three record
``path_base.kind`` ``scan_root`` with an explicit value, which is what makes their
records resolvable rather than rejected -- and AAP 0.5.4 is explicit that the
fallback *"is not a catch-all"*: where the metadata makes no base known, the record
is rejected and counted rather than guessed.

The base is read from the metadata and **never assumed**, which is the whole reason
this module is robust to how the runners were provisioned: whether a runner scans
from the tree or from its ruleset directory, and whether it passes absolute or
root-relative targets, changes the recorded base and nothing here.

Classification order, fixed so a class is reproducible
------------------------------------------------------
A record can be defective in more than one way at once, so the order in which the
checks run decides which class it is counted under.  The order is fixed and
documented rather than incidental:

1. the result is not an object -> ``malformed_record``;
2. the rule.  A result carrying ``ruleId`` and ``ruleIndex`` that name *different*
   rules -> ``malformed_record``, since SARIF 2.1.0 sections 3.27.5 and 3.27.6 make
   the two references to one ``reportingDescriptor``; then, no rule identifier at
   all -> ``missing_rule_id``.  The contradiction is tested first because a record
   whose two identifiers disagree has not established one, and choosing either would
   attach the other rule's severity, CWE and CVE to the row;
3. no message -> ``missing_message`` (a structurally wrong ``message`` ->
   ``malformed_record``);
4. the path -> ``absent_path``, ``invalid_uri``, ``unresolvable_path`` or
   ``malformed_record``, as ``paths.py`` classifies it;
5. a ``start_line`` present that is not a usable line number ->
   ``non_integer_start_line``.

Severity, ``cwe``/``cve`` and ``in_scope`` never reject: each has a defined value
for every input, so a record reaching step 5 becomes a row.

What this module does not do
----------------------------
AAP 0.3.2, in full force.  It performs no cross-tool interpretation of any kind:
one row per finding with the producing tool named, and two tools reporting the same
location produce two rows and no comment.  It judges nothing -- not real, not
important, not a false positive, not a duplicate.  It deduplicates nothing, not even
two identical results from one tool: those are two records and two rows.  It filters
nothing; every record is emitted or rejected, and a row outside the allowlist is
kept with ``in_scope: false`` (AAP 0.9.3).

It also has **no path-discovery logic of its own**, deliberately.  It reads only the
document handed to it.  ``harness/artifacts/logs/taint-ab-{on,off}.sarif`` -- the
Opengrep taint A/B arms -- are valid SARIF that would parse cleanly here, and they
are one of the run's two deliberate second appearances: they write outside
``harness/artifacts/raw/`` and contribute **no** dataset row, since folding one in
*"would corrupt both that tool's count and the dataset's total"* (AAP 0.3.2).  The
guard is that ``cli.py`` only ever passes artifacts from ``harness/artifacts/raw/``,
and this module must never acquire a way to reach further.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from os import fspath
from typing import Any, Final

from normalize import paths
from normalize import severity

__all__ = [
    "ABSENCE_PERMITTED_FIELDS",
    "COUNTER_KEYS",
    "CVE_TOKEN_PATTERN",
    "CWE_TOKEN_PATTERN",
    "FIELDS",
    "SCANNER_CLASS",
    "SUPPORTED_TOOLS",
    "SarifAdapterError",
    "adapt",
    "new_counters",
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class SarifAdapterError(ValueError):
    """Raised where a *caller* hands this adapter something its contract forbids.

    Deliberately distinct from a rejection.  A rejection describes a defective
    *record* inside an artifact and is counted and carried on from; this exception
    describes a defective *call* -- an unknown tool identifier, a relative root, a
    path base belonging to another tool, a document that is not a SARIF envelope --
    and stops the caller rather than being absorbed into a rejection count.

    A ``ValueError`` subclass rather than a bare ``assert``: ``python -O`` strips
    ``assert``, and an invariant that disappears under optimisation is not an
    invariant.  AAP 0.5.4's "reject rather than infer" governs record content; a
    caller fault is neither rejected nor inferred, it is raised.
    """


# --------------------------------------------------------------------------- #
# Fixed policy: the tools served, the scanner class, the twelve fields
# --------------------------------------------------------------------------- #

#: The three canonical tool identifiers whose runners write SARIF 2.1.0.
#:
#: Canonical identifiers are **hyphenated** (``datadog-static-analyzer``) while
#: adapter module filenames are underscored Python identifiers -- a naming split
#: that is deliberate and that ``shape.py``'s routing table carries too.  ``tool``
#: is a required argument to :func:`adapt` precisely because one module serves three
#: tools and must stamp the right identifier into every row.
SUPPORTED_TOOLS: Final[tuple[str, ...]] = (
    "opengrep",
    "semgrep",
    "datadog-static-analyzer",
)

_SUPPORTED_TOOL_SET: Final[frozenset[str]] = frozenset(SUPPORTED_TOOLS)

#: The ``scanner_class`` every row from this adapter carries.
#:
#: AAP 0.5.4's class table fixes ``sast`` for ``opengrep``, ``semgrep``,
#: ``datadog-static-analyzer`` and ``joern``.  It is authored here rather than
#: imported from ``shape.py`` because AAP 0.6.4 permits an adapter to import
#: ``paths`` and ``severity`` and nothing else; ``shape.py`` keeps the same
#: separation from the other direction, naming an adapter by string key rather than
#: importing it.  The duplication is required by the import constraint, not an
#: oversight -- and it is fixed in advance rather than derived from what the
#: artifacts turn out to contain.
SCANNER_CLASS: Final[str] = "sast"

#: The twelve fields, in the request's order (AAP 0.8.2).
#:
#: ``emit.py`` owns ``FIELDS`` as the single authored constant everything downstream
#: keys on, and cannot be imported from here, so this copy must agree with it by
#: construction.  Every row carries all twelve keys in this order, present-with-
#: ``None`` rather than omitted, so the CSV column set is uniform.
FIELDS: Final[tuple[str, ...]] = (
    "tool",
    "scanner_class",
    "rule_id",
    "message",
    "severity_native",
    "severity_norm",
    "path",
    "start_line",
    "cwe",
    "cve",
    "package_coordinate",
    "in_scope",
)

#: Absence is permitted for exactly these five fields and no others (AAP 0.8.2).
#:
#: ``path`` is not among them: AAP 0.5.4 states *"``path`` is not an optional
#: field"*, so a record whose path cannot be resolved is rejected and counted rather
#: than emitted with a null path.  ``severity_norm`` is likewise never absent, which
#: ``severity.py`` enforces on every construction of its result.
ABSENCE_PERMITTED_FIELDS: Final[frozenset[str]] = frozenset(
    {"severity_native", "start_line", "cwe", "cve", "package_coordinate"}
)

#: ``package_coordinate`` is always ``None`` for this shape: AAP 0.5.4's shared-SARIF
#: row ends *"package_coordinate absent"*.  A SARIF result names a code location, not
#: a package, and manufacturing a coordinate from a file path would be inference.
_PACKAGE_COORDINATE: Final[None] = None


# --------------------------------------------------------------------------- #
# SARIF member names.  The three the base resolution turns on are taken from
# paths.py so the two modules cannot spell them differently; the rest are local
# because paths.py has no reason to know them.
# --------------------------------------------------------------------------- #

_RUNS_KEY: Final[str] = "runs"
_RESULTS_KEY: Final[str] = "results"
_TOOL_KEY: Final[str] = "tool"
_DRIVER_KEY: Final[str] = "driver"
_EXTENSIONS_KEY: Final[str] = "extensions"
_RULES_KEY: Final[str] = "rules"
_RULE_KEY: Final[str] = "rule"
_RULE_ID_KEY: Final[str] = "ruleId"
_RULE_INDEX_KEY: Final[str] = "ruleIndex"
_TOOL_COMPONENT_KEY: Final[str] = "toolComponent"
_ID_KEY: Final[str] = "id"
_INDEX_KEY: Final[str] = "index"
_GUID_KEY: Final[str] = "guid"
_NAME_KEY: Final[str] = "name"
_MESSAGE_KEY: Final[str] = "message"
_TEXT_KEY: Final[str] = "text"
_LEVEL_KEY: Final[str] = "level"
_LOCATIONS_KEY: Final[str] = "locations"
_PHYSICAL_LOCATION_KEY: Final[str] = "physicalLocation"
_ARTIFACT_LOCATION_KEY: Final[str] = "artifactLocation"
_REGION_KEY: Final[str] = "region"
_START_LINE_KEY: Final[str] = "startLine"
_PROPERTIES_KEY: Final[str] = "properties"
_SEVERITY_KEY: Final[str] = "severity"
_PROBLEM_KEY: Final[str] = "problem"
_TAGS_KEY: Final[str] = "tags"
_CWE_PROPERTY_KEY: Final[str] = "cwe"

#: The driver component's label, used in a rejection detail and in provenance.
_DRIVER_LABEL: Final[str] = "tool.driver"


# --------------------------------------------------------------------------- #
# CWE and CVE token patterns, compiled once (AAP 0.5.4)
# --------------------------------------------------------------------------- #

# Matched as a whole token rather than as a substring of an unrelated tag.  The
# leading guard rejects an alphanumeric immediately before the prefix, so
# "NOTCWE-79" yields nothing while "external/cwe/cwe-079" and "A03:2021-CWE-79"
# both do -- both are genuine references, and a hyphen or a slash before the prefix
# is how real tag vocabularies compose one.  The trailing guard rejects a following
# digit, so "CWE-791" can never be read as "CWE-79".  Matching is case-insensitive
# because a producer that differs only in case is naming the same weakness; the
# emitted value is canonicalised to the upper-case prefix.
CWE_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?<![0-9A-Za-z])CWE-(\d+)(?![0-9])", re.IGNORECASE
)

# CVE-<4-digit year>-<4-or-more-digit sequence>, per AAP 0.5.4's stated pattern.
CVE_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?<![0-9A-Za-z])CVE-(\d{4})-(\d{4,})(?![0-9])", re.IGNORECASE
)


# --------------------------------------------------------------------------- #
# The counter key set.  Fixed and fully pre-initialised, so every call returns
# the same keys and a caller aggregating across artifacts never has to guess
# whether a missing key means zero or means "this adapter forgot".
# --------------------------------------------------------------------------- #

#: Runs walked, and the two run-level shapes that contribute no record -- counted
#: rather than silent, because ``reconcile.py`` counts them as zero too and a reader
#: comparing the two needs to see that the zero was observed.
COUNTER_RUNS: Final[str] = "runs"
COUNTER_RUNS_SKIPPED_NON_MAPPING: Final[str] = "runs_skipped_non_mapping"
COUNTER_RUNS_WITHOUT_RESULTS_ARRAY: Final[str] = "runs_without_results_array"

#: Records carrying more than one location.  The row takes the first; the record
#: still counts once; this is the number AAP 0.5.4 has reported per tool.
COUNTER_MULTI_LOCATION: Final[str] = "multi_location_records"

#: Records from which more than one distinct CWE or CVE identifier was collected.
#: The field carries one, chosen by ascending numeric identifier.
COUNTER_MULTI_VALUED_CWE: Final[str] = "multi_valued_cwe_records"
COUNTER_MULTI_VALUED_CVE: Final[str] = "multi_valued_cve_records"

#: Rows whose path names something other than a file in the scanned tree -- an
#: archive member or a location outside the root.  ``run-record.md`` reports the
#: count and the proportion (AAP 0.6.1).
COUNTER_NON_FILESYSTEM_PATHS: Final[str] = "non_filesystem_paths"

#: The ``in_scope`` decomposition of the emitted rows.  Their sum is the row count,
#: so this is one measurement split rather than a second count of the same thing.
COUNTER_ROWS_IN_SCOPE: Final[str] = "rows_in_scope"
COUNTER_ROWS_OUT_OF_SCOPE: Final[str] = "rows_out_of_scope"

#: Where each row's rule identifier came from, and what went wrong when the rule
#: metadata could not be reached -- provenance for ``tool-status.md``.
#:
#: :data:`COUNTER_RULE_INDEX_UNUSABLE` covers every way a ``ruleIndex`` could not be
#: applied, and there are now five: a non-integer index, an out-of-range index, an
#: array element that is not an object, an index whose ``toolComponent`` was not
#: resolved, and -- added with the identity check in :func:`_resolve_rule` -- an index
#: resolving to a rule that either declares no ``id`` to compare against or declares
#: one that disagrees with the result's own ``ruleId``.  All five mean the same thing
#: for the row: no property of the indexed rule was read.  The rejection's class and
#: detail are what separate the contradiction from the other four, since only it
#: refuses the record outright.
COUNTER_RULE_ID_FROM_RULE_ID: Final[str] = "rule_id_from_rule_id"
COUNTER_RULE_ID_FROM_RULE_INDEX: Final[str] = "rule_id_from_rule_index"
COUNTER_RULE_INDEX_UNUSABLE: Final[str] = "rule_index_unusable"
COUNTER_TOOL_COMPONENT_UNRESOLVED: Final[str] = "tool_component_reference_unresolved"
COUNTER_RULE_METADATA_FROM_EXTENSION: Final[str] = "rule_metadata_from_extension"
COUNTER_RULE_METADATA_UNRESOLVED: Final[str] = "rule_metadata_unresolved"

#: Which source supplied ``severity_native``.  Two different vocabularies are in
#: play and the two rule-borne sources are not the same field, so recording which
#: one was used is what makes the mapping auditable rather than merely plausible.
#: In this provisioning ``severity_absent`` is the counter that moves for two of the
#: three producers: every opengrep and semgrep result was measured to carry no
#: ``level`` of its own and no rule ``properties`` severity either, and AAP 0.5.4
#: enumerates exactly three field sources for a SARIF result, so those rows state the
#: absence rather than borrowing a literal from a source the AAP does not carry.
COUNTER_SEVERITY_FROM_LEVEL: Final[str] = "severity_from_level"
COUNTER_SEVERITY_FROM_RULE_PROPERTY: Final[str] = "severity_from_rule_property"
COUNTER_SEVERITY_ABSENT: Final[str] = "severity_absent"

#: Rows carrying no ``start_line``.  Absence is permitted for that field, so this is
#: the only way the number is visible.
COUNTER_START_LINE_ABSENT: Final[str] = "start_line_absent"

#: Prefixes for the two vocabularies that are *derived* rather than authored: one
#: key per :data:`normalize.paths.PATH_KINDS` member and one per
#: :data:`normalize.severity.BASIS_VALUES` member.  Deriving them means this
#: adapter's counter set cannot drift from the vocabularies it reports against.
COUNTER_PATH_KIND_PREFIX: Final[str] = "path_kind_"
COUNTER_SEVERITY_BASIS_PREFIX: Final[str] = "severity_basis_"

_AUTHORED_COUNTER_KEYS: Final[tuple[str, ...]] = (
    COUNTER_RUNS,
    COUNTER_RUNS_SKIPPED_NON_MAPPING,
    COUNTER_RUNS_WITHOUT_RESULTS_ARRAY,
    COUNTER_MULTI_LOCATION,
    COUNTER_MULTI_VALUED_CWE,
    COUNTER_MULTI_VALUED_CVE,
    COUNTER_NON_FILESYSTEM_PATHS,
    COUNTER_ROWS_IN_SCOPE,
    COUNTER_ROWS_OUT_OF_SCOPE,
    COUNTER_RULE_ID_FROM_RULE_ID,
    COUNTER_RULE_ID_FROM_RULE_INDEX,
    COUNTER_RULE_INDEX_UNUSABLE,
    COUNTER_TOOL_COMPONENT_UNRESOLVED,
    COUNTER_RULE_METADATA_FROM_EXTENSION,
    COUNTER_RULE_METADATA_UNRESOLVED,
    COUNTER_SEVERITY_FROM_LEVEL,
    COUNTER_SEVERITY_FROM_RULE_PROPERTY,
    COUNTER_SEVERITY_ABSENT,
    COUNTER_START_LINE_ABSENT,
)

#: Every key :func:`new_counters` initialises, in a stable order.
#:
#: Note what is deliberately **absent**: there is no adapter-side count of the
#: records walked, and none of the rows or rejections produced.  ``len(rows)`` and
#: ``len(rejections)`` are returned to the caller directly, and a record count taken
#: from *this* traversal would be an attractive nuisance on the left-hand side of
#: ``raw finding records = dataset rows + rejected records`` -- the one place AAP
#: 0.5.4 requires a genuinely independent traversal, which is
#: ``reconcile.count_records``.  Publishing a plausible substitute for it here is how
#: that requirement would quietly be lost.
COUNTER_KEYS: Final[tuple[str, ...]] = (
    *_AUTHORED_COUNTER_KEYS,
    *(f"{COUNTER_PATH_KIND_PREFIX}{kind}" for kind in paths.PATH_KINDS),
    *(f"{COUNTER_SEVERITY_BASIS_PREFIX}{basis}" for basis in severity.BASIS_VALUES),
)


def new_counters() -> dict[str, int]:
    """Return a fresh counter mapping with every key in :data:`COUNTER_KEYS` at zero.

    Exposed so a caller aggregating several artifacts can start from the same key set
    this adapter returns, rather than accumulating into a dict whose missing keys are
    ambiguous between "zero" and "not measured".
    """
    return {key: 0 for key in COUNTER_KEYS}


# --------------------------------------------------------------------------- #
# JSON shape helpers.
#
# These mirror ``reconcile.py``'s reading of the same document element for
# element, which is what keeps the count unit identical in the two modules.  A
# str, bytes or bytearray is never a JSON array here: ``len()`` over a string
# would count characters as findings.
# --------------------------------------------------------------------------- #


def _is_json_array(value: Any) -> bool:
    """Return whether ``value`` is a JSON array (a non-string sequence)."""
    if isinstance(value, (str, bytes, bytearray)):
        return False
    return isinstance(value, Sequence)


def _json_array(value: Any) -> Sequence[Any]:
    """Return ``value`` where it is a JSON array, else an empty tuple."""
    return value if _is_json_array(value) else ()


def _json_object(value: Any) -> Mapping[str, Any] | None:
    """Return ``value`` where it is a JSON object, else ``None``.

    ``None`` rather than an empty mapping, so a caller can tell "absent or wrong
    type" from "present and empty" and classify the two differently.
    """
    return value if isinstance(value, Mapping) else None


def _non_empty_string(value: Any) -> str | None:
    """Return ``value`` verbatim where it is a string with non-blank content.

    The blank test is on ``strip()`` while the returned value is the original: a
    field is present or it is not, and the content that reaches the dataset is what
    the producer wrote.  Nothing is trimmed, because a message may legitimately
    carry embedded newlines, so a single row can span several physical lines and a
    row count is only ever the parsed row count.
    """
    if isinstance(value, str) and value.strip():
        return value
    return None


def _literal_is_present(value: Any) -> bool:
    """Return whether ``value`` is a severity literal at all.

    Mirrors ``severity.py``'s own reading of an incoming literal: ``None`` is an
    absence, a whitespace-only string is an absence, and any other value -- string or
    not -- is a literal that module will render and either map or disclose.  This is
    a *presence* test only; the mapping is ``severity.py``'s and is never duplicated
    here.
    """
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _type_name(value: Any) -> str:
    """Name ``value``'s type in JSON's vocabulary where there is one.

    Used only in rejection details, which are read by a human looking at the
    artifact -- so ``array`` is more useful there than ``list``.
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, Mapping):
        return "object"
    if isinstance(value, str):
        return "string"
    if isinstance(value, (int, float)):
        return "number"
    if _is_json_array(value):
        return "array"
    return type(value).__name__


# --------------------------------------------------------------------------- #
# The per-run rule table
#
# Built once per run rather than once per result: a run carries thousands of
# results against one rules array, and rebuilding the index per result would turn
# a linear pass into a quadratic one for no gain.
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class _Component:
    """One resolved ``toolComponent`` -- its label for reporting, and its rules."""

    label: str
    rules: tuple[Any, ...]


@dataclass(frozen=True)
class _ComponentLookup:
    """The outcome of resolving a ``toolComponent`` reference.

    ``component`` is ``None`` exactly when ``detail`` is set: a reference that
    cannot be resolved yields no rules and a reason, and the caller then falls back
    to ``ruleId`` before rejecting anything (AAP-derived: resolve defensively, and
    reject only where no identifier can be established at all).
    """

    component: _Component | None
    detail: str | None


class _RuleTable:
    """The rule metadata of one run, indexed the three ways a result can reach it.

    SARIF gives a result three routes to its rule, and a producer may use any of
    them: ``ruleId``; ``ruleIndex`` into a component's ``rules`` array; and the
    ``rule`` reporting-descriptor reference, which carries its own ``id``/``index``
    and may point at an **extension** rather than at the driver.  The extension
    route is why this class exists at all -- an index is scoped to the component it
    came from, so applying an extension's index to the driver's rules would silently
    read the wrong rule's severity, CWE and tags.

    Nothing here rejects.  It answers "which rule object, if any" and says why not
    when the answer is none.
    """

    __slots__ = (
        "_driver",
        "_extensions",
        "_by_guid",
        "_by_name",
        "_by_id",
    )

    def __init__(self, run: Mapping[str, Any]) -> None:
        tool_object = _json_object(run.get(_TOOL_KEY)) or {}
        driver_object = _json_object(tool_object.get(_DRIVER_KEY)) or {}
        self._driver = _Component(
            label=_DRIVER_LABEL, rules=tuple(_json_array(driver_object.get(_RULES_KEY)))
        )

        extensions: list[_Component] = []
        self._by_guid: dict[str, _Component] = {}
        self._by_name: dict[str, _Component] = {}
        self._register(driver_object, self._driver)
        for index, raw in enumerate(_json_array(tool_object.get(_EXTENSIONS_KEY))):
            extension_object = _json_object(raw)
            component = _Component(
                label=f"{_TOOL_KEY}.{_EXTENSIONS_KEY}[{index}]",
                rules=tuple(
                    _json_array(extension_object.get(_RULES_KEY))
                    if extension_object is not None
                    else ()
                ),
            )
            extensions.append(component)
            if extension_object is not None:
                self._register(extension_object, component)
        self._extensions: tuple[_Component, ...] = tuple(extensions)

        # The identifier index, driver first and then each extension in document
        # order, first occurrence winning.  A result carrying only a ``ruleId``
        # reaches its rule's properties through this, which is how the CWE, CVE and
        # rule-property severity of an opengrep or semgrep result are found.
        self._by_id: dict[str, tuple[Mapping[str, Any], _Component]] = {}
        for component in (self._driver, *self._extensions):
            for rule in component.rules:
                rule_object = _json_object(rule)
                if rule_object is None:
                    continue
                identifier = _non_empty_string(rule_object.get(_ID_KEY))
                if identifier is not None and identifier not in self._by_id:
                    self._by_id[identifier] = (rule_object, component)

    def _register(self, component_object: Mapping[str, Any], component: _Component) -> None:
        """Index one component under its ``guid`` and its ``name``, where it has them."""
        guid = _non_empty_string(component_object.get(_GUID_KEY))
        if guid is not None:
            # GUIDs are case-insensitive in practice; a producer that changes the
            # case of its own guid between the reference and the definition would
            # otherwise silently lose its rule metadata.
            self._by_guid.setdefault(guid.lower(), component)
        name = _non_empty_string(component_object.get(_NAME_KEY))
        if name is not None:
            self._by_name.setdefault(name, component)

    @property
    def driver(self) -> _Component:
        """The driver component, which an absent reference means (SARIF default)."""
        return self._driver

    def resolve_component(self, reference: Any) -> _ComponentLookup:
        """Resolve a ``toolComponent`` reference to the component it names.

        An absent reference means the driver, which is SARIF's own default.  A
        reference is resolved by ``index`` into ``run.tool.extensions``, then by
        ``guid``, then by ``name`` -- each of which may name the driver as well as an
        extension.  Anything else yields no component and a reason.
        """
        if reference is None:
            return _ComponentLookup(component=self._driver, detail=None)
        reference_object = _json_object(reference)
        if reference_object is None:
            return _ComponentLookup(
                component=None,
                detail=(
                    f"the {_RULE_KEY}.{_TOOL_COMPONENT_KEY} reference is a "
                    f"{_type_name(reference)}, not an object, so the component its "
                    f"{_INDEX_KEY} is scoped to cannot be established"
                ),
            )

        index = reference_object.get(_INDEX_KEY)
        if index is not None:
            if isinstance(index, bool) or not isinstance(index, int):
                return _ComponentLookup(
                    component=None,
                    detail=(
                        f"the {_TOOL_COMPONENT_KEY} {_INDEX_KEY} {index!r} is a "
                        f"{_type_name(index)}, not an integer"
                    ),
                )
            if 0 <= index < len(self._extensions):
                return _ComponentLookup(component=self._extensions[index], detail=None)
            return _ComponentLookup(
                component=None,
                detail=(
                    f"the {_TOOL_COMPONENT_KEY} {_INDEX_KEY} {index} is out of range "
                    f"for the {len(self._extensions)} entries in "
                    f"{_TOOL_KEY}.{_EXTENSIONS_KEY}"
                ),
            )

        guid = _non_empty_string(reference_object.get(_GUID_KEY))
        if guid is not None:
            component = self._by_guid.get(guid.lower())
            if component is not None:
                return _ComponentLookup(component=component, detail=None)
            return _ComponentLookup(
                component=None,
                detail=(
                    f"no tool component in this run carries the {_GUID_KEY} {guid!r}"
                ),
            )

        name = _non_empty_string(reference_object.get(_NAME_KEY))
        if name is not None:
            component = self._by_name.get(name)
            if component is not None:
                return _ComponentLookup(component=component, detail=None)
            return _ComponentLookup(
                component=None,
                detail=f"no tool component in this run carries the {_NAME_KEY} {name!r}",
            )

        return _ComponentLookup(
            component=None,
            detail=(
                f"the {_TOOL_COMPONENT_KEY} reference carries no {_INDEX_KEY}, "
                f"{_GUID_KEY} or {_NAME_KEY} to resolve it by"
            ),
        )

    def rule_by_index(
        self, component: _Component, index: Any
    ) -> tuple[Mapping[str, Any] | None, str | None]:
        """Return the rule at ``index`` in ``component``, or ``None`` and the reason.

        A non-integer index, an out-of-range index and an array element that is not
        an object are each a distinct reason, reported rather than collapsed.  None
        of them raises: AAP 0.5.4 requires them handled as data, and a caller with no
        other identifier turns the reason into a ``missing_rule_id`` rejection.
        """
        if isinstance(index, bool) or not isinstance(index, int):
            return None, (
                f"{_RULE_INDEX_KEY} {index!r} is a {_type_name(index)}, not an integer"
            )
        if index < 0 or index >= len(component.rules):
            return None, (
                f"{_RULE_INDEX_KEY} {index} is out of range for the "
                f"{len(component.rules)} rules on {component.label}"
            )
        rule_object = _json_object(component.rules[index])
        if rule_object is None:
            return None, (
                f"the rule at {component.label}.{_RULES_KEY}[{index}] is a "
                f"{_type_name(component.rules[index])}, not an object"
            )
        return rule_object, None

    def rule_by_id(self, identifier: str) -> tuple[Mapping[str, Any] | None, _Component | None]:
        """Return the rule declaring ``identifier``, and the component it came from."""
        found = self._by_id.get(identifier)
        if found is None:
            return None, None
        return found



# --------------------------------------------------------------------------- #
# Field extraction (AAP 0.5.4, the shared-SARIF row of the per-shape table)
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class _RuleResolution:
    """What could be established about a result's rule.

    ``rule_id`` is ``None`` exactly when no identifier could be established from
    any route, which is the ``missing_rule_id`` rejection condition.  ``rule`` is
    ``None`` where the identifier is known but the rule *object* is not -- a
    producer that emits ``ruleId`` and no ``rules`` array, which is legitimate and
    simply means there is no ``properties`` to read a severity, CWE or CVE from.
    ``detail`` accumulates every reason encountered, so a rejection can name the
    route that failed rather than merely that resolution failed.

    ``failure`` is set only where the two descriptors a result carried
    *contradict* each other, which is a different outcome from either of the above:
    the record is rejected outright rather than resolved by choosing one of them.
    It carries ``(reject_class, detail)`` ready for :func:`normalize.paths.
    make_rejection`, and the caller honours it **before** testing ``rule_id`` --
    which is ``None`` on that path not because no identifier was found but because
    the identifiers found disagree, and an identity in dispute is not an identity.
    """

    rule_id: str | None
    rule: Mapping[str, Any] | None
    from_rule_index: bool
    component_label: str | None
    detail: str | None
    failure: tuple[str, str] | None = None


def _resolve_rule(
    result: Mapping[str, Any],
    table: _RuleTable,
    counters: dict[str, int],
) -> _RuleResolution:
    """Resolve a result's rule identifier and, where it exists, its rule object.

    ``rule_id`` comes from ``ruleId``; where that is absent it is resolved through
    ``ruleIndex`` into the referenced component's ``rules`` array (AAP 0.5.4).  The
    ``rule`` reporting-descriptor reference is honoured as a third route, and its
    ``toolComponent`` is resolved defensively: where the reference cannot be
    resolved the index is **not** applied to the driver's rules -- an index is scoped
    to its component, and reading the wrong component's rule would silently attach
    another rule's severity and CWE to this row -- so the resolution falls back to
    ``ruleId`` and rejects only if that is absent too.

    Where a result carries **both** descriptors, they are resolved independently and
    then *compared*, and the indexed rule's metadata is read only once the two are
    proven to name the same rule.  SARIF 2.1.0 requires that agreement rather than
    permitting it: sections 3.27.5 (``ruleId``) and 3.27.6 (``ruleIndex``) define the
    two as references to one ``reportingDescriptor``, so where both are present the
    indexed rule's ``id`` *is* the identifier ``ruleId`` states.  A producer that
    emits a disagreeing pair has therefore contradicted the format it declares, and
    accepting the pair would put rule A's identifier in the row's ``rule_id`` while
    rule B's ``properties`` supplied its ``severity_native``, ``cwe`` and ``cve`` --
    a row describing a finding that no rule in the artifact reports (CWE-345).
    Neither descriptor is preferred over the other, because nothing in the artifact
    says which one the producer meant: the record is rejected under
    ``malformed_record`` with both identifiers and the index named in the detail, and
    counted under :data:`COUNTER_RULE_INDEX_UNUSABLE`.

    Two neighbouring shapes are deliberately *not* rejections, and the distinction is
    the substance of the check rather than leniency in it:

    * the ``toolComponent`` reference could not be resolved, so the index was never
      applied to any rules array.  There is no second identifier to disagree with the
      first, so this stays the defensive fall back to ``ruleId`` it already was;
    * the indexed rule resolves but declares no usable ``id`` of its own.  Equality
      then cannot be *proven* either way, so the index is recorded unusable and its
      metadata is not read -- the identifier from ``ruleId`` stands and Route 3 looks
      the declaring rule up by it.  Treating unprovable as equal is exactly the
      silent attachment this check exists to stop; treating it as a contradiction
      would reject a record whose descriptors were never shown to conflict.

    ``malformed_record`` is used rather than a new class because AAP 0.5.4 fixes the
    rejection conditions as a closed list that names *"a malformed record"*, and
    :data:`normalize.paths.REJECT_CLASSES` is that closed set of ten; the class names
    the condition and the detail carries the sub-reason, exactly as it does for the
    ``uriBaseId`` terminal cases.  :data:`COUNTER_RULE_INDEX_UNUSABLE` is likewise the
    existing counter for *"what went wrong when the rule metadata could not be
    reached"*, and an index pointing at another rule is precisely an index that cannot
    be applied.  Both reuses are for the same reason: every committed
    ``expected/*.rows.json`` asserts this adapter's counter vocabulary and the
    rejection-class set key for key in both directions, so a new name in either would
    have to be introduced in the published records at the same moment -- and a count
    or a class that reads differently in the code from how it reads in the records is
    worse than one that shares an established name.

    Over this provisioning's three captured artifacts the comparison is reached 6,832
    times and refuses nothing: ``opengrep`` (1,322 results) and ``semgrep`` (1,162)
    emit ``ruleId`` alone, every one of ``datadog-static-analyzer``'s 6,832 results
    emits both descriptors, and in every one of those the indexed rule's ``id`` is the
    ``ruleId``.  No result in any of the three carries a ``rule`` reporting-descriptor
    reference.  The check therefore moves no dataset row, which is what makes it a
    guard against a future artifact rather than a change of this one.

    Nothing here raises on artifact content: a non-integer or out-of-range index is
    data, and it becomes a reason rather than an exception.
    """
    reasons: list[str] = []

    rule_reference = _json_object(result.get(_RULE_KEY))
    component_reference = (
        rule_reference.get(_TOOL_COMPONENT_KEY) if rule_reference is not None else None
    )
    lookup = table.resolve_component(component_reference)
    if lookup.detail is not None:
        counters[COUNTER_TOOL_COMPONENT_UNRESOLVED] += 1
        reasons.append(lookup.detail)

    # Route 1: ruleId on the result, then id on the rule reference.  The member the
    # identifier actually came from is kept, so a rejection detail can name it rather
    # than say "the identifier" and leave a reader to guess which of the two it was.
    rule_id = _non_empty_string(result.get(_RULE_ID_KEY))
    rule_id_source = _RULE_ID_KEY
    if rule_id is None and rule_reference is not None:
        rule_id = _non_empty_string(rule_reference.get(_ID_KEY))
        rule_id_source = f"{_RULE_KEY}.{_ID_KEY}"

    # Route 2: an index into the resolved component's rules array.
    raw_index = result.get(_RULE_INDEX_KEY)
    index_source = _RULE_INDEX_KEY
    if raw_index is None and rule_reference is not None:
        raw_index = rule_reference.get(_INDEX_KEY)
        index_source = f"{_RULE_KEY}.{_INDEX_KEY}"

    rule_object: Mapping[str, Any] | None = None
    component_label: str | None = None
    from_rule_index = False
    if raw_index is not None:
        if lookup.component is None:
            reasons.append(
                f"{_RULE_INDEX_KEY} {raw_index!r} cannot be applied because the "
                f"component it is scoped to was not resolved"
            )
            counters[COUNTER_RULE_INDEX_UNUSABLE] += 1
        else:
            indexed_rule, reason = table.rule_by_index(lookup.component, raw_index)
            if reason is not None:
                reasons.append(reason)
                counters[COUNTER_RULE_INDEX_UNUSABLE] += 1
            elif rule_id is None:
                # The index is the only route to an identifier, so there is nothing
                # for it to agree or disagree with.  Its rule is the rule.
                rule_object = indexed_rule
                component_label = lookup.component.label
            else:
                # Both descriptors are present.  Prove they name one rule before a
                # single property of the indexed one is read.
                indexed_rule_id = _non_empty_string(
                    indexed_rule.get(_ID_KEY) if indexed_rule is not None else None
                )
                if indexed_rule_id == rule_id:
                    rule_object = indexed_rule
                    component_label = lookup.component.label
                elif indexed_rule_id is None:
                    # Resolvable, but it declares no identifier to compare against,
                    # so equality is unprovable rather than false.  The metadata is
                    # left unread and the identifier from Route 1 stands.
                    reasons.append(
                        f"{index_source} {raw_index} resolves to "
                        f"{lookup.component.label}.{_RULES_KEY}[{raw_index}], which "
                        f"declares no usable {_ID_KEY}, so it cannot be shown to be "
                        f"the rule {rule_id_source} names ({rule_id!r}); its metadata "
                        "is not read"
                    )
                    counters[COUNTER_RULE_INDEX_UNUSABLE] += 1
                else:
                    # The two descriptors contradict each other.  Neither is
                    # preferred: the record is rejected and counted.
                    counters[COUNTER_RULE_INDEX_UNUSABLE] += 1
                    conflict = (
                        f"the result's {rule_id_source} names {rule_id!r} while its "
                        f"{index_source} {raw_index} names "
                        f"{lookup.component.label}.{_RULES_KEY}[{raw_index}], whose "
                        f"{_ID_KEY} is {indexed_rule_id!r}; SARIF 2.1.0 sections "
                        "3.27.5 and 3.27.6 make the two references to one rule, so "
                        "the record contradicts the format it declares and no rule "
                        "metadata is read from either descriptor"
                    )
                    reasons.append(conflict)
                    return _RuleResolution(
                        rule_id=None,
                        rule=None,
                        from_rule_index=False,
                        component_label=None,
                        detail="; ".join(reasons),
                        failure=(paths.REJECT_MALFORMED_RECORD, conflict),
                    )

    if rule_object is not None and rule_id is None:
        rule_id = _non_empty_string(rule_object.get(_ID_KEY))
        if rule_id is not None:
            from_rule_index = True

    # Route 3: the identifier is known but the rule object is not, so index the
    # declaring rule by its id.  This is the ordinary path for a producer that
    # emits ``ruleId`` without a ``ruleIndex``.
    if rule_object is None and rule_id is not None:
        rule_object, component = table.rule_by_id(rule_id)
        if component is not None:
            component_label = component.label

    if rule_object is None:
        counters[COUNTER_RULE_METADATA_UNRESOLVED] += 1
    elif component_label is not None and component_label != _DRIVER_LABEL:
        counters[COUNTER_RULE_METADATA_FROM_EXTENSION] += 1

    if rule_id is not None:
        counters[
            COUNTER_RULE_ID_FROM_RULE_INDEX if from_rule_index else COUNTER_RULE_ID_FROM_RULE_ID
        ] += 1

    return _RuleResolution(
        rule_id=rule_id,
        rule=rule_object,
        from_rule_index=from_rule_index,
        component_label=component_label,
        detail="; ".join(reasons) if reasons else None,
    )


def _rule_properties(rule: Mapping[str, Any] | None) -> Mapping[str, Any] | None:
    """Return a rule's ``properties`` object, or ``None``."""
    if rule is None:
        return None
    return _json_object(rule.get(_PROPERTIES_KEY))


def _message_text(result: Mapping[str, Any]) -> tuple[str | None, tuple[str, str] | None]:
    """Return the result's ``message.text``, or the rejection it earns.

    AAP 0.5.4: ``message`` <- ``message.text``, and an absent or empty one is the
    ``missing_message`` rejection condition.  Where a producer supplies only
    ``message.id`` and ``message.arguments`` with no ``text``, the message is treated
    as absent rather than formatted from the template: rendering a template would be
    manufacturing content the artifact does not contain.

    A ``message`` that is not an object at all is structurally wrong rather than
    merely absent, so it is classified ``malformed_record``.  The two are kept
    distinct because a reader of ``tool-status.md`` acts differently on "this
    producer omits message text" than on "this artifact is not shaped like SARIF".
    """
    raw = result.get(_MESSAGE_KEY)
    if raw is None:
        return None, (
            paths.REJECT_MISSING_MESSAGE,
            f"the result carries no {_MESSAGE_KEY} object",
        )
    message = _json_object(raw)
    if message is None:
        return None, (
            paths.REJECT_MALFORMED_RECORD,
            f"the result's {_MESSAGE_KEY} is a {_type_name(raw)}, not an object",
        )
    text = message.get(_TEXT_KEY)
    resolved = _non_empty_string(text)
    if resolved is None:
        if text is None:
            reason = (
                f"the result's {_MESSAGE_KEY} carries no {_TEXT_KEY}"
                + (
                    "; it supplies only a message template, which is not rendered "
                    "here because rendering one would manufacture content the "
                    "artifact does not carry"
                    if any(key in message for key in ("id", "arguments"))
                    else ""
                )
            )
        elif isinstance(text, str):
            reason = (
                f"the result's {_MESSAGE_KEY}.{_TEXT_KEY} is empty or whitespace only"
            )
        else:
            reason = (
                f"the result's {_MESSAGE_KEY}.{_TEXT_KEY} is a {_type_name(text)}, "
                "not a string"
            )
        return None, (paths.REJECT_MISSING_MESSAGE, reason)
    return resolved, None


def _severity_for(
    result: Mapping[str, Any],
    rule: Mapping[str, Any] | None,
) -> tuple[severity.SeverityResult, str]:
    """Resolve a result's severity, returning the result and the counter to bump.

    Two different vocabularies are in play and they are never mixed (AAP 0.5.4):

    * a SARIF ``level`` maps through ``severity.py``'s SARIF ``level`` table
      (``error`` -> High, ``warning`` -> Medium, ``note`` -> Low, ``none`` -> Info),
      so it is passed as ``sarif_level``;
    * a rule's ``properties.severity`` or ``properties.problem.severity`` maps
      through the case-insensitive native-label table, so it is passed as ``label``.

    Exactly three sources are consulted, in this fixed order, and the first that
    states a literal wins outright:

    1. ``result.level`` -- the level the producer stated on this very result;
    2. ``rule.properties.severity``;
    3. ``rule.properties.problem.severity``.

    That set is not a choice made here.  AAP 0.5.4's per-shape table enumerates the
    field sources for a shared-SARIF record as *"severity_native <- level, or the
    rule's properties.severity/problem.severity where level is absent"*, and the AAP
    is the frozen contract this module is aligned to rather than a document to be
    reinterpreted.  Three sources is therefore the whole of the authorisation.

    ``rule.defaultConfiguration.level`` is deliberately **not** consulted, and the
    omission is the substance of this function rather than an oversight in it.  The
    field exists, SARIF 2.1.0 does describe deriving an omitted ``result.level``
    through it, and mainstream consumers do implement that derivation -- and none of
    that is an authorisation, because AAP 0.5.4 enumerates the field sources for this
    shape and does not carry it.  Reading it would be this module deciding that a
    source the contract omits ought to count, which is exactly the reinterpretation
    the contract forbids; where the specification and the AAP describe different
    behaviour, the AAP is what the code is held to.

    The consequence is measured rather than assumed, and it is deliberate.  Over this
    provisioning's own captured output no opengrep or semgrep result carries a
    ``level`` and no rule of theirs carries ``properties.severity`` or
    ``properties.problem.severity``: what those two artifacts state lives solely in
    ``defaultConfiguration.level``.  Every one of their rows therefore resolves to
    ``severity_native`` ``None`` and ``severity_norm`` ``Info`` on the
    ``no_vocabulary`` basis -- the absence *stated*, which is what AAP 0.5.4's "No
    vocabulary at all" row requires -- and ``severity-map.md`` reports the two tools
    as contributing no native literal.  Datadog's results carry their own ``level``
    and are unaffected.

    Two further surfaces are likewise not consulted, for the same reason and named
    here so the omissions are visible rather than silent: the specification's terminal
    ``warning`` default for a result whose level cannot be derived at all, which would
    manufacture a Medium band for a record nothing in the artifact assigns a severity
    to; and ``run.policies`` / ``invocation.ruleConfigurationOverrides``, the run-time
    override surfaces that can outrank a rule's configuration.  Neither appears in the
    AAP's field sources, and no producer in this pipeline emits either.

    An earlier source outranks a later one even when its literal is unmappable: a
    ``level`` outside the SARIF vocabulary is disclosed as an unmapped literal rather
    than quietly replaced by a rule property, because reaching past a literal the
    producer did state would be inference, and the disclosure is what puts it in
    ``severity-map.md`` with the rows it affected.

    No mapping, banding or defaulting is performed here; ``severity.py`` owns the
    policy and this module only chooses the source and the vocabulary.
    """
    level = result.get(_LEVEL_KEY)
    if _literal_is_present(level):
        return severity.resolve(sarif_level=level), COUNTER_SEVERITY_FROM_LEVEL

    properties = _rule_properties(rule)
    if properties is not None:
        candidate = properties.get(_SEVERITY_KEY)
        if _literal_is_present(candidate):
            return severity.resolve(label=candidate), COUNTER_SEVERITY_FROM_RULE_PROPERTY
        problem = _json_object(properties.get(_PROBLEM_KEY))
        if problem is not None:
            candidate = problem.get(_SEVERITY_KEY)
            if _literal_is_present(candidate):
                return (
                    severity.resolve(label=candidate),
                    COUNTER_SEVERITY_FROM_RULE_PROPERTY,
                )

    return severity.resolve(), COUNTER_SEVERITY_ABSENT


def _identifier_sources(rule: Mapping[str, Any] | None) -> tuple[str, ...]:
    """Return the rule strings CWE and CVE identifiers are collected from.

    AAP 0.5.4 names two sources on the **rule**: ``properties.cwe``, which may be a
    scalar or a list, and ``properties.tags``.  They are returned in that order, so
    a de-duplicated identifier keeps the rendering it first appeared under.

    Only strings are collected.  A bare number under ``properties.cwe`` is not read
    as an identifier: turning ``79`` into ``CWE-79`` would be supplying a prefix the
    artifact never wrote, and AAP 0.5.4's rule is to reject rather than infer.
    """
    properties = _rule_properties(rule)
    if properties is None:
        return ()
    sources: list[str] = []
    raw_cwe = properties.get(_CWE_PROPERTY_KEY)
    if isinstance(raw_cwe, str):
        sources.append(raw_cwe)
    elif _is_json_array(raw_cwe):
        sources.extend(entry for entry in raw_cwe if isinstance(entry, str))
    tags = properties.get(_TAGS_KEY)
    if isinstance(tags, str):
        sources.append(tags)
    elif _is_json_array(tags):
        sources.extend(entry for entry in tags if isinstance(entry, str))
    return tuple(sources)


def _select_cwe(sources: Iterable[str]) -> tuple[str | None, int]:
    """Return the CWE to emit and how many distinct ones were found.

    The ascending-identifier rule (AAP 0.5.4): the field carries **one** value,
    chosen by ascending numeric identifier -- the integer after the ``CWE-`` prefix.
    That ordering is total over the integers, so no tie can arise and no
    producer-order tiebreak is needed.  The emitted value keeps the digits exactly as
    they appeared, including any leading zero, under the canonical upper-case prefix.
    """
    found: dict[int, str] = {}
    for text in sources:
        for match in CWE_TOKEN_PATTERN.finditer(text):
            digits = match.group(1)
            found.setdefault(int(digits), f"CWE-{digits}")
    if not found:
        return None, 0
    return found[min(found)], len(found)


def _select_cve(sources: Iterable[str]) -> tuple[str | None, int]:
    """Return the CVE to emit and how many distinct ones were found.

    The same ascending-identifier rule, ordered by **year then sequence** -- a total
    order over the pair, so again no tiebreak is needed.  The emitted value keeps
    both digit groups as they appeared under the canonical upper-case prefix.
    """
    found: dict[tuple[int, int], str] = {}
    for text in sources:
        for match in CVE_TOKEN_PATTERN.finditer(text):
            year, sequence = match.group(1), match.group(2)
            found.setdefault((int(year), int(sequence)), f"CVE-{year}-{sequence}")
    if not found:
        return None, 0
    return found[min(found)], len(found)


@dataclass(frozen=True)
class _LocationLookup:
    """The first location's ``artifactLocation`` and ``region``, or why neither exists."""

    artifact_location: Mapping[str, Any] | None
    region: Any
    failure: tuple[str, str] | None


def _first_location(result: Mapping[str, Any]) -> _LocationLookup:
    """Return the first location's addressable parts, or the rejection it earns.

    The first-location rule (AAP 0.5.4): where a result carries more than one
    location the row takes the **first**, the record still counts once, and the
    caller counts the record as multi-location for per-tool reporting.

    The failure cases are separated rather than collapsed, because they mean
    different things about the producer.  An absent or empty ``locations`` array and
    a location carrying no ``physicalLocation`` -- a result addressed only by
    logical location -- both name no file, which is ``absent_path``.  A ``locations``
    value that is not an array, or a member of the location chain that is not an
    object, is structurally wrong, which is ``malformed_record``.
    """
    raw_locations = result.get(_LOCATIONS_KEY)
    if raw_locations is None:
        return _LocationLookup(
            None,
            None,
            (
                paths.REJECT_ABSENT_PATH,
                f"the result carries no {_LOCATIONS_KEY} array, so it names no "
                "location; path is not an optional field",
            ),
        )
    if not _is_json_array(raw_locations):
        return _LocationLookup(
            None,
            None,
            (
                paths.REJECT_MALFORMED_RECORD,
                f"the result's {_LOCATIONS_KEY} is a {_type_name(raw_locations)}, "
                "not an array",
            ),
        )
    if not raw_locations:
        return _LocationLookup(
            None,
            None,
            (
                paths.REJECT_ABSENT_PATH,
                f"the result's {_LOCATIONS_KEY} array is empty, so it names no "
                "location; path is not an optional field",
            ),
        )

    first = _json_object(raw_locations[0])
    if first is None:
        return _LocationLookup(
            None,
            None,
            (
                paths.REJECT_MALFORMED_RECORD,
                f"the result's first location is a {_type_name(raw_locations[0])}, "
                "not an object",
            ),
        )

    raw_physical = first.get(_PHYSICAL_LOCATION_KEY)
    if raw_physical is None:
        return _LocationLookup(
            None,
            None,
            (
                paths.REJECT_ABSENT_PATH,
                f"the result's first location carries no {_PHYSICAL_LOCATION_KEY}, "
                "so it addresses no file in the tree",
            ),
        )
    physical = _json_object(raw_physical)
    if physical is None:
        return _LocationLookup(
            None,
            None,
            (
                paths.REJECT_MALFORMED_RECORD,
                f"the first location's {_PHYSICAL_LOCATION_KEY} is a "
                f"{_type_name(raw_physical)}, not an object",
            ),
        )

    raw_artifact = physical.get(_ARTIFACT_LOCATION_KEY)
    if raw_artifact is None:
        return _LocationLookup(
            None,
            None,
            (
                paths.REJECT_ABSENT_PATH,
                f"the first location's {_PHYSICAL_LOCATION_KEY} carries no "
                f"{_ARTIFACT_LOCATION_KEY}, so it names no file",
            ),
        )
    artifact = _json_object(raw_artifact)
    if artifact is None:
        return _LocationLookup(
            None,
            None,
            (
                paths.REJECT_MALFORMED_RECORD,
                f"the first location's {_ARTIFACT_LOCATION_KEY} is a "
                f"{_type_name(raw_artifact)}, not an object",
            ),
        )

    # The region is carried through unvalidated: ``start_line`` is resolved after the
    # path, so that a record defective in both is classified by the path -- the
    # documented order in this module's docstring.
    return _LocationLookup(artifact, physical.get(_REGION_KEY), None)


def _start_line(region: Any) -> tuple[int | None, tuple[str, str] | None]:
    """Return the ``region.startLine`` to emit, or the rejection it earns.

    Absence is permitted for ``start_line`` (AAP 0.8.2), so an absent ``region`` and
    an absent ``startLine`` both yield ``None`` with no rejection.

    A ``startLine`` that is present but not usable as a line number is the
    ``non_integer_start_line`` rejection condition (AAP 0.5.4).  Three shapes reach
    it, each named in the detail: a non-integer type, ``True``/``False`` -- which
    Python's numeric tower would otherwise admit as ``1`` and ``0`` -- and a value
    below ``1``, since SARIF 2.1.0 numbers lines from one and ``0`` is not a line.
    The class is used for all three rather than inventing a name outside
    ``paths.REJECT_CLASSES``, which is a closed set; the detail carries the
    sub-reason, exactly as AAP 0.5.4 does for the ``uriBaseId`` terminal cases.

    A ``region`` that is not an object is structurally wrong rather than an absence,
    so it is ``malformed_record``: silently treating it as "no line information"
    would drop the region of every record in a malformed artifact without a trace.
    """
    if region is None:
        return None, None
    region_object = _json_object(region)
    if region_object is None:
        return None, (
            paths.REJECT_MALFORMED_RECORD,
            f"the first location's {_REGION_KEY} is a {_type_name(region)}, "
            "not an object",
        )
    raw = region_object.get(_START_LINE_KEY)
    if raw is None:
        return None, None
    if isinstance(raw, bool) or not isinstance(raw, int):
        return None, (
            paths.REJECT_NON_INTEGER_START_LINE,
            f"{_REGION_KEY}.{_START_LINE_KEY} is {raw!r}, a {_type_name(raw)} rather "
            "than an integer",
        )
    if raw < 1:
        return None, (
            paths.REJECT_NON_INTEGER_START_LINE,
            f"{_REGION_KEY}.{_START_LINE_KEY} is {raw}, which is not a line number: "
            "SARIF 2.1.0 numbers lines from one",
        )
    return raw, None



# --------------------------------------------------------------------------- #
# Argument validation.
#
# Every one of these raises :class:`SarifAdapterError` rather than returning a
# rejection: a bad argument is a caller fault, and absorbing it into a rejection
# count would let a wrong root or a foreign path base produce a plausible dataset
# for a whole tool.  Each is validated once per call, before any record is read,
# so a fault surfaces on the call rather than on the first record.
# --------------------------------------------------------------------------- #


def _validated_tool(tool: Any) -> str:
    """Return ``tool`` where it is one of the three SARIF producers, else raise."""
    if not isinstance(tool, str):
        raise SarifAdapterError(
            f"tool must be a canonical tool identifier string; observed "
            f"{_type_name(tool)}"
        )
    if tool not in _SUPPORTED_TOOL_SET:
        raise SarifAdapterError(
            f"{tool!r} is not one of the SARIF producers this shared adapter serves "
            f"({', '.join(SUPPORTED_TOOLS)}). One module serves three tools, so the "
            "identifier is required rather than inferred, and a tool with a native "
            "shape has its own adapter -- joern included, which is sast but is not "
            "SARIF."
        )
    return tool


def _validated_root(root: Any) -> str:
    """Return the scan root as an absolute POSIX-normalised string, else raise.

    A :class:`pathlib.Path` and a string are both accepted -- ``os.fspath`` is the
    one thing ``os`` is imported for -- and the result is normalised through
    ``paths.py`` so that this module and every resolver agree on the root's spelling.

    A relative root is refused here rather than at the first record: ``paths.py``
    raises on one because a relative root *"cannot anchor anything, and accepting one
    would produce a plausible-looking wrong answer for every row"*, and the same
    reasoning says to fail on the call.
    """
    try:
        candidate = fspath(root)
    except TypeError as error:
        raise SarifAdapterError(
            f"root must be a str or an os.PathLike naming the SPARK_SRC root; "
            f"observed {_type_name(root)}"
        ) from error
    if isinstance(candidate, bytes):
        raise SarifAdapterError(
            "root must be a text path, not bytes: every path in the dataset is text, "
            "and decoding one here would guess an encoding"
        )
    if not candidate:
        raise SarifAdapterError("root must not be empty")
    normalised = paths.normalise_reported_path(candidate)
    if not paths.is_absolute_path(normalised):
        raise SarifAdapterError(
            f"root must be an absolute path to express a reported path against; "
            f"observed {candidate!r}"
        )
    return normalised


def _validated_tool_base(tool_base: Any, tool: str) -> paths.ToolPathBase:
    """Return ``tool_base`` where it is this tool's recorded path base, else raise.

    The identifier check is not ceremony.  ``tool_base`` is the per-tool view over
    ``harness/artifacts/logs/runner-metadata.json``, and handing this adapter another
    tool's view would resolve every path against the wrong base while every row still
    looked well-formed -- the exact failure AAP 0.5.4 requires *"every base taken
    from the recorded runner metadata"* to prevent.
    """
    if not isinstance(tool_base, paths.ToolPathBase):
        raise SarifAdapterError(
            f"tool_base must be a paths.ToolPathBase built from the runner metadata; "
            f"observed {_type_name(tool_base)}"
        )
    if tool_base.tool != tool:
        raise SarifAdapterError(
            f"tool_base names {tool_base.tool!r} but the artifact is {tool!r}; "
            "resolving one tool's paths against another tool's recorded base would "
            "produce a wrong path for every row of it"
        )
    return tool_base


def _validated_allowlist(allowlist: Any) -> tuple[str, ...]:
    """Return the allowlist globs as a tuple, materialised once, else raise.

    Materialising matters: a generator would be exhausted by the first row and every
    subsequent row would silently take ``in_scope: false``.

    The globs' *content* is not checked against the twelve authoritative ones here.
    ``cli.py`` owns that check -- ``paths.allowlist_matches_authoritative_globs`` --
    and duplicating it would put a second, divergable copy of the scope contract in
    an adapter.  What is checked is that each glob is a non-empty string, since a
    non-string pattern would raise from the matcher on the first row rather than on
    the call.
    """
    if isinstance(allowlist, (str, bytes)):
        raise SarifAdapterError(
            "allowlist must be an iterable of glob strings, not a single string: a "
            "string would be iterated character by character"
        )
    if not isinstance(allowlist, Iterable):
        raise SarifAdapterError(
            f"allowlist must be an iterable of glob strings from "
            f"paths.load_allowlist(); observed {_type_name(allowlist)}"
        )
    globs = tuple(allowlist)
    for index, glob in enumerate(globs):
        if not isinstance(glob, str) or not glob:
            raise SarifAdapterError(
                f"allowlist entry {index} must be a non-empty glob string; observed "
                f"{glob!r}"
            )
    return globs


def _validated_tally(tally: Any) -> Any:
    """Return ``tally`` where it can record a severity result, else raise.

    The capability is checked rather than the class, so a test double is as
    acceptable as a :class:`normalize.severity.LiteralTally`.  ``None`` is not: every
    row's literal has to reach ``severity-map.md``, and a silently skipped tally
    would leave that document under-reporting with nothing to show it had.
    """
    recorder = getattr(tally, "record", None)
    if not callable(recorder):
        raise SarifAdapterError(
            f"tally must expose a callable record(tool, result) -- normally a "
            f"severity.LiteralTally; observed {_type_name(tally)}"
        )
    return tally


def _validated_document(doc: Any) -> Mapping[str, Any]:
    """Return ``doc`` where it is a SARIF envelope this adapter can walk, else raise.

    Two things are required: an object top level, and a ``runs`` array.  Both are
    already guaranteed by ``shape.is_sarif``, which routes here, so neither can fire
    on an artifact that arrived through ``cli.py`` -- they fire on a mis-route or a
    hand-built fixture, which is exactly when a clear message is worth having.

    The ``version`` is deliberately **not** re-checked.  ``shape.py`` owns shape
    detection as the single authority on it (``version == "2.1.0"`` together with a
    ``runs`` array), and a second copy of that test here could disagree with the
    first, which is worse than not testing it twice.  AAP 0.5.4 makes an artifact
    matching no known shape a halt, and ``shape.py`` is where that halt lives.

    Raising rather than returning zero rows is the point: an empty result set is
    indistinguishable from a clean scan, which is the failure mode the mandated
    shape-routing negative test exists to prevent.
    """
    document = _json_object(doc)
    if document is None:
        raise SarifAdapterError(
            f"a SARIF artifact's top level is an object; observed {_type_name(doc)}. "
            "Shape detection belongs to shape.py, which routes an artifact here only "
            "when it carries version 2.1.0 together with a runs array"
        )
    if _RUNS_KEY not in document:
        raise SarifAdapterError(
            f"the document carries no {_RUNS_KEY} array, so it is not the SARIF "
            "envelope this adapter was routed for"
        )
    if not _is_json_array(document.get(_RUNS_KEY)):
        raise SarifAdapterError(
            f"the document's {_RUNS_KEY} is a "
            f"{_type_name(document.get(_RUNS_KEY))}, not an array"
        )
    return document


# --------------------------------------------------------------------------- #
# One result -> exactly one outcome
# --------------------------------------------------------------------------- #


def _adapt_result(
    result: Any,
    *,
    tool: str,
    root: str,
    tool_base: paths.ToolPathBase,
    globs: tuple[str, ...],
    tally: Any,
    table: _RuleTable,
    base_map: Mapping[str, Any] | None,
    run_index: int,
    result_index: int,
    counters: dict[str, int],
) -> dict[str, Any] | paths.Rejection:
    """Return one row **or** one rejection for one ``runs[].results[]`` element.

    Exactly one of the two, always.  The single return value is what makes the
    one-to-one property structural: there is no path through this function that
    emits both and none that emits neither, so
    ``dataset rows + rejected records == the records walked`` holds by construction
    rather than by an assertion that could be forgotten.

    The classification order is the one this module's docstring fixes: shape, rule
    identifier, message, path, then ``start_line``.  Severity, ``cwe``/``cve`` and
    ``in_scope`` cannot reject -- each is defined for every input -- so a record that
    reaches them becomes a row.

    Nothing is caught broadly here.  Each lookup and conversion is guarded where it
    happens, so a genuine programming error propagates instead of being converted
    into a rejection count that would satisfy reconciliation while hiding a defect.
    """
    result_object = _json_object(result)
    if result_object is None:
        return paths.make_rejection(
            paths.REJECT_MALFORMED_RECORD,
            tool,
            f"the {_RESULTS_KEY} element is a {_type_name(result)}, not an object, so "
            "no finding can be read from it",
            run_index=run_index,
            result_index=result_index,
        )

    # ruleId is the ordinary route; where it is absent the identifier is resolved
    # through runs[].tool.driver.rules[] by ruleIndex, scoped to the component that
    # index belongs to (AAP 0.5.4).  A result carrying neither is rejected under
    # missing_rule_id rather than emitted with a null rule identifier.
    rule = _resolve_rule(result_object, table, counters)
    if rule.failure is not None:
        # The result's two rule descriptors contradict each other.  Honoured before
        # the missing-identifier test because the two are different conditions: this
        # record carries identifiers and they disagree, so nothing here chooses one.
        # The record identity therefore names the record's position and *not* a rule
        # identifier -- the adapter carries a rule_id into an identity only where it
        # resolved one, and the whole finding is that neither of these was resolved;
        # both are named in the detail instead, which is where the evidence belongs.
        reject_class, detail = rule.failure
        return paths.make_rejection(
            reject_class,
            tool,
            detail,
            run_index=run_index,
            result_index=result_index,
        )
    if rule.rule_id is None:
        detail = (
            f"the result carries no usable {_RULE_ID_KEY}, and no rule could be "
            f"reached through {_RULE_INDEX_KEY} either"
        )
        if rule.detail is not None:
            detail = f"{detail}: {rule.detail}"
        return paths.make_rejection(
            paths.REJECT_MISSING_RULE_ID,
            tool,
            detail,
            run_index=run_index,
            result_index=result_index,
        )

    # message.text is required: an absent or empty one earns a missing_message
    # rejection rather than an empty string, and a message that is not an object at
    # all is structurally wrong and classified malformed_record.
    message, message_failure = _message_text(result_object)
    if message_failure is not None:
        reject_class, detail = message_failure
        return paths.make_rejection(
            reject_class,
            tool,
            detail,
            run_index=run_index,
            result_index=result_index,
            rule_id=rule.rule_id,
        )

    # The multi-location count is a property of the record, so it is taken whatever
    # the record's outcome turns out to be (AAP 0.5.4: the row takes the first
    # location, the record still counts once, and the number is reported per tool).
    raw_locations = result_object.get(_LOCATIONS_KEY)
    if _is_json_array(raw_locations) and len(raw_locations) > 1:
        counters[COUNTER_MULTI_LOCATION] += 1

    # Every base decision is delegated to paths.py; see this module's docstring for
    # the SARIF 2.1.0 sections and errata that make the delegation correct, and for
    # why reading one level of uriBaseId is wrong.
    location = _first_location(result_object)
    if location.failure is not None:
        reject_class, detail = location.failure
        return paths.make_rejection(
            reject_class,
            tool,
            detail,
            run_index=run_index,
            result_index=result_index,
            rule_id=rule.rule_id,
        )
    artifact_location = location.artifact_location
    # Narrowing for the reader and for a type checker: _first_location returns an
    # artifact_location whenever it returns no failure.  Unreachable while that holds,
    # and raised rather than rejected if it ever stops holding -- a broken internal
    # invariant is a defect in this module, not a defect in the artifact.
    if artifact_location is None:
        raise SarifAdapterError(
            "internal invariant: a location lookup returned neither an "
            "artifactLocation nor a failure"
        )

    resolved = paths.resolve_sarif_location(
        artifact_location.get(paths.SARIF_URI_KEY),
        artifact_location.get(paths.SARIF_URI_BASE_ID_KEY),
        base_map,
        root,
        tool_base,
        tool=tool,
        record_identity={
            "run_index": run_index,
            "result_index": result_index,
            "rule_id": rule.rule_id,
        },
    )
    if isinstance(resolved, paths.Rejection):
        # Returned as-is: paths.py has already named the class and written the
        # sub-reason -- which of the uriBaseId terminal cases it was, or that the
        # metadata supplied no explicit base and the documented fallback therefore
        # does not apply.  Rewording it here would lose that.
        return resolved

    # start_line is optional (AAP 0.8.2), so an absent region or an absent startLine
    # yields None with no rejection; a startLine that is present and not usable as a
    # line number is a non_integer_start_line rejection rather than a null.
    start_line, start_line_failure = _start_line(location.region)
    if start_line_failure is not None:
        reject_class, detail = start_line_failure
        return paths.make_rejection(
            reject_class,
            tool,
            detail,
            run_index=run_index,
            result_index=result_index,
            rule_id=rule.rule_id,
        )
    if start_line is None:
        counters[COUNTER_START_LINE_ABSENT] += 1

    # From here nothing can reject: this record is a row.
    severity_result, severity_counter = _severity_for(result_object, rule.rule)
    counters[severity_counter] += 1
    counters[f"{COUNTER_SEVERITY_BASIS_PREFIX}{severity_result.basis}"] += 1
    # The tally is fed once per emitted row, which is what makes severity-map.md's
    # per-literal counts the row counts it reports them as.  A rejected record
    # contributes no row, so counting one here would put a literal in that document
    # against rows the dataset does not contain.
    tally.record(tool, severity_result)

    identifier_sources = _identifier_sources(rule.rule)
    cwe, cwe_count = _select_cwe(identifier_sources)
    cve, cve_count = _select_cve(identifier_sources)
    if cwe_count > 1:
        counters[COUNTER_MULTI_VALUED_CWE] += 1
    if cve_count > 1:
        counters[COUNTER_MULTI_VALUED_CVE] += 1

    counters[f"{COUNTER_PATH_KIND_PREFIX}{resolved.kind}"] += 1
    if resolved.is_non_filesystem_coordinate:
        counters[COUNTER_NON_FILESYSTEM_PATHS] += 1

    # in_scope is decided by the allowlist alone, through paths.py's matcher, on the
    # resolved path and carrying its kind -- so an archive member cannot match a glob
    # on its segments and the literal src/test exclusion is applied once, where it
    # lives.  Nothing is ever filtered on it: a row outside the allowlist is kept
    # with in_scope false and counted (AAP 0.9.3).
    in_scope = bool(resolved.in_scope(globs))
    counters[COUNTER_ROWS_IN_SCOPE if in_scope else COUNTER_ROWS_OUT_OF_SCOPE] += 1

    row: dict[str, Any] = {
        "tool": tool,
        "scanner_class": SCANNER_CLASS,
        "rule_id": rule.rule_id,
        "message": message,
        "severity_native": severity_result.severity_native,
        "severity_norm": severity_result.severity_norm,
        "path": resolved.path,
        "start_line": start_line,
        "cwe": cwe,
        "cve": cve,
        "package_coordinate": _PACKAGE_COORDINATE,
        "in_scope": in_scope,
    }
    return row


# --------------------------------------------------------------------------- #
# The public entry point
# --------------------------------------------------------------------------- #


def adapt(
    doc: Any,
    *,
    tool: str,
    root: Any,
    tool_base: paths.ToolPathBase,
    allowlist: Iterable[str],
    tally: Any,
) -> tuple[list[dict[str, Any]], list[paths.Rejection], dict[str, int]]:
    """Turn one SARIF 2.1.0 artifact into dataset rows, rejections and counters.

    This is the uniform adapter entry point: every adapter module in this package
    exposes ``adapt`` with this shape, so ``cli.py``'s registry resolves it with
    ``getattr(module, "adapt")`` and every adapter test calls it directly.

    Args:
        doc: The **already-parsed** artifact document -- a mapping for this shape.
            Parsing and shape detection happen upstream, which is what lets a test
            exercise every behaviour on a fixture with no filesystem.
        tool: The canonical tool identifier, one of :data:`SUPPORTED_TOOLS`.
            Required: this one module serves three tools and stamps the identifier
            into every row's ``tool`` field.
        root: The ``SPARK_SRC`` root, as a :class:`pathlib.Path` or a string. Must be
            absolute.
        tool_base: This tool's :class:`normalize.paths.ToolPathBase`, the per-tool
            view over ``harness/artifacts/logs/runner-metadata.json``. Every base
            decision is taken from it and none is assumed.
        allowlist: The twelve authoritative globs, as loaded by
            :func:`normalize.paths.load_allowlist`. Consumed once into a tuple.
        tally: A :class:`normalize.severity.LiteralTally` (or anything exposing
            ``record(tool, result)``), fed once per emitted row so
            ``oss-scan-results/severity-map.md`` can list every observed literal with
            the rows it affected.

    Returns:
        A three-tuple ``(rows, rejections, counters)``:

        * ``rows`` -- a list of dicts, each carrying exactly the twelve fields of
          :data:`FIELDS` in that order, in document order;
        * ``rejections`` -- a list of :class:`normalize.paths.Rejection`, each under a
          named member of :data:`normalize.paths.REJECT_CLASSES` with its sub-reason
          retained verbatim;
        * ``counters`` -- a dict of ints over :data:`COUNTER_KEYS`.

        ``len(rows) + len(rejections)`` equals the number of ``runs[].results[]``
        elements walked, which is the same count unit
        :func:`normalize.reconcile.count_records` arrives at independently.

    Raises:
        SarifAdapterError: If an argument is not what the contract requires -- an
            unknown tool, a relative or non-text root, another tool's path base, a
            non-iterable allowlist, a tally that cannot record, or a document that is
            not a SARIF envelope. A caller fault is raised rather than absorbed into
            a rejection count.
        normalize.severity.SeverityPolicyError: If ``tally`` is a ``LiteralTally``
            and ``tool`` is outside its canonical vocabulary -- which cannot happen
            for the three tools this module serves, and is left to surface rather
            than be caught.

    A tool's exit code is never consulted: a valid artifact is normalized whatever
    its runner returned, since artifact status and exit status are independent
    (AAP 0.5.4). Two of the nine tools exit non-zero precisely because they found
    something.
    """
    canonical_tool = _validated_tool(tool)
    root_text = _validated_root(root)
    base = _validated_tool_base(tool_base, canonical_tool)
    globs = _validated_allowlist(allowlist)
    recorder = _validated_tally(tally)
    document = _validated_document(doc)

    rows: list[dict[str, Any]] = []
    rejections: list[paths.Rejection] = []
    counters = new_counters()

    for run_index, raw_run in enumerate(_json_array(document.get(_RUNS_KEY))):
        counters[COUNTER_RUNS] += 1
        run = _json_object(raw_run)
        if run is None:
            # Contributes no record, exactly as reconcile.py's traversal counts it.
            # Counted rather than passed over in silence, so the two agreeing on zero
            # is visible in normalize-run.json rather than merely assumed.
            counters[COUNTER_RUNS_SKIPPED_NON_MAPPING] += 1
            continue
        raw_results = run.get(_RESULTS_KEY)
        if not _is_json_array(raw_results):
            # A run with no results array, or one that is not an array, contributes
            # nothing and is not an error: an empty run is the ordinary shape of a
            # clean SARIF run.
            counters[COUNTER_RUNS_WITHOUT_RESULTS_ARRAY] += 1
            continue

        # Built once per run: the rules array is per run, and a per-result rebuild
        # would make a linear pass quadratic over an artifact with thousands of
        # results.
        table = _RuleTable(run)
        # The base map is the enclosing run's, handed to paths.py untouched.
        base_map = _json_object(run.get(paths.SARIF_ORIGINAL_URI_BASE_IDS_KEY))

        for result_index, raw_result in enumerate(raw_results):
            outcome = _adapt_result(
                raw_result,
                tool=canonical_tool,
                root=root_text,
                tool_base=base,
                globs=globs,
                tally=recorder,
                table=table,
                base_map=base_map,
                run_index=run_index,
                result_index=result_index,
                counters=counters,
            )
            if isinstance(outcome, paths.Rejection):
                rejections.append(outcome)
            else:
                rows.append(outcome)

    return rows, rejections, counters
