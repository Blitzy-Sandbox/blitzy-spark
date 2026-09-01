"""harness/lib/normalize/adapters/trivy.py — the native-JSON adapter for Trivy 0.74.0.

One adapter per non-SARIF artifact written (AAP 0.6.1), and this is the one for
``harness/artifacts/raw/trivy.json``.  Trivy is *"the only tool whose
``scanner_class`` varies row by row"* (AAP 0.4.1), and that variation is this module's
defining responsibility: the class is taken from the **section array the record was
read from** and never from the record's own content (AAP 0.5.4).

No user-specified rule governs this file; enterprise-standard best practice applies in
its place (AAP 0.7, AAP 0.10.2), held to the AAP's own bar: verification independent of
the thing verified, reject rather than infer, and a policy fixed before any output is
observed.  Everything cited below is an AAP *requirement*; none of it is a rule.

The field list is taken from primary sources rather than from memory: the JSON report
shape at https://trivy.dev/v0.74/docs/configuration/reporting/, the four scanners at
https://trivy.dev/v0.74/docs/scanner/, and the authoritative struct definitions in
``pkg/types/report.go`` at tag ``v0.74.0``
(https://github.com/aquasecurity/trivy/blob/v0.74.0/pkg/types/report.go).

``scanner_class`` is bound by construction, not by inspection
------------------------------------------------------------
A ``Result`` element can carry three finding arrays at once, and a secret and a
misconfiguration have overlapping-looking fields -- both may carry a ``Title``, a
``Severity`` and a line number -- so a content sniff yields plausible-but-wrong classes
that **no reconciliation check would catch**.  The iteration therefore binds the class
at the moment it chooses an array (:data:`SUPPORTED_SECTIONS`) and passes both the
section name and its class down to the record builder.  There is deliberately **no**
module-level ``SCANNER_CLASS`` constant, unlike every sibling adapter: a single constant
here would be the very defect this design exists to prevent.

The count unit, and the invariant that rests on it
--------------------------------------------------
``Results[]`` x one element of ``Vulnerabilities[]``, ``Secrets[]`` or
``Misconfigurations[]``: **one element of one of those three arrays is one record**
(AAP 0.5.4).  That is exactly the unit ``reconcile._count_trivy`` walks independently,
and every branch below mirrors its reading element for element, because a divergence in
what counts as "one record" would break
``raw finding records = dataset rows + rejected records`` silently while every
individual assertion still passed:

=========================================  ================================
document shape                             contribution
=========================================  ================================
``Results`` absent or ``null``              nothing (a legitimate empty report)
``Results`` an empty array                  nothing
a ``Results`` element that is not an object  nothing (counted, not rejected)
a section key absent or ``null``            nothing
an element of a supported section           exactly one row or one rejection
=========================================  ================================

Document order is preserved: ``Results[]`` in order, the three sections in the fixed
order of :data:`SUPPORTED_SECTIONS` within each element, and each section's elements in
order.  Both output files use that order and ``emit.py`` compares them row by row.

The halt this module owns
-------------------------
AAP 0.5.4: *"Trivy's unsupported finding sections are validated empty: version 0.74.0
can also emit ``Licenses`` and ``ExperimentalModifiedFindings``, and any non-empty
finding array outside the three supported sections halts the run with the observed
structure quoted, because otherwise reconciliation passes while real tool output is
silently dropped."*  AAP 0.9.2 lists the same among the conditions that stop the run,
adding *"a Trivy artifact with nothing distinguishing its three supported sections"*.

:func:`validate_finding_sections` runs over **every** ``Results[]`` element before a
single row is built and raises :class:`UnsupportedTrivySection` on four conditions,
each named by its own reason constant so a halt report can act without reading prose:

1. :data:`HALT_UNSUPPORTED_SECTION` -- ``Licenses`` or ``ExperimentalModifiedFindings``
   present and non-empty;
2. :data:`HALT_UNKNOWN_SECTION` -- a key outside the known ``Result`` members whose
   value is a non-empty array containing an object, which is exactly what a future
   Trivy version's new finding section would look like on the way to being dropped;
3. :data:`HALT_SECTION_NOT_AN_ARRAY` -- a supported section present as something other
   than an array or ``null``, so its records cannot be attributed to a section at all;
4. :data:`HALT_DECLARED_FINDINGS_UNHELD` -- ``MisconfSummary.Failures`` above zero with
   no non-empty ``Misconfigurations`` array, the case where the artifact itself declares
   findings that no supported section holds.

Three boundaries keep that halt honest, and each is a deliberate non-halt:

* an **empty** ``Licenses`` or ``ExperimentalModifiedFindings`` -- an empty array, an
  empty object, a scalar or ``null`` -- does not halt: validated empty is the
  requirement, and none of those holds a finding record to drop.  A **non-empty object**
  under either key does halt, because the key's name says it holds findings and an
  object is not a shape this adapter can claim to have read;
* a ``Results`` element carrying **no** finding section and declaring no failures does
  not halt.  Trivy legitimately reports a scanned target with nothing to say, and
  AAP 0.5.4 makes an empty report ordinary rather than defective, so it is counted
  under ``results_without_supported_section`` and contributes nothing;
* ``Packages`` and ``CustomResources`` are non-empty object arrays that are **not**
  finding sections -- ``report.go`` declares the first as the package inventory
  ``--list-all-pkgs`` emits and the second as custom resources -- so dropping them
  drops no finding.  They are counted under ``known_non_finding_object_lists`` so their
  presence is visible rather than silent.

The exception carries the reason, the section key, the ``Target``, the element index and
a **structural** excerpt: each element's keys mapped to their JSON types, never their
values.  That is what "the observed structure" is, and it is also what keeps a secret
out of a log this pipeline preserves verbatim (AAP 0.5.4: *"no adapter carries a secret
value into any field"*).  The exception is never converted into a rejection -- a
rejection is counted and the run continues, which is precisely the outcome AAP 0.5.4
rules out -- and the record loop catches nothing that could swallow it.

Path resolution is delegated in full to ``paths.py``
----------------------------------------------------
Not one base is resolved here.  The enclosing ``Results[].Target``, the section's own
per-record path where the section supplies one, the root and the recorded
:class:`~normalize.paths.ToolPathBase` are handed to
:func:`normalize.paths.resolve_trivy_path`, and whatever it returns is used --
``ResolvedPath`` or ``Rejection``.  Per-section target semantics, relativization, the
``<container>!<member>`` archive serialization and the ``in_scope`` matcher with true
zero-or-more-directories ``**`` semantics all live there (AAP 0.5.4, AAP 0.6.1).

The base is read from the runner metadata and never assumed.  In this provisioning
``run-trivy.sh`` invokes ``trivy fs`` once per scope directory -- the CLI takes exactly
one path -- and its merge step prefixes every ``Target`` with that part's own
``ArtifactName``, so the merged artifact's recorded ``path_base.kind`` is ``scan_root``
and every ``Target`` is root-relative.  The eighteen retained per-directory reports
under ``logs/trivy.parts/`` are **not** root-anchored; a caller reading those passes a
``per_section_target`` base, which is why the base is a parameter rather than a
constant.

Only ``Vulnerabilities[]`` supplies a per-record path (``PkgPath``, the package file
inside the target).  ``DetectedSecret`` and ``DetectedMisconfiguration`` declare no path
field in ``report.go`` -- a misconfiguration's ``CauseMetadata.Resource`` names a
resource, not a file -- so for those two the ``Target`` is the whole coordinate.

Classification order, fixed so a class is reproducible
------------------------------------------------------
A record can be defective in more than one way at once, so the order in which the checks
run decides which class it is counted under.  It is fixed and documented rather than
incidental:

1. the record is not an object -> ``malformed_record``;
2. the section cannot be attributed -> ``unattributable_section``;
3. no rule identifier -> ``missing_rule_id``;
4. no message -> ``missing_message``;
5. the path -> ``absent_path``, ``malformed_record`` or ``unresolvable_path``, as
   ``paths.py`` classifies it;
6. a ``start_line`` present that is not a usable line number ->
   ``non_integer_start_line`` (a ``CauseMetadata`` that is not an object ->
   ``malformed_record``);
7. a dependency-oriented record from which no package coordinate can be formed ->
   ``unformable_package_coordinate``.

Severity, ``cwe``/``cve`` and ``in_scope`` never reject: each has a defined value for
every input, so a record reaching step 7 becomes a row.

Secrets: the redacted match, and nothing else
---------------------------------------------
``message`` for a secret is the record's ``Match``, which Trivy emits **already
redacted**, and it is emitted as-is.  No other field of a secret record is read --
``Code`` carries the surrounding source lines and is never touched -- so no secret value
can reach any dataset field, whatever a future record shape carries (AAP 0.5.4).

Import constraints
------------------
A leaf that depends on exactly two modules.  AAP 0.6.4: *"each adapter depends on
``paths`` and ``severity`` and on nothing else."*  Taken literally --
:mod:`normalize.shape`, :mod:`normalize.cli`, :mod:`normalize.emit`,
:mod:`normalize.reconcile` and every sibling adapter are **not** imported, and neither is
any third-party package (AAP 0.4.1: standard library only, so this run introduces no
manifest, no lockfile and no install step, which AAP 0.4.3 forbids).

Two consequences are structural rather than stylistic.  ``reconcile`` is unreachable
from here, so the counting traversal that forms the left-hand side of the reconciliation
identity cannot reuse a line of row-building code -- which is the point.  And
``emit.FIELDS`` and ``shape.TRIVY_SECTION_SCANNER_CLASS`` cannot be imported, so
:data:`FIELDS`, :data:`SUPPORTED_SECTIONS` and
:data:`UNSUPPORTED_FINDING_SECTIONS` below are authored copies that must agree with them
**by construction**; ``shape.py`` keeps the same separation from the other direction,
naming an adapter by string key rather than importing it.

There is no ``__init__.py`` under ``harness/lib/normalize/`` or in this directory, by
design: the package is a PEP 420 implicit namespace package on the pinned CPython
3.13.7, resolved once ``harness/lib`` is on ``sys.path``.  Imports are therefore absolute
and rooted at the package (``from normalize import paths``), never a bare sibling
import.

Nothing here reads a file, an environment variable or a global, and nothing happens at
import time beyond defining constants.  The document, the root, the runner metadata, the
allowlist and the tally all arrive as arguments, which is what makes :func:`adapt`
callable on an already-parsed fixture with no live filesystem.  This module writes no
file: writing belongs to ``emit.py`` and ``cli.py``.  ``os`` is imported for
:func:`os.fspath` alone -- named directly so that no environment access is even in
scope.

What this module does not do
----------------------------
AAP 0.3.2, in full force.  No cross-tool interpretation of any kind: one row per finding
with the producing tool named, and two tools reporting the same location produce two rows
and no comment.  It judges nothing -- not real, not important, not a false positive, not
a duplicate.  It deduplicates nothing, not even two identical records in one section:
those are two records and two rows.  It filters nothing; every record is emitted or
rejected, and a row outside the allowlist is kept with ``in_scope: false`` and counted
(AAP 0.9.3).  A tool's exit code is never consulted: artifact status and exit status are
independent (AAP 0.5.4), and this runner's code is an aggregate over eighteen
invocations in any case.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from os import fspath
from types import MappingProxyType
from typing import Any, Final

from normalize import paths
from normalize import severity

__all__ = [
    "ABSENCE_PERMITTED_FIELDS",
    "COUNTER_KEYS",
    "CVE_TOKEN_PATTERN",
    "CWE_TOKEN_PATTERN",
    "FIELDS",
    "HALT_DECLARED_FINDINGS_UNHELD",
    "HALT_REASONS",
    "HALT_SECTION_NOT_AN_ARRAY",
    "HALT_UNKNOWN_SECTION",
    "HALT_UNSUPPORTED_SECTION",
    "KNOWN_NON_FINDING_KEYS",
    "RESULT_KNOWN_KEYS",
    "SCANNER_CLASSES",
    "SUPPORTED_SECTIONS",
    "TOOL",
    "TrivyAdapterError",
    "UNSUPPORTED_FINDING_SECTIONS",
    "UnsupportedTrivySection",
    "adapt",
    "adapt_record",
    "new_counters",
    "scanner_class_for_section",
    "validate_finding_sections",
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class TrivyAdapterError(ValueError):
    """Raised where a *caller* hands this adapter something its contract forbids.

    Deliberately distinct from a rejection.  A rejection describes a defective
    *record* inside an artifact and is counted and carried on from; this exception
    describes a defective *call* or a structurally impossible artifact -- an unknown
    tool identifier, a relative root, another tool's path base, a top level that is
    not an object, a ``Results`` member that is present but is not an array -- and
    stops the caller rather than being absorbed into a rejection count.

    A ``ValueError`` subclass rather than a bare ``assert``: ``python -O`` strips
    ``assert``, and an invariant that disappears under optimisation is not an
    invariant.  AAP 0.5.4's "reject rather than infer" governs record content; a
    caller fault is neither rejected nor inferred, it is raised.

    ``Results`` present as something other than an array is included here on purpose.
    Counting it as zero records would agree with ``reconcile._count_trivy`` and
    reconcile cleanly while reporting a malformed artifact as a clean scan -- and an
    empty result set is indistinguishable from a clean scan, which is the failure mode
    the mandated shape-routing negative test exists to prevent.  A ``Results`` key
    that is **null** is a different thing entirely and is not an error: an empty Trivy
    report is ordinary.  A ``Results`` key that is **absent** is likewise not an error
    here, and cannot arrive through routing at all -- ``shape.py``'s Trivy envelope
    requires the key -- so it is reachable only from a direct call.
    """


#: A known finding section this dataset does not support was present and non-empty.
HALT_UNSUPPORTED_SECTION: Final[str] = "unsupported_finding_section"

#: A key outside the known ``Result`` members held a non-empty array of objects: the
#: shape a future Trivy version's new finding section would have on its way to being
#: dropped silently.
HALT_UNKNOWN_SECTION: Final[str] = "unknown_finding_section"

#: A supported section was present as something other than an array or ``null``, so
#: nothing distinguishes the three supported sections for that element (AAP 0.9.2).
HALT_SECTION_NOT_AN_ARRAY: Final[str] = "supported_section_not_an_array"

#: The element's own ``MisconfSummary`` declared failures that no supported section
#: holds: real tool output that would otherwise be dropped while reconciliation passed.
HALT_DECLARED_FINDINGS_UNHELD: Final[str] = (
    "declared_findings_held_by_no_supported_section"
)

#: Every reason :class:`UnsupportedTrivySection` can carry, in the order
#: :func:`validate_finding_sections` checks them.  Named as data so a halt report and
#: the adapter test match on a constant rather than on prose.
HALT_REASONS: Final[tuple[str, ...]] = (
    HALT_UNSUPPORTED_SECTION,
    HALT_UNKNOWN_SECTION,
    HALT_SECTION_NOT_AN_ARRAY,
    HALT_DECLARED_FINDINGS_UNHELD,
)

#: One sentence per reason, for the halt report to quote rather than paraphrase.
_HALT_REASON_SENTENCES: Final[Mapping[str, str]] = MappingProxyType(
    {
        HALT_UNSUPPORTED_SECTION: (
            "a finding section outside the three this dataset supports was present "
            "and non-empty; normalizing the artifact would drop real tool output "
            "while reconciliation still passed"
        ),
        HALT_UNKNOWN_SECTION: (
            "a member outside the known Result fields held a non-empty array of "
            "objects, which is what an unrecognised finding section looks like; it is "
            "treated as one rather than dropped"
        ),
        HALT_SECTION_NOT_AN_ARRAY: (
            "a supported finding section was present as something other than an "
            "array, so no record in it can be attributed to a section and its "
            "scanner_class cannot be established"
        ),
        HALT_DECLARED_FINDINGS_UNHELD: (
            "the element's own MisconfSummary declares failures while no supported "
            "finding section holds them, so the artifact states findings this "
            "adapter can see no records for"
        ),
    }
)


class UnsupportedTrivySection(Exception):
    """The halt AAP 0.5.4 requires, carrying the observed structure it must quote.

    Raised by :func:`validate_finding_sections` and allowed to propagate out of
    :func:`adapt` untouched.  It is **not** a rejection: a rejection is counted under a
    named class and the run continues, which is exactly the outcome AAP 0.5.4 rules out
    here, and AAP 0.9.2 lists this condition among those that stop the run.

    Attributes
    ----------
    reason:
        One of :data:`HALT_REASONS`.
    section:
        The offending member's key, or ``None`` for a reason that is not about one key.
    target:
        The enclosing ``Results[].Target``, so the halt report can name the file.
    result_index:
        The offending element's index in ``Results[]``.
    element_count:
        How many elements the offending array held, or ``None`` where the offending
        value is not an array.
    structure:
        The **structural** excerpt: keys mapped to their JSON types, element counts and
        nothing else.  Values are deliberately excluded -- these logs are preserved
        verbatim and AAP 0.5.4 requires that no adapter carry a secret value into any
        field, so quoting shape rather than content is what keeps the halt report safe
        to publish while still naming what was observed.
    note:
        Any further observation the reason needs, such as the declared failure count.

    ``Exception`` rather than ``ValueError``: this is not a bad argument, it is an
    artifact condition the run is required to stop on, and inheriting from
    :class:`TrivyAdapterError` would let a caller catching argument faults swallow the
    halt.
    """

    def __init__(
        self,
        reason: str,
        *,
        section: str | None,
        target: Any,
        result_index: int,
        element_count: int | None = None,
        structure: Mapping[str, Any] | None = None,
        note: str | None = None,
    ) -> None:
        if reason not in HALT_REASONS:
            raise TrivyAdapterError(
                f"unknown halt reason {reason!r}; the closed set is "
                f"{', '.join(HALT_REASONS)}"
            )
        self.reason: str = reason
        self.section: str | None = section
        self.target: Any = target
        self.result_index: int = result_index
        self.element_count: int | None = element_count
        self.structure: Mapping[str, Any] = MappingProxyType(dict(structure or {}))
        self.note: str | None = note
        self.structure_excerpt: str = _dump_structure(self.structure)
        super().__init__(self._message())

    def _message(self) -> str:
        """Compose the halt message, with the observed structure quoted in it."""
        where = f"Results[{self.result_index}]"
        if self.target is not None:
            where = f"{where} (Target {self.target!r})"
        subject = f" member {self.section!r}" if self.section is not None else ""
        count = (
            f", {self.element_count} element(s)"
            if self.element_count is not None
            else ""
        )
        note = f" {self.note}" if self.note else ""
        return (
            f"{TOOL}: {self.reason}: {where}{subject}{count} -- "
            f"{_HALT_REASON_SENTENCES[self.reason]}.{note} Observed structure "
            f"(keys and JSON types only, never values): {self.structure_excerpt}"
        )

    def as_dict(self) -> dict[str, Any]:
        """Return a plain, JSON-serialisable dict of this halt, for the run record."""
        return {
            "tool": TOOL,
            "reason": self.reason,
            "section": self.section,
            "target": self.target,
            "result_index": self.result_index,
            "element_count": self.element_count,
            "structure": dict(self.structure),
            "note": self.note,
            "message": str(self),
        }


# --------------------------------------------------------------------------- #
# Fixed policy: the tool, the sections, their classes, the twelve fields
# --------------------------------------------------------------------------- #

#: The canonical tool identifier every row from this adapter carries.
#:
#: Produced mechanically from the stem of the runner (``run-trivy.sh``) and its artifact
#: (``trivy.json``) rather than from a product name (AAP 0.5.4).
TOOL: Final[str] = "trivy"

#: Trivy's three supported finding sections, each mapped to the ``scanner_class`` its
#: records take, in the order they are walked within one ``Results[]`` element.
#:
#: This is the *only* place a ``scanner_class`` is decided for this tool, and the
#: mapping is from the **section key** -- never from record content (AAP 0.5.4's class
#: table: *"vuln, secret or misconfig, per record, from the section it was read from"*).
#: It must agree by construction with ``shape.TRIVY_SECTION_SCANNER_CLASS``, which
#: cannot be imported here (AAP 0.6.4).  The insertion order is the iteration order and
#: therefore part of the row order both output files carry.
SUPPORTED_SECTIONS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "Vulnerabilities": "vuln",
        "Secrets": "secret",
        "Misconfigurations": "misconfig",
    }
)

#: The classes a Trivy row may carry, in section order.  Derived so it cannot drift
#: from the mapping above.
SCANNER_CLASSES: Final[tuple[str, ...]] = tuple(SUPPORTED_SECTIONS.values())

#: Finding sections Trivy 0.74.0 can emit that this dataset does not support, from the
#: ``Result`` struct in ``pkg/types/report.go`` at ``v0.74.0``: ``Licenses`` holds
#: detected licences and ``ExperimentalModifiedFindings`` is the JSON name of the
#: ``ModifiedFindings`` field.  Each is validated **empty**; a non-empty one halts
#: (AAP 0.5.4).
UNSUPPORTED_FINDING_SECTIONS: Final[tuple[str, ...]] = (
    "Licenses",
    "ExperimentalModifiedFindings",
)

#: Keys accepted on a ``Results[]`` element without being read as a finding section,
#: so a non-empty one drops no finding.  ``Target``, ``Class`` and ``Type`` are the
#: scalars ``Result`` declares; ``MisconfSummary`` is the pass/fail tally object, whose
#: own members are ``Successes`` and ``Failures``; ``Packages`` (``[]ftypes.Package``)
#: is the package inventory ``--list-all-pkgs`` emits (this runner passes no such flag)
#: and ``CustomResources`` (``[]ftypes.CustomResource``) holds custom resources -- both
#: are non-finding **arrays** of objects, which is why they are named rather than left
#: to trip the unknown-section halt.  ``Layer`` is not a ``Result`` member at
#: ``v0.74.0``: it is declared per finding and per resource, on
#: ``DetectedVulnerability``, ``DetectedMisconfiguration``, ``ftypes.SecretFinding`` and
#: ``ftypes.CustomResource``.  It is accepted here anyway so that a report carrying a
#: layer descriptor at element level is read as the image metadata it is rather than as
#: an unrecognised finding section.
KNOWN_NON_FINDING_KEYS: Final[tuple[str, ...]] = (
    "Target",
    "Class",
    "Type",
    "MisconfSummary",
    "Packages",
    "CustomResources",
    "Layer",
)

#: Every member this adapter recognises on a ``Results[]`` element -- the ``Result``
#: members it reads, plus the per-finding ``Layer`` above.  A key outside this set whose
#: value is a non-empty array of objects is treated as an unrecognised finding section
#: and halts (AAP 0.5.4).
RESULT_KNOWN_KEYS: Final[frozenset[str]] = frozenset(
    (*SUPPORTED_SECTIONS, *UNSUPPORTED_FINDING_SECTIONS, *KNOWN_NON_FINDING_KEYS)
)

#: The two ``Result`` members that are object arrays without being finding sections.
#: Counted when non-empty, so their presence is visible rather than silent.
_NON_FINDING_OBJECT_LIST_KEYS: Final[tuple[str, ...]] = ("Packages", "CustomResources")

#: The twelve fields, in the request's order (AAP 0.8.2).
#:
#: ``emit.py`` owns ``FIELDS`` as the single authored constant everything downstream
#: keys on, and cannot be imported from here, so this copy must agree with it by
#: construction.  Every row carries all twelve keys in this order, present-with-``None``
#: rather than omitted, so the CSV column set is uniform.
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
#: ``path`` is not among them: AAP 0.5.4 states *"``path`` is not an optional field"*,
#: so a record whose path cannot be resolved is rejected and counted rather than emitted
#: with a null path.  ``severity_norm`` is likewise never absent, which ``severity.py``
#: enforces on every construction of its result.
ABSENCE_PERMITTED_FIELDS: Final[frozenset[str]] = frozenset(
    {"severity_native", "start_line", "cwe", "cve", "package_coordinate"}
)



# --------------------------------------------------------------------------- #
# Trivy member names at ``v0.74.0``, named as constants so a typo is a NameError at
# import rather than a silently absent field.  They are declared across several
# structures rather than one, and which structure declares which member is what a
# claim about the report shape has to be checked against: ``Results`` belongs to
# ``Report``, while ``Target``, ``Class``, ``Type``, ``Packages``, ``MisconfSummary``
# and ``Misconfigurations`` belong to ``Result`` (``pkg/types/report.go``);
# ``Failures``, with ``Successes``, belongs to ``MisconfSummary``; the vulnerability
# members belong to ``DetectedVulnerability`` (``pkg/types/vulnerability.go``), which
# reaches ``Title``, ``Description``, ``Severity``, ``CweIDs`` and ``CVSS`` through the
# embedded ``trivy-db`` ``types.Vulnerability``; the secret members belong to
# ``DetectedSecret``, an alias of ``ftypes.SecretFinding``
# (``pkg/fanal/types/secret.go``); the misconfiguration members belong to
# ``DetectedMisconfiguration`` (``pkg/types/misconfiguration.go``), with ``StartLine``
# and ``Occurrences`` inside ``ftypes.CauseMetadata`` (``pkg/fanal/types/misconf.go``);
# and ``PURL`` belongs to ``ftypes.PkgIdentifier``, nested inside a package's
# ``Identifier`` or a vulnerability's ``PkgIdentifier``, alongside ``ftypes.Package``'s
# own ``ID``, ``Name`` and ``Version`` (``pkg/fanal/types/package.go``).
# --------------------------------------------------------------------------- #

_RESULTS_KEY: Final[str] = "Results"
_TARGET_KEY: Final[str] = "Target"
_CLASS_KEY: Final[str] = "Class"
_TYPE_KEY: Final[str] = "Type"
_PACKAGES_KEY: Final[str] = "Packages"
_MISCONF_SUMMARY_KEY: Final[str] = "MisconfSummary"
_FAILURES_KEY: Final[str] = "Failures"
_MISCONFIGURATIONS_KEY: Final[str] = "Misconfigurations"

_VULNERABILITY_ID_KEY: Final[str] = "VulnerabilityID"
_RULE_ID_KEY: Final[str] = "RuleID"
_ID_KEY: Final[str] = "ID"
_TITLE_KEY: Final[str] = "Title"
_DESCRIPTION_KEY: Final[str] = "Description"
_MATCH_KEY: Final[str] = "Match"
_SEVERITY_KEY: Final[str] = "Severity"
_CVSS_KEY: Final[str] = "CVSS"
_CWE_IDS_KEY: Final[str] = "CweIDs"
_START_LINE_KEY: Final[str] = "StartLine"
_CAUSE_METADATA_KEY: Final[str] = "CauseMetadata"
_OCCURRENCES_KEY: Final[str] = "Occurrences"

_PKG_IDENTIFIER_KEY: Final[str] = "PkgIdentifier"
_PURL_KEY: Final[str] = "PURL"
_PKG_ID_KEY: Final[str] = "PkgID"
_PKG_NAME_KEY: Final[str] = "PkgName"
_PKG_PATH_KEY: Final[str] = "PkgPath"
_INSTALLED_VERSION_KEY: Final[str] = "InstalledVersion"
_PACKAGE_ID_KEY: Final[str] = "ID"
_PACKAGE_NAME_KEY: Final[str] = "Name"
_PACKAGE_VERSION_KEY: Final[str] = "Version"

#: Where a package object carries its own identifier object.  ``ftypes.Package`` names
#: the field ``Identifier``; ``DetectedVulnerability`` names its own
#: ``PkgIdentifier``.  Both are checked so the enclosing-package level of the
#: coordinate precedence works on either spelling.
_PACKAGE_IDENTIFIER_KEYS: Final[tuple[str, ...]] = ("Identifier", "PkgIdentifier")

#: The rule-identifier field, per section (AAP 0.5.4: *"rule_id <- VulnerabilityID,
#: RuleID or ID by section"*).  Read by section rather than by trying all three,
#: because trying all three is content sniffing by another name.
_SECTION_RULE_ID_FIELD: Final[Mapping[str, str]] = MappingProxyType(
    {
        "Vulnerabilities": _VULNERABILITY_ID_KEY,
        "Secrets": _RULE_ID_KEY,
        "Misconfigurations": _ID_KEY,
    }
)

#: The message fields, in preference order, per section.
#:
#: AAP 0.5.4: *"message <- Title or Description, or the redacted Match for a secret"*.
#: ``Title`` is preferred over ``Description`` for a vulnerability and a
#: misconfiguration because it is the finding's name while ``Description`` is prose
#: about the class of problem; for a **secret** the redacted ``Match`` comes first,
#: which is the clause the AAP states for that section, with ``Title`` and
#: ``Description`` behind it only so that a secret record lacking a ``Match`` still
#: carries the words the tool did write instead of being rejected.
#:
#: ``Message`` is deliberately **not** consulted for a misconfiguration: it carries a
#: remediation instruction rather than the finding's message, and AAP 0.5.4 names
#: ``Title`` and ``Description`` as the sources.
#: For a secret, no field beyond these three is read at all -- ``Code`` carries the
#: surrounding source lines -- so no secret value can reach a dataset field.
_SECTION_MESSAGE_FIELDS: Final[Mapping[str, tuple[str, ...]]] = MappingProxyType(
    {
        "Vulnerabilities": (_TITLE_KEY, _DESCRIPTION_KEY),
        "Secrets": (_MATCH_KEY, _TITLE_KEY, _DESCRIPTION_KEY),
        "Misconfigurations": (_TITLE_KEY, _DESCRIPTION_KEY),
    }
)

#: Where a section states its start line, as a sequence of key paths tried in order.
#:
#: AAP 0.5.4 and AAP 0.2.2: *"Line information appears on secrets and
#: misconfigurations rather than on vulnerabilities, which is why ``start_line`` is
#: section-dependent."*  A secret carries ``StartLine`` directly
#: (``ftypes.SecretFinding``); a misconfiguration carries it inside
#: ``CauseMetadata`` (``ftypes.CauseMetadata``), with the top level checked first so a
#: record that states one there is still read.  A vulnerability declares no line
#: information at all, and its entry is present only so that a record unexpectedly
#: carrying a ``StartLine`` is validated rather than ignored -- absence there is
#: normal and is never a rejection.
_SECTION_START_LINE_PATHS: Final[Mapping[str, tuple[tuple[str, ...], ...]]] = (
    MappingProxyType(
        {
            "Vulnerabilities": ((_START_LINE_KEY,),),
            "Secrets": ((_START_LINE_KEY,),),
            "Misconfigurations": (
                (_START_LINE_KEY,),
                (_CAUSE_METADATA_KEY, _START_LINE_KEY),
            ),
        }
    )
)

#: The per-record path field, per section, handed to ``paths.resolve_trivy_path`` as a
#: refinement of the enclosing ``Target`` (AAP 0.5.4: *"refined by a per-record path
#: where the section supplies one"*).  Only ``DetectedVulnerability`` declares one --
#: ``PkgPath``, the package file inside the target.  ``None`` for the other two, whose
#: structs declare no path field: a misconfiguration's ``CauseMetadata.Resource`` names
#: a resource rather than a file, and reading it as a path would be inference.
_SECTION_PER_RECORD_PATH_FIELD: Final[Mapping[str, str | None]] = MappingProxyType(
    {
        "Vulnerabilities": _PKG_PATH_KEY,
        "Secrets": None,
        "Misconfigurations": None,
    }
)

#: The sections whose records are dependency-oriented, so that a record from which no
#: package coordinate can be formed is a rejection rather than an absent field
#: (AAP 0.5.4).  A secret and a misconfiguration are findings about a file, not about a
#: package, so the field is legitimately absent for both.
_DEPENDENCY_ORIENTED_SECTIONS: Final[frozenset[str]] = frozenset({"Vulnerabilities"})


# --------------------------------------------------------------------------- #
# CWE and CVE token patterns, compiled once (AAP 0.5.4)
# --------------------------------------------------------------------------- #

# Matched as whole tokens rather than as substrings of an unrelated identifier, with
# the same guards the SARIF adapter uses so the two cannot disagree on what a token is:
# the leading guard rejects an alphanumeric immediately before the prefix, so
# "NOTCWE-79" yields nothing, and the trailing guard rejects a following digit, so
# "CWE-791" can never be read as "CWE-79".  Matching is case-insensitive because a
# producer that differs only in case is naming the same weakness; the emitted value is
# canonicalised to the upper-case prefix.
CWE_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?<![0-9A-Za-z])CWE-(\d+)(?![0-9])", re.IGNORECASE
)

# CVE-<4-digit year>-<4-or-more-digit sequence>, per AAP 0.5.4's stated pattern.  Trivy
# also emits GHSA-, DLA-, DSA-, RUSTSEC- and vendor identifiers under the same
# ``VulnerabilityID`` field; none of those is a CVE, so each stays in ``rule_id`` and
# leaves ``cve`` absent rather than being reshaped into one.
CVE_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?<![0-9A-Za-z])CVE-(\d{4})-(\d{4,})(?![0-9])", re.IGNORECASE
)


# --------------------------------------------------------------------------- #
# The counter key set.  Fixed and fully pre-initialised, so every call returns the
# same keys and a caller aggregating across artifacts never has to guess whether a
# missing key means zero or means "this adapter forgot".
# --------------------------------------------------------------------------- #

#: ``Results[]`` elements walked, and the shapes among them that contribute no record --
#: counted rather than silent, because ``reconcile.py`` counts them as zero too and a
#: reader comparing the two needs to see that the zero was observed.
COUNTER_RESULTS: Final[str] = "results"
COUNTER_RESULTS_SKIPPED_NON_MAPPING: Final[str] = "results_skipped_non_mapping"
COUNTER_RESULTS_WITHOUT_SUPPORTED_SECTION: Final[str] = (
    "results_without_supported_section"
)

#: Set to 1 where the document carries a null ``Results`` member, or none at all: a
#: legitimate empty report, recorded so "nothing to normalize" is visible as an
#: observation rather than inferred from a zero row count.  Through routing only the
#: null form reaches here -- ``shape.py``'s Trivy envelope requires the key to be
#: present -- so a 1 from an absent key means the adapter was called directly.
COUNTER_RESULTS_ABSENT: Final[str] = "results_absent_or_null"

#: Non-empty ``Packages``/``CustomResources`` arrays observed.  Object arrays that are
#: not finding sections, so they neither halt nor contribute a record; counted so their
#: presence is never silent.
COUNTER_KNOWN_NON_FINDING_OBJECT_LISTS: Final[str] = "known_non_finding_object_lists"

#: ``MisconfSummary`` present but not an object, so its declared failure count could
#: not be read.  It holds no findings, so this is an observation rather than a halt.
COUNTER_MISCONF_SUMMARY_UNREADABLE: Final[str] = "misconf_summary_unreadable"

#: Records carrying more than one location.  For Trivy that is a misconfiguration whose
#: ``CauseMetadata.Occurrences`` holds more than one entry.  The row takes the first
#: location; the record still counts once; this is the number AAP 0.5.4 has reported
#: per tool.
COUNTER_MULTI_LOCATION: Final[str] = "multi_location_records"

#: Records from which more than one distinct CWE or CVE identifier was collected.  The
#: field carries one, chosen by ascending numeric identifier.
COUNTER_MULTI_VALUED_CWE: Final[str] = "multi_valued_cwe_records"
COUNTER_MULTI_VALUED_CVE: Final[str] = "multi_valued_cve_records"

#: Rows whose path names something other than a file in the scanned tree -- an archive
#: member or a location outside the root.  ``run-record.md`` reports the count and the
#: proportion (AAP 0.6.1).
COUNTER_NON_FILESYSTEM_PATHS: Final[str] = "non_filesystem_paths"

#: The ``in_scope`` decomposition of the emitted rows.  Their sum is the row count, so
#: this is one measurement split rather than a second count of the same thing.
COUNTER_ROWS_IN_SCOPE: Final[str] = "rows_in_scope"
COUNTER_ROWS_OUT_OF_SCOPE: Final[str] = "rows_out_of_scope"

#: Rows whose ``Target`` was refined by a per-record path, so the refinement is
#: auditable rather than invisible.
COUNTER_PER_RECORD_PATH_REFINEMENTS: Final[str] = "per_record_path_refinements"

#: What severity vocabulary each emitted row's record actually carried.  ``severity.py``
#: owns the precedence and reports the basis it acted on; these three record what was
#: *available*, which is what makes "the label governed and the score was not consulted"
#: checkable rather than assumed (AAP 0.5.4).
COUNTER_SEVERITY_LABEL_PRESENT: Final[str] = "severity_label_present"
COUNTER_SEVERITY_CVSS_ENTRIES_PRESENT: Final[str] = "severity_cvss_entries_present"
COUNTER_SEVERITY_ABSENT: Final[str] = "severity_absent"

#: Rows carrying no ``start_line``.  Absence is permitted for that field, so this is
#: the only way the number is visible.  The second counter is the Go zero value read as
#: an absence rather than as line zero -- see :func:`_start_line`.
COUNTER_START_LINE_ABSENT: Final[str] = "start_line_absent"
COUNTER_START_LINE_ZERO: Final[str] = "start_line_zero_read_as_absent"

#: Which level of the shared package-coordinate precedence supplied the field, and how
#: often none could (AAP 0.5.4).  A dependency-oriented record with none is a rejection
#: rather than a row, so the last counter is the non-dependency-oriented case.
COUNTER_COORDINATE_RECORD_PURL: Final[str] = "package_coordinate_from_record_purl"
COUNTER_COORDINATE_PACKAGE_PURL: Final[str] = (
    "package_coordinate_from_enclosing_package_purl"
)
COUNTER_COORDINATE_RECORD_FIELDS: Final[str] = "package_coordinate_from_record_fields"
COUNTER_COORDINATE_PACKAGE_FIELDS: Final[str] = (
    "package_coordinate_from_enclosing_package_fields"
)
COUNTER_COORDINATE_ABSENT: Final[str] = "package_coordinate_absent"

#: Prefixes for the four vocabularies that are *derived* rather than authored: one key
#: per supported section, one per ``scanner_class``, one per
#: :data:`normalize.paths.PATH_KINDS` member and one per
#: :data:`normalize.severity.BASIS_VALUES` member.  Deriving them means this adapter's
#: counter set cannot drift from the vocabularies it reports against.
COUNTER_RECORDS_PREFIX: Final[str] = "records_"
COUNTER_ROWS_CLASS_PREFIX: Final[str] = "rows_class_"
COUNTER_PATH_KIND_PREFIX: Final[str] = "path_kind_"
COUNTER_SEVERITY_BASIS_PREFIX: Final[str] = "severity_basis_"

_AUTHORED_COUNTER_KEYS: Final[tuple[str, ...]] = (
    COUNTER_RESULTS,
    COUNTER_RESULTS_SKIPPED_NON_MAPPING,
    COUNTER_RESULTS_WITHOUT_SUPPORTED_SECTION,
    COUNTER_RESULTS_ABSENT,
    COUNTER_KNOWN_NON_FINDING_OBJECT_LISTS,
    COUNTER_MISCONF_SUMMARY_UNREADABLE,
    COUNTER_MULTI_LOCATION,
    COUNTER_MULTI_VALUED_CWE,
    COUNTER_MULTI_VALUED_CVE,
    COUNTER_NON_FILESYSTEM_PATHS,
    COUNTER_ROWS_IN_SCOPE,
    COUNTER_ROWS_OUT_OF_SCOPE,
    COUNTER_PER_RECORD_PATH_REFINEMENTS,
    COUNTER_SEVERITY_LABEL_PRESENT,
    COUNTER_SEVERITY_CVSS_ENTRIES_PRESENT,
    COUNTER_SEVERITY_ABSENT,
    COUNTER_START_LINE_ABSENT,
    COUNTER_START_LINE_ZERO,
    COUNTER_COORDINATE_RECORD_PURL,
    COUNTER_COORDINATE_PACKAGE_PURL,
    COUNTER_COORDINATE_RECORD_FIELDS,
    COUNTER_COORDINATE_PACKAGE_FIELDS,
    COUNTER_COORDINATE_ABSENT,
)

#: Every key :func:`new_counters` initialises, in a stable order.
#:
#: Note what is deliberately **absent**: there is no adapter-side count of the records
#: walked, and none of the rows or rejections produced.  ``len(rows)`` and
#: ``len(rejections)`` are returned to the caller directly, and a record count taken
#: from *this* traversal would be an attractive nuisance on the left-hand side of
#: ``raw finding records = dataset rows + rejected records`` -- the one place AAP 0.5.4
#: requires a genuinely independent traversal, which is ``reconcile.count_records``.
#: The per-section ``records_*`` keys are a *decomposition* of this traversal for
#: reporting, never a substitute for that independent count.
COUNTER_KEYS: Final[tuple[str, ...]] = (
    *_AUTHORED_COUNTER_KEYS,
    *(f"{COUNTER_RECORDS_PREFIX}{section.lower()}" for section in SUPPORTED_SECTIONS),
    *(f"{COUNTER_ROWS_CLASS_PREFIX}{klass}" for klass in SCANNER_CLASSES),
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


def scanner_class_for_section(section: Any) -> str | None:
    """Return the ``scanner_class`` records of ``section`` take, or ``None``.

    The single seam through which a class is established, so there is exactly one place
    to read when checking that the class comes from the section (AAP 0.5.4).  ``None``
    for anything outside :data:`SUPPORTED_SECTIONS`, which :func:`adapt_record` turns
    into an ``unattributable_section`` rejection rather than a guess.
    """
    if not isinstance(section, str):
        return None
    return SUPPORTED_SECTIONS.get(section)



# --------------------------------------------------------------------------- #
# JSON shape helpers.
#
# These mirror ``reconcile.py``'s reading of the same document element for element,
# which is what keeps the count unit identical in the two modules.  A str, bytes or
# bytearray is never a JSON array here: ``len()`` over a string would count characters
# as findings.
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

    ``None`` rather than an empty mapping, so a caller can tell "absent or wrong type"
    from "present and empty" and classify the two differently.
    """
    return value if isinstance(value, Mapping) else None


def _non_empty_string(value: Any) -> str | None:
    """Return ``value`` verbatim where it is a string with non-blank content.

    The blank test is on ``strip()`` while the returned value is the original: a field
    is present or it is not, and the content that reaches the dataset is what the
    producer wrote.  Nothing is trimmed, because a message may legitimately carry
    embedded newlines, so a single row can span several physical lines and a row count
    is only ever the parsed row count.
    """
    if isinstance(value, str) and value.strip():
        return value
    return None


def _literal_is_present(value: Any) -> bool:
    """Return whether ``value`` is a severity literal at all.

    Mirrors ``severity.py``'s own reading of an incoming literal: ``None`` is an
    absence, a whitespace-only string is an absence, and any other value -- string or
    not -- is a literal that module will render and either map or disclose.  A
    *presence* test only; the mapping is ``severity.py``'s and is never duplicated here.
    """
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _type_name(value: Any) -> str:
    """Name ``value``'s type in JSON's vocabulary where there is one.

    Used in rejection details and in halt structures, both read by a human looking at
    the artifact -- so ``array`` is more useful there than ``list``.
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


def _is_object_array(value: Any) -> bool:
    """Return whether ``value`` is a non-empty array carrying at least one object.

    The finding-shaped test.  Every finding section Trivy emits is an array of objects,
    so an unknown member of that shape is treated as a finding section this adapter does
    not know (AAP 0.5.4's *"any non-empty finding array outside the three supported
    sections"*).  ``any`` rather than ``all`` on purpose: a mixed array is at least
    partly finding-shaped, and the conservative reading is the one that halts.  An array
    of strings -- a list of references, say -- is not finding-shaped and does not halt.
    """
    if not _is_json_array(value) or not value:
        return False
    return any(isinstance(element, Mapping) for element in value)


# --------------------------------------------------------------------------- #
# Structural excerpts: shape, never content
# --------------------------------------------------------------------------- #

#: How many elements of an offending array the excerpt describes.  Enough to see the
#: shape, bounded so a halt message stays quotable.
_EXCERPT_MAX_ELEMENTS: Final[int] = 3

#: How many keys of one object the excerpt describes, for the same reason.
_EXCERPT_MAX_KEYS: Final[int] = 24


def _structure_of(value: Any, *, depth: int = 0) -> dict[str, Any]:
    """Describe ``value`` by its shape: JSON types, key names and counts only.

    **No value ever enters the description.**  AAP 0.5.4 requires the halt report to
    quote the observed structure, and it equally requires that no adapter carry a
    secret value into any field; these logs are preserved verbatim, and an unrecognised
    finding section could hold anything.  Describing the shape satisfies the first
    requirement without risking the second: a reader learns that ``Licenses`` held four
    objects keyed ``Severity``, ``Name`` and ``Text``, which is what they need in order
    to act, and learns nothing about what those objects said.

    Key *names* are structure and are included; scalar contents are not and are never
    included.  The description is bounded in breadth by :data:`_EXCERPT_MAX_ELEMENTS`
    and :data:`_EXCERPT_MAX_KEYS`, and in depth to two levels, so a deeply nested
    artifact cannot produce an unbounded message.
    """
    described: dict[str, Any] = {"json_type": _type_name(value)}
    if isinstance(value, Mapping):
        keys = list(value)
        described["key_count"] = len(keys)
        shown = keys[:_EXCERPT_MAX_KEYS]
        described["keys"] = {
            str(key): _type_name(value[key]) for key in shown
        }
        if len(keys) > len(shown):
            described["keys_truncated"] = True
        return described
    if _is_json_array(value):
        described["element_count"] = len(value)
        shown_elements = list(value)[:_EXCERPT_MAX_ELEMENTS]
        if depth < 1:
            described["elements"] = [
                _structure_of(element, depth=depth + 1) for element in shown_elements
            ]
        else:
            described["element_types"] = [
                _type_name(element) for element in shown_elements
            ]
        if len(value) > len(shown_elements):
            described["elements_truncated"] = True
    return described


def _dump_structure(structure: Mapping[str, Any]) -> str:
    """Render a structural description as compact JSON for a halt message.

    ``default=str`` covers a key type JSON has no representation for; it can only ever
    be applied to a type name or a key name, because :func:`_structure_of` puts nothing
    else in the mapping.
    """
    return json.dumps(dict(structure), sort_keys=False, default=str)


# --------------------------------------------------------------------------- #
# The halt: every Results[] element, before a single row is built
# --------------------------------------------------------------------------- #


def validate_finding_sections(
    doc: Any,
    *,
    tool: str = TOOL,
    counters: dict[str, int] | None = None,
) -> None:
    """Validate that every finding this artifact holds sits in a supported section.

    The halt AAP 0.5.4 requires, run over **every** ``Results[]`` element before any row
    is built, so a defect in the last element stops the run as surely as one in the
    first and no partial dataset is produced.  It returns ``None`` where the artifact is
    normalizable and raises :class:`UnsupportedTrivySection` otherwise; the four
    conditions and the three deliberate non-halts are set out in this module's
    docstring.

    Args:
        doc: The already-parsed artifact document.
        tool: The canonical tool identifier, validated so a caller cannot route another
            tool's artifact through Trivy's structural policy.
        counters: An optional counter mapping to record the two observations this pass
            makes -- a non-empty non-finding object array, and a ``MisconfSummary`` that
            could not be read.  Neither halts.

    Raises:
        UnsupportedTrivySection: On any of the four halt conditions.
        TrivyAdapterError: If the document is not an object, or ``Results`` is present
            as something other than an array.
    """
    _validated_tool(tool)
    document = _validated_document(doc)
    for result_index, raw_result in enumerate(_json_array(document.get(_RESULTS_KEY))):
        element = _json_object(raw_result)
        if element is None:
            # Contributes no record, exactly as reconcile.py's traversal counts it, so
            # there is no finding here to be dropped and nothing to halt on.
            continue
        target = element.get(_TARGET_KEY)

        # 1. A known finding section this dataset does not support, present and
        #    non-empty.  An empty one satisfies "validated empty" and does not halt.
        #
        #    The name tells us the member holds findings, so *any* non-empty content
        #    under it halts -- an array with elements, and also an object with keys,
        #    which is a shape Trivy does not emit and therefore one whose contents this
        #    adapter cannot claim to have read.  A scalar is left alone: a number or a
        #    string under the key holds no finding records to drop.
        for section in UNSUPPORTED_FINDING_SECTIONS:
            if section not in element:
                continue
            value = element[section]
            count: int | None
            note: str | None
            if _is_json_array(value):
                if not value:
                    continue
                count, note = len(value), None
            elif isinstance(value, Mapping):
                if not value:
                    continue
                count, note = None, (
                    f"the member is a {_type_name(value)} rather than the array "
                    "report.go declares, so its contents cannot be read as findings "
                    "either."
                )
            else:
                # A null or a scalar under the key holds no finding records to drop.
                continue
            raise UnsupportedTrivySection(
                HALT_UNSUPPORTED_SECTION,
                section=section,
                target=target,
                result_index=result_index,
                element_count=count,
                structure=_structure_of(value),
                note=note,
            )

        # 2. A member outside the known Result fields holding a non-empty array of
        #    objects: an unrecognised finding section, treated as one rather than
        #    dropped.  Document order, so the reported key is reproducible.
        #
        #    The array-of-objects test is the whole test here, unlike condition 1.
        #    Every finding section report.go declares is an array of objects, so that
        #    is the shape an unrecognised finding section arrives as, and a non-empty
        #    one halts with the observed structure quoted (AAP 0.5.4).  What happens to
        #    anything else is this project's policy rather than a claim about the
        #    schema: an unknown member whose value is an object is treated as metadata
        #    and does not stop the run, because halting on one would halt on the next
        #    metadata field Trivy adds.  Non-finding arrays exist too -- ``Packages``
        #    and ``CustomResources`` are both object arrays -- so shape alone cannot
        #    separate a finding array from a non-finding one, which is why
        #    RESULT_KNOWN_KEYS names them instead.
        for key in element:
            if key in RESULT_KNOWN_KEYS:
                continue
            value = element[key]
            if not _is_object_array(value):
                continue
            raise UnsupportedTrivySection(
                HALT_UNKNOWN_SECTION,
                section=str(key),
                target=target,
                result_index=result_index,
                element_count=len(value),
                structure=_structure_of(value),
                note=(
                    f"{str(key)!r} is outside the known Result members "
                    f"({', '.join(sorted(RESULT_KNOWN_KEYS))})."
                ),
            )

        # 3. A supported section present as something other than an array or null: its
        #    records cannot be attributed to a section at all, which is AAP 0.9.2's
        #    "nothing distinguishing its three supported sections".
        for section in SUPPORTED_SECTIONS:
            if section not in element:
                continue
            value = element[section]
            if value is None or _is_json_array(value):
                continue
            raise UnsupportedTrivySection(
                HALT_SECTION_NOT_AN_ARRAY,
                section=section,
                target=target,
                result_index=result_index,
                structure=_structure_of(value),
                note=(
                    f"a {_type_name(value)} cannot be walked as a finding array, and "
                    "reconcile.py counts it as zero records, so normalizing it would "
                    "report a clean scan for an artifact nobody can read."
                ),
            )

        # 4. The element's own MisconfSummary declares failures that no supported
        #    section holds.  Deliberately not a count comparison: `--include-non-
        #    failures` puts passing checks in the array too, so `len(...) != Failures`
        #    is ordinary, while "failures declared and no section holding any record"
        #    is real output with nowhere to have come from.
        summary_raw = element.get(_MISCONF_SUMMARY_KEY)
        if summary_raw is not None:
            summary = _json_object(summary_raw)
            if summary is None:
                if counters is not None:
                    counters[COUNTER_MISCONF_SUMMARY_UNREADABLE] += 1
            else:
                failures = summary.get(_FAILURES_KEY)
                declared = (
                    failures
                    if isinstance(failures, int) and not isinstance(failures, bool)
                    else 0
                )
                if declared > 0 and not _json_array(
                    element.get(_MISCONFIGURATIONS_KEY)
                ):
                    raise UnsupportedTrivySection(
                        HALT_DECLARED_FINDINGS_UNHELD,
                        section=_MISCONFIGURATIONS_KEY,
                        target=target,
                        result_index=result_index,
                        element_count=0,
                        structure=_structure_of(element),
                        note=(
                            f"MisconfSummary declares {declared} failure(s) while "
                            f"{_MISCONFIGURATIONS_KEY} is "
                            f"{_type_name(element.get(_MISCONFIGURATIONS_KEY))}."
                        ),
                    )

        # Non-finding object arrays: neither a halt nor a record, but never silent.
        if counters is not None:
            for key in _NON_FINDING_OBJECT_LIST_KEYS:
                if _is_object_array(element.get(key)):
                    counters[COUNTER_KNOWN_NON_FINDING_OBJECT_LISTS] += 1



# --------------------------------------------------------------------------- #
# Per-section field extraction (AAP 0.5.4's trivy row)
# --------------------------------------------------------------------------- #


def _rule_id(
    record: Mapping[str, Any], section: str
) -> tuple[str | None, tuple[str, str] | None]:
    """Return the record's rule identifier, or the rejection it earns.

    The field is chosen **by section** -- ``VulnerabilityID``, ``RuleID`` or ``ID``
    (AAP 0.5.4) -- rather than by trying all three, because trying all three would read
    a record's content to decide which kind of record it is, which is the one thing this
    adapter must never do.

    Absent or blank is the ``missing_rule_id`` condition.  Present but not a string is
    structurally wrong rather than merely absent, so it is ``malformed_record``: the two
    are kept distinct because a reader acts differently on "this producer omits the
    identifier" than on "this artifact is not shaped like a Trivy report".
    """
    field = _SECTION_RULE_ID_FIELD[section]
    raw = record.get(field)
    if raw is None:
        return None, (
            paths.REJECT_MISSING_RULE_ID,
            f"the {section} record carries no {field}",
        )
    if not isinstance(raw, str):
        return None, (
            paths.REJECT_MALFORMED_RECORD,
            f"the {section} record's {field} is a {_type_name(raw)}, not a string",
        )
    resolved = _non_empty_string(raw)
    if resolved is None:
        return None, (
            paths.REJECT_MISSING_RULE_ID,
            f"the {section} record's {field} is empty or whitespace only",
        )
    return resolved, None


def _message(
    record: Mapping[str, Any], section: str
) -> tuple[str | None, tuple[str, str] | None]:
    """Return the record's message, or the rejection it earns.

    The preference order is :data:`_SECTION_MESSAGE_FIELDS`, which for a **secret** puts
    the already-redacted ``Match`` first, exactly as AAP 0.5.4 states.  No field outside
    that per-section tuple is consulted, which is what makes the secret guarantee
    structural: ``Code``, which carries the surrounding source lines, is never read, and
    neither is any other member, so a raw secret value has no route into a row whatever
    a record carries.

    Absent or empty across every consulted field is ``missing_message``.  A consulted
    field present as a non-string is ``malformed_record``, named with the field and the
    observed type.
    """
    fields = _SECTION_MESSAGE_FIELDS[section]
    malformed: tuple[str, str] | None = None
    for field in fields:
        if field not in record:
            continue
        raw = record[field]
        if raw is None:
            continue
        if not isinstance(raw, str):
            if malformed is None:
                malformed = (
                    paths.REJECT_MALFORMED_RECORD,
                    f"the {section} record's {field} is a {_type_name(raw)}, "
                    "not a string",
                )
            continue
        resolved = _non_empty_string(raw)
        if resolved is not None:
            return resolved, None
    if malformed is not None:
        return None, malformed
    return None, (
        paths.REJECT_MISSING_MESSAGE,
        f"the {section} record carries no message: none of "
        f"{', '.join(fields)} holds text",
    )


def _score_candidates(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return the record's CVSS score candidates, in a fixed order.

    Trivy states scores as ``CVSS: {"<source>": {"V2Score": .., "V3Score": ..}}`` (the
    embedded ``dbTypes.Vulnerability`` in ``report.go``).  Each score field found is
    emitted as one candidate carrying the ``score``, ``source`` and ``version`` keys
    ``severity.resolve`` reads, and the sources are walked in sorted order so the
    candidate list -- and therefore ``severity.py``'s tie-breaking on it -- is
    reproducible.

    The recorded version is the **major** version the field name states (``4``, ``3``,
    ``2``) and nothing more: ``V3Score`` does not say whether the score is CVSS 3.0 or
    3.1, and writing ``3.1`` would be supplying precision the artifact never stated.

    No banding, no precedence and no selection happens here.  ``severity.py`` owns all
    three, including AAP 0.5.4's rule that a mapped native label governs and a score is
    consulted only where no mapped label exists.
    """
    table = _json_object(record.get(_CVSS_KEY))
    if table is None:
        return []
    candidates: list[dict[str, Any]] = []
    for source in sorted(str(key) for key in table):
        entry = _json_object(table[source])
        if entry is None:
            continue
        for field, major in (("V40Score", "4"), ("V3Score", "3"), ("V2Score", "2")):
            if field not in entry:
                continue
            raw = entry[field]
            if raw is None:
                continue
            candidates.append(
                {"score": raw, "source": f"{source}:{field}", "version": major}
            )
    return candidates


def _severity_for(
    record: Mapping[str, Any], counters: dict[str, int]
) -> severity.SeverityResult:
    """Resolve the record's severity, recording what vocabulary it actually carried.

    ``Severity`` is passed as ``label`` and the ``CVSS`` table as ``scores``; the
    precedence between them is ``severity.py``'s and is not restated here.  AAP 0.5.4:
    *"the native label governs whenever it is in the mapped vocabulary, and a CVSS score
    is consulted only where no mapped label exists"*, and either way *"the entry used is
    recorded"* -- which the returned result's ``basis`` and ``selected_entry`` carry, and
    which the basis counter and the tally then publish.

    The three counters bumped here record what was **available** rather than what was
    used, so "a label was present and a score was not consulted" is checkable against
    the basis rather than merely asserted.  A literal outside every mapped vocabulary
    comes back on the ``unmapped_literal`` basis, banded ``Info`` and disclosed, so
    ``severity-map.md`` lists it with the rows it affected.
    """
    label = record.get(_SEVERITY_KEY)
    candidates = _score_candidates(record)
    label_present = _literal_is_present(label)
    if label_present:
        counters[COUNTER_SEVERITY_LABEL_PRESENT] += 1
    if candidates:
        counters[COUNTER_SEVERITY_CVSS_ENTRIES_PRESENT] += 1
    if not label_present and not candidates:
        counters[COUNTER_SEVERITY_ABSENT] += 1
    return severity.resolve(label=label, scores=candidates or None)


@dataclass(frozen=True)
class _LineLookup:
    """The record's start line, the rejection it earns, or a stated absence."""

    value: int | None
    failure: tuple[str, str] | None
    zero_read_as_absent: bool = False


def _start_line(record: Mapping[str, Any], section: str) -> _LineLookup:
    """Return the record's ``start_line``, or the rejection it earns.

    Section-dependent by the artifact's own shape (AAP 0.5.4, AAP 0.2.2): a secret
    states ``StartLine`` directly, a misconfiguration states it inside
    ``CauseMetadata``, and a vulnerability states none at all.  **Absence is never a
    rejection** -- ``start_line`` is one of the five fields absence is permitted for
    (AAP 0.8.2) -- so a vulnerability with no line, and a misconfiguration whose
    ``CauseMetadata`` carries none, both yield ``None`` and a row.

    A ``StartLine`` present but not usable as a line number is the
    ``non_integer_start_line`` condition.  Three shapes reach it, each named in the
    detail: a non-integer type, ``True``/``False`` -- which Python's numeric tower would
    otherwise admit as ``1`` and ``0`` -- and a negative value.

    ``0`` is treated as a **stated absence** rather than as a rejection or as line zero.
    It is Go's zero value for the field, there is no line zero in any file, and emitting
    it would put a number in the dataset that names nothing; rejecting a real finding
    over it would be worse still, since the rejection condition AAP 0.5.4 names is a
    ``start_line`` that is *not an integer* and ``0`` is one.  Every occurrence is
    counted under ``start_line_zero_read_as_absent``, so the reading is visible in
    ``normalize-run.json`` rather than silent.

    A ``CauseMetadata`` present as something other than an object is structurally wrong
    rather than an absence, so it is ``malformed_record``: treating it as "no line
    information" would drop the location of every record in a malformed artifact without
    a trace.
    """
    for key_path in _SECTION_START_LINE_PATHS.get(section, ()):
        container: Mapping[str, Any] | None = record
        for key in key_path[:-1]:
            if container is None:
                # Reachable only if a later key follows one whose value was absent; the
                # break below already handles that, and this guard keeps the invariant
                # enforced at run time rather than by an ``assert`` that ``python -O``
                # would strip.
                break
            raw_container = container.get(key)
            if raw_container is None:
                container = None
                break
            nested = _json_object(raw_container)
            if nested is None:
                return _LineLookup(
                    None,
                    (
                        paths.REJECT_MALFORMED_RECORD,
                        f"the {section} record's {key} is a "
                        f"{_type_name(raw_container)}, not an object",
                    ),
                )
            container = nested
        if container is None:
            continue
        leaf = key_path[-1]
        if leaf not in container:
            continue
        raw = container[leaf]
        if raw is None:
            continue
        label = ".".join(key_path)
        if isinstance(raw, bool) or not isinstance(raw, int):
            return _LineLookup(
                None,
                (
                    paths.REJECT_NON_INTEGER_START_LINE,
                    f"{label} is {raw!r}, a {_type_name(raw)} rather than an integer",
                ),
            )
        if raw == 0:
            return _LineLookup(None, None, zero_read_as_absent=True)
        if raw < 0:
            return _LineLookup(
                None,
                (
                    paths.REJECT_NON_INTEGER_START_LINE,
                    f"{label} is {raw}, which is not a line number: a file's lines are "
                    "numbered from one",
                ),
            )
        return _LineLookup(raw, None)
    return _LineLookup(None, None)


def _location_count(record: Mapping[str, Any], section: str) -> int:
    """Return how many locations the record names, for the first-location rule.

    AAP 0.5.4: where a record carries more than one location the row takes the
    **first**, the record still counts **once**, and the number of such records is
    reported per tool.  For Trivy that case is a misconfiguration whose
    ``CauseMetadata.Occurrences`` holds more than one entry -- ``ftypes.CauseMetadata``
    is the only member of any of the three sections that carries a list of locations.  A
    vulnerability and a secret each name exactly one.

    ``Occurrences`` feeds this count and nothing else: the emitted ``start_line``
    remains the record's own stated line, never an occurrence's, because reading a line
    out of an occurrence would be choosing a location the record did not put first.
    """
    if section != _MISCONFIGURATIONS_KEY:
        return 1
    cause = _json_object(record.get(_CAUSE_METADATA_KEY))
    if cause is None:
        return 1
    return max(1, len(_json_array(cause.get(_OCCURRENCES_KEY))))


def _cwe_sources(record: Mapping[str, Any]) -> tuple[str, ...]:
    """Return the strings CWE identifiers are collected from.

    ``CweIDs`` (AAP 0.5.4), read wherever a record declares it and accepted as either a
    string or an array of strings.  Only strings are collected: a bare number is not
    read as an identifier, because turning ``79`` into ``CWE-79`` would supply a prefix
    the artifact never wrote, and AAP 0.5.4's rule is to reject rather than infer.

    ``report.go`` declares the field on ``DetectedVulnerability`` only (through the
    embedded ``dbTypes.Vulnerability``), so for a secret or a misconfiguration the field
    is absent by the artifact's shape rather than by this adapter's choice -- and reading
    it by name means a future Trivy that states a CWE on another section is not silently
    dropped.
    """
    raw = record.get(_CWE_IDS_KEY)
    if isinstance(raw, str):
        return (raw,)
    if _is_json_array(raw):
        return tuple(entry for entry in raw if isinstance(entry, str))
    return ()


def _cve_sources(record: Mapping[str, Any]) -> tuple[str, ...]:
    """Return the strings CVE identifiers are collected from.

    ``VulnerabilityID`` alone (AAP 0.5.4: *"cve <- VulnerabilityID when CVE-shaped"*).
    ``VendorIDs`` -- RHSA, ALAS and the like -- are deliberately not collected: they are
    vendor advisories rather than CVEs, and they stay in the artifact rather than being
    reshaped into a field they do not belong in.
    """
    raw = record.get(_VULNERABILITY_ID_KEY)
    return (raw,) if isinstance(raw, str) else ()


def _select_cwe(sources: Iterable[str]) -> tuple[str | None, int]:
    """Return the CWE to emit and how many distinct ones were found.

    The ascending-identifier rule (AAP 0.5.4): the field carries **one** value, chosen
    by ascending numeric identifier -- the integer after the ``CWE-`` prefix.  That
    ordering is total over the integers, so no tie can arise and no producer-order
    tiebreak is needed.  The emitted value keeps the digits exactly as they appeared,
    including any leading zero, under the canonical upper-case prefix.
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
    order over the pair, so again no tiebreak is needed.  The emitted value keeps both
    digit groups as they appeared under the canonical upper-case prefix.
    """
    found: dict[tuple[int, int], str] = {}
    for text in sources:
        for match in CVE_TOKEN_PATTERN.finditer(text):
            year, sequence = match.group(1), match.group(2)
            found.setdefault((int(year), int(sequence)), f"CVE-{year}-{sequence}")
    if not found:
        return None, 0
    return found[min(found)], len(found)



# --------------------------------------------------------------------------- #
# One canonical package coordinate, by the shared four-level precedence
# (AAP 0.5.4, identical in every adapter)
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class _Coordinate:
    """The coordinate to emit, which level supplied it, and why none could be formed."""

    value: str | None
    counter: str
    detail: str | None = None


def _purl_candidates(container: Any, identifier_keys: Sequence[str]) -> tuple[str, ...]:
    """Return every package URL one object states, sorted lexicographically.

    A package URL is read from ``<identifier>.PURL`` for each identifier member named,
    and from a ``PURL`` stated directly on the object.  Sorting implements AAP 0.5.4's
    tiebreak -- *"Where several candidates sit at one level, the lexicographically
    smallest wins"* -- and returning the whole set rather than one value lets the caller
    apply that tiebreak across the objects at a level as well as within one.
    """
    obj = _json_object(container)
    if obj is None:
        return ()
    found: set[str] = set()
    direct = _non_empty_string(obj.get(_PURL_KEY))
    if direct is not None:
        found.add(direct)
    for key in identifier_keys:
        identifier = _json_object(obj.get(key))
        if identifier is None:
            continue
        purl = _non_empty_string(identifier.get(_PURL_KEY))
        if purl is not None:
            found.add(purl)
    return tuple(sorted(found))


def _matching_packages(
    record: Mapping[str, Any], packages: Sequence[Any]
) -> tuple[Mapping[str, Any], ...]:
    """Return the enclosing ``Packages[]`` entries that are this record's own package.

    The enclosing package object AAP 0.5.4's second and fourth precedence levels refer
    to.  A Trivy ``Result`` carries the inventory under ``Packages`` when the runner
    passes ``--list-all-pkgs``; this one does not, so the levels are normally vacuous and
    are implemented anyway because the precedence is the shared one and a future
    invocation may populate it.

    Identity is **exact equality** and nothing weaker: the record's ``PkgID`` against a
    package's ``ID``, or the record's ``PkgName`` and ``InstalledVersion`` against a
    package's ``Name`` and ``Version``.  No prefix match, no normalisation, no
    version-range reasoning -- a fuzzy match would attribute one package's coordinate to
    another package's finding, which is inference of exactly the kind AAP 0.5.4 forbids.
    """
    pkg_id = _non_empty_string(record.get(_PKG_ID_KEY))
    name = _non_empty_string(record.get(_PKG_NAME_KEY))
    version = _non_empty_string(record.get(_INSTALLED_VERSION_KEY))
    if pkg_id is None and (name is None or version is None):
        return ()
    matched: list[Mapping[str, Any]] = []
    for raw in packages:
        package = _json_object(raw)
        if package is None:
            continue
        if pkg_id is not None and _non_empty_string(package.get(_PACKAGE_ID_KEY)) == pkg_id:
            matched.append(package)
            continue
        if (
            name is not None
            and version is not None
            and _non_empty_string(package.get(_PACKAGE_NAME_KEY)) == name
            and _non_empty_string(package.get(_PACKAGE_VERSION_KEY)) == version
        ):
            matched.append(package)
    return tuple(matched)


def _ecosystem_coordinate(
    ecosystem: Any, name: Any, version: Any
) -> str | None:
    """Return ``<ecosystem>:<name>@<version>``, or ``None`` where it cannot be formed.

    AAP 0.5.4's third and fourth levels, with the ecosystem **lower-cased** as it
    requires.  The ecosystem is the enclosing ``Results[].Type`` -- Trivy's own target
    type, ``jar``, ``pom``, ``gomod``, ``npm`` and so on -- which is the artifact's
    statement of the ecosystem rather than a guess at one.  Where any of the three parts
    is absent the specified shape cannot be formed and this level fails rather than
    emitting a partial coordinate: ``:name@`` names nothing.
    """
    ecosystem_text = _non_empty_string(ecosystem)
    name_text = _non_empty_string(name)
    version_text = _non_empty_string(version)
    if ecosystem_text is None or name_text is None or version_text is None:
        return None
    return f"{ecosystem_text.strip().lower()}:{name_text.strip()}@{version_text.strip()}"


def _package_coordinate(
    record: Mapping[str, Any],
    *,
    section: str,
    packages: Sequence[Any],
    ecosystem: Any,
) -> _Coordinate:
    """Return the one canonical package coordinate, by the shared precedence.

    AAP 0.5.4, in order: a package URL on the record; failing that a package URL on the
    enclosing package object; failing that ``<ecosystem>:<name>@<version>`` from the
    record's own fields; failing that the same from the enclosing package's fields.
    Where several candidates sit at one level the lexicographically smallest wins, and
    where none can be formed the field is absent.

    For Trivy the record-level package URL is ``PkgIdentifier.PURL`` and the record's own
    fields are ``PkgName`` and ``InstalledVersion`` (AAP 0.5.4's trivy row), with the
    ecosystem taken from the enclosing ``Results[].Type``.

    A record from ``Vulnerabilities[]`` is **dependency-oriented**, so an absent
    coordinate there is a rejection condition; a secret and a misconfiguration are
    findings about a file rather than about a package, so absence is ordinary for them.
    The caller applies that distinction -- this function reports what it could form and
    why it could not, and never decides an outcome.
    """
    record_purls = _purl_candidates(record, (_PKG_IDENTIFIER_KEY,))
    if record_purls:
        return _Coordinate(record_purls[0], COUNTER_COORDINATE_RECORD_PURL)

    matched = _matching_packages(record, packages)
    package_purls = sorted(
        {
            purl
            for package in matched
            for purl in _purl_candidates(package, _PACKAGE_IDENTIFIER_KEYS)
        }
    )
    if package_purls:
        return _Coordinate(package_purls[0], COUNTER_COORDINATE_PACKAGE_PURL)

    from_record = _ecosystem_coordinate(
        ecosystem, record.get(_PKG_NAME_KEY), record.get(_INSTALLED_VERSION_KEY)
    )
    if from_record is not None:
        return _Coordinate(from_record, COUNTER_COORDINATE_RECORD_FIELDS)

    from_packages = sorted(
        {
            candidate
            for package in matched
            for candidate in (
                _ecosystem_coordinate(
                    ecosystem,
                    package.get(_PACKAGE_NAME_KEY),
                    package.get(_PACKAGE_VERSION_KEY),
                ),
            )
            if candidate is not None
        }
    )
    if from_packages:
        return _Coordinate(from_packages[0], COUNTER_COORDINATE_PACKAGE_FIELDS)

    return _Coordinate(
        None,
        COUNTER_COORDINATE_ABSENT,
        detail=(
            f"no package coordinate can be formed for this {section} record at any "
            f"candidate level: {_PKG_IDENTIFIER_KEY} is "
            f"{_type_name(record.get(_PKG_IDENTIFIER_KEY))} with no {_PURL_KEY}, "
            f"{_PKG_NAME_KEY} is {_type_name(record.get(_PKG_NAME_KEY))}, "
            f"{_INSTALLED_VERSION_KEY} is "
            f"{_type_name(record.get(_INSTALLED_VERSION_KEY))}, the enclosing "
            f"{_TYPE_KEY} is {_type_name(ecosystem)}, and {len(matched)} enclosing "
            f"{_PACKAGES_KEY} entr{'y' if len(matched) == 1 else 'ies'} match it"
        ),
    )



# --------------------------------------------------------------------------- #
# Argument validation.
#
# Every one of these raises :class:`TrivyAdapterError` rather than returning a
# rejection: a bad argument is a caller fault, and absorbing it into a rejection count
# would let a wrong root or a foreign path base produce a plausible dataset for a whole
# tool.  Each is validated once per call, before any record is read, so a fault
# surfaces on the call rather than on the first record.
# --------------------------------------------------------------------------- #


def _validated_tool(tool: Any) -> str:
    """Return ``tool`` where it is this adapter's canonical identifier, else raise.

    Validated even though this module serves exactly one tool, so ``cli.py``'s registry
    can call every adapter uniformly and a mis-keyed registry entry fails on the call
    instead of stamping ``trivy`` onto another tool's records.
    """
    if not isinstance(tool, str):
        raise TrivyAdapterError(
            f"tool must be a canonical tool identifier string; observed "
            f"{_type_name(tool)}"
        )
    if tool != TOOL:
        raise TrivyAdapterError(
            f"{tool!r} is not the tool this adapter serves ({TOOL!r}). One adapter per "
            "non-SARIF artifact written: a SARIF producer routes to the shared SARIF "
            "adapter, and every other native shape has its own module."
        )
    return tool


def _validated_root(root: Any) -> str:
    """Return the scan root as an absolute POSIX-normalised string, else raise.

    A :class:`pathlib.Path` and a string are both accepted -- ``os.fspath`` is the one
    thing ``os`` is imported for -- and the result is normalised through ``paths.py`` so
    that this module and every resolver agree on the root's spelling.  A relative root is
    refused here rather than at the first record: it cannot anchor anything, and
    accepting one would produce a plausible-looking wrong answer for every row.
    """
    try:
        candidate = fspath(root)
    except TypeError as error:
        raise TrivyAdapterError(
            f"root must be a str or an os.PathLike naming the SPARK_SRC root; observed "
            f"{_type_name(root)}"
        ) from error
    if isinstance(candidate, bytes):
        raise TrivyAdapterError(
            "root must be a text path, not bytes: every path in the dataset is text, "
            "and decoding one here would guess an encoding"
        )
    if not candidate:
        raise TrivyAdapterError("root must not be empty")
    normalised = paths.normalise_reported_path(candidate)
    if not paths.is_absolute_path(normalised):
        raise TrivyAdapterError(
            f"root must be an absolute path to express a reported path against; "
            f"observed {candidate!r}"
        )
    return normalised


def _validated_tool_base(tool_base: Any, tool: str) -> paths.ToolPathBase:
    """Return ``tool_base`` where it is this tool's recorded path base, else raise.

    The identifier check is not ceremony.  ``tool_base`` is the per-tool view over
    ``harness/artifacts/logs/runner-metadata.json``, and handing this adapter another
    tool's view would resolve every path against the wrong base while every row still
    looked well-formed -- the exact failure AAP 0.5.4 requires *"every base taken from
    the recorded runner metadata"* to prevent.
    """
    if not isinstance(tool_base, paths.ToolPathBase):
        raise TrivyAdapterError(
            f"tool_base must be a paths.ToolPathBase built from the runner metadata; "
            f"observed {_type_name(tool_base)}"
        )
    if tool_base.tool != tool:
        raise TrivyAdapterError(
            f"tool_base names {tool_base.tool!r} but the artifact is {tool!r}; resolving "
            "one tool's paths against another tool's recorded base would produce a wrong "
            "path for every row of it"
        )
    return tool_base


def _validated_allowlist(allowlist: Any) -> tuple[str, ...]:
    """Return the allowlist globs as a tuple, materialised once, else raise.

    Materialising matters: a generator would be exhausted by the first row and every
    subsequent row would silently take ``in_scope: false``.

    The globs' *content* is not checked against the twelve authoritative ones here.
    ``cli.py`` owns that check -- ``paths.allowlist_matches_authoritative_globs`` -- and
    duplicating it would put a second, divergable copy of the scope contract in an
    adapter.  What is checked is that each glob is a non-empty string, since a non-string
    pattern would raise from the matcher on the first row rather than on the call.
    """
    if isinstance(allowlist, (str, bytes)):
        raise TrivyAdapterError(
            "allowlist must be an iterable of glob strings, not a single string: a "
            "string would be iterated character by character"
        )
    if not isinstance(allowlist, Iterable):
        raise TrivyAdapterError(
            f"allowlist must be an iterable of glob strings from paths.load_allowlist(); "
            f"observed {_type_name(allowlist)}"
        )
    globs = tuple(allowlist)
    for index, glob in enumerate(globs):
        if not isinstance(glob, str) or not glob:
            raise TrivyAdapterError(
                f"allowlist entry {index} must be a non-empty glob string; observed "
                f"{glob!r}"
            )
    return globs


def _validated_tally(tally: Any) -> Any:
    """Return ``tally`` where it can record a severity result, else raise.

    The capability is checked rather than the class, so a test double is as acceptable as
    a :class:`normalize.severity.LiteralTally`.  ``None`` is not: every row's literal has
    to reach ``severity-map.md``, and a silently skipped tally would leave that document
    under-reporting with nothing to show it had.
    """
    recorder = getattr(tally, "record", None)
    if not callable(recorder):
        raise TrivyAdapterError(
            f"tally must expose a callable record(tool, result) -- normally a "
            f"severity.LiteralTally; observed {_type_name(tally)}"
        )
    return tally


def _validated_document(doc: Any) -> Mapping[str, Any]:
    """Return ``doc`` where it is a Trivy report this adapter can walk, else raise.

    Two things are required, and a third is deliberately *not*.

    The top level must be an **object**: ``report.go``'s ``Report`` is a struct, and
    ``shape.py`` routes ``trivy.json`` here by name, so a bare array or a scalar is a
    mis-route or a hand-built fixture rather than something to normalize.

    ``Results`` must be an **array where it is present at all**.  Present as an object or
    a string is refused rather than counted as zero records: counting it as zero would
    agree with ``reconcile._count_trivy`` and reconcile cleanly while reporting a
    malformed artifact as a clean scan, and an empty result set is indistinguishable from
    a clean scan.

    ``Results`` **null**, and ``Results`` absent, are explicitly not errors here.  Trivy
    legitimately emits a report with nothing to say and AAP 0.5.4 makes an empty report
    ordinary, so the emptiness is counted rather than raised and is visible in
    ``normalize-run.json``.  The two arrive by different routes, which is worth stating
    because the difference is not this function's to enforce: ``shape.py`` requires a
    ``trivy.json`` to carry the ``Results`` key -- as an array or ``null`` -- so an
    artifact with the key **absent** halts at the router under
    ``REASON_NATIVE_SHAPE_UNRECOGNIZED`` and never reaches this adapter, while a ``null``
    ``Results`` satisfies that envelope and does.  The absent case is still handled rather
    than assumed away, because this function is called directly by
    ``oss-scan-results/adapter-tests/test_trivy_adapter.py`` and duplicating the router's
    refusal here would report one condition under two vocabularies -- a caller who did
    bypass routing would get an ``AdapterError`` where the run record already has a shape
    halt for the same bytes.

    ``SchemaVersion`` is not checked.  The provisioned artifact carries ``2``, but the
    field is not part of any contract this pipeline was given, and refusing a report over
    it would halt on a version difference AAP 0.9.3 would have recorded and continued
    past.  What *is* validated is the structure this adapter actually walks.
    """
    document = _json_object(doc)
    if document is None:
        raise TrivyAdapterError(
            f"a Trivy report's top level is an object; observed {_type_name(doc)}. "
            "Shape detection belongs to shape.py, which routes an artifact here by its "
            "known name; this adapter owns the structural validation of what it walks."
        )
    if _RESULTS_KEY in document:
        results = document[_RESULTS_KEY]
        if results is not None and not _is_json_array(results):
            raise TrivyAdapterError(
                f"the report's {_RESULTS_KEY} is a {_type_name(results)}, not an array. "
                "Reading it as zero records would reconcile cleanly while reporting a "
                "malformed artifact as a clean scan."
            )
    return document


# --------------------------------------------------------------------------- #
# One record -> exactly one outcome
# --------------------------------------------------------------------------- #


def adapt_record(
    record: Any,
    *,
    section: str,
    target: Any,
    tool: str,
    root: str,
    tool_base: paths.ToolPathBase,
    globs: tuple[str, ...],
    tally: Any,
    counters: dict[str, int],
    packages: Sequence[Any] = (),
    ecosystem: Any = None,
    section_base: str | None = None,
    result_index: int = 0,
    record_index: int = 0,
) -> dict[str, Any] | paths.Rejection:
    """Return one row **or** one rejection for one element of one finding section.

    Exactly one of the two, always.  The single return value is what makes the
    one-to-one property structural: there is no path through this function that emits
    both and none that emits neither, so ``dataset rows + rejected records == the records
    walked`` holds by construction rather than by an assertion that could be forgotten.

    This is also the **section-attribution seam**, and it is public for that reason.
    ``section`` is a required argument and the ``scanner_class`` is derived from it alone,
    through :func:`scanner_class_for_section`; a section outside
    :data:`SUPPORTED_SECTIONS` yields an ``unattributable_section`` rejection rather than
    a class.  :func:`adapt` cannot reach that branch -- its iteration is section-bound by
    construction, and an unrecognised finding array halts instead, because
    ``reconcile.py`` counts zero records for one and rejecting its records would break
    the identity -- so the branch is exercised by calling this function directly, which
    is what the adapter test does.  It is a guard rather than dead code: it is what makes
    a future change to the iteration fail loudly instead of mis-classing rows.

    The classification order is the one this module's docstring fixes: shape, section,
    rule identifier, message, path, ``start_line``, then the dependency-oriented package
    coordinate.  Severity, ``cwe``/``cve`` and ``in_scope`` cannot reject -- each is
    defined for every input -- so a record that reaches them becomes a row.

    Nothing is caught broadly here.  Each lookup and conversion is guarded where it
    happens, so a genuine programming error propagates instead of being converted into a
    rejection count that would satisfy reconciliation while hiding a defect -- and
    :class:`UnsupportedTrivySection`, which is raised before this function is ever
    called, could not be swallowed even if it were raised inside it.

    Args:
        record: One element of one finding section, as parsed.
        section: The section key the element was read from. Decides ``scanner_class``.
        target: The enclosing ``Results[].Target``, the record's path base.
        tool: The canonical tool identifier, stamped into the row.
        root: The absolute ``SPARK_SRC`` root, already normalised.
        tool_base: This tool's recorded :class:`normalize.paths.ToolPathBase`.
        globs: The allowlist globs, already materialised.
        tally: The severity literal tally, fed once for an emitted row.
        counters: The counter mapping, mutated in place.
        packages: The enclosing ``Results[].Packages`` array, for the package-coordinate
            precedence's enclosing-object levels.
        ecosystem: The enclosing ``Results[].Type``, the ecosystem of a formed
            ``<ecosystem>:<name>@<version>`` coordinate.
        section_base: Passed through to ``paths.py`` for a ``per_section_target`` base --
            the shape the retained per-directory reports need, which the merged artifact
            does not.
        result_index: The enclosing element's index, for the rejection's identity.
        record_index: This record's index within its section, likewise.

    Returns:
        A twelve-field row dict, or a :class:`normalize.paths.Rejection`.
    """
    # The rejection's identity: enough to find the record in the artifact again, and
    # never the record itself and never a secret value (``paths.Rejection``'s own
    # contract).  ``Target`` carries the artifact's own spelling, which is also the key
    # ``paths.py`` sets, so a rejection it returns and a rejection built here identify
    # the record the same way rather than under two names.
    identity: dict[str, Any] = {
        "result_index": result_index,
        "section": section,
        "record_index": record_index,
        "Target": target,
    }

    record_object = _json_object(record)
    if record_object is None:
        return paths.make_rejection(
            paths.REJECT_MALFORMED_RECORD,
            tool,
            f"the {section} element is a {_type_name(record)}, not an object, so no "
            "finding can be read from it",
            **identity,
        )

    # The section the record was read from decides the scanner_class, here and nowhere
    # else: the record's own fields are never consulted to decide its class, and a
    # section outside SUPPORTED_SECTIONS is rejected under unattributable_section rather
    # than given a class (AAP 0.5.4).
    klass = scanner_class_for_section(section)
    if klass is None:
        return paths.make_rejection(
            paths.REJECT_UNATTRIBUTABLE_SECTION,
            tool,
            f"{section!r} is not one of this artifact's finding sections "
            f"({', '.join(SUPPORTED_SECTIONS)}), so the record's scanner_class cannot "
            "be established from the section it was read from, and record content is "
            "never used to establish one",
            **identity,
        )

    # The identifier field is chosen by section -- VulnerabilityID, RuleID or ID -- so
    # that no record's content is read to decide which kind of record it is; absent or
    # blank earns missing_rule_id, a non-string malformed_record.
    rule_id, rule_failure = _rule_id(record_object, section)
    if rule_failure is not None:
        reject_class, detail = rule_failure
        return paths.make_rejection(reject_class, tool, detail, **identity)
    identity["rule_id"] = rule_id

    # Likewise by section, and no field outside that per-section tuple is consulted,
    # which is what keeps a secret's surrounding source out of a row; nothing readable
    # across those fields earns missing_message.
    message, message_failure = _message(record_object, section)
    if message_failure is not None:
        reject_class, detail = message_failure
        return paths.make_rejection(reject_class, tool, detail, **identity)

    # The multi-location count is a property of the record, so it is taken whatever the
    # record's outcome turns out to be (AAP 0.5.4: the row takes the first location, the
    # record still counts once, and the number is reported per tool).
    if _location_count(record_object, section) > 1:
        counters[COUNTER_MULTI_LOCATION] += 1

    # Every base decision is delegated to paths.py: the enclosing Target, the section's
    # own per-record path where the section supplies one, and the recorded base go in,
    # and a ResolvedPath or a Rejection comes back.  Nothing is relativized, joined or
    # stripped here.
    per_record_field = _SECTION_PER_RECORD_PATH_FIELD.get(section)
    per_record_path = (
        record_object.get(per_record_field) if per_record_field is not None else None
    )
    resolved = paths.resolve_trivy_path(
        target,
        root,
        tool_base,
        per_record_path=per_record_path,
        section=section,
        section_base=section_base,
        tool=tool,
        record_identity=identity,
    )
    if isinstance(resolved, paths.Rejection):
        # Returned as-is: paths.py has already named the class and written the
        # sub-reason.  Rewording it here would lose that.
        return resolved
    if _non_empty_string(per_record_path) is not None:
        counters[COUNTER_PER_RECORD_PATH_REFINEMENTS] += 1

    # start_line is section-dependent by the artifact's own shape: a secret states it
    # directly, a misconfiguration inside CauseMetadata, and a vulnerability states none
    # at all -- an absence that is normal and never a rejection.  A present value that
    # is not a usable line number is one; a stated 0 is read as an absence and counted,
    # never emitted as line zero.
    line = _start_line(record_object, section)
    if line.failure is not None:
        reject_class, detail = line.failure
        return paths.make_rejection(reject_class, tool, detail, **identity)
    if line.zero_read_as_absent:
        counters[COUNTER_START_LINE_ZERO] += 1
    if line.value is None:
        counters[COUNTER_START_LINE_ABSENT] += 1

    # The package coordinate, and the rejection a dependency-oriented record earns
    # where none can be formed.
    coordinate = _package_coordinate(
        record_object, section=section, packages=packages, ecosystem=ecosystem
    )
    if coordinate.value is None and section in _DEPENDENCY_ORIENTED_SECTIONS:
        return paths.make_rejection(
            paths.REJECT_UNFORMABLE_PACKAGE_COORDINATE,
            tool,
            coordinate.detail
            or (
                f"no package coordinate can be formed for this {section} record, which "
                "is dependency-oriented"
            ),
            **identity,
        )
    counters[coordinate.counter] += 1

    # From here nothing can reject: this record is a row.
    severity_result = _severity_for(record_object, counters)
    counters[f"{COUNTER_SEVERITY_BASIS_PREFIX}{severity_result.basis}"] += 1
    # The tally is fed once per emitted row, which is what makes severity-map.md's
    # per-literal counts the row counts it reports them as.  A rejected record
    # contributes no row, so counting one here would put a literal in that document
    # against rows the dataset does not contain.
    tally.record(tool, severity_result)

    cwe, cwe_count = _select_cwe(_cwe_sources(record_object))
    cve, cve_count = _select_cve(_cve_sources(record_object))
    if cwe_count > 1:
        counters[COUNTER_MULTI_VALUED_CWE] += 1
    if cve_count > 1:
        counters[COUNTER_MULTI_VALUED_CVE] += 1

    counters[f"{COUNTER_PATH_KIND_PREFIX}{resolved.kind}"] += 1
    if resolved.is_non_filesystem_coordinate:
        counters[COUNTER_NON_FILESYSTEM_PATHS] += 1

    # in_scope is decided by the allowlist alone, through paths.py's matcher, on the
    # resolved path and carrying its kind -- so an archive member cannot match a glob on
    # its segments and the literal src/test exclusion is applied once, where it lives.
    # Nothing is ever filtered on it: a row outside the allowlist is kept with in_scope
    # false and counted (AAP 0.9.3).
    in_scope = bool(resolved.in_scope(globs))
    counters[COUNTER_ROWS_IN_SCOPE if in_scope else COUNTER_ROWS_OUT_OF_SCOPE] += 1
    counters[f"{COUNTER_ROWS_CLASS_PREFIX}{klass}"] += 1

    row: dict[str, Any] = {
        "tool": tool,
        "scanner_class": klass,
        "rule_id": rule_id,
        "message": message,
        "severity_native": severity_result.severity_native,
        "severity_norm": severity_result.severity_norm,
        "path": resolved.path,
        "start_line": line.value,
        "cwe": cwe,
        "cve": cve,
        "package_coordinate": coordinate.value,
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
    section_base: str | None = None,
) -> tuple[list[dict[str, Any]], list[paths.Rejection], dict[str, int]]:
    """Turn one Trivy 0.74.0 native artifact into dataset rows, rejections and counters.

    This is the uniform adapter entry point: every adapter module in this package
    exposes ``adapt`` with this shape, so ``cli.py``'s registry resolves it with
    ``getattr(module, "adapt")`` and every adapter test calls it directly.

    The traversal is two passes and the order matters.  :func:`validate_finding_sections`
    runs first over **every** ``Results[]`` element, so a non-empty unsupported section
    in the last element halts the run before a single row exists (AAP 0.5.4); only then
    are rows built.  Within the row pass the ``scanner_class`` is bound at the moment an
    array is chosen and handed to :func:`adapt_record` as an argument, so no record's
    class can come from its own content.

    Args:
        doc: The **already-parsed** artifact document -- a mapping for this shape.
            Parsing and shape detection happen upstream, which is what lets a test
            exercise every behaviour on a fixture with no filesystem.
        tool: The canonical tool identifier, ``"trivy"``. Required and validated even
            though this module serves one tool, so ``cli.py``'s registry can call every
            adapter uniformly.
        root: The ``SPARK_SRC`` root, as a :class:`pathlib.Path` or a string. Must be
            absolute.
        tool_base: This tool's :class:`normalize.paths.ToolPathBase`, the per-tool view
            over ``harness/artifacts/logs/runner-metadata.json``. Every base decision is
            taken from it and none is assumed: for the merged artifact this provisioning
            records ``scan_root``, while the retained per-directory reports under
            ``logs/trivy.parts/`` are ``per_section_target``.
        allowlist: The twelve authoritative globs, as loaded by
            :func:`normalize.paths.load_allowlist`. Consumed once into a tuple.
        tally: A :class:`normalize.severity.LiteralTally` (or anything exposing
            ``record(tool, result)``), fed once per emitted row so
            ``oss-scan-results/severity-map.md`` can list every observed literal with the
            rows it affected.
        section_base: The base a ``per_section_target`` reading needs, passed through to
            ``paths.py`` unchanged. ``None`` for the merged artifact, whose ``Target``
            values are root-relative; required by ``paths.py`` for a caller normalizing
            one of the unmerged per-directory reports, where passing nothing is a
            rejection rather than a silent reading against the root.

    Returns:
        A three-tuple ``(rows, rejections, counters)``:

        * ``rows`` -- a list of dicts, each carrying exactly the twelve fields of
          :data:`FIELDS` in that order, in document order;
        * ``rejections`` -- a list of :class:`normalize.paths.Rejection`, each under a
          named member of :data:`normalize.paths.REJECT_CLASSES` with its sub-reason
          retained verbatim;
        * ``counters`` -- a dict of ints over :data:`COUNTER_KEYS`.

        ``len(rows) + len(rejections)`` equals the number of elements walked across
        ``Results[]`` x (``Vulnerabilities`` | ``Secrets`` | ``Misconfigurations``), which
        is the same count unit :func:`normalize.reconcile.count_records` arrives at
        independently.

    Raises:
        UnsupportedTrivySection: On any of the four halt conditions in
            :func:`validate_finding_sections`. It propagates deliberately: AAP 0.5.4
            requires the run to stop with the observed structure quoted, and converting
            it into a counted rejection is precisely the outcome that requirement rules
            out.
        TrivyAdapterError: If an argument is not what the contract requires -- an unknown
            tool, a relative or non-text root, another tool's path base, a non-iterable
            allowlist, a tally that cannot record -- or if the document is not an object
            or carries a ``Results`` member that is not an array. A caller fault is
            raised rather than absorbed into a rejection count.
        normalize.severity.SeverityPolicyError: If ``tally`` is a ``LiteralTally`` and
            ``tool`` is outside its canonical vocabulary -- which cannot happen for
            ``trivy``, and is left to surface rather than be caught.

    A tool's exit code is never consulted: a valid artifact is normalized whatever its
    runner returned, since artifact status and exit status are independent (AAP 0.5.4).
    This runner's exit code is in any case an aggregate over eighteen invocations.
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

    # Pass one: the halt, over every element, before any row is built.  Raises rather
    # than returning, so no caller can proceed past a dropped finding section.
    validate_finding_sections(document, tool=canonical_tool, counters=counters)

    raw_results = document.get(_RESULTS_KEY)
    if raw_results is None:
        # A legitimate empty report (AAP 0.5.4), recorded rather than inferred from a
        # zero row count.  _validated_document has already refused a Results member that
        # is present as something other than an array.
        counters[COUNTER_RESULTS_ABSENT] = 1

    # Pass two: the rows, in document order -- Results[] in order, the three sections in
    # the fixed order of SUPPORTED_SECTIONS, each section's elements in order.
    for result_index, raw_result in enumerate(_json_array(raw_results)):
        counters[COUNTER_RESULTS] += 1
        element = _json_object(raw_result)
        if element is None:
            # Contributes no record, exactly as reconcile.py's traversal counts it.
            # Counted rather than passed over in silence, so the two agreeing on zero is
            # visible in normalize-run.json rather than merely assumed.
            counters[COUNTER_RESULTS_SKIPPED_NON_MAPPING] += 1
            continue

        target = element.get(_TARGET_KEY)
        ecosystem = element.get(_TYPE_KEY)
        packages = _json_array(element.get(_PACKAGES_KEY))
        held_a_record = False

        for section, _class_for_section in SUPPORTED_SECTIONS.items():
            # The section is chosen here and its name travels with every record read out
            # of it; adapt_record derives the class from that name alone.  A section key
            # absent or null contributes nothing and is not an error, exactly as
            # reconcile.py counts it.
            section_records = _json_array(element.get(section))
            if not section_records:
                continue
            held_a_record = True
            counters[f"{COUNTER_RECORDS_PREFIX}{section.lower()}"] += len(
                section_records
            )
            for record_index, raw_record in enumerate(section_records):
                outcome = adapt_record(
                    raw_record,
                    section=section,
                    target=target,
                    tool=canonical_tool,
                    root=root_text,
                    tool_base=base,
                    globs=globs,
                    tally=recorder,
                    counters=counters,
                    packages=packages,
                    ecosystem=ecosystem,
                    section_base=section_base,
                    result_index=result_index,
                    record_index=record_index,
                )
                if isinstance(outcome, paths.Rejection):
                    rejections.append(outcome)
                else:
                    rows.append(outcome)

        if not held_a_record:
            # A scanned target with nothing to report.  Ordinary rather than defective,
            # and validate_finding_sections has already established that the element
            # declares no findings it is not holding.
            counters[COUNTER_RESULTS_WITHOUT_SUPPORTED_SECTION] += 1

    return rows, rejections, counters
