"""harness/lib/normalize/adapters/dependency_check.py — the native-JSON adapter for OWASP Dependency-Check.

Serves the one tool whose canonical identifier is ``dependency-check`` (version
13.0.0 in this provisioning), per AAP 0.6.1's row *"One adapter per non-SARIF
artifact written"*.

Note the naming split, which is deliberate and which ``shape.py``'s routing table
carries too: the **canonical tool identifier is hyphenated** (``dependency-check``
-- the literal that goes in every row's ``tool`` field), while this **module
filename is the underscored Python identifier** (``dependency_check``).  AAP 0.5.4
is explicit that the identifier is produced mechanically from the runner and
artifact stem, so it is neither ``OWASP Dependency-Check`` nor ``dependency_check``
in any emitted field.

No user-specified rule governs this file, so enterprise-standard best practice
applies in its place (AAP 0.7, AAP 0.10.2), held to the AAP's own bar:
verification independent of the thing verified, reject rather than infer, and a
policy fixed before any output is observed.  Everything cited below is an AAP
*requirement*; none of it is a rule.

Position in the normalizer
--------------------------
A leaf that depends on exactly two modules.  AAP 0.6.4: *"each adapter depends on
``paths`` and ``severity`` and on nothing else."*  Taken literally --
:mod:`normalize.shape`, :mod:`normalize.cli`, :mod:`normalize.emit`,
:mod:`normalize.reconcile` and every sibling adapter (``sarif`` included, which is
read as the reference for this contract and never imported) are **not** imported,
and neither is any third-party package (AAP 0.4.1: standard library only, so this
run introduces no manifest, no lockfile and no install step, which AAP 0.4.3
forbids).

Two consequences are structural rather than stylistic:

* ``reconcile`` is unreachable from here, so the counting traversal that forms the
  left-hand side of ``raw finding records = dataset rows + rejected records`` cannot
  reuse a single line of row-building code.  That is the point: a count taken from
  the traversal that builds the rows satisfies the identity while testing nothing.
* ``emit.FIELDS`` and ``shape.SCANNER_CLASS_BY_TOOL`` cannot be imported, so
  :data:`FIELDS` and :data:`SCANNER_CLASS` below are authored copies that must agree
  with them **by construction**.  ``shape.py`` keeps the same separation from the
  other direction, naming this module by the string key ``dependency_check`` rather
  than importing it.

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
The count unit is ``dependencies[].vulnerabilities[]``: **one vulnerability is one
record** (AAP 0.5.4), which is exactly the unit ``reconcile._count_dependency_check``
walks.  Every vulnerability therefore yields **exactly one outcome -- one row or one
rejection, never both and never neither**.  :func:`_adapt_vulnerability` returns a
single value of one of those two types, so the invariant is structural rather than
asserted.

The traversal mirrors that independent count element for element, because a
divergence in what counts as "one record" would break the identity silently while
every individual assertion still passed:

===========================================  ================================
document shape                               contribution
===========================================  ================================
a ``dependencies`` element not an object      nothing (counted, not rejected)
a ``vulnerabilities`` value not an array      nothing (counted, not rejected)
a dependency with no vulnerabilities          nothing -- the ordinary shape
an element of ``vulnerabilities``             exactly one row or one rejection
===========================================  ================================

The third row is the common case rather than an edge case: provisioning measured 32
dependencies analysed with **zero** carrying a vulnerability, so a report with no
records at all is an expected outcome and not an error.  A dependency is not a
record; emitting a row per dependency would inflate every count in the dataset.

Document order is preserved -- ``dependencies[]`` in order and, within each,
``vulnerabilities[]`` in order -- since both output files use it and ``emit.py``
compares them ordered row by row.

Path resolution is delegated to ``paths.py``
--------------------------------------------
``path`` comes from the **enclosing dependency's** ``filePath``, which is why the
dependency object is carried down into the record handler: neither ``path`` nor
``package_coordinate`` can be built from the vulnerability alone.

The runner hands the tool 18 **absolute** ``--scan`` arguments in one invocation
(``harness/artifacts/logs/runner-metadata.json``, ``invocation_form.targets``), so
``filePath`` is filesystem-absolute and the recorded ``path_base.kind`` is
``filesystem_absolute`` with the scan root as its value.  AAP 0.5.4's path-base
table says *"dependency-check | filesystem-absolute | Relativize to SPARK_SRC"*, and
the relativization -- along with the ``../`` preservation for a location outside the
root, the ``<container>!<member>`` serialization, the emitted-path assertion and the
``in_scope`` matcher -- lives in ``paths.py``.  Not one of them is reimplemented
here.  This adapter passes the raw value and uses whatever
:func:`normalize.paths.resolve_dependency_check_path` returns.

One tool-specific convention is recognised here, because it is a fact about this
tool's output rather than about paths in general.  **Dependency-Check names a member
inside a container by concatenating the member onto the container path with a
``/``**, carrying no ``!`` at all -- measured on this provisioning's own tool:

    /opt/spark-src/…/spark-network-common_2.13-4.1.0-SNAPSHOT.jar/META-INF/maven/com.google.guava/guava/pom.xml

Handed to the resolver unchanged, that would relativize into a plausible-looking
path naming a file that does not exist on disk, and -- worse -- one whose segments
match an allowlist glob, so the row would take ``in_scope: true`` for a coordinate
that is not in the tree at all.  :func:`prepare_file_path` therefore inserts the
single ``!`` at the first segment whose extension names a container, keyed on
:data:`normalize.paths.ARCHIVE_EXTENSIONS` through
:func:`normalize.paths.looks_like_archive_container` so the rule is auditable, and
hands the result to the same resolver.  ``paths.py`` then performs the serialization
and the relativization; the split decides only *where* the container ends.  This is
the same shape ``paths.resolve_trivy_path`` already applies to the analogous Trivy
case.  A value that already carries a ``!`` is passed through untouched, since the
resolver's own archive branch handles it.

Severity: label over score, with the entry that governed recorded
-----------------------------------------------------------------
``severity_native`` comes from the record's ``severity`` field, *"with the CVSS entry
selected recorded alongside"* (AAP 0.5.4).  The decision itself is delegated in full
to :func:`normalize.severity.resolve`, which owns the precedence -- *"the native
label governs whenever it is in the mapped vocabulary, and a CVSS score is consulted
only where no mapped label exists.  Either way the entry used is recorded -- the
label, or the score with its source and version"* -- together with the
case-insensitive label map and the CVSS v3.1 section 5 band table.  Nothing is mapped
or banded locally.

Recording *which* entry governed is not optional here.  AAP 0.2.2: *"an advisory
commonly carries several scores from different sources, which is why the
implementation records which score entry it selected."*  A Dependency-Check
vulnerability routinely carries several CVSS blocks at once -- ``cvssv2`` with a
``score`` and ``cvssv3`` with a ``baseScore`` were both present on the very first
record measured -- so a band with no recorded selection is a band nobody can check.
Two seams make it checkable:

* :func:`score_candidates`, :func:`resolve_severity` and
  :func:`severity_literal_present` are **public**, so a caller
  rendering ``tool-status.md`` or ``severity-map.md``, and every adapter test, can
  obtain the same :class:`normalize.severity.SeverityResult` this adapter used and
  read its ``basis`` and its ``selected_entry`` per record.  The twelve-field row has
  nowhere to put a selection, and :func:`adapt`'s three-tuple is fixed, so exposing
  the seam is what keeps the requirement satisfiable without widening either;
* the counters carry one key per :data:`normalize.severity.BASIS_VALUES` member, plus
  :data:`COUNTER_SEVERITY_SELECTED_SCORE_WITH_SOURCE_AND_VERSION`, so the aggregate
  is visible in ``normalize-run.json`` without a per-row side channel.

Provenance inside a CVSS block is version-dependent, which is why
:func:`score_candidates` composes what a block omits rather than assuming every block
omits it.  ``templates/jsonReport.vsl`` in the pinned
``dependency-check-core-13.0.0.jar`` emits the ``cvssv2`` block (template lines
207-225) and the ``cvssv3`` block (227-242) with no ``source`` and no ``type`` member
and with ``version`` conditional on the metric, while the ``cvssv4`` block (244-257)
emits ``source`` and ``type`` whenever the underlying metric carries them.  A v2 or
v3 block therefore states no provenance of its own, so the selected entry's source is
composed from facts already in the artifact -- the record's own ``source`` (``NVD``,
``RETIREJS``, ``OSSINDEX``…) joined to the block's key, as ``NVD:cvssv3`` -- and its
version comes from the major the key itself states (``cvssv3`` names major 3).  A
block that does carry either field keeps it: each is supplied only where neither
spelling ``severity.py`` accepts is present, so a ``cvssv4`` ``source`` or ``type``
is consulted and reported as the selection's own provenance rather than displaced by
a composed key.  Neither supplied value is invented: a block named ``cvssv3`` *is* a
version-3 entry, and the record's ``source`` *is* the provenance of the scores under
it.

Two measured hazards, and why neither is handled locally
--------------------------------------------------------
*Case.*  Label literals arrive in mixed case: ``MODERATE`` and lower-case
``moderate`` name the same band.  ``severity.py``'s map is case-insensitive by
design, and this adapter passes the literal **exactly as the tool emitted it** so the
literal reaching the :class:`normalize.severity.LiteralTally` is the observed one.
``severity-map.md`` reports observed literals with per-literal row counts, so
upper-casing before the tally would misreport them as a literal the tool never wrote.

*Float representation.*  Severity values appear as raw floats carrying float32-to-
float64 representation artifacts -- ``3.200000047683716``, ``5.300000190734863`` --
alongside clean values such as ``7.5``, and as **strings** that happen to be numeric
as well as as JSON numbers.  The origin is a real code path: an OSS Index-sourced
record has no label and Dependency-Check renders the provider's ``float`` CVSS score
into the ``severity`` field, so the artifact carries the artifact tail.  This
provisioning passes ``--disableOssIndex``, so the path is unexercised by the
production artifact and still fully implemented and fixture-tested, because a
configuration is not a guarantee.  ``severity.py`` bands **numerically** -- so
``3.200000047683716`` bands ``Low`` exactly as ``3.2`` does -- and renders
``severity_native`` to one decimal place from the full-precision value, so no
spurious precision reaches a text field and no rounding can cross a band boundary.
A literal that is neither a mapped label nor a bandable number is disclosed as an
unmapped literal and banded ``Info`` with the rows it affected (AAP 0.5.4).
``severity_norm`` is never absent, on any path.

The package coordinate: this module is the folder's reference implementation
---------------------------------------------------------------------------
AAP 0.5.4 fixes one candidate precedence for every dependency-oriented shape, and
:func:`package_coordinate` implements it explicitly rather than taking the first
thing it finds:

1. a package URL on the **record**;
2. else a package URL on the **enclosing package object**;
3. else ``<ecosystem>:<name>@<version>`` from the **record's own** fields;
4. else the same from the **enclosing package's** fields.

The ecosystem is lower-cased, and *"Where several candidates sit at one level, the
lexicographically smallest wins"* -- which is a live case rather than a formality,
since a dependency routinely carries several ``packages[]`` entries.  A ``packages[]``
``id`` counts at level 2 only where it is a package URL (the ``pkg:`` scheme); the
CPE identifiers Dependency-Check also emits are not package URLs and are not
coerced into one.  At levels 3 and 4 a ``name`` is refused where it is the record's
own rule identifier or is CVE-shaped, because ``name`` on a Dependency-Check
vulnerability is the advisory identifier and reading it as a package name would put
``nvd:CVE-2018-17190@…`` in the field.

*"Where none can be formed the field is absent"* -- and because a Dependency-Check
record is dependency-oriented, that absence is a **rejection** under
:data:`normalize.paths.REJECT_UNFORMABLE_PACKAGE_COORDINATE` rather than a row with a
null field.  Not theoretical: provisioning measured 32 dependencies with **zero**
resolved package coordinates, all of them vendored web assets with no manifest
behind them, so the tool's only in-scope surface is precisely the surface that
produces this rejection.

Classification order, fixed so a class is reproducible
------------------------------------------------------
A record can be defective in more than one way at once, so the order in which the
checks run decides which class it is counted under.  The order is fixed and
documented rather than incidental:

1. the vulnerability is not an object -> ``malformed_record``;
2. no usable ``name`` -> ``missing_rule_id``, including a ``name`` that is present but
   is not a string: it identifies nothing either way, and the reason names which it
   was;
3. no usable ``description`` -> ``missing_message``, on the same reading;
4. the path -> ``absent_path``, ``unresolvable_path`` or ``malformed_record``, as
   ``paths.py`` classifies it;
5. no package coordinate at any level -> ``unformable_package_coordinate``.

Severity, ``cwe``/``cve`` and ``in_scope`` never reject: each has a defined value for
every input, so a record reaching them becomes a row.  ``start_line`` cannot reject
either, because this shape has none to reject -- Dependency-Check reports at
dependency granularity, so ``start_line`` is always ``None`` and is never
synthesised.

What this module does not do
----------------------------
AAP 0.3.2, in full force.  It performs no cross-tool interpretation of any kind: one
row per finding with the producing tool named, and two tools reporting the same
location produce two rows and no comment.  It judges nothing -- not real, not
important, not a false positive, not a duplicate.  It deduplicates nothing, not even
two identical vulnerabilities under one dependency: those are two records and two
rows.  It filters nothing.  A row naming one of the pin's 47 out-of-allowlist
``pom.xml`` files or its three lockfiles is a legitimate coordinate from a correctly
targeted runner and is **kept with** ``in_scope: false`` (AAP 0.9.3); AAP 0.8.3 is
explicit that only evidence about the *runner* establishes a wrong scan root, and an
individual out-of-tree coordinate never does.

It reads only the document handed to it and has no path-discovery logic of its own;
``cli.py`` supplies artifacts from ``harness/artifacts/raw/`` alone.  A tool's exit
code is never consulted: artifact status and exit status are independent (AAP 0.5.4),
so a valid artifact is normalized whatever its runner returned.  The runner's baked
flags are likewise none of this module's business -- this provisioning's runner passes
``--noupdate`` and ``--disableOssIndex`` under the 17 JDK, all three read from the
runner and recorded in ``runner-metadata.json`` and ``tool-status.md``, and **this
adapter neither adds, removes nor forces any of them**.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from os import fspath
from typing import Any, Final

from normalize import paths
from normalize import severity


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class DependencyCheckAdapterError(ValueError):
    """Raised where a *caller* hands this adapter something its contract forbids.

    Deliberately distinct from a rejection.  A rejection describes a defective
    *record* inside an artifact and is counted and carried on from; this exception
    describes a defective *call* -- the wrong tool identifier, a relative root,
    another tool's path base, a document that is not a Dependency-Check report --
    and stops the caller rather than being absorbed into a rejection count.

    A ``ValueError`` subclass rather than a bare ``assert``: ``python -O`` strips
    ``assert``, and an invariant that disappears under optimisation is not an
    invariant.  AAP 0.5.4's "reject rather than infer" governs record content; a
    caller fault is neither rejected nor inferred, it is raised.
    """


# --------------------------------------------------------------------------- #
# Fixed policy: the tool served, the scanner class, the twelve fields
# --------------------------------------------------------------------------- #

#: The canonical tool identifier, hyphenated, as it appears in every row's ``tool``
#: field and as the key of this tool's entry in ``runner-metadata.json``,
#: ``tool-status.md`` and ``severity-map.md``.
TOOL: Final[str] = "dependency-check"

#: The ``scanner_class`` every row from this adapter carries.
#:
#: AAP 0.5.4's class table fixes ``vuln`` for ``osv-scanner`` and
#: ``dependency-check``.  Trivy is the table's single per-record exception and this
#: tool is not it, so the value is a constant rather than something read off a
#: record.  It is authored here rather than imported from ``shape.py`` because AAP
#: 0.6.4 permits an adapter to import ``paths`` and ``severity`` and nothing else;
#: the duplication is required by that constraint, not an oversight -- and it is
#: fixed in advance rather than derived from what the artifact turns out to contain.
SCANNER_CLASS: Final[str] = "vuln"

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
#: ``path`` is not among them: AAP 0.5.4 states *"path is not an optional field"*, so
#: a record whose path cannot be resolved is rejected and counted rather than emitted
#: with a null path.  ``severity_norm`` is likewise never absent, which
#: ``severity.py`` enforces on every construction of its result.
ABSENCE_PERMITTED_FIELDS: Final[frozenset[str]] = frozenset(
    {"severity_native", "start_line", "cwe", "cve", "package_coordinate"}
)

#: ``start_line`` is always absent for this shape.  Dependency-Check reports at
#: dependency granularity -- a vulnerable component, not a line of code -- so there is
#: no line number to carry and synthesising one would be inference.  Named as a
#: constant so the absence is a stated policy rather than an omission a reader has to
#: infer from the row builder.
_START_LINE: Final[None] = None


# --------------------------------------------------------------------------- #
# Dependency-Check report member names, verified against a real 13.0.0 report
# --------------------------------------------------------------------------- #

_DEPENDENCIES_KEY: Final[str] = "dependencies"
_VULNERABILITIES_KEY: Final[str] = "vulnerabilities"
_FILE_PATH_KEY: Final[str] = "filePath"
_FILE_NAME_KEY: Final[str] = "fileName"
_RELATED_DEPENDENCIES_KEY: Final[str] = "relatedDependencies"
_PACKAGES_KEY: Final[str] = "packages"
_NAME_KEY: Final[str] = "name"
_DESCRIPTION_KEY: Final[str] = "description"
_SEVERITY_KEY: Final[str] = "severity"
_CWES_KEY: Final[str] = "cwes"
_SOURCE_KEY: Final[str] = "source"


# --------------------------------------------------------------------------- #
# Token patterns, compiled once
# --------------------------------------------------------------------------- #

# Matched as a whole token rather than as a substring of an unrelated identifier.
# The leading guard rejects an alphanumeric immediately before the prefix and the
# trailing guard rejects a following digit, so "CWE-791" can never be read as
# "CWE-79".  A hyphen before the prefix is deliberately *allowed*, because that is
# how a real vocabulary composes one -- and it is also why the digit requirement
# matters: Dependency-Check emits "NVD-CWE-noinfo" and "NVD-CWE-Other" for a
# vulnerability whose weakness is unknown, and neither yields an identifier, so such
# a record's ``cwe`` is absent rather than a fabricated "CWE-noinfo".
CWE_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?<![0-9A-Za-z])CWE-(\d+)(?![0-9])", re.IGNORECASE
)

#: ``CVE-<4-digit year>-<4-or-more-digit sequence>``, per AAP 0.5.4's stated pattern.
CVE_TOKEN_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"(?<![0-9A-Za-z])CVE-(\d{4})-(\d{4,})(?![0-9])", re.IGNORECASE
)

#: A CVSS block key on a vulnerability record: ``cvssv2``, ``cvssv3``, ``cvssv4``, and
#: whatever major version a later Dependency-Check emits.  The captured group is the
#: major version the key itself states, which is what lets
#: :func:`score_candidates` supply a version for a block that carries none without
#: inventing one.  Separator-tolerant (``cvss_v3``, ``cvss-v3``) so a spelling change
#: does not silently drop a score entry.
CVSS_BLOCK_KEY_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"\Acvss[ _-]?v?(\d+)\Z", re.IGNORECASE
)

#: A package URL, recognised by its ``pkg:`` scheme and nothing else.  A CPE such as
#: ``cpe:/a:jquery:jquery:3.5.1`` -- which Dependency-Check also emits, under
#: ``vulnerabilityIds[].id`` and occasionally under ``packages[].id`` -- is not a
#: package URL and is never coerced into one.
PACKAGE_URL_PATTERN: Final[re.Pattern[str]] = re.compile(r"\Apkg:\S+\Z", re.IGNORECASE)


# --------------------------------------------------------------------------- #
# Package-coordinate candidate sources (AAP 0.5.4's four levels)
# --------------------------------------------------------------------------- #

#: Keys a *record* may carry a package URL under.  A Dependency-Check vulnerability
#: carries none in any observed report; the level exists because AAP 0.5.4 defines it
#: first and a producer that starts emitting one must be honoured, not ignored.
_RECORD_PACKAGE_URL_KEYS: Final[tuple[str, ...]] = (
    "purl",
    "packageUrl",
    "packageURL",
    "package_url",
)

#: Keys a *package object* may carry a package URL under.  ``id`` leads because that
#: is where Dependency-Check puts it: ``{"id": "pkg:maven/com.google.guava/guava@33.4.0-jre"}``.
_PACKAGE_OBJECT_URL_KEYS: Final[tuple[str, ...]] = (
    "id",
    "purl",
    "packageUrl",
    "packageURL",
    "package_url",
)

#: Keys the ``<ecosystem>:<name>@<version>`` triple's ecosystem may arrive under.
_ECOSYSTEM_KEYS: Final[tuple[str, ...]] = (
    "ecosystem",
    "packageEcosystem",
    "package_ecosystem",
    "type",
)

#: Keys the triple's name may arrive under.  ``name`` is last and is guarded: on a
#: Dependency-Check vulnerability it holds the advisory identifier, not a package.
_PACKAGE_NAME_KEYS: Final[tuple[str, ...]] = (
    "packageName",
    "package_name",
    "artifactId",
    "artifact_id",
    "name",
)

#: Keys the triple's version may arrive under.
_PACKAGE_VERSION_KEYS: Final[tuple[str, ...]] = (
    "packageVersion",
    "package_version",
    "version",
)

#: The level a coordinate was formed at, in AAP 0.5.4's precedence order.  Reported
#: through the counters so the level that actually carried this artifact is visible
#: rather than assumed.
COORDINATE_LEVEL_RECORD_PACKAGE_URL: Final[str] = "record_package_url"
COORDINATE_LEVEL_PACKAGE_OBJECT_PACKAGE_URL: Final[str] = "package_object_package_url"
COORDINATE_LEVEL_RECORD_FIELDS: Final[str] = "record_ecosystem_name_version"
COORDINATE_LEVEL_PACKAGE_OBJECT_FIELDS: Final[str] = (
    "package_object_ecosystem_name_version"
)

#: The four levels, in precedence order.
COORDINATE_LEVELS: Final[tuple[str, ...]] = (
    COORDINATE_LEVEL_RECORD_PACKAGE_URL,
    COORDINATE_LEVEL_PACKAGE_OBJECT_PACKAGE_URL,
    COORDINATE_LEVEL_RECORD_FIELDS,
    COORDINATE_LEVEL_PACKAGE_OBJECT_FIELDS,
)


# --------------------------------------------------------------------------- #
# The counter key set.  Fixed and fully pre-initialised, so every call returns
# the same keys and a caller aggregating across artifacts never has to guess
# whether a missing key means zero or means "this adapter forgot".
# --------------------------------------------------------------------------- #

#: Dependencies walked, and the two dependency-level shapes that contribute no
#: record -- counted rather than silent, because ``reconcile.py``'s traversal counts
#: them as zero too and a reader comparing the two needs to see that the zero was
#: observed.  A dependency with an empty ``vulnerabilities`` array is the ordinary
#: shape and is counted under neither: it *has* the array, it is simply clean.
COUNTER_DEPENDENCIES: Final[str] = "dependencies"
COUNTER_DEPENDENCIES_SKIPPED_NON_MAPPING: Final[str] = "dependencies_skipped_non_mapping"
COUNTER_DEPENDENCIES_WITHOUT_VULNERABILITIES_ARRAY: Final[str] = (
    "dependencies_without_vulnerabilities_array"
)

#: Records whose enclosing dependency names more than one location -- a non-empty
#: ``relatedDependencies`` array, which is the only multi-location shape this report
#: has.  The row takes the primary ``filePath``; the record still counts once; this
#: is the number AAP 0.5.4 has reported per tool.
COUNTER_MULTI_LOCATION: Final[str] = "multi_location_records"

#: Records from which more than one distinct CWE or CVE identifier was collected.
#: The field carries one, chosen by ascending numeric identifier.
COUNTER_MULTI_VALUED_CWE: Final[str] = "multi_valued_cwe_records"
COUNTER_MULTI_VALUED_CVE: Final[str] = "multi_valued_cve_records"

#: Rows whose path names something other than a file in the scanned tree -- an
#: archive member or a location outside the root.  ``run-record.md`` reports the count
#: and the proportion (AAP 0.6.1).
COUNTER_NON_FILESYSTEM_PATHS: Final[str] = "non_filesystem_paths"

#: Records whose ``filePath`` was recognised as naming a member inside a container and
#: had the single ``!`` separator inserted before delegation.  Distinct from
#: :data:`COUNTER_NON_FILESYSTEM_PATHS`, which also counts a location outside the
#: root: this one isolates the tool-specific convention, so the split rule's reach is
#: visible rather than inferred from the kind tally.
COUNTER_ARCHIVE_REFERENCES_SPLIT: Final[str] = "archive_references_split"

#: The ``in_scope`` decomposition of the emitted rows.  Their sum is the row count, so
#: this is one measurement split rather than a second count of the same thing.
COUNTER_ROWS_IN_SCOPE: Final[str] = "rows_in_scope"
COUNTER_ROWS_OUT_OF_SCOPE: Final[str] = "rows_out_of_scope"

#: Rows carrying no ``start_line``.  Every row from this shape does, since
#: Dependency-Check reports at dependency granularity; the counter makes the policy
#: measurable rather than merely documented, and it equals the row count by design.
COUNTER_START_LINE_ABSENT: Final[str] = "start_line_absent"

#: Whether the record carried a ``severity`` literal at all, and whether any CVSS
#: block was available to consult.  Together with the ``severity_basis_*`` keys these
#: show *why* a band was reached: a label present with scores also present and a
#: ``severity_basis_label`` count is the label-over-score precedence, observed.
COUNTER_SEVERITY_LABEL_PRESENT: Final[str] = "severity_label_present"
COUNTER_SEVERITY_LABEL_ABSENT: Final[str] = "severity_label_absent"
COUNTER_SEVERITY_SCORE_CANDIDATES_PRESENT: Final[str] = "severity_score_candidates_present"

#: Score-governed rows whose selected entry named both a source and a version -- AAP
#: 0.5.4's *"the score with its source and version"*, in aggregate.  The per-record
#: selection is available through :func:`resolve_severity`, which is the seam a
#: caller uses to report it; this is the number that reaches
#: ``normalize-run.json`` without one.
COUNTER_SEVERITY_SELECTED_SCORE_WITH_SOURCE_AND_VERSION: Final[str] = (
    "severity_selected_score_with_source_and_version"
)

#: Rows carrying no ``cwe`` or no ``cve``, and records whose ``cwes`` array held at
#: least one entry from which no numeric identifier could be read -- the
#: ``NVD-CWE-noinfo`` case, which is common enough that its absence would otherwise
#: look like a defect in this adapter.
COUNTER_CWE_ABSENT: Final[str] = "cwe_absent"
COUNTER_CWE_ENTRIES_WITHOUT_IDENTIFIER: Final[str] = "cwe_entries_without_identifier"
COUNTER_CVE_ABSENT: Final[str] = "cve_absent"

#: Which of AAP 0.5.4's four levels formed the coordinate, plus the two conditions
#: worth seeing: several candidates at one level (the lexicographically smallest was
#: taken) and none at any level (the record was rejected).
COUNTER_COORDINATE_FROM_RECORD_PACKAGE_URL: Final[str] = (
    "package_coordinate_from_record_package_url"
)
COUNTER_COORDINATE_FROM_PACKAGE_OBJECT_PACKAGE_URL: Final[str] = (
    "package_coordinate_from_package_object_package_url"
)
COUNTER_COORDINATE_FROM_RECORD_FIELDS: Final[str] = (
    "package_coordinate_from_record_fields"
)
COUNTER_COORDINATE_FROM_PACKAGE_OBJECT_FIELDS: Final[str] = (
    "package_coordinate_from_package_object_fields"
)
COUNTER_COORDINATE_MULTIPLE_CANDIDATES: Final[str] = (
    "package_coordinate_multiple_candidates_at_level"
)
COUNTER_COORDINATE_UNFORMABLE: Final[str] = "package_coordinate_unformable"

#: The counter key each coordinate level increments.  A mapping rather than a chain
#: of conditionals, so a level added to :data:`COORDINATE_LEVELS` without a counter
#: raises here instead of being silently uncounted.
_COORDINATE_LEVEL_COUNTERS: Final[Mapping[str, str]] = {
    COORDINATE_LEVEL_RECORD_PACKAGE_URL: COUNTER_COORDINATE_FROM_RECORD_PACKAGE_URL,
    COORDINATE_LEVEL_PACKAGE_OBJECT_PACKAGE_URL: (
        COUNTER_COORDINATE_FROM_PACKAGE_OBJECT_PACKAGE_URL
    ),
    COORDINATE_LEVEL_RECORD_FIELDS: COUNTER_COORDINATE_FROM_RECORD_FIELDS,
    COORDINATE_LEVEL_PACKAGE_OBJECT_FIELDS: (
        COUNTER_COORDINATE_FROM_PACKAGE_OBJECT_FIELDS
    ),
}

#: Prefixes for the two vocabularies that are *derived* rather than authored: one key
#: per :data:`normalize.paths.PATH_KINDS` member and one per
#: :data:`normalize.severity.BASIS_VALUES` member.  Deriving them means this adapter's
#: counter set cannot drift from the vocabularies it reports against.
COUNTER_PATH_KIND_PREFIX: Final[str] = "path_kind_"
COUNTER_SEVERITY_BASIS_PREFIX: Final[str] = "severity_basis_"

_AUTHORED_COUNTER_KEYS: Final[tuple[str, ...]] = (
    COUNTER_DEPENDENCIES,
    COUNTER_DEPENDENCIES_SKIPPED_NON_MAPPING,
    COUNTER_DEPENDENCIES_WITHOUT_VULNERABILITIES_ARRAY,
    COUNTER_MULTI_LOCATION,
    COUNTER_MULTI_VALUED_CWE,
    COUNTER_MULTI_VALUED_CVE,
    COUNTER_NON_FILESYSTEM_PATHS,
    COUNTER_ARCHIVE_REFERENCES_SPLIT,
    COUNTER_ROWS_IN_SCOPE,
    COUNTER_ROWS_OUT_OF_SCOPE,
    COUNTER_START_LINE_ABSENT,
    COUNTER_SEVERITY_LABEL_PRESENT,
    COUNTER_SEVERITY_LABEL_ABSENT,
    COUNTER_SEVERITY_SCORE_CANDIDATES_PRESENT,
    COUNTER_SEVERITY_SELECTED_SCORE_WITH_SOURCE_AND_VERSION,
    COUNTER_CWE_ABSENT,
    COUNTER_CWE_ENTRIES_WITHOUT_IDENTIFIER,
    COUNTER_CVE_ABSENT,
    COUNTER_COORDINATE_FROM_RECORD_PACKAGE_URL,
    COUNTER_COORDINATE_FROM_PACKAGE_OBJECT_PACKAGE_URL,
    COUNTER_COORDINATE_FROM_RECORD_FIELDS,
    COUNTER_COORDINATE_FROM_PACKAGE_OBJECT_FIELDS,
    COUNTER_COORDINATE_MULTIPLE_CANDIDATES,
    COUNTER_COORDINATE_UNFORMABLE,
)

#: Every key :func:`new_counters` initialises, in a stable order.
#:
#: Note what is deliberately **absent**: there is no adapter-side count of the records
#: walked, and none of the rows or rejections produced.  ``len(rows)`` and
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

    ``None`` rather than an empty mapping, so a caller distinguishes "absent" from
    "present but not an object" and can classify the second as a malformed record.
    """
    return value if isinstance(value, Mapping) else None


def _non_empty_string(value: Any) -> str | None:
    """Return ``value`` stripped where it is a non-blank string, else ``None``.

    Whitespace-only is an absence rather than a value: a rule identifier of ``"   "``
    identifies nothing, and treating it as present would put a blank into a field the
    schema requires.
    """
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _type_name(value: Any) -> str:
    """Return ``value``'s type name, for a message that says what arrived."""
    return type(value).__name__



# --------------------------------------------------------------------------- #
# The one tool-specific path convention: a member named by concatenation
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class PreparedPath:
    """A ``filePath`` as it will be handed to ``paths.py``, and whether it was split.

    Attributes
    ----------
    value:
        The value to delegate.  Either the reported value untouched, or the same value
        with the single :data:`normalize.paths.ARCHIVE_SEPARATOR` inserted at the
        container boundary.  A non-string reported value is carried through unchanged
        so that ``paths.py`` classifies it, in the one place that classification
        lives.
    container:
        The container component where a split was performed, else ``None``.
    member:
        The member component where a split was performed, else ``None``.
    """

    value: Any
    container: str | None = None
    member: str | None = None

    @property
    def was_split(self) -> bool:
        """Whether the archive separator was inserted by :func:`prepare_file_path`."""
        return self.container is not None


def prepare_file_path(file_path: Any) -> PreparedPath:
    """Return ``file_path`` ready for ``paths.py``, splitting a concatenated member.

    Dependency-Check names a member inside a container by concatenating the member
    onto the container path with a ``/`` and no separator of any kind -- measured on
    this provisioning's own tool, which reported a Guava POM inside a Spark JAR as
    ``…/spark-network-common_2.13-4.1.0-SNAPSHOT.jar/META-INF/maven/com.google.guava/guava/pom.xml``.
    The shared resolver keys its archive branch on :data:`normalize.paths.ARCHIVE_SEPARATOR`,
    so without this step that coordinate would relativize into an ordinary-looking
    path naming a file that is not on disk, and its segments would match an allowlist
    glob -- giving ``in_scope: true`` for something the tree does not contain.

    The boundary is the **first** segment whose extension names a container, keyed on
    :data:`normalize.paths.ARCHIVE_EXTENSIONS` through
    :func:`normalize.paths.looks_like_archive_container` so the rule is auditable
    rather than buried in a condition.  First rather than last because the defined
    serialization carries exactly one ``!`` and the container is the outermost thing;
    a nested container therefore stays inside the member component, where it is a
    plain path segment and cannot introduce a second separator.

    Four values are returned unsplit, each for a stated reason:

    * a non-string, so ``paths.py`` classifies it as a malformed record;
    * a value already carrying a ``!``, since the resolver's archive branch handles
      it and inserting a second separator would make it unserializable;
    * a value whose **last** segment is the container, which names the archive itself
      rather than anything inside it -- the shape of a JAR reported as a dependency in
      its own right;
    * a value with no container segment at all, which is the ordinary case.
    """
    if not isinstance(file_path, str):
        return PreparedPath(value=file_path)
    normalised = paths.normalise_reported_path(file_path)
    if not normalised:
        return PreparedPath(value=file_path)
    if paths.ARCHIVE_SEPARATOR in normalised:
        return PreparedPath(value=file_path)

    if normalised.startswith("//"):
        prefix = "//"
    elif normalised.startswith("/"):
        prefix = "/"
    else:
        prefix = ""
    segments = paths.split_segments(normalised)
    # The last segment is excluded: a container there is the dependency itself.
    for index in range(len(segments) - 1):
        if not paths.looks_like_archive_container(segments[index]):
            continue
        container = prefix + "/".join(segments[: index + 1])
        member = "/".join(segments[index + 1 :])
        return PreparedPath(
            value=f"{container}{paths.ARCHIVE_SEPARATOR}{member}",
            container=container,
            member=member,
        )
    return PreparedPath(value=file_path)


# --------------------------------------------------------------------------- #
# Severity: the score candidates, and the delegated decision
# --------------------------------------------------------------------------- #


def score_candidates(vulnerability: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return the record's CVSS entries as candidates for :func:`normalize.severity.resolve`.

    Public because AAP 0.2.2 requires the implementation to record *which* score entry
    it selected, and a caller rendering ``tool-status.md`` needs the same candidate set
    this adapter passed in order to report the selection.  Pure: it reads the record
    and nothing else.

    Every key matching :data:`CVSS_BLOCK_KEY_PATTERN` contributes one candidate, in
    descending major version so the supplied order is deterministic -- ``severity.py``'s
    own selection order (highest version, then highest score, then the
    lexicographically smallest source, then the supplied index) makes the choice total
    either way, and a stable input order is what makes it reproducible.

    Two fields are supplied where the block omits them, and neither is invented:

    * ``version`` from the **major version the block's key states** -- a block named
      ``cvssv3`` is a version-3 entry -- and only where the block carries no version of
      its own, so a block declaring ``"version": "3.1"`` keeps that precision;
    * ``source`` from the record's own ``source`` composed with the block key, as
      ``NVD:cvssv3``, so the recorded selection names both the provider that supplied
      the score and which of the record's blocks it came from.  Where the record names
      no source, the block key alone is used.

    Both are supplied only when *neither* recognised spelling of the field is present
    (:mod:`normalize.severity` accepts ``source``/``type`` and
    ``version``/``cvss_version``), so this can never displace a value the artifact
    carried -- a ``cvssv4`` block, the one shape the pinned tool emits ``source`` and
    ``type`` on, keeps its own provenance and its own version precision.  Everything
    else in the block is passed through untouched; ``severity.py`` owns which key holds
    the score, which is why ``cvssv2``'s ``score`` and ``cvssv3``'s ``baseScore`` need
    no special case here.
    """
    record_source = _non_empty_string(vulnerability.get(_SOURCE_KEY))
    keyed: list[tuple[int, str, dict[str, Any]]] = []
    for key, value in vulnerability.items():
        if not isinstance(key, str):
            continue
        match = CVSS_BLOCK_KEY_PATTERN.match(key)
        if match is None:
            continue
        block = _json_object(value)
        if block is None:
            # A CVSS member that is not an object carries no score entry to select.
            # Not a rejection: severity is defined for every input, and a record whose
            # only score entry is unreadable still bands from its label or discloses
            # its literal.
            continue
        major = int(match.group(1))
        candidate = dict(block)
        if not any(version_key in candidate for version_key in ("version", "cvss_version")):
            candidate["version"] = str(major)
        if not any(source_key in candidate for source_key in ("source", "type")):
            candidate["source"] = f"{record_source}:{key}" if record_source else key
        keyed.append((major, key, candidate))
    keyed.sort(key=lambda entry: (-entry[0], entry[1]))
    return [candidate for _, _, candidate in keyed]


def severity_literal_present(vulnerability: Mapping[str, Any]) -> bool:
    """Return whether the record carries a severity literal at all.

    Blank and whitespace-only count as **absent**, which is the reading
    ``severity.py`` takes -- *"an empty or whitespace-only literal is treated as an
    absence rather than as a literal to disclose, because there is no literal there to
    list"*.  Sharing that reading is what keeps
    :data:`COUNTER_SEVERITY_LABEL_PRESENT` from claiming a literal for a row whose
    basis says ``no_vocabulary``.
    """
    raw = vulnerability.get(_SEVERITY_KEY)
    if raw is None:
        return False
    if isinstance(raw, str):
        return bool(raw.strip())
    return True


def resolve_severity(
    vulnerability: Mapping[str, Any],
    *,
    candidates: Sequence[Mapping[str, Any]] | None = None,
) -> severity.SeverityResult:
    """Return the record's severity, decided entirely by ``severity.py``.

    ``candidates`` lets a caller that has already built the record's score candidates
    pass them in rather than have them rebuilt; ``None`` means build them here.  It
    exists so this function is the **single** resolution path -- the row builder needs
    the candidate list for a counter and must not acquire its own second copy of the
    label extraction, which is exactly how the seam and the row would drift apart.

    Public for the same reason as :func:`score_candidates`: the returned
    :class:`normalize.severity.SeverityResult` carries the ``basis`` and the
    ``selected_entry`` AAP 0.5.4 requires be recorded, the twelve-field row has nowhere
    to put them, and :func:`adapt`'s three-tuple is fixed -- so this is the seam a
    caller and every adapter test use to read the selection per record.

    The label is passed **exactly as observed**, neither case-folded nor coerced.
    ``severity.py``'s label map is case-insensitive, so lower-case ``moderate`` bands
    ``Medium`` while the literal the :class:`normalize.severity.LiteralTally` records
    stays the literal the tool wrote -- which is what ``severity-map.md`` reports with
    its per-literal row counts.  A numeric ``severity`` -- a JSON number or a numeric
    string, including the float32-to-float64 artifacts an OSS Index-sourced record
    carries -- is banded numerically there and rendered to one decimal place, so
    ``3.200000047683716`` bands ``Low`` exactly as ``3.2`` does and no artifact tail
    reaches a text field.
    """
    return severity.resolve(
        label=vulnerability.get(_SEVERITY_KEY),
        scores=score_candidates(vulnerability) if candidates is None else list(candidates),
    )


# --------------------------------------------------------------------------- #
# CWE and CVE: one value each, by ascending numeric identifier (AAP 0.5.4)
# --------------------------------------------------------------------------- #


def _cwe_numbers(entries: Iterable[Any]) -> tuple[set[int], int]:
    """Return the distinct CWE numbers in ``entries``, and how many yielded none.

    Three shapes are read, because all three occur: ``"CWE-79"``, a bare integer or
    numeric string, and a decorated form such as ``"CWE-79 Improper Neutralization…"``
    or ``"NVD-CWE-noinfo"``.  The first two are read directly; the third is scanned
    with :data:`CWE_TOKEN_PATTERN`, which finds every token in it and finds nothing in
    ``NVD-CWE-noinfo`` -- so that entry contributes no identifier and is counted as
    such rather than becoming a fabricated ``CWE-noinfo``.

    ``bool`` is excluded even though it is an ``int`` subclass: ``True`` is not
    ``CWE-1``.  A float is excluded for the same reason -- a weakness identifier is an
    integer, and reading ``79.0`` as one would be a coercion this policy does not make.
    """
    numbers: set[int] = set()
    without_identifier = 0
    for entry in entries:
        if isinstance(entry, bool):
            without_identifier += 1
            continue
        if isinstance(entry, int):
            numbers.add(entry)
            continue
        if isinstance(entry, str):
            text = entry.strip()
            if text.isdigit():
                numbers.add(int(text))
                continue
            found = CWE_TOKEN_PATTERN.findall(text)
            if found:
                numbers.update(int(number) for number in found)
                continue
        without_identifier += 1
    return numbers, without_identifier


def select_cwe(entries: Iterable[Any]) -> tuple[str | None, int, int]:
    """Return ``(cwe, distinct_count, entries_without_identifier)`` for a ``cwes`` array.

    The value is the **lowest** numeric identifier, canonicalised to ``CWE-<n>``.  AAP
    0.5.4: one value per field, chosen by ascending numeric identifier, an ordering
    that is *"total, so no tie arises and no producer-order tiebreak is needed"*.
    ``distinct_count`` is what the multi-valued counter reports; Dependency-Check
    commonly reports several, so this path is genuinely exercised.

    The third element is the number of entries from which no identifier could be read
    -- the ``NVD-CWE-noinfo`` case.  Returned from the same single traversal rather
    than recomputed by the caller, so the two numbers cannot disagree about the array
    they describe.
    """
    numbers, without_identifier = _cwe_numbers(entries)
    if not numbers:
        return None, 0, without_identifier
    return f"CWE-{min(numbers)}", len(numbers), without_identifier


def select_cve(value: Any) -> tuple[str | None, int]:
    """Return ``(cve, distinct_count)`` for a record's ``name``.

    AAP 0.5.4: ``cve`` <- ``name`` **when CVE-shaped**.  Dependency-Check also reports
    non-CVE advisory identifiers -- ``GHSA-…``, RetireJS' own identifiers -- and those
    belong in ``rule_id`` alone, so a name matching nothing yields ``None`` rather than
    being copied across.

    Selection is by ascending year then sequence, with the observed sequence text as a
    final tiebreak so the order stays total where two identifiers differ only in
    leading zeros.  The emitted form carries the upper-case prefix with the digits
    exactly as observed.
    """
    if not isinstance(value, str):
        return None, 0
    found = CVE_TOKEN_PATTERN.findall(value)
    if not found:
        return None, 0
    identifiers = {f"CVE-{year}-{sequence}" for year, sequence in found}
    ordered = sorted(
        identifiers,
        key=lambda identifier: (
            int(identifier.split("-")[1]),
            int(identifier.split("-")[2]),
            identifier,
        ),
    )
    return ordered[0], len(identifiers)



# --------------------------------------------------------------------------- #
# The package coordinate: AAP 0.5.4's four candidate levels, in precedence order
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class PackageCoordinate:
    """One canonical package coordinate, with the level it was formed at.

    Attributes
    ----------
    value:
        The coordinate as it will reach the dataset's ``package_coordinate`` field:
        either a package URL exactly as the artifact carried it, or
        ``<ecosystem>:<name>@<version>`` with the ecosystem lower-cased.
    level:
        One of :data:`COORDINATE_LEVELS` -- which of AAP 0.5.4's four candidate levels
        supplied it.  Recorded rather than discarded so ``tool-status.md`` can state
        how this artifact's coordinates were actually formed.
    candidates_at_level:
        How many candidates that level offered.  More than one means the
        lexicographically smallest was taken, which is the AAP's stated tiebreak and
        the reason a dependency carrying several ``packages[]`` entries is resolved
        explicitly rather than by taking the first.
    """

    value: str
    level: str
    candidates_at_level: int


def _package_url_candidates(
    holder: Mapping[str, Any],
    keys: Iterable[str],
) -> list[str]:
    """Return every package URL ``holder`` carries under ``keys``, deduplicated.

    A value counts only where it matches :data:`PACKAGE_URL_PATTERN` -- the ``pkg:``
    scheme.  Dependency-Check puts a CPE under the same ``id`` key on some package
    objects, and a CPE is not a package URL: coercing one into the field would put a
    coordinate in the dataset that no package manager can resolve.
    """
    found: list[str] = []
    for key in keys:
        candidate = _non_empty_string(holder.get(key))
        if candidate is None:
            continue
        if PACKAGE_URL_PATTERN.match(candidate) is None:
            continue
        if candidate not in found:
            found.append(candidate)
    return found


def _first_present_string(holder: Mapping[str, Any], keys: Iterable[str]) -> str | None:
    """Return the first non-blank string ``holder`` carries under ``keys``."""
    for key in keys:
        candidate = _non_empty_string(holder.get(key))
        if candidate is not None:
            return candidate
    return None


def _triple_candidates(
    holder: Mapping[str, Any],
    *,
    rule_id: str | None,
) -> list[str]:
    """Return ``<ecosystem>:<name>@<version>`` for ``holder``, or an empty list.

    All three components are required: a coordinate missing its version identifies a
    package but not the one that was found, so a partial triple is treated as no
    candidate rather than emitted with a gap.  The **ecosystem is lower-cased** (AAP
    0.5.4); the name and version are carried exactly as observed, since a package name
    is case-sensitive in several ecosystems and folding it would name a different
    package.

    The name is refused where it is the record's own rule identifier or is CVE-shaped.
    That guard is load-bearing rather than defensive: ``name`` on a Dependency-Check
    vulnerability *is* the advisory identifier, so without it every record would form
    a plausible-looking coordinate such as ``nvd:CVE-2018-17190@…`` and the
    unformable-coordinate rejection this shape genuinely produces would never fire.
    """
    name = _first_present_string(holder, _PACKAGE_NAME_KEYS)
    if name is None:
        return []
    if rule_id is not None and name == rule_id:
        return []
    if CVE_TOKEN_PATTERN.search(name) is not None:
        return []
    ecosystem = _first_present_string(holder, _ECOSYSTEM_KEYS)
    version = _first_present_string(holder, _PACKAGE_VERSION_KEYS)
    if ecosystem is None or version is None:
        return []
    return [f"{ecosystem.lower()}:{name}@{version}"]


def package_coordinate(
    vulnerability: Mapping[str, Any],
    dependency: Mapping[str, Any],
    *,
    rule_id: str | None = None,
) -> PackageCoordinate | None:
    """Return the record's canonical package coordinate, or ``None`` if none can be formed.

    AAP 0.5.4's precedence, evaluated in order and stopping at the first level that
    offers a candidate:

    1. a package URL on the **record**;
    2. else a package URL on the **enclosing package object** -- for this shape, the
       enclosing dependency's ``packages[]`` entries, which is where Dependency-Check
       puts ``pkg:maven/…``;
    3. else ``<ecosystem>:<name>@<version>`` from the **record's own** fields;
    4. else the same from the **enclosing package's** fields.

    Where several candidates sit at one level the **lexicographically smallest** wins,
    and the count is carried on the result so the tiebreak's use is visible.  Levels 2
    and 4 pool the candidates from every ``packages[]`` entry before comparing, because
    the AAP's tiebreak is over the level rather than over one object -- taking the first
    entry's value instead would make the coordinate depend on producer order.

    ``None`` is a **rejection condition** for this shape, not an absent field: a
    Dependency-Check record is dependency-oriented, so AAP 0.5.4 makes an unformable
    coordinate ``unformable_package_coordinate``.  Public so the adapter test can
    exercise each level in turn, and so a caller can report which level carried an
    artifact.

    ``vulnerabilityIds[]`` is deliberately not consulted: it carries CPEs, which are
    not the coordinate form the schema defines, and reading one as a coordinate would
    be the inference AAP 0.5.4 forbids.
    """
    packages = [
        package
        for package in (_json_object(entry) for entry in _json_array(dependency.get(_PACKAGES_KEY)))
        if package is not None
    ]

    levelled: tuple[tuple[str, list[str]], ...] = (
        (
            COORDINATE_LEVEL_RECORD_PACKAGE_URL,
            _package_url_candidates(vulnerability, _RECORD_PACKAGE_URL_KEYS),
        ),
        (
            COORDINATE_LEVEL_PACKAGE_OBJECT_PACKAGE_URL,
            [
                candidate
                for package in packages
                for candidate in _package_url_candidates(package, _PACKAGE_OBJECT_URL_KEYS)
            ],
        ),
        (
            COORDINATE_LEVEL_RECORD_FIELDS,
            _triple_candidates(vulnerability, rule_id=rule_id),
        ),
        (
            COORDINATE_LEVEL_PACKAGE_OBJECT_FIELDS,
            [
                candidate
                for package in packages
                for candidate in _triple_candidates(package, rule_id=rule_id)
            ],
        ),
    )

    for level, candidates in levelled:
        distinct = sorted(set(candidates))
        if not distinct:
            continue
        return PackageCoordinate(
            value=distinct[0],
            level=level,
            candidates_at_level=len(distinct),
        )
    return None



# --------------------------------------------------------------------------- #
# Argument validation.  A caller fault is raised, never rejected.
# --------------------------------------------------------------------------- #


def _validated_tool(tool: Any) -> str:
    """Return ``tool`` where it is this adapter's canonical identifier, else raise.

    One module serves exactly one tool here, so the argument is a check rather than a
    selector -- but it is still required, because ``cli.py`` resolves every adapter
    through the same uniform entry point and a mis-routed artifact would otherwise be
    stamped with the wrong ``tool`` field on every row.
    """
    if not isinstance(tool, str):
        raise DependencyCheckAdapterError(
            f"tool must be the canonical identifier {TOOL!r}; observed "
            f"{_type_name(tool)}"
        )
    if tool != TOOL:
        raise DependencyCheckAdapterError(
            f"{tool!r} is not the tool this adapter serves ({TOOL!r}). The canonical "
            "identifier is hyphenated while this module's filename is the underscored "
            "Python identifier, and only the hyphenated form ever reaches a row"
        )
    return tool


def _validated_root(root: Any) -> str:
    """Return the scan root as an absolute POSIX-normalised string, else raise.

    A :class:`pathlib.Path` and a string are both accepted -- ``os.fspath`` is the one
    thing ``os`` is imported for -- and the result is normalised through ``paths.py`` so
    that this module and every resolver agree on the root's spelling.

    A relative root is refused here rather than at the first record: ``paths.py`` raises
    on one because a relative root cannot anchor anything, and *every* path from this
    tool is absolute and relativized against it, so accepting one would produce a
    plausible-looking wrong answer for every row of this artifact.
    """
    try:
        candidate = fspath(root)
    except TypeError as error:
        raise DependencyCheckAdapterError(
            f"root must be a str or an os.PathLike naming the SPARK_SRC root; observed "
            f"{_type_name(root)}"
        ) from error
    if isinstance(candidate, bytes):
        raise DependencyCheckAdapterError(
            "root must be a text path, not bytes: every path in the dataset is text, "
            "and decoding one here would guess an encoding"
        )
    if not candidate:
        raise DependencyCheckAdapterError("root must not be empty")
    normalised = paths.normalise_reported_path(candidate)
    if not paths.is_absolute_path(normalised):
        raise DependencyCheckAdapterError(
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
    the recorded runner metadata"* to prevent.  For this tool the recorded kind is
    ``filesystem_absolute`` with the scan root as its value, because the runner passes
    18 absolute ``--scan`` arguments; the kind is read rather than asserted here, since
    a differing base is a condition to record and never a runner to repair.
    """
    if not isinstance(tool_base, paths.ToolPathBase):
        raise DependencyCheckAdapterError(
            f"tool_base must be a paths.ToolPathBase built from the runner metadata; "
            f"observed {_type_name(tool_base)}"
        )
    if tool_base.tool != tool:
        raise DependencyCheckAdapterError(
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
    adapter.  What is checked is that each glob is a non-empty string, since a
    non-string pattern would raise from the matcher on the first row rather than on the
    call.
    """
    if isinstance(allowlist, (str, bytes)):
        raise DependencyCheckAdapterError(
            "allowlist must be an iterable of glob strings, not a single string: a "
            "string would be iterated character by character"
        )
    if not isinstance(allowlist, Iterable):
        raise DependencyCheckAdapterError(
            f"allowlist must be an iterable of glob strings from paths.load_allowlist(); "
            f"observed {_type_name(allowlist)}"
        )
    globs = tuple(allowlist)
    for index, glob in enumerate(globs):
        if not isinstance(glob, str) or not glob:
            raise DependencyCheckAdapterError(
                f"allowlist entry {index} must be a non-empty glob string; observed "
                f"{glob!r}"
            )
    return globs


def _validated_tally(tally: Any) -> Any:
    """Return ``tally`` where it can record a severity result, else raise.

    The capability is checked rather than the class, so a test double is as acceptable
    as a :class:`normalize.severity.LiteralTally`.  ``None`` is not: every row's literal
    has to reach ``severity-map.md``, and a silently skipped tally would leave that
    document under-reporting with nothing to show it had.
    """
    recorder = getattr(tally, "record", None)
    if not callable(recorder):
        raise DependencyCheckAdapterError(
            f"tally must expose a callable record(tool, result) -- normally a "
            f"severity.LiteralTally; observed {_type_name(tally)}"
        )
    return tally


def _validated_document(doc: Any) -> Mapping[str, Any]:
    """Return ``doc`` where it is a Dependency-Check report this adapter can walk, else raise.

    Two things are required: an object top level, and a ``dependencies`` array.  A real
    13.0.0 report carries ``reportSchema``, ``scanInfo``, ``projectInfo`` and
    ``dependencies``; the last is the only one this adapter reads, so it is the only one
    required -- checking the others would reject a valid report for carrying less
    metadata than one particular version happened to.

    Raising rather than returning zero rows is the point, and it is where this differs
    from ``reconcile.py``'s deliberately tolerant traversal, which counts a missing or
    non-array ``dependencies`` as zero.  AAP 0.5.4 makes *"an artifact matching neither
    the SARIF shape nor a known native shape"* a **halt**, because an empty result set is
    indistinguishable from a clean scan -- and for this tool a clean scan is the expected
    outcome, so a silent zero here would be invisible.  A report whose ``dependencies``
    array is **empty** is not that case: it is a legitimate clean report and walks to
    zero rows without complaint.
    """
    document = _json_object(doc)
    if document is None:
        raise DependencyCheckAdapterError(
            f"a {TOOL} artifact's top level is an object; observed {_type_name(doc)}. "
            "Shape detection belongs to shape.py, which routes an artifact here by the "
            "tool that wrote it once it is not SARIF"
        )
    if _DEPENDENCIES_KEY not in document:
        raise DependencyCheckAdapterError(
            f"the document carries no {_DEPENDENCIES_KEY!r} array, so it is not the "
            f"{TOOL} report shape this adapter was routed for; a report with nothing to "
            f"say still carries an empty {_DEPENDENCIES_KEY!r} array"
        )
    if not _is_json_array(document.get(_DEPENDENCIES_KEY)):
        raise DependencyCheckAdapterError(
            f"the document's {_DEPENDENCIES_KEY!r} is a "
            f"{_type_name(document.get(_DEPENDENCIES_KEY))}, not an array"
        )
    return document



# --------------------------------------------------------------------------- #
# One vulnerability -> exactly one outcome
# --------------------------------------------------------------------------- #


def _adapt_vulnerability(
    vulnerability: Any,
    *,
    dependency: Mapping[str, Any],
    tool: str,
    root: str,
    tool_base: paths.ToolPathBase,
    globs: tuple[str, ...],
    tally: Any,
    dependency_index: int,
    vulnerability_index: int,
    counters: dict[str, int],
) -> dict[str, Any] | paths.Rejection:
    """Return one row **or** one rejection for one ``dependencies[].vulnerabilities[]`` element.

    Exactly one of the two, always.  The single return value is what makes the
    one-to-one property structural: there is no path through this function that emits
    both and none that emits neither, so
    ``dataset rows + rejected records == the records walked`` holds by construction
    rather than by an assertion that could be forgotten.

    ``dependency`` is the **enclosing** dependency object, carried down because both
    ``path`` and ``package_coordinate`` come from it and a design that lost it could
    build neither.

    The classification order is the one this module's docstring fixes: shape, rule
    identifier, message, path, then package coordinate.  Severity, ``cwe``/``cve`` and
    ``in_scope`` cannot reject -- each is defined for every input -- so a record that
    reaches them becomes a row.

    Nothing is caught broadly here.  Each lookup and conversion is guarded where it
    happens, so a genuine programming error propagates instead of being converted into
    a rejection count that would satisfy reconciliation while hiding a defect.
    """
    identity: dict[str, Any] = {
        "dependency_index": dependency_index,
        "vulnerability_index": vulnerability_index,
        "fileName": dependency.get(_FILE_NAME_KEY),
        "filePath": dependency.get(_FILE_PATH_KEY),
    }

    record = _json_object(vulnerability)
    if record is None:
        return paths.make_rejection(
            paths.REJECT_MALFORMED_RECORD,
            tool,
            f"the {_VULNERABILITIES_KEY} element is a {_type_name(vulnerability)}, not "
            "an object, so no finding can be read from it",
            **identity,
        )

    rule_id = _non_empty_string(record.get(_NAME_KEY))
    if rule_id is None:
        raw_name = record.get(_NAME_KEY)
        if raw_name is None:
            reason = f"the vulnerability carries no {_NAME_KEY}"
        elif isinstance(raw_name, str):
            reason = f"the vulnerability's {_NAME_KEY} is empty or whitespace only"
        else:
            reason = (
                f"the vulnerability's {_NAME_KEY} is a {_type_name(raw_name)}, not a "
                "string, so it identifies nothing"
            )
        return paths.make_rejection(
            paths.REJECT_MISSING_RULE_ID,
            tool,
            reason,
            **identity,
        )
    identity[_NAME_KEY] = rule_id

    message = _non_empty_string(record.get(_DESCRIPTION_KEY))
    if message is None:
        raw_description = record.get(_DESCRIPTION_KEY)
        if raw_description is None:
            reason = f"the vulnerability carries no {_DESCRIPTION_KEY}"
        elif isinstance(raw_description, str):
            reason = (
                f"the vulnerability's {_DESCRIPTION_KEY} is empty or whitespace only"
            )
        else:
            reason = (
                f"the vulnerability's {_DESCRIPTION_KEY} is a "
                f"{_type_name(raw_description)}, not a string"
            )
        return paths.make_rejection(
            paths.REJECT_MISSING_MESSAGE,
            tool,
            reason,
            **identity,
        )

    # The multi-location count is a property of the record, so it is taken before the
    # path is resolved (AAP 0.5.4: the row takes the first location, the record still
    # counts once, and the number is reported per tool).  For this shape the only
    # multi-location form is the enclosing dependency's relatedDependencies array --
    # the same component found at more than one place in the tree -- and the row takes
    # the primary filePath.
    related = dependency.get(_RELATED_DEPENDENCIES_KEY)
    if _is_json_array(related) and len(related) > 0:
        counters[COUNTER_MULTI_LOCATION] += 1

    # Every base decision about the path is delegated to paths.py; the only
    # tool-specific work is inserting the archive separator at the container boundary,
    # which prepare_file_path documents in full.
    prepared = prepare_file_path(dependency.get(_FILE_PATH_KEY))
    resolved = paths.resolve_dependency_check_path(
        prepared.value,
        root,
        tool_base,
        tool=tool,
        record_identity=identity,
    )
    if isinstance(resolved, paths.Rejection):
        # Returned as-is: paths.py has already named the class and written the
        # sub-reason -- an absent filePath, a non-string one, or a base its recorded
        # kind supplies nothing for.  Rewording it here would lose that.
        return resolved
    if prepared.was_split:
        counters[COUNTER_ARCHIVE_REFERENCES_SPLIT] += 1

    # A package coordinate that cannot be formed at any of the four candidate levels
    # is a rejection for this shape, because the record is dependency-oriented (AAP
    # 0.5.4), rather than a row with a null field.
    coordinate = package_coordinate(record, dependency, rule_id=rule_id)
    if coordinate is None:
        counters[COUNTER_COORDINATE_UNFORMABLE] += 1
        return paths.make_rejection(
            paths.REJECT_UNFORMABLE_PACKAGE_COORDINATE,
            tool,
            "no package coordinate can be formed at any of the four candidate levels: "
            f"the record carries no package URL under {', '.join(_RECORD_PACKAGE_URL_KEYS)}, "
            f"the enclosing dependency's {_PACKAGES_KEY} supplies none under "
            f"{', '.join(_PACKAGE_OBJECT_URL_KEYS)}, and neither the record nor a package "
            "object carries an ecosystem, name and version to form "
            "<ecosystem>:<name>@<version> from",
            **identity,
        )
    counters[_COORDINATE_LEVEL_COUNTERS[coordinate.level]] += 1
    if coordinate.candidates_at_level > 1:
        counters[COUNTER_COORDINATE_MULTIPLE_CANDIDATES] += 1

    # From here nothing can reject: this record is a row.
    if severity_literal_present(record):
        counters[COUNTER_SEVERITY_LABEL_PRESENT] += 1
    else:
        counters[COUNTER_SEVERITY_LABEL_ABSENT] += 1
    candidates = score_candidates(record)
    if candidates:
        counters[COUNTER_SEVERITY_SCORE_CANDIDATES_PRESENT] += 1
    # One resolution path: the candidate list is handed to the same public seam a
    # caller uses, so the counters and the recorded selection describe one decision.
    severity_result = resolve_severity(record, candidates=candidates)
    counters[f"{COUNTER_SEVERITY_BASIS_PREFIX}{severity_result.basis}"] += 1
    selected = severity_result.selected_entry
    if (
        severity_result.basis == severity.BASIS_CVSS_SCORE
        and selected is not None
        and selected.get("source") is not None
        and selected.get("version") is not None
    ):
        counters[COUNTER_SEVERITY_SELECTED_SCORE_WITH_SOURCE_AND_VERSION] += 1
    # The tally is fed once per emitted row, which is what makes severity-map.md's
    # per-literal counts the row counts it reports them as.  A rejected record
    # contributes no row, so counting one here would put a literal in that document
    # against rows the dataset does not contain.
    tally.record(tool, severity_result)

    cwe, cwe_count, cwe_without_identifier = select_cwe(
        _json_array(record.get(_CWES_KEY))
    )
    cve, cve_count = select_cve(rule_id)
    if cwe_count > 1:
        counters[COUNTER_MULTI_VALUED_CWE] += 1
    if cve_count > 1:
        counters[COUNTER_MULTI_VALUED_CVE] += 1
    if cwe is None:
        counters[COUNTER_CWE_ABSENT] += 1
    if cwe_without_identifier > 0:
        counters[COUNTER_CWE_ENTRIES_WITHOUT_IDENTIFIER] += 1
    if cve is None:
        counters[COUNTER_CVE_ABSENT] += 1

    counters[f"{COUNTER_PATH_KIND_PREFIX}{resolved.kind}"] += 1
    if resolved.is_non_filesystem_coordinate:
        counters[COUNTER_NON_FILESYSTEM_PATHS] += 1
    counters[COUNTER_START_LINE_ABSENT] += 1

    # in_scope is decided by the allowlist alone, through paths.py's matcher, on the
    # resolved path and carrying its kind -- so an archive member cannot match a glob on
    # its segments and the literal src/test exclusion is applied once, where it lives.
    # Nothing is ever filtered on it: a row outside the allowlist -- one of the pin's 47
    # out-of-scope pom.xml files or its three lockfiles, which this tool legitimately
    # reaches -- is kept with in_scope false and counted (AAP 0.9.3).
    in_scope = bool(resolved.in_scope(globs))
    counters[COUNTER_ROWS_IN_SCOPE if in_scope else COUNTER_ROWS_OUT_OF_SCOPE] += 1

    row: dict[str, Any] = {
        "tool": tool,
        "scanner_class": SCANNER_CLASS,
        "rule_id": rule_id,
        "message": message,
        "severity_native": severity_result.severity_native,
        "severity_norm": severity_result.severity_norm,
        "path": resolved.path,
        "start_line": _START_LINE,
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
) -> tuple[list[dict[str, Any]], list[paths.Rejection], dict[str, int]]:
    """Turn one Dependency-Check JSON artifact into dataset rows, rejections and counters.

    This is the uniform adapter entry point: every adapter module in this package
    exposes ``adapt`` with this shape, so ``cli.py``'s registry resolves it with
    ``getattr(module, "adapt")`` and every adapter test calls it directly.

    Args:
        doc: The **already-parsed** artifact document -- a mapping for this shape.
            Parsing and shape detection happen upstream, which is what lets a test
            exercise every behaviour on a fixture with no filesystem.
        tool: The canonical tool identifier, which for this adapter is :data:`TOOL`.
            Required rather than assumed, because ``cli.py`` resolves every adapter
            through one entry point and a mis-route must fail loudly.
        root: The ``SPARK_SRC`` root, as a :class:`pathlib.Path` or a string. Must be
            absolute: every ``filePath`` this tool reports is absolute and is expressed
            against it.
        tool_base: This tool's :class:`normalize.paths.ToolPathBase`, the per-tool view
            over ``harness/artifacts/logs/runner-metadata.json``. Every base decision is
            taken from it and none is assumed.
        allowlist: The twelve authoritative globs, as loaded by
            :func:`normalize.paths.load_allowlist`. Consumed once into a tuple.
        tally: A :class:`normalize.severity.LiteralTally` (or anything exposing
            ``record(tool, result)``), fed once per emitted row so
            ``oss-scan-results/severity-map.md`` can list every observed literal with the
            rows it affected.

    Returns:
        A three-tuple ``(rows, rejections, counters)``:

        * ``rows`` -- a list of dicts, each carrying exactly the twelve fields of
          :data:`FIELDS` in that order, in document order (``dependencies[]`` in order
          and, within each, ``vulnerabilities[]`` in order);
        * ``rejections`` -- a list of :class:`normalize.paths.Rejection`, each under a
          named member of :data:`normalize.paths.REJECT_CLASSES` with its sub-reason
          retained verbatim;
        * ``counters`` -- a dict of ints over :data:`COUNTER_KEYS`.

        ``len(rows) + len(rejections)`` equals the number of
        ``dependencies[].vulnerabilities[]`` elements walked, which is the same count
        unit :func:`normalize.reconcile.count_records` arrives at independently.

    Raises:
        DependencyCheckAdapterError: If an argument is not what the contract requires --
            the wrong tool, a relative or non-text root, another tool's path base, a
            non-iterable allowlist, a tally that cannot record, or a document that is not
            a Dependency-Check report. A caller fault is raised rather than absorbed into
            a rejection count.
        normalize.severity.SeverityPolicyError: If ``tally`` is a ``LiteralTally`` and
            ``tool`` is outside its canonical vocabulary -- which cannot happen for this
            tool, and is left to surface rather than be caught.

    A tool's exit code is never consulted: a valid artifact is normalized whatever its
    runner returned, since artifact status and exit status are independent (AAP 0.5.4).
    An artifact with zero records is the expected outcome for this tool on this scope --
    provisioning measured 32 dependencies with none carrying a vulnerability -- and
    yields three empty results rather than anything resembling an error.
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

    for dependency_index, raw_dependency in enumerate(
        _json_array(document.get(_DEPENDENCIES_KEY))
    ):
        counters[COUNTER_DEPENDENCIES] += 1
        dependency = _json_object(raw_dependency)
        if dependency is None:
            # Contributes no record, exactly as reconcile.py's traversal counts it.
            # Counted rather than passed over in silence, so the two agreeing on zero is
            # visible in normalize-run.json rather than merely assumed.
            counters[COUNTER_DEPENDENCIES_SKIPPED_NON_MAPPING] += 1
            continue
        raw_vulnerabilities = dependency.get(_VULNERABILITIES_KEY)
        if not _is_json_array(raw_vulnerabilities):
            # A dependency with no vulnerabilities array, or one that is not an array,
            # contributes nothing and is not an error: most dependencies have none, and
            # a dependency is not a record.
            counters[COUNTER_DEPENDENCIES_WITHOUT_VULNERABILITIES_ARRAY] += 1
            continue

        for vulnerability_index, raw_vulnerability in enumerate(raw_vulnerabilities):
            outcome = _adapt_vulnerability(
                raw_vulnerability,
                dependency=dependency,
                tool=canonical_tool,
                root=root_text,
                tool_base=base,
                globs=globs,
                tally=recorder,
                dependency_index=dependency_index,
                vulnerability_index=vulnerability_index,
                counters=counters,
            )
            if isinstance(outcome, paths.Rejection):
                rejections.append(outcome)
            else:
                rows.append(outcome)

    return rows, rejections, counters
