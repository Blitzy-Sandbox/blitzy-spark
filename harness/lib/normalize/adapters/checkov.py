"""harness/lib/normalize/adapters/checkov.py — the native-JSON adapter for Checkov 3.3.12.

One of the five per-tool native adapters AAP 0.6.1 specifies with *"One adapter per
non-SARIF artifact written"*.  It serves exactly one tool, ``checkov``, whose
``scanner_class`` AAP 0.5.4's class table fixes at ``misconfig`` and which never
varies -- unlike ``trivy``, the single tool whose class is decided per record.

No user-specified rule governs this file; enterprise-standard best practice applies
in its place (AAP 0.7, AAP 0.10.2), held to the AAP's own methodology bar (AAP
0.1.3): verification independent of the thing verified, **reject rather than
infer**, and a policy fixed before any output is observed.  Everything cited below
is an AAP *requirement*; none of it is a rule, and inventing one would be
fabrication.

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
  with them **by construction**.  ``shape.py`` keeps the same separation from the
  other direction, naming this adapter by string key rather than importing it.

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

Both top-level shapes, and why the shape varies with content
------------------------------------------------------------
This is the first of the two things that make this the trickiest native adapter.
AAP 0.5.4's checkov row: *"both shapes are detected and handled -- the object form
``results.failed_checks[]``, and the multi-framework form, a top-level array of
report objects, counted as the union of every element's ``results.failed_checks[]``;
one failed check either way"*.

The counter-intuitive part, and the reason it is stated here rather than left to be
rediscovered: **the top-level shape changes with the artifact's content, not with
the invocation.**  A single JSON *object* appears when one framework reports, and a
JSON *array* of ``{check_type, results: {failed_checks: [...]}}`` report objects
appears when more than one does.  Provisioning measured the object form with
``check_type`` ``dockerfile``, because the only IaC content in the twelve
authoritative roots is the three Kubernetes Dockerfiles under
``resource-managers/kubernetes/docker/src/main`` -- so a single framework reported.
Add one YAML manifest a second framework recognises and the same runner, unchanged,
writes the array form instead.  An implementation that handled only the shape it
happened to see first would work on one run and fail on the next, which is why
:func:`_report_sequence` reduces both forms to **one** sequence of report objects
and exactly one record loop walks it.  A single path is also what makes the two
shapes provably equivalent: the dual-shape test asserts equivalent content in
either form produces identical rows, and it could not assert that of two forks.

The count unit, and the invariant that rests on it
--------------------------------------------------
The count unit is **one failed check** -- the union of ``results.failed_checks[]``
across every report object, in either shape.  That is exactly the unit
``reconcile.count_records`` walks for this tool, and the traversal here mirrors
``reconcile._count_checkov`` element for element, because a divergence in what
counts as "one record" would break the identity silently while every individual
assertion still passed:

===========================================  ================================
document shape                               contribution
===========================================  ================================
a top level that is a JSON array             each element is one report object
a top level that is a JSON object            the document is one report object
an array element that is not an object       nothing (counted, not rejected)
a ``results`` that is not an object          nothing (counted, not rejected)
a ``failed_checks`` that is not an array     nothing (counted, not rejected)
an element of ``failed_checks``              exactly one row or one rejection
===========================================  ================================

The third row deserves its reason.  A non-object array element *is* a malformed
artifact condition, and it is surfaced -- under
:data:`COUNTER_REPORTS_SKIPPED_NON_MAPPING` -- but it is **counted rather than
rejected**, because ``reconcile._count_checkov`` reads it through its own
``_as_mapping`` and contributes zero for it.  Emitting a rejection would make
``rows + rejections`` exceed the independent count by one and break the identity in
the direction hardest to notice.  ``adapters/sarif.py`` treats a non-object ``runs``
element the same way and for the same reason.

Two of those container shapes cannot reach this module in a run
---------------------------------------------------------------
``shape.NATIVE_SIGNATURES["checkov"]`` requires a ``checkov.json`` to be *either* a
JSON object carrying a ``results`` **object**, *or* a JSON array whose **every**
element is a JSON object carrying a ``results`` object.  ``shape.route`` halts on
anything else with ``shape.REASON_NATIVE_SIGNATURE_MISMATCH`` (AAP 0.5.4: an artifact
matching neither the SARIF shape nor a known native shape is a halt rather than a
best-effort parse; AAP 0.9.2 lists it among the conditions that stop the run).  So
two rows of the table above -- an array element that is not an object, and a
``results`` that is not an object -- are **unreachable from ``cli.py``**: such a
document stops the run before an adapter is named.  A ``failed_checks`` that is not an
array still reaches here, because the signature deliberately does not read inside
``results``: an empty or absent ``failed_checks`` is a legitimate report and a
signature that required the array would reject one.

The handling of all three is nevertheless kept exactly as it is, as the second line of
defence rather than as dead code.  This adapter is called directly by
``oss-scan-results/adapter-tests/`` and could be called directly by a later consumer,
and the counted-not-rejected treatment is what keeps the reconciliation identity
honest whenever it *is* reached.  What the two layers must never do is disagree about
which of them halts, which is why the boundary is stated here rather than left to be
inferred: shape belongs to ``shape.py``, and per-record attribution belongs here.

Every failed check therefore yields **exactly one outcome -- one row or one
rejection, never both and never neither**.  :func:`_adapt_failed_check` returns a
single value of one of those two types, so the invariant is structural rather than
asserted.  Document order is preserved throughout: report objects in order, and
within each, ``failed_checks`` in order, because both output files use that order
and ``emit.py`` compares them row by row.

Failures only, and ``parsing_errors`` as status evidence
--------------------------------------------------------
AAP 0.5.4, as a structural check: *"Checkov's ``passed_checks`` and
``skipped_checks`` are neither counted nor emitted in either shape -- only failures
are findings ... ``parsing_errors`` are reported in ``tool-status.md`` as status
evidence."*  AAP 0.2.2 confirms the ``results`` object carries all three buckets
alongside a ``parsing_errors`` list, *"which is why the adapter counts and emits
failed checks only, and treats parsing errors as status evidence rather than as
findings."*

So ``passed_checks`` and ``skipped_checks`` are never read into a row and never
enter any count that feeds reconciliation.  They are counted -- under
:data:`COUNTER_PASSED_CHECKS_OBSERVED` and
:data:`COUNTER_SKIPPED_CHECKS_OBSERVED` -- purely so the failures-only contract is
*observable*: provisioning measured ``passed=201 failed=6 skipped=0``, and a reader
of ``tool-status.md`` can see that 201 passes were present and produced no row.

``parsing_errors`` are neither findings nor rejections.  A rejection describes a
*record* this adapter could not attribute; a parsing error is Checkov's own report
about a file it could not read, and it is not one of the counted ``failed_checks``.
Converting one into a :class:`normalize.paths.Rejection` would therefore corrupt the
reconciliation identity.  They are surfaced two ways, both non-destructive: the
count in :data:`COUNTER_PARSING_ERRORS`, and the entries **verbatim** from
:func:`collect_parsing_errors`, which ``cli.py`` carries into ``tool-status.md``
whose per-tool contract requires *"records parsed and rejected with each class named
and any parser error verbatim"*.  :func:`report_summaries` returns each report's own
``check_type`` and ``summary`` object verbatim beside them, so the tool's statement
about itself is available without this module ever publishing a substitute for the
record count.  Provisioning measured ``parsing_errors=0``; the channel exists so a
future non-zero one cannot vanish.

The path convention: the user's own worked example
--------------------------------------------------
This is the second thing that makes this adapter the trickiest, and the field the
user singled out.  AAP 0.5.3 requires the example be carried in unchanged.

``file_path`` is scan-target-relative *and* carries a leading slash, as in
``/folder1/A.tf``; ``file_abs_path`` holds the filesystem-absolute path and
``repo_file_path`` a root-relative path that also carries a leading slash.  The
triple is verified in a real report at
``https://github.com/bridgecrewio/checkov/issues/3047``, and the JSON output option
that produces it is documented at
``https://www.checkov.io/2.Basics/CLI%20Command%20Reference.html``.

The failure the example warns about is silent rather than loud: **read that leading
slash as filesystem-absolute and the path relativizes to a long ``../`` chain, so
the row quietly takes ``in_scope: false``.**  Nothing crashes; a whole tool's rows
are simply wrong in the same direction, which is far harder to notice than an error.

And the reconciliation is the *reliable route* rather than a mere cross-check.  This
provisioning's runner invokes ``checkov`` with **one ``-d`` per expanded scope
directory** -- eighteen of them in a single invocation, which is why
``runner-metadata.json`` records ``path_base.kind`` ``per_target_directory`` with
``anchor_fields`` ``[repo_file_path, file_abs_path]``.  So the slash-stripped
``file_path`` is relative to *whichever ``-d`` root matched*, not to ``SPARK_SRC``:
a measured record from the pinned tree carries ``file_path``
``/dockerfiles/spark/bindings/R/Dockerfile`` beside ``repo_file_path``
``/resource-managers/kubernetes/docker/src/main/dockerfiles/spark/bindings/R/Dockerfile``.
A naive strip-and-join against the tree root names a directory that does not exist,
**even after the leading slash is handled correctly** -- and that is the single most
likely defect in this file.  ``file_abs_path`` (or ``repo_file_path``) is what
disambiguates which target a record came from.

Not one of those decisions is taken here.  The whole failed check, the root and the
per-tool :class:`~normalize.paths.ToolPathBase` are handed to
:func:`normalize.paths.resolve_checkov_path`, and whatever it returns is used: the
leading-slash strip, the anchor order, the anchor-against-anchor reconciliation, the
``file_path`` corroboration, the ``../`` preservation and the ``in_scope`` matcher
with true zero-or-more-directories ``**`` semantics all live there.  Where the pair
cannot be reconciled -- no anchor field present under ``per_target_directory``, so
there is no way to know which of eighteen targets a bare ``file_path`` came from --
that module returns an ``unresolvable_path`` rejection and this adapter counts it,
rather than picking one of the two candidates.  AAP 0.1.3: *"Where a record cannot
be attributed with certainty, it is rejected and the rejection recorded as a class
with its count -- never guessed into a field."*  A disagreement that *is*
resolvable is recorded on :attr:`~normalize.paths.ResolvedPath.corroboration` and
counted here under :data:`COUNTER_PATH_CORROBORATION_RECORDED`, never suppressing
the row.

Classification order, fixed so a class is reproducible
------------------------------------------------------
A record can be defective in more than one way at once, so the order in which the
checks run decides which class it is counted under.  The order is fixed and
documented rather than incidental:

1. the failed check is not an object -> ``malformed_record``;
2. no ``check_id`` -> ``missing_rule_id``;
3. no ``check_name`` -> ``missing_message``;
4. the path -> ``absent_path``, ``unresolvable_path`` or ``malformed_record``, as
   ``paths.py`` classifies it;
5. a ``file_line_range`` that is present but not usable -> ``malformed_record`` for
   a non-array or for an array carrying no first element, ``non_integer_start_line``
   for a first element that is present and unusable.

Severity, ``cwe``/``cve``, ``package_coordinate`` and ``in_scope`` never reject:
each has a defined value for every input, so a record reaching step 5 becomes a row.
``package_coordinate`` in particular is always ``None`` here and its absence is
**not** a rejection: AAP 0.5.4 makes an unformable coordinate a rejection condition
only for a *dependency-oriented* record, and a misconfiguration record is not one.

What this module does not do
----------------------------
AAP 0.3.2, in full force.  It performs no cross-tool interpretation of any kind:
one row per finding with the producing tool named, and two tools reporting the same
location produce two rows and no comment.  It judges nothing -- not real, not
important, not a false positive, not a duplicate -- so no failed check is ever
filtered or ranked.  It deduplicates nothing, not even two identical failed checks
in one artifact: those are two records and two rows.  A row outside the allowlist is
kept with ``in_scope: false`` and counted (AAP 0.9.3); only evidence about the
*runner* could establish a wrong scan root (AAP 0.8.3), and that is not a judgement
an adapter makes.

It also has **no path-discovery logic of its own**, deliberately: it reads only the
document handed to it, and ``cli.py`` only ever passes artifacts from
``harness/artifacts/raw/``.  A tool's exit code is never consulted -- Checkov exits
non-zero *precisely because* it found something (measured: exit 1 with six findings),
and AAP 0.5.4 makes artifact status and exit status independent, so a valid artifact
is normalized whatever its runner returned.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping, Sequence
from os import fspath
from typing import Any, Final

from normalize import paths
from normalize import severity

# Sorted, and deliberately a subset: the counter-name and member-name constants
# below are public and importable but stay out of ``__all__``, exactly as
# ``adapters/sarif.py`` keeps its own counter names out of its.  What is listed is
# the contract a caller needs -- the entry point, the status channels, the schema
# constants and the error type.
__all__ = [
    "ABSENCE_PERMITTED_FIELDS",
    "CHECK_ID_FIELD",
    "CHECK_NAME_FIELD",
    "COUNTER_KEYS",
    "CVE_TOKEN_PATTERN",
    "CWE_TOKEN_PATTERN",
    "CheckovAdapterError",
    "EMITTED_RESULT_SECTION",
    "FIELDS",
    "IDENTIFIER_SOURCE_FIELDS",
    "NEVER_EMITTED_RESULT_SECTIONS",
    "SCANNER_CLASS",
    "TOOL",
    "adapt",
    "collect_parsing_errors",
    "new_counters",
    "report_summaries",
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class CheckovAdapterError(ValueError):
    """Raised where a *caller* hands this adapter something its contract forbids.

    Deliberately distinct from a rejection.  A rejection describes a defective
    *record* inside an artifact and is counted and carried on from; this exception
    describes a defective *call* -- the wrong tool identifier, a relative root,
    another tool's path base, a document whose top level is neither of Checkov's two
    shapes -- and stops the caller rather than being absorbed into a rejection count.

    A ``ValueError`` subclass rather than a bare ``assert``: ``python -O`` strips
    ``assert``, and an invariant that disappears under optimisation is not an
    invariant.  AAP 0.5.4's "reject rather than infer" governs record content; a
    caller fault is neither rejected nor inferred, it is raised.
    """


# --------------------------------------------------------------------------- #
# Fixed policy: the tool, the scanner class, the twelve fields
# --------------------------------------------------------------------------- #

#: The one canonical tool identifier this adapter serves (AAP 0.5.4).
#:
#: Canonical identifiers are the lower-case stems the runners and artifacts carry,
#: which for this tool is also a legal Python module name -- unlike
#: ``datadog-static-analyzer``, whose adapter file is underscored.  ``tool`` is still
#: a required argument to :func:`adapt` so that every adapter in the package exposes
#: one signature, and it is validated rather than ignored.
TOOL: Final[str] = "checkov"

#: The ``scanner_class`` every row from this adapter carries.
#:
#: AAP 0.5.4's class table fixes ``misconfig`` for ``checkov`` and it never varies --
#: not by ``check_type``, not by framework, not by record content.  Only ``trivy``
#: varies, and it varies by the section a record was read from.  Authored here rather
#: than imported from ``shape.py`` because AAP 0.6.4 permits an adapter to import
#: ``paths`` and ``severity`` and nothing else; the duplication is required by that
#: constraint, and it is fixed in advance rather than derived from what the artifact
#: turns out to contain.
SCANNER_CLASS: Final[str] = "misconfig"

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

#: ``package_coordinate`` is always ``None`` for this shape.  A misconfiguration
#: names a location in a configuration file, not a package, and manufacturing a
#: coordinate from a file path would be inference.  Its absence is **not** a
#: rejection condition here: AAP 0.5.4 attaches ``unformable_package_coordinate`` to
#: a *dependency-oriented* record, which this is not.
_PACKAGE_COORDINATE: Final[None] = None


# --------------------------------------------------------------------------- #
# Checkov member names.  The three the path resolution turns on are taken from
# paths.py so the two modules cannot spell them differently; the rest are local
# because paths.py has no reason to know them.
# --------------------------------------------------------------------------- #

#: The per-report container holding the three result buckets and the parse errors.
RESULTS_KEY: Final[str] = "results"

#: The **only** result bucket that holds findings (AAP 0.5.4).  Named as a constant
#: so the failures-only contract is legible at every use site, and so it reads
#: identically to ``reconcile.CHECKOV_COUNTED_SECTION``, whose independent traversal
#: walks the same bucket.
EMITTED_RESULT_SECTION: Final[str] = "failed_checks"

#: The two buckets that are **never** counted and never emitted, in either shape.
#: Read only to count them as status evidence, which is what makes "produced no row"
#: an observation rather than a claim.
NEVER_EMITTED_RESULT_SECTIONS: Final[tuple[str, ...]] = (
    "passed_checks",
    "skipped_checks",
)

#: Checkov's own report about files it could not read.  Status evidence, never a
#: finding and never a rejection -- see this module's docstring.
PARSING_ERRORS_KEY: Final[str] = "parsing_errors"

#: The report's own summary object, returned verbatim by :func:`report_summaries`
#: for ``tool-status.md``.  The measured 3.3.12 artifact carries six keys --
#: ``checkov_version``, ``passed``, ``failed``, ``skipped``, ``parsing_errors`` and
#: ``resource_count`` -- and the object is passed through whole rather than read key
#: by key, so a seventh key a newer Checkov emits travels with it untouched.  It is
#: the tool's own statement about itself and is treated as status evidence only: no
#: count in it is ever read as the record count, which comes from walking
#: :data:`EMITTED_RESULT_SECTION`, and none of it reaches a dataset row.
SUMMARY_KEY: Final[str] = "summary"

#: Which framework produced a report object.  Preserved per report for the rejection
#: records and for status reporting -- and it does **not** vary
#: :data:`SCANNER_CLASS`, which is fixed at ``misconfig``.
CHECK_TYPE_KEY: Final[str] = "check_type"

#: ``rule_id`` <- ``check_id`` (AAP 0.5.4).
CHECK_ID_FIELD: Final[str] = "check_id"

#: Checkov's alternate identifier.  **Not** a fallback for ``check_id``: AAP 0.5.4
#: names ``check_id`` and substituting another field would put an identifier in the
#: dataset the row's own named source never carried.  It is read only to make a
#: ``missing_rule_id`` rejection's detail more useful.
BC_CHECK_ID_FIELD: Final[str] = "bc_check_id"

#: ``message`` <- ``check_name`` (AAP 0.5.4).
CHECK_NAME_FIELD: Final[str] = "check_name"

#: ``severity_native`` <- ``severity``, which requires a licence and is therefore
#: ``null`` on every row in this configuration (AAP 0.5.4: *"absent in the
#: unlicensed configuration"*; AAP 0.3.2 provisions no credential).
SEVERITY_FIELD: Final[str] = "severity"

#: ``start_line`` <- ``file_line_range[0]`` (AAP 0.5.4).
FILE_LINE_RANGE_FIELD: Final[str] = "file_line_range"

#: The calling file, where a finding reached its resource through a module call.
#: This is the only second-location shape Checkov emits, so it is what
#: :data:`COUNTER_MULTI_LOCATION` counts -- and it is never used for the row's path,
#: which always comes from the primary location.
CALLER_FILE_PATH_FIELD: Final[str] = "caller_file_path"

#: The fixed, closed set of fields CWE and CVE identifiers are collected from.
#:
#: Checkov reports misconfigurations rather than package vulnerabilities, so most
#: records carry no weakness identifier at all and all three of ``cwe``, ``cve`` and
#: ``package_coordinate`` are ``None``.  Where a record genuinely does carry one it
#: is in its guideline or metadata, and these are the fields that hold it -- fixed in
#: advance (AAP 0.1.3) rather than tuned to what the artifact turns out to contain.
#:
#: ``check_name`` is deliberately **excluded**: it is the row's ``message``, it is
#: prose about the misconfiguration, and AAP 0.5.4's checkov row names no identifier
#: source on it.  Harvesting an identifier out of a message would put a weakness
#: reference in the dataset that the record never made as a reference.
IDENTIFIER_SOURCE_FIELDS: Final[tuple[str, ...]] = (
    "guideline",
    "description",
    "short_description",
    "bc_category",
    "benchmarks",
    "details",
    "vulnerability_details",
)

#: How deep :func:`_identifier_sources` walks a nested identifier source before it
#: stops.  ``vulnerability_details`` and ``benchmarks`` are objects of lists of
#: strings, so three levels reach every string either can hold; a bound is set at all
#: because an artifact is untrusted input and an unbounded walk over a deeply nested
#: value is a denial of service on the normalizer.
_IDENTIFIER_SOURCE_MAX_DEPTH: Final[int] = 3


# --------------------------------------------------------------------------- #
# CWE and CVE token patterns, compiled once (AAP 0.5.4)
# --------------------------------------------------------------------------- #

# Character-for-character the patterns ``adapters/sarif.py`` uses, because a token
# has to mean the same thing in every tool's rows or the dataset's cwe column is not
# one column.  Matched as a whole token rather than as a substring of an unrelated
# word: the leading guard rejects an alphanumeric immediately before the prefix, so
# "NOTCWE-79" yields nothing while "external/cwe/cwe-079" and "A03:2021-CWE-79" both
# do -- a hyphen or a slash before the prefix is how real vocabularies compose one.
# The trailing guard rejects a following digit, so "CWE-791" can never be read as
# "CWE-79".  Matching is case-insensitive because a producer that differs only in
# case is naming the same weakness; the emitted value takes the upper-case prefix.
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
#
# Every value is an ``int``, without exception.  A caller aggregating several
# artifacts adds them, and a list or a string smuggled in under one key would
# make that addition raise -- which is why the verbatim parsing errors and
# summaries travel through :func:`collect_parsing_errors` and
# :func:`report_summaries` instead of through here.
# --------------------------------------------------------------------------- #

#: Which of the two top-level shapes this artifact carried.  Exactly one of the pair
#: is ``1`` and the other ``0``, so ``normalize-run.json`` records the shape actually
#: observed rather than the one a reader assumed -- the whole point of a shape that
#: varies with content.
COUNTER_TOP_LEVEL_FORM_ARRAY: Final[str] = "top_level_form_array"
COUNTER_TOP_LEVEL_FORM_OBJECT: Final[str] = "top_level_form_object"

#: Report objects walked, and the three container shapes that contribute no record.
#: Counted rather than passed over in silence, because ``reconcile.py`` counts them
#: as zero too and a reader comparing the two needs to see that the zero was
#: observed rather than assumed.
COUNTER_REPORTS: Final[str] = "reports"
COUNTER_REPORTS_SKIPPED_NON_MAPPING: Final[str] = "reports_skipped_non_mapping"
COUNTER_REPORTS_WITHOUT_RESULTS_OBJECT: Final[str] = "reports_without_results_object"
COUNTER_REPORTS_WITHOUT_FAILED_CHECKS_ARRAY: Final[str] = (
    "reports_without_failed_checks_array"
)

#: The two never-emitted buckets, counted so the failures-only contract is
#: observable.  Neither feeds reconciliation and neither ever produces a row.
COUNTER_PASSED_CHECKS_OBSERVED: Final[str] = "passed_checks_observed"
COUNTER_SKIPPED_CHECKS_OBSERVED: Final[str] = "skipped_checks_observed"

#: Checkov's own parse errors.  Status evidence, not findings: this count never
#: enters the reconciliation identity, and the entries themselves are available
#: verbatim from :func:`collect_parsing_errors`.
COUNTER_PARSING_ERRORS: Final[str] = "parsing_errors"

#: Records carrying more than one location -- for this tool, a non-empty
#: ``caller_file_path`` beside the primary one.  The row takes the first location;
#: the record still counts once; this is the number AAP 0.5.4 has reported per tool.
COUNTER_MULTI_LOCATION: Final[str] = "multi_location_records"

#: Records from which more than one distinct CWE or CVE identifier was collected.
#: The field carries one, chosen by ascending numeric identifier.
COUNTER_MULTI_VALUED_CWE: Final[str] = "multi_valued_cwe_records"
COUNTER_MULTI_VALUED_CVE: Final[str] = "multi_valued_cve_records"

#: Rows whose path names something other than a file in the scanned tree -- for this
#: tool, a location outside the root, which is what an anchor pointing outside
#: ``SPARK_SRC`` resolves to.  ``run-record.md`` reports the count and the proportion
#: (AAP 0.6.1).
COUNTER_NON_FILESYSTEM_PATHS: Final[str] = "non_filesystem_paths"

#: The ``in_scope`` decomposition of the emitted rows.  Their sum is the row count,
#: so this is one measurement split rather than a second count of the same thing.
COUNTER_ROWS_IN_SCOPE: Final[str] = "rows_in_scope"
COUNTER_ROWS_OUT_OF_SCOPE: Final[str] = "rows_out_of_scope"

#: Rows whose resolved path came with a recorded disagreement between the anchor
#: fields, or between an anchor and ``file_path``.  AAP 0.5.3 has such a mismatch
#: *recorded* rather than resolved, and this is the number that makes it visible; a
#: non-zero count is the user's worked example biting, which is exactly the thing a
#: silent implementation would hide.
COUNTER_PATH_CORROBORATION_RECORDED: Final[str] = "path_corroboration_recorded"

#: Whether a record carried a severity literal at all.  In this configuration
#: severities require a licence, so ``severity_absent`` is expected to equal the row
#: count exactly -- and that expectation being *checkable* is why the pair is here.
COUNTER_SEVERITY_PRESENT: Final[str] = "severity_present"
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
    COUNTER_TOP_LEVEL_FORM_ARRAY,
    COUNTER_TOP_LEVEL_FORM_OBJECT,
    COUNTER_REPORTS,
    COUNTER_REPORTS_SKIPPED_NON_MAPPING,
    COUNTER_REPORTS_WITHOUT_RESULTS_OBJECT,
    COUNTER_REPORTS_WITHOUT_FAILED_CHECKS_ARRAY,
    COUNTER_PASSED_CHECKS_OBSERVED,
    COUNTER_SKIPPED_CHECKS_OBSERVED,
    COUNTER_PARSING_ERRORS,
    COUNTER_MULTI_LOCATION,
    COUNTER_MULTI_VALUED_CWE,
    COUNTER_MULTI_VALUED_CVE,
    COUNTER_NON_FILESYSTEM_PATHS,
    COUNTER_ROWS_IN_SCOPE,
    COUNTER_ROWS_OUT_OF_SCOPE,
    COUNTER_PATH_CORROBORATION_RECORDED,
    COUNTER_SEVERITY_PRESENT,
    COUNTER_SEVERITY_ABSENT,
    COUNTER_START_LINE_ABSENT,
)

#: Every key :func:`new_counters` initialises, in a stable order.
#:
#: Note what is deliberately **absent**: there is no adapter-side count of the failed
#: checks walked, and none of the rows or rejections produced.  ``len(rows)`` and
#: ``len(rejections)`` are returned to the caller directly, and a record count taken
#: from *this* traversal would be an attractive nuisance on the left-hand side of
#: ``raw finding records = dataset rows + rejected records`` -- the one place AAP
#: 0.5.4 requires a genuinely independent traversal, which is
#: ``reconcile.count_records``.  Publishing a plausible substitute for it here is how
#: that requirement would quietly be lost.  The same reasoning keeps the report's own
#: ``summary.failed`` out of this mapping: it is the tool's statement about itself,
#: available verbatim from :func:`report_summaries`, and not a record count.
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
    the producer wrote.  Nothing is trimmed, because a ``check_name`` may legitimately
    carry embedded newlines, so a single row can span several physical lines, which is
    why equality between the two output files is asserted by parsing rather than by
    counting lines.
    """
    if isinstance(value, str) and value.strip():
        return value
    return None


def _type_name(value: Any) -> str:
    """Name ``value``'s type in JSON's vocabulary where there is one.

    Used only in rejection details and caller-fault messages, which are read by a
    human looking at the artifact -- so ``array`` is more useful there than ``list``.
    """
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, Mapping):
        return "object"
    if isinstance(value, str):
        return "string"
    if isinstance(value, int):
        return "number"
    if isinstance(value, float):
        return "number"
    if _is_json_array(value):
        return "array"
    return type(value).__name__


# --------------------------------------------------------------------------- #
# The one normalizing step: both top-level shapes -> one sequence of reports
# --------------------------------------------------------------------------- #


def _report_sequence(doc: Any) -> tuple[Sequence[Any], bool]:
    """Reduce either top-level shape to one sequence of report objects.

    Returns the sequence together with a flag saying which shape it came from, so
    :func:`adapt` can record the shape actually observed.  The reduction is the whole
    of the dual-shape handling: everything after it walks one sequence, so the two
    shapes cannot diverge into two behaviours (AAP 0.5.4, and the dual-shape
    assertion in ``oss-scan-results/adapter-tests/test_checkov_adapter.py``).

    The list/mapping test is exactly ``reconcile._count_checkov``'s -- ``doc if
    isinstance(doc, list) else [doc]`` -- and exactly the pair ``shape.py`` accepts as
    a supported container, so the three modules agree on what a report is.  Elements
    are **not** filtered here: a non-object element is counted by the caller, because
    the independent traversal contributes zero for it and dropping it silently would
    make the two counts agree for the wrong reason.

    ``shape.py`` is now stricter than this reduction, and deliberately so.
    ``shape.NATIVE_SIGNATURES["checkov"]`` additionally requires the object form to
    carry a ``results`` object and every element of the array form to be an object
    carrying one, halting under ``shape.REASON_NATIVE_SIGNATURE_MISMATCH`` otherwise.
    A document reaching here from ``cli.py`` has therefore already satisfied that
    signature, so the raise below is unreachable in a run and is kept as the second
    line of defence for a direct caller -- an adapter test, or a later consumer that
    calls this module without routing first.  It is not relaxed on that account: an
    adapter that returned zero rows for a top level it could not read would report a
    clean scan over a document nobody parsed.

    Raises
    ------
    CheckovAdapterError
        If the top level is neither a JSON array nor a JSON object.  A parsed JSON
        document is always one of the two, and ``shape.py`` routes nothing else here,
        so this is a mis-route or a hand-built fixture -- a caller fault, raised
        rather than turned into zero rows, since an empty result set is
        indistinguishable from a clean scan.
    """
    if isinstance(doc, list):
        return doc, True
    report = _json_object(doc)
    if report is not None:
        return (report,), False
    raise CheckovAdapterError(
        f"a checkov artifact's top level is either an array of report objects "
        f"(the multi-framework form) or a single report object; observed "
        f"{_type_name(doc)}. Shape detection belongs to shape.py, which halts on an "
        "artifact matching neither SARIF nor a known native shape"
    )


def report_summaries(doc: Any) -> tuple[dict[str, Any], ...]:
    """Return each report's ``check_type`` and ``summary``, verbatim, in order.

    Status evidence for ``oss-scan-results/tool-status.md``, which quotes what the
    tool said about its own reach -- provisioning measured ``passed=201 failed=6
    skipped=0 parsing_errors=0 resource_count=3``.  The summary object is returned
    **as observed**, so a reader compares the tool's own numbers with this run's
    measured ones instead of taking either on trust.

    It is deliberately *not* a counter and deliberately *not* the record count: the
    left-hand side of the reconciliation identity is
    ``reconcile.count_records``' independent traversal, and a plausible substitute
    published here is how that requirement would quietly be lost.

    Each element carries ``report_index``, ``check_type`` (``None`` where the report
    names none) and ``summary`` (``None`` where the report carries none).  Raises
    :class:`CheckovAdapterError` on a top level that is neither shape, exactly as
    :func:`adapt` does, so the two cannot disagree about what a report is.
    """
    reports, _ = _report_sequence(doc)
    summaries: list[dict[str, Any]] = []
    for index, raw_report in enumerate(reports):
        report = _json_object(raw_report)
        if report is None:
            continue
        summary = report.get(SUMMARY_KEY)
        summaries.append(
            {
                "report_index": index,
                CHECK_TYPE_KEY: _non_empty_string(report.get(CHECK_TYPE_KEY)),
                SUMMARY_KEY: summary if isinstance(summary, Mapping) else None,
            }
        )
    return tuple(summaries)


def collect_parsing_errors(doc: Any) -> tuple[dict[str, Any], ...]:
    """Return Checkov's own ``parsing_errors``, verbatim, across both shapes.

    AAP 0.5.4 has ``parsing_errors`` *"reported in ``tool-status.md`` as status
    evidence"*, and that document's per-tool contract requires *"any parser error
    verbatim"* -- so the entries are returned exactly as the artifact carries them,
    with no rendering, truncation or interpretation.

    They are emphatically **not** rejections and **not** findings.  A rejection is a
    *record* this adapter could not attribute; a parsing error is Checkov's report
    about a file it could not read, and it is not one of the counted
    ``failed_checks``.  Converting one into a :class:`normalize.paths.Rejection` would
    put it on the right-hand side of ``raw finding records = dataset rows + rejected
    records`` while the independent traversal never counted it on the left, breaking
    the identity for a record that was never a finding.

    Each element carries ``report_index``, ``check_type`` and the ``entry`` verbatim.
    An entry is normally the path of the file that would not parse; anything else the
    artifact carries is passed through unchanged rather than coerced, because status
    evidence a reader cannot see in the producer's own words is not evidence.
    """
    reports, _ = _report_sequence(doc)
    errors: list[dict[str, Any]] = []
    for index, raw_report in enumerate(reports):
        report = _json_object(raw_report)
        if report is None:
            continue
        results = _json_object(report.get(RESULTS_KEY))
        if results is None:
            continue
        check_type = _non_empty_string(report.get(CHECK_TYPE_KEY))
        for entry in _json_array(results.get(PARSING_ERRORS_KEY)):
            errors.append(
                {
                    "report_index": index,
                    CHECK_TYPE_KEY: check_type,
                    "entry": entry,
                }
            )
    return tuple(errors)



# --------------------------------------------------------------------------- #
# Field extraction.  Each helper returns either the value to emit or the
# (reject_class, detail) pair the record earns, never both -- which is what keeps
# the caller's one-row-XOR-one-rejection invariant readable.
# --------------------------------------------------------------------------- #


def _rule_id(check: Mapping[str, Any]) -> tuple[str | None, tuple[str, str] | None]:
    """Return the failed check's ``check_id``, or the rejection it earns.

    AAP 0.5.4: ``rule_id`` <- ``check_id``, and an absent or empty one is the
    ``missing_rule_id`` rejection condition.  ``bc_check_id`` is **not** substituted
    -- AAP 0.5.4 names ``check_id``, and emitting a different field's identifier
    under the same column would make the dataset's ``rule_id`` two things at once --
    but its presence is named in the detail, because "the record has an identifier,
    just not that one" is what a reader of ``tool-status.md`` needs to know.
    """
    raw = check.get(CHECK_ID_FIELD)
    resolved = _non_empty_string(raw)
    if resolved is not None:
        return resolved, None
    if raw is None:
        reason = f"the failed check carries no {CHECK_ID_FIELD}"
    elif isinstance(raw, str):
        reason = f"the failed check's {CHECK_ID_FIELD} is empty or whitespace only"
    else:
        reason = (
            f"the failed check's {CHECK_ID_FIELD} is a {_type_name(raw)}, not a string"
        )
    alternate = _non_empty_string(check.get(BC_CHECK_ID_FIELD))
    if alternate is not None:
        reason = (
            f"{reason}; it carries {BC_CHECK_ID_FIELD} {alternate!r}, which is not "
            f"substituted because AAP 0.5.4 names {CHECK_ID_FIELD} as the source of "
            "rule_id"
        )
    return None, (paths.REJECT_MISSING_RULE_ID, reason)


def _message(check: Mapping[str, Any]) -> tuple[str | None, tuple[str, str] | None]:
    """Return the failed check's ``check_name``, or the rejection it earns.

    AAP 0.5.4: ``message`` <- ``check_name``, and an absent or empty one is the
    ``missing_message`` rejection condition.  A non-string ``check_name`` is reported
    with its type under the same class rather than rendered with ``str()``: rendering
    would put a Python repr in the dataset's ``message`` column and call it the
    producer's text.
    """
    raw = check.get(CHECK_NAME_FIELD)
    resolved = _non_empty_string(raw)
    if resolved is not None:
        return resolved, None
    if raw is None:
        reason = f"the failed check carries no {CHECK_NAME_FIELD}"
    elif isinstance(raw, str):
        reason = f"the failed check's {CHECK_NAME_FIELD} is empty or whitespace only"
    else:
        reason = (
            f"the failed check's {CHECK_NAME_FIELD} is a {_type_name(raw)}, not a "
            "string"
        )
    return None, (paths.REJECT_MISSING_MESSAGE, reason)


def _start_line(check: Mapping[str, Any]) -> tuple[int | None, tuple[str, str] | None]:
    """Return ``file_line_range[0]`` as the line to emit, or the rejection it earns.

    AAP 0.5.4: ``start_line`` <- ``file_line_range[0]``.  Absence is permitted for
    that field (AAP 0.8.2), so an absent range and an explicitly null first element
    both yield ``None`` with no rejection -- JSON ``null`` is this dataset's absence
    convention, and reading one as a defect would reject a record for using it.

    Two shapes are structurally wrong rather than absent, and both are
    ``malformed_record``.  A ``file_line_range`` that is present but is **not an
    array** is the first: silently treating it as "no line information" would drop the
    line of every record in a malformed artifact without a trace.  An **empty array**
    is the second, and it is the one shape where the natural reading
    ``check["file_line_range"][0]`` *raises* rather than returning something wrong.
    Checkov's contract for the field is a two-element ``[start, end]``, which an empty
    array does not satisfy, so neither available alternative is acceptable: letting
    the ``IndexError`` escape would discard every later check in the artifact, and
    returning ``None`` would make a record whose line information is present but
    unusable indistinguishable from one that legitimately states no line at all --
    which is precisely the distinction
    ``oss-scan-results/adapter-tests/fixtures/reject-checkov-non-integer-start-line.json``
    exists to assert, since it carries both shapes side by side.  It is therefore
    counted under a named class, exactly as ``sarif.py`` counts a ``region`` that is
    present but not an object: a structurally wrong container is not an absence.

    A first element that is present and not usable as a line number is the
    ``non_integer_start_line`` rejection condition.  Three shapes reach it, each named
    in the detail: a non-integer type; ``True``/``False``, which Python's numeric
    tower would otherwise admit as ``1`` and ``0``; and a value below ``1``, since
    Checkov numbers lines from one and ``0`` is not a line.  One class covers all
    three because :data:`normalize.paths.REJECT_CLASSES` is closed, with the
    sub-reason in the detail -- exactly as AAP 0.5.4 does for the ``uriBaseId``
    terminal cases.
    """
    raw_range = check.get(FILE_LINE_RANGE_FIELD)
    if raw_range is None:
        return None, None
    if not _is_json_array(raw_range):
        return None, (
            paths.REJECT_MALFORMED_RECORD,
            f"the failed check's {FILE_LINE_RANGE_FIELD} is a "
            f"{_type_name(raw_range)}, not an array",
        )
    if len(raw_range) == 0:
        return None, (
            paths.REJECT_MALFORMED_RECORD,
            f"the failed check's {FILE_LINE_RANGE_FIELD} is an empty array, so it "
            "carries no first element to read a line number from",
        )
    raw = raw_range[0]
    if raw is None:
        return None, None
    if isinstance(raw, bool) or not isinstance(raw, int):
        return None, (
            paths.REJECT_NON_INTEGER_START_LINE,
            f"{FILE_LINE_RANGE_FIELD}[0] is {raw!r}, a {_type_name(raw)} rather than "
            "an integer",
        )
    if raw < 1:
        return None, (
            paths.REJECT_NON_INTEGER_START_LINE,
            f"{FILE_LINE_RANGE_FIELD}[0] is {raw}, which is not a line number: "
            "checkov numbers lines from one",
        )
    return raw, None


def _identifier_sources(check: Mapping[str, Any]) -> tuple[str, ...]:
    """Return the strings CWE and CVE identifiers are collected from, in field order.

    The fields are :data:`IDENTIFIER_SOURCE_FIELDS` and nothing else -- a closed,
    named set fixed before any artifact was observed (AAP 0.1.3).  Most Checkov
    records carry no weakness identifier at all, which is why ``cwe`` and ``cve`` are
    ordinarily ``None`` for this tool; where a record genuinely does carry one it is
    in its guideline or metadata, and this is where those live.

    Only string leaves are collected, and only down to
    :data:`_IDENTIFIER_SOURCE_MAX_DEPTH`.  A bare number is never read as an
    identifier: turning ``79`` into ``CWE-79`` would supply a prefix the artifact
    never wrote, and AAP 0.5.4's rule is to reject rather than infer.  Mapping keys
    are not collected either -- ``benchmarks`` is keyed by benchmark identifier, and
    a key is the vocabulary's own label rather than a reference the record made.
    """
    sources: list[str] = []

    def harvest(value: Any, depth: int) -> None:
        """Append every string leaf of ``value`` reachable within the depth bound."""
        if isinstance(value, str):
            if value.strip():
                sources.append(value)
            return
        if depth >= _IDENTIFIER_SOURCE_MAX_DEPTH:
            return
        if isinstance(value, Mapping):
            for nested in value.values():
                harvest(nested, depth + 1)
            return
        if _is_json_array(value):
            for nested in value:
                harvest(nested, depth + 1)

    for field_name in IDENTIFIER_SOURCE_FIELDS:
        harvest(check.get(field_name), 0)
    return tuple(sources)


def _select_cwe(sources: Iterable[str]) -> tuple[str | None, int]:
    """Return the CWE to emit and how many distinct ones were found.

    The ascending-identifier rule (AAP 0.5.4): the field carries **one** value,
    chosen by ascending numeric identifier -- the integer after the ``CWE-`` prefix.
    That ordering is total over the integers, so no tie can arise and no
    producer-order tiebreak is needed.  The emitted value keeps the digits exactly as
    they appeared, including any leading zero, under the canonical upper-case prefix.
    Identical to ``adapters/sarif.py``'s selection, because one identifier has to
    render one way across every tool's rows.
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


def _has_second_location(check: Mapping[str, Any]) -> bool:
    """Return whether the failed check names a second location.

    Checkov emits exactly one second-location shape: ``caller_file_path`` (with its
    own ``caller_file_line_range``), present where a finding reached its resource
    through a module call.  AAP 0.5.4's representation decision applies to it -- the
    row takes the **first** location, the record still counts **once**, and the
    number of records carrying more than one is reported per tool -- so this decides
    :data:`COUNTER_MULTI_LOCATION` and nothing else.  The caller location never
    reaches the row's ``path``.
    """
    return _non_empty_string(check.get(CALLER_FILE_PATH_FIELD)) is not None



# --------------------------------------------------------------------------- #
# Argument validation.
#
# Every one of these raises :class:`CheckovAdapterError` rather than returning a
# rejection: a bad argument is a caller fault, and absorbing it into a rejection
# count would let a wrong root or a foreign path base produce a plausible dataset
# for a whole tool.  Each is validated once per call, before any record is read,
# so a fault surfaces on the call rather than on the first record.
# --------------------------------------------------------------------------- #


def _validated_tool(tool: Any) -> str:
    """Return ``tool`` where it is this adapter's tool identifier, else raise."""
    if not isinstance(tool, str):
        raise CheckovAdapterError(
            f"tool must be a canonical tool identifier string; observed "
            f"{_type_name(tool)}"
        )
    if tool != TOOL:
        raise CheckovAdapterError(
            f"{tool!r} is not the tool this adapter serves ({TOOL!r}). The identifier "
            "is required rather than assumed so that every adapter in this package "
            "exposes one signature, and stamping another tool's name into these rows "
            "would misattribute every finding in the artifact"
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
    reasoning says to fail on the call.  Validating it up front is also what makes the
    narrow ``PathPolicyError`` catch in :func:`_adapt_failed_check` honest: with the
    root already known good, what remains from that call is record content.
    """
    try:
        candidate = fspath(root)
    except TypeError as error:
        raise CheckovAdapterError(
            f"root must be a str or an os.PathLike naming the SPARK_SRC root; "
            f"observed {_type_name(root)}"
        ) from error
    if isinstance(candidate, bytes):
        raise CheckovAdapterError(
            "root must be a text path, not bytes: every path in the dataset is text, "
            "and decoding one here would guess an encoding"
        )
    if not candidate:
        raise CheckovAdapterError("root must not be empty")
    normalised = paths.normalise_reported_path(candidate)
    if not paths.is_absolute_path(normalised):
        raise CheckovAdapterError(
            f"root must be an absolute path to express a reported path against; "
            f"observed {candidate!r}"
        )
    return normalised


def _validated_tool_base(tool_base: Any, tool: str) -> paths.ToolPathBase:
    """Return ``tool_base`` where it is this tool's recorded path base, else raise.

    The identifier check is not ceremony.  ``tool_base`` is the per-tool view over
    ``harness/artifacts/logs/runner-metadata.json``, and it is what tells the resolver
    that this provisioning passes eighteen ``-d`` roots in one invocation
    (``path_base.kind`` ``per_target_directory``, ``anchor_fields``
    ``[repo_file_path, file_abs_path]``).  Handing this adapter another tool's view
    would resolve every path against the wrong base while every row still looked
    well-formed -- the exact failure AAP 0.5.4 requires *"every base taken from the
    recorded runner metadata"* to prevent.
    """
    if not isinstance(tool_base, paths.ToolPathBase):
        raise CheckovAdapterError(
            f"tool_base must be a paths.ToolPathBase built from the runner metadata; "
            f"observed {_type_name(tool_base)}"
        )
    if tool_base.tool != tool:
        raise CheckovAdapterError(
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
        raise CheckovAdapterError(
            "allowlist must be an iterable of glob strings, not a single string: a "
            "string would be iterated character by character"
        )
    if not isinstance(allowlist, Iterable):
        raise CheckovAdapterError(
            f"allowlist must be an iterable of glob strings from "
            f"paths.load_allowlist(); observed {_type_name(allowlist)}"
        )
    globs = tuple(allowlist)
    for index, glob in enumerate(globs):
        if not isinstance(glob, str) or not glob:
            raise CheckovAdapterError(
                f"allowlist entry {index} must be a non-empty glob string; observed "
                f"{glob!r}"
            )
    return globs


def _validated_tally(tally: Any) -> Any:
    """Return ``tally`` where it can record a severity result, else raise.

    The capability is checked rather than the class, so a test double is as
    acceptable as a :class:`normalize.severity.LiteralTally`.  ``None`` is not: every
    row's literal has to reach ``severity-map.md``, and for this tool that literal is
    an *absence* on every row in the unlicensed configuration -- precisely the kind of
    entry a silently skipped tally would leave the document under-reporting with
    nothing to show it had.
    """
    recorder = getattr(tally, "record", None)
    if not callable(recorder):
        raise CheckovAdapterError(
            f"tally must expose a callable record(tool, result) -- normally a "
            f"severity.LiteralTally; observed {_type_name(tally)}"
        )
    return tally



# --------------------------------------------------------------------------- #
# One failed check -> exactly one outcome
# --------------------------------------------------------------------------- #


def _adapt_failed_check(
    check: Any,
    *,
    tool: str,
    root: str,
    tool_base: paths.ToolPathBase,
    globs: tuple[str, ...],
    tally: Any,
    check_type: str | None,
    report_index: int,
    check_index: int,
    counters: dict[str, int],
) -> dict[str, Any] | paths.Rejection:
    """Return one row **or** one rejection for one ``results.failed_checks[]`` element.

    Exactly one of the two, always.  The single return value is what makes the
    one-to-one property structural: there is no path through this function that emits
    both and none that emits neither, so ``dataset rows + rejected records == the
    failed checks walked`` holds by construction rather than by an assertion that
    could be forgotten.

    The classification order is the one this module's docstring fixes: shape, rule
    identifier, message, path, then ``start_line``.  Severity, ``cwe``/``cve``,
    ``package_coordinate`` and ``in_scope`` cannot reject -- each is defined for every
    input -- so a record that reaches them becomes a row.

    Nothing is caught broadly here.  Each lookup and conversion is guarded where it
    happens, and the one ``except`` clause is a named exception type from one
    delegated call, so a genuine programming error propagates instead of being
    converted into a rejection count that would satisfy reconciliation while hiding a
    defect.
    """
    # paths.resolve_checkov_path would classify a non-object the same way; it is
    # checked here first so that every check after it can read the record as a
    # mapping, and so the class does not depend on how far the record got.
    check_object = _json_object(check)
    if check_object is None:
        return paths.make_rejection(
            paths.REJECT_MALFORMED_RECORD,
            tool,
            f"the {EMITTED_RESULT_SECTION} element is a {_type_name(check)}, not an "
            "object, so no finding can be read from it",
            report_index=report_index,
            check_index=check_index,
            check_type=check_type,
        )

    rule_id, rule_id_failure = _rule_id(check_object)
    if rule_id_failure is not None:
        reject_class, detail = rule_id_failure
        return paths.make_rejection(
            reject_class,
            tool,
            detail,
            report_index=report_index,
            check_index=check_index,
            check_type=check_type,
        )

    message, message_failure = _message(check_object)
    if message_failure is not None:
        reject_class, detail = message_failure
        return paths.make_rejection(
            reject_class,
            tool,
            detail,
            report_index=report_index,
            check_index=check_index,
            check_type=check_type,
            check_id=rule_id,
        )

    # The multi-location count is a property of the record, so it is taken whatever
    # the record's outcome turns out to be (AAP 0.5.4: the row takes the first
    # location, the record still counts once, and the number is reported per tool).
    if _has_second_location(check_object):
        counters[COUNTER_MULTI_LOCATION] += 1

    # Every base decision is delegated to paths.py: the leading slash off `file_path`,
    # the anchor order recorded in the runner metadata, the anchor-against-anchor
    # reconciliation and the `file_path` corroboration all live there.  See this
    # module's docstring for why the reconciliation is the reliable route rather than
    # a cross-check: with eighteen `-d` roots in one invocation, a slash-stripped
    # `file_path` is relative to whichever root matched, so a strip-and-join against
    # the tree root is wrong even once the slash is handled -- and read as
    # filesystem-absolute the same value relativizes to a long `../` chain and the row
    # silently takes `in_scope: false`.
    try:
        resolved = paths.resolve_checkov_path(check_object, root, tool_base, tool=tool)
    except paths.RunnerMetadataError:
        # A metadata fault is a caller fault, not a defective record: re-raised so it
        # cannot be absorbed into a rejection count for every row of the artifact.
        # Listed before PathPolicyError because it is a subclass of it.
        raise
    except paths.PathPolicyError as error:
        # Narrow by construction: the root was validated absolute before any record
        # was read, so what remains from this call is record content that cannot be
        # expressed as a legal emitted path -- an anchor value carrying a `//`
        # authority prefix, say, which survives the single-slash strip.  That is a
        # defective record, counted with the reason verbatim (AAP 0.5.4: partial parse
        # is a first-class outcome), never a crash that discards the whole artifact.
        return paths.make_rejection(
            paths.REJECT_UNRESOLVABLE_PATH,
            tool,
            f"the failed check's path fields cannot be expressed against the scan "
            f"root: {error}",
            report_index=report_index,
            check_index=check_index,
            check_type=check_type,
            check_id=rule_id,
        )
    if isinstance(resolved, paths.Rejection):
        # Returned as-is: paths.py has already named the class -- absent_path where
        # the record names no location at all, unresolvable_path where only a
        # target-relative `file_path` is present and there is no way to know which of
        # eighteen targets it came from -- and written the sub-reason, with the
        # record's own check_id and path fields as its identity.  Rewording it here
        # would lose that, and picking one of the two candidate paths instead is
        # exactly the guess AAP 0.1.3 forbids.
        return resolved

    start_line, start_line_failure = _start_line(check_object)
    if start_line_failure is not None:
        reject_class, detail = start_line_failure
        return paths.make_rejection(
            reject_class,
            tool,
            detail,
            report_index=report_index,
            check_index=check_index,
            check_type=check_type,
            check_id=rule_id,
        )
    if start_line is None:
        counters[COUNTER_START_LINE_ABSENT] += 1

    # From here nothing can reject: this record is a row.
    #
    # Severity is handed to severity.py exactly as the record carries it and mapped
    # nowhere else.  `severity` requires a licence, so in this configuration it is
    # null on every row and severity.py takes its no-vocabulary path: severity_native
    # None, severity_norm Info, the absence *stated* through the recorded basis rather
    # than a level assumed locally.  No score is consulted, because AAP 0.5.4's
    # checkov row names `severity` as the field and nothing else -- a misconfiguration
    # carries no CVSS score, and reaching for one would extend a policy the AAP fixed
    # for this tool.  A literal that ever does appear is mapped by severity.py's
    # case-insensitive label table, or disclosed as unmapped with the rows it
    # affected; either way the policy was fixed before this artifact was observed.
    raw_severity = check_object.get(SEVERITY_FIELD)
    severity_result = severity.resolve(label=raw_severity)
    if raw_severity is None:
        counters[COUNTER_SEVERITY_ABSENT] += 1
    else:
        counters[COUNTER_SEVERITY_PRESENT] += 1
    counters[f"{COUNTER_SEVERITY_BASIS_PREFIX}{severity_result.basis}"] += 1
    # The tally is fed once per emitted row, which is what makes severity-map.md's
    # per-literal counts the row counts it reports them as.  A rejected record
    # contributes no row, so counting one here would put a literal in that document
    # against rows the dataset does not contain.
    tally.record(tool, severity_result)

    identifier_sources = _identifier_sources(check_object)
    cwe, cwe_count = _select_cwe(identifier_sources)
    cve, cve_count = _select_cve(identifier_sources)
    if cwe_count > 1:
        counters[COUNTER_MULTI_VALUED_CWE] += 1
    if cve_count > 1:
        counters[COUNTER_MULTI_VALUED_CVE] += 1

    counters[f"{COUNTER_PATH_KIND_PREFIX}{resolved.kind}"] += 1
    if resolved.is_non_filesystem_coordinate:
        counters[COUNTER_NON_FILESYSTEM_PATHS] += 1
    if resolved.corroboration is not None:
        # A disagreement between the anchor fields, or between an anchor and
        # `file_path`, is recorded rather than resolved (AAP 0.5.3) and never
        # suppresses the row.  Counting it is what makes the user's worked example
        # observable instead of a silent preference for one field.
        counters[COUNTER_PATH_CORROBORATION_RECORDED] += 1

    # in_scope is decided by the allowlist alone, through paths.py's matcher, on the
    # resolved path and carrying its kind -- so the literal src/test exclusion and the
    # true zero-or-more-directories ** semantics are applied once, where they live.
    # Nothing is ever filtered on it: a row outside the allowlist is kept with
    # in_scope false and counted (AAP 0.9.3).  Checkov legitimately reaches such
    # coordinates, since its own reach is shaped by its eighteen -d roots while
    # in_scope is the allowlist's answer and not the runner's.
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
    """Turn one Checkov artifact into dataset rows, rejections and counters.

    This is the uniform adapter entry point: every adapter module in this package
    exposes ``adapt`` with this shape, so ``cli.py``'s registry resolves it with
    ``getattr(module, "adapt")`` and every adapter test calls it directly.

    Both top-level shapes are handled by one code path: :func:`_report_sequence`
    reduces the object form and the multi-framework array form to a single sequence of
    report objects, and one loop walks it.  Only ``results.failed_checks[]`` is read;
    ``passed_checks`` and ``skipped_checks`` are counted as status evidence and never
    emitted, and ``parsing_errors`` are counted here and available verbatim from
    :func:`collect_parsing_errors` -- never as rejections, which would corrupt the
    reconciliation identity.

    Args:
        doc: The **already-parsed** artifact document -- for this tool either a
            mapping (the object form) or a list of report objects (the multi-framework
            form). Parsing and shape detection happen upstream, which is what lets a
            test exercise every behaviour on a fixture with no filesystem.
        tool: The canonical tool identifier, which must be :data:`TOOL`. Required and
            validated rather than assumed, so that every adapter shares one signature
            and no artifact can be misattributed.
        root: The ``SPARK_SRC`` root, as a :class:`pathlib.Path` or a string. Must be
            absolute.
        tool_base: This tool's :class:`normalize.paths.ToolPathBase`, the per-tool
            view over ``harness/artifacts/logs/runner-metadata.json``. Every base
            decision is taken from it and none is assumed -- for this provisioning it
            records ``per_target_directory`` with the anchor fields that disambiguate
            which of the runner's eighteen ``-d`` roots a record came from.
        allowlist: The twelve authoritative globs, as loaded by
            :func:`normalize.paths.load_allowlist`. Consumed once into a tuple.
        tally: A :class:`normalize.severity.LiteralTally` (or anything exposing
            ``record(tool, result)``), fed once per emitted row so
            ``oss-scan-results/severity-map.md`` can list every observed literal --
            including, for this tool, the absence on every row -- with the rows it
            affected.

    Returns:
        A three-tuple ``(rows, rejections, counters)``:

        * ``rows`` -- a list of dicts, each carrying exactly the twelve fields of
          :data:`FIELDS` in that order, in document order (report objects in order,
          and within each, ``failed_checks`` in order);
        * ``rejections`` -- a list of :class:`normalize.paths.Rejection`, each under a
          named member of :data:`normalize.paths.REJECT_CLASSES` with its sub-reason
          retained verbatim;
        * ``counters`` -- a dict of ints over :data:`COUNTER_KEYS`.

        ``len(rows) + len(rejections)`` equals the number of
        ``results.failed_checks[]`` elements walked across every report object, which
        is the same count unit :func:`normalize.reconcile.count_records` arrives at
        independently.

    Raises:
        CheckovAdapterError: If an argument is not what the contract requires -- the
            wrong tool identifier, a relative or non-text root, another tool's path
            base, a non-iterable allowlist, a tally that cannot record, or a document
            whose top level is neither of Checkov's two shapes. A caller fault is
            raised rather than absorbed into a rejection count, because zero rows is
            indistinguishable from a clean scan.
        normalize.paths.RunnerMetadataError: If the recorded metadata cannot supply
            what the path resolver needs. Re-raised rather than rejected: it is a
            fault in the metadata, not in a record, and it would otherwise be counted
            once per row of the artifact.
        normalize.severity.SeverityPolicyError: If ``tally`` is a ``LiteralTally``
            and ``tool`` is outside its canonical vocabulary -- which cannot happen
            for the one tool this module serves, and is left to surface rather than be
            caught.

    A tool's exit code is never consulted: a valid artifact is normalized whatever its
    runner returned, since artifact status and exit status are independent (AAP
    0.5.4). Checkov is one of the two tools that exit non-zero *precisely because*
    they found something -- provisioning measured exit 1 with six findings -- so
    treating a non-zero exit as doubt about the artifact would discard real output.
    """
    canonical_tool = _validated_tool(tool)
    root_text = _validated_root(root)
    base = _validated_tool_base(tool_base, canonical_tool)
    globs = _validated_allowlist(allowlist)
    recorder = _validated_tally(tally)
    reports, is_array_form = _report_sequence(doc)

    rows: list[dict[str, Any]] = []
    rejections: list[paths.Rejection] = []
    counters = new_counters()
    counters[
        COUNTER_TOP_LEVEL_FORM_ARRAY if is_array_form else COUNTER_TOP_LEVEL_FORM_OBJECT
    ] = 1
    # Counted through the same public traversal ``cli.py`` reads the entries with, so
    # the count and the verbatim list can never disagree about how many there were.
    counters[COUNTER_PARSING_ERRORS] = len(collect_parsing_errors(doc))

    for report_index, raw_report in enumerate(reports):
        counters[COUNTER_REPORTS] += 1
        report = _json_object(raw_report)
        if report is None:
            # Contributes no record, exactly as reconcile.py's traversal counts it --
            # a malformed element surfaced as a count rather than as a rejection,
            # because a rejection would put it on the right-hand side of the identity
            # while the independent count never put it on the left.
            counters[COUNTER_REPORTS_SKIPPED_NON_MAPPING] += 1
            continue
        results = _json_object(report.get(RESULTS_KEY))
        if results is None:
            # A report with no results object contributes nothing and is not an error.
            counters[COUNTER_REPORTS_WITHOUT_RESULTS_OBJECT] += 1
            continue

        # The never-emitted buckets, counted before the findings so that "produced no
        # row" is an observation rather than a claim.  Provisioning measured
        # passed=201 skipped=0 against six failures.
        counters[COUNTER_PASSED_CHECKS_OBSERVED] += len(
            _json_array(results.get(NEVER_EMITTED_RESULT_SECTIONS[0]))
        )
        counters[COUNTER_SKIPPED_CHECKS_OBSERVED] += len(
            _json_array(results.get(NEVER_EMITTED_RESULT_SECTIONS[1]))
        )

        raw_failed = results.get(EMITTED_RESULT_SECTION)
        if not _is_json_array(raw_failed):
            # No failed_checks array, or one that is not an array: nothing to walk,
            # and not an error.  A clean framework report is exactly this shape.
            counters[COUNTER_REPORTS_WITHOUT_FAILED_CHECKS_ARRAY] += 1
            continue

        check_type = _non_empty_string(report.get(CHECK_TYPE_KEY))
        for check_index, raw_check in enumerate(raw_failed):
            outcome = _adapt_failed_check(
                raw_check,
                tool=canonical_tool,
                root=root_text,
                tool_base=base,
                globs=globs,
                tally=recorder,
                check_type=check_type,
                report_index=report_index,
                check_index=check_index,
                counters=counters,
            )
            if isinstance(outcome, paths.Rejection):
                rejections.append(outcome)
            else:
                rows.append(outcome)

    return rows, rejections, counters
