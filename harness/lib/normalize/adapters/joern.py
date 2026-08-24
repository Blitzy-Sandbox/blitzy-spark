"""harness/lib/normalize/adapters/joern.py — the adapter for ``joern.json``.

AAP 0.6.1 specifies *"One adapter per non-SARIF artifact written"*, and this is
``joern``'s.  It is the only adapter AAP 0.6.4 singles out: *"The Joern adapter is the
only one that depends on an existing harness file, consuming the bytecode-path mapping
the harness's Joern collector performs -- and extending it, since a ``-tests``
artifact's classes resolve into ``src/test`` rather than ``src/main``."*

No user-specified rule governs this file.  ``review_rules`` returns exactly one line,
``No user rules provided.``, corroborated by AAP 0.7 and AAP 0.10.2.  Enterprise best
practice applies in their place, held to the AAP's own bar: verification independent of
the thing verified, **reject rather than infer**, and a policy fixed before any output
is observed.  Everything cited below is an AAP *requirement*; none of it is a rule, and
none is invented here.

The artifact is this harness's own shape, not a tool-native format
-----------------------------------------------------------------
There is no Joern output format to look up.  ``harness/bin/run-joern.sh`` loads the
code-property graph with ``importCpg`` and runs the bounded query set baked into
``harness/lib/joern-scan.sc``, and **that script writes the artifact itself**.  Two
shapes of it exist and this adapter reads both, because assuming one is exactly the
mistake AAP 0.1.1 forbids -- *"detecting each artifact's shape rather than assuming
it"*.

*The shape this provisioning writes*, read from ``harness/lib/joern-scan.sc`` lines
106-121::

    {tool, tool_version, cpg,
     graph: {methods, type_declarations, files},
     query_set,
     queries:  [{id, bound, returned, bound_reached, elapsed_ms, callee_prefixes[]}],
     findings: [{query_id, severity, message, callee, class, method, file, line}]}

*The shape the specification describes*, from a provisioned ``joern_collect.py``::

    {tool, cpg_path, generated_at, cpg_methods, cpg_typedecls,
     source_index_size, declaration_index_size,
     queries:  [{id, count}],
     findings: [{rule_id, message, path, start_line,
                 method_full_name, class_file, path_resolution}]}

**The divergence is recorded, not repaired** (AAP 0.1.3's authority rule).  No
``harness/lib/joern_collect.py`` exists in this provisioning -- the collector is the
Scala script above -- so this module depends on the *contract* rather than importing a
collector, and it redeclares locally whatever it needs.  AAP 0.5.2 is explicit that *no
runner or harness helper is edited*: the two gaps below are closed **here**, never by
touching the collector.  ``paths.py`` records the same divergence from the other side,
which is why :func:`normalize.paths.class_key` already accepts either a dotted type
full name or a class-file path.

Three consequences of the shape actually written, each load-bearing:

* the coordinate is ``class`` -- a **dotted** type full name such as
  ``org.apache.spark.deploy.master.Master$$anon$1`` -- and
  ``harness/artifacts/logs/runner-metadata.json`` names it ``record_path_field``;
* ``file`` is the frontend's ephemeral ``/tmp/jimple2cpg-<id>/<pkg>/<Class>.class``
  extraction path -- measured at 692 of 692 findings -- so the metadata names it
  ``record_path_field_to_ignore`` and this adapter **never reads it as a path**.  It is
  retained in a rejection's identity as context, which is diagnostics rather than a
  dataset field;
* every query in the baked set declares a ``severity``, so ``severity_native`` is
  **present** for this provisioning's records.  AAP 0.5.4 places ``joern`` under *"No
  vocabulary at all (gitleaks; joern **unless a query defines one**)"*, and here one
  does: the label is mapped through ``severity.py``'s label table with the basis
  recorded, and where a record carries none the no-vocabulary path states the absence
  rather than assuming a level.

``findings[].path`` is already root-relative; ``class`` is the raw coordinate
-------------------------------------------------------------------------------
The correction a naive reading misses.  Where the documented shape carries ``path``, it
is **already** ``$SPARK_SRC``-relative -- the collector's own resolved answer -- and is
emitted **as-is**: not relativized again, not joined onto the root, never made
absolute.  ``class``/``class_file`` is the separate raw bytecode coordinate, and it is
the only thing that needs resolving.

The precedence between them is fixed, and it is not "prefer the collector":

1. **this run's own unique class-to-source resolution governs.**  The collector reached
   its answer with ``setdefault`` -- first wins in walk order -- and AAP 0.5.4 requires
   the resolution be taken *"only where it is unique"*;
2. an **ambiguous** class key is a rejection and is **never** overridden by the
   collector's path.  That is gap 2 below, and overriding it would reinstate exactly
   the silent guess the AAP forbids;
3. where this run can resolve **nothing** and the collector did supply a root-relative
   ``path``, that path is emitted as-is with the basis saying so and a corroboration
   note recording that this run's own index could not corroborate it.  A stated
   coordinate is a fact about the artifact, not an inference;
4. where the two both resolve and **disagree**, the row keeps this run's resolution and
   the disagreement is recorded in ``corroboration`` and counted.  AAP 0.5.3's posture
   on Checkov applies unchanged: record a mismatch rather than silently preferring one,
   and never suppress the row.

``path_resolution`` -- the collector's own explanation, one of
``source-index-filename``, ``source-index-declaration`` or
``unresolved-bytecode-only`` -- is retained **only** in a rejection.  AAP 0.5.4: *"any
collector explanation for an unmappable bytecode path is retained in the rejection
record, not in a dataset field."*  It reaches no row, and the twelve-field row set makes
that structural.

``path`` is never absent from a row (AAP 0.8.2), so an unresolvable coordinate with no
collector path is a **counted rejection** rather than a row with a null path.

The two collector gaps, closed here
-----------------------------------
AAP 0.5.4: *"The adapter resolves a finding's class file against ``src/main`` **and**
``src/test`` under the pinned root, takes the resolution only where it is unique, and
rejects the ambiguous and the unresolvable."*

*Gap 1 -- the collector walks only* ``src/main``.  Every ``-tests`` artifact the build
emitted is in the graph input (AAP 0.5.1 retains *"main artifacts, pre-shade and shaded
siblings, classifier artifacts and ``-tests`` artifacts"*), so a finding can legitimately
name bytecode compiled from a test tree.  ``paths.SOURCE_TREES`` spans both and
:func:`normalize.paths.build_source_index` walks both; this adapter asks for that index
and never accepts a ``src/main``-only answer.

*Gap 2 -- the collector resolves ambiguity with* ``setdefault``, first-wins in
``os.walk`` order and silently.  :func:`normalize.paths.resolve_bytecode_class` takes
the **union** of both key schemes across both trees and succeeds only where that union
is exactly one distinct path; two or more is
``ambiguous_source_resolution`` with every competing candidate named in the detail.

Closing gap 1 *increases* ambiguity, which is why closing gap 2 is mandatory rather
than optional.  Measured by ``build_source_index`` over the pinned tree at
``/opt/spark-src``: 6,759 files indexed, 6,755 ``by_filename`` keys of which **4** are
ambiguous, and 15,230 ``by_decl`` keys of which **107** are ambiguous.  Four verified
collisions, each reproducible:

============================================================  =========================
class key                                                     competing sources
============================================================  =========================
``org/apache/spark/SparkContext``                             ``core``'s
                                                              ``SparkContext.scala``
                                                              and
                                                              ``sql/connect/shims``'s
                                                              ``shims.scala``
``org/apache/spark/SparkConf``                                the same pair, via
                                                              ``SparkConf.scala``
``org/apache/spark/sql/catalyst/expressions/rows``            ``sql/api`` and
                                                              ``sql/catalyst``, both
                                                              ``src/main``
``org/apache/spark/SparkContextSuite``                        ``core`` and
                                                              ``hadoop-cloud``, both
                                                              ``src/test``
============================================================  =========================

``sql/connect/shims/src/main/scala/org/apache/spark/shims.scala`` really does declare
stub ``class SparkContext`` (line 19), ``class SparkConf`` (line 20) and
``package rdd { class RDD[T] }`` (lines 28-29) for client-only builds, which is what
makes the first two collisions structural rather than incidental.

A ``src/test`` resolution is **retained, never dropped**
-------------------------------------------------------
AAP 0.5.4: *"A finding resolving into ``src/test`` is retained with ``in_scope: false``,
and a fixture asserts exactly that -- a test-JAR finding kept out of scope rather than
dropped."*  AAP 0.9.3 repeats it among the recorded-not-halting conditions.  The literal
``src/test`` exclusion lives in ``paths.in_scope`` and is applied there, once; this
module neither re-implements it nor pre-filters on it.

Delegating matters for a second reason: ``python/pyspark/**`` holds 832 test modules and
**zero** ``src/test`` path segments, so all of them are in scope with ``in_scope: true``.
A hand-rolled "is this a test?" heuristic would wrongly exclude a fifth of the in-scope
file count.

The count unit, and the invariant that rests on it
--------------------------------------------------
``findings[]``: **one finding is one record** (AAP 0.5.4), which is exactly the unit
``reconcile._count_joern`` walks independently.  Every finding therefore yields
**exactly one outcome -- one row or one rejection, never both and never neither**.
:func:`_adapt_finding` returns a single value of one of those two types, so the
invariant is structural rather than asserted, and
``len(rows) + len(rejections)`` equals the number of ``findings[]`` elements walked.

Two things deliberately contribute nothing to that count, because letting either in
would break the identity ``raw finding records = dataset rows + rejected records``
silently while every individual assertion still passed:

* the envelope fields -- ``cpg``/``cpg_path``, ``graph``, ``cpg_methods``,
  ``cpg_typedecls``, ``source_index_size``, ``declaration_index_size``, ``query_set``
  and ``queries`` -- are **not findings and produce no rows**.  Where one is an integer
  it is surfaced through :data:`COUNTER_KEYS` under the ``envelope_`` prefix, which is
  metadata for ``oss-scan-results/tool-status.md`` and never a record count;
* ``queries[].returned`` and ``queries[].count`` are the collector's **own** per-query
  tallies.  AAP 0.5.4 makes ``findings[]`` the reconciliation unit, so their sum is
  neither published nor used -- a plausible substitute for the independent count is how
  the requirement for one would quietly be lost.

A ``findings`` value that is absent, empty or not an array contributes nothing and is
**not** an error, which is how ``reconcile._count_joern`` reads it too (``_length`` over
a non-array yields zero, and a ``str`` is never an array).  Document order is
preserved, since both output files use it and ``emit.py`` compares them row by row.

Position in the normalizer
--------------------------
A leaf that depends on exactly two modules.  AAP 0.6.4: *"each adapter depends on
``paths`` and ``severity`` and on nothing else."*  Taken literally --
:mod:`normalize.shape`, :mod:`normalize.cli`, :mod:`normalize.emit`,
:mod:`normalize.reconcile` and every sibling adapter are **not** imported, and neither
is ``joern_collect`` in any form, nor any third-party package (AAP 0.4.1: standard
library only, so this run introduces no manifest, no lockfile and no install step,
which AAP 0.4.3 forbids).

``emit.FIELDS`` and ``shape.SCANNER_CLASS_BY_TOOL`` therefore cannot be imported, so
:data:`FIELDS` and :data:`SCANNER_CLASS` below are authored copies that agree with them
**by construction**.  ``SCANNER_CLASS`` is ``sast``, fixed for ``joern`` by AAP 0.5.4's
class table and fixed in advance rather than derived from what the artifact turns out
to contain.

There is no ``__init__.py`` under ``harness/lib/normalize/`` or in this directory, by
design: the package is a PEP 420 implicit namespace package on the pinned CPython
3.13.7, resolved once ``harness/lib`` is on ``sys.path``.  Imports are absolute and
rooted at the package (``from normalize import paths``), never a bare sibling import.

Nothing here reads an environment variable or a global, and nothing happens at import
time beyond defining constants.  The document, the root, the runner metadata, the
allowlist and the tally all arrive as arguments, which is what makes :func:`adapt`
callable on an already-parsed fixture.  This module writes no file.  The one piece of
filesystem contact the resolution needs -- the source index over the pinned tree -- is
performed *inside* ``paths.py`` from the ``root`` argument, and a caller may inject a
prebuilt or synthetic index instead through the optional ``source_index`` keyword, so
every behaviour here is assertable with no live tree.

What this module does not do
----------------------------
AAP 0.3.2, in full force.  No cross-tool interpretation of any kind: one row per
finding with the producing tool named, and two tools reporting the same location produce
two rows and no comment.  It judges nothing -- not real, not important, not a false
positive, not a duplicate.  It deduplicates nothing, not across tools and not within
one: two identical findings are two records and two rows.  It filters nothing; every
record is emitted or rejected, and a row outside the allowlist is kept with
``in_scope: false`` and counted (AAP 0.9.3).

It also has **no path-discovery logic of its own**, deliberately, and this is the tool
where that matters most.  ``queries/joern/results/*`` -- the Joern capability probe -- is
Joern output about the same graph, and it is one of the run's two deliberate second
appearances: *"Joern is both a runner and the subject of the capability probe.  In each
case the second appearance writes outside ``harness/artifacts/raw/`` and contributes no
dataset row"*, and folding it in *"would corrupt both that tool's count and the
dataset's total"* (AAP 0.3.2).  The guard is that ``cli.py`` only ever passes artifacts
from ``harness/artifacts/raw/``; this module reads only the document handed to it and
must never acquire a way to reach further.

A tool's exit code is never consulted: a valid artifact is normalized whatever its
runner returned, since artifact status and exit status are independent (AAP 0.5.4).  For
context, the Joern runner *"guards its input and exits 78 with a message naming the
missing graph"* (AAP 0.1.1) -- a configuration fault to fix at the gate, never a
scanning outcome for this adapter to classify.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from os import fspath
from typing import Any, Final

from normalize import paths
from normalize import severity

__all__ = [
    "ABSENCE_PERMITTED_FIELDS",
    "CLASS_COORDINATE_FIELDS",
    "COLLECTOR_EXPLANATIONS",
    "COLLECTOR_PATH_FIELD",
    "COLLECTOR_UNKNOWN_CLASS",
    "COUNTER_KEYS",
    "FIELDS",
    "FINDINGS_KEY",
    "JoernAdapterError",
    "KNOWN_RULE_IDS",
    "MESSAGE_FIELDS",
    "RULE_ID_FIELDS",
    "SCANNER_CLASS",
    "START_LINE_FIELDS",
    "TOOL",
    "adapt",
    "new_counters",
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #


class JoernAdapterError(ValueError):
    """Raised where a *caller* hands this adapter something its contract forbids.

    Deliberately distinct from a rejection.  A rejection describes a defective
    *record* inside an artifact, is counted under a named class and is carried on
    from; this exception describes a defective *call* -- the wrong tool identifier, a
    relative root, another tool's path base, a path base whose kind is not
    ``bytecode_class``, a source index that is not one, or a document that is not a
    mapping -- and stops the caller rather than being absorbed into a rejection count.

    A ``ValueError`` subclass rather than a bare ``assert``: ``python -O`` strips
    ``assert``, and an invariant that disappears under optimisation is not an
    invariant.  AAP 0.5.4's *"reject rather than infer"* governs record content; a
    caller fault is neither rejected nor inferred, it is raised.
    """


# --------------------------------------------------------------------------- #
# Fixed policy: the tool, its scanner class, the twelve fields
# --------------------------------------------------------------------------- #

#: The canonical tool identifier this adapter serves.
#:
#: One tool, unlike the shared SARIF adapter, so ``tool`` is still a required argument
#: to :func:`adapt` -- the uniform entry point every adapter exposes -- and is checked
#: against this value rather than trusted.  Handing this module another tool's
#: identifier would stamp the wrong ``tool`` into every row of a dataset that otherwise
#: looked well-formed.
TOOL: Final[str] = "joern"

#: The ``scanner_class`` every row from this adapter carries.
#:
#: AAP 0.5.4's class table fixes ``sast`` for ``opengrep``, ``semgrep``,
#: ``datadog-static-analyzer`` and ``joern``, and ``joern`` is the one of the four that
#: writes a native shape.  Authored here rather than imported from ``shape.py`` because
#: AAP 0.6.4 permits an adapter to import ``paths`` and ``severity`` and nothing else;
#: ``shape.py`` keeps the same separation from the other direction, naming an adapter by
#: string key rather than importing it.  The duplication is required by the import
#: constraint, not an oversight.
SCANNER_CLASS: Final[str] = "sast"

#: The twelve fields, in the request's order (AAP 0.8.2).
#:
#: ``emit.py`` owns ``FIELDS`` as the single authored constant everything downstream
#: keys on and cannot be imported from here, so this copy must agree with it by
#: construction.  Every row carries all twelve keys in this order,
#: present-with-``None`` rather than omitted, so the CSV column set is uniform.
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
#: ``path`` is not among them -- AAP 0.5.4 states *"``path`` is not an optional
#: field"* -- so a record whose coordinate cannot be resolved is rejected and counted
#: rather than emitted with a null path.  ``severity_norm`` is likewise never absent,
#: which ``severity.py`` enforces on every construction of its result.
ABSENCE_PERMITTED_FIELDS: Final[frozenset[str]] = frozenset(
    {"severity_native", "start_line", "cwe", "cve", "package_coordinate"}
)

#: ``cwe``, ``cve`` and ``package_coordinate`` are always ``None`` for this shape.
#:
#: A Joern finding names a bytecode call site, not a weakness catalogue entry and not a
#: package.  AAP 0.5.4's ``joern`` row lists neither, and manufacturing one from a class
#: name would be inference.  The absence of ``package_coordinate`` is explicitly **not**
#: a rejection here: the ``unformable_package_coordinate`` class covers a
#: *dependency-oriented* record, and this shape is not one.
_CWE: Final[None] = None
_CVE: Final[None] = None
_PACKAGE_COORDINATE: Final[None] = None


# --------------------------------------------------------------------------- #
# Member names, for both shapes.  Read in a fixed precedence so a document
# carrying either is handled and the choice is reproducible.
# --------------------------------------------------------------------------- #

#: The findings array: the count unit, and the only element that produces rows.
FINDINGS_KEY: Final[str] = "findings"

#: The rule identifier, in precedence order: the documented collector's ``rule_id``
#: first, then this provisioning's ``query_id``.  Both name the query that fired, which
#: is what AAP 0.5.4 means by *"rule_id <- the query identifier"*.
RULE_ID_FIELDS: Final[tuple[str, ...]] = ("rule_id", "query_id")

#: The finding's description.  Both shapes spell it the same way.
MESSAGE_FIELDS: Final[tuple[str, ...]] = ("message",)

#: The query's severity label, where the query defines one.  Absent in the documented
#: shape; ``HIGH`` or ``MEDIUM`` in this provisioning's six baked queries.
SEVERITY_FIELDS: Final[tuple[str, ...]] = ("severity",)

#: The reported line, in precedence order: documented ``start_line``, then this
#: provisioning's ``line``.
START_LINE_FIELDS: Final[tuple[str, ...]] = ("start_line", "line")

#: The bytecode class coordinate, in precedence order: this provisioning's ``class``
#: (a dotted type full name), then the documented ``class_file`` (a class-file path).
#: :func:`normalize.paths.class_key` accepts either form, so the precedence decides
#: which field is *read*, never how it is interpreted.
CLASS_COORDINATE_FIELDS: Final[tuple[str, ...]] = ("class", "class_file")

#: The collector's own already-``$SPARK_SRC``-relative answer, where it supplies one.
#: Emitted as-is; see this module's docstring for the precedence against a class
#: resolution of this run's own.
COLLECTOR_PATH_FIELD: Final[str] = "path"

#: The collector's explanation of how it resolved -- or failed to resolve -- a class.
#: Retained in a rejection's detail and in **no** dataset field (AAP 0.5.4).
COLLECTOR_EXPLANATION_FIELD: Final[str] = "path_resolution"

#: The three values the documented collector's explanation takes.  Held for
#: documentation: an unlisted value is retained verbatim rather than refused, because
#: the explanation is evidence about the collector and not a vocabulary this adapter
#: polices.  ``unresolved-bytecode-only`` is taken from ``paths.py`` so the two modules
#: cannot spell it differently.
COLLECTOR_EXPLANATIONS: Final[tuple[str, ...]] = (
    paths.BASIS_SOURCE_INDEX_FILENAME,
    paths.BASIS_SOURCE_INDEX_DECLARATION,
    paths.COLLECTOR_UNRESOLVED_BYTECODE_ONLY,
)

#: Fields carried into a rejection's identity as *context* so the rejection is
#: diagnosable, and into no row: the schema is exactly twelve fields.  ``method`` and
#: ``method_full_name`` are the enclosing method, ``callee`` the matched call, and
#: ``file`` the field the metadata names as the one to ignore -- retained here as
#: evidence of what the collector reported, never read as a path.
CONTEXT_FIELDS: Final[tuple[str, ...]] = (
    "method_full_name",
    "method",
    "callee",
    "file",
)

#: The sentinel ``harness/lib/joern-scan.sc`` writes where a method has no enclosing
#: type declaration: ``m.typeDecl.fullName.headOption.getOrElse("<unknown>")`` at line
#: 96.  It names the *absence* of a coordinate, so it is classified ``absent_path``
#: rather than resolved -- keying an index on the literal ``<unknown>`` would turn a
#: stated absence into an ordinary lookup miss and lose why the record failed.
COLLECTOR_UNKNOWN_CLASS: Final[str] = "<unknown>"

#: The ``rule_id`` literals both shapes are known to emit, for documentation and for a
#: validation cross-check.
#:
#: **Never used to filter.**  AAP 0.4.2 has the baked query bundle's actual composition
#: *"read from the runner"*, so an identifier outside this set is a legitimate finding
#: from a query set this constant predates -- dropping it would silently shrink a tool's
#: count.  The first six are the queries
#: ``harness/lib/joern-scan.sc`` bakes in this provisioning, read from its lines 50-78;
#: the last five are the documented collector's literals, kept so a reader of either
#: shape recognises what they are looking at.
KNOWN_RULE_IDS: Final[tuple[str, ...]] = (
    "joern-process-exec",
    "joern-unsafe-deserialization",
    "joern-reflection-forname",
    "joern-message-digest",
    "joern-cipher-getinstance",
    "joern-xml-factory",
    "joern.process-launch-site",
    "joern.java-deserialization-site",
    "joern.reflective-class-load",
    "joern.weak-hash-algorithm",
    "joern.rpc-handler-reaches-process-launch",
)

#: Envelope members that carry an integer this adapter surfaces as an ``envelope_``
#: counter, mapped to the counter suffix.  Metadata for ``tool-status.md``; **no**
#: envelope member produces a row and none enters the reconciliation record count.
#: ``queries[].returned``/``queries[].count`` are deliberately absent: they are the
#: collector's own tallies, and AAP 0.5.4 makes ``findings[]`` the reconciliation unit.
_ENVELOPE_INT_FIELDS: Final[Mapping[str, str]] = {
    "cpg_methods": "graph_methods",
    "cpg_typedecls": "graph_type_declarations",
    "source_index_size": "collector_source_index_size",
    "declaration_index_size": "collector_declaration_index_size",
}

#: The nested ``graph`` object this provisioning writes, and the same mapping for it.
_ENVELOPE_GRAPH_KEY: Final[str] = "graph"
_ENVELOPE_GRAPH_INT_FIELDS: Final[Mapping[str, str]] = {
    "methods": "graph_methods",
    "type_declarations": "graph_type_declarations",
    "files": "graph_files",
}

#: The envelope's query record, walked for its declared count and its truncation flag
#: only.
_QUERIES_KEY: Final[str] = "queries"
_QUERY_BOUND_REACHED_KEY: Final[str] = "bound_reached"


# --------------------------------------------------------------------------- #
# The counter key set.  Fixed and fully pre-initialised, so every call returns
# the same keys and a caller aggregating across artifacts never has to guess
# whether a missing key means zero or means "this adapter forgot".
# --------------------------------------------------------------------------- #

#: Records carrying more than one location.  The row takes the first, the record still
#: counts once, and the number is reported per tool (AAP 0.5.4).  This shape carries one
#: location per finding, so the expected value is zero -- published anyway, because a
#: shared representation decision whose counter is missing is one nobody can check.
COUNTER_MULTI_LOCATION: Final[str] = "multi_location_records"

#: Rows whose path names something other than a file in the scanned tree.
#: ``run-record.md`` reports the count and the proportion (AAP 0.6.1).  A resolution
#: from the source index is a real file, so this moves only for a collector-supplied
#: path that lands outside the root or names an archive member.
COUNTER_NON_FILESYSTEM_PATHS: Final[str] = "non_filesystem_paths"

#: The ``in_scope`` decomposition of the emitted rows.  Their sum is the row count, so
#: this is one measurement split rather than a second count of the same thing.
COUNTER_ROWS_IN_SCOPE: Final[str] = "rows_in_scope"
COUNTER_ROWS_OUT_OF_SCOPE: Final[str] = "rows_out_of_scope"

#: Rows whose resolved path lies in a ``src/test`` tree.  These are **retained** with
#: ``in_scope: false`` (AAP 0.5.4 and 0.9.3), and this is the only place that number is
#: visible -- a dropped test-JAR row and a retained one look identical in a row count.
COUNTER_ROWS_FROM_SRC_TEST: Final[str] = "rows_from_src_test"

#: Rows carrying no ``start_line``.  Absence is permitted for that field, so this is the
#: only way the number is visible.
COUNTER_START_LINE_ABSENT: Final[str] = "start_line_absent"

#: Where each row's severity came from.  ``severity_from_record_label`` counts a query
#: that defined one; ``severity_absent`` counts the no-vocabulary path, which states the
#: absence rather than assuming a level.
COUNTER_SEVERITY_FROM_RECORD_LABEL: Final[str] = "severity_from_record_label"
COUNTER_SEVERITY_ABSENT: Final[str] = "severity_absent"

#: How the path was arrived at.  ``resolution_from_class`` is this run's own unique
#: class-to-source resolution; ``collector_path_used`` is a collector-supplied
#: root-relative path emitted as-is because this run could resolve nothing;
#: ``collector_path_disagreed`` counts the records where both resolved and differed --
#: the row keeps this run's answer and the disagreement is recorded, never suppressed.
COUNTER_RESOLUTION_FROM_CLASS: Final[str] = "resolution_from_class"
COUNTER_COLLECTOR_PATH_USED: Final[str] = "collector_path_used"
COUNTER_COLLECTOR_PATH_DISAGREED: Final[str] = "collector_path_disagreed"
COUNTER_COLLECTOR_PATH_CORROBORATED: Final[str] = "collector_path_corroborated"

#: Records carrying the collector's own explanation, and records whose class field is
#: the collector's ``<unknown>`` sentinel.  Both are evidence about the collector, and
#: both stay out of every dataset field.
COUNTER_COLLECTOR_EXPLANATION_PRESENT: Final[str] = "collector_explanation_present"
COUNTER_UNKNOWN_CLASS_SENTINEL: Final[str] = "unknown_class_sentinel_records"

#: Whether the caller injected a source index (``1``) or this adapter built one from the
#: root (``0``), and the index's own shape.  A rejection count is only interpretable
#: beside the index that produced it: 585 unresolvable findings against an index of
#: 6,759 files means something different from 585 against an index of 12.
COUNTER_SOURCE_INDEX_SUPPLIED: Final[str] = "source_index_supplied"
COUNTER_SOURCE_INDEX_FILES: Final[str] = "source_index_files_indexed"
COUNTER_SOURCE_INDEX_FILENAME_KEYS: Final[str] = "source_index_by_filename_keys"
COUNTER_SOURCE_INDEX_DECLARATION_KEYS: Final[str] = "source_index_by_decl_keys"
COUNTER_SOURCE_INDEX_AMBIGUOUS_FILENAME: Final[str] = "source_index_ambiguous_by_filename"
COUNTER_SOURCE_INDEX_AMBIGUOUS_DECLARATION: Final[str] = "source_index_ambiguous_by_decl"
COUNTER_SOURCE_INDEX_DECLARATIONS_READ: Final[str] = "source_index_declarations_read"

#: The envelope, and the one query-level fact worth a counter: how many queries reported
#: reaching their traversal bound.  Truncation is observed rather than assumed because
#: the baked script takes ``bound + 1`` and reports the flag; a truncated query's
#: silence would otherwise be indistinguishable from a clean one.
COUNTER_ENVELOPE_QUERIES_DECLARED: Final[str] = "envelope_queries_declared"
COUNTER_ENVELOPE_QUERIES_BOUND_REACHED: Final[str] = "envelope_queries_bound_reached"

#: Prefixes for the vocabularies that are *derived* rather than authored, so this
#: adapter's counter set cannot drift from the vocabularies it reports against: one key
#: per :data:`normalize.paths.PATH_KINDS` member, one per
#: :data:`normalize.severity.BASIS_VALUES` member, one per member of each field-candidate
#: tuple (which field of which shape supplied the value), and one per envelope integer.
COUNTER_PATH_KIND_PREFIX: Final[str] = "path_kind_"
COUNTER_SEVERITY_BASIS_PREFIX: Final[str] = "severity_basis_"
COUNTER_RULE_ID_FIELD_PREFIX: Final[str] = "rule_id_from_"
COUNTER_COORDINATE_FIELD_PREFIX: Final[str] = "coordinate_from_"
COUNTER_START_LINE_FIELD_PREFIX: Final[str] = "start_line_from_"
COUNTER_ENVELOPE_PREFIX: Final[str] = "envelope_"

_AUTHORED_COUNTER_KEYS: Final[tuple[str, ...]] = (
    COUNTER_MULTI_LOCATION,
    COUNTER_NON_FILESYSTEM_PATHS,
    COUNTER_ROWS_IN_SCOPE,
    COUNTER_ROWS_OUT_OF_SCOPE,
    COUNTER_ROWS_FROM_SRC_TEST,
    COUNTER_START_LINE_ABSENT,
    COUNTER_SEVERITY_FROM_RECORD_LABEL,
    COUNTER_SEVERITY_ABSENT,
    COUNTER_RESOLUTION_FROM_CLASS,
    COUNTER_COLLECTOR_PATH_USED,
    COUNTER_COLLECTOR_PATH_DISAGREED,
    COUNTER_COLLECTOR_PATH_CORROBORATED,
    COUNTER_COLLECTOR_EXPLANATION_PRESENT,
    COUNTER_UNKNOWN_CLASS_SENTINEL,
    COUNTER_SOURCE_INDEX_SUPPLIED,
    COUNTER_SOURCE_INDEX_FILES,
    COUNTER_SOURCE_INDEX_FILENAME_KEYS,
    COUNTER_SOURCE_INDEX_DECLARATION_KEYS,
    COUNTER_SOURCE_INDEX_AMBIGUOUS_FILENAME,
    COUNTER_SOURCE_INDEX_AMBIGUOUS_DECLARATION,
    COUNTER_SOURCE_INDEX_DECLARATIONS_READ,
    COUNTER_ENVELOPE_QUERIES_DECLARED,
    COUNTER_ENVELOPE_QUERIES_BOUND_REACHED,
)

#: The envelope integer counter names, deduplicated and ordered, derived from the two
#: shape mappings so the key set follows the mappings rather than a hand-kept list.
_ENVELOPE_COUNTER_KEYS: Final[tuple[str, ...]] = tuple(
    dict.fromkeys(
        f"{COUNTER_ENVELOPE_PREFIX}{suffix}"
        for suffix in (
            *_ENVELOPE_INT_FIELDS.values(),
            *_ENVELOPE_GRAPH_INT_FIELDS.values(),
        )
    )
)

#: Every key :func:`new_counters` initialises, in a stable order.
#:
#: Note what is deliberately **absent**: no count of the findings walked, and none of
#: the rows or rejections produced.  ``len(rows)`` and ``len(rejections)`` are returned
#: to the caller directly, and a record count taken from *this* traversal would be an
#: attractive nuisance on the left-hand side of
#: ``raw finding records = dataset rows + rejected records`` -- the one place AAP 0.5.4
#: requires a genuinely independent traversal, which is ``reconcile.count_records``.
#: Publishing a plausible substitute for it here is how that requirement would quietly
#: be lost.  The sum of ``queries[].returned`` is absent for the same reason.
COUNTER_KEYS: Final[tuple[str, ...]] = (
    *_AUTHORED_COUNTER_KEYS,
    *_ENVELOPE_COUNTER_KEYS,
    *(f"{COUNTER_PATH_KIND_PREFIX}{kind}" for kind in paths.PATH_KINDS),
    *(f"{COUNTER_SEVERITY_BASIS_PREFIX}{basis}" for basis in severity.BASIS_VALUES),
    *(f"{COUNTER_RULE_ID_FIELD_PREFIX}{name}" for name in RULE_ID_FIELDS),
    *(f"{COUNTER_COORDINATE_FIELD_PREFIX}{name}" for name in CLASS_COORDINATE_FIELDS),
    *(f"{COUNTER_START_LINE_FIELD_PREFIX}{name}" for name in START_LINE_FIELDS),
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
    """Return ``value`` where it is a JSON array, else an empty sequence.

    An absent, empty or non-array ``findings`` contributes nothing and is not an error,
    which is exactly how ``reconcile._count_joern`` reads it -- ``_length`` over a
    non-array yields zero.  The two agreeing on zero is what keeps the identity exact
    for a document that carries no findings at all.
    """
    return value if _is_json_array(value) else ()


def _json_object(value: Any) -> Mapping[str, Any] | None:
    """Return ``value`` where it is a JSON object, else ``None``."""
    return value if isinstance(value, Mapping) else None


def _non_empty_string(value: Any) -> str | None:
    """Return ``value`` stripped where it is a non-blank string, else ``None``.

    Whitespace-only is treated as absent rather than as content: a rule identifier or a
    message of three spaces names nothing, and emitting it would put an unusable value
    in a field the schema requires.
    """
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _type_name(value: Any) -> str:
    """Return a readable type name for a rejection detail."""
    return type(value).__name__


def _positive_int(value: Any) -> int | None:
    """Return ``value`` where it is a genuine positive integer, else ``None``.

    ``bool`` is excluded even though Python's numeric tower admits ``True`` as ``1``:
    a flag arriving as a count is the kind of defect a record should expose rather than
    absorb.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value > 0 else None


def _non_negative_int(value: Any) -> int | None:
    """Return ``value`` where it is a genuine non-negative integer, else ``None``.

    Used for the envelope's own integers, where zero is a legitimate measurement -- a
    graph with no files would be a finding about the graph, not a value to discard.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value >= 0 else None


# --------------------------------------------------------------------------- #
# Field reads.  Each returns the value plus the field it came from, so the
# provenance counter records *which* shape supplied it rather than leaving the
# two indistinguishable.
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class _FieldRead:
    """One field read from a finding: the raw value, and the member it came from.

    ``field`` is ``None`` where no candidate member was present at all, which is what
    separates "the shape carries this field and its value is unusable" from "neither
    shape's spelling of this field is present".  The two earn different rejection
    details and a reader of ``tool-status.md`` acts differently on each.
    """

    value: Any = None
    field: str | None = None

    @property
    def present(self) -> bool:
        """Whether a candidate member was present, whatever its value."""
        return self.field is not None


def _read_first(record: Mapping[str, Any], candidates: Iterable[str]) -> _FieldRead:
    """Return the first candidate member present in ``record``, in candidate order.

    Presence rather than truthiness is the test: a member present with an unusable
    value must reach the classification step that names *why* it is unusable, not be
    skipped in favour of the next candidate.  Skipping would let a ``null`` ``rule_id``
    be answered by a ``query_id`` from the other shape, mixing the two.
    """
    for name in candidates:
        if name in record:
            return _FieldRead(value=record[name], field=name)
    return _FieldRead()


def _rule_id(record: Mapping[str, Any]) -> tuple[_FieldRead, tuple[str, str] | None]:
    """Return the finding's rule identifier, or the rejection it earns.

    AAP 0.5.4: ``rule_id`` <- the query identifier.  An absent, empty or non-string
    identifier is the ``missing_rule_id`` rejection condition; a value outside
    :data:`KNOWN_RULE_IDS` is **not**, because AAP 0.4.2 has the baked query set read
    from the runner and an unlisted identifier is a legitimate finding from a set this
    constant predates.
    """
    read = _read_first(record, RULE_ID_FIELDS)
    resolved = _non_empty_string(read.value)
    if resolved is not None:
        return _FieldRead(value=resolved, field=read.field), None
    if not read.present:
        return read, (
            paths.REJECT_MISSING_RULE_ID,
            "the finding carries no rule identifier under any of the members this "
            f"shape uses ({', '.join(RULE_ID_FIELDS)})",
        )
    if read.value is None:
        reason = f"the finding's {read.field} is null, so it names no query"
    elif isinstance(read.value, str):
        reason = f"the finding's {read.field} is empty or whitespace only"
    else:
        reason = (
            f"the finding's {read.field} is a {_type_name(read.value)}, not a string"
        )
    return read, (paths.REJECT_MISSING_RULE_ID, reason)


def _message(record: Mapping[str, Any]) -> tuple[str | None, tuple[str, str] | None]:
    """Return the finding's description, or the rejection it earns.

    AAP 0.5.4: ``message`` <- the finding's description.  Absent, null or empty is the
    ``missing_message`` rejection condition.  A non-string message is structurally wrong
    rather than merely absent, so it is ``malformed_record`` -- a reader acts differently
    on "this collector omitted the text" than on "this artifact is not shaped as
    expected".
    """
    read = _read_first(record, MESSAGE_FIELDS)
    resolved = _non_empty_string(read.value)
    if resolved is not None:
        return resolved, None
    if not read.present:
        return None, (
            paths.REJECT_MISSING_MESSAGE,
            "the finding carries no message member",
        )
    if read.value is None:
        return None, (
            paths.REJECT_MISSING_MESSAGE,
            f"the finding's {read.field} is null",
        )
    if isinstance(read.value, str):
        return None, (
            paths.REJECT_MISSING_MESSAGE,
            f"the finding's {read.field} is empty or whitespace only",
        )
    return None, (
        paths.REJECT_MALFORMED_RECORD,
        f"the finding's {read.field} is a {_type_name(read.value)}, not a string",
    )


def _start_line(
    record: Mapping[str, Any],
) -> tuple[int | None, str | None, tuple[str, str] | None]:
    """Return the reported line, the member it came from, or the rejection it earns.

    Absence is permitted for ``start_line`` (AAP 0.8.2), so a member that is absent or
    explicitly ``null`` yields ``None`` with no rejection -- and ``null`` is the ordinary
    shape here, since ``harness/lib/joern-scan.sc`` writes ``"line": null`` wherever a
    call carries no line number.

    A value that is present and non-null but unusable as a line number is the
    ``non_integer_start_line`` rejection condition (AAP 0.5.4).  Three shapes reach it,
    each named in the detail: a non-integer type, ``True``/``False`` -- which Python's
    numeric tower would otherwise admit as ``1`` and ``0`` -- and a value below ``1``,
    since a line is numbered from one and ``0`` is not a line.  The closed class carries
    all three and the detail carries the sub-reason, exactly as AAP 0.5.4 does for the
    ``uriBaseId`` terminal cases.
    """
    read = _read_first(record, START_LINE_FIELDS)
    if not read.present or read.value is None:
        return None, read.field, None
    resolved = _positive_int(read.value)
    if resolved is not None:
        return resolved, read.field, None
    if isinstance(read.value, bool) or not isinstance(read.value, int):
        return None, read.field, (
            paths.REJECT_NON_INTEGER_START_LINE,
            f"the finding's {read.field} is {read.value!r}, a "
            f"{_type_name(read.value)} rather than an integer",
        )
    return None, read.field, (
        paths.REJECT_NON_INTEGER_START_LINE,
        f"the finding's {read.field} is {read.value}, which is not a line number: "
        "a line is numbered from one",
    )


def _severity_of(record: Mapping[str, Any]) -> tuple[severity.SeverityResult, str]:
    """Return the finding's severity and the counter to bump.

    AAP 0.5.4 places ``joern`` under *"No vocabulary at all (gitleaks; joern **unless a
    query defines one**) -- ``severity_native`` absent, ``severity_norm`` ``Info``, the
    absence stated rather than a level assumed"*.  Both halves of that are implemented,
    and which one applies is a property of the artifact rather than of this module:

    * a query that **defines** a severity supplies a label, and it is mapped through
      ``severity.py``'s label table with the basis and the selected entry recorded.  The
      six queries baked into ``harness/lib/joern-scan.sc`` define ``HIGH`` and
      ``MEDIUM``, so this is the live path for this provisioning;
    * a record with **no** label takes ``severity.py``'s no-vocabulary path, which
      returns ``severity_native`` ``None`` with ``severity_norm`` ``Info`` and a basis
      that *states* the absence.

    Nothing is hard-coded and nothing is synthesised: a native label is never
    manufactured from the rule identifier, and ``Info`` is never written directly --
    ``severity.py`` owns the vocabulary, the precedence and the bands, so this adapter
    cannot disagree with ``severity-map.md``.  A literal outside the mapped vocabulary
    is disclosed by ``severity.py`` as unmapped and banded ``Info`` with the literal
    retained, which is how it reaches ``severity-map.md`` with the rows it affected.
    """
    read = _read_first(record, SEVERITY_FIELDS)
    result = severity.resolve(label=read.value)
    if result.basis == severity.BASIS_NO_VOCABULARY:
        return result, COUNTER_SEVERITY_ABSENT
    return result, COUNTER_SEVERITY_FROM_RECORD_LABEL


# --------------------------------------------------------------------------- #
# The path.  Every filesystem decision is delegated to paths.py; see this
# module's docstring for the precedence and for why the collector's own answer
# does not override an ambiguity.
# --------------------------------------------------------------------------- #


def _collector_explanation(record: Mapping[str, Any]) -> str | None:
    """Return the collector's own resolution explanation, verbatim, or ``None``.

    Retained for a rejection's detail and for **no** dataset field (AAP 0.5.4: *"any
    collector explanation for an unmappable bytecode path is retained in the rejection
    record, not in a dataset field"*).  A value outside :data:`COLLECTOR_EXPLANATIONS`
    is kept as it stands rather than refused: the explanation is evidence about the
    collector, and normalising it would destroy the very thing it is retained for.
    """
    return _non_empty_string(record.get(COLLECTOR_EXPLANATION_FIELD))


def _coordinate_field(tool_base: paths.ToolPathBase) -> tuple[str, ...]:
    """Return the class-coordinate members to read, in the order to read them.

    The metadata leads, because AAP 0.5.4 requires *"every base taken from the recorded
    runner metadata"*: where ``path_base.record_path_field`` names one of
    :data:`CLASS_COORDINATE_FIELDS`, that member is tried first and the remaining
    candidates follow, so a document carrying the other shape still resolves.

    ``path_base.record_path_field_to_ignore`` is honoured as an exclusion rather than a
    hint.  In this provisioning it names ``file``, which is the frontend's ephemeral
    extraction path -- reading it as a coordinate would relativize a ``/tmp`` path into a
    plausible-looking wrong answer for every row.  ``file`` is not a candidate here in
    any case, so the exclusion is defensive; but were the metadata ever to name a member
    that *is* a candidate, that member is dropped and the record is rejected for want of
    a coordinate rather than resolved against the metadata's own instruction.
    """
    ignored = tool_base.record_path_field_to_ignore
    preferred = tool_base.record_path_field
    ordered: list[str] = []
    if isinstance(preferred, str) and preferred in CLASS_COORDINATE_FIELDS:
        ordered.append(preferred)
    ordered.extend(name for name in CLASS_COORDINATE_FIELDS if name not in ordered)
    if isinstance(ignored, str):
        ordered = [name for name in ordered if name != ignored]
    return tuple(ordered)


def _collector_path(
    record: Mapping[str, Any],
    *,
    corroboration: str | None,
    record_identity: Mapping[str, Any],
) -> paths.ResolvedPath | paths.Rejection | None:
    """Return the collector's already-root-relative path, or the rejection it earns.

    ``None`` where the collector supplied no path at all, which is the ordinary case for
    this provisioning -- ``harness/lib/joern-scan.sc`` writes no ``path`` member -- and
    which leaves the caller's own rejection standing.

    **The path is emitted as-is.**  It is *already* ``$SPARK_SRC``-relative (the
    collector's own resolved answer), so it is neither relativized again nor joined onto
    the root: doing either would produce a doubled path or an absolute one.  This is
    also why ``paths.resolve_recorded_path`` is deliberately not called -- it raises for
    a ``bytecode_class`` base precisely to stop a bytecode coordinate reaching the
    filesystem resolver, and its relative branch would join this value onto a base.

    What *is* checked is that the value is a legal emitted path.  ``assert_relative_path``
    -- which every :class:`~normalize.paths.ResolvedPath` runs on construction -- refuses
    an absolute path, a Windows drive prefix, a URI form and a second ``!`` separator, so
    a collector that ever wrote an absolute path here is a counted rejection rather than
    an absolute value in the dataset (AAP 0.8.2: *"No absolute path is ever emitted"*).
    The kind is read off the serialized form by ``paths.path_kind_for``, so an archive
    member or an outside-the-root coordinate is discriminated by the string that actually
    reaches the dataset.
    """
    raw = record.get(COLLECTOR_PATH_FIELD)
    if raw is None:
        return None
    if not isinstance(raw, str):
        return paths.make_rejection(
            paths.REJECT_MALFORMED_RECORD,
            TOOL,
            f"the finding's {COLLECTOR_PATH_FIELD} is a {_type_name(raw)}, not a "
            "string, so the collector's own resolved answer cannot be read",
            **dict(record_identity),
        )
    candidate = _non_empty_string(paths.normalise_reported_path(raw))
    if candidate is None:
        return None
    try:
        return paths.ResolvedPath(
            path=candidate,
            kind=paths.path_kind_for(candidate),
            basis=paths.BASIS_ALREADY_ROOT_RELATIVE,
            tool=TOOL,
            corroboration=corroboration,
        )
    except paths.PathPolicyError as error:
        return paths.make_rejection(
            paths.REJECT_UNRESOLVABLE_PATH,
            TOOL,
            f"the collector's {COLLECTOR_PATH_FIELD} {raw!r} is not a legal "
            f"root-relative path: {error}",
            **dict(record_identity),
        )


@dataclass(frozen=True)
class _PathOutcome:
    """The path decision for one finding: a resolution, or the rejection it earned.

    Exactly one of the two is set.  ``counters_to_bump`` names the provenance counters
    the caller applies once the record's outcome is known, so the decision and its
    bookkeeping stay in one place rather than being re-derived at the call site.
    """

    resolved: paths.ResolvedPath | None
    rejection: paths.Rejection | None
    counters_to_bump: tuple[str, ...] = ()


def _resolve_path(
    record: Mapping[str, Any],
    *,
    tool_base: paths.ToolPathBase,
    index: paths.SourceIndex,
    root: str,
    coordinate_fields: tuple[str, ...],
    record_identity: Mapping[str, Any],
) -> _PathOutcome:
    """Resolve one finding's coordinate, closing both collector gaps.

    The precedence this module's docstring fixes, implemented once:

    1. **this run's own class-to-source resolution governs.**  The class coordinate is
       handed to :func:`normalize.paths.resolve_bytecode_class`, which keys both the
       filename and the declaration schemes over ``src/main`` **and** ``src/test`` (gap
       1) and succeeds only where the union of candidates is exactly one distinct path
       (gap 2).  The declaration scheme is not optional: ``RangePartitioner`` is declared
       in ``Partitioner.scala``, so a filename-only index loses it silently;
    2. an **ambiguous** key is returned as the rejection ``paths.py`` built, with every
       competing candidate already named in its detail.  It is **never** overridden by
       the collector's path -- doing so would reinstate the ``setdefault`` first-wins
       guess AAP 0.1.3 forbids;
    3. **unresolvable, absent or malformed**: the collector's own root-relative answer is
       used where it supplied one, emitted as-is and marked as uncorroborated; otherwise
       ``paths.py``'s rejection stands, with the collector's explanation appended to its
       detail;
    4. where both resolved and **disagree**, this run's answer is kept and the
       disagreement is recorded in ``corroboration``.  The row is never suppressed
       (AAP 0.5.3's posture on a Checkov mismatch, applied unchanged).

    A ``src/test`` resolution is a **success** here, not an exclusion: it is retained and
    the literal ``src/test`` rule in ``paths.in_scope`` then gives the row
    ``in_scope: false`` (AAP 0.5.4 and 0.9.3).  Nothing is pre-filtered on it.
    """
    explanation = _collector_explanation(record)
    read = _read_first(record, coordinate_fields)
    identity = dict(record_identity)
    if read.field is not None:
        identity.setdefault("coordinate_field", read.field)

    coordinate_counters: tuple[str, ...] = (
        (f"{COUNTER_COORDINATE_FIELD_PREFIX}{read.field}",)
        if read.field is not None
        else ()
    )

    # The collector's ``<unknown>`` sentinel names the *absence* of a type declaration
    # rather than a class to look up, so it is classified as an absent coordinate with
    # the sentinel named.  Keying an index on the literal would turn a stated absence
    # into an ordinary lookup miss and lose why the record failed.
    if isinstance(read.value, str) and read.value.strip() == COLLECTOR_UNKNOWN_CLASS:
        fallback = _collector_path(
            record,
            corroboration=(
                "the collector reported the sentinel "
                f"{COLLECTOR_UNKNOWN_CLASS!r} for {read.field}, so this run resolved "
                f"no class of its own and the collector's {COLLECTOR_PATH_FIELD} was "
                "taken as reported"
            ),
            record_identity=identity,
        )
        if isinstance(fallback, paths.ResolvedPath):
            return _PathOutcome(
                resolved=fallback,
                rejection=None,
                counters_to_bump=(
                    *coordinate_counters,
                    COUNTER_UNKNOWN_CLASS_SENTINEL,
                    COUNTER_COLLECTOR_PATH_USED,
                ),
            )
        if isinstance(fallback, paths.Rejection):
            return _PathOutcome(
                resolved=None,
                rejection=fallback,
                counters_to_bump=(*coordinate_counters, COUNTER_UNKNOWN_CLASS_SENTINEL),
            )
        return _PathOutcome(
            resolved=None,
            rejection=paths.make_rejection(
                paths.REJECT_ABSENT_PATH,
                TOOL,
                _with_explanation(
                    f"the finding's {read.field} is the collector's "
                    f"{COLLECTOR_UNKNOWN_CLASS!r} sentinel, written where a method has "
                    "no enclosing type declaration, so it names no class to resolve; "
                    "the metadata records "
                    f"{tool_base.record_path_field_to_ignore or 'file'} as the field to "
                    "ignore, so no coordinate remains",
                    explanation,
                ),
                **identity,
            ),
            counters_to_bump=(*coordinate_counters, COUNTER_UNKNOWN_CLASS_SENTINEL),
        )

    resolved = paths.resolve_bytecode_class(
        read.value,
        index,
        root,
        tool=TOOL,
        collector_explanation=explanation,
        record_identity=identity,
    )

    if isinstance(resolved, paths.ResolvedPath):
        # The collector's own answer, where it supplied one, is read purely to
        # corroborate: this run's unique resolution is what reaches the row either way.
        collector = _collector_path(
            record,
            corroboration=None,
            record_identity=identity,
        )
        note: str | None = None
        extra_counters: tuple[str, ...] = ()
        if isinstance(collector, paths.ResolvedPath):
            if collector.path == resolved.path:
                extra_counters = (COUNTER_COLLECTOR_PATH_CORROBORATED,)
            else:
                # Both answers exist and differ.  This run's own unique resolution is
                # kept and the disagreement is recorded rather than resolved -- and the
                # row is never suppressed for it.
                note = (
                    f"the collector reported {collector.path!r} for this finding while "
                    f"this run's unique resolution is {resolved.path!r}; the resolution "
                    "is kept and the disagreement recorded rather than one being "
                    "silently preferred"
                )
                extra_counters = (COUNTER_COLLECTOR_PATH_DISAGREED,)
        elif isinstance(collector, paths.Rejection):
            # The record resolves on its own coordinate, so an unreadable collector path
            # is not a reason to reject it -- but it is evidence, and evidence that is
            # dropped rather than recorded is evidence nobody can act on.
            note = (
                f"this run's resolution stands; the collector's "
                f"{COLLECTOR_PATH_FIELD} member could not be read as a "
                f"root-relative path ({collector.detail})"
            )
        if note is not None:
            resolved = paths.ResolvedPath(
                path=resolved.path,
                kind=resolved.kind,
                basis=resolved.basis,
                tool=resolved.tool,
                corroboration=_join_corroboration(resolved.corroboration, note),
            )
        return _PathOutcome(
            resolved=resolved,
            rejection=None,
            counters_to_bump=(
                *coordinate_counters,
                COUNTER_RESOLUTION_FROM_CLASS,
                *extra_counters,
            ),
        )

    # A rejection.  An ambiguity is final: the collector's first-won answer is exactly
    # what must not be used to break the tie (gap 2).
    if resolved.reject_class == paths.REJECT_AMBIGUOUS_SOURCE_RESOLUTION:
        return _PathOutcome(
            resolved=None,
            rejection=resolved,
            counters_to_bump=coordinate_counters,
        )

    fallback = _collector_path(
        record,
        corroboration=(
            "taken from the collector's own already-root-relative path: this run's "
            "resolution over src/main and src/test produced no unique candidate for "
            "the class, so the reported coordinate is emitted as reported and is "
            "uncorroborated by this run's source index"
        ),
        record_identity=identity,
    )
    if isinstance(fallback, paths.ResolvedPath):
        return _PathOutcome(
            resolved=fallback,
            rejection=None,
            counters_to_bump=(*coordinate_counters, COUNTER_COLLECTOR_PATH_USED),
        )
    if isinstance(fallback, paths.Rejection):
        return _PathOutcome(
            resolved=None,
            rejection=fallback,
            counters_to_bump=coordinate_counters,
        )
    return _PathOutcome(
        resolved=None,
        rejection=resolved,
        counters_to_bump=coordinate_counters,
    )


def _with_explanation(detail: str, explanation: str | None) -> str:
    """Append the collector's own explanation to a rejection detail.

    The same composition ``paths._with_collector_explanation`` performs, applied to the
    details this module builds itself so the explanation reaches every rejection rather
    than only those ``paths.py`` constructed.  It is retained here and nowhere else.
    """
    if not explanation:
        return detail
    return f"{detail}; collector {COLLECTOR_EXPLANATION_FIELD}: {explanation}"


def _join_corroboration(existing: str | None, addition: str) -> str:
    """Join a resolution's own corroboration note with one this module adds."""
    if not existing:
        return addition
    return f"{existing}; {addition}"


# --------------------------------------------------------------------------- #
# Argument validation.
#
# Every one of these raises :class:`JoernAdapterError` rather than returning a
# rejection: a bad argument is a caller fault, and absorbing it into a rejection
# count would let a wrong root or a foreign path base produce a plausible
# dataset for a whole tool.  Each is validated once per call, before any record
# is read, so a fault surfaces on the call rather than on the first record.
# --------------------------------------------------------------------------- #


def _validated_tool(tool: Any) -> str:
    """Return ``tool`` where it is this adapter's canonical identifier, else raise."""
    if not isinstance(tool, str):
        raise JoernAdapterError(
            f"tool must be a canonical tool identifier string; observed "
            f"{_type_name(tool)}"
        )
    if tool != TOOL:
        raise JoernAdapterError(
            f"{tool!r} is not the tool this adapter serves ({TOOL!r}). The identifier "
            "is required rather than inferred so it is checked rather than trusted: "
            "stamping another tool's identifier into every row would produce a dataset "
            "that looked well-formed and attributed a whole artifact to the wrong tool"
        )
    return tool


def _validated_root(root: Any) -> str:
    """Return the scan root as an absolute POSIX-normalised string, else raise.

    A :class:`pathlib.Path` and a string are both accepted -- ``os.fspath`` is imported
    directly so that no environment access is even in scope -- and the result is
    normalised through ``paths.py`` so this module and every resolver agree on the
    root's spelling.

    A relative root is refused here rather than at the first record: it cannot anchor
    anything, and accepting one would produce a plausible-looking wrong answer for every
    row.
    """
    try:
        candidate = fspath(root)
    except TypeError as error:
        raise JoernAdapterError(
            f"root must be a str or an os.PathLike naming the SPARK_SRC root; observed "
            f"{_type_name(root)}"
        ) from error
    if isinstance(candidate, bytes):
        raise JoernAdapterError(
            "root must be a text path, not bytes: every path in the dataset is text, "
            "and decoding one here would guess an encoding"
        )
    if not candidate:
        raise JoernAdapterError("root must not be empty")
    normalised = paths.normalise_reported_path(candidate)
    if not paths.is_absolute_path(normalised):
        raise JoernAdapterError(
            f"root must be an absolute path for the source index to be built over and "
            f"for a coordinate to be expressed against; observed {candidate!r}"
        )
    return normalised


def _validated_tool_base(tool_base: Any, tool: str) -> paths.ToolPathBase:
    """Return ``tool_base`` where it is this tool's recorded path base, else raise.

    Two checks, and neither is ceremony.  ``tool_base`` is the per-tool view over
    ``harness/artifacts/logs/runner-metadata.json``, so handing this adapter another
    tool's view would resolve against the wrong base while every row still looked
    well-formed -- the exact failure AAP 0.5.4 requires *"every base taken from the
    recorded runner metadata"* to prevent.

    The ``kind`` is checked too.  ``bytecode_class`` is what the metadata records for
    ``joern``, and it is the kind that carries the metadata's own instructions: which
    member is the coordinate, and which member is the ephemeral extraction path to
    ignore.  Any other kind means the record handed here describes a filesystem-based
    tool, and resolving a class name as though it were a reported path is precisely the
    mistake ``paths.resolve_recorded_path`` raises to prevent.
    """
    if not isinstance(tool_base, paths.ToolPathBase):
        raise JoernAdapterError(
            f"tool_base must be a paths.ToolPathBase built from the runner metadata; "
            f"observed {_type_name(tool_base)}"
        )
    if tool_base.tool != tool:
        raise JoernAdapterError(
            f"tool_base names {tool_base.tool!r} but the artifact is {tool!r}; "
            "resolving one tool's paths against another tool's recorded base would "
            "produce a wrong path for every row of it"
        )
    if tool_base.kind != paths.PATH_BASE_KIND_BYTECODE_CLASS:
        raise JoernAdapterError(
            f"tool_base records path_base.kind {tool_base.kind!r} for {tool!r}, but "
            f"this adapter resolves a bytecode class coordinate and requires "
            f"{paths.PATH_BASE_KIND_BYTECODE_CLASS!r}. The metadata names the "
            "coordinate member and the ephemeral member to ignore, and a filesystem "
            "base for this tool does not exist to fall back on"
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
        raise JoernAdapterError(
            "allowlist must be an iterable of glob strings, not a single string: a "
            "string would be iterated character by character"
        )
    if not isinstance(allowlist, Iterable):
        raise JoernAdapterError(
            f"allowlist must be an iterable of glob strings from "
            f"paths.load_allowlist(); observed {_type_name(allowlist)}"
        )
    globs = tuple(allowlist)
    for index, glob in enumerate(globs):
        if not isinstance(glob, str) or not glob:
            raise JoernAdapterError(
                f"allowlist entry {index} must be a non-empty glob string; observed "
                f"{glob!r}"
            )
    return globs


def _validated_tally(tally: Any) -> Any:
    """Return ``tally`` where it can record a severity result, else raise.

    The capability is checked rather than the class, so a test double is as acceptable
    as a :class:`normalize.severity.LiteralTally`.  ``None`` is not: every row's literal
    has to reach ``severity-map.md`` -- which is where AAP 0.6.2 has ``joern``'s
    severity posture named explicitly -- and a silently skipped tally would leave that
    document under-reporting with nothing to show it had.
    """
    recorder = getattr(tally, "record", None)
    if not callable(recorder):
        raise JoernAdapterError(
            f"tally must expose a callable record(tool, result) -- normally a "
            f"severity.LiteralTally; observed {_type_name(tally)}"
        )
    return tally


def _validated_document(doc: Any) -> Mapping[str, Any]:
    """Return ``doc`` where it is the envelope this adapter can walk, else raise.

    One requirement: an object top level.  ``findings`` is deliberately **not**
    required, because an envelope carrying none is a legitimate artifact -- a query set
    that matched nothing -- and ``reconcile._count_joern`` reads its absence as zero
    records rather than as an error, so raising here would disagree with the counting
    traversal.

    The shape is not re-detected.  ``shape.py`` owns detection as the single authority
    on it (SARIF is ``version == "2.1.0"`` together with a ``runs`` array; everything
    else written by a known runner is native), and a second copy of that test here could
    disagree with the first, which is worse than not testing it twice.  AAP 0.5.4 makes
    an artifact matching no known shape a halt, and ``shape.py`` is where that halt
    lives.

    Raising rather than returning zero rows for a non-object is the point: an empty
    result set is indistinguishable from a clean scan, which is the failure mode the
    mandated shape-routing negative test exists to prevent.
    """
    document = _json_object(doc)
    if document is None:
        raise JoernAdapterError(
            f"the joern artifact's top level is an object carrying a {FINDINGS_KEY} "
            f"array; observed {_type_name(doc)}. Parsing and shape detection happen "
            "upstream -- shape.py routes an artifact here by its filename key, and "
            "cli.py hands over the already-parsed document"
        )
    return document


def _validated_source_index(source_index: Any, root: str) -> tuple[paths.SourceIndex, int]:
    """Return the source index to resolve against, and whether the caller supplied it.

    ``None`` builds one over ``root`` with :func:`normalize.paths.build_source_index`,
    which walks ``src/main`` **and** ``src/test`` and keys both the filename and the
    declaration schemes.  That is the only filesystem contact this adapter's work
    requires, it happens inside ``paths.py``, and it is driven entirely by the ``root``
    argument -- so nothing here reads a path this module chose for itself.

    A caller may inject an index instead: ``cli.py`` to build it once and reuse it
    across a run, and an adapter test to supply a synthetic one through
    :meth:`normalize.paths.SourceIndex.from_mapping`, which is what makes the ambiguous,
    declaration-only and ``src/test`` cases assertable with no live tree.  There is no
    module-level cache, because a cache keyed on a path is hidden state a test cannot
    reset.
    """
    if source_index is None:
        return paths.build_source_index(root), 0
    if not isinstance(source_index, paths.SourceIndex):
        raise JoernAdapterError(
            f"source_index must be a paths.SourceIndex -- built by "
            f"paths.build_source_index(root) or paths.SourceIndex.from_mapping(...) -- "
            f"or None to have one built over the root; observed "
            f"{_type_name(source_index)}"
        )
    return source_index, 1


# --------------------------------------------------------------------------- #
# One finding -> exactly one outcome
# --------------------------------------------------------------------------- #


def _adapt_finding(
    finding: Any,
    *,
    root: str,
    tool_base: paths.ToolPathBase,
    index: paths.SourceIndex,
    coordinate_fields: tuple[str, ...],
    globs: tuple[str, ...],
    tally: Any,
    finding_index: int,
    counters: dict[str, int],
) -> dict[str, Any] | paths.Rejection:
    """Return one row **or** one rejection for one ``findings[]`` element.

    Exactly one of the two, always.  The single return value is what makes the
    one-to-one property structural: there is no path through this function that emits
    both and none that emits neither, so
    ``dataset rows + rejected records == the records walked`` holds by construction
    rather than by an assertion that could be forgotten.

    The classification order is fixed and documented rather than incidental, because a
    record can be defective in more than one way at once and the order decides which
    class it is counted under:

    1. the finding is not an object -> ``malformed_record``;
    2. no rule identifier -> ``missing_rule_id``;
    3. no message -> ``missing_message`` (a non-string ``message`` ->
       ``malformed_record``);
    4. the path -> ``absent_path``, ``ambiguous_source_resolution``,
       ``unresolvable_path`` or ``malformed_record``, as ``paths.py`` classifies it;
    5. a ``start_line`` present that is not a usable line number ->
       ``non_integer_start_line``.

    Severity and ``in_scope`` never reject: each has a defined value for every input, so
    a record reaching step 5 becomes a row.

    Nothing is caught broadly here.  Each lookup and conversion is guarded where it
    happens, so a genuine programming error propagates instead of being converted into a
    rejection count that would satisfy reconciliation while hiding a defect.
    """
    finding_object = _json_object(finding)
    if finding_object is None:
        return paths.make_rejection(
            paths.REJECT_MALFORMED_RECORD,
            TOOL,
            f"the {FINDINGS_KEY} element is a {_type_name(finding)}, not an object, so "
            "no finding can be read from it",
            finding_index=finding_index,
        )

    # The record's identifying fields, carried into every rejection this finding can
    # earn.  ``method``/``method_full_name``, ``callee`` and the metadata's ignored
    # ``file`` member are context that makes a rejection diagnosable; none of them is a
    # dataset field, and ``file`` is never read as a path.
    identity: dict[str, Any] = {"finding_index": finding_index}
    for name in CONTEXT_FIELDS:
        if name in finding_object:
            identity[name] = finding_object[name]

    # Step 2 -- the rule identifier.
    rule_read, rule_failure = _rule_id(finding_object)
    if rule_failure is not None:
        reject_class, detail = rule_failure
        return paths.make_rejection(reject_class, TOOL, detail, **identity)
    rule_id = rule_read.value
    if rule_read.field is not None:
        counters[f"{COUNTER_RULE_ID_FIELD_PREFIX}{rule_read.field}"] += 1
    identity["rule_id"] = rule_id

    # Step 3 -- the message.
    message, message_failure = _message(finding_object)
    if message_failure is not None:
        reject_class, detail = message_failure
        return paths.make_rejection(reject_class, TOOL, detail, **identity)

    # The multi-location count is a property of the record, so it is taken whatever the
    # record's outcome turns out to be (AAP 0.5.4: the row takes the first location, the
    # record still counts once, and the number is reported per tool).  This shape names
    # one call site per finding, so the count stays at zero unless a future collector
    # emits several -- published so that a shape carrying more is visible rather than
    # silently reduced to its first.
    locations = finding_object.get("locations")
    if _is_json_array(locations) and len(locations) > 1:
        counters[COUNTER_MULTI_LOCATION] += 1

    if _collector_explanation(finding_object) is not None:
        counters[COUNTER_COLLECTOR_EXPLANATION_PRESENT] += 1

    # Step 4 -- the path.  Delegated in full; see _resolve_path for the precedence and
    # for why an ambiguity is never broken by the collector's own answer.
    outcome = _resolve_path(
        finding_object,
        tool_base=tool_base,
        index=index,
        root=root,
        coordinate_fields=coordinate_fields,
        record_identity=identity,
    )
    for key in outcome.counters_to_bump:
        counters[key] += 1
    if outcome.rejection is not None:
        # Returned as ``paths.py`` built it: the class is already named and the
        # sub-reason already written -- which candidates made a resolution ambiguous, or
        # that no source file in either tree declares the class, with the collector's own
        # explanation appended.  Rewording it here would lose that.
        return outcome.rejection
    resolved = outcome.resolved
    if resolved is None:
        # Unreachable while _PathOutcome carries exactly one of the two, and raised
        # rather than rejected if it ever stops holding: a broken internal invariant is a
        # defect in this module, not a defect in the artifact.
        raise JoernAdapterError(
            "internal invariant: the path resolution returned neither a resolved path "
            "nor a rejection"
        )

    # Step 5 -- start_line.
    start_line, start_line_field, start_line_failure = _start_line(finding_object)
    if start_line_failure is not None:
        reject_class, detail = start_line_failure
        return paths.make_rejection(reject_class, TOOL, detail, **identity)
    if start_line is None:
        counters[COUNTER_START_LINE_ABSENT] += 1
    elif start_line_field is not None:
        counters[f"{COUNTER_START_LINE_FIELD_PREFIX}{start_line_field}"] += 1

    # From here nothing can reject: this record is a row.
    severity_result, severity_counter = _severity_of(finding_object)
    counters[severity_counter] += 1
    counters[f"{COUNTER_SEVERITY_BASIS_PREFIX}{severity_result.basis}"] += 1
    # The tally is fed once per emitted row, which is what makes severity-map.md's
    # per-literal counts the row counts it reports them as.  A rejected record
    # contributes no row, so counting one here would put a literal in that document
    # against rows the dataset does not contain.  AAP 0.6.2 has that document name
    # joern's severity posture explicitly, and this is the only channel that reaches it.
    tally.record(TOOL, severity_result)

    counters[f"{COUNTER_PATH_KIND_PREFIX}{resolved.kind}"] += 1
    if resolved.is_non_filesystem_coordinate:
        counters[COUNTER_NON_FILESYSTEM_PATHS] += 1
    if paths.contains_src_test(resolved.path):
        # Counted, and emitted: AAP 0.5.4 requires a test-JAR finding *kept* with
        # in_scope false rather than dropped, and this counter is the only place the
        # number is visible -- a dropped row and a retained one look identical in a row
        # count.
        counters[COUNTER_ROWS_FROM_SRC_TEST] += 1

    # in_scope is decided by the allowlist alone, through paths.py's matcher, on the
    # resolved path and carrying its kind -- so an archive member cannot match a glob on
    # its segments and the literal src/test exclusion is applied once, where it lives.
    # Nothing is ever filtered on it: a row outside the allowlist is kept with in_scope
    # false and counted (AAP 0.9.3).
    in_scope = bool(resolved.in_scope(globs))
    counters[COUNTER_ROWS_IN_SCOPE if in_scope else COUNTER_ROWS_OUT_OF_SCOPE] += 1

    row: dict[str, Any] = {
        "tool": TOOL,
        "scanner_class": SCANNER_CLASS,
        "rule_id": rule_id,
        "message": message,
        "severity_native": severity_result.severity_native,
        "severity_norm": severity_result.severity_norm,
        "path": resolved.path,
        "start_line": start_line,
        "cwe": _CWE,
        "cve": _CVE,
        "package_coordinate": _PACKAGE_COORDINATE,
        "in_scope": in_scope,
    }
    return row


def _record_envelope(document: Mapping[str, Any], counters: dict[str, int]) -> None:
    """Record the envelope's integer metadata into ``counters``.

    The envelope fields outside ``findings`` are **not findings and produce no rows**.
    They are surfaced here under the ``envelope_`` prefix because
    ``oss-scan-results/tool-status.md`` reports what the runner recorded about the graph
    it queried, and a number that reaches no channel reaches no document.

    Both shapes are read: this provisioning's nested ``graph`` object and the documented
    collector's flat ``cpg_methods``/``cpg_typedecls``/index-size members.  A non-integer
    or negative value is skipped rather than coerced -- envelope metadata is evidence,
    and a coerced figure is worse than an absent one.

    ``queries`` contributes exactly two facts: how many queries the envelope declares,
    and how many reported reaching their traversal bound.  ``queries[].returned`` and
    ``queries[].count`` are deliberately **not** summed: they are the collector's own
    per-query tallies, AAP 0.5.4 makes ``findings[]`` the reconciliation unit, and
    publishing a plausible substitute for the independent count is how the requirement
    for one would quietly be lost.
    """
    for member, suffix in _ENVELOPE_INT_FIELDS.items():
        value = _non_negative_int(document.get(member))
        if value is not None:
            counters[f"{COUNTER_ENVELOPE_PREFIX}{suffix}"] = value
    graph = _json_object(document.get(_ENVELOPE_GRAPH_KEY))
    if graph is not None:
        for member, suffix in _ENVELOPE_GRAPH_INT_FIELDS.items():
            value = _non_negative_int(graph.get(member))
            if value is not None:
                counters[f"{COUNTER_ENVELOPE_PREFIX}{suffix}"] = value

    queries = _json_array(document.get(_QUERIES_KEY))
    counters[COUNTER_ENVELOPE_QUERIES_DECLARED] = len(queries)
    bound_reached = 0
    for entry in queries:
        query = _json_object(entry)
        # ``is True`` rather than truthiness: the flag is a JSON boolean, and a string
        # "false" is truthy in Python.  A malformed flag counts as not reached and stays
        # visible in the declared count beside it.
        if query is not None and query.get(_QUERY_BOUND_REACHED_KEY) is True:
            bound_reached += 1
    counters[COUNTER_ENVELOPE_QUERIES_BOUND_REACHED] = bound_reached


def _record_index_shape(
    index: paths.SourceIndex,
    supplied: int,
    counters: dict[str, int],
) -> None:
    """Record the source index's own shape into ``counters``.

    A rejection count is only interpretable beside the index that produced it: 585
    unresolvable findings against an index of 6,759 files means something different from
    585 against an index of twelve.  The figures come from
    :meth:`normalize.paths.SourceIndex.statistics` rather than being recomputed here, so
    the two cannot drift.
    """
    statistics = index.statistics()
    counters[COUNTER_SOURCE_INDEX_SUPPLIED] = supplied
    counters[COUNTER_SOURCE_INDEX_FILES] = int(statistics["files_indexed"])
    counters[COUNTER_SOURCE_INDEX_FILENAME_KEYS] = int(statistics["by_filename_keys"])
    counters[COUNTER_SOURCE_INDEX_DECLARATION_KEYS] = int(statistics["by_decl_keys"])
    counters[COUNTER_SOURCE_INDEX_AMBIGUOUS_FILENAME] = int(
        statistics["ambiguous_by_filename"]
    )
    counters[COUNTER_SOURCE_INDEX_AMBIGUOUS_DECLARATION] = int(
        statistics["ambiguous_by_decl"]
    )
    counters[COUNTER_SOURCE_INDEX_DECLARATIONS_READ] = int(
        bool(statistics["declarations_read"])
    )


# --------------------------------------------------------------------------- #
# The public entry point
# --------------------------------------------------------------------------- #


def adapt(
    doc: Any,
    *,
    tool: str = TOOL,
    root: Any,
    tool_base: paths.ToolPathBase,
    allowlist: Iterable[str],
    tally: Any,
    source_index: paths.SourceIndex | None = None,
) -> tuple[list[dict[str, Any]], list[paths.Rejection], dict[str, int]]:
    """Turn one ``joern.json`` artifact into dataset rows, rejections and counters.

    This is the uniform adapter entry point: every adapter module in this package
    exposes ``adapt`` with this shape, so ``cli.py``'s registry resolves it with
    ``getattr(module, "adapt")`` and every adapter test calls it directly.

    Args:
        doc: The **already-parsed** artifact document -- a mapping for this shape.
            Parsing and shape detection happen upstream, which is what lets a test
            exercise every behaviour on a fixture.
        tool: The canonical tool identifier. Defaults to :data:`TOOL` and is checked
            against it rather than trusted, so a mis-routed artifact raises instead of
            being attributed to ``joern``.
        root: The ``SPARK_SRC`` root, as a :class:`pathlib.Path` or a string. Must be
            absolute. It anchors the source index the class-to-source resolution needs.
        tool_base: This tool's :class:`normalize.paths.ToolPathBase`, the per-tool view
            over ``harness/artifacts/logs/runner-metadata.json``. Its ``kind`` must be
            ``bytecode_class``; it supplies the coordinate member to read and the
            ephemeral member to ignore, and neither is assumed.
        allowlist: The twelve authoritative globs, as loaded by
            :func:`normalize.paths.load_allowlist`. Consumed once into a tuple.
        tally: A :class:`normalize.severity.LiteralTally` (or anything exposing
            ``record(tool, result)``), fed once per emitted row so
            ``oss-scan-results/severity-map.md`` can list every observed literal with
            the rows it affected.
        source_index: An optional prebuilt :class:`normalize.paths.SourceIndex` over
            ``src/main`` and ``src/test``. ``None`` builds one over ``root`` with
            :func:`normalize.paths.build_source_index`. Injecting one lets ``cli.py``
            build it once for a run and lets an adapter test supply a synthetic index
            through :meth:`normalize.paths.SourceIndex.from_mapping`, so the ambiguous,
            declaration-only and ``src/test`` cases are assertable with no live tree.
            The six preceding arguments are the ones every sibling adapter takes, so a
            caller that omits this keyword calls this adapter exactly as it calls them.

    Returns:
        A three-tuple ``(rows, rejections, counters)``:

        * ``rows`` -- a list of dicts, each carrying exactly the twelve fields of
          :data:`FIELDS` in that order, in document order;
        * ``rejections`` -- a list of :class:`normalize.paths.Rejection`, each under a
          named member of :data:`normalize.paths.REJECT_CLASSES` with its sub-reason
          retained verbatim, together with the collector's own ``path_resolution``
          explanation, the class coordinate, and -- for an ambiguity -- every competing
          candidate;
        * ``counters`` -- a dict of ints over :data:`COUNTER_KEYS`.

        ``len(rows) + len(rejections)`` equals the number of ``findings[]`` elements
        walked, which is the same count unit
        :func:`normalize.reconcile.count_records` arrives at independently. The envelope
        members and ``queries[].returned`` contribute nothing to it.

    Raises:
        JoernAdapterError: If an argument is not what the contract requires -- the wrong
            tool identifier, a relative or non-text root, another tool's path base, a
            path base whose kind is not ``bytecode_class``, a non-iterable allowlist, a
            tally that cannot record, a source index that is not one, or a document that
            is not an object. A caller fault is raised rather than absorbed into a
            rejection count.
        normalize.severity.SeverityPolicyError: If ``tally`` is a ``LiteralTally`` whose
            vocabulary does not carry ``joern`` -- which the canonical vocabulary does,
            and which is left to surface rather than be caught.

    A tool's exit code is never consulted: a valid artifact is normalized whatever its
    runner returned, since artifact status and exit status are independent (AAP 0.5.4).
    """
    canonical_tool = _validated_tool(tool)
    root_text = _validated_root(root)
    base = _validated_tool_base(tool_base, canonical_tool)
    globs = _validated_allowlist(allowlist)
    recorder = _validated_tally(tally)
    document = _validated_document(doc)
    index, index_supplied = _validated_source_index(source_index, root_text)
    coordinate_fields = _coordinate_field(base)

    rows: list[dict[str, Any]] = []
    rejections: list[paths.Rejection] = []
    counters = new_counters()

    _record_envelope(document, counters)
    _record_index_shape(index, index_supplied, counters)

    # findings[] in document order: one finding is one record, and one record yields
    # exactly one row or one rejection.  An absent, empty or non-array findings value
    # contributes nothing and is not an error, which is how reconcile._count_joern reads
    # it too -- so the two agree on zero for an artifact that carries no findings.
    for finding_index, raw_finding in enumerate(_json_array(document.get(FINDINGS_KEY))):
        outcome = _adapt_finding(
            raw_finding,
            root=root_text,
            tool_base=base,
            index=index,
            coordinate_fields=coordinate_fields,
            globs=globs,
            tally=recorder,
            finding_index=finding_index,
            counters=counters,
        )
        if isinstance(outcome, paths.Rejection):
            rejections.append(outcome)
        else:
            rows.append(outcome)

    return rows, rejections, counters
