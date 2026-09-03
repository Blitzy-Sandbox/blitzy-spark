"""Artifact shape detection and adapter routing for the OSS scan normalizer.

Purpose (AAP 0.6.1, verbatim): *"Detects SARIF by version ``"2.1.0"`` together with a
runs array; halts on an artifact matching neither SARIF nor a known native shape."*

This module is the first thing the normalizer does with a raw artifact and the only
place the run halts on an unrecognised artifact. It answers three questions and
nothing else -- what shape a document is, which adapter reads it, and what
``scanner_class`` its rows carry. It never parses findings, never resolves a path and
never interprets one tool's output against another's (AAP 0.3.2 forbids cross-tool
interpretation of any kind).

The detection test -- exactly two conditions
--------------------------------------------
A document is SARIF when it carries ``version == "2.1.0"`` **and** a ``runs`` array.
AAP 0.5.4: *"those two together are the test."* Neither condition alone is
sufficient and neither is relaxed: ``"2.1"`` is not ``"2.1.0"``, a ``runs`` mapping is
not a ``runs`` array, and ``$schema``, a ``.sarif`` extension or a ``tool.driver`` key
are *not* substitutes -- a filename extension is a naming convention, not a shape
test. A permissive detector is the specific failure this strictness prevents: a
native artifact accepted as SARIF yields an *empty* result set rather than an error,
and an empty result set is indistinguishable from a clean scan (AAP 0.5.4).

Routing is keyed by the writing runner, never by content
-------------------------------------------------------
A SARIF-detected artifact routes to the one shared SARIF adapter. Anything else
routes to *"the native adapter keyed by the runner that wrote it"* (AAP 0.5.4): the
artifact's filename identifies the writer, and a native document is never
fingerprinted to guess which tool produced it. Nine tools route to seven adapter
module keys -- six unconditional, plus ``osv_scanner``, whose module exists only if
OSV-Scanner writes an artifact:

    writing runner / artifact stem                    -> adapter module key
    opengrep, semgrep, datadog-static-analyzer        -> sarif
    trivy                                            -> trivy
    gitleaks                                         -> gitleaks
    checkov                                          -> checkov
    dependency-check                                 -> dependency_check
    joern                                            -> joern
    osv-scanner                                      -> osv_scanner

Canonical **tool** identifiers are hyphenated (AAP 0.5.4 -- produced mechanically
from the stem of the runner and its artifact, so no "OWASP Dependency-Check" and no
"Semgrep CE"); adapter **module** keys are underscored Python identifiers. There is
deliberately no ``opengrep``, ``semgrep`` or ``datadog_static_analyzer`` adapter.

``scanner_class`` -- fixed per tool, with Trivy the single exception
-------------------------------------------------------------------
``sast`` for ``opengrep``/``semgrep``/``datadog-static-analyzer``/``joern``,
``secret`` for ``gitleaks``, ``misconfig`` for ``checkov``, ``vuln`` for
``osv-scanner``/``dependency-check``. Trivy's class is decided *per record, from the
section array the record was read from, never from record content*, so this module
hands the Trivy adapter the :data:`PER_RECORD` sentinel rather than a plausible
default: the sentinel is not a string, so a caller that forgets to resolve it fails
loudly (``json.dumps`` raises, ``str()`` raises) instead of emitting a wrong class
into a dataset row. :func:`scanner_class_for_trivy_section` performs that
resolution. This is the same class table AAP 0.8.1 has the gate check every
``harness/bin/`` entry against, exposed as data so the gate record and the validation
criteria consume one authored table rather than a second copy.

The halt this module owns
------------------------
An artifact matching neither SARIF nor a known native shape *"is a halt rather than a
best-effort parse"* (AAP 0.5.4) and is listed among the conditions that stop the run
(AAP 0.9.2). :func:`route` raises :class:`UnknownArtifactShape` carrying the artifact
path, the observed top-level type, the observed ``version`` value and the top-level
keys -- enough for the halt report to quote the observed structure. It never returns
``None`` and never falls back to an adapter.

Two boundaries keep that halt honest:

* A **known** artifact name is not on its own a licence to route. The name says who
  wrote the artifact; it is no evidence that the bytes are that writer's shape. So a
  document that is not SARIF must carry the envelope AAP 0.5.4's per-shape table
  names for its tool -- ``Results`` for Trivy, a top-level array for Gitleaks, a
  ``results`` object (or a non-empty array of report objects) for Checkov,
  ``dependencies`` for Dependency-Check, ``results`` for OSV-Scanner, ``findings``
  for Joern -- and an artifact written by one of the three SARIF producers must
  satisfy the SARIF conjunction, since those runners write SARIF and nothing else. A
  ``trivy.json`` carrying no ``Results`` key, and an ``opengrep.sarif`` carrying
  ``version`` ``"2.1.0-rtm.5"``, both halt here rather than reaching a walker that
  would find none of its containers and report zero rows. What is checked is the
  envelope and only the envelope: an **empty** report from any tool routes normally,
  and every judgement about record structure inside the container stays the adapter's
  -- including the Trivy non-empty-unsupported-section halt. This module states which
  shape an artifact is; it does not pre-empt an adapter's validation of the records
  in it.
* An **unknown** artifact name always halts, *even when the document is valid SARIF*.
  ``harness/artifacts/raw/`` is runner-only -- one artifact per tool that writes one
  and nothing else ever (AAP 0.6.1). The Opengrep taint A/B arms are valid SARIF and
  live in ``harness/artifacts/logs/taint-ab-{on,off}.sarif``; folding one in would
  corrupt both Opengrep's count and the dataset total (AAP 0.1.3).

``joern.json`` is this harness's own shape
------------------------------------------
It is not a Joern output format and there is no Joern spec to look up. The runner
bakes ``harness/lib/joern-scan.sc``, which writes
``{tool, tool_version, cpg, graph{...}, query_set, queries[], findings[]}``. It is a
mapping with no ``version``/``runs`` pair, so it correctly detects as native.

Inventory completeness
----------------------
``findings.json``/``findings.csv`` are row-only, so a tool that produced no row is
invisible in them by construction; ``tool-status.md`` and ``severity-map.md`` are the
authoritative inventory of all nine (AAP 0.5.4). The nine-identifier inventory here
therefore stays complete even for a tool whose artifact never appears -- OSV-Scanner
is expected to write none (exit 128, "No package sources found"), and its adapter
entry is kept anyway so a present-but-unmapped artifact cannot trip the halt above.

Import constraints
------------------
A leaf. Nothing is imported from the ``normalize`` package and no adapter module is
imported: routing names an adapter by **string key** and ``cli.py`` resolves the key
to a callable. That keeps the import graph acyclic and lets
``oss-scan-results/adapter-tests/test_shape_routing_negative.py`` exercise routing
without importing any adapter module. Standard library only -- no third-party import,
no manifest, no lockfile, no install step (AAP 0.4.1).

There is deliberately no ``__init__.py`` under ``harness/lib/normalize/``: PEP 420
namespace packages make ``import normalize.shape`` work once ``harness/lib`` is on
``sys.path``.

No user-specified rule governs this file, so enterprise-standard best practice applies
in its place (AAP 0.7, 0.10.2), held to the bar the AAP sets for this pipeline:
verification independent of the thing verified, reject rather than infer, and a policy
fixed before any output is observed.
"""

from __future__ import annotations

# Standard library only, and only these seven modules:
#   hashlib         -- sha256, so a bounded excerpt is still identified by its whole value;
#   re              -- the one anchored userinfo pattern, compiled once at import;
#   collections.abc -- Mapping, so the detector accepts any parsed-JSON mapping;
#   dataclasses     -- the frozen RoutingDecision;
#   pathlib         -- PurePath, to take an artifact's filename without importing os;
#   types           -- MappingProxyType, so the authored tables cannot be mutated by a
#                      consumer (these are closed sets; a mutable dict invites drift);
#   typing          -- Final, which the five bounded-guard constants below are annotated
#                      with. ``from __future__ import annotations`` defers evaluating an
#                      annotation, it does not supply the name, so Final is imported like
#                      anything else the module annotates with -- the convention paths.py,
#                      the six adapters and every helper under harness/lib already follow.
import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import PurePath
from types import MappingProxyType
from typing import Final

__all__ = [
    # SARIF detection
    "SARIF_VERSION",
    "SARIF_VERSION_KEY",
    "SARIF_RUNS_KEY",
    "is_sarif",
    "describe_document",
    "detection_evidence",
    "safe_text",
    "safe_value",
    "safe_scalar",
    "safe_keys",
    "SHAPE_VALUE_LIMIT",
    "SHAPE_KEYS_REPORTED_LIMIT",
    "USERINFO_REDACTION",
    "REDACTED_TEXT",
    "WELL_KNOWN_DOCUMENT_KEYS",
    "publishable_literals",
    "is_publishable_literal",
    # shapes
    "SHAPE_SARIF",
    "SHAPE_NATIVE",
    "SHAPES",
    # scanner classes
    "SCANNER_CLASS_SAST",
    "SCANNER_CLASS_SECRET",
    "SCANNER_CLASS_MISCONFIG",
    "SCANNER_CLASS_VULN",
    "SCANNER_CLASSES",
    "PER_RECORD",
    "PER_RECORD_LABEL",
    "is_per_record",
    # authored tables
    "CANONICAL_TOOLS",
    "ARTIFACT_FILENAME_BY_TOOL",
    "TOOL_BY_ARTIFACT_FILENAME",
    "ARTIFACT_FILENAMES",
    "SARIF_PRODUCERS",
    "ADAPTER_PACKAGE",
    "SHARED_SARIF_ADAPTER",
    "ADAPTER_MODULE_BY_TOOL",
    "ADAPTER_MODULES",
    "SCANNER_CLASS_BY_TOOL",
    "TOOLS_BY_SCANNER_CLASS",
    "TRIVY_SECTION_SCANNER_CLASS",
    "TRIVY_SCANNER_CLASSES",
    "TRIVY_UNSUPPORTED_FINDING_SECTIONS",
    # native envelope validation
    "TRIVY_RESULTS_KEY",
    "CHECKOV_RESULTS_KEY",
    "DEPENDENCY_CHECK_DEPENDENCIES_KEY",
    "OSV_SCANNER_RESULTS_KEY",
    "JOERN_FINDINGS_KEY",
    "matches_native_shape",
    "native_shape_requirement",
    # lookups
    "resolve_tool",
    "is_known_tool",
    "artifact_filename_for",
    "adapter_module_for",
    "scanner_class_for",
    "scanner_class_for_trivy_section",
    # routing and the halt
    "RoutingDecision",
    "route",
    "route_artifact",
    "UnknownArtifactShape",
    "REASON_UNRECOGNIZED_ARTIFACT_NAME",
    "REASON_UNSUPPORTED_DOCUMENT_TYPE",
    "REASON_SARIF_PRODUCER_NOT_SARIF",
    "REASON_NATIVE_SHAPE_UNRECOGNIZED",
    "UNKNOWN_SHAPE_REASONS",
    # native document signatures (the signature layer)
    "JSON_TYPE_OBJECT",
    "JSON_TYPE_ARRAY",
    "NativeSignature",
    "NATIVE_SIGNATURES",
    "NATIVE_SIGNATURE_TOOLS",
    "TOOLS_WITHOUT_A_NATIVE_SIGNATURE",
    "native_signature_for",
    "matches_native_signature",
    "native_signature_evidence",
    "REASON_NATIVE_SIGNATURE_MISMATCH",
    "HALT_REASONS",
]

# --------------------------------------------------------------------------------------
# SARIF detection constants (AAP 0.5.4: version "2.1.0" together with a runs array)
# --------------------------------------------------------------------------------------

#: The only SARIF version this dataset accepts. An exact string compare -- not a
#: prefix, not a parsed version -- because "2.1" and "2.1.0-rc" are different shapes
#: whose result objects this normalizer has not been validated against.
SARIF_VERSION = "2.1.0"

#: Top-level key carrying the SARIF version.
SARIF_VERSION_KEY = "version"

#: Top-level key carrying the SARIF runs array.
SARIF_RUNS_KEY = "runs"

# --------------------------------------------------------------------------------------
# Detected shapes
# --------------------------------------------------------------------------------------

#: A document that satisfies both SARIF conditions.
SHAPE_SARIF = "sarif"

#: Anything else written by a known runner -- each tool's own native shape, plus
#: ``joern.json``, which is this harness's own shape rather than a tool format.
SHAPE_NATIVE = "native"

#: The closed set of detected shapes.
SHAPES = (SHAPE_SARIF, SHAPE_NATIVE)

# --------------------------------------------------------------------------------------
# scanner_class vocabulary and the Trivy per-record sentinel (AAP 0.5.4)
# --------------------------------------------------------------------------------------

SCANNER_CLASS_SAST = "sast"
SCANNER_CLASS_SECRET = "secret"
SCANNER_CLASS_MISCONFIG = "misconfig"
SCANNER_CLASS_VULN = "vuln"

#: The closed set of ``scanner_class`` literals a dataset row may carry.
SCANNER_CLASSES = (
    SCANNER_CLASS_SAST,
    SCANNER_CLASS_SECRET,
    SCANNER_CLASS_MISCONFIG,
    SCANNER_CLASS_VULN,
)

#: Label used *only* when serialising a routing decision into a log record
#: (``harness/artifacts/logs/normalize-run.json``). It is never a dataset value: a row's
#: ``scanner_class`` must be one of :data:`SCANNER_CLASSES`.
PER_RECORD_LABEL = "per-record"


class _PerRecordScannerClass:
    """Sentinel standing in for a ``scanner_class`` that is decided per record.

    Trivy is the single tool whose class varies row by row, taken from the section
    array the record was read from and *never* from record content (AAP 0.5.4). This
    sentinel is deliberately hostile to being mistaken for a class literal:

    * it is not a ``str``, so ``json.dumps`` raises ``TypeError`` on it;
    * ``str()`` and f-string formatting raise ``TypeError`` with a message naming the
      resolver to call, so a row built from it fails loudly rather than carrying a
      plausible-looking wrong class into ``findings.csv``;
    * ``repr()`` stays safe (``PER_RECORD``) so diagnostics, tracebacks and the
      ``RoutingDecision`` repr remain usable.

    It is a singleton, so ``is`` comparisons hold across copies and round trips.
    """

    __slots__ = ()

    _instance: _PerRecordScannerClass | None = None

    def __new__(cls) -> _PerRecordScannerClass:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "PER_RECORD"

    def __str__(self) -> str:
        raise TypeError(
            "trivy's scanner_class is decided per record from the section array it was "
            "read from; resolve it with scanner_class_for_trivy_section(section) -- the "
            "PER_RECORD sentinel must never be written into a dataset row"
        )

    def __format__(self, format_spec: str) -> str:
        # Delegates to __str__, so f"{PER_RECORD}" fails as loudly as str(PER_RECORD).
        return self.__str__()

    def __bool__(self) -> bool:
        # Truthy: "there is a class, it just is not decided yet" -- a falsy sentinel
        # would silently pass an `if scanner_class:` guard as "no class at all".
        return True

    def __copy__(self) -> _PerRecordScannerClass:
        return self

    def __deepcopy__(self, memo: dict[int, object]) -> _PerRecordScannerClass:
        return self

    def __reduce__(self) -> tuple[type[_PerRecordScannerClass], tuple[()]]:
        # Pickling round-trips through __new__, preserving the singleton identity.
        return (self.__class__, ())


#: Sentinel for Trivy's per-record ``scanner_class``. See
#: :class:`_PerRecordScannerClass` and :func:`scanner_class_for_trivy_section`.
PER_RECORD = _PerRecordScannerClass()


def is_per_record(value: object) -> bool:
    """Return ``True`` when *value* is the :data:`PER_RECORD` sentinel.

    Callers use this to branch before building a row, which is the only safe way to
    consume Trivy's class: any attempt to stringify the sentinel raises.
    """
    return value is PER_RECORD


# --------------------------------------------------------------------------------------
# The nine canonical tool identifiers (AAP 0.5.4)
# --------------------------------------------------------------------------------------
# Produced mechanically from the stem of the runner and its artifact -- no "OWASP
# Dependency-Check", no "Semgrep CE". Hyphenated, case-sensitive, and a closed set: the
# gate requires exactly these nine runners in harness/bin/ and halts on a runner naming
# a scanner absent from the class table below (AAP 0.8.1).
CANONICAL_TOOLS = (
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

_CANONICAL_TOOL_SET = frozenset(CANONICAL_TOOLS)

# --------------------------------------------------------------------------------------
# The nine fixed artifact filenames
# --------------------------------------------------------------------------------------
# Verified character-for-character against the ART= line of every provisioned runner
# (harness/bin/run-<tool>.sh) and corroborated by AAP 0.2.3: SARIF 2.1.0 for the three
# SAST producers that emit it, each tool's native shape for the other six. The
# extension split is part of the contract -- ".sarif" for exactly three tools, ".json"
# for exactly six -- so "trivy.sarif" and "opengrep.json" are unrecognised names.
ARTIFACT_FILENAME_BY_TOOL = MappingProxyType(
    {
        "opengrep": "opengrep.sarif",
        "semgrep": "semgrep.sarif",
        "datadog-static-analyzer": "datadog-static-analyzer.sarif",
        "gitleaks": "gitleaks.json",
        "checkov": "checkov.json",
        "trivy": "trivy.json",
        "osv-scanner": "osv-scanner.json",
        "dependency-check": "dependency-check.json",
        "joern": "joern.json",
    }
)

#: Inverse of :data:`ARTIFACT_FILENAME_BY_TOOL`. The filename identifies the writing
#: runner, which is what routing keys on.
TOOL_BY_ARTIFACT_FILENAME = MappingProxyType(
    {filename: tool for tool, filename in ARTIFACT_FILENAME_BY_TOOL.items()}
)

#: The nine artifact filenames in canonical tool order.
ARTIFACT_FILENAMES = tuple(ARTIFACT_FILENAME_BY_TOOL[tool] for tool in CANONICAL_TOOLS)

#: The three tools whose runners write SARIF 2.1.0. Detection is still performed on the
#: document -- this set is provenance, never a substitute for :func:`is_sarif`.
SARIF_PRODUCERS = frozenset({"opengrep", "semgrep", "datadog-static-analyzer"})

# --------------------------------------------------------------------------------------
# Adapter routing table: nine tools, seven adapter module keys (one conditional)
# --------------------------------------------------------------------------------------

#: Package holding the adapter modules. A string only -- this module imports no adapter.
ADAPTER_PACKAGE = "normalize.adapters"

#: The one shared adapter every SARIF producer routes to.
SHARED_SARIF_ADAPTER = "sarif"

# Not an identity function, and the naming split is deliberate: canonical tool
# identifiers are hyphenated, adapter module keys are underscored Python identifiers
# ("dependency_check", "osv_scanner"). The osv-scanner entry is present even though
# adapters/osv_scanner.py is created "if and only if OSV-Scanner writes an artifact"
# (AAP 0.6.1): the runner passes --output-file unconditionally, so the artifact can
# exist, and an artifact present but unmapped would fall into this module's halt path --
# stopping the run for a tool doing exactly what it was configured to do. cli.py raises
# a specific error naming the missing adapter module instead.
ADAPTER_MODULE_BY_TOOL = MappingProxyType(
    {
        "opengrep": SHARED_SARIF_ADAPTER,
        "semgrep": SHARED_SARIF_ADAPTER,
        "datadog-static-analyzer": SHARED_SARIF_ADAPTER,
        "gitleaks": "gitleaks",
        "checkov": "checkov",
        "trivy": "trivy",
        "osv-scanner": "osv_scanner",
        "dependency-check": "dependency_check",
        "joern": "joern",
    }
)

#: The distinct adapter module keys, in canonical tool order with duplicates removed.
ADAPTER_MODULES = tuple(
    dict.fromkeys(ADAPTER_MODULE_BY_TOOL[tool] for tool in CANONICAL_TOOLS)
)

# --------------------------------------------------------------------------------------
# The scanner_class table (AAP 0.5.4), fixed per tool with Trivy the single exception
# --------------------------------------------------------------------------------------
SCANNER_CLASS_BY_TOOL = MappingProxyType(
    {
        "opengrep": SCANNER_CLASS_SAST,
        "semgrep": SCANNER_CLASS_SAST,
        "datadog-static-analyzer": SCANNER_CLASS_SAST,
        "joern": SCANNER_CLASS_SAST,
        "gitleaks": SCANNER_CLASS_SECRET,
        "checkov": SCANNER_CLASS_MISCONFIG,
        "osv-scanner": SCANNER_CLASS_VULN,
        "dependency-check": SCANNER_CLASS_VULN,
        # Per record, from the section array it was read from, never from record
        # content. The sentinel forces the Trivy adapter to resolve it explicitly.
        "trivy": PER_RECORD,
    }
)


def _tools_by_scanner_class() -> MappingProxyType[str, tuple[str, ...]]:
    """Derive the class -> tools inverse from the one authored table.

    Derived rather than authored a second time, so the two cannot drift. Trivy appears
    under no fixed class: its rows land in whichever of the three the section decides.
    """
    grouped: dict[str, list[str]] = {name: [] for name in SCANNER_CLASSES}
    for tool in CANONICAL_TOOLS:
        scanner_class = SCANNER_CLASS_BY_TOOL[tool]
        if is_per_record(scanner_class):
            continue
        grouped[str(scanner_class)].append(tool)
    return MappingProxyType({name: tuple(tools) for name, tools in grouped.items()})


#: ``scanner_class`` -> the tools fixed to it, in canonical tool order. Consumed by the
#: gate record and the validation criteria so they read one authored table (AAP 0.8.1).
TOOLS_BY_SCANNER_CLASS = _tools_by_scanner_class()

# --------------------------------------------------------------------------------------
# Trivy sections -- the only place a scanner_class is decided per record
# --------------------------------------------------------------------------------------

#: Trivy's three supported finding sections mapped to the class their records take.
#: The section the record was read from decides the class; record content never does
#: (AAP 0.5.4).
TRIVY_SECTION_SCANNER_CLASS = MappingProxyType(
    {
        "Vulnerabilities": SCANNER_CLASS_VULN,
        "Secrets": SCANNER_CLASS_SECRET,
        "Misconfigurations": SCANNER_CLASS_MISCONFIG,
    }
)

#: The classes a Trivy row may carry, in section order.
TRIVY_SCANNER_CLASSES = tuple(TRIVY_SECTION_SCANNER_CLASS.values())

#: Finding sections Trivy 0.74.0 can emit that this dataset does not support. The Trivy
#: adapter validates them empty and halts on a non-empty one (AAP 0.5.4) -- named here
#: so the adapter and the validation criteria share one authored list. This module never
#: reads them: its own Trivy check is the envelope alone -- a ``Results`` key present and
#: carrying a JSON array -- and every judgement about what the sections hold stays with
#: the adapter that walks them.
TRIVY_UNSUPPORTED_FINDING_SECTIONS = ("Licenses", "ExperimentalModifiedFindings")


# --------------------------------------------------------------------------------------
# Detection
# --------------------------------------------------------------------------------------

# JSON type names, so a halt report quotes the structure in the vocabulary of the
# artifact rather than of Python. bool is checked before int on purpose (bool is an int).
_JSON_TYPE_NAMES = (
    (bool, "boolean"),
    (str, "string"),
    (int, "number"),
    (float, "number"),
    (list, "array"),
    (tuple, "array"),
)

# Keeps a halt message bounded: an artifact may carry an arbitrarily long version
# string, and a malformed document an arbitrary number of top-level keys.
_MESSAGE_VALUE_LIMIT = 120
_MESSAGE_KEY_LIMIT = 12


def is_sarif(doc: object) -> bool:
    """Return ``True`` when *doc* is a SARIF 2.1.0 document.

    The test is exactly the conjunction AAP 0.5.4 states -- ``version == "2.1.0"``
    **and** a ``runs`` array -- and nothing else is consulted: not ``$schema``, not the
    filename, not ``tool.driver``. *doc* is an already-parsed document, so this function
    performs no I/O and the mandated negative test can call it without a filesystem.

    Any non-mapping top level returns ``False`` without raising. That guard is
    load-bearing rather than defensive: ``gitleaks.json`` is a top-level array and
    Checkov's multi-framework form is an array of report objects, so calling ``.get`` on
    the document unguarded is the exact bug that would make a native artifact either
    explode here or slip past a permissive test.

    >>> is_sarif({"version": "2.1.0", "runs": []})
    True
    >>> is_sarif({"version": "2.1", "runs": []})
    False
    >>> is_sarif([{"RuleID": "generic-api-key"}])
    False
    """
    if not isinstance(doc, Mapping):
        return False
    return (
        doc.get(SARIF_VERSION_KEY) == SARIF_VERSION
        and isinstance(doc.get(SARIF_RUNS_KEY), list)
    )


def _json_type_name(value: object) -> str:
    """Name *value*'s top level in JSON's vocabulary, falling back to its Python type."""
    if value is None:
        return "null"
    if isinstance(value, Mapping):
        return "object"
    for python_type, json_name in _JSON_TYPE_NAMES:
        if isinstance(value, python_type):
            return json_name
    return type(value).__name__


# --------------------------------------------------------------------------------------
# The redaction guard this module applies to artifact-supplied values (SEC-04)
# --------------------------------------------------------------------------------------
# Every value below comes out of an artifact, which means an adversary chooses it: a
# top-level key, and the observed ``version``. Both reach durable records --
# ``harness/artifacts/logs/normalize-run.json`` on the SUCCESS path through
# :func:`detection_evidence`, and the halt section through
# :meth:`UnknownArtifactShape.details`, whose ``str()`` is also the message written
# verbatim to stderr -- so neither may be written as it arrived.
#
# A BOUNDED EXCERPT IS NOT ENOUGH, and that is the whole of SEC-04. Sanitising an
# artifact-supplied value removes what is dangerous to a *renderer* -- a control
# sequence, a credential-bearing URI authority -- and bounds how much of this
# pipeline's record the artifact gets to choose. It does not stop the value itself
# being copied into a preserved file, and a scanner artifact is not a trusted source:
# a synthetic credential placed in nothing but an invalid SARIF ``version`` came back
# whole in ``str(exc)``, in ``details()["version"]``, in
# ``details()["version_evidence"]["excerpt"]`` and in the 0644 run record. A 512-
# character prefix of a 40-character secret is the secret.
#
# So the policy here is the one ``adapters/gitleaks.py::_safe_value_repr`` already
# states for a rejection detail, applied at this module's persistence boundary:
#
#   1. NO persisted diagnostic carries artifact-supplied bytes. What is published is
#      the evidence that is METADATA rather than content -- the value's Python type
#      name, the caller's structural ``context``, the FULL character length, the FULL
#      64-hex sha256, and the counts of what sanitising would have changed. Those
#      four keep the original identifiable and measurable: two records carrying the
#      same oversized value are recognisable as the same value from the digest alone,
#      and a reader who holds the artifact can check it against the record. The text
#      itself is replaced by :data:`REDACTED_TEXT`, a fixed marker containing no
#      artifact bytes at all.
#   2. ONE carve-out, and it is the only one: text may be published verbatim where it
#      is BYTE-EQUAL to a literal this module itself authors -- see
#      :func:`publishable_literals`. Publishing a literal the code already contains
#      discloses nothing about the artifact, which is exactly why the carve-out is
#      sound; it is what lets a document whose keys are the ordinary ones
#      (``$schema``, ``version``, ``runs``) still read as itself in a halt report,
#      while ``"2.1.0-rtm.5"`` -- a value only the artifact knows -- is redacted.
#   3. ``None``, a ``bool``, an ``int`` and a ``float`` are published as themselves.
#      None of them can carry a captured secret and none can carry a control
#      character, and the actual value is what makes the diagnostic actionable.
#
# What is deliberately NOT weakened: the userinfo redaction and control escaping stay,
# because they still govern the composed prose these renderings are interpolated into,
# and their counts remain the evidence that the value carried a credential or a control
# at all. The bound stays too, as the size measurement it always was.
#
# Implemented here rather than imported because this module is a leaf that imports
# nothing from the package (AAP 0.6.4 fixes that an adapter depends only on ``paths``
# and ``severity``), the same reason ``CANONICAL_TOOLS`` is duplicated here. The two
# implementations are pinned to each other by
# ``test_shape_routing_negative.SafeRenderingParityTests``, which runs both over the
# same hostile inputs and requires identical output -- so this is one policy with two
# call sites rather than two policies free to drift.

#: The fixed marker that stands in for artifact-supplied text in a persisted
#: diagnostic. It contains no artifact bytes, it is the same string for every value,
#: and it is equal to ``paths.REDACTED_TEXT``; the parity test asserts they stay
#: equal. A reader who needs to identify what was redacted uses the ``value_type``,
#: ``character_length`` and ``sha256`` published beside it.
REDACTED_TEXT: Final[str] = "<redacted-artifact-text>"

#: The excerpt bound for one artifact-supplied value. Equal to
#: ``paths.DIAGNOSTIC_VALUE_LIMIT``; the parity test asserts they stay equal.
#:
#: With redaction in force nothing is truncated for length any more -- the marker is
#: fixed-width -- so this constant survives as the *size measurement* it always was:
#: it decides the ``truncated`` flag, which reports that the value the artifact sent
#: was longer than one this pipeline would have been willing to publish.
SHAPE_VALUE_LIMIT: Final[int] = 512

#: How many top-level keys are carried into a record. A document may legitimately
#: carry a handful; one carrying thousands is choosing this record's size, which is
#: exactly what the bound removes. The full count is reported alongside, so bounding
#: the list never hides how many there were.
SHAPE_KEYS_REPORTED_LIMIT: Final[int] = 64

#: What replaces a URI's userinfo component. Equal to ``paths.USERINFO_REDACTION``.
USERINFO_REDACTION: Final[str] = "<redacted-userinfo>"

# `scheme://userinfo@` -- the only place a URI may carry a credential (RFC 3986
# section 3.2.1). Anchored on the scheme and the authority marker, so an ordinary
# `name@domain` in prose and a `git@host:path` SSH shorthand are left alone.
_SHAPE_URI_USERINFO_RE: Final[re.Pattern[str]] = re.compile(
    r"([A-Za-z][A-Za-z0-9+.\-]*://)([^/?#\s@]+)@"
)

#: Tab and newline survive: this dataset carries messages with embedded newlines by
#: design (AAP 0.5.4). ESC -- the actual terminal injection vector -- does not.
_SHAPE_KEEP_CONTROLS: Final[frozenset[str]] = frozenset({"\t", "\n"})

#: Well-known top-level document key names, authored here explicitly rather than
#: derived, so that a document whose keys are the ordinary ones still reads as itself
#: in a halt report or a detection record. Every entry is a top-level key of one of
#: the nine artifact shapes this module routes -- SARIF's three, Trivy's envelope,
#: Checkov's report object, Dependency-Check's report, and ``joern.json``, which is
#: this harness's own shape. Several are already named constants above; they are
#: repeated here because this tuple is the *vocabulary*, and a reader checking whether
#: a published key was authored or artifact-supplied needs one list to check against.
#:
#: The tuple is deliberately small and deliberately closed. It is NOT a place to add a
#: key because some document happened to carry it: an entry here is a literal this
#: code contains, and padding it with names taken from an artifact would re-open the
#: channel the redaction closes. A key outside it is redacted, and its full length and
#: sha256 published in its place -- which is the correct outcome for a document
#: imitating nothing in the contract.
WELL_KNOWN_DOCUMENT_KEYS: Final[tuple[str, ...]] = (
    # SARIF 2.1.0 (opengrep, semgrep, datadog-static-analyzer)
    "$schema",
    "version",
    "runs",
    # Trivy's native envelope
    "SchemaVersion",
    "CreatedAt",
    "ArtifactName",
    "ArtifactType",
    "Metadata",
    "Results",
    # Checkov's report object
    "check_type",
    "results",
    "summary",
    # Dependency-Check's report
    "reportSchema",
    "scanInfo",
    "projectInfo",
    "dependencies",
    # joern.json -- written by harness/lib/joern-scan.sc, this harness's own shape
    "tool",
    "tool_version",
    "cpg",
    "graph",
    "query_set",
    "queries",
    "findings",
)

# Built once on first use rather than at import, because the vocabulary draws on
# authored tables defined further down this module (the native envelope keys and the
# JSON type names) and a module-level frozenset here could not see them. Construction
# is idempotent and the result is immutable, so the lazy cache is safe under
# concurrent readers: two threads racing to build it produce equal sets and either may
# win.
_PUBLISHABLE_LITERALS: frozenset[str] | None = None


def publishable_literals() -> frozenset[str]:
    """Return every string this module authors and may therefore publish verbatim.

    The one carve-out in the redaction policy above. A value byte-equal to one of
    these discloses nothing about the artifact that sent it -- the string is already
    in this file, so a reader learns only that the artifact chose a name this code
    knows -- while any other value is replaced by :data:`REDACTED_TEXT`.

    The membership test is exact equality, never a prefix and never a case-insensitive
    match: ``"2.1.0"`` publishes and ``"2.1.0-rtm.5"`` does not, which is precisely the
    distinction a wrong-version halt turns on. A relaxed test would let an artifact
    publish arbitrary text by prefixing it with an authored one.

    The set is assembled from this module's own authored tables so that adding a tool,
    a shape, a section or an envelope key extends the vocabulary automatically and
    cannot leave the two out of step:

    * the SARIF version and its two key names;
    * the detected shapes and the ``scanner_class`` vocabulary, with the per-record
      label a routing record uses;
    * the nine canonical tool identifiers and the nine artifact filenames;
    * the adapter package, the shared SARIF adapter key and the adapter module keys;
    * Trivy's three supported section names and its two unsupported ones;
    * the five native envelope keys and the two JSON container type names;
    * :data:`WELL_KNOWN_DOCUMENT_KEYS`.

    Returns:
        The vocabulary as an immutable set. The same object on every call, so a
        membership test costs one hash.
    """
    global _PUBLISHABLE_LITERALS
    if _PUBLISHABLE_LITERALS is None:
        _PUBLISHABLE_LITERALS = frozenset(
            (
                SARIF_VERSION,
                SARIF_VERSION_KEY,
                SARIF_RUNS_KEY,
                *SHAPES,
                *SCANNER_CLASSES,
                PER_RECORD_LABEL,
                *CANONICAL_TOOLS,
                *ARTIFACT_FILENAMES,
                ADAPTER_PACKAGE,
                SHARED_SARIF_ADAPTER,
                *ADAPTER_MODULES,
                *TRIVY_SECTION_SCANNER_CLASS,
                *TRIVY_UNSUPPORTED_FINDING_SECTIONS,
                TRIVY_RESULTS_KEY,
                CHECKOV_RESULTS_KEY,
                DEPENDENCY_CHECK_DEPENDENCIES_KEY,
                OSV_SCANNER_RESULTS_KEY,
                JOERN_FINDINGS_KEY,
                JSON_TYPE_OBJECT,
                JSON_TYPE_ARRAY,
                *WELL_KNOWN_DOCUMENT_KEYS,
            )
        )
    return _PUBLISHABLE_LITERALS


def is_publishable_literal(value: object) -> bool:
    """Return whether *value* may be published verbatim in a persisted diagnostic.

    ``True`` only for a ``str`` byte-equal to a member of
    :func:`publishable_literals`. A non-string is never publishable *as text*: an
    inert scalar is published by :func:`safe_scalar` as the value it is, and anything
    else is described.
    """
    return isinstance(value, str) and value in publishable_literals()


def _shape_is_escapable_control(char: str) -> bool:
    """Whether ``char`` must be escaped before it reaches a record."""
    if char in _SHAPE_KEEP_CONTROLS:
        return False
    code = ord(char)
    return code < 0x20 or code == 0x7F or 0x80 <= code <= 0x9F


def _shape_escape_controls(text: str) -> tuple[str, int]:
    """Return ``text`` with every escapable control replaced, and how many there were."""
    if not any(_shape_is_escapable_control(char) for char in text):
        return text, 0
    out: list[str] = []
    count = 0
    for char in text:
        if _shape_is_escapable_control(char):
            out.append(f"<U+{ord(char):04X}>")
            count += 1
        else:
            out.append(char)
    return "".join(out), count


def _shape_redact_userinfo(text: str) -> tuple[str, int]:
    """Return ``text`` with every URI userinfo component replaced, and the count."""
    redactions = 0

    def replace(match: "re.Match[str]") -> str:
        nonlocal redactions
        redactions += 1
        return f"{match.group(1)}{USERINFO_REDACTION}@"

    return _SHAPE_URI_USERINFO_RE.sub(replace, text), redactions


def _shape_digest(text: str) -> str:
    """Return the sha256 of ``text`` as UTF-8, so a bounded excerpt is still identified."""
    return hashlib.sha256(text.encode("utf-8", "surrogatepass")).hexdigest()


def safe_text(text: str, *, limit: int = SHAPE_VALUE_LIMIT) -> dict[str, object]:
    """Render one artifact-supplied string safe to persist, and say what changed.

    ``text`` is the published rendering and it is one of exactly two things: the
    string itself, where :func:`is_publishable_literal` holds -- an authored literal
    discloses nothing -- or :data:`REDACTED_TEXT`, the fixed marker. It is never a
    prefix, an excerpt or a sanitised copy of an artifact-supplied value (SEC-04).

    Everything else in the mapping is the evidence that keeps the redaction honest,
    and every figure is of the WHOLE original rather than of what is published:
    ``original_length`` is the full character count, ``sha256`` the full 64-hex digest
    of the whole value, and the two change counts say whether the value carried a URI
    userinfo credential or a control character at all.

    The counts are still taken redact-then-escape, in that order and over the whole
    value, because redaction matches on the URI's own syntax and an escaped control
    inside the authority would hide the ``@`` the pattern anchors on. ``truncated``
    reports that the sanitised value would have exceeded ``limit``; nothing is
    actually truncated, because nothing artifact-supplied is published.
    """
    if not isinstance(text, str):
        raise TypeError(f"safe_text expects a str; observed {type(text).__name__}")
    original_length = len(text)
    digest = _shape_digest(text)
    redacted, userinfo_redactions = _shape_redact_userinfo(text)
    escaped, controls_escaped = _shape_escape_controls(redacted)
    publishable = is_publishable_literal(text)
    return {
        "text": text if publishable else REDACTED_TEXT,
        "original_length": original_length,
        "sha256": digest,
        # Measured on the sanitised form, as it always was: the question the flag
        # answers is whether this pipeline would have been willing to publish the
        # value at its full rendered length.
        "truncated": len(escaped) > limit,
        "controls_escaped": controls_escaped,
        "userinfo_redactions": userinfo_redactions,
        "publishable": publishable,
        "redacted": not publishable,
    }


def safe_value(
    value: object, *, context: str | None = None, limit: int = SHAPE_VALUE_LIMIT
) -> dict[str, object]:
    """Describe one artifact-supplied value: type, context, length, digest, no content.

    The description a persisted diagnostic gets in place of the value. ``excerpt``
    carries the published rendering -- an authored literal or :data:`REDACTED_TEXT`,
    never artifact bytes -- and keeps its key name because it is the member every
    consumer of this mapping already reads; ``redacted`` says which of the two it is,
    so a reader never has to compare against the marker to find out.

    A non-string is rendered from its ``repr`` rather than refused, because the values
    that reach here are exactly the ones whose type was wrong; the type name is
    reported separately so ``dict from version`` reads as the shape fault it is. That
    ``repr`` is measured and digested but not published: a ``dict``'s repr is the
    artifact's own keys and values spelled out.
    """
    text = value if isinstance(value, str) else repr(value)
    rendered = safe_text(text, limit=limit)
    return {
        "value_type": type(value).__name__,
        "context": context,
        "character_length": rendered["original_length"],
        "sha256": rendered["sha256"],
        "excerpt": rendered["text"],
        "truncated": rendered["truncated"],
        "controls_escaped": rendered["controls_escaped"],
        "userinfo_redactions": rendered["userinfo_redactions"],
        "publishable": rendered["publishable"],
        "redacted": rendered["redacted"],
    }


def safe_scalar(value: object, *, limit: int = SHAPE_VALUE_LIMIT) -> object:
    """Return ``value`` itself where its TYPE makes it safe, else a redaction marker.

    The same three-way split ``adapters/gitleaks.py::_safe_value_repr`` makes:

    * ``None``, a ``bool``, an ``int`` and a ``float`` are published as themselves.
      None can carry a captured secret and none can carry a control character, and
      the actual value is what makes the record comparable -- a ``version`` of
      ``null`` must read as ``null`` rather than as a redaction.
    * a ``str`` publishes verbatim only where it is an authored literal, and is
      otherwise the marker.
    * anything else -- a mapping or a list where a scalar was expected -- is the
      marker, with its type, length and digest carried by the sibling ``*_evidence``
      field every call site publishes beside this one.
    """
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return safe_text(value, limit=limit)["text"]
    return safe_value(value, limit=limit)["excerpt"]


def safe_keys(
    keys: "tuple[object, ...]",
    *,
    limit: int = SHAPE_KEYS_REPORTED_LIMIT,
    value_limit: int = SHAPE_VALUE_LIMIT,
    context: str = "top-level key",
) -> dict[str, object]:
    """Return the top-level *keys* bounded and redacted, with full per-key provenance.

    A key is as artifact-controlled as a value: a JSON key can carry a credential as
    easily as a value can, a control character in one is exactly as hostile to a
    reader, and a key long enough to bury a diagnostic is exactly as effective at
    burying it.

    So each reported key carries the SAME evidence contract a scalar value gets from
    :func:`safe_value` -- its context, its Python type, its **full** original character
    length, its **full** 64-character sha256, the published rendering, and the counts
    of what sanitising would have changed. Publishing only a rendering would discard
    the evidence: a reader who needs to know what the artifact actually sent could then
    neither measure it nor identify it. The rendering is what is safe to read -- an
    authored key name such as ``version`` verbatim, anything else :data:`REDACTED_TEXT`
    -- and the length and the digest are what make the original checkable.

    ``keys`` is a JSON object's key sequence, so every element is a ``str`` in practice.
    A non-string is still handled rather than refused, because a document that reached
    here is one whose shape was already wrong, and :func:`safe_value` reports the type
    separately so an unexpected one reads as the fault it is.

    Two bounds apply and they are deliberately separate parameters. ``limit`` caps HOW
    MANY keys are reported; ``value_limit`` caps how long each reported key's excerpt
    may be. Passing one where the other belongs silently truncates every excerpt to the
    key-count cap -- a defect that leaves the record looking bounded-as-designed while
    the excerpt limit nobody chose is in force.
    """
    reported = [
        safe_value(key, context=context, limit=value_limit) for key in keys[:limit]
    ]
    return {
        # Per-key diagnostic objects, each carrying context/type/full length/full
        # sha256/published rendering/change counts.
        "keys": reported,
        # The published renderings alone, in the same order, so a reader scanning for a
        # name does not have to walk the objects. This is a projection of `keys`
        # rather than a second measurement.
        "key_excerpts": [entry["excerpt"] for entry in reported],
        "total": len(keys),
        "reported": len(reported),
        "truncated": len(keys) > limit,
        "per_key_evidence": (
            "each entry of `keys` carries the key's context, Python type, full original "
            "character length, full sha256 and its published rendering, plus the number "
            "of control characters escaped and credential userinfo segments redacted. "
            "The rendering is the key itself only where it is byte-equal to a literal "
            "shape.py authors (shape.publishable_literals); every other key is published "
            "as the fixed shape.REDACTED_TEXT marker, because a JSON key is "
            "artifact-supplied text and this record is preserved. The length and digest "
            "are full so the original stays checkable against this record."
        ),
    }


def describe_document(doc: object) -> dict[str, object]:
    """Describe *doc*'s top level for a halt report.

    Returns the observed top-level JSON type, the Python type, the observed ``version``
    value (``None`` when the document is not a mapping or carries no ``version``), the
    top-level keys in document order, and the top-level length. Everything AAP 0.5.4
    requires a halt to quote, computed without touching the document's contents beyond
    its top level -- a 73 MB SARIF artifact is described as cheaply as a two-key stub.
    """
    is_mapping = isinstance(doc, Mapping)
    if is_mapping:
        keys: tuple[object, ...] = tuple(doc.keys())
        version: object | None = doc.get(SARIF_VERSION_KEY)
    else:
        keys = ()
        version = None
    try:
        length: int | None = len(doc)  # type: ignore[arg-type]
    except TypeError:
        # Scalars (a bare string is sized, an int is not) simply have no length.
        length = None
    return {
        "top_level_type": _json_type_name(doc),
        "python_type": type(doc).__name__,
        "version": version,
        "top_level_keys": keys,
        "top_level_length": length,
    }


def detection_evidence(doc: object) -> dict[str, object]:
    """Return the *evidence* for the shape decision: the two field checks, separately.

    :func:`is_sarif` returns the conjunction, which is what routing needs and all that
    routing should need. A reader of ``harness/artifacts/logs/normalize-run.json``
    needs more than the conjunction: the AAP requires the detection outcome recorded
    per artifact *"including the evidence (the two field checks)"*, so that a native
    artifact recorded as native can be seen to have failed the tests rather than merely
    asserted to have failed them. That distinction is the whole point of the mandated
    negative direction -- a permissive detector that accepted a native artifact as SARIF
    would produce an empty result set rather than an error, and an empty result set is
    indistinguishable from a clean scan.

    The evidence is produced here, in the module that owns the test, and never
    recomputed by the recorder: the conjunction below is :func:`is_sarif` itself rather
    than a second spelling of it, so the record cites one measurement rather than a
    reconstruction that could drift from the decision it describes.

    Only the document's top level is touched -- a 73 MB SARIF artifact is described as
    cheaply as a two-key stub -- and the top-level keys are carried as a tuple so a
    reader can see *what was there instead* when a check failed.

    >>> evidence = detection_evidence({"version": "2.1.0", "runs": []})
    >>> evidence["version_matches"], evidence["runs_is_array"], evidence["is_sarif"]
    (True, True, True)
    >>> native = detection_evidence([{"RuleID": "generic-api-key"}])
    >>> native["version_matches"], native["runs_is_array"], native["is_sarif"]
    (False, False, False)
    >>> native["top_level_type"]
    'array'
    """
    description = describe_document(doc)
    is_mapping = isinstance(doc, Mapping)
    version_observed = description["version"]
    runs_observed = doc.get(SARIF_RUNS_KEY) if is_mapping else None
    keys_rendered = safe_keys(tuple(description["top_level_keys"] or ()))
    return {
        "test": (
            f'a document is SARIF when {SARIF_VERSION_KEY} == "{SARIF_VERSION}" '
            f"AND {SARIF_RUNS_KEY} is an array; those two together are the whole test "
            "(AAP 0.5.4). Nothing else is consulted: not $schema, not the filename, "
            "not tool.driver."
        ),
        "top_level_type": description["top_level_type"],
        # Redacted at this owning site rather than at the recorder: this mapping
        # reaches the SUCCESS path of normalize-run.json, where a valid native envelope
        # may legitimately carry arbitrary extra top-level keys, so an adversary would
        # otherwise choose both the content and the size of a durable record. Each
        # entry is a diagnostic object carrying the key's full character length and
        # full sha256 alongside its published rendering, so redacting the text does not
        # cost the evidence; the full count is published beside the list.
        "top_level_keys": keys_rendered["keys"],
        "top_level_key_excerpts": keys_rendered["key_excerpts"],
        "top_level_keys_total": keys_rendered["total"],
        "top_level_keys_reported": keys_rendered["reported"],
        "top_level_keys_truncated": keys_rendered["truncated"],
        "top_level_keys_evidence": keys_rendered["per_key_evidence"],
        "top_level_length": description["top_level_length"],
        "version_key": SARIF_VERSION_KEY,
        "version_expected": SARIF_VERSION,
        # The published value is safe -- an authored literal or the redaction marker --
        # while the DECISION below is taken from the raw value, so redaction can never
        # change what the detector concluded. That is why `version_matches` remains
        # readable for a version this record does not publish.
        "version_observed": safe_scalar(version_observed),
        "version_observed_evidence": safe_value(version_observed, context=SARIF_VERSION_KEY),
        "version_matches": version_observed == SARIF_VERSION,
        "runs_key": SARIF_RUNS_KEY,
        "runs_observed_type": _json_type_name(runs_observed) if is_mapping else "absent",
        "runs_is_array": isinstance(runs_observed, list),
        "runs_length": len(runs_observed) if isinstance(runs_observed, list) else None,
        # The conjunction, taken from the detector itself rather than from the two
        # booleans above, so this field cannot disagree with the routing decision.
        "is_sarif": is_sarif(doc),
    }


def _is_supported_container(doc: object) -> bool:
    """Return ``True`` when *doc*'s top level could be any of the nine artifacts.

    Every artifact in the contract is a JSON object or a JSON array. A scalar or
    ``None`` top level is not a shape any adapter can own, so it is a halt rather than a
    best-effort parse (AAP 0.5.4). This is the *only* structural statement this module
    makes: everything inside the container belongs to the adapter.
    """
    return isinstance(doc, (Mapping, list))


# --------------------------------------------------------------------------------------
# Native document signatures (AAP 0.5.4's per-shape record-location table, 0.9.2's halt)
# --------------------------------------------------------------------------------------
# One signature per native writer, each naming the record container that writer's
# artifact must carry -- the same container its adapter walks and the same one
# normalize.reconcile.count_records counts. The signature is the whole of the test: the
# container's presence and its JSON type, never its length and never an optional sibling
# key, because an empty finding set is legitimate for every one of the six.
#
# A signature is expressed as data rather than as six hand-written predicates, and
# NativeSignature.matches derives the test from that data. One authority, so the words a
# halt report quotes and the test the halt was raised by cannot drift apart.
#
# The three lookups at the end of this section resolve their argument through
# resolve_tool, which is authored in the next section: the section boundary follows the
# subject matter rather than the call graph, and resolution happens at call time.

#: The two JSON container types a signature can require, in JSON's vocabulary rather
#: than Python's -- these strings appear verbatim in a halt report, beside the observed
#: type :func:`_json_type_name` produces for the same document.
JSON_TYPE_OBJECT = "object"
JSON_TYPE_ARRAY = "array"

#: Container type name -> the isinstance test for it. ``list`` alone stands for a JSON
#: array, exactly as :func:`is_sarif` tests ``runs``: a parsed JSON array is always a
#: ``list``, and accepting a broader ``Sequence`` would accept a ``str`` as an array.
_CONTAINER_TESTS = MappingProxyType(
    {
        JSON_TYPE_OBJECT: lambda value: isinstance(value, Mapping),
        JSON_TYPE_ARRAY: lambda value: isinstance(value, list),
    }
)


@dataclass(frozen=True, slots=True)
class NativeSignature:
    """One native writer's document signature: the test, and the test stated in words.

    Frozen and authored as data. :meth:`matches` derives the predicate from the fields
    below, so the ``statement`` a halt report quotes describes the test that was
    actually run rather than a second, hand-maintained spelling of it.

    :param tool: the canonical tool identifier this signature belongs to.
    :param statement: the signature in words, for a halt report and for
        ``harness/artifacts/logs/normalize-run.json``. Written so a reader who cannot
        open the artifact still knows what was required of it.
    :param top_level: the accepted top-level container types, in JSON's vocabulary.
        Two entries only for Checkov, whose artifact legitimately takes either form.
    :param required_key: the key an **object** top level must carry, or ``None`` where
        an object top level is not accepted at all (Gitleaks).
    :param required_key_type: the JSON type ``required_key``'s value must have.
    :param element_required_key: the key every element of an **array** top level must
        carry (Checkov's multi-framework form), or ``None`` where the array itself is
        the record container (Gitleaks).
    :param element_required_key_type: the JSON type ``element_required_key``'s value
        must have.
    """

    tool: str
    statement: str
    top_level: tuple[str, ...]
    required_key: str | None = None
    required_key_type: str | None = None
    element_required_key: str | None = None
    element_required_key_type: str | None = None

    def __post_init__(self) -> None:
        """Reject a signature that could not be evaluated, at authoring time.

        These are authored constants, so every failure here is a typo in this module
        rather than an artifact condition -- and a typo that went unchecked would make a
        signature silently unfalsifiable, which is the one defect a shape test must not
        have.
        """
        if self.tool not in _CANONICAL_TOOL_SET:
            raise ValueError(
                f"unknown tool {self.tool!r}; expected one of: "
                f"{', '.join(CANONICAL_TOOLS)}"
            )
        if not self.statement.strip():
            raise ValueError(
                f"the signature for {self.tool!r} must state its test in words: the "
                "halt report quotes it, and an empty statement leaves a reader with a "
                "reason and no requirement"
            )
        if not self.top_level:
            raise ValueError(
                f"the signature for {self.tool!r} must accept at least one top-level "
                "container type"
            )
        for name in self.top_level:
            if name not in _CONTAINER_TESTS:
                raise ValueError(
                    f"the signature for {self.tool!r} names top-level type {name!r}; "
                    f"expected one of: {', '.join(_CONTAINER_TESTS)}"
                )
        for label, key, key_type in (
            ("required_key", self.required_key, self.required_key_type),
            (
                "element_required_key",
                self.element_required_key,
                self.element_required_key_type,
            ),
        ):
            if key is None:
                if key_type is not None:
                    raise ValueError(
                        f"the signature for {self.tool!r} gives {label}_type without "
                        f"{label}"
                    )
                continue
            if key_type not in _CONTAINER_TESTS:
                raise ValueError(
                    f"the signature for {self.tool!r} requires {label} {key!r} to be "
                    f"{key_type!r}; expected one of: {', '.join(_CONTAINER_TESTS)}"
                )
        if JSON_TYPE_OBJECT in self.top_level and self.required_key is None:
            raise ValueError(
                f"the signature for {self.tool!r} accepts an object top level but names "
                "no required key, so every object would satisfy it"
            )

    @property
    def accepts_object(self) -> bool:
        """``True`` when an object top level can satisfy this signature."""
        return JSON_TYPE_OBJECT in self.top_level

    @property
    def accepts_array(self) -> bool:
        """``True`` when an array top level can satisfy this signature."""
        return JSON_TYPE_ARRAY in self.top_level

    def matches(self, doc: object) -> bool:
        """Return ``True`` when *doc* is this writer's native document shape.

        Only the top level and the named container are read: the container's *length* is
        never consulted, because an empty finding set is legitimate for every writer --
        Gitleaks writes ``[]`` when it matches nothing, a Checkov report can carry
        ``"failed_checks": []``, and an empty ``findings`` array is a Joern query set
        that matched nothing. Nothing inside a record is read at all; that is the
        adapter's work, over a document whose shape this test has established.

        >>> NATIVE_SIGNATURES["gitleaks"].matches([])
        True
        >>> NATIVE_SIGNATURES["joern"].matches({"findings": []})
        True
        >>> NATIVE_SIGNATURES["joern"].matches({})
        False
        >>> NATIVE_SIGNATURES["checkov"].matches([{"results": {}}])
        True
        >>> NATIVE_SIGNATURES["checkov"].matches([{"results": "not an object"}])
        False
        """
        if isinstance(doc, Mapping):
            if not self.accepts_object or self.required_key is None:
                return False
            return _CONTAINER_TESTS[self.required_key_type](doc.get(self.required_key))
        if isinstance(doc, list):
            if not self.accepts_array:
                return False
            if self.element_required_key is None:
                # The array itself is the record container (Gitleaks), so there is
                # nothing further to require: an element's own shape is a per-record
                # question the adapter answers as a counted rejection.
                return True
            element_test = _CONTAINER_TESTS[self.element_required_key_type]
            return all(
                isinstance(element, Mapping)
                and element_test(element.get(self.element_required_key))
                for element in doc
            )
        return False

    def observe(self, doc: object) -> dict[str, object]:
        """Describe what *doc* carries where this signature requires a container.

        The other half of a diagnosable halt: :attr:`statement` says what was required
        and this says what was there instead, in JSON's vocabulary and without reading a
        single record. Derived from the same fields :meth:`matches` uses, so the two
        cannot describe different tests.

        For an array top level with a per-element requirement, the **first** failing
        element is named by index and type -- one offender is what a reader needs, and
        walking the whole array to collect every offender would turn a bounded halt
        report into an unbounded one.
        """
        observed: dict[str, object] = {
            "tool": self.tool,
            "expected": self.statement,
            "accepted_top_level_types": list(self.top_level),
            "observed_top_level_type": _json_type_name(doc),
            "matches": self.matches(doc),
        }
        if isinstance(doc, Mapping) and self.required_key is not None:
            present = self.required_key in doc
            observed["required_key"] = self.required_key
            observed["required_key_type"] = self.required_key_type
            observed["required_key_present"] = present
            observed["observed_key_type"] = (
                _json_type_name(doc.get(self.required_key)) if present else "absent"
            )
        elif isinstance(doc, list) and self.element_required_key is not None:
            observed["element_required_key"] = self.element_required_key
            observed["element_required_key_type"] = self.element_required_key_type
            observed["observed_element_count"] = len(doc)
            element_test = _CONTAINER_TESTS[self.element_required_key_type]
            for index, element in enumerate(doc):
                if not isinstance(element, Mapping):
                    observed["first_failing_element_index"] = index
                    observed["first_failing_element_type"] = _json_type_name(element)
                    break
                if not element_test(element.get(self.element_required_key)):
                    observed["first_failing_element_index"] = index
                    observed["first_failing_element_type"] = JSON_TYPE_OBJECT
                    observed["first_failing_element_key_type"] = (
                        _json_type_name(element.get(self.element_required_key))
                        if self.element_required_key in element
                        else "absent"
                    )
                    break
        return observed


#: One signature per native writer, in canonical tool order. Each ``statement`` is the
#: requirement AAP 0.5.4's per-shape record-location table places on that artifact,
#: written for a reader of a halt report rather than for a parser.
#:
#: The three SARIF producers are deliberately absent: SARIF is their only legitimate
#: shape and the conjunction already tests it, so a signature for them would either
#: duplicate that test or invent a native shape they never write. See
#: :data:`TOOLS_WITHOUT_A_NATIVE_SIGNATURE`.
NATIVE_SIGNATURES = MappingProxyType(
    {
        "gitleaks": NativeSignature(
            tool="gitleaks",
            statement=(
                "a JSON array at the top level, each element one finding. An empty "
                "array is legitimate: it is what gitleaks writes when it matches nothing"
            ),
            top_level=(JSON_TYPE_ARRAY,),
        ),
        "checkov": NativeSignature(
            tool="checkov",
            statement=(
                'either a JSON object carrying "results" whose value is a JSON object, '
                "or a JSON array whose every element is a JSON object carrying a "
                '"results" object (the multi-framework form). An empty or absent '
                '"failed_checks" inside "results" is legitimate: this dataset emits '
                "failed checks only, and a framework that failed nothing still reports"
            ),
            top_level=(JSON_TYPE_OBJECT, JSON_TYPE_ARRAY),
            required_key="results",
            required_key_type=JSON_TYPE_OBJECT,
            element_required_key="results",
            element_required_key_type=JSON_TYPE_OBJECT,
        ),
        "trivy": NativeSignature(
            tool="trivy",
            statement=(
                'a JSON object carrying "Results" whose value is a JSON array, each '
                "element one scanned target holding the finding sections. An empty "
                "array is legitimate"
            ),
            top_level=(JSON_TYPE_OBJECT,),
            required_key="Results",
            required_key_type=JSON_TYPE_ARRAY,
        ),
        "osv-scanner": NativeSignature(
            tool="osv-scanner",
            statement=(
                'a JSON object carrying "results" whose value is a JSON array, each '
                "element one source holding its packages. An empty array is legitimate"
            ),
            top_level=(JSON_TYPE_OBJECT,),
            required_key="results",
            required_key_type=JSON_TYPE_ARRAY,
        ),
        "dependency-check": NativeSignature(
            tool="dependency-check",
            statement=(
                'a JSON object carrying "dependencies" whose value is a JSON array, '
                "each element one scanned dependency holding its vulnerabilities. An "
                "empty array is legitimate, and so is a report whose every dependency "
                "carries no vulnerability at all"
            ),
            top_level=(JSON_TYPE_OBJECT,),
            required_key="dependencies",
            required_key_type=JSON_TYPE_ARRAY,
        ),
        "joern": NativeSignature(
            tool="joern",
            statement=(
                'a JSON object carrying "findings" whose value is a JSON array, each '
                "element one finding. An empty array is legitimate: it is a query set "
                "that matched nothing. No envelope member is required, so the collector "
                "may add one without invalidating its own artifact"
            ),
            top_level=(JSON_TYPE_OBJECT,),
            required_key="findings",
            required_key_type=JSON_TYPE_ARRAY,
        ),
    }
)

#: The six writers carrying a native signature, in canonical tool order.
NATIVE_SIGNATURE_TOOLS = tuple(
    tool for tool in CANONICAL_TOOLS if tool in NATIVE_SIGNATURES
)

#: The three writers carrying none, in canonical tool order. Derived rather than
#: authored a second time: it is exactly :data:`SARIF_PRODUCERS`, and asserting that
#: identity is cheaper than maintaining two lists that must agree.
TOOLS_WITHOUT_A_NATIVE_SIGNATURE = tuple(
    tool for tool in CANONICAL_TOOLS if tool not in NATIVE_SIGNATURES
)


def native_signature_for(tool: str) -> NativeSignature | None:
    """Return *tool*'s native signature, or ``None`` where it has none.

    ``None`` is a real answer rather than a missing one: the three SARIF producers
    write no native document, so there is no native shape of theirs to test.
    :func:`route` treats ``None`` as "no signature test applies" and routes on, which is
    what keeps a non-SARIF document under a producer's name reaching the shared SARIF
    adapter that owns its validation.

    Raises ``ValueError`` for anything outside the nine canonical identifiers, exactly
    as the other lookups over the authored tables do.
    """
    canonical = resolve_tool(tool)
    if canonical is None:
        raise ValueError(
            f"unknown tool {tool!r}; expected one of: {', '.join(CANONICAL_TOOLS)}"
        )
    return NATIVE_SIGNATURES.get(canonical)


def matches_native_signature(tool: str, doc: object) -> bool:
    """Return ``True`` when *doc* is *tool*'s native document shape.

    A tool with no native signature returns ``True``: there is no native shape of its to
    contradict, and returning ``False`` would halt a producer's artifact for failing a
    test that does not exist. The SARIF conjunction is not consulted here -- :func:`route`
    tests that first, and this function answers only the native half.
    """
    signature = native_signature_for(tool)
    return True if signature is None else signature.matches(doc)


def native_signature_evidence(tool: str, doc: object) -> dict[str, object]:
    """Return the *evidence* for the native signature decision, for the run record.

    Mirrors :func:`detection_evidence`, which does the same for the SARIF conjunction: a
    document recorded as a writer's native shape must be *seen* to have satisfied that
    writer's signature rather than merely asserted to have. The verdict is taken from
    :meth:`NativeSignature.matches` itself rather than recomputed, so the record cites
    the decision's own measurement.

    A tool with no native signature is reported as such -- ``signature_required`` false,
    ``matches`` true -- rather than omitted, so all nine tools have an entry a reader can
    account for.
    """
    signature = native_signature_for(tool)
    if signature is None:
        return {
            "tool": resolve_tool(tool),
            "signature_required": False,
            "expected": (
                "no native signature: this writer emits SARIF 2.1.0 only, so the "
                "version-plus-runs conjunction is the whole of its shape test"
            ),
            "observed_top_level_type": _json_type_name(doc),
            "matches": True,
        }
    return {"signature_required": True, **signature.observe(doc)}


# --------------------------------------------------------------------------------------
# Native envelope validation (AAP 0.5.4's per-shape table)
# --------------------------------------------------------------------------------------
# A recognised filename says who wrote the artifact; it does not say that the bytes are
# that tool's shape. Routing on the name alone let two documents through that must not
# be normalized: a SARIF artifact carrying a version this dataset has not been
# validated against, and an arbitrary JSON container that happens to sit under a
# recognised name. Both then reached an adapter that found none of the containers it
# walks and reported zero rows -- and an empty result set is indistinguishable from a
# clean scan, which is the failure AAP 0.5.4 makes a halt rather than a best-effort
# parse.
#
# So each native shape carries the one envelope marker AAP 0.5.4's per-shape table
# names for it, and that marker is checked here before an adapter is named. The markers
# are containers rather than contents: an **empty** report is an ordinary outcome for
# every one of these tools and must route, so the test is "the container this tool's
# records live in is present and is the right JSON type", never "there is at least one
# record". Nothing beyond that is checked -- the record-level structure inside the
# container stays the adapter's to validate and the adapter's to halt on.

#: Container keys, spelled once each. Trivy's is capitalised because ``report.go``
#: capitalises it; Checkov's, OSV-Scanner's and Joern's are lower-case because their
#: producers write them that way. A misspelling here would reject every artifact of
#: that tool, which is why each is asserted against a captured fixture by
#: ``oss-scan-results/adapter-tests/test_shape_routing_negative.py``.
TRIVY_RESULTS_KEY = "Results"
CHECKOV_RESULTS_KEY = "results"
DEPENDENCY_CHECK_DEPENDENCIES_KEY = "dependencies"
OSV_SCANNER_RESULTS_KEY = "results"
JOERN_FINDINGS_KEY = "findings"


def _is_json_array(value: object) -> bool:
    """Return ``True`` when *value* is a JSON array.

    A tuple is not accepted: ``json.load`` never produces one, so a tuple here means a
    hand-built caller argument rather than a parsed artifact, and accepting it would let
    a test pass against a document no runner can write.
    """
    return isinstance(value, list)


def _is_checkov_report(report: object) -> bool:
    """Return ``True`` when *report* is one Checkov report object.

    The marker is a ``results`` **object**: that is where
    ``results.failed_checks[]`` -- the count unit AAP 0.5.4 names for this shape --
    lives, in both of Checkov's top-level forms. ``passed_checks`` and
    ``skipped_checks`` are not required, because a report that failed nothing still
    carries the object and legitimately omits neither more nor less than Checkov chose
    to write.
    """
    return isinstance(report, Mapping) and isinstance(
        report.get(CHECKOV_RESULTS_KEY), Mapping
    )


def _matches_trivy_envelope(doc: object) -> bool:
    """``Results`` present **and** a JSON array (AAP 0.5.4: ``Results[]``).

    One contract, and it is the fail-closed one. AAP 0.5.4 names this shape's count unit
    ``Results[]``, makes an artifact matching neither SARIF nor a known native shape a
    halt rather than a best-effort parse, and states the reject-rather-than-infer
    principle -- so the array is required rather than inferred from a member that is
    merely present. That is exactly the test :data:`NATIVE_SIGNATURES`'s ``trivy`` entry
    declares (``required_key_type`` :data:`JSON_TYPE_ARRAY`), so the declared signature
    and this predicate answer one question once, and a halt report's quoted signature
    cannot say something the router did not do.

    Presence is required and emptiness is not: Trivy writes the key on every report,
    including one with nothing to say, so its **absence** means the document is not a
    Trivy report at all, while ``"Results": []`` is a complete report of a scan that
    resolved no target and routes normally. A ``Results`` that is present as ``null``,
    an object or a string is refused *here*, at the envelope, which is where a
    mis-shaped document must stop: an adapter handed a document with none of the
    containers it walks reports zero rows, and an empty result set is indistinguishable
    from a clean scan. ``null`` gets no exemption for being Go's rendering of an unset
    slice -- a document that states no ``Results[]`` states no count unit either, and
    admitting it would put the emptiness this dataset reports beyond the reach of the
    reconciliation identity that is supposed to establish it.
    """
    if not isinstance(doc, Mapping):
        return False
    if TRIVY_RESULTS_KEY not in doc:
        return False
    return _is_json_array(doc[TRIVY_RESULTS_KEY])


def _matches_gitleaks_envelope(doc: object) -> bool:
    """A bare top-level array (AAP 0.5.4: *"top-level array; one element"*).

    Elements are deliberately not inspected. A malformed element is a per-record
    rejection the adapter counts under its class, and the independent traversal counts
    it too, so promoting it to a document-level halt would destroy the reconciliation
    identity it exists to protect. An **empty** array is Gitleaks finding nothing, which
    is this provisioning's near-expected outcome.
    """
    return _is_json_array(doc)


def _matches_checkov_envelope(doc: object) -> bool:
    """One report object, or the multi-framework array of them (AAP 0.5.4).

    Both forms are the shape; neither is preferred. The array form must be **non-empty**
    and every element must be a report object: an empty array carries no report at all,
    and a top-level array whose elements are findings rather than reports is the
    ``gitleaks.json`` shape, which under this name would otherwise be walked as Checkov
    and yield zero rows.
    """
    if _is_json_array(doc):
        return bool(doc) and all(_is_checkov_report(report) for report in doc)
    return _is_checkov_report(doc)


def _matches_dependency_check_envelope(doc: object) -> bool:
    """``dependencies`` present as an array (AAP 0.5.4: ``dependencies[]``).

    The one container this shape's count unit walks. ``reportSchema``, ``scanInfo`` and
    ``projectInfo`` are not required: refusing a report for carrying less metadata than
    one particular version happened to would halt on a version difference AAP 0.9.3
    records and continues past. An empty array is a legitimate clean report, and for
    this tool over this scope a clean report is the expected outcome.
    """
    return isinstance(doc, Mapping) and _is_json_array(
        doc.get(DEPENDENCY_CHECK_DEPENDENCIES_KEY)
    )


def _matches_osv_scanner_envelope(doc: object) -> bool:
    """``results`` present as an array (AAP 0.5.4: ``results[].packages[]...``).

    Validated even though this provisioning expects no ``osv-scanner.json`` at all: an
    artifact that does appear must be recognised rather than met with a routing table
    that was never exercised, and the conditional adapter is created *"if and only if"*
    the artifact exists (AAP 0.6.1). Keeping the envelope authored here is what makes
    the two consistent.
    """
    return isinstance(doc, Mapping) and _is_json_array(doc.get(OSV_SCANNER_RESULTS_KEY))


def _matches_joern_envelope(doc: object) -> bool:
    """``findings`` present as an array -- this harness's own shape, not a Joern format.

    ``harness/lib/joern-scan.sc`` writes ``{tool, tool_version, cpg, graph{...},
    query_set, queries[], findings[]}``. Only ``findings`` is required: it is the count
    unit, and the collector's other members are provenance the adapter reads where they
    are present rather than structure it depends on.
    """
    return isinstance(doc, Mapping) and _is_json_array(doc.get(JOERN_FINDINGS_KEY))


#: The envelope test per tool, and the prose a halt quotes when it fails. The three
#: SARIF producers have **no** native shape: their artifact is SARIF or it is a halt,
#: which is why their entry is a validator that always refuses. Written as one table so
#: the test and the message it produces cannot describe different requirements.
_NATIVE_ENVELOPE: Mapping[str, tuple[object, str]] = MappingProxyType(
    {
        "opengrep": (
            lambda doc: False,
            'a SARIF 2.1.0 document -- version == "2.1.0" together with a runs array; '
            "this runner writes SARIF and nothing else, so there is no native shape to "
            "fall back to",
        ),
        "semgrep": (
            lambda doc: False,
            'a SARIF 2.1.0 document -- version == "2.1.0" together with a runs array; '
            "this runner writes SARIF and nothing else, so there is no native shape to "
            "fall back to",
        ),
        "datadog-static-analyzer": (
            lambda doc: False,
            'a SARIF 2.1.0 document -- version == "2.1.0" together with a runs array; '
            "this runner writes SARIF and nothing else, so there is no native shape to "
            "fall back to",
        ),
        "gitleaks": (
            _matches_gitleaks_envelope,
            "a JSON array of finding objects at the top level (an empty array is a "
            "legitimate clean report)",
        ),
        "checkov": (
            _matches_checkov_envelope,
            f"either one report object carrying a {CHECKOV_RESULTS_KEY!r} object, or a "
            f"non-empty top-level array of such report objects (the multi-framework "
            f"form)",
        ),
        "trivy": (
            _matches_trivy_envelope,
            f"a report object carrying {TRIVY_RESULTS_KEY!r} as a JSON array (an empty "
            f"array is a legitimate clean report, while an absent or null "
            f"{TRIVY_RESULTS_KEY!r} means the document is not a Trivy report)",
        ),
        "osv-scanner": (
            _matches_osv_scanner_envelope,
            f"a report object carrying an {OSV_SCANNER_RESULTS_KEY!r} array",
        ),
        "dependency-check": (
            _matches_dependency_check_envelope,
            f"a report object carrying a {DEPENDENCY_CHECK_DEPENDENCIES_KEY!r} array "
            f"(an empty array is a legitimate clean report)",
        ),
        "joern": (
            _matches_joern_envelope,
            f"a collector object carrying a {JOERN_FINDINGS_KEY!r} array, as written by "
            f"harness/lib/joern-scan.sc",
        ),
    }
)


def matches_native_shape(tool: str, doc: object) -> bool:
    """Return ``True`` when *doc* is *tool*'s known native shape.

    The envelope test only: the container AAP 0.5.4's per-shape table names for this
    tool is present and is the right JSON type. Record contents are never examined, and
    an empty container passes -- a tool finding nothing is an outcome, not a fault.

    Always ``False`` for the three SARIF producers: their artifact satisfies the SARIF
    conjunction or it halts, and there is no second shape they may take.

    Raises ``ValueError`` for anything outside the nine canonical identifiers, so a
    caller cannot silently ask about a tool this table does not carry.

    >>> matches_native_shape("gitleaks", [])
    True
    >>> matches_native_shape("trivy", {"SchemaVersion": 2})
    False
    >>> matches_native_shape("opengrep", {"version": "2.1.0", "runs": []})
    False
    """
    canonical = resolve_tool(tool)
    if canonical is None:
        raise ValueError(
            f"unknown tool {tool!r}; expected one of: {', '.join(CANONICAL_TOOLS)}"
        )
    predicate, _requirement = _NATIVE_ENVELOPE[canonical]
    return bool(predicate(doc))  # type: ignore[operator]


def native_shape_requirement(tool: str) -> str:
    """Return the prose statement of what *tool*'s artifact must be.

    Quoted verbatim into the halt message and into
    ``harness/artifacts/logs/normalize-run.json``, so a reader of the record is told
    what was expected rather than only that something was not it. Taken from the same
    table :func:`matches_native_shape` tests against, so the requirement stated can
    never drift from the requirement enforced.

    Raises ``ValueError`` for anything outside the nine canonical identifiers.
    """
    canonical = resolve_tool(tool)
    if canonical is None:
        raise ValueError(
            f"unknown tool {tool!r}; expected one of: {', '.join(CANONICAL_TOOLS)}"
        )
    return _NATIVE_ENVELOPE[canonical][1]


# --------------------------------------------------------------------------------------
# Lookups over the authored tables
# --------------------------------------------------------------------------------------


def _artifact_name(value: object) -> str:
    """Return the filename component of *value*, accepting a str or a path-like.

    ``PurePath`` handles both, so no ``os`` import is needed. A value of any other type
    is a programming error rather than an artifact condition, so it raises ``TypeError``
    instead of the shape halt.
    """
    if isinstance(value, str):
        text = value
    elif isinstance(value, PurePath):
        text = str(value)
    elif hasattr(value, "__fspath__"):
        fspath = value.__fspath__()  # type: ignore[union-attr]
        if isinstance(fspath, bytes):
            raise TypeError(
                "artifact identifiers must be str or an os.PathLike[str]; got a "
                "bytes path, which cannot be compared against the authored filenames"
            )
        text = fspath
    else:
        raise TypeError(
            "expected a canonical tool identifier, an artifact filename or a path; got "
            f"{type(value).__name__}"
        )
    return PurePath(text).name if text else ""


def resolve_tool(value: object) -> str | None:
    """Resolve *value* to a canonical tool identifier, or ``None`` when it is unknown.

    Accepts a canonical tool identifier (``"dependency-check"``), one of the nine
    artifact filenames (``"dependency-check.json"``), or a path ending in one
    (``"harness/artifacts/raw/dependency-check.json"``). Matching is exact and
    case-sensitive: the identifiers and filenames are a closed set, and an underscored
    or differently-cased spelling is a caller bug worth surfacing rather than
    absorbing.

    Returning ``None`` rather than raising keeps this usable as a predicate -- ``cli.py``
    uses it to spot a stray file in ``harness/artifacts/raw/``, which must hold one
    artifact per tool and nothing else ever (AAP 0.6.1). :func:`route` is the function
    that turns an unresolvable name into the halt.
    """
    name = _artifact_name(value)
    if not name:
        return None
    tool = TOOL_BY_ARTIFACT_FILENAME.get(name)
    if tool is not None:
        return tool
    if name in _CANONICAL_TOOL_SET:
        return name
    return None


def is_known_tool(value: object) -> bool:
    """Return ``True`` when *value* names one of the nine tools or their artifacts."""
    return resolve_tool(value) is not None


def artifact_filename_for(tool: str) -> str:
    """Return the fixed artifact filename *tool*'s runner writes.

    Raises ``ValueError`` for anything outside the nine canonical identifiers.
    """
    canonical = resolve_tool(tool)
    if canonical is None:
        raise ValueError(
            f"unknown tool {tool!r}; expected one of: {', '.join(CANONICAL_TOOLS)}"
        )
    return ARTIFACT_FILENAME_BY_TOOL[canonical]


def adapter_module_for(tool: str) -> str:
    """Return the adapter module key that reads *tool*'s native artifact.

    Nine tools resolve to seven module keys: the three SARIF producers share
    ``sarif``. Raises ``ValueError`` for anything outside the nine canonical
    identifiers.
    """
    canonical = resolve_tool(tool)
    if canonical is None:
        raise ValueError(
            f"unknown tool {tool!r}; expected one of: {', '.join(CANONICAL_TOOLS)}"
        )
    return ADAPTER_MODULE_BY_TOOL[canonical]


def scanner_class_for(tool: str) -> object:
    """Return *tool*'s fixed ``scanner_class``, or :data:`PER_RECORD` for Trivy.

    The return type is deliberately not ``str``: Trivy's class is decided per record,
    and the sentinel is what stops a caller emitting a plausible-looking wrong class.
    Test with :func:`is_per_record` before using the value in a row.

    Raises ``ValueError`` for anything outside the nine canonical identifiers -- which
    is the same condition the gate halts on when a ``harness/bin/`` entry names a
    scanner absent from this table (AAP 0.8.1).
    """
    canonical = resolve_tool(tool)
    if canonical is None:
        raise ValueError(
            f"unknown tool {tool!r}; expected one of: {', '.join(CANONICAL_TOOLS)}"
        )
    return SCANNER_CLASS_BY_TOOL[canonical]


def scanner_class_for_trivy_section(section: str) -> str:
    """Resolve a Trivy finding section to the ``scanner_class`` its records carry.

    *section* is the name of the array the record was read from -- ``Vulnerabilities``,
    ``Secrets`` or ``Misconfigurations``. Record content is never consulted (AAP
    0.5.4). An unattributable section raises ``ValueError``, which the Trivy adapter
    records as its ``unattributable_section`` rejection class rather than guessing a
    class for the record.
    """
    if not isinstance(section, str):
        raise TypeError(
            "a trivy finding section must be named by a string; got "
            f"{type(section).__name__}"
        )
    try:
        return TRIVY_SECTION_SCANNER_CLASS[section]
    except KeyError:
        raise ValueError(
            f"unsupported trivy finding section {section!r}; supported sections are: "
            f"{', '.join(TRIVY_SECTION_SCANNER_CLASS)} (unsupported sections this "
            f"dataset validates empty: {', '.join(TRIVY_UNSUPPORTED_FINDING_SECTIONS)})"
        ) from None



# --------------------------------------------------------------------------------------
# The halt (AAP 0.5.4, 0.9.2)
# --------------------------------------------------------------------------------------

#: The artifact's name is not one of the nine. Includes a document that is valid SARIF
#: under an unexpected name -- ``harness/artifacts/raw/`` is runner-only, and the
#: Opengrep taint A/B arms are valid SARIF living in ``logs/`` (AAP 0.1.3).
REASON_UNRECOGNIZED_ARTIFACT_NAME = "unrecognized-artifact-name"

#: The document's top level is neither a JSON object nor a JSON array, so it is not a
#: shape any adapter can own.
REASON_UNSUPPORTED_DOCUMENT_TYPE = "unsupported-document-type"

#: The artifact was written by one of the three SARIF producers and its document does
#: not satisfy the SARIF conjunction. These runners write SARIF and nothing else, so
#: there is no native shape to fall back to: a ``version`` this dataset has not been
#: validated against, a ``runs`` mapping instead of an array, or an unrelated JSON
#: container under one of their names all land here.
REASON_SARIF_PRODUCER_NOT_SARIF = "sarif-producer-artifact-not-sarif"

#: The artifact's name is one of the nine, its document is not SARIF, and it does not
#: carry the envelope AAP 0.5.4's per-shape table names for that tool. A recognised name
#: over unrecognised bytes halts here (AAP 0.5.4, 0.9.2): a permissive detector would
#: route it to an adapter that finds none of its containers and reports zero rows, and
#: an empty result set is indistinguishable from a clean scan.
REASON_NATIVE_SHAPE_UNRECOGNIZED = "native-shape-unrecognized"

#: Every reason :class:`UnknownArtifactShape` can carry, as a closed set a reader of the
#: run record can enumerate.
UNKNOWN_SHAPE_REASONS = (
    REASON_UNRECOGNIZED_ARTIFACT_NAME,
    REASON_UNSUPPORTED_DOCUMENT_TYPE,
    REASON_SARIF_PRODUCER_NOT_SARIF,
    REASON_NATIVE_SHAPE_UNRECOGNIZED,
)

#: The same literal under the name the signature layer uses. One condition, two names:
#: the envelope layer calls the shape unrecognised and the signature layer calls it a
#: signature mismatch, and a record quoting two different strings for one condition
#: would report one defect as two.
REASON_NATIVE_SIGNATURE_MISMATCH = REASON_NATIVE_SHAPE_UNRECOGNIZED

#: The closed inventory of halt reasons, in the order :func:`route` tests them, for the
#: three consumers that enumerate them: a halt record, ``tool-status.md`` and the
#: validation criteria. It is :data:`UNKNOWN_SHAPE_REASONS` itself rather than a subset of
#: it: every reason :func:`route` raises is a halt, including a SARIF producer's artifact
#: that is not SARIF, and an inventory that omitted one would let a consumer enumerate a
#: closed set that the router can still step outside of. The two names remain because the
#: two layers speak of the same conditions differently -- the envelope layer names the
#: unrecognised shape, the signature layer names the signature mismatch -- and
#: :data:`REASON_NATIVE_SIGNATURE_MISMATCH` is the one literal under both names.
HALT_REASONS = UNKNOWN_SHAPE_REASONS


class UnknownArtifactShape(Exception):
    """Raised when an artifact matches neither SARIF nor a known native shape.

    AAP 0.5.4 requires this to be a halt rather than a best-effort parse, and AAP 0.9.2
    lists it among the conditions that stop the run. The exception carries everything a
    halt report needs to quote the observed structure without re-reading the artifact:
    the artifact path, the name that failed to resolve, the reason, the observed
    top-level type, the observed ``version`` value, the top-level keys, and whether the
    document nonetheless detected as SARIF.

    That last attribute is what distinguishes the two failures a reader must not
    conflate: a corrupt or foreign document, and a perfectly valid SARIF file that has
    no business in ``harness/artifacts/raw/``.

    ``expectation`` carries, for the two shape reasons, the prose statement of what the
    named tool's artifact must be -- taken from the same table the test uses, so the
    halt tells a reader what was expected and not merely that something was not it.
    """

    def __init__(
        self,
        *,
        reason: str,
        artifact_path: object = None,
        stem: object = None,
        document: object = None,
        sarif_detected: bool = False,
        description: Mapping[str, object] | None = None,
        expectation: str | None = None,
        signature: NativeSignature | None = None,
    ) -> None:
        described = dict(description) if description is not None else describe_document(document)
        self.reason = reason
        self.artifact_path = None if artifact_path is None else str(artifact_path)
        self.stem = None if stem is None else str(stem)
        self.expectation = expectation
        self.top_level_type = described.get("top_level_type")
        self.python_type = described.get("python_type")
        self.version = described.get("version")
        self.top_level_keys = tuple(described.get("top_level_keys") or ())
        self.top_level_length = described.get("top_level_length")
        self.sarif_detected = bool(sarif_detected)
        # None for the two reasons that quote no signature, so a consumer reads one
        # shape of record for every reason rather than branching on presence.
        self.signature_tool = None if signature is None else signature.tool
        self.expected_signature = None if signature is None else signature.statement
        self.signature_observation = (
            None if signature is None else signature.observe(document)
        )
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        subject = self.artifact_path or self.stem or "<unnamed document>"
        parts = [
            f"unknown artifact shape ({self.reason}): {subject}",
            f"top-level type {self.top_level_type}",
            f"version {_truncate_repr(self.version)}",
            f"top-level keys {_format_keys(self.top_level_keys)}",
        ]
        if self.reason == REASON_UNRECOGNIZED_ARTIFACT_NAME:
            if self.sarif_detected:
                parts.append(
                    "the document parses as SARIF "
                    f"{SARIF_VERSION} but its name is not one of the nine runner "
                    "artifacts; harness/artifacts/raw/ is runner-only (the Opengrep "
                    "taint A/B arms are valid SARIF and live in harness/artifacts/logs/)"
                )
            parts.append(f"expected one of: {', '.join(ARTIFACT_FILENAMES)}")
        elif self.reason == REASON_UNSUPPORTED_DOCUMENT_TYPE:
            parts.append(
                "every supported artifact is a JSON object or a JSON array; a scalar "
                "top level is not a shape any adapter owns"
            )
        else:
            # The two shape reasons. The name resolved to a tool and the container is
            # supported, so what failed is the shape itself -- and the expectation is
            # what a reader needs in order to tell a wrong-version SARIF from an
            # unrelated document sitting under a recognised name.
            parts.append(
                f"expected {self.expectation}"
                if self.expectation
                else "the document is not the shape this artifact name is routed for"
            )
            if self.signature_tool is not None:
                # The two halves a reader needs and cannot reconstruct from the artifact
                # name alone: what that writer's shape is, and what was there instead.
                parts.append(
                    f"{self.signature_tool} artifacts are {self.expected_signature}"
                )
                parts.append(self._format_observation())
            parts.append(
                "a recognised artifact name identifies the writing runner; it is not "
                "evidence that the bytes are that runner's shape, and routing "
                "unrecognised bytes to an adapter reports zero rows, which is "
                "indistinguishable from a clean scan (AAP 0.5.4)"
            )
        return "; ".join(parts)

    def _format_observation(self) -> str:
        """Render the signature observation as one bounded clause for the message.

        The structured form stays on :attr:`signature_observation` for the run record;
        this is the human sentence, and it names only the container that failed rather
        than restating the whole document.
        """
        observed = self.signature_observation or {}
        key = observed.get("required_key")
        if key is not None:
            return (
                f"observed {key} of type {observed.get('observed_key_type')} where a "
                f"JSON {observed.get('required_key_type')} is required"
            )
        element_key = observed.get("element_required_key")
        if element_key is not None and "first_failing_element_index" in observed:
            index = observed["first_failing_element_index"]
            element_type = observed.get("first_failing_element_type")
            count = observed.get("observed_element_count")
            if "first_failing_element_key_type" in observed:
                return (
                    f"observed element {index} of {count} carrying {element_key} of "
                    f"type {observed['first_failing_element_key_type']} where a JSON "
                    f"{observed.get('element_required_key_type')} is required"
                )
            return (
                f"observed element {index} of {count} of type {element_type} where a "
                "JSON object is required"
            )
        return (
            f"observed a top level of type {observed.get('observed_top_level_type')} "
            f"where one of {', '.join(observed.get('accepted_top_level_types') or ())} "
            "is required"
        )

    def details(self) -> dict[str, object]:
        """Return the halt as a flat mapping for the structured run record.

        Keys are stable, so ``harness/artifacts/logs/normalize-run.json`` and the halt
        section of ``oss-scan-results/run-record.md`` quote one measurement rather than
        two. ``top_level_keys`` is a list of per-key diagnostic objects -- each carrying
        the key's context, type, full character length, full sha256 and its published
        rendering -- and ``top_level_key_excerpts`` is the flat list of those
        renderings, so the mapping is JSON-serialisable even when the offending document
        keyed something exotic, and redacting the text does not discard the evidence.

        Nothing in the returned mapping is artifact-supplied text unless it is
        byte-equal to a literal this module authors (SEC-04). This method IS the
        persistence boundary for a halt -- what it returns is written into a 0644 record
        this pipeline preserves -- so the observed ``version`` and every observed key
        are published as an authored literal or as :data:`REDACTED_TEXT`, with their
        full length and full digest beside them.
        """
        halt_keys = _halt_keys(self.top_level_keys)
        return {
            "reason": self.reason,
            "artifact_path": self.artifact_path,
            "stem": self.stem,
            "expectation": self.expectation,
            "top_level_type": self.top_level_type,
            "python_type": self.python_type,
            # Redacted here, at the persistence boundary this method IS. The raw values
            # stay on the exception for an in-memory caller; what a durable record gets
            # is the safe rendering -- an authored literal or the fixed marker -- with
            # the full length and digest so what was redacted is still identified.
            "version": safe_scalar(self.version),
            "version_evidence": safe_value(self.version, context=SARIF_VERSION_KEY),
            # Rendered ONCE and read four ways: calling the renderer per field would
            # re-hash every key for each of them, and a reader comparing two fields
            # would be comparing two computations rather than one measurement.
            "top_level_keys": halt_keys["keys"],
            "top_level_key_excerpts": halt_keys["key_excerpts"],
            "top_level_keys_total": halt_keys["total"],
            "top_level_keys_reported": halt_keys["reported"],
            "top_level_keys_truncated": halt_keys["truncated"],
            "top_level_keys_evidence": halt_keys["per_key_evidence"],
            "top_level_length": self.top_level_length,
            "sarif_detected": self.sarif_detected,
            # Present for every reason, ``None`` for the two that quote no signature, so
            # a reader parses one record shape rather than branching on the reason.
            "signature_tool": self.signature_tool,
            "expected_signature": self.expected_signature,
            "signature_observation": (
                None
                if self.signature_observation is None
                else dict(self.signature_observation)
            ),
            "expected_artifacts": list(ARTIFACT_FILENAMES),
        }



def _halt_keys(keys: "tuple[object, ...]") -> dict[str, object]:
    """Bounded, sanitised top-level keys for a halt record."""
    return safe_keys(keys)


def _redacted_clause(described: Mapping[str, object]) -> str:
    """Render one redacted value as a clause for a halt MESSAGE.

    The message is the only channel some readers get -- it is what
    :class:`UnknownArtifactShape` passes to ``Exception.__init__``, so it is what
    reaches stderr and what a traceback shows -- and it is prose rather than a
    mapping. So the clause carries the same metadata the structured description does,
    inline: the Python type, the full character length and the **full** 64-hex digest,
    followed by the fixed marker. The full digest rather than a prefix, because this
    clause has to stand on its own: a 16-hex prefix identifies a value only against a
    record that already holds the whole digest.

    It is still smaller than what it replaces. The previous rendering allowed a
    512-character excerpt per value and up to twelve of them in one message.
    """
    return (
        f"a {described['value_type']} of length {described['character_length']} "
        f"(sha256 {described['sha256']}; {REDACTED_TEXT})"
    )


def _truncate_repr(value: object, limit: int = _MESSAGE_VALUE_LIMIT) -> str:
    """Render one artifact-supplied value for a halt message, publishing no content.

    Three cases, the same three :func:`safe_scalar` makes, because a message and a
    record must not disagree about what a value was:

    * an inert scalar -- ``None``, a ``bool``, an ``int``, a ``float`` -- is rendered
      from its ``repr`` in full. ``version None`` is the diagnosis for a document
      carrying no ``version`` at all, and redacting it would say nothing;
    * a ``str`` byte-equal to an authored literal is rendered from its ``repr``, so a
      halt on a document whose ``version`` is the accepted ``"2.1.0"`` still quotes
      it;
    * anything else is :func:`_redacted_clause`. That covers the wrong-version SARIF
      this module halts on: ``"2.1.0-rtm.5"`` is a value only the artifact knows, and
      a version field is as good a place to hide a credential as any other (SEC-04).
      ``version_matches`` in the structured details still records the verdict, and
      the length and digest still identify what was sent.
    """
    if value is None or isinstance(value, (bool, int, float)):
        return repr(value)
    described = safe_value(value, limit=limit)
    if described["publishable"]:
        return repr(value)
    return _redacted_clause(described)


def _format_keys(keys: tuple[object, ...], limit: int = _MESSAGE_KEY_LIMIT) -> str:
    """Render top-level *keys* bounded to *limit* entries for a halt message.

    A key that is one of the well-known authored names is printed as itself, so a
    document whose top level is ``$schema``/``version``/``runs`` still reads as
    itself. Every other key is a redaction clause carrying its type, full length and
    full digest: this message is carried verbatim into the halt record and to stderr,
    so a key is artifact-supplied text like any other (SEC-04), and an ESC sequence or
    a credential in one would otherwise reach whatever renders it.
    """
    if not keys:
        return "[]"
    shown: list[str] = []
    for key in keys[:limit]:
        if is_publishable_literal(key):
            shown.append(str(key))
        else:
            shown.append(_redacted_clause(safe_value(key)))
    suffix = "" if len(keys) <= limit else f", ...(+{len(keys) - limit} more)"
    return f"[{', '.join(shown)}{suffix}]"


# --------------------------------------------------------------------------------------
# Routing
# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RoutingDecision:
    """One artifact's routing outcome: who wrote it, what shape it is, who reads it.

    Frozen, because a decision is a measurement: the same artifact must route the same
    way for the reconciliation identity and the ``tool-status.md`` entry, and a mutable
    decision invites one of those two to be recomputed differently. ``__post_init__``
    rejects an inconsistent decision -- a hand-built one claiming the SARIF shape while
    naming a native adapter, or a ``scanner_class`` that contradicts the authored table
    -- so an error surfaces where the decision is made rather than in the emitted rows.

    :param tool: the canonical tool identifier of the runner that wrote the artifact.
    :param shape: :data:`SHAPE_SARIF` or :data:`SHAPE_NATIVE`, as detected.
    :param adapter: the adapter **module key** (a string; this module imports no
        adapter, and ``cli.py`` resolves the key to a callable).
    :param scanner_class: the tool's fixed class, or :data:`PER_RECORD` for Trivy.
    :param artifact_path: the path or name the decision was made about, as given, for
        the run record. ``None`` when the caller supplied only a tool identifier.
    """

    tool: str
    shape: str
    adapter: str
    scanner_class: object
    artifact_path: str | None = None

    def __post_init__(self) -> None:
        if self.tool not in _CANONICAL_TOOL_SET:
            raise ValueError(
                f"unknown tool {self.tool!r}; expected one of: "
                f"{', '.join(CANONICAL_TOOLS)}"
            )
        if self.shape not in SHAPES:
            raise ValueError(
                f"unknown shape {self.shape!r}; expected one of: {', '.join(SHAPES)}"
            )
        expected_adapter = (
            SHARED_SARIF_ADAPTER
            if self.shape == SHAPE_SARIF
            else ADAPTER_MODULE_BY_TOOL[self.tool]
        )
        if self.adapter != expected_adapter:
            raise ValueError(
                f"adapter {self.adapter!r} contradicts the routing table for tool "
                f"{self.tool!r} at shape {self.shape!r}: expected "
                f"{expected_adapter!r}"
            )
        expected_class = SCANNER_CLASS_BY_TOOL[self.tool]
        if self.scanner_class is not expected_class and self.scanner_class != expected_class:
            raise ValueError(
                f"scanner_class {self.scanner_class!r} contradicts the authored table "
                f"for tool {self.tool!r}: expected {expected_class!r}"
            )
        if self.artifact_path is not None and not isinstance(self.artifact_path, str):
            # Normalise a path-like into a string on a frozen instance, so consumers
            # never have to care which form the caller passed.
            object.__setattr__(self, "artifact_path", str(self.artifact_path))

    @property
    def is_sarif_shape(self) -> bool:
        """``True`` when the document satisfied both SARIF conditions."""
        return self.shape == SHAPE_SARIF

    @property
    def is_native_shape(self) -> bool:
        """``True`` when the artifact routes to a per-tool native adapter."""
        return self.shape == SHAPE_NATIVE

    @property
    def scanner_class_is_per_record(self) -> bool:
        """``True`` for Trivy, whose class each record takes from its own section."""
        return is_per_record(self.scanner_class)

    @property
    def adapter_module_name(self) -> str:
        """The adapter's importable module name, e.g. ``normalize.adapters.sarif``.

        A string built from :data:`ADAPTER_PACKAGE`; nothing is imported here.
        """
        return f"{ADAPTER_PACKAGE}.{self.adapter}"

    def as_dict(self) -> dict[str, object]:
        """Return the decision as a JSON-serialisable mapping for the run record.

        Trivy's sentinel is rendered as :data:`PER_RECORD_LABEL`. That label exists for
        ``harness/artifacts/logs/normalize-run.json`` only -- it is never a dataset
        value, and the sentinel itself still refuses to be stringified anywhere else.
        """
        return {
            "tool": self.tool,
            "shape": self.shape,
            "adapter": self.adapter,
            "adapter_module": self.adapter_module_name,
            "scanner_class": (
                PER_RECORD_LABEL
                if self.scanner_class_is_per_record
                else str(self.scanner_class)
            ),
            "scanner_class_per_record": self.scanner_class_is_per_record,
            "artifact_path": self.artifact_path,
        }


def route(
    stem_or_tool: object,
    doc: object,
    *,
    artifact_path: object = None,
) -> RoutingDecision:
    """Route an already-parsed artifact to the adapter that reads it.

    *stem_or_tool* is the writing runner's identity -- a canonical tool identifier, one
    of the nine artifact filenames, or a path ending in one. *doc* is the parsed
    document. Routing is keyed by the writer, never sniffed from content: a native
    document is never fingerprinted to guess its tool (AAP 0.5.4).

    A document satisfying :func:`is_sarif` routes to the one shared ``sarif`` adapter;
    anything else must satisfy the writer's own native envelope and routes to that
    tool's adapter. For the three SARIF producers both paths name ``sarif`` anyway, so
    the shape and the table agree.

    A known name is **not** on its own a licence to route. It says who wrote the
    artifact; it is no evidence that the bytes are that writer's shape, and this
    function refuses two documents a name alone would have let through:

    * a document under one of the three SARIF producers' names that fails the SARIF
      conjunction -- a ``version`` this dataset has not been validated against being the
      case that matters, since those runners write SARIF and nothing else;
    * a document under any recognised name that carries neither the SARIF conjunction
      nor the envelope AAP 0.5.4's per-shape table names for that tool.

    Both are halts for the same reason the detector's own strictness exists: an adapter
    handed a document with none of the containers it walks reports zero rows, and an
    empty result set is indistinguishable from a clean scan. What is checked is the
    envelope and only the envelope -- the container is present and is the right JSON
    type -- so an **empty** report from any tool still routes, and every judgement about
    record structure inside the container remains the adapter's, including the Trivy
    unsupported-section halt.

    :param artifact_path: overrides what the halt report and
        :attr:`RoutingDecision.artifact_path` name. Defaults to *stem_or_tool*.
    :raises UnknownArtifactShape: when the name is not one of the nine (including a
        valid SARIF document under an unexpected name, which ``harness/artifacts/raw/``
        must never contain); when the document's top level is neither a JSON object nor
        a JSON array; when a SARIF producer's artifact is not SARIF; or when a native
        artifact does not carry its tool's envelope.
    :raises TypeError: when *stem_or_tool* is neither a string nor a path-like, which is
        a caller bug rather than an artifact condition.
    """
    subject = stem_or_tool if artifact_path is None else artifact_path
    tool = resolve_tool(stem_or_tool)
    sarif = is_sarif(doc)

    if tool is None:
        raise UnknownArtifactShape(
            reason=REASON_UNRECOGNIZED_ARTIFACT_NAME,
            artifact_path=subject,
            stem=_artifact_name(stem_or_tool),
            document=doc,
            sarif_detected=sarif,
        )

    if not _is_supported_container(doc):
        raise UnknownArtifactShape(
            reason=REASON_UNSUPPORTED_DOCUMENT_TYPE,
            artifact_path=subject,
            stem=tool,
            document=doc,
            sarif_detected=sarif,
        )

    if not sarif and not matches_native_shape(tool, doc):
        # Two distinct reasons over one condition, because they send a reader to
        # different places: a SARIF producer's artifact that is not SARIF is a producer
        # or version problem, and any other recognised name over an unrecognised
        # envelope is a mis-written or mis-placed artifact.
        raise UnknownArtifactShape(
            reason=(
                REASON_SARIF_PRODUCER_NOT_SARIF
                if tool in SARIF_PRODUCERS
                else REASON_NATIVE_SHAPE_UNRECOGNIZED
            ),
            artifact_path=subject,
            stem=tool,
            document=doc,
            sarif_detected=sarif,
            expectation=native_shape_requirement(tool),
            signature=native_signature_for(tool),
        )

    # Named detected_shape rather than shape: a local called shape inside shape.py reads
    # as the module and is exactly the kind of ambiguity a later editor trips over.
    detected_shape = SHAPE_SARIF if sarif else SHAPE_NATIVE
    adapter = SHARED_SARIF_ADAPTER if sarif else ADAPTER_MODULE_BY_TOOL[tool]
    return RoutingDecision(
        tool=tool,
        shape=detected_shape,
        adapter=adapter,
        scanner_class=SCANNER_CLASS_BY_TOOL[tool],
        artifact_path=None if subject is None else str(subject),
    )


def route_artifact(path: object, doc: object) -> RoutingDecision:
    """Route the artifact at *path*, whose parsed document is *doc*.

    A convenience over :func:`route` for walking ``harness/artifacts/raw/``: the
    filename identifies the writing runner and the full path is what the run record and
    any halt report name. Only artifacts in that directory are routed -- the taint A/B
    arms and the capability probe write outside it and contribute no dataset row
    (AAP 0.1.3).
    """
    return route(path, doc, artifact_path=path)
