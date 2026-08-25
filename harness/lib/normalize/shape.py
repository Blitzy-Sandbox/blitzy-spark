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
fingerprinted to guess which tool produced it. Six adapters serve nine tools:

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

* A **known** artifact name never halts here. A ``trivy.json`` that parses as JSON
  but carries no ``Results`` key still routes to the Trivy adapter, which owns its
  own structural validation and its own halt (the non-empty unsupported-section
  case). This module does not pre-empt an adapter's validation.
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
without importing six adapters. Standard library only -- no third-party import, no
manifest, no lockfile, no install step (AAP 0.4.1).

There is deliberately no ``__init__.py`` under ``harness/lib/normalize/``: PEP 420
namespace packages make ``import normalize.shape`` work once ``harness/lib`` is on
``sys.path``.

No user-specified rules govern this file -- ``review_rules`` reports
"No user rules provided.", corroborated by AAP 0.7 and 0.10.2 -- so
enterprise-standard best practice applies in their place.
"""

from __future__ import annotations

# Standard library only, and only these four modules:
#   collections.abc -- Mapping, so the detector accepts any parsed-JSON mapping;
#   dataclasses     -- the frozen RoutingDecision;
#   pathlib         -- PurePath, to take an artifact's filename without importing os;
#   types           -- MappingProxyType, so the authored tables cannot be mutated by a
#                      consumer (these are closed sets; a mutable dict invites drift).
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import PurePath
from types import MappingProxyType

__all__ = [
    # SARIF detection
    "SARIF_VERSION",
    "SARIF_VERSION_KEY",
    "SARIF_RUNS_KEY",
    "is_sarif",
    "describe_document",
    "detection_evidence",
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
# Adapter routing table: six adapters serve nine tools
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
#: so the adapter and the validation criteria share one authored list. This module
#: performs no structural validation of its own.
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
    return {
        "test": (
            f'a document is SARIF when {SARIF_VERSION_KEY} == "{SARIF_VERSION}" '
            f"AND {SARIF_RUNS_KEY} is an array; those two together are the whole test "
            "(AAP 0.5.4). Nothing else is consulted: not $schema, not the filename, "
            "not tool.driver."
        ),
        "top_level_type": description["top_level_type"],
        "top_level_keys": description["top_level_keys"],
        "top_level_length": description["top_level_length"],
        "version_key": SARIF_VERSION_KEY,
        "version_expected": SARIF_VERSION,
        "version_observed": version_observed,
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

    Six adapters serve nine tools: the three SARIF producers share ``sarif``. Raises
    ``ValueError`` for anything outside the nine canonical identifiers.
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
    ) -> None:
        described = dict(description) if description is not None else describe_document(document)
        self.reason = reason
        self.artifact_path = None if artifact_path is None else str(artifact_path)
        self.stem = None if stem is None else str(stem)
        self.top_level_type = described.get("top_level_type")
        self.python_type = described.get("python_type")
        self.version = described.get("version")
        self.top_level_keys = tuple(described.get("top_level_keys") or ())
        self.top_level_length = described.get("top_level_length")
        self.sarif_detected = bool(sarif_detected)
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
        else:
            parts.append(
                "every supported artifact is a JSON object or a JSON array; a scalar "
                "top level is not a shape any adapter owns"
            )
        return "; ".join(parts)

    def details(self) -> dict[str, object]:
        """Return the halt as a flat mapping for the structured run record.

        Keys are stable, so ``harness/artifacts/logs/normalize-run.json`` and the halt
        section of ``oss-scan-results/run-record.md`` quote one measurement rather than
        two. ``top_level_keys`` is a list of strings, so the mapping is JSON-serialisable
        even when the offending document keyed something exotic.
        """
        return {
            "reason": self.reason,
            "artifact_path": self.artifact_path,
            "stem": self.stem,
            "top_level_type": self.top_level_type,
            "python_type": self.python_type,
            "version": (
                self.version
                if self.version is None or isinstance(self.version, (str, int, float, bool))
                else repr(self.version)
            ),
            "top_level_keys": [str(key) for key in self.top_level_keys],
            "top_level_length": self.top_level_length,
            "sarif_detected": self.sarif_detected,
            "expected_artifacts": list(ARTIFACT_FILENAMES),
        }


def _truncate_repr(value: object, limit: int = _MESSAGE_VALUE_LIMIT) -> str:
    """Return ``repr(value)`` bounded to *limit* characters for a halt message."""
    text = repr(value)
    if len(text) <= limit:
        return text
    return f"{text[:limit]}...(+{len(text) - limit} chars)"


def _format_keys(keys: tuple[object, ...], limit: int = _MESSAGE_KEY_LIMIT) -> str:
    """Render top-level *keys* bounded to *limit* entries for a halt message."""
    if not keys:
        return "[]"
    shown = [str(key) for key in keys[:limit]]
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
    anything else routes to the native adapter keyed by the writer. For the three SARIF
    producers both paths name ``sarif`` anyway, so the shape and the table agree.

    A **known** name never halts here. A ``trivy.json`` that parses as JSON but carries
    no ``Results`` key still routes to the Trivy adapter, which owns its own structural
    validation and its own halt; this function does not pre-empt an adapter's
    validation.

    :param artifact_path: overrides what the halt report and
        :attr:`RoutingDecision.artifact_path` name. Defaults to *stem_or_tool*.
    :raises UnknownArtifactShape: when the name is not one of the nine (including a
        valid SARIF document under an unexpected name, which ``harness/artifacts/raw/``
        must never contain), or when the document's top level is neither a JSON object
        nor a JSON array.
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

